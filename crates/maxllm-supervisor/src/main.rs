// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! `maxllm-supervisor` — layer 1 of MaxLLM.
//!
//! Layer 1 owns the public listen socket, the URL path table, and the
//! lifecycle of one or more layer-2 worker generations. It never runs
//! provider logic; it forwards whole requests to a worker over a private Unix
//! socket and adds generation/build headers to the response.
//!
//! A layer-2 version update is: spawn the new binary, wait for `/health`,
//! atomically swap the backend pointer, then drain the old generation. The
//! public listener is never closed and in-flight requests finish on the
//! generation they started on.

mod control;
mod proxy;
mod state;
mod watch;
mod worker;

use clap::Parser;
use maxllm_config::Config;
use pingora::prelude::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "maxllm-supervisor",
    version,
    about = "MaxLLM layer-1 supervisor: stable entry, hot-upgradable layer-2 workers"
)]
struct Args {
    /// Path to the shared maxllm configuration file
    #[arg(short, long, default_value = "maxllm.toml")]
    config: PathBuf,

    /// Daemonise: fork off before this process owns any threads, then keep
    /// running in the background
    #[arg(short, long)]
    daemon: bool,

    /// Disable config hot-reload (routes/backends only; workers keep their
    /// own watcher unless started with --no-reload via backend args)
    #[arg(long)]
    no_reload: bool,

    /// Validate the supervisor section and print the resolved plan, then exit
    #[arg(long)]
    check: bool,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config = Config::from_file(&args.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config from {:?}: {e}", args.config);
        std::process::exit(1);
    });
    let sup_cfg = config.supervisor.clone().unwrap_or_else(|| {
        eprintln!(
            "Config {:?} has no [supervisor] section. Add one to run the two-layer setup, \
             or run maxllm-server directly for single-process mode.",
            args.config
        );
        std::process::exit(2);
    });

    if args.check {
        let names = state::desired_backend_names(&sup_cfg);
        let routes = state::derive_base_routes(&config, &sup_cfg);
        println!(
            "layer-1 check OK: control={} backends={:?} routes={}",
            sup_cfg.control_listen,
            names,
            routes.len()
        );
        return;
    }

    if !sup_cfg.control_listen.ip().is_loopback() {
        tracing::warn!(
            control = %sup_cfg.control_listen,
            "supervisor control API is not loopback-only; it can replace worker binaries"
        );
    }

    // Bind the control listener before daemonising, so a bad control address
    // fails the foreground process with a readable message instead of leaving
    // a daemon that silently exits.
    let control_std = std::net::TcpListener::bind(sup_cfg.control_listen).unwrap_or_else(|e| {
        eprintln!(
            "Failed to bind layer-1 control API on {}: {e}",
            sup_cfg.control_listen
        );
        std::process::exit(1);
    });
    control_std
        .set_nonblocking(true)
        .expect("set control listener nonblocking");

    // Last single-threaded point in `main`: fork here so the daemon owns the
    // control listener and every thread is created inside the forked process.
    // The listener is bound first, so a bad control address still fails the
    // foreground process with a readable message.
    if args.daemon {
        daemonize_detached();
    }

    // `--daemon` is handled above, not by pingora: pingora daemonises inside
    // `Server::run()`, which runs *after* `Supervisor::new` has spawned the
    // lifecycle thread (the parent of every layer-2 worker) and the control
    // API thread. A fork there silently drops both, leaving a bound control
    // listener nobody accepts on and workers killed through
    // `PR_SET_PDEATHSIG` the moment the pre-fork process exits.
    let opt = Opt::default();
    let mut server = Server::new(Some(opt)).expect("Failed to create Pingora server");
    {
        let conf = Arc::get_mut(&mut server.configuration)
            .expect("configuration must be uniquely owned before bootstrap");
        if let Some(threads) = config.server.threads {
            conf.threads = threads;
        }
        conf.work_stealing = true;
        conf.grace_period_seconds = Some(config.server.grace_period_secs);
    }
    // Threads must only be created now — `Supervisor::new`'s lifecycle thread
    // is the parent of every worker and has to belong to the final process.
    server.bootstrap();

    let sup = state::Supervisor::new(args.config.clone(), &config, &sup_cfg);

    // Layer 2 comes up before the public listener does, so the first client
    // request already has a healthy worker to talk to.
    sup.start_all(&config, &sup_cfg).unwrap_or_else(|e| {
        tracing::error!(error = %e, "failed to start layer-2 workers");
        std::process::exit(1);
    });
    if let Err(e) = sup.load_persisted_routes() {
        tracing::warn!(error = %e, "ignoring persisted route overlay");
    }
    sup.rebuild_routes();

    // Control API on its own runtime thread.
    {
        let sup = sup.clone();
        std::thread::Builder::new()
            .name("supervisor-control".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to build control runtime");
                        return;
                    }
                };
                rt.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(control_std) {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to adopt control listener");
                            return;
                        }
                    };
                    if let Err(e) = control::serve(sup, listener).await {
                        tracing::error!(error = %e, "control API stopped");
                    }
                });
            })
            .expect("spawn control thread");
    }

    if !args.no_reload {
        watch::start(args.config.clone(), sup.clone());
    }

    let listen = config.server.listen;
    let proxy = proxy::Layer1Proxy::new(sup.clone());
    let mut service = http_proxy_service(&server.configuration, proxy);
    let mut sock_opt = pingora::listeners::TcpSocketOptions::default();
    sock_opt.so_reuseport = Some(config.server.tcp_reuseport);
    sock_opt.tcp_fastopen = config.server.tcp_fastopen;
    service.add_tcp_with_settings(&listen.to_string(), sock_opt);
    server.add_service(service);

    tracing::info!(
        layer = 1,
        version = env!("CARGO_PKG_VERSION"),
        listen = %listen,
        control = %sup_cfg.control_listen,
        "MaxLLM layer-1 supervisor is running"
    );
    server.run_forever();
}

/// Detach from the calling terminal: fork once, `setsid` in the child and
/// exit the parent.
///
/// Called from `main` while the process is still single-threaded — the only
/// safe place to fork. The `umask` matches what pingora's own daemoniser
/// would have applied, so files the daemon creates (sockets, the admin store,
/// recordings) keep the same permissions as in a pingora daemon.
fn daemonize_detached() {
    // SAFETY: no thread exists yet, and both syscalls are async-signal-safe.
    unsafe {
        match libc::fork() {
            -1 => {
                eprintln!(
                    "failed to fork for --daemon: {}",
                    std::io::Error::last_os_error()
                );
                std::process::exit(1);
            }
            0 => {
                if libc::setsid() == -1 {
                    eprintln!(
                        "failed to create a session for --daemon: {}",
                        std::io::Error::last_os_error()
                    );
                    std::process::exit(1);
                }
                libc::umask(0o007);
            }
            // The launcher exits: the caller gets its prompt back while the
            // forked child carries on as the daemon.
            _ => std::process::exit(0),
        }
    }
}

/// Keep the unused import warning quiet on platforms where the type is only
/// used in a cfg branch.
#[allow(dead_code)]
fn _assert_socket_addr(_: SocketAddr) {}
