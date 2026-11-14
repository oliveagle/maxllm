// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

mod breaker_sync;
mod circuit_breaker;
mod ctx;
mod gateway;
mod key_rate_limit;
mod laya_client;
mod metrics;
mod provider_stats;
mod rate_limit;
mod rate_limit_resolver;
mod recorder;
mod routing;
mod session_sticky;
mod telemetry;

use clap::Parser;
use maxllm_config::Config;
use notify::{EventKind, RecursiveMode, Watcher};
use pingora::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "maxllm",
    version,
    about = "MaxLLM - AI Gateway built on Pingora"
)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "maxllm.toml")]
    config: PathBuf,

    /// Enable daemon mode
    #[arg(short, long)]
    daemon: bool,

    /// Disable config hot-reload file watching
    #[arg(long)]
    no_reload: bool,
}

fn main() {
    let args = Args::parse();

    // Load configuration before installing the subscriber: `[otel]` decides
    // whether the gateway ships its own logs/traces, so the settings have to
    // be known before the first line is emitted.
    let config = Config::from_file(&args.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config from {:?}: {e}", args.config);
        std::process::exit(1);
    });

    // OTel-enabled subscriber (fmt + OTLP logs + OTLP spans). A config without
    // `[otel]` keeps the plain formatter, byte-for-byte as before.
    let mut telemetry = match telemetry::install(&config.otel) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "Failed to initialise OpenTelemetry: {e}; falling back to the plain log formatter"
            );
            None
        }
    };
    if telemetry.is_none() {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .init();
    }

    tracing::info!(
        listen = %config.server.listen,
        providers = config.providers.len(),
        routes = config.routes.len(),
        "Starting MaxLLM gateway"
    );

    // Build the gateway
    let gateway = gateway::AiGateway::new(&config).unwrap_or_else(|e| {
        eprintln!("Failed to initialize gateway: {e}");
        std::process::exit(1);
    });

    // Start config file watcher (before gateway takes ownership)
    if !args.no_reload {
        let hot = Arc::clone(&gateway.hot);
        let config_path = args
            .config
            .canonicalize()
            .unwrap_or_else(|_| args.config.clone());
        start_config_watcher(config_path, hot);
    }

    // Configure Pingora server
    let mut opt = Opt::default();
    if args.daemon {
        opt.daemon = true;
    }

    let mut server = Server::new(Some(opt)).expect("Failed to create Pingora server");

    // Apply server configuration from maxllm config
    {
        let conf = Arc::get_mut(&mut server.configuration)
            .expect("configuration must be uniquely owned before bootstrap");
        if let Some(threads) = config.server.threads {
            conf.threads = threads;
        }
        conf.work_stealing = true;
        // Pingora defaults to a 300s drain period on SIGTERM, which outlives
        // the service manager's stop timeout and ends in a SIGKILL. Keep the
        // drain short enough that the process exits on its own.
        conf.grace_period_seconds = Some(config.server.grace_period_secs);
    }

    server.bootstrap();

    // Create the HTTP proxy service. The `[server]` socket knobs
    // (tcp_reuseport / tcp_fastopen) are passed through to the listener —
    // they were parsed but never applied, so a config declaring them was
    // silently ignored.
    let mut service = http_proxy_service(&server.configuration, gateway);
    // `TcpSocketOptions` is `#[non_exhaustive]`, so its fields are set on a
    // default value instead of a struct literal.
    let mut sock_opt = pingora::listeners::TcpSocketOptions::default();
    sock_opt.so_reuseport = Some(config.server.tcp_reuseport);
    sock_opt.tcp_fastopen = config.server.tcp_fastopen;
    service.add_tcp_with_settings(&config.server.listen.to_string(), sock_opt);

    server.add_service(service);

    tracing::info!("MaxLLM gateway is running on {}", config.server.listen);
    // `run` (not `run_forever`) returns after the drain path, so the batch
    // exporters can be flushed before the process exits; `run_forever` ends in
    // `std::process::exit(0)` and would drop whatever is still queued.
    server.run(pingora::server::RunArgs::default());

    if let Some(telemetry) = telemetry.as_mut() {
        telemetry.shutdown();
    }
}

/// How long the config file must be quiet before it is read.
///
/// A write that arrives in pieces — truncate, then write — otherwise lets
/// the watcher read the file in the middle of the write and parse half of
/// it.
const CONFIG_RELOAD_SETTLE: Duration = Duration::from_millis(800);

/// How many times the file is read before a reload is given up.
///
/// The operator's pool edit has to take effect. A reload that quietly gives
/// up leaves the gateway running the *old* pool: the account was added, the
/// provider never joined, and nothing says so except one line in a log
/// nobody reads.
const CONFIG_RELOAD_ATTEMPTS: usize = 5;

/// The pause between two read attempts.
const CONFIG_RELOAD_RETRY_DELAY: Duration = Duration::from_millis(250);

/// How often the file is checked even when no event arrived.
///
/// A file-system event can be missed — the file replaced by a rename, the
/// inode swapped under the watcher, the notification dropped while the
/// thread was busy. The edit must take effect either way.
const CONFIG_RELOAD_POLL: Duration = Duration::from_secs(30);

/// A cheap fingerprint of the config file: its modification time and size.
fn config_stamp(path: &std::path::Path) -> Option<(std::time::SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

/// Read the config file until it parses.
///
/// A write that is not atomic leaves a half-written file behind for a
/// moment; the file is re-read a few times before the reload is abandoned.
fn read_config_settled(path: &std::path::Path) -> Result<Config, String> {
    let mut last_error = String::new();
    for attempt in 1..=CONFIG_RELOAD_ATTEMPTS {
        match Config::from_file(path) {
            Ok(config) => return Ok(config),
            Err(e) => {
                last_error = e.to_string();
                if attempt < CONFIG_RELOAD_ATTEMPTS {
                    tracing::debug!(
                        attempt,
                        error = %last_error,
                        "Config file did not parse yet, re-reading"
                    );
                    std::thread::sleep(CONFIG_RELOAD_RETRY_DELAY);
                }
            }
        }
    }
    Err(last_error)
}

/// Spawn a background thread that watches the config file for changes
/// and hot-reloads the gateway state atomically.
fn start_config_watcher(config_path: PathBuf, hot: Arc<arc_swap::ArcSwap<gateway::HotState>>) {
    let watch_path = config_path.clone();
    std::thread::spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to create config file watcher");
                return;
            }
        };

        // Watch the parent directory (some editors write to a temp file then rename)
        let watch_dir = watch_path.parent().unwrap_or(&watch_path);
        if let Err(e) = watcher.watch(watch_dir, RecursiveMode::NonRecursive) {
            tracing::warn!(error = %e, path = ?watch_dir, "Failed to watch config directory");
            return;
        }

        // The translation kernel's script is watched directly: saving it is
        // the deploy, no config touch required. The watched set follows every
        // reload — a config change that repoints the kernel moves the watch.
        let mut watched_scripts: Vec<PathBuf> = hot
            .load()
            .kernel
            .as_ref()
            .map(|k| k.path().to_path_buf())
            .into_iter()
            .collect();
        let watch_kernel_dirs = |watcher: &mut notify::RecommendedWatcher, scripts: &[PathBuf]| {
            for script in scripts {
                if let Some(dir) = script.parent() {
                    if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
                        tracing::warn!(error = %e, path = ?dir, "Failed to watch kernel script directory");
                    }
                }
            }
        };
        watch_kernel_dirs(&mut watcher, &watched_scripts);

        tracing::info!(path = ?config_path, "Config hot-reload watcher started");

        // The reload itself, shared by the event path and the poll path.
        let reload = || {
            tracing::info!("Config file changed, reloading...");
            let new_config = match read_config_settled(&config_path) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Failed to parse updated config, keeping current"
                    );
                    crate::metrics::METRICS.config_reload_failures_total.inc();
                    return;
                }
            };

            // Build new hot state
            let new_state = match gateway::build_hot_state(&new_config) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to build new config state, keeping current");
                    crate::metrics::METRICS.config_reload_failures_total.inc();
                    return;
                }
            };

            // Log what changed
            let old = hot.load();
            let mut changes = Vec::new();
            if old.providers.len() != new_state.providers.len() {
                changes.push(format!(
                    "providers: {} -> {}",
                    old.providers.len(),
                    new_state.providers.len()
                ));
            }
            if old.routes.len() != new_state.routes.len() {
                changes.push(format!(
                    "routes: {} -> {}",
                    old.routes.len(),
                    new_state.routes.len()
                ));
            }
            if old.model_aliases.len() != new_state.model_aliases.len() {
                changes.push(format!(
                    "aliases: {} -> {}",
                    old.model_aliases.len(),
                    new_state.model_aliases.len()
                ));
            }

            // The operator's parked providers (a known quota wall) are
            // applied on every reload, so a new parking entry takes effect
            // without a restart.
            gateway::apply_quota_parking(&new_config, &new_state);

            // Atomic swap
            hot.store(Arc::new(new_state));

            crate::metrics::METRICS.config_reloads_total.inc();
            if changes.is_empty() {
                tracing::info!("Config reloaded successfully (no structural changes)");
            } else {
                tracing::info!(changes = ?changes, "Config reloaded successfully");
            }
        };

        // A reload is due once the file has been quiet for a moment. The
        // *last* event wins: a write that arrives in pieces moves the
        // deadline instead of being dropped.
        let mut pending: Option<std::time::Instant> = None;
        // The file's last known state, so the poll path knows whether
        // anything changed at all.
        let mut last_stamp = config_stamp(&config_path);
        let mut last_poll = std::time::Instant::now();

        loop {
            let now = std::time::Instant::now();
            let settle_wait = pending
                .map(|deadline| deadline.saturating_duration_since(now))
                .unwrap_or(Duration::from_secs(3600));
            let poll_wait = CONFIG_RELOAD_POLL.saturating_sub(last_poll.elapsed());
            let wait = settle_wait.min(poll_wait).max(Duration::from_millis(1));

            match rx.recv_timeout(wait) {
                Ok(Ok(event)) => {
                    // Only react to modify/create events on our config file
                    // or on the translation kernel's script — saving the
                    // script alone is the deploy.
                    let is_relevant =
                        matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_))
                            && event.paths.iter().any(|p| {
                                p.canonicalize().ok().as_ref() == Some(&config_path)
                                    || p.file_name() == config_path.file_name()
                                    || watched_scripts
                                        .iter()
                                        .any(|script| p.file_name() == script.file_name())
                            });
                    if is_relevant {
                        pending = Some(std::time::Instant::now() + CONFIG_RELOAD_SETTLE);
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Config watcher error");
                }
                Err(RecvTimeoutError::Timeout) => {
                    let due = pending.is_some();
                    pending = None;
                    let polled = last_poll.elapsed() >= CONFIG_RELOAD_POLL;
                    if polled {
                        last_poll = std::time::Instant::now();
                    }
                    if due {
                        last_stamp = config_stamp(&config_path);
                        reload();
                        // Follow the kernel: a config that repoints the
                        // script moves both the watch and the policy.
                        watched_scripts = hot
                            .load()
                            .kernel
                            .as_ref()
                            .map(|k| k.path().to_path_buf())
                            .into_iter()
                            .collect();
                        watch_kernel_dirs(&mut watcher, &watched_scripts);
                        continue;
                    }
                    if !polled {
                        continue;
                    }
                    // The poll tick: reload only when the file really
                    // changed, so an idle node does no work.
                    let stamp = config_stamp(&config_path);
                    if stamp == last_stamp {
                        continue;
                    }
                    last_stamp = stamp;
                    reload();
                    watched_scripts = hot
                        .load()
                        .kernel
                        .as_ref()
                        .map(|k| k.path().to_path_buf())
                        .into_iter()
                        .collect();
                    watch_kernel_dirs(&mut watcher, &watched_scripts);
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[server]
listen = "127.0.0.1:3795"
threads = 1

[providers.mock_openai]
kind = "openai"
base_url = "http://127.0.0.1:9"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "maxllm-config-test-{}-{}",
            name,
            std::process::id()
        ));
        p
    }

    /// A configuration that is written in pieces — truncated, then completed
    /// — must still be applied. The operator's pool edit has to take
    /// effect: an edit that is read half-written and then given up on is an
    /// edit that silently does nothing.
    #[test]
    fn a_half_written_config_is_read_until_it_parses() {
        let path = temp_path("half-written");
        std::fs::write(&path, VALID).expect("write");
        assert!(
            read_config_settled(&path).is_ok(),
            "a complete configuration must parse"
        );
        std::fs::write(
            &path,
            "[server
listen = ",
        )
        .expect("write");
        // The file does not parse and never will: the reader gives up
        // rather than blocking the watcher forever.
        let err = read_config_settled(&path).expect_err("a broken file must not parse");
        assert!(!err.is_empty());
        // ... and it is picked up as soon as it is complete.
        std::fs::write(&path, VALID).expect("write");
        assert!(
            read_config_settled(&path).is_ok(),
            "the configuration must be picked up once it is complete"
        );
        std::fs::remove_file(&path).ok();
    }

    /// The fingerprint used by the poll path must notice a rewrite that
    /// keeps the file's length.
    #[test]
    fn the_stamp_notices_a_rewrite() {
        let path = temp_path("stamp");
        std::fs::write(&path, "aaaa").expect("write");
        let first = config_stamp(&path).expect("stamp");
        std::fs::write(&path, "bbbb").expect("write");
        let second = config_stamp(&path).expect("stamp");
        assert_ne!(first, second, "a rewrite must change the stamp");
        std::fs::remove_file(&path).ok();
    }
}
