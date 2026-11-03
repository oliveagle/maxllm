// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

mod circuit_breaker;
mod ctx;
mod gateway;
mod metrics;
mod recorder;
mod routing;
mod session_sticky;

use clap::Parser;
use maxllm_config::Config;
use notify::{EventKind, RecursiveMode, Watcher};
use pingora::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Load configuration
    let config = Config::from_file(&args.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config from {:?}: {e}", args.config);
        std::process::exit(1);
    });

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

    // Create the HTTP proxy service
    let mut service = http_proxy_service(&server.configuration, gateway);
    service.add_tcp(&config.server.listen.to_string());

    server.add_service(service);

    tracing::info!("MaxLLM gateway is running on {}", config.server.listen);
    server.run_forever();
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

        tracing::info!(path = ?config_path, "Config hot-reload watcher started");

        // Debounce: ignore events within 1 second of the last reload
        let mut last_reload = std::time::Instant::now() - std::time::Duration::from_secs(10);

        for event in rx {
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "Config watcher error");
                    continue;
                }
            };

            // Only react to modify/create events on our config file
            let is_relevant = matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_))
                && event.paths.iter().any(|p| {
                    p.canonicalize().ok().as_ref() == Some(&config_path)
                        || p.file_name() == config_path.file_name()
                });

            if !is_relevant {
                continue;
            }

            // Debounce
            if last_reload.elapsed() < std::time::Duration::from_secs(1) {
                continue;
            }
            last_reload = std::time::Instant::now();

            tracing::info!("Config file changed, reloading...");

            // Parse and validate new config
            let new_config = match Config::from_file(&config_path) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to parse updated config, keeping current");
                    continue;
                }
            };

            // Build new hot state
            let new_state = match gateway::build_hot_state(&new_config) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to build new config state, keeping current");
                    continue;
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

            // Atomic swap
            hot.store(Arc::new(new_state));

            if changes.is_empty() {
                tracing::info!("Config reloaded successfully (no structural changes)");
            } else {
                tracing::info!(changes = ?changes, "Config reloaded successfully");
            }
        }
    });
}
