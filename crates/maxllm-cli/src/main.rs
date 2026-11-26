// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;

#[derive(Parser)]
#[command(name = "maxllm", version, about = "MaxLLM — AI Gateway CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the gateway server
    Start {
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
        /// Run as a background daemon
        #[arg(short, long)]
        daemon: bool,
    },
    /// Stop a running gateway
    Stop,
    /// Show gateway status
    Status {
        /// Path to the configuration file (used to determine listen port)
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Check if the gateway is healthy
    Health {
        /// Gateway URL (overrides config-derived URL)
        #[arg(short, long)]
        url: Option<String>,
        /// Path to the configuration file (used to determine listen port)
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Generate a starter configuration file
    Init {
        /// Output file path
        #[arg(short, long, default_value = "maxllm.toml")]
        output: PathBuf,
    },
    /// Test connectivity to each configured provider
    Test {
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// Manage virtual API keys
    Keys {
        #[command(subcommand)]
        command: KeysCommands,
    },
    /// Manage configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    /// Admin operations that run against the SQLite store
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
    },
    /// The translation kernel: compile-check and dry-run a kernel script
    /// without starting the gateway (the fast inner loop for kernel edits).
    Kernel {
        #[command(subcommand)]
        command: KernelCommands,
    },
    /// Manage the layer-1 supervisor (status, hot upgrade, routes)
    Supervisor {
        #[command(subcommand)]
        command: SupervisorCommands,
    },
}

#[derive(Subcommand)]
enum KernelCommands {
    /// Compile a kernel script and report which hooks it defines
    Check {
        /// Path to the kernel script
        #[arg(short, long)]
        script: PathBuf,
        /// Rhai operation budget (the gateway default is 2,000,000)
        #[arg(long)]
        max_operations: Option<u64>,
    },
    /// Run a kernel script's `on_upstream_error` hook against a status/body
    Run {
        /// Path to the kernel script
        #[arg(short, long)]
        script: PathBuf,
        /// The upstream status to classify
        #[arg(long, default_value_t = 400)]
        status: u16,
        /// The upstream error body to classify ("-" reads stdin)
        #[arg(long)]
        error_body: Option<String>,
        /// The provider name the script should see
        #[arg(long, default_value = "kimi")]
        provider: String,
        /// The model name the script should see
        #[arg(long, default_value = "model")]
        model: String,
        /// Rhai operation budget (the gateway default is 2,000,000)
        #[arg(long)]
        max_operations: Option<u64>,
    },
}

#[derive(Subcommand)]
enum SupervisorCommands {
    /// Show layer-1 state: active worker generations, versions and routes
    Status {
        /// Control API URL (overrides config-derived URL)
        #[arg(long)]
        url: Option<String>,
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// Hot-upgrade a layer-2 backend to a new binary
    Upgrade {
        /// Backend name (`default`, `voice`, …)
        #[arg(long, default_value = "default")]
        backend: String,
        /// New worker binary. Omit to re-spawn the configured binary.
        #[arg(long)]
        binary: Option<PathBuf>,
        /// Build identifier exposed through /version and status
        #[arg(long)]
        build_id: Option<String>,
        /// Extra worker environment, KEY=VALUE (repeatable)
        #[arg(long = "env", value_name = "KEY=VALUE")]
        env: Vec<String>,
        /// Control API URL (overrides config-derived URL)
        #[arg(long)]
        url: Option<String>,
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// Ask layer 1 to re-read the config (routes and backend versions)
    Reload {
        /// Control API URL (overrides config-derived URL)
        #[arg(long)]
        url: Option<String>,
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// Inspect or replace the layer-1 path-prefix route table
    Routes {
        #[command(subcommand)]
        command: RouteCommands,
    },
}

#[derive(Subcommand)]
enum RouteCommands {
    /// List the effective route table
    List {
        #[arg(long)]
        url: Option<String>,
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// Replace the hot-registered route overlay
    ///
    /// Each `--route` is `/path` (default backend) or `/path=backend`.
    Set {
        /// Repeatable: `/path` targets the default backend, `/path=backend`
        /// targets a named one. Required unless `--clear` is given.
        #[arg(
            long = "route",
            value_name = "/path[=backend]",
            required_unless_present = "clear"
        )]
        routes: Vec<String>,
        /// Drop the overlay entirely, back to the config-derived routes
        #[arg(long, conflicts_with = "routes")]
        clear: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum KeysCommands {
    /// List virtual keys
    List {
        /// Admin API URL (overrides config-derived URL)
        #[arg(long)]
        url: Option<String>,
        /// Path to the configuration file (used to determine listen port)
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Create a new virtual key
    Create {
        /// Human-readable name for the key
        #[arg(long)]
        name: String,
        /// Admin API URL (overrides config-derived URL)
        #[arg(long)]
        url: Option<String>,
        /// Path to the configuration file (used to determine listen port)
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Move a renamed provider's breaker state and session pins to its new name
    ///
    /// Rename the provider block in the config first, then run this, then
    /// restart the gateway — otherwise the moved rows are pruned at startup.
    RenameProvider {
        /// Old provider name (as it appears in the database)
        #[arg(long)]
        from: String,
        /// New provider name (must already exist in the config)
        #[arg(long)]
        to: String,
        /// SQLite database path (overrides the config's [admin] db_path)
        #[arg(long)]
        db: Option<PathBuf>,
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
        /// Report what would move, write nothing
        #[arg(long)]
        dry_run: bool,
        /// Machine-readable output
        #[arg(long)]
        json: bool,
        /// Proceed even if the config does not reflect the rename yet
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Validate a configuration file
    Check {
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
    /// List configured providers
    Providers {
        /// Path to the configuration file
        #[arg(short, long, default_value = "maxllm.toml")]
        config: PathBuf,
    },
}

/// Resolve the gateway base URL from an explicit --url flag or from config.
fn resolve_url(url: Option<&str>, config: Option<&std::path::Path>) -> String {
    if let Some(u) = url {
        return u.to_string();
    }
    let path = config.unwrap_or_else(|| std::path::Path::new("maxllm.toml"));
    if let Ok(cfg) = maxllm_config::Config::from_file(path) {
        let port = cfg.server.listen.port();
        return format!("http://127.0.0.1:{port}");
    }
    "http://127.0.0.1:8080".to_string()
}

/// Resolve the supervisor control API base URL from an explicit --url flag or
/// the `[supervisor] control_listen` of the config.
fn resolve_control_url(url: Option<&str>, config: &std::path::Path) -> String {
    if let Some(u) = url {
        return u.trim_end_matches('/').to_string();
    }
    if let Ok(cfg) = maxllm_config::Config::from_file(config) {
        if let Some(sup) = cfg.supervisor {
            return format!("http://{}", sup.control_listen);
        }
    }
    "http://127.0.0.1:9001".to_string()
}

/// Read the admin key from MAXLLM_ADMIN_KEY env var or exit with a helpful error.
fn require_admin_key() -> String {
    match std::env::var("MAXLLM_ADMIN_KEY") {
        Ok(key) if !key.is_empty() => key,
        _ => {
            eprintln!("Error: MAXLLM_ADMIN_KEY environment variable is not set.");
            eprintln!("Set it to your admin master key:");
            eprintln!("  export MAXLLM_ADMIN_KEY=\"your-master-key\"");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let exit_code = match cli.command {
        Commands::Start { config, daemon } => commands::start::run(config, daemon),
        Commands::Stop => commands::stop::run(),
        Commands::Status { config } => commands::status::run(config).await,
        Commands::Health { url, config } => {
            let resolved = resolve_url(url.as_deref(), config.as_deref());
            commands::health::run(&resolved).await
        }
        Commands::Init { output } => commands::init::run(output),
        Commands::Test { config } => commands::test::run(config).await,
        Commands::Keys { command } => match command {
            KeysCommands::List { url, config } => {
                let admin_key = require_admin_key();
                let resolved = resolve_url(url.as_deref(), config.as_deref());
                commands::keys::list(&resolved, &admin_key).await
            }
            KeysCommands::Create { name, url, config } => {
                let admin_key = require_admin_key();
                let resolved = resolve_url(url.as_deref(), config.as_deref());
                commands::keys::create(&resolved, &admin_key, &name).await
            }
        },
        Commands::Config { command } => match command {
            ConfigCommands::Check { config } => commands::config::check(config),
            ConfigCommands::Providers { config } => commands::config::providers(config),
        },
        Commands::Kernel { command } => match command {
            KernelCommands::Check {
                script,
                max_operations,
            } => commands::kernel::check(&script, max_operations),
            KernelCommands::Run {
                script,
                status,
                error_body,
                provider,
                model,
                max_operations,
            } => commands::kernel::run(
                &script,
                status,
                error_body.as_deref(),
                &provider,
                &model,
                max_operations,
            ),
        },
        Commands::Supervisor { command } => match command {
            SupervisorCommands::Status { url, config } => {
                let base = resolve_control_url(url.as_deref(), &config);
                commands::supervisor::status(&base).await
            }
            SupervisorCommands::Upgrade {
                backend,
                binary,
                build_id,
                env,
                url,
                config,
            } => {
                let base = resolve_control_url(url.as_deref(), &config);
                commands::supervisor::upgrade(&base, &backend, binary, build_id, env).await
            }
            SupervisorCommands::Reload { url, config } => {
                let base = resolve_control_url(url.as_deref(), &config);
                commands::supervisor::reload(&base).await
            }
            SupervisorCommands::Routes { command } => match command {
                RouteCommands::List { url, config } => {
                    let base = resolve_control_url(url.as_deref(), &config);
                    commands::supervisor::routes_list(&base).await
                }
                RouteCommands::Set {
                    routes,
                    clear,
                    url,
                    config,
                } => {
                    let base = resolve_control_url(url.as_deref(), &config);
                    commands::supervisor::routes_set(&base, if clear { Vec::new() } else { routes })
                        .await
                }
            },
        },
        Commands::Admin { command } => match command {
            AdminCommands::RenameProvider {
                from,
                to,
                db,
                config,
                dry_run,
                json,
                force,
            } => commands::admin::rename_provider(
                db.as_deref(),
                Some(config.as_path()),
                &from,
                &to,
                dry_run,
                json,
                force,
            ),
        },
    };

    std::process::exit(exit_code);
}
