// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! `maxllm-commandcode` — an OpenAI bridge in front of the Command Code
//! CLI (`cmd`).
//!
//! commandcode.ai's OpenAI-compatible Provider API (`/provider/v1/*`) is a
//! paid add-on: on the Go plan it answers HTTP 403 `upgrade_required`. The
//! plan the CLI speaks (`/alpha/*`) is covered by the Go plan, but the
//! vendor states that it serves the CLI only.
//!
//! This bridge therefore runs the vendor's own client instead of speaking
//! its protocol: `cmd -p <prompt> --output-format json` is the CLI's
//! documented non-interactive mode, and the bridge translates its NDJSON
//! event stream into an OpenAI contract.
//!
//! The bridge speaks the **Responses** contract natively (`POST /v1/responses`,
//! the `response.*` SSE event family) and the Chat Completions contract as
//! well (`POST /v1/chat/completions`). A Responses client therefore talks to
//! the bridge without any translation on the way in.
//!
//! ```sh
//! maxllm-commandcode --listen 127.0.0.1:3801 \
//!     --cmd-bin /usr/local/bin/cmd \
//!     --model deepseek/deepseek-v4.1-flash
//! ```

mod cli;
mod protocol;
mod responses;
mod server;

use clap::Parser;
use cli::CliConfig;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command(
    name = "maxllm-commandcode",
    about = "OpenAI Responses / Chat Completions bridge in front of the Command Code CLI"
)]
struct Args {
    /// Address the bridge listens on.
    #[arg(long, default_value = "127.0.0.1:3801")]
    listen: String,
    /// Path to the `cmd` binary.
    #[arg(long, default_value = "cmd")]
    cmd_bin: String,
    /// Model the bridge serves when the client does not name a served one.
    ///
    /// This id is what the client asks for and what the answer reports. It
    /// is deliberately not the CLI's own model id: the CLI plan is a flat
    /// subscription, so the served id must not match a per-token price entry
    /// in the gateway's cost table.
    #[arg(long, default_value = "commandcode-cli/deepseek-v4.1-flash")]
    model: String,
    /// The model the CLI runs (`cmd --model`).
    #[arg(long, default_value = "deepseek/deepseek-v4.1-flash")]
    cli_model: String,
    /// Additional models to serve. Repeatable.
    #[arg(long = "allow-model")]
    allow_model: Vec<String>,
    /// Directory the CLI runs in. The agent must not see a real workspace.
    #[arg(long)]
    workspace: Option<PathBuf>,
    /// Cap on the CLI's internal conversation turns.
    #[arg(long, default_value_t = 8)]
    max_turns: u32,
    /// Wall-clock budget for one CLI run, in seconds.
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    /// Reasoning effort handed to the CLI (low / medium / high).
    #[arg(long)]
    effort: Option<String>,
    /// HOME for the CLI process. The CLI reads its credentials from
    /// `$HOME/.commandcode/auth.json`.
    #[arg(long)]
    home: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let workspace = match &args.workspace {
        Some(path) => path.clone(),
        None => std::env::temp_dir().join("maxllm-commandcode-workspace"),
    };
    std::fs::create_dir_all(&workspace)?;

    let mut allowed_models = vec![args.model.clone()];
    // The CLI's own model id is served as well: a client that hardcodes it
    // gets the same answer (it is billed as if it were metered, though --
    // the served id is the one that keeps a subscription out of the cost
    // table).
    if !allowed_models.contains(&args.cli_model) {
        allowed_models.push(args.cli_model.clone());
    }
    for model in &args.allow_model {
        if !allowed_models.contains(model) {
            allowed_models.push(model.clone());
        }
    }

    let cli = CliConfig {
        bin: args.cmd_bin.clone(),
        default_model: args.model.clone(),
        cli_model: args.cli_model.clone(),
        allowed_models,
        workspace,
        max_turns: args.max_turns,
        effort: args.effort.clone(),
    };

    let state = Arc::new(server::State {
        cli,
        timeout: Duration::from_secs(args.timeout_secs),
    });

    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(
        listen = %args.listen,
        cmd = %args.cmd_bin,
        model = %args.model,
        cli_model = %args.cli_model,
        "maxllm-commandcode is listening"
    );

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!("cannot accept a connection: {err}");
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |request| {
                let state = state.clone();
                async move { Ok::<_, std::convert::Infallible>(server::handle(state, request).await) }
            });
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!("connection from {peer} ended: {err}");
            }
        });
    }
}
