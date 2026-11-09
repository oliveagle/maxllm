// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Cross-node breaker sharing.
//!
//! The circuit breaker lives in the node's own memory and its own SQLite
//! file, so a provider that ran out of quota on one node is still probed by
//! the next one: every node pays one failed request — and, before the
//! gateway owned its errors, one error leaked to its client — before it
//! learns the same wall.
//!
//! Sharing the quota force-open turns that into a single learning event:
//! the node that saw the 429 parks the provider until the reset instant, and
//! the peers adopt the deadline on their next poll. The generic
//! failure-count breaker is *not* shared: it is short-lived and node-local by
//! nature, and copying it would move traffic around for no reason.
//!
//! ```text
//!   node A: 429 (quota) ──► force-open until T ──► GET /admin/breakers ──┐
//!   node B: ◄── adopt force-open until T ◄────────────────────────────────┘
//! ```

use crate::gateway::HotState;
use crate::metrics::METRICS;
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// What one peer reports about one provider.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PeerBreaker {
    pub provider: String,
    /// 0 = closed, 1 = open, 2 = half-open.
    #[serde(default)]
    pub state: u8,
    /// The epoch-ms instant the provider's quota force-open ends (0 = none).
    #[serde(default)]
    pub force_open_until: u64,
    /// How often the provider ran into a quota wall.
    #[serde(default)]
    pub quota_opens: u64,
}

/// The peer's breaker report.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PeerBreakerReport {
    #[serde(default)]
    pub breakers: Vec<PeerBreaker>,
}

/// Poll the peers and adopt their quota force-opens.
///
/// The poll is fail-open: an unreachable peer (or a peer that does not run
/// this version) is logged and skipped — it never delays a request and never
/// changes the local routing.
pub fn spawn(
    hot: Arc<ArcSwap<HotState>>,
    admin_api: Option<Arc<maxllm_admin::AdminApi>>,
    peers: Vec<String>,
    interval_secs: u64,
    auth_token: Option<String>,
) -> std::thread::JoinHandle<()> {
    // A dedicated thread with its own runtime: the gateway is built before
    // pingora starts its runtime, so there is no reactor to spawn onto here.
    std::thread::Builder::new()
        .name("maxllm-breaker-sync".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    warn!(error = %e, "cross-node breaker sync disabled — no runtime");
                    return;
                }
            };
            runtime.block_on(async move {
                let client = match reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(error = %e, "cross-node breaker sync disabled — no HTTP client");
                        return;
                    }
                };
                let mut ticker =
                    tokio::time::interval(Duration::from_secs(interval_secs.clamp(1, 3600)));
                loop {
                    ticker.tick().await;
                    for peer in &peers {
                        match fetch(&client, peer, auth_token.as_deref()).await {
                            Ok(report) => {
                                let mut adopted = 0usize;
                                for breaker in &report.breakers {
                                    if adopt(&hot, &admin_api, breaker) {
                                        adopted += 1;
                                    }
                                }
                                if adopted > 0 {
                                    info!(peer = %peer, adopted, "adopted the peer's quota force-opens");
                                }
                            }
                            Err(e) => {
                                warn!(peer = %peer, error = %e, "breaker sync poll failed");
                            }
                        }
                    }
                }
            });
        })
        .expect("spawn the breaker sync thread")
}

/// Fetch one peer's breaker report.
async fn fetch(
    client: &reqwest::Client,
    peer: &str,
    token: Option<&str>,
) -> Result<PeerBreakerReport, String> {
    let url = format!("{}/admin/breakers", peer.trim_end_matches('/'));
    let mut req = client.get(&url);
    if let Some(token) = token {
        if !token.is_empty() {
            req = req.bearer_auth(token);
        }
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<PeerBreakerReport>()
        .await
        .map_err(|e| e.to_string())
}

/// Adopt one peer's quota force-open locally.
///
/// Only ever *extends* the local deadline: the local observation is at
/// least as fresh as the peer's, so a peer must never shorten a wall this
/// node already knows about.
fn adopt(
    hot: &ArcSwap<HotState>,
    admin_api: &Option<Arc<maxllm_admin::AdminApi>>,
    breaker: &PeerBreaker,
) -> bool {
    if breaker.force_open_until == 0 {
        return false;
    }
    let current = hot.load();
    let Some(provider) = current.providers.get(&breaker.provider) else {
        return false;
    };
    if !provider
        .circuit_breaker
        .extend_quota_until(breaker.force_open_until)
    {
        return false;
    }
    METRICS
        .circuit_breaker_peer_quota_opens_total
        .with_label_values(&[&breaker.provider])
        .inc();
    METRICS
        .circuit_breaker_state
        .with_label_values(&[&breaker.provider])
        .set(provider.circuit_breaker.state() as i64);
    if let Some(api) = admin_api {
        if let Err(err) =
            api.save_circuit_breaker_state(provider.circuit_breaker.snapshot(&breaker.provider))
        {
            warn!(
                error = %err,
                provider = %breaker.provider,
                "failed to persist an adopted quota force-open"
            );
        }
    }
    info!(
        provider = %breaker.provider,
        until = breaker.force_open_until,
        "adopted a peer's quota force-open"
    );
    true
}
