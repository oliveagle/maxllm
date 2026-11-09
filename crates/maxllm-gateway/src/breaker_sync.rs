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
//! the peers adopt the deadline on their next poll.
//!
//! The *failure* breaker travels too, but with a different shape: it has no
//! deadline to copy, only the fact that the provider kept failing on the
//! peer. A node that never sent traffic to that provider cannot know it is
//! dead, so it keeps picking it and keeps paying a failed attempt per
//! request. Adopting the peer's open turns that into one learning event as
//! well — the local circuit is opened for the local fail window and then
//! half-opens and probes, exactly like a provider that failed here.
//!
//! Two guards keep that from costing usable capacity: a locally parked
//! provider keeps its own (longer) deadline, and a provider that answered
//! successfully *here* inside the fail window stays — the local success is
//! fresher evidence than the peer's failure count.
//!
//! ```text
//!   node A: 429 (quota) ──► force-open until T ──► GET /admin/breakers ──┐
//!   node B: ◄── adopt force-open until T ◄────────────────────────────────┘
//!
//!   node A: operator release ──► POST /admin/breakers/close {until: T} ──► node B
//!   node B: ◄── drops the park, but only if its own deadline is exactly T
//! ```
//!
//! The release travels with the deadline it releases, so a peer that knows a
//! *later* wall keeps it: the releaser may not know everything.

use crate::gateway::HotState;
use crate::metrics::METRICS;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// How often a failing peer is reported. The first failure is logged
/// immediately; after that only every n-th one is, so an unreachable peer
/// cannot drown the log of a busy node.
const PEER_WARN_EVERY: u64 = 10;

/// How long a peer that does not know the breaker API is left alone.
///
/// A peer that answers 404 runs a version without `/admin/breakers` — it
/// cannot share anything with us. Polling it every interval is a request
/// every fifteen seconds that can never succeed, and an error line every
/// tenth one. The peer is skipped for this long and then tried again, so a
/// node that is upgraded later starts sharing without a restart.
const PEER_TOO_OLD_BACKOFF: Duration = Duration::from_secs(600);

/// What one peer reports about one provider.
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
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
    /// Consecutive failures the peer counted against the provider. The
    /// report carries it so a peer can tell a failure-driven open apart from
    /// an operator park or a stream hold (both of which carry a deadline).
    #[serde(default)]
    pub failures: u64,
}

/// The peer's breaker report.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PeerBreakerReport {
    #[serde(default)]
    pub breakers: Vec<PeerBreaker>,
    /// The peer's instance identity, when it reports one.
    ///
    /// The peer list is the same on every node (that is what makes a config
    /// file copyable from node to node), so a node inevitably lists itself.
    /// A peer that reports this instance's id *is* this gateway, and the
    /// sync skips it: adopting its own failure open would re-open a
    /// provider that is half-open and probing, and the provider would never
    /// be given another chance. A peer too old to report an id is simply not
    /// recognised as self.
    #[serde(default)]
    pub instance_id: Option<String>,
}

/// One release the operator made on this node, on its way to the peers.
#[derive(Debug, Clone)]
struct Release {
    provider: String,
    until_ms: u64,
}

/// The gateway's handle on the sync thread.
///
/// The operator's release is a *cluster* decision: a provider that is
/// parked on one node because the account was topped up must not stay
/// parked on the others. The gateway hands the release to the sync thread,
/// which pushes it to the peers.
#[derive(Clone)]
pub struct BreakerSyncHandle {
    releases: tokio::sync::mpsc::UnboundedSender<Release>,
}

impl BreakerSyncHandle {
    /// Tell the peers that the park they adopted from this node is over.
    pub fn release(&self, provider: &str, until_ms: u64) {
        let _ = self.releases.send(Release {
            provider: provider.to_string(),
            until_ms,
        });
    }
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
    share_failures: bool,
    own_instance_id: String,
) -> BreakerSyncHandle {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Release>();
    let keepalive = tx.clone();

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
                // Keep a sender alive so `rx.recv()` never resolves to None
                // and the select arm stays pending instead of spinning.
                let _keepalive = keepalive.clone();
                let mut ticker =
                    tokio::time::interval(Duration::from_secs(interval_secs.clamp(1, 3600)));
                let mut failures: HashMap<String, u64> = HashMap::new();
                // Peers that answered 404: they run a version without the
                // breaker API and cannot share anything. They are left alone
                // for a while instead of being polled every interval.
                let mut too_old: HashMap<String, Instant> = HashMap::new();
                // This gateway's own identity, so a peer list that names
                // this node (the normal state once the same config file is
                // used on every node) does not make the node talk to itself.
                let own_instance_id = own_instance_id.clone();
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            for peer in &peers {
                                if let Some(until) = too_old.get(peer) {
                                    if Instant::now() < *until {
                                        continue;
                                    }
                                    too_old.remove(peer);
                                }
                                match fetch(&client, peer, auth_token.as_deref()).await {
                                    Ok(report) => {
                                        if failures.remove(peer).is_some() {
                                            info!(peer = %peer, "breaker sync poll recovered");
                                        }
                                        // The peer is this node: the peer list
                                        // is the same on every node, and a node
                                        // that peers with itself adopts its own
                                        // state and shares nothing.
                                        if peer_is_self(report.instance_id.as_deref(), &own_instance_id) {
                                            info!(peer = %peer, instance_id = %own_instance_id, "the peer is this gateway — skipped");
                                            continue;
                                        }
                                        let mut adopted = 0usize;
                                        for breaker in &report.breakers {
                                            // A quota wall and a failure-driven
                                            // open are mutually exclusive (the
                                            // wall carries a deadline), so the
                                            // first one that applies wins.
                                            if adopt(&hot, &admin_api, breaker)
                                                || (share_failures
                                                    && adopt_failure(&hot, &admin_api, breaker))
                                            {
                                                adopted += 1;
                                            }
                                        }
                                        if adopted > 0 {
                                            info!(peer = %peer, adopted, "adopted the peer's breaker state");
                                        }
                                    }
                                    Err(e) => {
                                        // A peer without the breaker API can
                                        // never share anything: leave it alone
                                        // for a while instead of asking again
                                        // every interval.
                                        if e.starts_with("HTTP 404") {
                                            too_old.insert(
                                                peer.clone(),
                                                Instant::now() + PEER_TOO_OLD_BACKOFF,
                                            );
                                            warn!(peer = %peer, "breaker sync peer does not expose the breaker API — skipped for {}s", PEER_TOO_OLD_BACKOFF.as_secs());
                                            continue;
                                        }
                                        let count = failures.entry(peer.clone()).or_insert(0);
                                        *count += 1;
                                        if *count == 1 || (*count).is_multiple_of(PEER_WARN_EVERY) {
                                            warn!(peer = %peer, error = %e, failures = *count, "breaker sync poll failed");
                                        }
                                    }
                                }
                            }
                        }
                        Some(release) = rx.recv() => {
                            for peer in &peers {
                                if let Err(e) = relay_release(&client, peer, auth_token.as_deref(), &release).await {
                                    warn!(peer = %peer, provider = %release.provider, error = %e, "failed to relay a release to the peer");
                                } else {
                                    info!(peer = %peer, provider = %release.provider, until = release.until_ms, "relayed a release to the peer");
                                }
                            }
                        }
                    }
                }
            });
        })
        .expect("spawn the breaker sync thread");

    BreakerSyncHandle { releases: tx }
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

/// Tell a peer that a park is over.
///
/// The deadline travels with the release: the peer drops the park only if it
/// holds exactly that deadline, so a peer that knows a later wall keeps it.
async fn relay_release(
    client: &reqwest::Client,
    peer: &str,
    token: Option<&str>,
    release: &Release,
) -> Result<(), String> {
    let url = format!("{}/admin/breakers/close", peer.trim_end_matches('/'));
    let mut req = client.post(&url).json(&serde_json::json!({
        "provider": release.provider,
        "until_ms": release.until_ms,
        "relay": true,
    }));
    if let Some(token) = token {
        if !token.is_empty() {
            req = req.bearer_auth(token);
        }
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

/// Whether a peer is this gateway.
///
/// The peer list is the same on every node — that is what makes a config
/// file copyable from node to node — so a node inevitably lists itself. A
/// peer that reports this instance's id *is* this gateway: syncing with it
/// adopts this node's own state and shares nothing, and it looks like the
/// sharing works while the real peer is never contacted.
///
/// The identity is per *instance*, so two gateways on one host (a canary
/// next to the live one, a test pair) are two nodes and still share.
fn peer_is_self(report_instance_id: Option<&str>, own_instance_id: &str) -> bool {
    matches!(report_instance_id, Some(id) if id == own_instance_id)
}

/// Adopt one peer's failure-driven open locally.
///
/// Only the *failure* breaker travels this way. A peer that put a provider
/// out of rotation because it kept failing tells the others, so a node that
/// never sent traffic to that provider stops picking a dead one. The
/// deadline is the local fail window, not the peer's: the local circuit
/// half-opens and probes after it, so the provider earns its place back on
/// local evidence.
///
/// A quota wall keeps its own path (it travels with its deadline, and the
/// deadline is the thing worth copying); a peer that is merely half-open is
/// probing already, so there is nothing to adopt.
fn adopt_failure(
    hot: &ArcSwap<HotState>,
    admin_api: &Option<Arc<maxllm_admin::AdminApi>>,
    breaker: &PeerBreaker,
) -> bool {
    if !peer_report_is_failure_open(breaker) {
        return false;
    }
    let current = hot.load();
    let Some(provider) = current.providers.get(&breaker.provider) else {
        return false;
    };
    if !provider.circuit_breaker.adopt_failure_open() {
        return false;
    }
    METRICS
        .circuit_breaker_peer_failure_opens_total
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
                "failed to persist an adopted failure-driven open"
            );
        }
    }
    info!(
        provider = %breaker.provider,
        failures = breaker.failures,
        "adopted a peer's failure-driven open"
    );
    true
}

/// Does the peer's report describe a failure-driven open?
///
/// A quota wall, an operator park and a stream hold all carry a deadline;
/// they travel the quota path, which copies the deadline itself. A peer that
/// is merely half-open is probing already — there is nothing to adopt, and
/// an open without a failure count is not a failure breaker either.
fn peer_report_is_failure_open(breaker: &PeerBreaker) -> bool {
    breaker.state == crate::circuit_breaker::STATE_OPEN
        && breaker.force_open_until == 0
        && breaker.failures > 0
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

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(state: u8, force_open_until: u64, failures: u64) -> PeerBreaker {
        PeerBreaker {
            provider: "dead".to_string(),
            state,
            force_open_until,
            quota_opens: 0,
            failures,
        }
    }

    /// A peer that put a provider out of rotation because it kept failing
    /// tells the others.
    #[test]
    fn test_a_failure_driven_open_is_recognised() {
        assert!(peer_report_is_failure_open(&peer(
            crate::circuit_breaker::STATE_OPEN,
            0,
            3
        )));
    }

    /// A quota wall carries a deadline and travels its own path.
    #[test]
    fn test_a_quota_wall_is_not_a_failure_open() {
        assert!(!peer_report_is_failure_open(&peer(
            crate::circuit_breaker::STATE_OPEN,
            1_700_000_000_000,
            3
        )));
    }

    /// A peer that reports this instance's own id is this gateway.
    #[test]
    fn test_a_peer_that_reports_our_own_instance_id_is_this_gateway() {
        assert!(peer_is_self(Some("oleNas01-42-0"), "oleNas01-42-0"));
        assert!(!peer_is_self(Some("oleFedora01-43-0"), "oleNas01-42-0"));
    }

    /// Two gateways on the same host are two nodes: their instance ids
    /// differ, so neither mistakes the other for itself.
    #[test]
    fn test_two_gateways_on_one_host_are_not_each_other() {
        let a = "oleNas01-42-0";
        let b = "oleNas01-42-1";
        assert!(!peer_is_self(Some(b), a));
        assert!(!peer_is_self(Some(a), b));
    }

    /// A peer too old to report an id is not recognised as self — it is
    /// simply not this gateway, and its state (if it ever answers) is
    /// adopted.
    #[test]
    fn test_a_peer_without_an_instance_id_is_not_this_gateway() {
        assert!(!peer_is_self(None, "oleNas01-42-0"));
    }

    /// A closed or half-open peer is not failing: a half-open peer is
    /// probing already, and there is nothing to adopt.
    #[test]
    fn test_a_healthy_or_probing_peer_is_not_adopted() {
        assert!(!peer_report_is_failure_open(&peer(
            crate::circuit_breaker::STATE_CLOSED,
            0,
            0
        )));
        assert!(!peer_report_is_failure_open(&peer(
            crate::circuit_breaker::STATE_HALF_OPEN,
            0,
            3
        )));
    }
}
