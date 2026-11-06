// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Per-provider runtime stats for latency- and concurrency-aware routing.
//!
//! Kept in process-wide statics (not in `HotState`) so the stats survive a
//! config hot-reload — the same reason the round-robin counter and the
//! session store live outside the hot state. Keyed by provider name, so a
//! reload that keeps a provider's name keeps its history.

use ahash::AHashMap;
use once_cell::sync::Lazy;
use std::sync::RwLock;

/// EWMA smoothing factor for the latency estimate: the newest sample weighs
/// 30%, the previous estimate 70%. Responsive to a genuine slowdown without
/// letting one outlier swing the whole routing decision.
const EWMA_ALPHA: f64 = 0.3;

#[derive(Debug, Clone, Copy, Default)]
struct LatencyStats {
    /// Milliseconds, exponentially weighted.
    ewma_ms: f64,
    samples: u64,
}

static LATENCY: Lazy<RwLock<AHashMap<String, LatencyStats>>> =
    Lazy::new(|| RwLock::new(AHashMap::new()));

static INFLIGHT: Lazy<RwLock<AHashMap<String, u64>>> = Lazy::new(|| RwLock::new(AHashMap::new()));

/// Record one successful upstream attempt's latency for `provider`.
///
/// Only successful attempts count: a provider that fails fast must not look
/// "low latency" — its failures are the circuit breaker's job to punish.
pub fn record_latency(provider: &str, latency_ms: f64) {
    let mut map = LATENCY.write().unwrap_or_else(|e| e.into_inner());
    let stats = map.entry(provider.to_string()).or_default();
    stats.ewma_ms = if stats.samples == 0 {
        latency_ms
    } else {
        EWMA_ALPHA * latency_ms + (1.0 - EWMA_ALPHA) * stats.ewma_ms
    };
    stats.samples += 1;
}

/// Current latency estimate in milliseconds, `None` before the first
/// successful attempt.
pub fn latency_ms(provider: &str) -> Option<f64> {
    LATENCY
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(provider)
        .filter(|s| s.samples > 0)
        .map(|s| s.ewma_ms)
}

/// A request started being routed to `provider` (initial selection or a
/// failover target). Matched by exactly one [`release`].
pub fn acquire(provider: &str) {
    let mut map = INFLIGHT.write().unwrap_or_else(|e| e.into_inner());
    *map.entry(provider.to_string()).or_insert(0) += 1;
}

/// A request finished with `provider`.
pub fn release(provider: &str) {
    let mut map = INFLIGHT.write().unwrap_or_else(|e| e.into_inner());
    if let Some(v) = map.get_mut(provider) {
        *v = v.saturating_sub(1);
    }
}

/// All providers with requests currently in flight, as (provider, count).
/// Feeds the Prometheus gauge, so the least-connections decision is
/// observable — the gauge is derived here rather than incremented
/// separately, so it can never drift from the counter the router reads.
pub fn inflight_counts() -> Vec<(String, u64)> {
    INFLIGHT
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

/// Requests currently routed to `provider`.
pub fn inflight(provider: &str) -> u64 {
    INFLIGHT
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(provider)
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_first_sample_is_the_value() {
        record_latency("lat-ewma-1", 120.0);
        assert_eq!(latency_ms("lat-ewma-1"), Some(120.0));
    }

    #[test]
    fn ewma_converges_toward_new_samples() {
        // 100, then 200: 0.3*200 + 0.7*100 = 130.
        record_latency("lat-ewma-2", 100.0);
        record_latency("lat-ewma-2", 200.0);
        assert_eq!(latency_ms("lat-ewma-2"), Some(130.0));
        // Again: 0.3*200 + 0.7*130 = 151.
        record_latency("lat-ewma-2", 200.0);
        assert!((latency_ms("lat-ewma-2").unwrap() - 151.0).abs() < 1e-9);
    }

    #[test]
    fn latency_unknown_provider_is_none() {
        assert_eq!(latency_ms("lat-never-seen"), None);
    }

    #[test]
    fn inflight_acquire_release_balances() {
        acquire("lc-1");
        acquire("lc-1");
        acquire("lc-2");
        assert_eq!(inflight("lc-1"), 2);
        assert_eq!(inflight("lc-2"), 1);
        release("lc-1");
        assert_eq!(inflight("lc-1"), 1);
        release("lc-1");
        release("lc-1"); // saturates at 0, no underflow
        assert_eq!(inflight("lc-1"), 0);
        assert_eq!(inflight("lc-unknown"), 0);
    }
}
