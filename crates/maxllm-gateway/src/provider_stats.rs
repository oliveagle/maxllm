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
use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// EWMA smoothing factor for the time-to-first-byte estimate: the newest
/// sample weighs 30%, the previous estimate 70%. Responsive to a genuine
/// slowdown without letting one outlier swing the whole routing decision.
const EWMA_ALPHA: f64 = 0.3;

/// How long one whole-exchange sample stays evidence for the auto pool.
///
/// The auto pool does not merely *rank* on this number — it drops a provider
/// from the rotation and lets a warm session escape its pin (see
/// `auto_pool_fast_cohort` / `auto_pool_pin_escapable` in `gateway`). A
/// verdict that never expires is therefore a permanent ban: a provider judged
/// slow stops receiving traffic, nothing ever updates the estimate, and it
/// stays "slow" forever while its upstream prompt cache goes cold beside it.
/// Idle is not evidence of slowness, so the evidence ages out and the provider
/// reports as *unmeasured* again — which the rotation admits, and which is
/// exactly what produces the next measurement.
const TOTAL_LATENCY_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Hard cap on retained samples per provider, independent of the window, so a
/// provider inside the window cannot grow the map without bound. The oldest
/// samples go first; reaching this within the window needs a rate no route
/// here sustains, so in practice the window is what prunes.
const TOTAL_LATENCY_MAX_SAMPLES: usize = 256;
/// How many windowed samples must accumulate with no warm one before the
/// cold latencies are used as the estimate.
///
/// A request whose input the upstream mostly had to re-read measures prefix
/// prefill, not the provider — one such turn must not banish the provider
/// that is about to serve the warm ones. But a provider that *never* reports
/// a cache hit would otherwise stay unmeasured forever, and an unmeasured
/// entry is never escaped by a pin, so it would hold sessions no matter how
/// slow it is. After this many samples with no warm evidence at all, the cold
/// latencies are the only evidence there is and the estimate takes them.
const TOTAL_LATENCY_COLD_JUDGE_AFTER: usize = 5;

#[derive(Debug, Clone, Default)]
struct TotalLatencyStats {
    /// `(recorded_at, whole-exchange ms, warm)`, oldest first. `warm` is the
    /// fraction of the input the upstream served from its prompt cache, over
    /// the threshold the caller applied — see [`record_total_latency`].
    samples: VecDeque<(Instant, f64, bool)>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LatencyStats {
    /// Milliseconds, exponentially weighted.
    ewma_ms: f64,
    samples: u64,
}

static LATENCY: Lazy<RwLock<AHashMap<String, LatencyStats>>> =
    Lazy::new(|| RwLock::new(AHashMap::new()));

/// Recent whole-exchange samples (request start to the client's last byte),
/// as opposed to [`LATENCY`], which is time-to-first-byte. The two are
/// different routing signals: TTFB answers "how fast does a provider start
/// speaking", the full duration answers "how long is a client actually
/// waiting" — a provider can have a snappy first byte and a slow generation,
/// so the auto pool's latency filter uses this one.
///
/// Kept as a window of raw samples rather than an EWMA because this signal
/// *excludes* providers instead of merely ranking them. An EWMA here would
/// compound both failure modes the window exists to avoid: it never forgets
/// (a provider that stops being measured keeps its last verdict forever) and
/// it overweights one bad turn (a single 90 s exchange moved one provider's
/// estimate from ~5 s to ~32 s, which with an unexpiring verdict banished it
/// from the rotation permanently).
static TOTAL_LATENCY: Lazy<RwLock<AHashMap<String, TotalLatencyStats>>> =
    Lazy::new(|| RwLock::new(AHashMap::new()));

static INFLIGHT: Lazy<RwLock<AHashMap<String, u64>>> = Lazy::new(|| RwLock::new(AHashMap::new()));

/// How long one cache observation stays evidence for the auto pool.
///
/// The same window the latency evidence uses: a verdict that outlives its
/// evidence is a permanent verdict, and the pool must be able to re-measure
/// a provider that starts caching again (a vendor that turned prefix caching
/// on, a key that moved to a warm tier).
const CACHE_RATIO_WINDOW: Duration = Duration::from_secs(10 * 60);

/// The smallest input whose cache ratio is worth believing.
///
/// A 20-token request either hits the cache or does not, and that says
/// nothing about how the provider treats the long conversations the pool
/// actually serves. Only a request big enough to have a prefix worth
/// caching is evidence.
const CACHE_RATIO_MIN_INPUT_TOKENS: u64 = 1024;

/// Smoothing for the cache ratio. The newest observation weighs 30%.
const CACHE_RATIO_ALPHA: f64 = 0.3;

#[derive(Debug, Clone, Copy)]
struct CacheRatioStats {
    /// The share of the input the upstream served from its prompt cache,
    /// exponentially weighted.
    ewma: f64,
    samples: u64,
    last_seen: Instant,
}

impl Default for CacheRatioStats {
    fn default() -> Self {
        CacheRatioStats {
            ewma: 0.0,
            samples: 0,
            last_seen: Instant::now(),
        }
    }
}

static CACHE_RATIO: Lazy<RwLock<AHashMap<String, CacheRatioStats>>> =
    Lazy::new(|| RwLock::new(AHashMap::new()));

/// Record how much of one exchange's input the upstream served from its
/// prompt cache.
///
/// This is the signal that separates a provider that *keeps* a conversation
/// warm from one that re-reads the whole context every turn. The two look
/// the same on latency once the conversation is short, and very different on
/// a long one: a provider with no prefix cache bills and waits for the full
/// prefill every single turn.
pub fn record_cache_ratio(provider: &str, cached_tokens: u64, tokens_in: u64) {
    if tokens_in < CACHE_RATIO_MIN_INPUT_TOKENS {
        return;
    }
    let ratio = (cached_tokens as f64 / tokens_in as f64).clamp(0.0, 1.0);
    let mut map = CACHE_RATIO.write().unwrap_or_else(|e| e.into_inner());
    let stats = map.entry(provider.to_string()).or_default();
    stats.ewma = if stats.samples == 0 {
        ratio
    } else {
        CACHE_RATIO_ALPHA * ratio + (1.0 - CACHE_RATIO_ALPHA) * stats.ewma
    };
    stats.samples += 1;
    stats.last_seen = Instant::now();
}

/// The cache evidence for one provider.
#[derive(Debug, Clone, Copy)]
pub struct CacheEvidence {
    /// The share of the input the upstream served from its prompt cache.
    pub ratio: f64,
    /// How many exchanges the ratio is built on.
    ///
    /// A ratio built on one observation is a guess: the pool reads it as
    /// evidence for the *rotation* (a member is not written off after a
    /// single cold turn), but never as a verdict on a session that is
    /// already pinned to it.
    pub samples: u64,
}

/// The cache evidence for `provider`, or `None` when there is none fresh.
///
/// `None` means *unmeasured*, not *cold*: a provider that has not been
/// measured is not judged, so a new member of the pool is admitted and
/// measured instead of being written off before it has served a turn.
pub fn cache_evidence(provider: &str) -> Option<CacheEvidence> {
    let map = CACHE_RATIO.read().unwrap_or_else(|e| e.into_inner());
    let stats = map.get(provider)?;
    if stats.samples == 0 {
        return None;
    }
    if stats.last_seen.elapsed() > CACHE_RATIO_WINDOW {
        return None;
    }
    Some(CacheEvidence {
        ratio: stats.ewma,
        samples: stats.samples,
    })
}

/// The share of the input `provider` served from its prompt cache (0.0-1.0),
/// or `None` when there is no fresh evidence.
pub fn cache_ratio(provider: &str) -> Option<f64> {
    cache_evidence(provider).map(|ev| ev.ratio)
}

/// Test-only: age `provider`'s cache evidence past [`CACHE_RATIO_WINDOW`].
#[cfg(test)]
pub(crate) fn expire_cache_ratio_evidence(provider: &str) {
    let mut map = CACHE_RATIO.write().unwrap_or_else(|e| e.into_inner());
    let Some(stats) = map.get_mut(provider) else {
        return;
    };
    stats.last_seen = Instant::now() - CACHE_RATIO_WINDOW - Duration::from_secs(1);
}

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

/// Record one completed exchange's total duration for `provider` (ms), for
/// the auto pool's latency filter. Only successful exchanges count.
///
/// `warm` says the upstream served at least half of this request's input from
/// its prompt cache. A cold request is dominated by a prefix prefill that the
/// next turn will not repeat, so it is kept as evidence but ranked behind
/// warm evidence — see [`total_latency_ms`].
pub fn record_total_latency(provider: &str, latency_ms: f64, warm: bool) {
    let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
    let stats = map.entry(provider.to_string()).or_default();
    let now = Instant::now();
    stats.samples.push_back((now, latency_ms, warm));
    while stats
        .samples
        .front()
        .is_some_and(|(at, _, _)| now.duration_since(*at) > TOTAL_LATENCY_WINDOW)
    {
        stats.samples.pop_front();
    }
    while stats.samples.len() > TOTAL_LATENCY_MAX_SAMPLES {
        stats.samples.pop_front();
    }
}

/// The median whole-exchange latency (ms) among the samples `provider`
/// recorded inside [`TOTAL_LATENCY_WINDOW`], or `None` when it has none —
/// which is how an idle provider reports as *unmeasured* rather than as
/// whatever it used to be, and therefore how it earns its way back into the
/// auto pool's rotation.
///
/// Three rules, in order:
///
/// 1. Warm samples win. A turn the upstream served from cache is the
///    steady-state wait; a cold turn is mostly a prefill that will not
///    repeat, and judging a provider on it is what turns the first request of
///    every conversation into a ban.
/// 2. Median rather than a mean or an EWMA, because a whole-exchange
///    duration is dominated by how much the client asked the model to
///    generate: the tail is the client's workload, not the provider's speed.
/// 3. With no warm evidence after [`TOTAL_LATENCY_COLD_JUDGE_AFTER`]
///    samples, take the cold ones — otherwise a provider that never reports a
///    cache hit is unmeasured forever and can never be escaped by a pin.
pub fn total_latency_ms(provider: &str) -> Option<f64> {
    let map = TOTAL_LATENCY.read().unwrap_or_else(|e| e.into_inner());
    let stats = map.get(provider)?;
    let now = Instant::now();
    let fresh: Vec<(f64, bool)> = stats
        .samples
        .iter()
        .filter(|(at, _, _)| now.duration_since(*at) <= TOTAL_LATENCY_WINDOW)
        .map(|(_, ms, warm)| (*ms, *warm))
        .collect();
    if fresh.is_empty() {
        return None;
    }
    let mut readings: Vec<f64> = fresh
        .iter()
        .filter(|(_, warm)| *warm)
        .map(|(ms, _)| *ms)
        .collect();
    if readings.is_empty() && fresh.len() < TOTAL_LATENCY_COLD_JUDGE_AFTER {
        return None;
    }
    if readings.is_empty() {
        readings.extend(fresh.iter().map(|(ms, _)| *ms));
    }
    readings.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(readings[readings.len() / 2])
}

/// Test-only: age `provider`'s samples past [`TOTAL_LATENCY_WINDOW`], the way
/// ten minutes of silence would.
///
/// The window's whole promise is that a verdict outlives its evidence by
/// nothing, and the failure it replaced was exactly a verdict that outlived
/// it forever — but proving that from the auto pool needs time to pass, so
/// the test moves this one provider's timestamps instead of sleeping. One
/// provider at a time: the store is process-global and the gateway's cohort
/// tests run beside this one, each on its own names.
#[cfg(test)]
pub(crate) fn expire_total_latency_evidence(provider: &str) {
    let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
    let Some(stats) = map.get_mut(provider) else {
        return;
    };
    let aged = Instant::now() - TOTAL_LATENCY_WINDOW - Duration::from_secs(1);
    for (at, _, _) in stats.samples.iter_mut() {
        *at = aged;
    }
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
    fn total_latency_is_the_median_of_what_was_recorded() {
        // A fast first byte (100 ms) with a slow generation (200 ms): the
        // whole-exchange signal is the exchange, not the first byte.
        record_total_latency("lat-total-1", 100.0, true);
        assert_eq!(total_latency_ms("lat-total-1"), Some(100.0));
        record_total_latency("lat-total-1", 200.0, true);
        // Two samples: the upper middle one. Never below either endpoint.
        assert_eq!(total_latency_ms("lat-total-1"), Some(200.0));
        record_total_latency("lat-total-1", 150.0, true);
        assert_eq!(total_latency_ms("lat-total-1"), Some(150.0));
    }

    #[test]
    fn total_latency_is_none_before_the_first_sample() {
        assert_eq!(total_latency_ms("lat-total-never"), None);
    }

    /// One pathological exchange must not decide a provider's fate. The
    /// auto pool *excludes* on this number, so an outlier-sensitive
    /// estimator turns one long generation into a ban that, with no expiry,
    /// the provider can never escape.
    #[test]
    fn one_outlier_does_not_move_the_total_latency_estimate() {
        for ms in [4_000.0, 4_200.0, 3_800.0, 4_100.0] {
            record_total_latency("lat-outlier-1", ms, true);
        }
        assert_eq!(total_latency_ms("lat-outlier-1"), Some(4_100.0));
        record_total_latency("lat-outlier-1", 90_000.0, true);
        let after = total_latency_ms("lat-outlier-1").unwrap();
        assert!(
            after < 10_000.0,
            "a single 90 s turn must not push the estimate out of the fast band, got {after}"
        );
        // Sorted [3800, 4000, 4100, 4200, 90000]: the middle sample, the 90 s
        // turn nowhere near the reading.
        assert_eq!(after, 4_100.0);
    }

    /// Evidence ages out. An idle provider must go back to *unmeasured* —
    /// the state the rotation admits — instead of carrying its last verdict
    /// forever while it receives no traffic that could ever revise it.
    #[test]
    fn stale_total_latency_samples_stop_being_evidence() {
        record_total_latency("lat-stale-1", 5_000.0, true);
        assert_eq!(total_latency_ms("lat-stale-1"), Some(5_000.0));
        {
            let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
            let stats = map.get_mut("lat-stale-1").expect("recorded above");
            for (at, _, _) in stats.samples.iter_mut() {
                *at = Instant::now() - TOTAL_LATENCY_WINDOW - Duration::from_secs(1);
            }
        }
        assert_eq!(
            total_latency_ms("lat-stale-1"),
            None,
            "an expired estimate must read as unmeasured, not as a stale verdict"
        );
        // ... and the next exchange re-measures it from scratch: the old
        // history must not blend into the new reading.
        record_total_latency("lat-stale-1", 3_000.0, true);
        assert_eq!(total_latency_ms("lat-stale-1"), Some(3_000.0));
    }

    /// The first turn of a conversation is a full-prefix prefill. Judging a
    /// provider on it is what banished one after two cold-start requests and
    /// then kept it out of the rotation forever: the ban stopped the traffic
    /// that could ever have revised the verdict.
    #[test]
    fn cold_samples_never_outweigh_warm_evidence() {
        // A cold 40 s prefill arrives after a warm 4 s turn.
        record_total_latency("lat-cold-1", 4_000.0, true);
        record_total_latency("lat-cold-1", 40_000.0, false);
        assert_eq!(
            total_latency_ms("lat-cold-1"),
            Some(4_000.0),
            "warm evidence must stand, one cold prefill notwithstanding"
        );
        // More cold samples still do not move it while warm evidence exists.
        for _ in 0..8 {
            record_total_latency("lat-cold-1", 40_000.0, false);
        }
        assert_eq!(total_latency_ms("lat-cold-1"), Some(4_000.0));
    }

    /// A provider that never reports a cache hit must not become permanently
    /// unmeasured: an unmeasured entry is never escaped by a pin, so it would
    /// hold sessions no matter how slow it is.
    #[test]
    fn a_provider_with_only_cold_samples_is_still_measured() {
        for _ in 0..TOTAL_LATENCY_COLD_JUDGE_AFTER - 1 {
            record_total_latency("lat-cold-2", 30_000.0, false);
        }
        assert_eq!(
            total_latency_ms("lat-cold-2"),
            None,
            "too little evidence to judge"
        );
        record_total_latency("lat-cold-2", 30_000.0, false);
        assert_eq!(total_latency_ms("lat-cold-2"), Some(30_000.0));
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
