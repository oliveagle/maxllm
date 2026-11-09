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
use serde::{Deserialize, Serialize};
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
/// How long a sample is *kept*, as opposed to how long it counts as
/// evidence.
///
/// The two are different on purpose. A verdict may only outlive its evidence
/// by nothing (`TOTAL_LATENCY_WINDOW`), but the samples themselves are kept
/// for an hour because the decode-rate estimate needs many of them and a
/// provider's speed is a slow property: it does not change between two
/// requests the way a cache ratio does.
const TOTAL_LATENCY_RETENTION: Duration = Duration::from_secs(60 * 60);
/// The window of the decode-rate estimate: the samples kept for an hour.
const DECODE_RATE_WINDOW: Duration = TOTAL_LATENCY_RETENTION;
/// Hard cap on retained samples per provider, independent of the window, so a
/// provider inside the window cannot grow the map without bound. The oldest
/// samples go first; reaching this within the window needs a rate no route
/// here sustains, so in practice the window is what prunes.
const TOTAL_LATENCY_MAX_SAMPLES: usize = 1024;

/// How long the pool remembers how fast a member was.
///
/// A provider's speed is a slow property: it does not change between two
/// requests the way a cache ratio does. The *samples* behind the estimate
/// age out (an hour), and so does the estimate's own window — so a member
/// that goes quiet is suddenly *unmeasured*, and the rotation weighs it like
/// a brand-new account. That is how a member the pool had already measured
/// as slow got its traffic back on NAS: `xiaomi` (2.5-3.5x slower than the
/// tier) took two of twelve new sessions within half an hour of its
/// evidence expiring — and a restart wiped the evidence entirely, so a
/// deploy handed the slowest member the same share as the fastest.
///
/// The remembered rate is what the *rotation* weighs a member by. It is not
/// what evicts a warm session: that decision needs fresh evidence, because a
/// stale verdict would throw away a live upstream cache.
const SPEED_MEMORY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// How often the remembered rate is refreshed while a member is active.
///
/// Frequent enough that a member which got faster re-earns its share
/// quickly, rare enough that the median is not recomputed per request.
const SPEED_MEMORY_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// What the pool remembers about one member's speed.
#[derive(Debug, Clone, Copy)]
struct SpeedMemory {
    /// Milliseconds per 1 000 output tokens.
    ms_per_1k: f64,
    /// How many samples the estimate is built on.
    samples: u64,
    /// When the estimate was taken.
    measured_at: Instant,
}

/// The last speed estimate the pool computed for each provider.
///
/// Unlike the samples it is built on, this does not age out with the
/// window: it is refreshed every [`SPEED_MEMORY_REFRESH_INTERVAL`] while
/// the provider is serving, and read afterwards for as long as
/// [`SPEED_MEMORY_TTL`] allows.
static LAST_SPEED: Lazy<RwLock<AHashMap<String, SpeedMemory>>> =
    Lazy::new(|| RwLock::new(AHashMap::new()));
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
    /// `(recorded_at, whole-exchange ms, warm, output tokens)`, oldest first.
    /// `warm` is the fraction of the input the upstream served from its
    /// prompt cache, over the threshold the caller applied — see
    /// [`record_total_latency`]. The output tokens are what make the
    /// *workload-independent* speed estimate possible: without them a
    /// whole-exchange duration says as much about how much the client asked
    /// to generate as about how fast the provider is.
    samples: VecDeque<(Instant, f64, bool, u64)>,
    /// When the pool's memory of this provider's speed was last refreshed.
    speed_refreshed_at: Option<Instant>,
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

/// The smallest answer whose decode time is worth measuring.
///
/// A 40-token answer is dominated by the round trip and the prefill; its
/// "milliseconds per output token" says more about the request than about
/// the provider.
const DECODE_RATE_MIN_OUTPUT_TOKENS: u64 = 256;

/// How many such answers a provider needs before its decode rate may be
/// used for anything.
const DECODE_RATE_MIN_SAMPLES: usize = 4;

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
pub fn record_total_latency(provider: &str, latency_ms: f64, warm: bool, tokens_out: u64) {
    let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
    let stats = map.entry(provider.to_string()).or_default();
    let now = Instant::now();
    stats.samples.push_back((now, latency_ms, warm, tokens_out));
    while stats
        .samples
        .front()
        .is_some_and(|(at, _, _, _)| now.duration_since(*at) > TOTAL_LATENCY_RETENTION)
    {
        stats.samples.pop_front();
    }
    while stats.samples.len() > TOTAL_LATENCY_MAX_SAMPLES {
        stats.samples.pop_front();
    }
    // Keep the pool's *memory* of this provider's speed alive while it is
    // serving: the samples behind the estimate age out, the estimate itself
    // must not — otherwise an idle member becomes indistinguishable from a
    // brand-new one and the rotation hands it the same share as the fastest.
    // The estimate needs enough samples to exist at all; until then there is
    // nothing to remember, and the next sample tries again.
    let have_estimate = stats.samples.len() >= DECODE_RATE_MIN_SAMPLES;
    let refresh = have_estimate
        && stats
            .speed_refreshed_at
            .is_none_or(|at| now.duration_since(at) >= SPEED_MEMORY_REFRESH_INTERVAL);
    if refresh {
        if let Some(rate) = decode_rate_of(&stats.samples, now) {
            // Only a *successful* estimate starts the interval. An attempt
            // that came up short (too few qualifying samples) must not burn
            // it: the next sample then tries again at once. Otherwise a
            // member whose traffic arrives in bursts is remembered never —
            // the first attempt lands before the pool has four qualifying
            // samples, and every later sample falls inside the interval it
            // spent. Measured on NAS 2026-11-09: `deepseek` answered six
            // qualifying turns in 50 s and was remembered *not at all*.
            stats.speed_refreshed_at = Some(now);
            let samples = stats.samples.len() as u64;
            let mut speed = LAST_SPEED.write().unwrap_or_else(|e| e.into_inner());
            speed.insert(
                provider.to_string(),
                SpeedMemory {
                    ms_per_1k: rate,
                    samples,
                    measured_at: now,
                },
            );
        }
    }
}

/// The provider's speed as the pool *remembers* it.
///
/// Unlike [`decode_rate_ms_per_1k`], this does not need a fresh sample: it
/// is the last estimate the pool computed while the provider was serving,
/// valid for [`SPEED_MEMORY_TTL`]. `None` means the provider was never
/// measured — that is a different thing from "measured, and idle since",
/// and the two must not be treated the same.
pub fn known_decode_rate_ms_per_1k(provider: &str) -> Option<f64> {
    let map = LAST_SPEED.read().unwrap_or_else(|e| e.into_inner());
    let memory = map.get(provider)?;
    if memory.measured_at.elapsed() > SPEED_MEMORY_TTL {
        return None;
    }
    Some(memory.ms_per_1k)
}

/// Seed the remembered speed of one provider from the store.
///
/// The estimate comes back *aged*: a speed measured yesterday describes the
/// provider as it was yesterday. It is still better than no estimate at all
/// — a provider the pool already measured as slow must not be handed the
/// fastest member's share just because it went quiet.
pub fn restore_speed_memory(provider: &str, ms_per_1k: f64, samples: u64, age_secs: u64) {
    if !(ms_per_1k.is_finite() && ms_per_1k > 0.0) || samples == 0 {
        return;
    }
    if age_secs > SPEED_MEMORY_TTL.as_secs() {
        return;
    }
    let now = Instant::now();
    let mut map = LAST_SPEED.write().unwrap_or_else(|e| e.into_inner());
    map.entry(provider.to_string()).or_insert(SpeedMemory {
        ms_per_1k,
        samples,
        measured_at: now
            .checked_sub(Duration::from_secs(age_secs))
            .unwrap_or(now),
    });
}

/// The provider's speed, measured in milliseconds per 1 000 output tokens.
///
/// This is the *workload-independent* speed: a whole-exchange duration says
/// as much about how much the client asked to generate as about how fast the
/// provider is, so a provider that happened to serve short requests used to
/// look like the fastest member of the pool. Measured on NAS (2026-11-09),
/// the two signals disagreed completely: `opencode_go_2` had the best
/// whole-exchange median (4 101 ms) and the worst decode rate of the cloud
/// members (13 950 ms/1k ≈ 72 tok/s), while `ollama_local` looked ordinary
/// (5 555 ms) and was in fact the fastest (6 119 ms/1k ≈ 163 tok/s).
///
/// Only warm turns count (a cold turn's prefill dominates its duration) and
/// only answers long enough to be mostly decoding (a 40-token answer says
/// more about the round trip than about the provider).
pub fn decode_rate_ms_per_1k(provider: &str) -> Option<f64> {
    let map = TOTAL_LATENCY.read().unwrap_or_else(|e| e.into_inner());
    let stats = map.get(provider)?;
    decode_rate_of(&stats.samples, Instant::now())
}

/// The decode rate of a sample set: milliseconds per 1 000 output tokens.
///
/// Only warm turns count (a cold turn's prefill dominates its duration) and
/// only answers long enough to be mostly decoding (a 40-token answer says
/// more about the round trip than about the provider).
fn decode_rate_of(samples: &VecDeque<(Instant, f64, bool, u64)>, now: Instant) -> Option<f64> {
    let mut rates: Vec<f64> = samples
        .iter()
        .filter(|(at, _, warm, tokens_out)| {
            *warm
                && *tokens_out >= DECODE_RATE_MIN_OUTPUT_TOKENS
                && now.duration_since(*at) <= DECODE_RATE_WINDOW
        })
        .map(|(_, latency_ms, _, tokens_out)| latency_ms * 1000.0 / *tokens_out as f64)
        .collect();
    if rates.len() < DECODE_RATE_MIN_SAMPLES {
        return None;
    }
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(rates[rates.len() / 2])
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
        .filter(|(at, _, _, _)| now.duration_since(*at) <= TOTAL_LATENCY_WINDOW)
        .map(|(_, ms, warm, _)| (*ms, *warm))
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

/// The median whole-exchange duration over the member's recent turns that
/// produced too few output tokens for a per-token rate to be meaningful.
///
/// A long answer dominates the duration with its generation, so the member
/// looks slow because the client asked for a lot. A *short* answer cannot do
/// that, and a warm one rules out the other suspect — a cold prefill of the
/// whole context. What is left is the member's own latency: the queue, the
/// round trip, the thinking time. That is the only duration evidence allowed
/// to condemn a member.
///
/// Measured on NAS (2026-11-09), every slow sample of the 06:00 window was a
/// long answer: `minimax` 13 250 ms for 1 879 output tokens, `volc_1`
/// 23 696 ms for 1 189, `volc_t_1` 19 179 ms for 1 589, `xiaomi` 28 500 ms
/// for 880. Those durations said what the client asked for, not how fast the
/// member is.
pub fn unattributable_latency_ms(provider: &str) -> Option<f64> {
    let map = TOTAL_LATENCY.read().unwrap_or_else(|e| e.into_inner());
    let stats = map.get(provider)?;
    let now = Instant::now();
    let mut readings: Vec<f64> = stats
        .samples
        .iter()
        .filter(|(at, _, warm, tokens_out)| {
            *warm
                && *tokens_out < DECODE_RATE_MIN_OUTPUT_TOKENS
                && now.duration_since(*at) <= TOTAL_LATENCY_WINDOW
        })
        .map(|(_, ms, _, _)| *ms)
        .collect();
    if readings.is_empty() {
        return None;
    }
    readings.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(readings[readings.len() / 2])
}

/// One whole-exchange latency sample, as it crosses the process boundary.
///
/// The in-memory sample carries an [`Instant`], which means nothing in
/// another process: on the wire it becomes an *age*, and the age is what the
/// restored sample is turned back into.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySample {
    /// How long ago the sample was taken.
    pub age_secs: u64,
    /// The whole-exchange duration, in milliseconds.
    pub latency_ms: f64,
    /// Whether the upstream served at least half of the input from its cache.
    pub warm: bool,
    /// How many tokens the provider generated.
    ///
    /// `default` keeps the rows written before this field existed readable:
    /// their samples simply do not count towards the decode rate.
    #[serde(default)]
    pub tokens_out: u64,
}

/// The routing evidence for one provider, as it crosses the process boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderStatsSnapshot {
    pub provider: String,
    /// The cached share of the input; `None` when the provider is unmeasured.
    pub cache_ratio: Option<f64>,
    /// How many exchanges the ratio is built on.
    pub cache_samples: u64,
    /// How long ago the newest cache observation was taken.
    pub cache_age_secs: u64,
    /// The whole-exchange latency window.
    pub total_latency: Vec<LatencySample>,
    /// The remembered speed (milliseconds per 1 000 output tokens); `None`
    /// when the provider was never measured.
    pub speed_ms_per_1k: Option<f64>,
    /// How many samples the remembered speed is built on.
    pub speed_samples: u64,
    /// How long ago the remembered speed was taken.
    pub speed_age_secs: u64,
}

/// A snapshot of every provider's routing evidence.
///
/// The evidence is what the auto pool knows about its members: how much of
/// the input a provider serves from its prompt cache, and how long a whole
/// exchange takes on it. It lives in process memory, so a restart makes the
/// pool *relearn blind* — measured on 2026-11-09, a restart took the cache
/// ratio from 99.0% to 76.7% within one minute, because the sessions moved
/// and the upstream caches went cold. A snapshot is what makes a restart
/// invisible.
pub fn snapshot() -> Vec<ProviderStatsSnapshot> {
    let now = Instant::now();
    let cache = CACHE_RATIO.read().unwrap_or_else(|e| e.into_inner());
    let latency = TOTAL_LATENCY.read().unwrap_or_else(|e| e.into_inner());

    let speed = LAST_SPEED.read().unwrap_or_else(|e| e.into_inner());
    let mut out: Vec<ProviderStatsSnapshot> =
        Vec::with_capacity(cache.len() + latency.len() + speed.len());
    for (provider, stats) in cache.iter() {
        out.push(ProviderStatsSnapshot {
            provider: provider.clone(),
            cache_ratio: (stats.samples > 0).then_some(stats.ewma),
            cache_samples: stats.samples,
            cache_age_secs: now.duration_since(stats.last_seen).as_secs(),
            total_latency: latency_samples(&latency, provider, now),
            ..speed_snapshot(&speed, provider, now)
        });
    }
    for provider in latency.keys() {
        if cache.contains_key(provider) {
            continue;
        }
        out.push(ProviderStatsSnapshot {
            provider: provider.clone(),
            cache_ratio: None,
            cache_samples: 0,
            cache_age_secs: 0,
            total_latency: latency_samples(&latency, provider, now),
            ..speed_snapshot(&speed, provider, now)
        });
    }
    // A provider that only has a *remembered* speed still knows something
    // the pool must not forget: it was measured, and the measurement is
    // what keeps it from being handed the fastest member's share.
    for provider in speed.keys() {
        if cache.contains_key(provider) || latency.contains_key(provider) {
            continue;
        }
        out.push(ProviderStatsSnapshot {
            provider: provider.clone(),
            cache_ratio: None,
            cache_samples: 0,
            cache_age_secs: 0,
            total_latency: Vec::new(),
            ..speed_snapshot(&speed, provider, now)
        });
    }
    out
}

/// The remembered speed of one provider, as a snapshot fragment.
fn speed_snapshot(
    speed: &AHashMap<String, SpeedMemory>,
    provider: &str,
    now: Instant,
) -> ProviderStatsSnapshot {
    match speed.get(provider) {
        Some(memory) => ProviderStatsSnapshot {
            provider: provider.to_string(),
            cache_ratio: None,
            cache_samples: 0,
            cache_age_secs: 0,
            total_latency: Vec::new(),
            speed_ms_per_1k: Some(memory.ms_per_1k),
            speed_samples: memory.samples,
            speed_age_secs: now.duration_since(memory.measured_at).as_secs(),
        },
        None => ProviderStatsSnapshot {
            provider: provider.to_string(),
            cache_ratio: None,
            cache_samples: 0,
            cache_age_secs: 0,
            total_latency: Vec::new(),
            speed_ms_per_1k: None,
            speed_samples: 0,
            speed_age_secs: 0,
        },
    }
}

/// The fresh latency samples of one provider, as ages.
fn latency_samples(
    map: &AHashMap<String, TotalLatencyStats>,
    provider: &str,
    now: Instant,
) -> Vec<LatencySample> {
    let Some(stats) = map.get(provider) else {
        return Vec::new();
    };
    stats
        .samples
        .iter()
        .filter(|(at, _, _, _)| now.duration_since(*at) <= TOTAL_LATENCY_WINDOW)
        .map(|(at, latency_ms, warm, tokens_out)| LatencySample {
            age_secs: now.duration_since(*at).as_secs(),
            latency_ms: *latency_ms,
            warm: *warm,
            tokens_out: *tokens_out,
        })
        .collect()
}

/// Restore one provider's routing evidence.
///
/// The evidence comes back **aged**: a sample that fell out of its window
/// while the process was down is not evidence any more, and a provider
/// without fresh evidence is *unmeasured* — the state the rotation admits —
/// rather than judged on a measurement that no longer describes it.
/// Restoring a stale verdict would be worse than losing it: the pool would
/// keep excluding a provider on the strength of a reading that expired.
///
/// Returns whether anything was applied.
pub fn restore(snapshot: &ProviderStatsSnapshot) -> bool {
    let now = Instant::now();
    let mut applied = false;

    // The cache evidence.
    if let Some(ratio) = snapshot.cache_ratio.filter(|_| snapshot.cache_samples > 0) {
        let age = Duration::from_secs(snapshot.cache_age_secs);
        if age <= CACHE_RATIO_WINDOW {
            let mut map = CACHE_RATIO.write().unwrap_or_else(|e| e.into_inner());
            let stats = map.entry(snapshot.provider.clone()).or_default();
            // Fresh in-process evidence always wins: a restore never
            // overwrites what this process measured itself.
            if stats.samples == 0 {
                stats.ewma = ratio.clamp(0.0, 1.0);
                stats.samples = snapshot.cache_samples;
                stats.last_seen = now.checked_sub(age).unwrap_or(now);
                applied = true;
            }
        }
    }

    // The latency window.
    let samples: Vec<(Instant, f64, bool, u64)> = snapshot
        .total_latency
        .iter()
        .map(|s| {
            (
                now.checked_sub(Duration::from_secs(s.age_secs))
                    .unwrap_or(now),
                s.latency_ms,
                s.warm,
                s.tokens_out,
            )
        })
        .filter(|(at, _, _, _)| now.duration_since(*at) <= TOTAL_LATENCY_WINDOW)
        .collect();
    if !samples.is_empty() {
        let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
        let stats = map.entry(snapshot.provider.clone()).or_default();
        if stats.samples.is_empty() {
            stats.samples.extend(samples);
            applied = true;
        }
    }

    // The remembered speed. Unlike the two windows above this one is *not*
    // dropped when it is old: it is the pool's memory of a slow property,
    // and a restart must not hand the slowest member the fastest member's
    // share (measured on NAS: a deploy wiped `xiaomi`'s evidence and it
    // took two of twelve new sessions within the half hour that followed).
    if let Some(rate) = snapshot.speed_ms_per_1k {
        restore_speed_memory(
            &snapshot.provider,
            rate,
            snapshot.speed_samples,
            snapshot.speed_age_secs,
        );
    }

    applied
}

/// Test-only: forget one provider's evidence.
///
/// One provider at a time: the store is process-global and the tests run
/// beside each other, so clearing the whole map would take the other tests'
/// evidence with it.
#[cfg(test)]
pub(crate) fn forget(provider: &str) {
    CACHE_RATIO
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(provider);
    TOTAL_LATENCY
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(provider);
    LAST_SPEED
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(provider);
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
    for (at, _, _, _) in stats.samples.iter_mut() {
        *at = aged;
    }
}

/// Test-only: age `provider`'s samples past the decode-rate window, the way
/// an hour of silence would.
///
/// The window's promise is that a verdict never outlives its evidence; the
/// pool's *memory* of a slow property is the one thing that does outlive it,
/// because forgetting it would hand the slowest member the fastest member's
/// share. Proving that needs time to pass, so the test moves this one
/// provider's timestamps instead of sleeping.
#[cfg(test)]
pub(crate) fn expire_decode_rate_evidence(provider: &str) {
    let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
    let Some(stats) = map.get_mut(provider) else {
        return;
    };
    let aged = Instant::now() - DECODE_RATE_WINDOW - Duration::from_secs(1);
    for (at, _, _, _) in stats.samples.iter_mut() {
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
        record_total_latency("lat-total-1", 100.0, true, 4096);
        assert_eq!(total_latency_ms("lat-total-1"), Some(100.0));
        record_total_latency("lat-total-1", 200.0, true, 4096);
        // Two samples: the upper middle one. Never below either endpoint.
        assert_eq!(total_latency_ms("lat-total-1"), Some(200.0));
        record_total_latency("lat-total-1", 150.0, true, 4096);
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
            record_total_latency("lat-outlier-1", ms, true, 4096);
        }
        assert_eq!(total_latency_ms("lat-outlier-1"), Some(4_100.0));
        record_total_latency("lat-outlier-1", 90_000.0, true, 4096);
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
        record_total_latency("lat-stale-1", 5_000.0, true, 4096);
        assert_eq!(total_latency_ms("lat-stale-1"), Some(5_000.0));
        {
            let mut map = TOTAL_LATENCY.write().unwrap_or_else(|e| e.into_inner());
            let stats = map.get_mut("lat-stale-1").expect("recorded above");
            for (at, _, _, _) in stats.samples.iter_mut() {
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
        record_total_latency("lat-stale-1", 3_000.0, true, 4096);
        assert_eq!(total_latency_ms("lat-stale-1"), Some(3_000.0));
    }

    /// The pool remembers a member's speed after the samples behind it age
    /// out.
    ///
    /// A provider's speed is a slow property. The samples behind the estimate
    /// are kept for an hour, and so is the estimate's own window — so a
    /// member that goes quiet is suddenly *unmeasured*, and the rotation
    /// weighs it like a brand-new account. Measured on NAS: `xiaomi`, 2.5-3.5x
    /// slower than the tier, took two of twelve new sessions within half an
    /// hour of its evidence expiring.
    #[test]
    fn the_remembered_speed_outlives_its_samples() {
        forget("speed-memory");
        for _ in 0..8 {
            record_total_latency("speed-memory", 20_000.0, true, 1_000);
        }
        assert_eq!(
            known_decode_rate_ms_per_1k("speed-memory"),
            Some(20_000.0),
            "the pool must remember what it measured"
        );
        // An hour of silence: the fresh estimate is gone ...
        expire_decode_rate_evidence("speed-memory");
        assert!(
            decode_rate_ms_per_1k("speed-memory").is_none(),
            "the fresh estimate must be gone"
        );
        // ... but what the pool measured is not.
        assert_eq!(
            known_decode_rate_ms_per_1k("speed-memory"),
            Some(20_000.0),
            "a member the pool measured as slow must not be re-measured from scratch"
        );
        forget("speed-memory");
        assert!(known_decode_rate_ms_per_1k("speed-memory").is_none());
    }

    /// A member whose traffic arrives in bursts is remembered too.
    ///
    /// The refresh interval is there to keep the median from being
    /// recomputed on every request — but an attempt that came up short (too
    /// few qualifying samples yet) must not *start* that interval, or the
    /// samples that would have qualified are all inside it. Measured on NAS
    /// 2026-11-09: `deepseek` answered six qualifying turns in 50 s and the
    /// pool remembered nothing at all — the first attempt landed before the
    /// fourth qualifying sample, and every later turn fell inside the
    /// interval that attempt spent.
    #[test]
    fn a_bursty_member_is_remembered_too() {
        forget("speed-bursty");
        // Two cold turns (a cold prefill says nothing about the speed) and
        // then four warm ones: the first refresh attempt lands on the
        // fourth sample, when only two samples qualify.
        for _ in 0..2 {
            record_total_latency("speed-bursty", 20_000.0, false, 1_000);
        }
        for _ in 0..4 {
            record_total_latency("speed-bursty", 20_000.0, true, 1_000);
        }
        assert_eq!(
            known_decode_rate_ms_per_1k("speed-bursty"),
            Some(20_000.0),
            "a member measured in bursts must be remembered"
        );
        forget("speed-bursty");
    }

    /// A member the pool never measured has no remembered speed — and it
    /// must be given traffic to be measured, so the rotation treats it as
    /// unmeasured, not as slow.
    #[test]
    fn a_never_measured_member_has_no_remembered_speed() {
        forget("speed-never");
        assert!(known_decode_rate_ms_per_1k("speed-never").is_none());
        assert!(decode_rate_ms_per_1k("speed-never").is_none());
    }

    /// The remembered speed survives a restart: it comes back from the store,
    /// aged, and still counts. A memory too old to describe the provider is
    /// dropped instead.
    #[test]
    fn the_remembered_speed_comes_back_from_the_store() {
        forget("speed-restored");
        restore_speed_memory("speed-restored", 20_000.0, 12, 60);
        assert_eq!(
            known_decode_rate_ms_per_1k("speed-restored"),
            Some(20_000.0)
        );
        forget("speed-stale");
        restore_speed_memory("speed-stale", 20_000.0, 12, 30 * 24 * 3600);
        assert!(
            known_decode_rate_ms_per_1k("speed-stale").is_none(),
            "a memory older than its lifetime must not describe the provider"
        );
        forget("speed-restored");
    }

    /// The snapshot carries the remembered speed, so the store can persist
    /// it: a provider that only has a remembered speed still has a row.
    #[test]
    fn the_snapshot_carries_the_remembered_speed() {
        forget("speed-snapshot");
        for _ in 0..8 {
            record_total_latency("speed-snapshot", 20_000.0, true, 1_000);
        }
        expire_decode_rate_evidence("speed-snapshot");
        let snapshot = snapshot()
            .into_iter()
            .find(|s| s.provider == "speed-snapshot")
            .expect("the provider has evidence");
        assert_eq!(snapshot.speed_ms_per_1k, Some(20_000.0));
        assert!(snapshot.speed_samples >= 4);
        forget("speed-snapshot");
    }

    /// The first turn of a conversation is a full-prefix prefill. Judging a
    /// provider on it is what banished one after two cold-start requests and
    /// then kept it out of the rotation forever: the ban stopped the traffic
    /// that could ever have revised the verdict.
    #[test]
    fn cold_samples_never_outweigh_warm_evidence() {
        // A cold 40 s prefill arrives after a warm 4 s turn.
        record_total_latency("lat-cold-1", 4_000.0, true, 4096);
        record_total_latency("lat-cold-1", 40_000.0, false, 4096);
        assert_eq!(
            total_latency_ms("lat-cold-1"),
            Some(4_000.0),
            "warm evidence must stand, one cold prefill notwithstanding"
        );
        // More cold samples still do not move it while warm evidence exists.
        for _ in 0..8 {
            record_total_latency("lat-cold-1", 40_000.0, false, 4096);
        }
        assert_eq!(total_latency_ms("lat-cold-1"), Some(4_000.0));
    }

    /// A provider that never reports a cache hit must not become permanently
    /// unmeasured: an unmeasured entry is never escaped by a pin, so it would
    /// hold sessions no matter how slow it is.
    #[test]
    fn a_provider_with_only_cold_samples_is_still_measured() {
        for _ in 0..TOTAL_LATENCY_COLD_JUDGE_AFTER - 1 {
            record_total_latency("lat-cold-2", 30_000.0, false, 4096);
        }
        assert_eq!(
            total_latency_ms("lat-cold-2"),
            None,
            "too little evidence to judge"
        );
        record_total_latency("lat-cold-2", 30_000.0, false, 4096);
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

    /// The routing evidence is the pool's memory of its members. If it does
    /// not survive the process, every restart makes the pool relearn blind:
    /// the sessions move, the caches go cold, and the cache ratio collapses
    /// (measured: 99.0% -> 76.7% within a minute of a restart on NAS).
    #[test]
    fn the_routing_evidence_survives_a_restart() {
        forget("persist-1");
        record_cache_ratio("persist-1", 1800, 2000); // 90% of the input cached
        record_total_latency("persist-1", 4_000.0, true, 4096);

        let snap = snapshot();
        let one = snap
            .iter()
            .find(|s| s.provider == "persist-1")
            .expect("the provider is in the snapshot")
            .clone();
        assert!(one.cache_ratio.is_some());
        assert_eq!(one.cache_samples, 1);
        assert_eq!(one.total_latency.len(), 1);

        // The process dies: the evidence is gone from memory ...
        forget("persist-1");
        assert_eq!(cache_ratio("persist-1"), None);
        assert_eq!(total_latency_ms("persist-1"), None);

        // ... and comes back from the snapshot.
        assert!(restore(&one));
        let ratio = cache_ratio("persist-1").expect("restored");
        assert!(
            (ratio - 0.9).abs() < 1e-6,
            "the restored ratio must be the measured one, got {ratio}"
        );
        assert_eq!(total_latency_ms("persist-1"), Some(4_000.0));
        forget("persist-1");
    }

    /// Evidence that fell out of its window while the process was down is not
    /// evidence any more. Restoring it would make the pool exclude a provider
    /// on the strength of a reading that no longer describes it.
    #[test]
    fn stale_evidence_is_not_restored() {
        forget("persist-stale");
        record_cache_ratio("persist-stale", 100, 2000);
        record_total_latency("persist-stale", 30_000.0, false, 4096);

        let mut aged = snapshot()
            .into_iter()
            .find(|s| s.provider == "persist-stale")
            .expect("the provider is in the snapshot");
        aged.cache_age_secs = CACHE_RATIO_WINDOW.as_secs() + 60;
        aged.total_latency[0].age_secs = TOTAL_LATENCY_WINDOW.as_secs() + 60;

        forget("persist-stale");
        assert!(
            !restore(&aged),
            "evidence outside its window must not be restored"
        );
        assert_eq!(cache_ratio("persist-stale"), None);
        assert_eq!(total_latency_ms("persist-stale"), None);
        forget("persist-stale");
    }

    /// A restore never overwrites what this process measured itself.
    #[test]
    fn a_restore_does_not_overwrite_fresh_evidence() {
        forget("persist-fresh");
        record_cache_ratio("persist-fresh", 1_000, 2_000); // 50%
        let aged = ProviderStatsSnapshot {
            provider: "persist-fresh".into(),
            cache_ratio: Some(0.99),
            cache_samples: 99,
            cache_age_secs: 1,
            total_latency: Vec::new(),
            speed_ms_per_1k: None,
            speed_samples: 0,
            speed_age_secs: 0,
        };
        assert!(!restore(&aged));
        let ratio = cache_ratio("persist-fresh").expect("measured");
        assert!((ratio - 0.5).abs() < 1e-6, "got {ratio}");
        forget("persist-fresh");
    }

    /// The speed estimate must not be fooled by the client's workload: two
    /// providers can have the *same* whole-exchange median while one of them
    /// answers eight times as fast per token. Measured on NAS 2026-11-09:
    /// `opencode_go_2` had the best median (4 101 ms) and the worst decode
    /// rate of the cloud members (13 950 ms/1k), `ollama_local` looked
    /// ordinary (5 555 ms) and was the fastest (6 119 ms/1k).
    #[test]
    fn the_decode_rate_sees_the_speed_not_the_workload() {
        forget("decode-fast");
        forget("decode-slow");
        for _ in 0..5 {
            record_total_latency("decode-fast", 10_000.0, true, 8_000);
            record_total_latency("decode-slow", 10_000.0, true, 1_000);
        }
        // The duration is the same for both — the old signal could not tell
        // them apart.
        assert_eq!(total_latency_ms("decode-fast"), Some(10_000.0));
        assert_eq!(total_latency_ms("decode-slow"), Some(10_000.0));
        let fast = decode_rate_ms_per_1k("decode-fast").expect("measured");
        let slow = decode_rate_ms_per_1k("decode-slow").expect("measured");
        assert!((fast - 1_250.0).abs() < 1e-6, "got {fast}");
        assert!((slow - 10_000.0).abs() < 1e-6, "got {slow}");
        forget("decode-fast");
        forget("decode-slow");
    }

    /// A short answer is mostly round trip and prefill: it says nothing
    /// about how fast the provider decodes.
    #[test]
    fn a_short_answer_is_not_evidence_of_the_speed() {
        forget("decode-short");
        for _ in 0..5 {
            record_total_latency("decode-short", 500.0, true, 10);
        }
        assert_eq!(decode_rate_ms_per_1k("decode-short"), None);
        forget("decode-short");
    }

    /// A cold turn's prefill dominates its duration, so it is not evidence
    /// of the provider's decode speed.
    #[test]
    fn a_cold_turn_is_not_evidence_of_the_speed() {
        forget("decode-cold");
        for _ in 0..5 {
            record_total_latency("decode-cold", 40_000.0, false, 4_000);
        }
        assert_eq!(decode_rate_ms_per_1k("decode-cold"), None);
        forget("decode-cold");
    }

    /// The duration of a *long* answer says what the client asked for, so it
    /// may not convict a member. This is the shape of the 2026-11-09 06:00
    /// sample that cost a warm session its upstream prompt cache: 13 250 ms
    /// for a 1 879-token answer.
    #[test]
    fn a_long_answer_is_not_unattributable_evidence() {
        forget("unattr-long");
        record_total_latency("unattr-long", 13_250.0, true, 1_879);
        assert_eq!(unattributable_latency_ms("unattr-long"), None);
        forget("unattr-long");
    }

    /// A *short* answer cannot hide behind its length: the wait is the
    /// member's own, and the duration may convict it.
    #[test]
    fn a_short_answer_is_unattributable_evidence() {
        forget("unattr-short");
        for _ in 0..5 {
            record_total_latency("unattr-short", 13_250.0, true, 100);
        }
        assert_eq!(unattributable_latency_ms("unattr-short"), Some(13_250.0));
        forget("unattr-short");
    }

    /// A cold turn's prefill dominates its duration — the other suspect
    /// besides the generation. It is not evidence either.
    #[test]
    fn a_cold_short_answer_is_not_unattributable_evidence() {
        forget("unattr-cold");
        for _ in 0..5 {
            record_total_latency("unattr-cold", 13_250.0, false, 100);
        }
        assert_eq!(unattributable_latency_ms("unattr-cold"), None);
        forget("unattr-cold");
    }

    /// Long and short answers are judged separately: the median of the
    /// *short* ones is the only duration that may convict.
    #[test]
    fn the_unattributable_median_ignores_the_long_answers() {
        forget("unattr-mixed");
        record_total_latency("unattr-mixed", 60_000.0, true, 4_000);
        record_total_latency("unattr-mixed", 4_000.0, true, 50);
        record_total_latency("unattr-mixed", 5_000.0, true, 50);
        record_total_latency("unattr-mixed", 6_000.0, true, 50);
        // The 60 s turn is a long answer: it is not evidence, and it must
        // not enter the median either.
        assert_eq!(unattributable_latency_ms("unattr-mixed"), Some(5_000.0));
        forget("unattr-mixed");
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
