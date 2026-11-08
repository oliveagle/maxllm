// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use maxllm_admin::CircuitBreakerStateRow;

/// Circuit states (kept as small ints so the whole breaker fits in atomics).
pub const STATE_CLOSED: u8 = 0;
pub const STATE_OPEN: u8 = 1;
pub const STATE_HALF_OPEN: u8 = 2;

/// Lock-free circuit breaker using atomic counters.
///
/// Three states (mirroring litellm_lb_go's breaker):
/// - Closed (healthy): failures < max_fails
/// - Open (unhealthy): failures >= max_fails and still within the fail window,
///   OR a quota (HTTP 429) force-opened it until the upstream's cooldown ends.
/// - Half-open: the fail window elapsed, so the breaker lets a small number of
///   probe requests through; `successes_to_close` successes in a row close it,
///   any probe failure re-opens it.
///
/// On top of the plain failure counter there is a quota force-open: an HTTP 429
/// opens the circuit until `retry_after` (or the default cooldown) passes,
/// regardless of the failure count. That mirrors litellm_lb_go's
/// `ForceOpenUntil(quotaResetTime)` — a per-quota exhausted channel is known to
/// be unavailable until the reset, so retrying it is wasted work.
pub struct CircuitBreaker {
    failures: AtomicU32,
    /// Consecutive successes while probing in half-open.
    successes_half_open: AtomicU32,
    /// In-flight probes in half-open (bounds concurrent probing).
    pending_probes: AtomicU32,
    /// When the most recent half-open probe slot was reserved (ms epoch;
    /// 0 = none). Used only to reclaim a slot a request never released —
    /// see `reap_stale_probe`.
    last_probe_admit_ms: AtomicU64,
    last_failure_at: AtomicU64,
    max_fails: u32,
    fail_timeout_ms: u64,
    /// Max concurrent probe requests admitted while half-open.
    half_open_max: u32,
    /// Successes required (while half-open) to transition back to closed.
    successes_to_close: u32,
    state: AtomicU8,
    /// Quota force-open deadline (ms epoch); 0 = no force-open.
    force_open_until_ms: AtomicU64,
    /// Total state transitions (Closed→Open→HalfOpen→Closed…).
    state_transitions: AtomicU64,
    /// Force-opens caused by HTTP 429 (quota exhaustion).
    quota_opens: AtomicU64,
    /// Start of the current stream-health window (ms epoch; 0 = none yet).
    stream_window_start_ms: AtomicU64,
    /// Streams whose completeness was determined inside the window.
    stream_judged: AtomicU32,
    /// The truncated subset of `stream_judged` inside the window.
    stream_bad: AtomicU32,
    /// Consecutive stream-demotions with no clean window in between. Drives
    /// the hold escalation: a provider that keeps truncating after its
    /// cool-down is put away for longer each time instead of flapping back
    /// into rotation every `fail_timeout`.
    stream_demotion_streak: AtomicU32,
    /// Force-opens caused by a bad stream-truncation ratio.
    stream_opens: AtomicU64,
}

/// How long a stream-health sample window lasts before it restarts.
const STREAM_WINDOW_MS: u64 = 60_000;
/// Truncations needed inside a window before the ratio may demote a provider.
/// Below this the sample is too small to distinguish a flake from a bad
/// provider, and one unlucky request must not move production traffic.
const STREAM_MIN_BAD: u32 = 4;
/// Truncation ratio that demotes a provider for its normal fail timeout.
const STREAM_BAD_RATIO: f64 = 0.25;

/// Hold applied for the 1st, 2nd, 3rd and 4th+ consecutive stream-demotion,
/// clamped from below by the provider's own `fail_timeout`.
///
/// A provider that truncates a *systematic* share of its streams (the ollama
/// endpoint sat at ~34% for a day) is demoted at the normal 60 s timeout,
/// comes back, truncates four more answers in the first minute, and is
/// demoted again — a flap that costs the client a broken answer every cycle
/// and never actually takes the provider out of service. Escalating the hold
/// keeps it out for as long as the evidence says it is broken; a clean window
/// of real traffic resets the ladder.
const STREAM_DEMOTION_HOLD_SECS: [u64; 4] = [60, 300, 900, 1800];

impl CircuitBreaker {
    pub fn new(max_fails: u32, fail_timeout_secs: u64) -> Self {
        Self::new_with_opts(max_fails, fail_timeout_secs, 1, 1)
    }

    /// `half_open_max`: how many concurrent probe requests half-open admits.
    /// `successes_to_close`: how many probe successes close the circuit.
    pub fn new_with_opts(
        max_fails: u32,
        fail_timeout_secs: u64,
        half_open_max: u32,
        successes_to_close: u32,
    ) -> Self {
        Self {
            failures: AtomicU32::new(0),
            successes_half_open: AtomicU32::new(0),
            pending_probes: AtomicU32::new(0),
            last_probe_admit_ms: AtomicU64::new(0),
            last_failure_at: AtomicU64::new(0),
            max_fails: max_fails.max(1),
            fail_timeout_ms: fail_timeout_secs * 1000,
            half_open_max: half_open_max.max(1),
            successes_to_close: successes_to_close.max(1),
            state: AtomicU8::new(STATE_CLOSED),
            force_open_until_ms: AtomicU64::new(0),
            state_transitions: AtomicU64::new(0),
            quota_opens: AtomicU64::new(0),
            stream_window_start_ms: AtomicU64::new(0),
            stream_judged: AtomicU32::new(0),
            stream_bad: AtomicU32::new(0),
            stream_demotion_streak: AtomicU32::new(0),
            stream_opens: AtomicU64::new(0),
        }
    }

    /// Pure read: would a routing decision consider this provider usable?
    ///
    /// This is the one predicate a *filter* may use — it inspects a candidate
    /// without advancing its state machine and without reserving a half-open
    /// probe slot. A route walks every candidate of a route (twenty-five for
    /// the auto pool) and only ever picks one; the old `is_open()` was used
    /// there, so each inspected provider transitioned Closed→HalfOpen and
    /// consumed its single probe slot, and twelve pool entries ended up
    /// permanently half-open answering `503 circuit breakers open` (the
    /// 2026-11-08 pool-health probe saw 16 of 25 members in that state).
    pub fn allows_traffic(&self) -> bool {
        // Reclaim a probe slot a request never released before deciding.
        self.reap_stale_probe();
        self.effective_state() != STATE_OPEN
    }

    /// Reserve the one half-open probe slot for the provider that will
    /// actually serve a request.
    ///
    /// Exactly one call per selected provider: it advances the state machine
    /// (Closed→HalfOpen on the first request after the fail window elapses)
    /// and bounds concurrent probes to `half_open_max`. Returns `true` when
    /// the circuit is open and the request must go elsewhere; `false` when
    /// the request may use this provider. Every admission is balanced by a
    /// `release_probe()` when the request finishes.
    pub fn is_open(&self) -> bool {
        // A leaked slot (a request that ended without releasing) must not
        // keep this provider blocked forever.
        self.reap_stale_probe();
        let now = now_ms();

        // Quota force-open takes precedence over everything.
        let force_until = self.force_open_until_ms.load(Ordering::Relaxed);
        if force_until != 0 {
            if now < force_until {
                return true;
            }
            // Cooldown elapsed: clear the force-open and return to closed.
            self.force_open_until_ms.store(0, Ordering::Relaxed);
            self.failures.store(0, Ordering::Relaxed);
            self.successes_half_open.store(0, Ordering::Relaxed);
            self.pending_probes.store(0, Ordering::Relaxed);
            self.last_probe_admit_ms.store(0, Ordering::Relaxed);
            self.state.store(STATE_CLOSED, Ordering::Relaxed);
            return false;
        }

        match self.state.load(Ordering::Relaxed) {
            STATE_CLOSED => {
                let failures = self.failures.load(Ordering::Relaxed);
                if failures < self.max_fails {
                    return false;
                }
                let last = self.last_failure_at.load(Ordering::Relaxed);
                if now.saturating_sub(last) < self.fail_timeout_ms {
                    // Still inside the fail window → open.
                    self.state.store(STATE_OPEN, Ordering::Relaxed);
                    self.state_transitions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                // Fail window elapsed → half-open. CAS so exactly one caller
                // performs the transition; the winner becomes the first probe
                // request and occupies one probe slot.
                if self
                    .state
                    .compare_exchange(
                        STATE_CLOSED,
                        STATE_HALF_OPEN,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.successes_half_open.store(0, Ordering::Relaxed);
                    self.state_transitions.fetch_add(1, Ordering::Relaxed);
                }
                // Re-evaluate as a half-open caller (the CAS may have been
                // lost to a concurrent transition).
                self.half_open_admission()
            }
            STATE_OPEN => {
                let last = self.last_failure_at.load(Ordering::Relaxed);
                if now.saturating_sub(last) < self.fail_timeout_ms {
                    return true;
                }
                // CAS so exactly one caller performs the transition. The
                // winner becomes the first probe request.
                if self
                    .state
                    .compare_exchange(
                        STATE_OPEN,
                        STATE_HALF_OPEN,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.successes_half_open.store(0, Ordering::Relaxed);
                    self.state_transitions.fetch_add(1, Ordering::Relaxed);
                }
                // Lost the CAS race — another thread already transitioned to
                // half-open; evaluate as a half-open caller.
                self.half_open_admission()
            }
            STATE_HALF_OPEN => self.half_open_admission(),
            _ => false,
        }
    }

    /// Half-open admission: allow at most `half_open_max` concurrent probes;
    /// close the circuit once `successes_to_close` probes succeeded.
    fn half_open_admission(&self) -> bool {
        let successes = self.successes_half_open.load(Ordering::Relaxed);
        if successes >= self.successes_to_close {
            self.state.store(STATE_CLOSED, Ordering::Relaxed);
            self.failures.store(0, Ordering::Relaxed);
            self.successes_half_open.store(0, Ordering::Relaxed);
            self.pending_probes.store(0, Ordering::Relaxed);
            self.last_probe_admit_ms.store(0, Ordering::Relaxed);
            self.state_transitions.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let admitted = self.pending_probes.fetch_add(1, Ordering::Relaxed);
        if admitted == 0 {
            // The first slot of a new probe window: remember when it was
            // reserved so a request that never releases it can be reclaimed.
            self.last_probe_admit_ms.store(now_ms(), Ordering::Relaxed);
        }
        if admitted >= self.half_open_max {
            self.pending_probes.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Give back one half-open probe slot, if any is held.
    ///
    /// Idempotent and clamped at zero: a late stream completion may release a
    /// slot the staleness reaper already reclaimed, and an underflow would
    /// wrap the counter to `u32::MAX` and permanently block the provider.
    pub fn release_probe(&self) {
        let _ = self
            .pending_probes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
    }

    /// Reclaim a probe slot that has been held for longer than two fail
    /// windows without ever being released.
    ///
    /// The routing path is not the only way a slot can leak: a health probe
    /// deliberately books no breaker outcome, a client can hang up mid-stream,
    /// and a 4xx that fails over never records anything. Without this a single
    /// leaked slot left a provider half-open for ever, answering
    /// `503 circuit breakers open` until the gateway restarted.
    fn reap_stale_probe(&self) {
        if self.pending_probes.load(Ordering::Relaxed) == 0 {
            return;
        }
        let since = self.last_probe_admit_ms.load(Ordering::Relaxed);
        if since == 0 {
            return; // a slot predating the timestamp bookkeeping
        }
        let stale_after = self.fail_timeout_ms.saturating_mul(2).max(120_000);
        if now_ms().saturating_sub(since) <= stale_after {
            return;
        }
        self.pending_probes.store(0, Ordering::Relaxed);
        self.last_probe_admit_ms.store(0, Ordering::Relaxed);
    }

    /// Force-open the circuit until an absolute epoch-ms instant.
    ///
    /// Used when a layer resolved the provider's exact quota reset time —
    /// the provider is known unavailable until that moment, so retrying it
    /// before then is wasted work. Only ever extends the existing deadline
    /// (a later observation wins), never shortens it.
    pub fn record_quota_until(&self, reset_at_ms: u64) {
        let now = now_ms();
        if reset_at_ms <= now {
            return; // already reset; nothing to force-open
        }
        self.force_open_until_ms
            .store(reset_at_ms, Ordering::Relaxed);
        self.state.store(STATE_OPEN, Ordering::Relaxed);
        self.failures.store(self.max_fails, Ordering::Relaxed);
        // No probe can be in flight against an open circuit: the slot of the
        // request that just hit the quota wall (if it held one) is returned.
        self.pending_probes.store(0, Ordering::Relaxed);
        self.last_probe_admit_ms.store(0, Ordering::Relaxed);
        self.quota_opens.fetch_add(1, Ordering::Relaxed);
        self.state_transitions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failure. Increments failure count and updates timestamp.
    pub fn record_failure(&self) {
        self.record_http_failure(500, None);
    }

    /// Record an HTTP-level failure. A 429 (quota exhaustion) force-opens the
    /// circuit until `retry_after_secs` (default: the breaker's fail timeout)
    /// passes; any other failure counts against `max_fails` and, while
    /// half-open, re-opens the circuit.
    pub fn record_http_failure(&self, status: u16, retry_after_secs: Option<u64>) {
        let now = now_ms();

        if status == 429 {
            let cooldown_secs = retry_after_secs.unwrap_or(self.fail_timeout_ms / 1000);
            self.force_open_until_ms
                .store(now + cooldown_secs * 1000, Ordering::Relaxed);
            // Pin the state so is_open() reads it as open even outside the
            // force-open check path.
            self.state.store(STATE_OPEN, Ordering::Relaxed);
            self.failures.store(self.max_fails, Ordering::Relaxed);
            // The probe that saw the 429 is finished; it must not keep
            // holding the half-open slot while the circuit is force-open.
            self.pending_probes.store(0, Ordering::Relaxed);
            self.last_probe_admit_ms.store(0, Ordering::Relaxed);
            self.quota_opens.fetch_add(1, Ordering::Relaxed);
            self.state_transitions.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match self.state.load(Ordering::Relaxed) {
            STATE_HALF_OPEN => {
                // A probe failed → back to open. Hand the slot back before
                // clearing the counter so a concurrent release cannot
                // underflow it.
                self.release_probe();
                self.state.store(STATE_OPEN, Ordering::Relaxed);
                self.failures.store(self.max_fails, Ordering::Relaxed);
                self.last_failure_at.store(now, Ordering::Relaxed);
                self.state_transitions.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                self.last_failure_at.store(now, Ordering::Relaxed);
            }
        }
    }

    /// Record a success. Resets failure count; while half-open, counts toward
    /// closing the circuit.
    /// Feed one judged stream outcome into the provider's stream-health window.
    ///
    /// A provider that answers `200` and then drops the body mid-stream is
    /// invisible to the plain failure counter: the status was a success, so
    /// `record_success` resets the streak and the provider keeps every byte of
    /// the traffic. A *consecutive*-failure breaker cannot catch it either —
    /// for a provider that truncates ~40% of its streams independently, three
    /// failures in a row is a ~6% event, so the circuit would essentially
    /// never trip.
    ///
    /// This measures the *ratio* over a short window instead. Once enough
    /// truncations have accumulated and they exceed [`STREAM_BAD_RATIO`], the
    /// circuit is force-opened. Routing skips open circuits, so the route's
    /// traffic moves to the next fallback provider; the provider then gets a
    /// probe window to prove itself again. Consecutive demotions without a
    /// clean window in between escalate the hold
    /// ([`STREAM_DEMOTION_HOLD_SECS`]) so a systematically broken provider does
    /// not flap back into rotation every minute.
    pub fn record_stream_outcome(&self, truncated: bool) {
        let now = now_ms();
        let window_start = self.stream_window_start_ms.load(Ordering::Relaxed);
        if window_start == 0 || now.saturating_sub(window_start) >= STREAM_WINDOW_MS {
            // Racy under concurrency, which is harmless: the loser of the race
            // just restarts the sample, it never miscounts a truncation.
            if self
                .stream_window_start_ms
                .compare_exchange(window_start, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // A window that carried real stream traffic and no truncation
                // is proof the provider recovered: drop the escalation so a
                // later relapse starts again at the short hold.
                let judged = self.stream_judged.load(Ordering::Relaxed);
                let bad = self.stream_bad.load(Ordering::Relaxed);
                if judged >= STREAM_MIN_BAD && bad == 0 {
                    self.stream_demotion_streak.store(0, Ordering::Relaxed);
                }
                self.stream_judged.store(0, Ordering::Relaxed);
                self.stream_bad.store(0, Ordering::Relaxed);
            }
        }
        self.stream_judged.fetch_add(1, Ordering::Relaxed);
        if !truncated {
            return;
        }
        let bad = self.stream_bad.fetch_add(1, Ordering::Relaxed) + 1;
        let judged = self.stream_judged.load(Ordering::Relaxed);
        if bad < STREAM_MIN_BAD || (bad as f64) / (judged.max(1) as f64) < STREAM_BAD_RATIO {
            return;
        }
        let streak = self.stream_demotion_streak.fetch_add(1, Ordering::Relaxed) + 1;
        self.force_open_until_ms
            .store(now + self.demotion_hold_ms(streak), Ordering::Relaxed);
        self.state.store(STATE_OPEN, Ordering::Relaxed);
        self.failures.store(self.max_fails, Ordering::Relaxed);
        self.last_failure_at.store(now, Ordering::Relaxed);
        // The circuit is now open, so no probe slot may stay reserved.
        self.pending_probes.store(0, Ordering::Relaxed);
        self.last_probe_admit_ms.store(0, Ordering::Relaxed);
        self.state_transitions.fetch_add(1, Ordering::Relaxed);
        self.stream_opens.fetch_add(1, Ordering::Relaxed);
        // Start a fresh window: after the cool-down the provider must earn its
        // place back on new samples, not on the ones that demoted it.
        self.stream_judged.store(0, Ordering::Relaxed);
        self.stream_bad.store(0, Ordering::Relaxed);
        self.stream_window_start_ms.store(now, Ordering::Relaxed);
    }

    /// Force-opens caused by a bad stream-truncation ratio.
    pub fn stream_opens(&self) -> u64 {
        self.stream_opens.load(Ordering::Relaxed)
    }

    /// Hold for the `streak`-th consecutive stream-demotion.
    fn demotion_hold_ms(&self, streak: u32) -> u64 {
        let step = usize::try_from(streak.saturating_sub(1))
            .unwrap_or(usize::MAX)
            .min(STREAM_DEMOTION_HOLD_SECS.len() - 1);
        (STREAM_DEMOTION_HOLD_SECS[step] * 1000).max(self.fail_timeout_ms)
    }

    pub fn record_success(&self) {
        match self.state.load(Ordering::Relaxed) {
            STATE_HALF_OPEN => {
                // The probe finished: return its slot (clamped, because the
                // staleness reaper may already have reclaimed it).
                self.release_probe();
                let successes = self.successes_half_open.fetch_add(1, Ordering::Relaxed) + 1;
                if successes >= self.successes_to_close {
                    self.state.store(STATE_CLOSED, Ordering::Relaxed);
                    self.failures.store(0, Ordering::Relaxed);
                    self.successes_half_open.store(0, Ordering::Relaxed);
                    self.pending_probes.store(0, Ordering::Relaxed);
                    self.last_probe_admit_ms.store(0, Ordering::Relaxed);
                    self.state_transitions.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => {
                // Closed or open: a success resets the failure streak. It does
                // not close an open circuit — that still waits for the fail
                // window to elapse before half-open probing.
                self.failures.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Consecutive failures recorded against the breaker. Only the tests
    /// read it directly — production code observes the state, not the count
    /// (since the count is reset to zero once half-open probing starts).
    #[cfg(test)]
    pub fn failure_count(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn state(&self) -> u8 {
        // Normalise: a force-open reads as OPEN even before the state field is
        // consulted (is_open() checks force_open_until_ms first).
        let now = now_ms();
        let force_until = self.force_open_until_ms.load(Ordering::Relaxed);
        if force_until != 0 && now < force_until {
            return STATE_OPEN;
        }
        self.state.load(Ordering::Relaxed)
    }

    /// The state the next routing decision would see, without driving the
    /// state machine (a metrics scrape must not consume a half-open probe
    /// slot). `state()` alone is a lazy snapshot: a breaker whose fail window
    /// elapsed still reads OPEN until some request calls `is_open()`.
    pub fn effective_state(&self) -> u8 {
        let now = now_ms();
        let force_until = self.force_open_until_ms.load(Ordering::Relaxed);
        if force_until != 0 {
            return if now < force_until {
                STATE_OPEN
            } else {
                STATE_CLOSED
            };
        }
        let last = self.last_failure_at.load(Ordering::Relaxed);
        let window_elapsed = now.saturating_sub(last) >= self.fail_timeout_ms;
        match self.state.load(Ordering::Relaxed) {
            STATE_CLOSED => {
                if self.failures.load(Ordering::Relaxed) < self.max_fails {
                    STATE_CLOSED
                } else if window_elapsed {
                    STATE_HALF_OPEN
                } else {
                    STATE_OPEN
                }
            }
            STATE_OPEN => {
                if window_elapsed {
                    STATE_HALF_OPEN
                } else {
                    STATE_OPEN
                }
            }
            other => other,
        }
    }

    pub fn state_transitions(&self) -> u64 {
        self.state_transitions.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn quota_opens(&self) -> u64 {
        self.quota_opens.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn force_open_until_ms(&self) -> u64 {
        self.force_open_until_ms.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn is_force_open(&self) -> bool {
        let force_until = self.force_open_until_ms.load(Ordering::Relaxed);
        force_until != 0 && now_ms() < force_until
    }

    /// Snapshot the breaker's runtime state for persistence.
    ///
    /// The gateway persists a row after every transition / quota force-open so
    /// a restart (or hot reload) keeps an open circuit — otherwise a bounced
    /// gateway would immediately hammer the channel that just failed.
    pub fn snapshot(&self, provider: &str) -> CircuitBreakerStateRow {
        let now = now_ms();
        let force_until = self.force_open_until_ms.load(Ordering::Relaxed);
        let state = if force_until != 0 && now < force_until {
            STATE_OPEN
        } else {
            self.state.load(Ordering::Relaxed)
        };
        CircuitBreakerStateRow {
            provider: provider.to_string(),
            state,
            failures: self.failures.load(Ordering::Relaxed),
            last_failure_at: self.last_failure_at.load(Ordering::Relaxed),
            force_open_until: force_until,
            successes_half_open: self.successes_half_open.load(Ordering::Relaxed),
            transitions: self.state_transitions.load(Ordering::Relaxed),
            quota_opens: self.quota_opens.load(Ordering::Relaxed),
            updated_at: chrono::Utc::now(),
        }
    }

    /// Restore a persisted snapshot into this breaker's atomics.
    ///
    /// Startup only: the breaker is restored before the server accepts
    /// traffic, so a plain `store()` is safe here (no concurrent callers).
    /// `max_fails` / `fail_timeout_ms` are left untouched — they come from the
    /// provider's live config, not the persisted row.
    pub fn restore(&self, row: &CircuitBreakerStateRow) {
        self.failures.store(row.failures, Ordering::Relaxed);
        self.successes_half_open
            .store(row.successes_half_open, Ordering::Relaxed);
        self.pending_probes.store(0, Ordering::Relaxed);
        self.last_probe_admit_ms.store(0, Ordering::Relaxed);
        self.last_failure_at
            .store(row.last_failure_at, Ordering::Relaxed);
        self.state.store(row.state, Ordering::Relaxed);
        self.force_open_until_ms
            .store(row.force_open_until, Ordering::Relaxed);
        self.state_transitions
            .store(row.transitions, Ordering::Relaxed);
        self.quota_opens.store(row.quota_opens, Ordering::Relaxed);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starts_closed() {
        let cb = CircuitBreaker::new(3, 60);
        assert_eq!(cb.state(), STATE_CLOSED);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_opens_after_max_fails() {
        let cb = CircuitBreaker::new(3, 60);
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
        assert_eq!(cb.state(), STATE_OPEN);
    }

    #[test]
    fn test_success_resets_failure_streak() {
        let cb = CircuitBreaker::new(2, 60);
        cb.record_failure();
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
        // A success resets the failure streak, but an open circuit stays open
        // until the fail window elapses — it then half-open probes before
        // closing. (Requests are not routed to an open provider anyway, so a
        // success only arrives as a half-open probe.)
        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
        assert!(cb.is_open());
    }

    #[test]
    fn test_half_open_after_timeout() {
        let cb = CircuitBreaker::new(1, 1); // 1s fail window
        cb.record_failure();
        // Still inside the fail window → open.
        assert!(cb.is_open());
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // Window elapsed → half-open, probe admitted.
        assert!(!cb.is_open());
        assert_eq!(cb.state(), STATE_HALF_OPEN);
    }

    #[test]
    fn test_half_open_probe_success_closes() {
        let cb = CircuitBreaker::new(1, 1);
        cb.record_failure();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // Half-open admits a probe
        assert!(!cb.is_open());
        cb.record_success();
        assert_eq!(cb.state(), STATE_CLOSED);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_half_open_probe_failure_reopens() {
        let cb = CircuitBreaker::new(1, 1);
        cb.record_failure();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!cb.is_open()); // probe admitted
        cb.record_http_failure(503, None);
        // Probe failed → back to open, still inside the fresh fail window.
        assert!(cb.is_open());
        assert_eq!(cb.state(), STATE_OPEN);
    }

    #[test]
    fn test_quota_429_force_opens() {
        let cb = CircuitBreaker::new(3, 60);
        // No failures yet, but a 429 force-opens immediately
        cb.record_http_failure(429, Some(30));
        assert!(cb.is_open());
        assert_eq!(cb.state(), STATE_OPEN);
        assert!(cb.is_force_open());
        assert_eq!(cb.quota_opens(), 1);
        // Even a success does not close a force-open breaker until cooldown
        cb.record_success();
        assert!(cb.is_open());
    }

    #[test]
    fn test_quota_cooldown_expiry_clears() {
        let cb = CircuitBreaker::new(3, 1); // default cooldown fallback
        cb.record_http_failure(429, Some(1));
        assert!(cb.is_open());
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!cb.is_open());
        assert_eq!(cb.state(), STATE_CLOSED);
    }

    #[test]
    fn test_snapshot_restore_round_trip() {
        let cb = CircuitBreaker::new(3, 60);
        cb.record_http_failure(429, Some(30));
        let snap = cb.snapshot("provider-a");

        assert_eq!(snap.provider, "provider-a");
        assert_eq!(snap.state, STATE_OPEN);
        assert_eq!(snap.quota_opens, 1);
        assert!(snap.force_open_until > 0);
        assert_eq!(snap.failures, 3);

        // A fresh breaker is closed; after restoring the snapshot it is open
        // again — this is what keeps a 429 force-open alive across a restart.
        let restored = CircuitBreaker::new(3, 60);
        assert!(!restored.is_open());
        restored.restore(&snap);
        assert!(restored.is_open());
        assert_eq!(restored.state(), STATE_OPEN);
        assert_eq!(restored.quota_opens(), 1);
        assert_eq!(restored.failure_count(), 3);
    }

    #[test]
    fn test_half_open_limits_concurrent_probes() {
        let cb = CircuitBreaker::new_with_opts(1, 1, 1, 1);
        cb.record_failure();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // First probe admitted (it occupies the single probe slot)
        assert!(!cb.is_open());
        // Second concurrent probe blocked while the first is in flight
        assert!(cb.is_open());
        cb.record_success();
        assert!(!cb.is_open());
    }

    /// Regression for the 2026-11-08 pool-health outage: merely *inspecting*
    /// a provider must not consume its half-open probe slot.
    ///
    /// The routing path walks every candidate of a route / every entry of the
    /// auto pool and only ever picks one. When the filter called `is_open()`,
    /// each inspected provider transitioned Closed→HalfOpen and reserved its
    /// single probe slot for a request that never came — sixteen of the
    /// twenty-five pool members ended up permanently half-open and answered
    /// `503 circuit breakers open` until the gateway was restarted.
    #[test]
    fn test_inspection_does_not_consume_a_probe_slot() {
        let cb = CircuitBreaker::new(1, 1);
        cb.record_failure();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // The fail window has elapsed: the next request may probe.
        assert_eq!(cb.effective_state(), STATE_HALF_OPEN);

        // Twenty-five read-only inspections (one per pool member) must leave
        // the slot free.
        for _ in 0..25 {
            assert!(cb.allows_traffic(), "half-open providers stay eligible");
        }

        // The provider is still admitted when the request actually picks it.
        assert!(
            !cb.is_open(),
            "25 inspections must not have reserved the probe slot"
        );
        // Admitted → the single slot is occupied; a second probe is refused.
        assert!(cb.is_open(), "half_open_max=1 must bound concurrent probes");
        // Releasing the slot (what the request's terminal path does) frees it.
        cb.release_probe();
        assert!(!cb.is_open(), "a released slot must admit the next probe");
    }

    /// A request that ends without booking an outcome (client abort, a 4xx
    /// that fails over, a health probe) must not leave its provider blocked
    /// for ever: a slot held for longer than two fail windows is reclaimed.
    #[test]
    fn test_a_stale_probe_slot_is_reclaimed() {
        let cb = CircuitBreaker::new(1, 1);
        cb.record_failure();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!cb.is_open(), "first probe admitted");
        // Pretend the request that held the slot vanished: backdate the
        // admission stamp beyond the staleness threshold (two fail windows,
        // floored at 120 s).
        cb.last_probe_admit_ms
            .store(now_ms().saturating_sub(200_000), Ordering::Relaxed);
        assert!(
            cb.allows_traffic(),
            "an expired slot must not keep the provider blocked"
        );
        assert!(
            !cb.is_open(),
            "the leaked slot must have been reclaimed, admitting a new probe"
        );
    }

    /// A provider that truncates a large share of its streams must be demoted,
    /// even though every one of those streams answered 200 (so the plain
    /// failure counter never saw a failure).
    #[test]
    fn test_a_bad_stream_ratio_force_opens_the_circuit() {
        let cb = CircuitBreaker::new(3, 60);
        // 50% truncation: 4 good + 4 truncated. The ratio trips on the 8th
        // sample (4/8 = 0.5 >= 0.25) but only after `STREAM_MIN_BAD`.
        for _ in 0..4 {
            cb.record_stream_outcome(false);
        }
        assert!(!cb.is_open(), "a healthy prefix must not demote");
        for _ in 0..3 {
            cb.record_stream_outcome(true);
        }
        assert!(
            !cb.is_open(),
            "{} truncations alone are not enough evidence",
            STREAM_MIN_BAD - 1
        );
        cb.record_stream_outcome(true);
        assert!(
            cb.is_open(),
            "4 truncations in 8 judged streams must demote"
        );
        assert_eq!(cb.stream_opens(), 1, "the demotion is counted once");
    }

    fn hold_secs(cb: &CircuitBreaker) -> u64 {
        (cb.force_open_until_ms
            .load(Ordering::Relaxed)
            .saturating_sub(now_ms())
            + 500)
            / 1000
    }

    /// A provider that keeps truncating after its cool-down must be put away
    /// for longer each time instead of flapping back in every 60 seconds.
    #[test]
    fn test_repeated_stream_demotions_escalate_the_hold() {
        let cb = CircuitBreaker::new(3, 60);
        let mut holds = Vec::new();
        for expected_opens in 1..=4u64 {
            for _ in 0..STREAM_MIN_BAD {
                cb.record_stream_outcome(true);
            }
            assert_eq!(cb.stream_opens(), expected_opens);
            holds.push(hold_secs(&cb));
            // Force the next burst to start from an empty window, exactly as
            // it would after the hold expires and traffic returns.
            cb.stream_window_start_ms.store(0, Ordering::Relaxed);
            cb.stream_judged.store(0, Ordering::Relaxed);
            cb.stream_bad.store(0, Ordering::Relaxed);
        }
        assert_eq!(holds, STREAM_DEMOTION_HOLD_SECS.to_vec());
    }

    /// Serving a clean window proves recovery: the ladder restarts at the
    /// short hold instead of carrying the old escalation forever.
    #[test]
    fn test_a_clean_window_resets_the_demotion_hold() {
        let cb = CircuitBreaker::new(3, 60);
        for _ in 0..STREAM_MIN_BAD {
            cb.record_stream_outcome(true);
        }
        assert_eq!(cb.stream_opens(), 1);
        assert_eq!(hold_secs(&cb), STREAM_DEMOTION_HOLD_SECS[0]);

        // A full window of real traffic with zero truncations.
        for _ in 0..STREAM_MIN_BAD {
            cb.record_stream_outcome(false);
        }
        // Roll the window: the roll observes judged=4, bad=0 and resets the
        // streak. This sample opens the next window.
        cb.stream_window_start_ms.store(0, Ordering::Relaxed);
        cb.record_stream_outcome(false);

        // Relapse: the hold is back to the first step.
        for _ in 0..STREAM_MIN_BAD {
            cb.record_stream_outcome(true);
        }
        assert_eq!(cb.stream_opens(), 2);
        assert_eq!(hold_secs(&cb), STREAM_DEMOTION_HOLD_SECS[0]);
    }

    /// One truncation among many good streams is a flake, not a bad provider.
    #[test]
    fn test_a_single_truncation_does_not_demote() {
        let cb = CircuitBreaker::new(3, 60);
        for i in 0..50 {
            cb.record_stream_outcome(i == 7);
        }
        assert!(!cb.is_open());
        assert_eq!(cb.stream_opens(), 0);
    }
}
