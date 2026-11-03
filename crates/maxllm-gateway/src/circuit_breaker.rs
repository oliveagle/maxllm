// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
}

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
            last_failure_at: AtomicU64::new(0),
            max_fails: max_fails.max(1),
            fail_timeout_ms: fail_timeout_secs * 1000,
            half_open_max: half_open_max.max(1),
            successes_to_close: successes_to_close.max(1),
            state: AtomicU8::new(STATE_CLOSED),
            force_open_until_ms: AtomicU64::new(0),
            state_transitions: AtomicU64::new(0),
            quota_opens: AtomicU64::new(0),
        }
    }

    /// Returns true if the circuit is open (provider should be skipped).
    ///
    /// This doubles as the state machine driver: called from the routing path,
    /// it advances Open→HalfOpen when the fail window elapses and HalfOpen→
    /// Closed once enough probes succeeded. All transitions are atomic and
    /// idempotent, so concurrent callers cannot corrupt the breaker.
    pub fn is_open(&self) -> bool {
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
                // Fail window elapsed → half-open. This caller becomes the
                // first probe request and occupies one probe slot.
                self.state.store(STATE_HALF_OPEN, Ordering::Relaxed);
                self.successes_half_open.store(0, Ordering::Relaxed);
                self.state_transitions.fetch_add(1, Ordering::Relaxed);
                let admitted = self.pending_probes.fetch_add(1, Ordering::Relaxed);
                admitted >= self.half_open_max
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
                    let admitted = self.pending_probes.fetch_add(1, Ordering::Relaxed);
                    return admitted >= self.half_open_max;
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
            self.state_transitions.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let admitted = self.pending_probes.fetch_add(1, Ordering::Relaxed);
        if admitted >= self.half_open_max {
            self.pending_probes.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
        false
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
            self.quota_opens.fetch_add(1, Ordering::Relaxed);
            self.state_transitions.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match self.state.load(Ordering::Relaxed) {
            STATE_HALF_OPEN => {
                // A probe failed → back to open.
                self.pending_probes.fetch_sub(1, Ordering::Relaxed);
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
    pub fn record_success(&self) {
        match self.state.load(Ordering::Relaxed) {
            STATE_HALF_OPEN => {
                self.pending_probes.fetch_sub(1, Ordering::Relaxed);
                let successes = self.successes_half_open.fetch_add(1, Ordering::Relaxed) + 1;
                if successes >= self.successes_to_close {
                    self.state.store(STATE_CLOSED, Ordering::Relaxed);
                    self.failures.store(0, Ordering::Relaxed);
                    self.successes_half_open.store(0, Ordering::Relaxed);
                    self.pending_probes.store(0, Ordering::Relaxed);
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
}
