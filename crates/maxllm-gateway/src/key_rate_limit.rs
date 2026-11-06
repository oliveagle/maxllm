// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Per-key rate limiting for virtual keys (RPM / TPM / concurrency).
//!
//! Each virtual key may carry an RPM limit, a TPM limit and a concurrency
//! limit. The counters are process-local, keyed by the virtual key id, with
//! fixed 60-second sliding windows (the same `pingora_limits::rate::Rate`
//! the `rate_limit` plugin uses). The scope matches the budget enforcer:
//! a single gateway process enforces its own per-key limits.
//!
//! - **RPM** is consumed at request time: each arriving request observes the
//!   window and is denied once the window is full.
//! - **TPM** cannot be known until the response arrives. The tokens already
//!   in the window are peeked at request time (a key whose window is already
//!   over its TPM limit is denied), and the actual tokens are recorded when
//!   the response completes.
//! - **Concurrency** (`max_parallel_requests`) is a count of in-flight
//!   requests: a slot is taken at request time and released when the request
//!   context is dropped, so every exit path (including the early responses
//!   that skip the logging hook) releases it.

use pingora_limits::rate::Rate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Why a request was denied by a per-key limit. The shared `Limit` suffix is
/// meaningful (each variant names the kind of limit hit), so the lint is
/// allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum KeyRateError {
    RpmLimit { limit: u32 },
    TpmLimit { limit: u64 },
    ConcurrencyLimit { limit: u32 },
}

/// Sliding-window per-key rate limiter.
pub struct KeyRateLimiter {
    requests: Rate,
    tokens: Rate,
    in_flight: Mutex<HashMap<String, u32>>,
}

impl KeyRateLimiter {
    pub fn new() -> Self {
        Self {
            requests: Rate::new(Duration::from_secs(60)),
            tokens: Rate::new(Duration::from_secs(60)),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Observe one request against the key's RPM limit. Returns `Err` when
    /// the sliding window is already full. A denied request still counts in
    /// the window (matching the `rate_limit` plugin).
    pub fn check_request(&self, key_id: &str, rpm_limit: Option<u32>) -> Result<(), KeyRateError> {
        if let Some(limit) = rpm_limit {
            let current = self.requests.observe(&key_id.to_string(), 1);
            if current > limit as isize {
                return Err(KeyRateError::RpmLimit { limit });
            }
        }
        Ok(())
    }

    /// Peek the tokens already consumed in the current window without
    /// consuming anything — the actual tokens are recorded by
    /// [`Self::record_tokens`] when the response completes.
    pub fn check_tokens(&self, key_id: &str, tpm_limit: Option<u64>) -> Result<(), KeyRateError> {
        if let Some(limit) = tpm_limit {
            let current = self.tokens.observe(&key_id.to_string(), 0);
            if current >= limit as isize {
                return Err(KeyRateError::TpmLimit { limit });
            }
        }
        Ok(())
    }

    /// Record the tokens consumed by a completed request.
    pub fn record_tokens(&self, key_id: &str, tokens: u64) {
        if tokens > 0 {
            self.tokens.observe(&key_id.to_string(), tokens as isize);
        }
    }

    /// Take an in-flight slot for the key. Returns `Err` when the key is
    /// already at its concurrency limit.
    pub fn acquire(&self, key_id: &str, max_parallel: Option<u32>) -> Result<(), KeyRateError> {
        if let Some(limit) = max_parallel {
            let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let entry = map.entry(key_id.to_string()).or_insert(0);
            if *entry >= limit {
                return Err(KeyRateError::ConcurrencyLimit { limit });
            }
            *entry += 1;
        }
        Ok(())
    }

    /// Release an in-flight slot.
    pub fn release(&self, key_id: &str) {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get_mut(key_id) {
            *entry = entry.saturating_sub(1);
        }
    }
}

/// An in-flight slot guard. Dropping it releases the slot, so the
/// concurrency counter is decremented on every exit path.
pub struct KeyInFlightGuard {
    limiter: Arc<KeyRateLimiter>,
    key_id: String,
}

impl KeyInFlightGuard {
    pub fn new(limiter: Arc<KeyRateLimiter>, key_id: String) -> Self {
        Self { limiter, key_id }
    }
}

impl Drop for KeyInFlightGuard {
    fn drop(&mut self) {
        self.limiter.release(&self.key_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter() -> KeyRateLimiter {
        KeyRateLimiter::new()
    }

    #[test]
    fn rpm_limit_blocks_when_window_full() {
        let l = limiter();
        assert!(l.check_request("k1", Some(2)).is_ok());
        assert!(l.check_request("k1", Some(2)).is_ok());
        assert_eq!(
            l.check_request("k1", Some(2)),
            Err(KeyRateError::RpmLimit { limit: 2 })
        );
        // A different key is independent.
        assert!(l.check_request("k2", Some(2)).is_ok());
    }

    #[test]
    fn tpm_limit_peeks_then_records() {
        let l = limiter();
        // No limit -> always allowed.
        assert!(l.check_tokens("k1", None).is_ok());
        // Window empty -> allowed.
        assert!(l.check_tokens("k1", Some(10)).is_ok());
        // Record 12 tokens; the next request sees the window already full.
        l.record_tokens("k1", 12);
        assert_eq!(
            l.check_tokens("k1", Some(10)),
            Err(KeyRateError::TpmLimit { limit: 10 })
        );
        // A higher limit admits the same window.
        assert!(l.check_tokens("k1", Some(20)).is_ok());
    }

    #[test]
    fn concurrency_limit_acquire_release() {
        let l = limiter();
        assert!(l.acquire("k1", Some(2)).is_ok());
        assert!(l.acquire("k1", Some(2)).is_ok());
        assert_eq!(
            l.acquire("k1", Some(2)),
            Err(KeyRateError::ConcurrencyLimit { limit: 2 })
        );
        l.release("k1");
        assert!(l.acquire("k1", Some(2)).is_ok());
    }

    #[test]
    fn guard_releases_on_drop() {
        let l = Arc::new(limiter());
        {
            let guard = KeyInFlightGuard::new(Arc::clone(&l), "k1".to_string());
            let _ = l.acquire("k1", Some(1));
            assert_eq!(
                l.acquire("k1", Some(1)),
                Err(KeyRateError::ConcurrencyLimit { limit: 1 })
            );
            drop(guard);
        }
        assert!(l.acquire("k1", Some(1)).is_ok());
    }
}
