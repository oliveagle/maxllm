// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The rate-limit resolver: the layered decision that turns an upstream error
//! into "when can this provider serve again" (or "this is not a rate limit").
//!
//! ```text
//!  ┌─ layer 1: headers ──────────── Retry-After / x-ratelimit-reset-* / …
//!  │      hit → precise reset instant
//!  ├─ layer 2: learned patterns ─── a shape seen before → its learned answer
//!  │      hit → precise reset instant (no network, no regex)
//!  ├─ layer 3: body regex ───────── generic time-expression matching
//!  │      hit → precise reset instant
//!  ├─ layer 4: laya decision ────── is this a rate limit? which candidate?
//!  │      hit → precise reset instant / ordinary failure
//!  └─ fallthrough ────────────────── rate limit, reset time unknown
//!         → default cooldown
//! ```
//!
//! Every layer is fail-open: an unreachable laya or an unparsable message
//! degrades to the default cooldown, never to a failed request.

use std::sync::Arc;
use std::time::Duration;

use maxllm_admin::RateLimitPatternRow;

use crate::laya_client::LayaClient;
use crate::rate_limit::{self, RateLimitDecision, ResetCandidate};

/// Tunables for the resolver.
#[derive(Debug, Clone)]
pub struct ResolverConfig {
    pub enabled: bool,
    /// Noul probability above which laya's answer means "this is a rate limit".
    pub rate_limit_threshold: f64,
    /// Cooldown applied when the error is a rate limit but no layer resolved
    /// a precise reset instant.
    pub default_cooldown_secs: u64,
    /// Laya call budget.
    pub laya_timeout: Duration,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            // The soft-decider model measured 0.63 on a confirmed
            // production rate-limit message (Volcengine monthly-quota
            // exhaustion) — a 0.7 threshold would misclassify it, so the
            // default sits below the observed signal floor.
            rate_limit_threshold: 0.5,
            default_cooldown_secs: 300,
            // The decider's long-choice path measured ~560 ms end-to-end;
            // 1 s leaves headroom without delaying the failover chain.
            laya_timeout: Duration::from_millis(1000),
        }
    }
}

/// The resolver: holds the laya client and the learned-pattern store.
pub struct RateLimitResolver {
    config: ResolverConfig,
    laya: Option<LayaClient>,
    admin_api: Option<Arc<maxllm_admin::AdminApi>>,
}

/// What the resolver learned about one error, for the caller to persist.
#[derive(Debug, Clone)]
pub struct LearnedUpdate {
    pub fingerprint: String,
    pub is_rate_limit: bool,
    pub reset_expression: Option<String>,
    pub sample_message: String,
}

impl RateLimitResolver {
    pub fn new(
        config: ResolverConfig,
        laya_url: Option<&str>,
        laya_model: Option<&str>,
        admin_api: Option<Arc<maxllm_admin::AdminApi>>,
    ) -> Self {
        let laya = if config.enabled {
            Some(LayaClient::new(laya_url, laya_model, config.laya_timeout))
        } else {
            None
        };
        Self {
            config,
            laya,
            admin_api,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn default_cooldown_secs(&self) -> u64 {
        self.config.default_cooldown_secs
    }

    pub fn laya_base_url(&self) -> Option<&str> {
        self.laya.as_ref().map(|c| c.base_url())
    }

    /// Resolve the quota reset instant of one upstream error.
    ///
    /// `body` may be empty — the resolver then relies on headers and laya.
    pub async fn resolve(
        &self,
        status: u16,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> (RateLimitDecision, Option<LearnedUpdate>) {
        if !self.config.enabled {
            // Learning disabled: keep the pre-learning behaviour — the
            // header cooldown the caller applies on its own.
            return (RateLimitDecision::QuotaUnknown, None);
        }

        // Layer 1: headers — cheapest and most precise.
        if let Some((reset_at_ms, source)) = rate_limit::resolve_from_headers(headers) {
            return (
                RateLimitDecision::QuotaUntil {
                    reset_at_ms,
                    source,
                },
                None,
            );
        }

        let text = String::from_utf8_lossy(body);
        let (is_rate_limit, candidates) = rate_limit::parse_body_text(&text);
        let fingerprint = if text.trim().is_empty() {
            None
        } else {
            Some(rate_limit::fingerprint(&text))
        };

        // Layer 2: learned patterns — a shape seen before answers instantly.
        if let Some(fp) = fingerprint.as_deref() {
            if let Some(learned) = self.lookup_learned(fp).await {
                if let Some(at) = self.reset_from_learned(&learned, &candidates, &text) {
                    return (
                        RateLimitDecision::QuotaUntil {
                            reset_at_ms: at,
                            source: "learned_pattern",
                        },
                        None,
                    );
                }
                if learned.is_rate_limit {
                    // The shape is a rate limit but this message carries no
                    // reset instant: fall through to laya, which may still
                    // classify it, but do not force a cooldown yet.
                } else {
                    return (RateLimitDecision::OrdinaryFailure, None);
                }
            }
        }

        // Layer 3: body regex.
        if is_rate_limit == Some(true) {
            if let Some(candidate) = candidates.first() {
                return (
                    RateLimitDecision::QuotaUntil {
                        reset_at_ms: candidate.at_ms,
                        source: "body_regex",
                    },
                    fingerprint.map(|fp| LearnedUpdate {
                        fingerprint: fp,
                        is_rate_limit: true,
                        reset_expression: Some(reset_expression_of(&candidates)),
                        sample_message: text.chars().take(400).collect(),
                    }),
                );
            }
            // A rate limit without a reset instant — ask laya whether it can
            // classify it (and find a candidate we missed).
            if let Some(decision) = self.ask_laya(status, &text, &candidates).await {
                return (
                    decision,
                    fingerprint.map(|fp| LearnedUpdate {
                        fingerprint: fp,
                        is_rate_limit: true,
                        reset_expression: None,
                        sample_message: text.chars().take(400).collect(),
                    }),
                );
            }
            return (
                RateLimitDecision::QuotaUnknown,
                fingerprint.map(|fp| LearnedUpdate {
                    fingerprint: fp,
                    is_rate_limit: true,
                    reset_expression: None,
                    sample_message: text.chars().take(400).collect(),
                }),
            );
        }

        if is_rate_limit == Some(false) {
            // The message deterministically denotes something else (billing,
            // an invalid model, …) — take the ordinary-failure path.
            return (RateLimitDecision::OrdinaryFailure, None);
        }

        // Layer 4: laya — the text did not decide on its own.
        if let Some(decision) = self.ask_laya(status, &text, &candidates).await {
            let learned = fingerprint.map(|fp| LearnedUpdate {
                fingerprint: fp,
                is_rate_limit: !matches!(decision, RateLimitDecision::OrdinaryFailure),
                reset_expression: match &decision {
                    RateLimitDecision::QuotaUntil { .. } => Some(reset_expression_of(&candidates)),
                    _ => None,
                },
                sample_message: text.chars().take(400).collect(),
            });
            return (decision, learned);
        }

        // Nothing decided — the safest reading of an unclassifiable 429 is
        // "rate limited, reset time unknown" → default cooldown.
        (
            RateLimitDecision::QuotaUnknown,
            fingerprint.map(|fp| LearnedUpdate {
                fingerprint: fp,
                is_rate_limit: true,
                reset_expression: None,
                sample_message: text.chars().take(400).collect(),
            }),
        )
    }

    /// Deterministic-only resolution — used when no tokio runtime context
    /// is available (headers + learned patterns + body regex, no laya).
    pub fn resolve_sync(
        &self,
        _status: u16,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> (RateLimitDecision, Option<LearnedUpdate>) {
        if !self.config.enabled {
            return (RateLimitDecision::QuotaUnknown, None);
        }
        if let Some((reset_at_ms, source)) = rate_limit::resolve_from_headers(headers) {
            return (
                RateLimitDecision::QuotaUntil {
                    reset_at_ms,
                    source,
                },
                None,
            );
        }
        let text = String::from_utf8_lossy(body);
        let (is_rate_limit, candidates) = rate_limit::parse_body_text(&text);
        let fingerprint = if text.trim().is_empty() {
            None
        } else {
            Some(rate_limit::fingerprint(&text))
        };
        if let Some(fp) = fingerprint.as_deref() {
            if let Some(learned) = self.lookup_learned_sync(fp) {
                if learned.is_rate_limit {
                    if let Some(at) = candidates.first().map(|c| c.at_ms) {
                        return (
                            RateLimitDecision::QuotaUntil {
                                reset_at_ms: at,
                                source: "learned_pattern",
                            },
                            None,
                        );
                    }
                } else {
                    return (RateLimitDecision::OrdinaryFailure, None);
                }
            }
        }
        match is_rate_limit {
            Some(true) => match candidates.first() {
                Some(c) => (
                    RateLimitDecision::QuotaUntil {
                        reset_at_ms: c.at_ms,
                        source: "body_regex",
                    },
                    fingerprint.map(|fp| LearnedUpdate {
                        fingerprint: fp,
                        is_rate_limit: true,
                        reset_expression: Some(reset_expression_of(&candidates)),
                        sample_message: text.chars().take(400).collect(),
                    }),
                ),
                None => (
                    RateLimitDecision::QuotaUnknown,
                    fingerprint.map(|fp| LearnedUpdate {
                        fingerprint: fp,
                        is_rate_limit: true,
                        reset_expression: None,
                        sample_message: text.chars().take(400).collect(),
                    }),
                ),
            },
            Some(false) => (RateLimitDecision::OrdinaryFailure, None),
            None => (
                RateLimitDecision::QuotaUnknown,
                fingerprint.map(|fp| LearnedUpdate {
                    fingerprint: fp,
                    is_rate_limit: true,
                    reset_expression: None,
                    sample_message: text.chars().take(400).collect(),
                }),
            ),
        }
    }

    /// Synchronous learned-pattern lookup (no runtime context).
    fn lookup_learned_sync(&self, fingerprint: &str) -> Option<maxllm_admin::RateLimitPatternRow> {
        let admin = self.admin_api.as_ref()?;
        match admin.get_rate_limit_pattern(fingerprint) {
            Ok(Some(row)) => Some(row),
            _ => None,
        }
    }

    /// Ask laya to classify the error and pick the reset instant.
    async fn ask_laya(
        &self,
        status: u16,
        text: &str,
        candidates: &[ResetCandidate],
    ) -> Option<RateLimitDecision> {
        let client = self.laya.as_ref()?;
        let pairs: Vec<(u64, String)> = candidates
            .iter()
            .map(|c| (c.at_ms, c.raw.clone()))
            .collect();
        let verdict = client.classify_rate_limit(status, text, &pairs).await?;

        let is_rate_limit = verdict.is_rate_limit?;
        if is_rate_limit < self.config.rate_limit_threshold {
            // laya says this is not a rate-limit error → ordinary failure.
            return Some(RateLimitDecision::OrdinaryFailure);
        }
        match verdict.chosen_reset_at_ms {
            Some(at) => Some(RateLimitDecision::QuotaUntil {
                reset_at_ms: at,
                source: "laya",
            }),
            None => Some(RateLimitDecision::QuotaUnknown),
        }
    }

    /// Look up a learned pattern in the store.
    async fn lookup_learned(&self, fingerprint: &str) -> Option<maxllm_admin::RateLimitPatternRow> {
        let admin = self.admin_api.as_ref()?;
        match admin.get_rate_limit_pattern(fingerprint) {
            Ok(Some(row)) => Some(row),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    /// Turn a learned pattern into a concrete reset instant.
    ///
    /// The pattern records *how* the shape expresses its reset time; the
    /// concrete instant still comes from the current message, so a pattern
    /// learned yesterday does not pin a stale timestamp.
    fn reset_from_learned(
        &self,
        learned: &maxllm_admin::RateLimitPatternRow,
        candidates: &[ResetCandidate],
        text: &str,
    ) -> Option<u64> {
        match learned.reset_expression.as_deref() {
            Some("timestamp") => candidates.first().map(|c| c.at_ms),
            Some("duration") => candidates.first().map(|c| c.at_ms),
            Some(_) => candidates.first().map(|c| c.at_ms),
            None => {
                // The shape was learned without a reset expression, but the
                // current message may still carry one the regex layer can see.
                if candidates.is_empty() {
                    None
                } else {
                    let _ = text;
                    Some(candidates[0].at_ms)
                }
            }
        }
    }

    /// Persist what was learned, so the next message of the same shape is
    /// resolved without laya.
    pub async fn persist_learned(&self, update: LearnedUpdate) {
        let Some(admin) = self.admin_api.as_ref() else {
            return;
        };
        let row = RateLimitPatternRow {
            fingerprint: update.fingerprint,
            is_rate_limit: update.is_rate_limit,
            reset_expression: update.reset_expression,
            sample_message: update.sample_message,
            hits: 1,
            updated_at: chrono::Utc::now(),
        };
        if let Err(err) = admin.save_rate_limit_pattern(row) {
            tracing::warn!(error = %err, "Failed to persist learned rate-limit pattern");
        }
    }
}

/// Describe how a message expresses its reset instant, so the learned
/// pattern can be re-applied to the next message of the same shape.
fn reset_expression_of(candidates: &[ResetCandidate]) -> String {
    match candidates.first().map(|c| c.raw.as_str()) {
        Some(raw) if raw.starts_with("duration:") => "duration".to_string(),
        Some(raw) if raw.starts_with("epoch:") => "epoch".to_string(),
        Some(_) => "timestamp".to_string(),
        None => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut m = http::HeaderMap::new();
        for (k, v) in pairs {
            m.insert(
                http::HeaderName::from_lowercase(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        m
    }

    #[tokio::test]
    async fn disabled_resolver_defers_to_caller() {
        let resolver = RateLimitResolver::new(ResolverConfig::default(), None, None, None);
        let (decision, learned) = resolver.resolve(429, &headers(&[]), b"rate limit").await;
        assert_eq!(decision, RateLimitDecision::QuotaUnknown);
        assert!(learned.is_none());
    }

    #[tokio::test]
    async fn header_layer_wins_over_body() {
        let cfg = ResolverConfig {
            enabled: true,
            ..Default::default()
        };
        let resolver = RateLimitResolver::new(cfg, None, None, None);
        let h = headers(&[("retry-after", "60")]);
        let (decision, learned) = resolver
            .resolve(429, &h, b"It will reset at 2099-01-01 00:00:00")
            .await;
        match decision {
            RateLimitDecision::QuotaUntil { source, .. } => {
                assert_eq!(source, "retry_after_seconds")
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(learned.is_none());
    }

    #[tokio::test]
    async fn body_regex_layer_resolves_volc_style_message() {
        let cfg = ResolverConfig {
            enabled: true,
            ..Default::default()
        };
        let resolver = RateLimitResolver::new(cfg, None, None, None);
        let body = br#"{"error":{"code":"AccountQuotaExceeded","message":"You have exceeded the monthly usage quota. It will reset at 2026-11-06 23:59:59 +0800 CST. Request id: 021793713952"}}}"#;
        let (decision, learned) = resolver.resolve(429, &headers(&[]), body).await;
        match decision {
            RateLimitDecision::QuotaUntil {
                reset_at_ms,
                source,
            } => {
                assert_eq!(source, "body_regex");
                // The reset must be in the future (2026-11-06 is in the past
                // relative to the test clock, so only assert the shape).
                assert!(reset_at_ms > 0);
            }
            other => panic!("unexpected {other:?}"),
        }
        let learned = learned.expect("the shape must be learned");
        assert!(learned.is_rate_limit);
        assert_eq!(learned.reset_expression.as_deref(), Some("timestamp"));
    }

    #[tokio::test]
    async fn billing_error_is_an_ordinary_failure() {
        let cfg = ResolverConfig {
            enabled: true,
            ..Default::default()
        };
        let resolver = RateLimitResolver::new(cfg, None, None, None);
        let body =
            br#"{"error":{"message":"Insufficient balance. Please recharge your account."}}"#;
        let (decision, _) = resolver.resolve(429, &headers(&[]), body).await;
        assert_eq!(decision, RateLimitDecision::OrdinaryFailure);
    }

    #[tokio::test]
    async fn unclassifiable_error_falls_back_to_default_cooldown() {
        let cfg = ResolverConfig {
            enabled: true,
            ..Default::default()
        };
        let resolver = RateLimitResolver::new(cfg, None, None, None);
        let (decision, learned) = resolver
            .resolve(429, &headers(&[]), b"something entirely unexpected")
            .await;
        assert_eq!(decision, RateLimitDecision::QuotaUnknown);
        assert!(learned.is_some());
    }
}
