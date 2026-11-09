// Copyright 2024 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! Data models for the admin subsystem: virtual keys, teams, spend records,
//! cost definitions, and API request/response types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Type alias — use f64 for now; swap to rust_decimal::Decimal when precision matters.
type Decimal = f64;

// ---------------------------------------------------------------------------
// Core domain models
// ---------------------------------------------------------------------------

/// A virtual API key that proxies requests through the gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualKey {
    /// Unique identifier (UUID v4).
    pub id: String,
    /// SHA-256 hex digest of the raw key string.
    pub key_hash: String,
    /// First 12 characters of the raw key for display (e.g. `sk-maxllm-ab`).
    pub key_prefix: String,
    /// Human-readable label.
    pub name: String,
    /// Optional team association.
    pub team_id: Option<String>,
    /// Model allow-list. Empty means all models are permitted.
    pub allowed_models: Vec<String>,
    /// Maximum spend in USD before the key is blocked.
    pub max_budget_usd: Option<Decimal>,
    /// Rolling budget window in days. `None` = lifetime budget.
    pub budget_reset_days: Option<u32>,
    /// Accumulated spend in the current budget window.
    pub budget_spent_usd: Decimal,
    /// When the current budget window expires.
    pub budget_reset_at: Option<DateTime<Utc>>,
    /// Requests-per-minute limit.
    pub rpm_limit: Option<u32>,
    /// Tokens-per-minute limit.
    pub tpm_limit: Option<u64>,
    /// Maximum concurrent in-flight requests.
    pub max_parallel_requests: Option<u32>,
    /// Hard expiration timestamp.
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub is_active: bool,
    /// Arbitrary user-defined metadata.
    pub metadata: serde_json::Map<String, serde_json::Value>,
    // Lifetime counters
    pub total_requests: u64,
    pub total_tokens_in: u64,
    pub total_tokens_out: u64,
    pub total_spend_usd: Decimal,
}

/// A team groups virtual keys together and can enforce a shared budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    pub id: String,
    pub name: String,
    pub max_budget_usd: Option<Decimal>,
    pub budget_spent_usd: Decimal,
    /// Key IDs that belong to this team.
    pub members: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// A single spend event tied to one LLM request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendRecord {
    pub id: String,
    pub key_id: String,
    pub team_id: Option<String>,
    pub model: String,
    pub provider: String,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_usd: Decimal,
    pub request_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// Per-model pricing definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCost {
    /// Exact model name or glob pattern (e.g. `gpt-4o*`).
    pub model_pattern: String,
    /// USD per 1 000 000 input tokens.
    pub input_cost_per_1m: Decimal,
    /// USD per 1 000 000 output tokens.
    pub output_cost_per_1m: Decimal,
}

// ---------------------------------------------------------------------------
// API request / response types
// ---------------------------------------------------------------------------

/// Body for `POST /admin/keys`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyCreateRequest {
    pub name: String,
    #[serde(default)]
    pub team_id: Option<String>,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    #[serde(default)]
    pub max_budget_usd: Option<Decimal>,
    #[serde(default)]
    pub budget_reset_days: Option<u32>,
    #[serde(default)]
    pub rpm_limit: Option<u32>,
    #[serde(default)]
    pub tpm_limit: Option<u64>,
    #[serde(default)]
    pub max_parallel_requests: Option<u32>,
    #[serde(default)]
    pub expires_in_days: Option<u32>,
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Returned exactly once after key creation — contains the raw key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyCreateResponse {
    /// The raw key value. **Only returned once.**
    pub key: String,
    pub key_id: String,
    pub name: String,
    pub key_prefix: String,
}

/// Body for `POST /admin/teams`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCreateRequest {
    pub name: String,
    #[serde(default)]
    pub max_budget_usd: Option<Decimal>,
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Body for `POST /admin/teams/{id}/members`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberRequest {
    pub key_id: String,
}

/// Aggregated spend report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendReport {
    pub total_spend_usd: Decimal,
    pub total_requests: u64,
    pub total_tokens_in: u64,
    pub total_tokens_out: u64,
    pub by_model: Vec<SpendByGroup>,
    pub by_provider: Vec<SpendByGroup>,
    pub by_key: Vec<SpendByGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendByGroup {
    pub name: String,
    pub spend_usd: Decimal,
    pub requests: u64,
    pub tokens_in: u64,
    pub tokens_out: u64,
}

/// A per-request log entry for observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLog {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub provider: String,
    pub model: String,
    pub tokens_in: u64,
    pub tokens_out: u64,
    /// Prompt tokens the upstream reported as served from its prompt cache.
    /// `cached_tokens / tokens_in` is the cache hit rate, which is the number
    /// this column exists to make queryable without re-parsing gateway logs.
    pub cached_tokens: u64,
    pub cost_usd: Decimal,
    pub latency_ms: u64,
    pub status: u16,
    pub request_id: Option<String>,
    /// Session identity the request resolved to (headers, then body). Joins a
    /// request to the conversation whose upstream prompt cache it should hit.
    pub session_id: Option<String>,
    pub client_ip: Option<String>,
    pub route_path: String,
    pub endpoint_type: String,
    pub fallback_used: bool,
    pub error: Option<String>,
}

/// Persisted circuit-breaker state for one provider.
///
/// The gateway restores these rows on startup so a quota force-open (or an
/// open circuit) survives a restart — otherwise a gateway bounced mid-incident
/// would immediately hammer the channel that just failed.
#[derive(Debug, Clone)]
pub struct CircuitBreakerStateRow {
    pub provider: String,
    /// 0 = closed, 1 = open, 2 = half-open.
    pub state: u8,
    pub failures: u32,
    /// Epoch millis of the last recorded failure.
    pub last_failure_at: u64,
    /// Epoch millis until which a quota (429) force-open holds.
    pub force_open_until: u64,
    pub successes_half_open: u32,
    pub transitions: u64,
    /// How often the provider was force-opened by a 429.
    pub quota_opens: u64,
    pub updated_at: DateTime<Utc>,
}

impl CircuitBreakerStateRow {
    /// Fold `other` into this row, keeping the stricter of each field.
    ///
    /// Used when a rename targets a name that already carries breaker state:
    /// the merged row must not look healthier than either input, or the
    /// gateway would start trusting a channel whose history says otherwise.
    /// Counters are not summed — a repeated rename must not inflate them.
    pub fn merge_stricter(self, other: &Self) -> Self {
        Self {
            provider: self.provider,
            state: self.state.max(other.state),
            failures: self.failures.max(other.failures),
            last_failure_at: self.last_failure_at.max(other.last_failure_at),
            force_open_until: self.force_open_until.max(other.force_open_until),
            successes_half_open: self.successes_half_open.min(other.successes_half_open),
            transitions: self.transitions.max(other.transitions),
            quota_opens: self.quota_opens.max(other.quota_opens),
            updated_at: self.updated_at.max(other.updated_at),
        }
    }
}

/// Persisted routing evidence for one provider.
///
/// The auto pool's two verdicts — the share of the input a provider serves
/// from its prompt cache, and how long a whole exchange takes on it — live
/// in process memory only. A restart therefore makes the pool *relearn
/// blind*: the sessions move, the upstream caches go cold, and the measured
/// cache ratio falls from 99% to 77% within a minute of every bounce — the
/// very bounce that was supposed to be invisible.
///
/// These rows carry the evidence across the bounce, **aged** rather than
/// reset: a verdict older than the window it was measured in is restored as
/// no verdict at all, so a restored provider that has been silent for an
/// hour is *unmeasured* (and therefore admitted by the rotation) instead of
/// being judged on evidence that no longer exists.
#[derive(Debug, Clone)]
pub struct ProviderStatsStateRow {
    pub provider: String,
    /// The cached share of the input (0.0-1.0). Meaningless when
    /// `cache_samples` is 0 — the provider is then *unmeasured*, not cold.
    pub cache_ratio: f64,
    /// How many exchanges the ratio is built on.
    pub cache_samples: u64,
    /// How long ago (in seconds) the newest cache observation was taken.
    pub cache_age_secs: u64,
    /// The whole-exchange latency window, as a JSON array of
    /// `{"age_secs":N,"latency_ms":F,"warm":B}` samples.
    pub total_latency: String,
    /// The remembered speed of the provider (milliseconds per 1 000 output
    /// tokens). `None` when the provider was never measured.
    ///
    /// Speed is a slow property: the samples behind it age out, and so does
    /// the estimate's window. Without this column a restart — or an hour of
    /// silence — would make a member the pool had already measured as slow
    /// indistinguishable from a brand-new account, and the rotation would
    /// hand it the same share as the fastest member.
    pub speed_ms_per_1k: Option<f64>,
    /// How many samples the remembered speed is built on.
    pub speed_samples: u64,
    /// How long ago the remembered speed was taken.
    pub speed_age_secs: u64,
    pub updated_at: DateTime<Utc>,
}

/// Outcome of a provider rename.
///
/// A provider rename in the config used to strand the old name's resilience
/// state: the breaker row kept the old name until the next restart, where the
/// startup prune deleted it (the name was no longer in the config), and the
/// new name started from a clean slate — a quota force-open or an open circuit
/// silently evaporated. This report records what actually moved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderRenameReport {
    /// The old provider name.
    pub from: String,
    /// The new provider name.
    pub to: String,
    /// A breaker row existed under `from` and now lives (or was merged) under `to`.
    pub breaker_migrated: bool,
    /// Both names had a breaker row; the stricter of the two was kept.
    pub breaker_merged: bool,
    /// Session pins rewritten from `from` to `to`.
    pub sessions_moved: usize,
}

/// Body of `POST /admin/providers/rename`.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderRenameRequest {
    /// The old provider name.
    pub from: String,
    /// The new provider name.
    pub to: String,
}

/// RBAC role for future use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    User,
    Viewer,
}

/// A learned rate-limit message pattern.
///
/// The gateway normalises an upstream quota error into a *shape fingerprint*
/// (digits → `#`, long identifiers → `<V>`) and remembers what it learned
/// about that shape: whether the error is a rate limit at all, and how its
/// reset instant is expressed. The next time a provider answers with the same
/// shape, the learned answer applies immediately — no laya call, no regex
/// scan — so a phrasing change upstream costs one extra round trip, not a
/// permanent blind cooldown.
#[derive(Debug, Clone)]
pub struct RateLimitPatternRow {
    /// Normalised message shape (the lookup key).
    pub fingerprint: String,
    /// Whether this shape denotes a rate limit / quota exhaustion.
    pub is_rate_limit: bool,
    /// How the reset instant is expressed in this shape, e.g.
    /// `timestamp` (an absolute instant in the text) or `duration`
    /// (a relative duration). `None` = the shape is a rate limit but
    /// carries no reset time.
    pub reset_expression: Option<String>,
    /// The concrete example that produced the pattern (for auditing).
    pub sample_message: String,
    /// How often the pattern was seen.
    pub hits: u64,
    /// When the pattern was last seen.
    pub updated_at: DateTime<Utc>,
}
