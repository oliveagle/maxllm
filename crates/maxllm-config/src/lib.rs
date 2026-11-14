// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("validation error: {0}")]
    Validation(String),
}

/// Top-level configuration
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    pub auth: Option<AuthConfig>,
    pub rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Named plugin definitions.
    #[serde(default)]
    pub plugins: HashMap<String, PluginConfig>,
    /// Global plugin names, executed on every request in order.
    #[serde(default)]
    pub global_plugins: Vec<String>,
    /// The updatable translation kernel (optional).
    #[serde(default)]
    pub translation_kernel: Option<TranslationKernelConfig>,
    /// Model aliases: map requested model name to actual model name.
    #[serde(default)]
    pub model_aliases: HashMap<String, String>,
    /// Admin API configuration.
    pub admin: Option<AdminConfig>,
    /// Session-sticky routing settings (global switch + tuning). Routes opt in
    /// individually with `session_sticky = true`.
    #[serde(default)]
    pub session_sticky: Option<SessionStickyConfig>,
    /// Request/response recording — a JSONL audit trail of every client and
    /// provider exchange (mirrors litellm_lb_go's recorder).
    #[serde(default)]
    pub recording: Option<RecordingConfig>,
    /// Rate-limit learning: layered quota-reset-time resolution (headers →
    /// learned patterns → body regex → laya decision) that replaces the
    /// "blind cooldown" a 429 used to trigger.
    #[serde(default)]
    pub rate_limit_learning: Option<RateLimitLearningConfig>,
    /// Model cost overrides (supplement built-in costs).
    #[serde(default)]
    pub model_costs: HashMap<String, ModelCostConfig>,
    /// Cost accounting switches. `enabled = false` keeps `cost_usd` at 0
    /// (budgets become advisory) and `warn_on_missing = false` silences the
    /// per-request "no cost entry" warning for models outside the table.
    #[serde(default)]
    pub costs: CostsConfig,
    /// Guardrail definitions (content-level guardrails).
    #[serde(default)]
    pub guardrails: Vec<GuardrailConfig>,
    /// The default auto pool: a sentinel model name that rotates across an
    /// explicit, self-contained pool of (provider, model) pairs. The pool is
    /// declared in the gateway's own config — it never reads one-api's channel
    /// table, so the rotation set stays stable even if one-api is not running.
    #[serde(default)]
    pub auto_model: Option<AutoModelConfig>,
    /// Additional named pools (`[auto_models.<key>]`). Each key is its own
    /// sentinel model, so a deployment can separate rotation sets — e.g.
    /// `auto` over cloud vendors and `auto_home` over the local GPUs. A
    /// request whose model equals a pool's name rotates over that pool only.
    #[serde(default)]
    pub auto_models: HashMap<String, AutoModelConfig>,
    /// Providers parked until a known instant (`[quota_parking]`).
    ///
    /// An operator knows more than the gateway: a vendor's weekly quota is
    /// exhausted until a specific moment, and every node should keep that
    /// provider out of the rotation until then instead of discovering the
    /// wall with a 429 of its own.
    #[serde(default)]
    pub quota_parking: HashMap<String, QuotaParkingSpec>,
    /// Cross-node breaker sharing (`[breaker_sync]`).
    ///
    /// The circuit breaker is per node: a provider that ran out of quota on
    /// one node is still probed by the next one, so every node pays one
    /// failed request before it learns the same wall. With the peers
    /// configured here, a quota force-open learned on one node is adopted by
    /// the others.
    #[serde(default)]
    pub breaker_sync: Option<BreakerSyncConfig>,
}

/// A provider parked until a known instant.
#[derive(Debug, Clone)]
pub enum QuotaParkingSpec {
    /// An RFC3339 timestamp, e.g. `"2026-11-10T22:58:54+08:00"`.
    Timestamp(String),
    /// Epoch milliseconds.
    EpochMs(i64),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum QuotaParkingRepr {
    Timestamp(String),
    EpochMs(i64),
}

impl<'de> Deserialize<'de> for QuotaParkingSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match QuotaParkingRepr::deserialize(deserializer)? {
            QuotaParkingRepr::Timestamp(ts) => Ok(QuotaParkingSpec::Timestamp(ts)),
            QuotaParkingRepr::EpochMs(ms) => Ok(QuotaParkingSpec::EpochMs(ms)),
        }
    }
}

/// Cross-node breaker sharing.
#[derive(Debug, Deserialize, Clone)]
pub struct BreakerSyncConfig {
    /// Master switch.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How often the peers are polled, in seconds.
    #[serde(default = "default_breaker_sync_interval")]
    pub interval_secs: u64,
    /// The peers' base URLs, e.g. `["http://10.126.126.199:18081"]`.
    #[serde(default)]
    pub peers: Vec<String>,
    /// The bearer token the peers expect. The config file expands
    /// `${ENV_VAR}`, so this normally reads `"${MAXLLM_MASTER_KEY}"` and no
    /// secret is stored in the file itself.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Whether a peer's *failure-driven* open is adopted too.
    ///
    /// A provider that keeps failing on one node is dead for the cluster as
    /// long as every node keeps it in rotation. Sharing the open makes the
    /// learning event happen once instead of once per node — the local
    /// circuit opens for the local fail window and then probes, so the
    /// provider earns its place back on local evidence. A provider that
    /// answers successfully here inside the fail window is kept: the local
    /// success is fresher than the peer's failure count.
    #[serde(default = "default_true")]
    pub share_failures: bool,
}

fn default_breaker_sync_interval() -> u64 {
    15
}

/// Configuration for one auto sentinel model.
#[derive(Debug, Deserialize, Clone)]
pub struct AutoModelConfig {
    /// Whether auto rotation is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// The sentinel model name clients send. Requests whose body model equals
    /// this string get their model rewritten to the selected pool entry's
    /// model. Defaults to the table key under `[auto_models.<key>]`, and to
    /// `auto` for the `[auto_model]` block.
    #[serde(default)]
    pub name: Option<String>,
    /// Explicit pool of (provider, model) pairs. Providers must be declared
    /// in `[providers.*]`; the gateway filters out entries whose provider is
    /// missing at startup.
    #[serde(default)]
    pub pool: Vec<AutoPoolEntry>,
    /// Prefer the low-latency cohort inside the preferred billing tier.
    ///
    /// The rotation measures each provider's EWMA upstream latency; an entry
    /// whose latency is far above the tier's median is skipped, so one slow
    /// subscription plan cannot take an equal share of interactive traffic.
    /// Every [`AUTO_POOL_EXPLORE_EVERY`]th rotation still walks the whole
    /// tier, so a recovered (or brand-new) provider is re-measured and can
    /// rejoin. Defaults to `true`; set `prefer_fast = false` to restore plain
    /// round-robin over the whole tier.
    #[serde(default = "default_true")]
    pub prefer_fast: bool,
    /// Which side of the deployment boundary this pool may draw from.
    /// Checked at load: a member that violates it is a startup error, not a
    /// silent misroute. Defaults to `any` so existing configs keep working.
    #[serde(default)]
    pub scope: AutoPoolScope,
    /// Which models this pool is allowed to serve.
    ///
    /// An empty list means "any model" — the behaviour every config had
    /// before this field existed. When it is set, a member is only legal if
    /// the model it serves is on the list, and the model its provider
    /// defaults to is on the list too (so a member cannot answer with a
    /// different model than the one it claims).
    ///
    /// This is the model-axis twin of [`AutoPoolScope`]: `scope` says
    /// *whose* hardware may answer, `allowed_models` says *which model* may
    /// answer. `auto_home` wants both — hardware we run ourselves, and only
    /// the one home model family — and a pool that says so in config should
    /// not be able to drift away from it silently. Like `scope`, a
    /// violation is a startup error, not a runtime surprise.
    ///
    /// Not to be confused with a virtual key's `allowed_models` (the admin
    /// API's per-key "which models may this client ask for" filter): that
    /// one filters *requests* and is skipped by the static master key, so
    /// it cannot be what keeps a pool honest. This one is a property of the
    /// pool itself.
    #[serde(default)]
    pub allowed_models: Vec<String>,
}

/// Which side of the deployment boundary an auto pool may draw from.
///
/// `auto` is meant to rotate over external vendors and `auto_home` over the
/// machines we run ourselves. Without an enforced scope those two pools are
/// only a naming convention: a vendor added to `auto_home` would serve the
/// request silently, and a caller who asked for `auto_home` would have no
/// way to tell that the self-hosted pool was never reached.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AutoPoolScope {
    /// No boundary check (the default; pre-existing configs keep working).
    #[default]
    Any,
    /// Every member must be a self-hosted provider.
    SelfHosted,
    /// Every member must be an external vendor provider.
    Vendor,
}

impl AutoModelConfig {
    /// The sentinel clients send: an explicit `name`, else the table key for a
    /// named pool, else `auto` (the singular `[auto_model]` default).
    pub fn sentinel_name(&self, table_key: Option<&str>) -> String {
        match (self.name.as_deref(), table_key) {
            (Some(name), _) if !name.is_empty() => name.to_string(),
            (_, Some(key)) => key.to_string(),
            _ => default_auto_model_name(),
        }
    }
}

impl Config {
    /// Every auto pool in declaration order: the singular `[auto_model]`
    /// first (sentinel `auto` unless renamed), then each `[auto_models.<key>]`
    /// sorted by key so a hot reload resolves them deterministically.
    pub fn auto_pools(&self) -> Vec<(String, &AutoModelConfig)> {
        let mut pools = Vec::new();
        if let Some(auto) = self.auto_model.as_ref() {
            pools.push((auto.sentinel_name(None), auto));
        }
        let mut keys: Vec<&String> = self.auto_models.keys().collect();
        keys.sort();
        for key in keys {
            let auto = &self.auto_models[key];
            pools.push((auto.sentinel_name(Some(key.as_str())), auto));
        }
        pools
    }
}

/// How a pool entry is paid for. Selection is tiered by this: the rotation
/// only reaches a `metered` (pay-per-token) entry once every `subscription`
/// entry of the pool is unavailable, so a prepaid plan is never left idle
/// while a per-token key is being billed.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BillingMode {
    /// Prepaid plan / membership (Coding Plan, Code membership, monthly
    /// subscription). The default: an entry without `billing` is assumed to
    /// be prepaid, which keeps older configs on the preferred tier.
    #[default]
    Subscription,
    /// Pay per token against a balance. Always ranked after every
    /// subscription entry.
    Metered,
}

/// One entry of the auto-rotation pool.
#[derive(Debug, Deserialize, Clone)]
pub struct AutoPoolEntry {
    /// Provider name (must match a `[providers.<name>]` section).
    pub provider: String,
    /// Model name forwarded to that provider (rewritten into the request
    /// body, so the upstream sees its own model rather than the sentinel).
    pub model: String,
    /// Billing model of this entry. Defaults to `subscription`.
    #[serde(default)]
    pub billing: BillingMode,
}

fn default_auto_model_name() -> String {
    "auto".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct AdminConfig {
    /// Master key for admin API access.
    pub master_key: String,
    /// Enable admin API endpoints under /admin/*.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Path to SQLite database file for persistent storage.
    /// If not set, uses in-memory storage.
    pub db_path: Option<String>,
}

impl Default for SessionStickyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_secs: default_session_timeout_secs(),
            max_entries: default_session_max_entries(),
            body_affinity: false,
            body_affinity_max_bytes: default_session_body_affinity_max_bytes(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SessionStickyConfig {
    /// Master switch. When false, no route pins sessions even if it opts in.
    #[serde(default = "default_false")]
    pub enabled: bool,
    /// How long a session binding stays valid since last use, in seconds.
    #[serde(default = "default_session_timeout_secs")]
    pub timeout_secs: u64,
    /// Max number of session entries kept in memory (LRU eviction on overflow).
    #[serde(default = "default_session_max_entries")]
    pub max_entries: usize,
    /// Resolve the session identity from the **request body** when the
    /// client sends no session header.
    ///
    /// The header spellings (`session-id`, `thread-id`, `X-Session-ID`, ...)
    /// are the obvious source, but a proxy in front of the gateway (one-api
    /// is one) drops them: the request reaches the gateway with no session
    /// identity at all, and every turn of a conversation is then free to land
    /// on a different account. The identity also lives in the body —
    /// codex sends `prompt_cache_key` and `client_metadata.session_id` —
    /// and the body survives every proxy.
    ///
    /// Reading the body costs one buffered copy of the request and delays
    /// the upstream call by the time it takes to receive it, which is why
    /// it is opt-in.
    #[serde(default = "default_false")]
    pub body_affinity: bool,
    /// Largest request body that is buffered for the body affinity, in
    /// bytes. A bigger body is left alone (no affinity) rather than being
    /// held in memory.
    #[serde(default = "default_session_body_affinity_max_bytes")]
    pub body_affinity_max_bytes: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RecordingConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    /// Directory for the JSONL recording files (created if missing).
    #[serde(default = "default_recording_dir")]
    pub dir: String,
    /// Cap on how many bytes of a request/response body are stored per entry.
    #[serde(default = "default_recording_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Max bytes per recording file before it rotates to the next shard.
    #[serde(default = "default_recording_max_file_bytes")]
    pub max_file_bytes: u64,
    /// Delete recording shards older than this many hours.
    #[serde(default = "default_recording_retention_hours")]
    pub retention_hours: u64,
    /// Scrub credentials out of recorded bodies before they hit disk.
    ///
    /// Applies to both the JSON keys that conventionally carry credentials
    /// (`authorization`, `api_key`, `token`, …) and high-confidence secret
    /// shapes inside ordinary strings (`sk-…`, `Bearer …`, `AKIA…`, JWTs).
    /// On by default: recordings are shipped to a log store, so an unredacted
    /// body is an exfiltration path for any key a client pastes into a prompt.
    #[serde(default = "default_true")]
    pub redact: bool,
    /// Also scrub obvious personal data — e-mail addresses, mainland China
    /// mobile numbers and 18-digit resident IDs — from recorded bodies.
    /// On by default; turn off only on a deployment whose audit trail must
    /// preserve the exact prompt text.
    #[serde(default = "default_true")]
    pub redact_pii: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelCostConfig {
    pub input_per_1m: f64,
    pub output_per_1m: f64,
    /// USD per 1M input tokens served from the vendor's prompt cache. When
    /// present, the cached part of the input is billed at this rate instead of
    /// `input_per_1m`. Omit it to keep the current behaviour (everything at
    /// list price) — e.g. when the channel's cache price is unknown.
    #[serde(default)]
    pub cached_input_per_1m: Option<f64>,
}

/// Global cost-accounting switches.
#[derive(Debug, Deserialize, Clone)]
pub struct CostsConfig {
    /// Compute `cost_usd` at all. When false every request books 0 and the
    /// "no cost entry" warning is suppressed.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Warn (once per request) when a model is not in the price table.
    /// A deployment whose price table is known to be incomplete should set
    /// this false; the `maxllm_usage_missing_total` metric still flags
    /// responses that carry no usage at all.
    #[serde(default = "default_true")]
    pub warn_on_missing: bool,
}

impl Default for CostsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            warn_on_missing: true,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub threads: Option<usize>,
    #[serde(default = "default_true")]
    pub tcp_reuseport: bool,
    pub tcp_fastopen: Option<usize>,
    /// Seconds to drain in-flight requests after SIGTERM before the runtimes
    /// are torn down. Pingora defaults to 300s, which is longer than the
    /// service manager's stop timeout — the process was then SIGKILLed
    /// mid-shutdown. Defaults to 30s.
    #[serde(default = "default_grace_period_secs")]
    pub grace_period_secs: u64,
}

fn default_true() -> bool {
    true
}

fn default_grace_period_secs() -> u64 {
    30
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub kind: ProviderKind,
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    pub default_model: Option<String>,
    /// The only models this provider may serve (empty = no restriction).
    ///
    /// A request whose model is not on this list is refused with
    /// `403 model_not_allowed` *before* it reaches the upstream, so a
    /// deployment that pays for one model cannot be driven with another.
    /// Unlike an auto pool's `allowed_models` (a load-time invariant over the
    /// pool's members) this is a per-request gate on the provider that was
    /// selected to answer. Not to be confused with a virtual key's
    /// allow-list, which filters what a *client* may ask for.
    #[serde(default)]
    pub allowed_models: Vec<String>,
    #[serde(default = "default_max_fails")]
    pub max_fails: u32,
    #[serde(default = "default_fail_timeout_secs")]
    pub fail_timeout_secs: u64,
    /// Azure OpenAI deployment name.
    pub deployment: Option<String>,
    /// Azure OpenAI API version.
    pub api_version: Option<String>,
    /// AWS region for Bedrock.
    pub region: Option<String>,
    /// Custom upstream path override.
    pub upstream_path: Option<String>,
    /// Whether this provider speaks the Responses API natively. When true the
    /// gateway forwards `/v1/responses` requests unchanged instead of
    /// translating them into chat completions — fewer moving parts, and the
    /// full Responses feature set (previous_response_id, encrypted reasoning)
    /// survives the round trip. Defaults to true for `kind = "opencode"`,
    /// false for every other kind.
    #[serde(default)]
    pub native_responses: Option<bool>,
    /// Upstream path of the native Responses endpoint. Derived from
    /// `upstream_path` (`…/chat/completions` → `…/responses`) when unset.
    #[serde(default)]
    pub responses_path: Option<String>,
    /// Named repair a provider needs on its **native Responses** request
    /// body, because its upstream rejects part of the contract. Valid values:
    ///
    /// * `"ark_trailing_assistant"` — drop a trailing assistant text turn.
    ///   Volcengine Ark's prefill validation answers
    ///   `400 MissingParameter \`partial\`` for such an input (the parameter
    ///   is a red herring; the rejected thing is the trailing turn), which is
    ///   the shape codex remote compaction sends.
    /// * `"none"` — force no repair, overriding the host-based default.
    ///
    /// Unset means the gateway picks by upstream host: ark (`*.volces.com`)
    /// providers get `ark_trailing_assistant`, every other provider gets no
    /// repair.
    #[serde(default)]
    pub native_responses_repair: Option<String>,
    /// The tool-schema dialect the upstream validates `tools[].function.
    /// parameters` against. Valid values:
    ///
    /// * `"moonshot"` — project every tool schema into the Moonshot/Kimi
    ///   dialect before it is forwarded: a `$ref` beside another keyword is
    ///   inlined, an untyped reference target gains its inferred type, a
    ///   `type` beside an `anyOf`/`oneOf` moves into the branches, and every
    ///   reference cycle is cut at a terminating object. Kimi answers
    ///   `400 invalid_request_error … not a valid moonshot flavored json
    ///   schema` for the shapes this projection repairs (codex's zod 4
    ///   `$defs.__schemaN` schemas are the known offenders).
    /// * `"none"` — force no projection, overriding the host-based default.
    ///
    /// Unset means the gateway picks by upstream host: Moonshot-family
    /// providers (`api.moonshot.ai`, `api.moonshot.cn`, `api.kimi.com`, …)
    /// get the projection, every other provider keeps its body untouched.
    #[serde(default)]
    pub tool_schema: Option<String>,
    /// Maximum number of requests the gateway keeps in flight to this
    /// provider at once. When the count is reached the provider is skipped by
    /// the rotation and the fallback walk — the same way an open circuit is
    /// — so the excess request is served by another member instead of being
    /// rejected by the upstream's tier limit.
    ///
    /// Subscription tiers answer over-limit traffic in two shapes: a 429
    /// (the gateway already parks the provider) and — worse — a 400
    /// `tier_limit_exceeded` wearing a request-fault status (open-design
    /// answers `membership concurrency limit exceeded: 11/10` and
    /// `membership rpm limit exceeded: 101/100` this way), which would end
    /// the caller's agent loop. The cap keeps the gateway under the tier's
    /// own ceiling so the rejection never happens.
    ///
    /// Unset means no local cap: the provider's own limits are discovered by
    /// its answers (429 → park; tier-limit 400 → park + failover).
    #[serde(default)]
    pub max_concurrency: Option<u32>,
    /// Skip the `reasoning_content` placeholder injection for thinking-mode
    /// tool-calling turns (echo collapse still runs). Set on opencode-kind
    /// channels whose upstream does not require `reasoning_content` back, to
    /// avoid feeding a fake reasoning marker into the model context.
    #[serde(default)]
    pub skip_reasoning_injection: Option<bool>,
    /// TCP connect timeout to the upstream, in seconds. Defaults to 10.
    /// Raise it for upstreams on a slow or lossy path (e.g. opencode.ai
    /// behind Cloudflare from an international link, where the TCP handshake
    /// was measured taking up to 15s) — a too-tight budget turns a slow but
    /// healthy upstream into a 502.
    #[serde(default)]
    pub connect_timeout_secs: Option<u64>,
    /// Total budget for establishing the upstream connection (TCP connect plus
    /// TLS handshake), in seconds. Unset means no separate total budget.
    #[serde(default)]
    pub total_connect_timeout_secs: Option<u64>,
    /// How long the gateway waits for the upstream to produce something, in
    /// seconds.
    ///
    /// This is the *stall* budget, not the answer budget: a stream that
    /// keeps sending tokens may take as long as it needs, but an upstream
    /// that accepts the connection and then says nothing is cut off here
    /// instead of holding the client for the route's own (much longer)
    /// timeout. The default route timeout is 120 s — two minutes of a
    /// client waiting on an upstream that is never going to answer.
    ///
    /// Unset means the route's timeout, as before.
    #[serde(default)]
    pub read_timeout_secs: Option<u64>,
    /// How long the gateway waits for the upstream's **first** byte, in
    /// seconds.
    ///
    /// The stall budget above is per read: an upstream that keeps sending
    /// something may hold the client for as long as the route's timeout. But
    /// an upstream that accepts the request and then says nothing — or sends
    /// its headers only when it has an answer — is *slow to start*, and the
    /// client waits for nothing. The pool has nine other members that could
    /// answer in two seconds, so the request is moved on instead of waiting
    /// out the stall budget.
    ///
    /// Only streaming requests are bounded this way: for a non-streaming
    /// request the upstream sends its headers when the answer is *ready*, so
    /// the first byte is the answer and the route's own timeout governs it.
    ///
    /// `0` disables the deadline. Unset means the built-in default (30 s).
    #[serde(default)]
    pub first_byte_timeout_secs: Option<u64>,
    /// Fixed UTC offset used to interpret **zone-less** reset timestamps in
    /// upstream rate-limit messages, e.g. Zhipu's
    /// `您的限额将在 2026-11-10 22:58:54 重置` (Beijing time, but written
    /// without a zone). Accepts `+08:00`, `+0800`, `+08`, `-05:30` and `Z`.
    /// Unset means UTC.
    ///
    /// Only text-derived instants use it. `Retry-After`,
    /// `x-ratelimit-reset-*` and RFC 3339 timestamps that carry their own
    /// offset are unaffected.
    #[serde(default)]
    pub utc_offset: Option<String>,
    /// Weight for load balancing (higher = more traffic).
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Arbitrary tags on the provider (parsed and carried through, but not
    /// yet consumed by any routing rule — reserved for tag-based routing).
    #[serde(default)]
    pub tags: Vec<String>,
    /// Whether this provider runs on hardware we control rather than on a
    /// vendor's API. `None` means "infer it from `base_url`" — see
    /// [`ProviderConfig::is_self_hosted`]. Set it explicitly when the
    /// inference would be wrong in either direction, e.g. a home GPU box
    /// published through a public DNS name, or a vendor reached over a
    /// private link.
    #[serde(default)]
    pub self_hosted: Option<bool>,
}

impl ProviderConfig {
    /// Whether this provider runs on hardware we control.
    ///
    /// An explicit `self_hosted` wins; otherwise it is inferred from the
    /// base URL: loopback, RFC1918, CGNAT, link-local and `.local` hosts are
    /// ours, a DNS name on the public internet is a vendor's.
    ///
    /// This is what `[auto_models.<key>] scope = "self_hosted"` is checked
    /// against, so the inference is a safety boundary, not a label: a
    /// deployment that reaches its own models through a public hostname must
    /// say `self_hosted = true` explicitly, and a vendor reached over a
    /// private address must say `self_hosted = false`.
    pub fn is_self_hosted(&self) -> bool {
        self.self_hosted
            .unwrap_or_else(|| url_host_is_local(&self.base_url))
    }
}

/// Whether a base URL points at a private/local host.
///
/// Deliberately conservative: anything that is not recognisably local —
/// including every public DNS name — reads as "vendor". The alternative
/// (defaulting to "ours") would let a mistyped pool entry spend a vendor's
/// budget on what the operator believes is the home pool.
fn url_host_is_local(base_url: &str) -> bool {
    let rest = base_url.trim();
    let rest = rest.split_once("://").map(|(_, r)| r).unwrap_or(rest);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Drop userinfo (`user:pass@host:port`).
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, r)| r)
        .unwrap_or(authority);
    let host = if let Some(v6) = host_port.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return false;
    }
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        return true;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => {
            let o = ip.octets();
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                // 100.64.0.0/10 — carrier-grade NAT, which is also the
                // address family some EasyTier mesh nodes hand out.
                || (o[0] == 100 && (64..=127).contains(&o[1]))
        }
        Ok(std::net::IpAddr::V6(ip)) => {
            let seg = ip.segments();
            ip.is_loopback() || (seg[0] & 0xfe00) == 0xfc00
        }
        Err(_) => false,
    }
}

/// Parse a fixed UTC offset into minutes east of UTC.
///
/// Accepted shapes: `Z` / `UTC` / `+00:00`, `+08:00`, `+0800`, `+08`,
/// `-05:30`, `-0530`, `-05`. Range is checked to `-12:00 ..= +14:00`.
pub fn parse_utc_offset(raw: &str) -> Option<i32> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    if t.eq_ignore_ascii_case("z") || t.eq_ignore_ascii_case("utc") || t == "GMT" {
        return Some(0);
    }
    let bytes = t.as_bytes();
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let rest = &t[1..];
    let (hh, mm) = match rest.len() {
        2 => (rest, "00"),
        4 => (&rest[..2], &rest[2..]),
        5 if rest.as_bytes()[2] == b':' => (&rest[..2], &rest[3..]),
        _ => return None,
    };
    if !hh.bytes().all(|b| b.is_ascii_digit()) || !mm.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i32 = hh.parse().ok()?;
    let minutes: i32 = mm.parse().ok()?;
    if minutes > 59 || hours > 14 {
        return None;
    }
    let total = sign * (hours * 60 + minutes);
    if !(-12 * 60..=14 * 60).contains(&total) {
        return None;
    }
    Some(total)
}

impl ProviderConfig {
    /// The provider's fixed UTC offset in minutes east of UTC (0 when unset).
    ///
    /// Validated at config load, so an invalid value never reaches here.
    #[must_use]
    pub fn utc_offset_minutes(&self) -> i32 {
        self.utc_offset
            .as_deref()
            .and_then(parse_utc_offset)
            .unwrap_or(0)
    }
}

fn default_max_fails() -> u32 {
    3
}

fn default_fail_timeout_secs() -> u64 {
    60
}

fn default_weight() -> u32 {
    100
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    #[serde(rename = "openai")]
    OpenAI,
    #[serde(rename = "anthropic")]
    Anthropic,
    #[serde(rename = "gemini")]
    Gemini,
    #[serde(rename = "azure_openai")]
    AzureOpenai,
    #[serde(rename = "bedrock")]
    Bedrock,
    #[serde(rename = "groq")]
    Groq,
    #[serde(rename = "together")]
    Together,
    #[serde(rename = "fireworks")]
    Fireworks,
    #[serde(rename = "deepinfra")]
    DeepInfra,
    #[serde(rename = "mistral")]
    Mistral,
    #[serde(rename = "xai")]
    XAI,
    #[serde(rename = "deepseek")]
    DeepSeek,
    #[serde(rename = "ollama")]
    Ollama,
    #[serde(rename = "cohere")]
    Cohere,
    /// Generic OpenAI-compatible provider.
    #[serde(rename = "openai_compat")]
    OpenaiCompat,
    /// opencode.ai ("zen") provider — OpenAI-compatible with stricter
    /// validation (session header, tool_choice, tool call arguments).
    #[serde(rename = "opencode")]
    OpenCode,
    /// commandcode.ai ("provider") — a multi-vendor gateway that speaks the
    /// OpenAI Chat Completions contract on `/provider/v1/chat/completions`,
    /// the OpenAI Responses contract on `/provider/v1/responses` and the
    /// Anthropic Messages contract on `/provider/v1/messages` (the only
    /// route that serves its Claude models).
    #[serde(rename = "commandcode")]
    CommandCode,
    /// OpenDesign (open-design.ai) — the "AMR link" endpoint of the design
    /// workspace. It speaks the OpenAI Chat Completions contract on
    /// `/v1/chat/completions` and the OpenAI Responses contract natively on
    /// `/v1/responses`; the link is authenticated with an `sk-amr-…` bearer
    /// token.
    #[serde(rename = "open_design")]
    OpenDesign,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteConfig {
    pub path: String,
    pub provider: String,
    #[serde(default)]
    pub fallback: Vec<String>,
    pub timeout_secs: Option<u64>,
    /// Route-specific plugin names, appended after global plugins.
    #[serde(default)]
    pub plugins: Vec<String>,
    /// Route-specific guardrail names. Only these guardrails run for this route.
    /// If empty, all `default_on` guardrails apply (plus any client-requested ones).
    #[serde(default)]
    pub guardrails: Vec<String>,
    /// Endpoint type for multi-endpoint routing.
    #[serde(default)]
    pub endpoint_type: EndpointType,
    /// Load balancing strategy when multiple providers are available.
    #[serde(default)]
    pub strategy: RoutingStrategy,
    /// Opt-in for session-sticky routing. When true and the global
    /// `[session_sticky]` section is enabled, requests that carry a session id
    /// are pinned to the provider they first succeeded on (until it trips or
    /// the binding expires). Mirrors litellm_lb_go's session affinity.
    #[serde(default)]
    pub session_sticky: bool,
    /// Maximum number of providers a single request may walk when the
    /// current provider fails (an HTTP error such as 429/5xx/404, or a
    /// connect failure). Mirrors litellm_lb_go's
    /// `maxResponsesFailoverAttempts`. 0 disables request-level failover.
    #[serde(default = "default_max_failover_attempts")]
    pub max_failover_attempts: u32,
    /// How many times a transient upstream failure (HTTP 5xx) is retried on
    /// the SAME provider before the request moves to the next provider of
    /// the fallback chain. Deterministic rejections (429 quota, 401/403
    /// auth, 404 missing model) and connect failures are not retried on the
    /// same provider — they move on immediately. 0 (the default) disables
    /// the same-provider retry and the request walks straight to the
    /// fallback chain.
    #[serde(default)]
    pub num_retries: u32,
    /// Backoff between those same-provider retries, in milliseconds.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// This route carries the `auto` sentinel model: provider selection
    /// rotates over the `[auto_model]` pool instead of the route's
    /// fallback chain, and the request body's model field is rewritten to
    /// the selected entry's model.
    #[serde(default)]
    pub auto_model: bool,
}

fn default_retry_backoff_ms() -> u64 {
    500
}

fn default_max_failover_attempts() -> u32 {
    6
}

fn default_false() -> bool {
    false
}

fn default_session_timeout_secs() -> u64 {
    3600
}

fn default_session_max_entries() -> usize {
    10_000
}

/// 8 MiB: an agent request with a 1M-token context is ~4 MB, so the cap
/// has to be well above that to keep the affinity for the big ones too.
fn default_session_body_affinity_max_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_recording_dir() -> String {
    "./recordings".to_string()
}

fn default_recording_max_body_bytes() -> usize {
    8192
}

fn default_recording_max_file_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_recording_retention_hours() -> u64 {
    168
}

/// Endpoint type determines which translator pipeline to use.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EndpointType {
    #[default]
    ChatCompletions,
    /// OpenAI Responses API — the client speaks the Responses contract
    /// (`input`/`output`/`instructions`); the gateway translates to and
    /// from chat completions for any provider kind.
    Responses,
    Embeddings,
    ImageGenerations,
    AudioTranscriptions,
    AudioSpeech,
    Moderations,
    Completions,
    Rerank,
    /// Native provider format — accept provider-native request/response format.
    /// No body translation; metadata (model, tokens) extracted from native format.
    Native,
    /// Raw pass-through proxy — zero body parsing or translation.
    /// Only auth injection, Host header rewriting, and logging.
    Passthrough,
}

/// Load balancing strategy for provider selection.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    /// Use primary provider, fall back on failure.
    #[default]
    Fallback,
    /// Weighted random selection across providers.
    Weighted,
    /// Route to provider with lowest observed latency.
    LatencyBased,
    /// Route to provider with fewest active connections.
    LeastConnections,
    /// Route to cheapest provider.
    CostBased,
    /// Even distribution across providers.
    RoundRobin,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PluginConfig {
    pub category: String,
    #[serde(flatten)]
    pub params: toml::Table,
}

/// The translation kernel: a Rhai script that owns the provider-dialect
/// decision surface (request adaptation + upstream-error classification).
/// Edited without a release cycle; recompiled on every config hot-reload
/// and watched directly, so saving the file alone is the deploy.
#[derive(Debug, Deserialize, Clone)]
pub struct TranslationKernelConfig {
    /// Path to the kernel script. Relative paths resolve against the
    /// process working directory (the systemd unit runs from the config
    /// directory, so a bare filename works there).
    pub script: PathBuf,
    /// Rhai operation budget for one script call. The default is generous
    /// enough for schema walks; a runaway script is stopped by this, never
    /// by the request timing out.
    #[serde(default)]
    pub max_operations: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(default)]
    pub api_keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub listen: Option<SocketAddr>,
    /// Serve `/metrics` to non-loopback clients.
    ///
    /// The exposition carries per-provider traffic, quota and cost figures, so
    /// it is loopback-only by default: the OTel collector scrapes it locally.
    /// Turn this on only when the scraper lives on another host and the
    /// listener is already behind a firewall.
    #[serde(default)]
    pub allow_remote: bool,
}

/// Guardrail configuration — content-level inspection with provider integration.
///
/// Supports built-in providers (prompt_guard, pii_filter, secret_scan, keyword_block,
/// regex_guard, cel) and external providers (webhook for generic HTTP APIs, lakera
/// for Lakera AI).
#[derive(Debug, Deserialize, Clone)]
pub struct GuardrailConfig {
    /// Unique name for this guardrail (used in API requests and response headers).
    pub name: String,
    /// Provider type: "prompt_guard", "pii_filter", "secret_scan", "keyword_block",
    /// "regex_guard", "webhook", "lakera".
    pub provider: String,
    /// When to run: "pre_call" (before LLM), "post_call" (after LLM), "both".
    #[serde(default = "default_guardrail_mode")]
    pub mode: String,
    /// Whether this guardrail runs on every request automatically.
    #[serde(default)]
    pub default_on: bool,
    /// What to do on match: "block", "redact", "log_only".
    #[serde(default = "default_guardrail_action")]
    pub action: String,
    /// Provider-specific parameters (patterns, rules, api_base, api_key, etc.).
    #[serde(flatten)]
    pub params: toml::Table,
}

fn default_guardrail_mode() -> String {
    "pre_call".into()
}

fn default_guardrail_action() -> String {
    "block".into()
}

impl Config {
    /// Load config from a TOML file, expanding `${ENV_VAR}` references.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        let expanded = expand_env_vars(&raw);
        let config: Config = toml::from_str(&expanded)?;
        config.validate()?;
        Ok(config)
    }

    /// Parse config from a TOML string (env vars already expanded).
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let expanded = expand_env_vars(s);
        let config: Config = toml::from_str(&expanded)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.providers.is_empty() {
            return Err(ConfigError::Validation(
                "at least one provider must be configured".into(),
            ));
        }
        // An auto pool's name is the sentinel model clients send, so it must
        // exist and be unique — two pools answering to one name would make the
        // rotation set depend on declaration order.
        let pools = self.auto_pools();
        let mut pool_names = std::collections::HashSet::new();
        for (name, _) in &pools {
            if name.is_empty() {
                return Err(ConfigError::Validation(
                    "an auto pool has an empty name — each pool needs a sentinel model name".into(),
                ));
            }
            if !pool_names.insert(name.clone()) {
                return Err(ConfigError::Validation(format!(
                    "auto pool name '{name}' is declared twice — each pool needs its own sentinel"
                )));
            }
        }
        // A pool's scope is a deployment boundary, not a label. Check it
        // here so that a member on the wrong side of the boundary fails the
        // load instead of quietly serving traffic it was never meant to see.
        for (name, auto) in &pools {
            // Two independent boundaries, checked in the same place and in
            // the same fail-closed shape: `scope` bounds whose hardware may
            // answer, `allowed_models` bounds which model may answer.
            let scope_checked = auto.scope != AutoPoolScope::Any;
            let models_checked = !auto.allowed_models.is_empty();
            if !scope_checked && !models_checked {
                continue;
            }
            for entry in &auto.pool {
                let provider = self.providers.get(&entry.provider);
                if scope_checked {
                    let Some(provider) = provider else {
                        return Err(ConfigError::Validation(format!(
                            "auto pool '{name}' references unknown provider '{}'",
                            entry.provider
                        )));
                    };
                    let self_hosted = provider.is_self_hosted();
                    let off_scope = matches!(
                        (auto.scope, self_hosted),
                        (AutoPoolScope::SelfHosted, false) | (AutoPoolScope::Vendor, true)
                    );
                    if off_scope {
                        let want = match auto.scope {
                            AutoPoolScope::SelfHosted => "self-hosted",
                            _ => "external vendor",
                        };
                        let got = if self_hosted {
                            "self-hosted"
                        } else {
                            "a vendor"
                        };
                        return Err(ConfigError::Validation(format!(
                            "auto pool '{name}' is scoped to {want} providers, but member '{}' \
                             ({}) is {got} ({}) — remove the entry, or set `self_hosted` on \
                             that provider if the inference is wrong",
                            entry.provider, entry.model, provider.base_url
                        )));
                    }
                }
                if models_checked {
                    let Some(provider) = provider else {
                        return Err(ConfigError::Validation(format!(
                            "auto pool '{name}' references unknown provider '{}'",
                            entry.provider
                        )));
                    };
                    let allowed = auto.allowed_models.join(", ");
                    // The model the pool entry asks for is what the upstream
                    // is told to serve — the client sees this name.
                    if !auto.allowed_models.iter().any(|m| m == &entry.model) {
                        return Err(ConfigError::Validation(format!(
                            "auto pool '{name}' is restricted to the models [{allowed}], but \
                             member '{}' serves '{}' — remove the entry, or widen \
                             `allowed_models`",
                            entry.provider, entry.model
                        )));
                    }
                    // ...and the model the provider falls back to when the
                    // request does not name one must be on the list too, so a
                    // member cannot answer with a model it did not declare.
                    if let Some(default_model) = provider.default_model.as_deref() {
                        if !default_model.is_empty()
                            && !auto.allowed_models.iter().any(|m| m == default_model)
                        {
                            return Err(ConfigError::Validation(format!(
                                "auto pool '{name}' is restricted to the models [{allowed}], but \
                                 member '{}' defaults to '{}' — remove the entry, or widen \
                                 `allowed_models`",
                                entry.provider, default_model
                            )));
                        }
                    }
                }
            }
        }
        for route in &self.routes {
            // An auto route rotates over an auto pool instead of naming a
            // single provider, so its `provider` field is a sentinel (not a
            // [providers.*] key). It names the pool when it matches one and
            // otherwise uses the default `[auto_model]` pool.
            if route.auto_model {
                let named = pools.iter().find(|(name, _)| name == &route.provider);
                let pool_ok = match named {
                    Some((_, auto)) => auto.enabled && !auto.pool.is_empty(),
                    None => pools
                        .iter()
                        .any(|(_, auto)| auto.enabled && !auto.pool.is_empty()),
                };
                if !pool_ok {
                    return Err(ConfigError::Validation(format!(
                        "route '{}' is auto_model but [auto_model] / [auto_models.{}] is missing, disabled or has an empty pool",
                        route.path, route.provider
                    )));
                }
                continue;
            }
            if !self.providers.contains_key(&route.provider) {
                return Err(ConfigError::Validation(format!(
                    "route '{}' references unknown provider '{}'",
                    route.path, route.provider
                )));
            }
            for fb in &route.fallback {
                if !self.providers.contains_key(fb) {
                    return Err(ConfigError::Validation(format!(
                        "route '{}' fallback references unknown provider '{}'",
                        route.path, fb
                    )));
                }
            }
        }
        // Validate plugin references
        for name in &self.global_plugins {
            if !self.plugins.contains_key(name) {
                return Err(ConfigError::Validation(format!(
                    "global_plugins references unknown plugin '{name}'"
                )));
            }
        }
        for (name, provider) in &self.providers {
            if let Some(raw) = provider.utc_offset.as_deref() {
                if parse_utc_offset(raw).is_none() {
                    return Err(ConfigError::Validation(format!(
                        "provider '{name}' has invalid utc_offset '{raw}' (expected e.g. \"+08:00\", \"+0800\" or \"Z\")"
                    )));
                }
            }
        }
        for route in &self.routes {
            for name in &route.plugins {
                if !self.plugins.contains_key(name) {
                    return Err(ConfigError::Validation(format!(
                        "route '{}' references unknown plugin '{name}'",
                        route.path
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Replace `${VAR_NAME}` patterns with environment variable values.
fn expand_env_vars(input: &str) -> String {
    let re = Regex::new(r"\$\{([^}]+)\}").expect("valid regex");
    re.replace_all(input, |caps: &regex::Captures| {
        let var_name = &caps[1];
        std::env::var(var_name).unwrap_or_default()
    })
    .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body affinity is opt-in: a config that does not ask for it keeps
    /// the old behaviour (headers only), and the size cap has a usable
    /// default when only the switch is set.
    #[test]
    fn session_body_affinity_defaults_off_but_caps_at_8mib() {
        let cfg: Config = toml::from_str(
            r#"
[server]
listen = "0.0.0.0:1"

[providers.mock]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "k"

[session_sticky]
enabled = true
"#,
        )
        .expect("config parses");
        let sticky = cfg.session_sticky.expect("session_sticky");
        assert!(!sticky.body_affinity, "body affinity is opt-in");
        assert_eq!(sticky.body_affinity_max_bytes, 8 * 1024 * 1024);

        let cfg: Config = toml::from_str(
            r#"
[server]
listen = "0.0.0.0:1"

[providers.mock]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "k"

[session_sticky]
enabled = true
body_affinity = true
body_affinity_max_bytes = 1048576
"#,
        )
        .expect("config parses");
        let sticky = cfg.session_sticky.expect("session_sticky");
        assert!(sticky.body_affinity);
        assert_eq!(sticky.body_affinity_max_bytes, 1024 * 1024);
    }

    #[test]
    fn utc_offset_shapes_parse() {
        for (raw, want) in [
            ("+08:00", 480),
            ("+0800", 480),
            ("+08", 480),
            ("-05:30", -330),
            ("-0530", -330),
            ("-05", -300),
            ("Z", 0),
            ("utc", 0),
            ("GMT", 0),
            ("+14:00", 840),
            ("-12:00", -720),
        ] {
            assert_eq!(parse_utc_offset(raw), Some(want), "{raw}");
        }
        for raw in [
            "",
            "8:00",
            "08:00",
            "+8:0",
            "+15:00",
            "-13:00",
            "+08:60",
            "Asia/Shanghai",
            "++08",
        ] {
            assert_eq!(parse_utc_offset(raw), None, "{raw}");
        }
    }

    #[test]
    fn provider_utc_offset_is_validated_at_load() {
        let base = r#"
[server]
listen = "0.0.0.0:8080"

[providers.zhipu]
kind = "openai_compat"
base_url = "https://open.bigmodel.cn"
default_model = "glm-5.3-flash"
"#;
        let ok = format!("{base}utc_offset = \"+08:00\"\n");
        let config = Config::parse(&ok).unwrap();
        assert_eq!(config.providers["zhipu"].utc_offset_minutes(), 480);

        let unset = Config::parse(base).unwrap();
        assert_eq!(unset.providers["zhipu"].utc_offset_minutes(), 0);

        let bad = format!("{base}utc_offset = \"Asia/Shanghai\"\n");
        let err = Config::parse(&bad).unwrap_err().to_string();
        assert!(err.contains("utc_offset"), "{err}");
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.server.listen.port(), 8080);
        assert_eq!(config.providers.len(), 1);
        assert_eq!(config.providers["openai"].kind, ProviderKind::OpenAI);
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"
threads = 4

[auth]
api_keys = ["key1", "key2"]

[rate_limit]
requests_per_minute = 600

[metrics]
enabled = true
listen = "0.0.0.0:9090"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
default_model = "gpt-4o"

[providers.anthropic]
kind = "anthropic"
base_url = "https://api.anthropic.com"
api_key = "sk-ant-test"

[[routes]]
path = "/v1/chat/completions"
provider = "openai"
fallback = ["anthropic"]
timeout_secs = 120
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.providers.len(), 2);
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.routes[0].fallback, vec!["anthropic"]);
        assert!(config.auth.is_some());
        assert!(config.metrics.enabled);
    }

    #[test]
    fn test_tool_schema_dialect_is_parsed() {
        let config: Config = toml::from_str(
            r#"
[server]
listen = "127.0.0.1:8080"

[providers.kimi]
kind = "openai_compat"
base_url = "https://api.kimi.com"
tool_schema = "moonshot"
"#,
        )
        .expect("parse");
        assert_eq!(
            config.providers["kimi"].tool_schema.as_deref(),
            Some("moonshot")
        );

        // Unset means the gateway picks by upstream host.
        let config: Config = toml::from_str(
            r#"
[server]
listen = "127.0.0.1:8080"

[providers.kimi]
kind = "openai_compat"
base_url = "https://api.kimi.com"
"#,
        )
        .expect("parse");
        assert!(config.providers["kimi"].tool_schema.is_none());
    }

    #[test]
    fn test_unknown_provider_in_route() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"

[[routes]]
path = "/v1/chat"
provider = "nonexistent"
"#;
        let err = Config::parse(toml).unwrap_err();
        assert!(err.to_string().contains("unknown provider"));
    }

    #[test]
    fn test_env_var_expansion() {
        std::env::set_var("MAXLLM_TEST_KEY", "expanded-value");
        let result = expand_env_vars("key = \"${MAXLLM_TEST_KEY}\"");
        assert_eq!(result, "key = \"expanded-value\"");
        std::env::remove_var("MAXLLM_TEST_KEY");
    }

    #[test]
    fn test_default_values() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        let p = &config.providers["openai"];
        assert_eq!(p.max_fails, 3);
        assert_eq!(p.fail_timeout_secs, 60);
        assert!(config.server.tcp_reuseport);
    }

    #[test]
    fn test_grace_period_defaults_to_thirty_seconds() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.server.grace_period_secs, 30);
    }

    #[test]
    fn test_grace_period_is_configurable() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"
grace_period_secs = 5

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.server.grace_period_secs, 5);
    }

    #[test]
    fn test_connect_timeout_defaults_to_unset() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        let p = &config.providers["openai"];
        assert_eq!(p.connect_timeout_secs, None);
        assert_eq!(p.total_connect_timeout_secs, None);
    }

    #[test]
    fn test_connect_timeout_is_parsed() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
connect_timeout_secs = 30
total_connect_timeout_secs = 60
"#;
        let config = Config::parse(toml).unwrap();
        let p = &config.providers["opencode"];
        assert_eq!(p.connect_timeout_secs, Some(30));
        assert_eq!(p.total_connect_timeout_secs, Some(60));
    }

    /// The stall budget: how long an upstream may stay silent before the
    /// gateway gives up on it. Unset means the route's timeout applies.
    #[test]
    fn test_read_timeout_defaults_to_unset_and_is_parsed() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.providers["openai"].read_timeout_secs, None);

        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
read_timeout_secs = 45
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.providers["opencode"].read_timeout_secs, Some(45));
    }

    #[test]
    fn test_first_byte_timeout_defaults_to_unset_and_is_parsed() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.providers["openai"].first_byte_timeout_secs, None);

        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
first_byte_timeout_secs = 20
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(
            config.providers["opencode"].first_byte_timeout_secs,
            Some(20)
        );
    }

    #[test]
    fn test_commandcode_kind_is_parsed() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.cmd]
kind = "commandcode"
base_url = "https://api.commandcode.ai"
api_key = "cmd-test"
default_model = "deepseek/deepseek-v4.1-flash"
"#;
        let config = Config::parse(toml).unwrap();
        let p = &config.providers["cmd"];
        assert_eq!(p.kind, ProviderKind::CommandCode);
        assert_eq!(p.base_url, "https://api.commandcode.ai");
        assert_eq!(
            p.default_model.as_deref(),
            Some("deepseek/deepseek-v4.1-flash")
        );
        // The Responses route is native for this kind; the config may still
        // override it.
        assert_eq!(p.native_responses, None);
    }
}

/// Layered quota-reset-time resolution (see `rate_limit.rs` in the gateway).
///
/// When a provider answers 429, the gateway walks the layers in order of
/// increasing cost to find the moment the quota resets, then force-opens the
/// provider's circuit breaker until that exact instant — no blind cooldown
/// retry against a quota-exhausted channel.
#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitLearningConfig {
    /// Master switch. When false the gateway falls back to its plain
    /// `Retry-After`-header cooldown (the pre-learning behaviour).
    #[serde(default = "default_false")]
    pub enabled: bool,
    /// Base URL of the Laya decision server (`POST /v1/systemone`).
    /// Only consulted when the deterministic layers cannot pin the reset
    /// time; an unreachable server falls back to the default cooldown.
    #[serde(default)]
    pub laya_url: Option<String>,
    /// Laya model id (defaults to the soft-decider deployment).
    #[serde(default)]
    pub laya_model: Option<String>,
    /// Laya call budget in milliseconds. A slow decision must never delay
    /// the failover chain; the gateway falls back to the default cooldown
    /// when this budget is exceeded.
    #[serde(default = "default_laya_timeout_ms")]
    pub laya_timeout_ms: u64,
    /// Noul probability threshold: a laya answer above this classifies the
    /// error as a rate limit; below it the error takes the ordinary-failure
    /// path (no quota force-open).
    #[serde(default = "default_rate_limit_threshold")]
    pub rate_limit_threshold: f64,
    /// Cooldown (seconds) applied when the error is a rate limit but no
    /// layer resolved a precise reset instant. Mirrors litellm_lb_go's
    /// 5-minute probing default.
    #[serde(default = "default_quota_cooldown_secs")]
    pub default_cooldown_secs: u64,
    /// Cap the number of learned patterns kept in the store (LRU by hit
    /// recency). 0 disables learning persistence.
    #[serde(default = "default_max_learned_patterns")]
    pub max_learned_patterns: usize,
}

fn default_laya_timeout_ms() -> u64 {
    1000
}
fn default_rate_limit_threshold() -> f64 {
    0.5
}
fn default_quota_cooldown_secs() -> u64 {
    300
}
fn default_max_learned_patterns() -> usize {
    1000
}

impl Default for RateLimitLearningConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            laya_url: None,
            laya_model: None,
            laya_timeout_ms: default_laya_timeout_ms(),
            rate_limit_threshold: default_rate_limit_threshold(),
            default_cooldown_secs: default_quota_cooldown_secs(),
            max_learned_patterns: default_max_learned_patterns(),
        }
    }
}

#[cfg(test)]
mod mutation_tests {
    use super::*;

    /// The serde default functions are part of the config contract: every
    /// one of them is asserted here so a changed default fails a test
    /// instead of silently changing the runtime behaviour.
    #[test]
    fn test_provider_defaults_are_exact() {
        assert_eq!(default_max_fails(), 3);
        assert_eq!(default_fail_timeout_secs(), 60);
        assert_eq!(default_weight(), 100);
        assert_eq!(default_retry_backoff_ms(), 500);
        assert_eq!(default_max_failover_attempts(), 6);
        assert!(!default_false());
    }

    #[test]
    fn test_session_and_recording_defaults_are_exact() {
        assert_eq!(default_session_timeout_secs(), 3600);
        assert_eq!(default_session_max_entries(), 10_000);
        assert_eq!(default_recording_dir(), "./recordings");
        assert_eq!(default_recording_max_body_bytes(), 8192);
        assert_eq!(default_recording_max_file_bytes(), 64 * 1024 * 1024);
        assert_eq!(default_recording_retention_hours(), 168);
        // Redaction is a safety default, not an opt-in: a `[recording]` block
        // written before the switch existed must still scrub bodies.
        assert!(default_true());
        let cfg: RecordingConfig =
            toml::from_str("enabled = true\ndir = \"/tmp/rec\"\n").expect("recording config");
        assert!(cfg.redact);
        assert!(cfg.redact_pii);
    }

    #[test]
    fn test_auto_model_defaults_are_exact() {
        assert_eq!(default_auto_model_name(), "auto");
        assert!(default_true());
    }

    #[test]
    fn test_guardrail_and_laya_defaults_are_exact() {
        assert_eq!(default_guardrail_mode(), "pre_call");
        assert_eq!(default_guardrail_action(), "block");
        assert_eq!(default_laya_timeout_ms(), 1000);
        assert_eq!(default_rate_limit_threshold(), 0.5);
        assert_eq!(default_quota_cooldown_secs(), 300);
        assert_eq!(default_max_learned_patterns(), 1000);
    }

    const PROVIDER_TOML: &str = r#"
[server]
listen = "0.0.0.0:8080"

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "sk-test"
"#;

    #[test]
    fn test_auto_model_route_with_a_valid_pool_is_accepted() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
enabled = true
pool = [{{ provider = "openai", model = "gpt-4o" }}]

[[routes]]
path = "/v1/auto"
provider = "auto"
auto_model = true
"#
        );
        let config = Config::parse(&toml).unwrap();
        assert!(config.routes[0].auto_model);
        let pool = config.auto_model.as_ref().unwrap();
        assert!(pool.enabled);
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool[0].model, "gpt-4o");
        assert!(
            pool.prefer_fast,
            "prefer_fast must default to true so the latency filter is on"
        );
    }

    #[test]
    fn test_auto_model_prefer_fast_can_be_turned_off() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
enabled = true
prefer_fast = false
pool = [{{ provider = "openai", model = "gpt-4o" }}]
"#
        );
        let config = Config::parse(&toml).unwrap();
        assert!(!config.auto_model.as_ref().unwrap().prefer_fast);
    }

    #[test]
    fn test_auto_model_route_with_an_empty_pool_is_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
enabled = true

[[routes]]
path = "/v1/auto"
provider = "auto"
auto_model = true
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("auto_model"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_auto_model_route_with_a_disabled_pool_is_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
enabled = false
pool = [{{ provider = "openai", model = "gpt-4o" }}]

[[routes]]
path = "/v1/auto"
provider = "auto"
auto_model = true
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("auto_model"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_named_auto_pool_uses_the_table_key_as_its_sentinel() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
enabled = true
pool = [{{ provider = "openai", model = "gpt-4o" }}]

[auto_models.auto_home]
pool = [{{ provider = "openai", model = "local-model" }}]
"#
        );
        let config = Config::parse(&toml).unwrap();
        let pools = config.auto_pools();
        assert_eq!(pools.len(), 2);
        assert_eq!(pools[0].0, "auto");
        assert_eq!(pools[1].0, "auto_home");
        assert_eq!(pools[1].1.pool[0].model, "local-model");
    }

    #[test]
    fn test_named_auto_pool_honours_an_explicit_name() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_models.anything]
name = "auto_lan"
pool = [{{ provider = "openai", model = "local-model" }}]
"#
        );
        let config = Config::parse(&toml).unwrap();
        let pools = config.auto_pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].0, "auto_lan");
    }

    #[test]
    fn test_duplicate_auto_pool_names_are_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
pool = [{{ provider = "openai", model = "gpt-4o" }}]

[auto_models.auto]
pool = [{{ provider = "openai", model = "local-model" }}]
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("declared twice"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_auto_pool_entry_billing_defaults_to_subscription() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
pool = [{{ provider = "openai", model = "gpt-4o" }}]
"#
        );
        let config = Config::parse(&toml).unwrap();
        let pool = &config.auto_model.as_ref().unwrap().pool;
        assert_eq!(pool[0].billing, BillingMode::Subscription);
    }

    #[test]
    fn test_auto_pool_entry_billing_parses_metered() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
pool = [
  {{ provider = "openai", model = "gpt-4o" }},
  {{ provider = "openai", model = "deepseek", billing = "metered" }},
]
"#
        );
        let config = Config::parse(&toml).unwrap();
        let pool = &config.auto_model.as_ref().unwrap().pool;
        assert_eq!(pool[0].billing, BillingMode::Subscription);
        assert_eq!(pool[1].billing, BillingMode::Metered);
    }

    #[test]
    fn test_auto_pool_entry_billing_rejects_an_unknown_value() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
pool = [{{ provider = "openai", model = "gpt-4o", billing = "prepaid" }}]
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("billing"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_auto_route_naming_a_disabled_pool_is_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[auto_model]
pool = [{{ provider = "openai", model = "gpt-4o" }}]

[auto_models.auto_home]
enabled = false
pool = [{{ provider = "openai", model = "local-model" }}]

[[routes]]
path = "/v1/responses/auto_home"
provider = "auto_home"
auto_model = true
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("auto_home"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unknown_global_plugin_is_rejected() {
        // Top-level TOML keys must appear before the first `[section]` header.
        let toml = format!(
            r#"global_plugins = ["does-not-exist"]
{PROVIDER_TOML}"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("global_plugins references unknown plugin"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_known_global_plugin_is_accepted() {
        // Top-level TOML keys must appear before the first `[section]` header.
        let toml = format!(
            r#"global_plugins = ["auth"]

{PROVIDER_TOML}
[plugins.auth]
category = "key_auth"
"#
        );
        let config = Config::parse(&toml).unwrap();
        assert_eq!(config.global_plugins, vec!["auth"]);
        assert_eq!(config.plugins["auth"].category, "key_auth");
    }

    #[test]
    fn test_unknown_route_plugin_is_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[[routes]]
path = "/v1/chat"
provider = "openai"
plugins = ["does-not-exist"]
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string().contains("references unknown plugin"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_known_route_plugin_is_accepted() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[plugins.auth]
category = "key_auth"

[[routes]]
path = "/v1/chat"
provider = "openai"
plugins = ["auth"]
"#
        );
        let config = Config::parse(&toml).unwrap();
        assert_eq!(config.routes[0].plugins, vec!["auth"]);
    }

    #[test]
    fn test_unknown_fallback_provider_is_rejected() {
        let toml = format!(
            r#"{PROVIDER_TOML}
[[routes]]
path = "/v1/chat"
provider = "openai"
fallback = ["does-not-exist"]
"#
        );
        let err = Config::parse(&toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("fallback references unknown provider"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_config_without_any_provider_is_rejected() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"
"#;
        let err = Config::parse(toml).unwrap_err();
        // `providers` is a required TOML field, so an empty config fails at
        // serde time (missing field) before validation runs.
        assert!(
            err.to_string().contains("providers"),
            "unexpected error: {err}"
        );
    }

    // ── auto pool scope: the deployment boundary ────────────────────────
    //
    // `auto_home` must never be able to reach a vendor, and `auto` must
    // never spend a home GPU. These are the tests that keep the two pools
    // from silently trading places when someone edits the config.

    #[test]
    fn test_self_hosted_is_inferred_from_the_base_url() {
        let mut p = Config::parse(
            r#"
[server]
listen = "0.0.0.0:8080"

[providers.home]
kind = "openai_compat"
base_url = "http://10.126.126.20:11081"
api_key = "x"
"#,
        )
        .unwrap()
        .providers
        .remove("home")
        .unwrap();
        assert!(p.is_self_hosted(), "RFC1918 host must read as self-hosted");

        for (url, want) in [
            ("http://127.0.0.1:8000", true),
            ("http://localhost:8000/v1", true),
            ("http://192.168.2.190:3795", true),
            ("http://172.16.4.9:11060", true),
            ("http://100.64.7.3:11082", true), // CGNAT / mesh
            ("http://[::1]:8000", true),
            ("http://gpu-box.local:8000", true),
            ("https://api.minimaxi.com", false),
            ("https://opencode.ai", false),
            ("https://api.openai.com/v1", false),
            ("http://10.126.126.199:3795", true), // private mesh address
        ] {
            p.base_url = url.to_string();
            assert_eq!(p.is_self_hosted(), want, "base_url={url}");
        }
    }

    #[test]
    fn test_explicit_self_hosted_overrides_the_inference() {
        let mut p = Config::parse(
            r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu]
kind = "openai_compat"
base_url = "https://gpu.example.com"
self_hosted = true
api_key = "x"
"#,
        )
        .unwrap()
        .providers
        .remove("gpu")
        .unwrap();
        assert!(p.is_self_hosted());

        p.base_url = "http://10.0.0.5:8000".into();
        p.self_hosted = Some(false);
        assert!(!p.is_self_hosted());
    }

    #[test]
    fn test_self_hosted_pool_rejects_a_vendor_member() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11081"
api_key = "x"
default_model = "groot-awq"

[providers.cloud]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "x"
default_model = "deepseek-v4.1-flash"

[auto_models.auto_home]
scope = "self_hosted"

[[auto_models.auto_home.pool]]
provider = "gpu1"
model = "groot-awq"

[[auto_models.auto_home.pool]]
provider = "cloud"
model = "deepseek-v4.1-flash"
"#;
        let err = Config::parse(toml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auto pool 'auto_home'"), "{msg}");
        assert!(msg.contains("self-hosted"), "{msg}");
        assert!(msg.contains("'cloud'"), "{msg}");
    }

    #[test]
    fn test_self_hosted_pool_accepts_only_private_members() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[providers.gpu2]
kind = "openai_compat"
base_url = "http://10.126.126.20:11080"
api_key = "x"
default_model = "groot-awq"

[providers.humanizer]
kind = "openai_compat"
base_url = "http://10.126.126.10:11060"
api_key = "x"
default_model = "humanizer-12b"

[auto_models.auto_home]
scope = "self_hosted"

[[auto_models.auto_home.pool]]
provider = "gpu1"
model = "groot-awq"
[[auto_models.auto_home.pool]]
provider = "gpu2"
model = "groot-awq"
[[auto_models.auto_home.pool]]
provider = "humanizer"
model = "humanizer-12b"
"#;
        Config::parse(toml).expect("all three members are self-hosted");
    }

    #[test]
    fn test_pool_model_allowlist_rejects_a_foreign_member() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[providers.humanizer]
kind = "openai_compat"
base_url = "http://10.126.126.10:11060"
api_key = "x"
default_model = "humanizer-12b"

[auto_models.auto_home]
scope = "self_hosted"
allowed_models = ["groot-awq"]

[[auto_models.auto_home.pool]]
provider = "gpu1"
model = "groot-awq"
[[auto_models.auto_home.pool]]
provider = "humanizer"
model = "humanizer-12b"
"#;
        let err = Config::parse(toml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auto pool 'auto_home'"), "{msg}");
        assert!(msg.contains("groot-awq"), "{msg}");
        assert!(msg.contains("humanizer-12b"), "{msg}");
        assert!(msg.contains("allowed_models"), "{msg}");
    }

    #[test]
    fn test_pool_model_allowlist_accepts_the_listed_members() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[providers.gpu2]
kind = "openai_compat"
base_url = "http://10.126.126.20:11080"
api_key = "x"
default_model = "groot-awq"

[auto_models.auto_home]
scope = "self_hosted"
allowed_models = ["groot-awq"]

[[auto_models.auto_home.pool]]
provider = "gpu1"
model = "groot-awq"
[[auto_models.auto_home.pool]]
provider = "gpu2"
model = "groot-awq"
"#;
        Config::parse(toml).expect("every member serves an allowed model");
    }

    #[test]
    fn test_pool_model_allowlist_catches_a_provider_defaulting_outside_it() {
        // The entry claims an allowed model, but the provider's own default
        // is a different one: the member would answer with a model the pool
        // never allowed, so the load has to fail.
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.humanizer]
kind = "openai_compat"
base_url = "http://10.126.126.10:11060"
api_key = "x"
default_model = "humanizer-12b"

[auto_models.auto_home]
scope = "self_hosted"
allowed_models = ["groot-awq"]

[[auto_models.auto_home.pool]]
provider = "humanizer"
model = "groot-awq"
"#;
        let err = Config::parse(toml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auto pool 'auto_home'"), "{msg}");
        assert!(msg.contains("defaults to 'humanizer-12b'"), "{msg}");
    }

    #[test]
    fn test_pool_model_allowlist_defaults_to_unrestricted() {
        // No `allowed_models` means no model boundary at all — the behaviour
        // every config had before the field existed.
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[providers.cloud]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "x"
default_model = "deepseek-v4.1-flash"

[auto_models.anything]
scope = "any"

[[auto_models.anything.pool]]
provider = "gpu1"
model = "groot-awq"
[[auto_models.anything.pool]]
provider = "cloud"
model = "deepseek-v4.1-flash"
"#;
        Config::parse(toml).expect("without allowed_models any model is allowed");
    }

    #[test]
    fn test_vendor_pool_rejects_a_self_hosted_member() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[auto_model]
name = "auto"
scope = "vendor"

[[auto_model.pool]]
provider = "gpu1"
model = "groot-awq"
"#;
        let err = Config::parse(toml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auto pool 'auto'"), "{msg}");
        assert!(msg.contains("external vendor"), "{msg}");
    }

    #[test]
    fn test_pool_scope_defaults_to_any() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[providers.cloud]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "x"
default_model = "deepseek-v4.1-flash"

[auto_model]
[[auto_model.pool]]
provider = "gpu1"
model = "groot-awq"
[[auto_model.pool]]
provider = "cloud"
model = "deepseek-v4.1-flash"
"#;
        let cfg = Config::parse(toml).expect("mixed pool without a scope stays legal");
        assert_eq!(cfg.auto_model.as_ref().unwrap().scope, AutoPoolScope::Any);
    }

    #[test]
    fn test_scoped_pool_rejects_an_unknown_member() {
        let toml = r#"
[server]
listen = "0.0.0.0:8080"

[providers.gpu1]
kind = "openai_compat"
base_url = "http://10.126.126.20:11082"
api_key = "x"
default_model = "groot-awq"

[auto_models.auto_home]
scope = "self_hosted"

[[auto_models.auto_home.pool]]
provider = "ghost"
model = "groot-awq"
"#;
        let err = Config::parse(toml).unwrap_err();
        assert!(
            err.to_string().contains("unknown provider 'ghost'"),
            "{err}"
        );
    }
}
