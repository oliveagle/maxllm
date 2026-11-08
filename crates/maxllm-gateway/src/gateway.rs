// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::circuit_breaker::CircuitBreaker;
use crate::ctx::RequestCtx;
use crate::metrics::METRICS;
use crate::recorder::{
    RecordEntry, Recorder, EVENT_CLIENT_REQUEST, EVENT_CLIENT_RESPONSE, EVENT_ERROR,
    EVENT_PROVIDER_REQUEST, EVENT_PROVIDER_RESPONSE,
};
use crate::routing::{ProviderSelector, ProviderTarget};
use crate::session_sticky::SessionStore;

use ahash::AHashMap;
use async_trait::async_trait;
use bytes::Bytes;
use maxllm_config::{BillingMode, Config, EndpointType, ProviderKind, RouteConfig};
use maxllm_plugin::builtin::auto_compaction::{
    apply_llm_compaction, compact_messages, CompactionParams, CompactionResult,
};
use maxllm_plugin::guardrail::{
    self, GuardrailEngine, GuardrailInput, GuardrailOutput, GuardrailVerdict,
};
use maxllm_plugin::{Plugin, PluginChain};
use maxllm_translate::{ProviderTranslator, StreamTranslator, TranslatedRequest};
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::proxy::{FailToProxy, ProxyHttp, Session};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Shared HTTP client for LLM-based auto-compaction summarization calls.
static LLM_HTTP_CLIENT: once_cell::sync::Lazy<reqwest::Client> = once_cell::sync::Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("reqwest client init failed")
});

/// Process-wide circuit breakers, keyed by provider name.
///
/// They live outside `HotState` on purpose: a config hot-reload rebuilds
/// every `ProviderState`, and a breaker that started from scratch on each
/// reload would forget an open circuit — and a quota force-open — exactly
/// when the gateway is being reconfigured.
static BREAKERS: once_cell::sync::Lazy<Mutex<HashMap<String, Arc<CircuitBreaker>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Get (or create) the circuit breaker of a provider. The breaker instance is
/// shared across hot reloads so its state survives them.
fn breaker_for(name: &str, max_fails: u32, fail_timeout_secs: u64) -> Arc<CircuitBreaker> {
    let mut breakers = match BREAKERS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    breakers
        .entry(name.to_string())
        .or_insert_with(|| Arc::new(CircuitBreaker::new(max_fails, fail_timeout_secs)))
        .clone()
}

/// Get the shared breaker of a provider without creating one.
///
/// The breakers are created by `build_hot_state` with the provider's real
/// `max_fails` / `fail_timeout` config, so the startup restore path reuses
/// them instead of creating a second breaker with default parameters.
fn existing_breaker(name: &str) -> Option<Arc<CircuitBreaker>> {
    let breakers = match BREAKERS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    breakers.get(name).cloned()
}

/// Well-known models for each provider kind.
fn well_known_models(kind: ProviderKind) -> &'static [&'static str] {
    match kind {
        ProviderKind::OpenAI => &[
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4-turbo",
            "gpt-4",
            "gpt-3.5-turbo",
            "o1",
            "o1-mini",
            "o1-preview",
            "o3-mini",
        ],
        ProviderKind::Anthropic => &[
            "claude-sonnet-4-20250514",
            "claude-3-5-sonnet-20241022",
            "claude-3-5-haiku-20241022",
            "claude-3-opus-20240229",
        ],
        ProviderKind::Gemini => &[
            "gemini-2.5-flash",
            "gemini-2.5-pro",
            "gemini-2.0-flash",
            "gemini-1.5-pro",
            "gemini-1.5-flash",
        ],
        ProviderKind::AzureOpenai => &["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
        ProviderKind::Bedrock => &[
            "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "anthropic.claude-3-haiku-20240307-v1:0",
        ],
        ProviderKind::Groq => &[
            "llama-3.3-70b-versatile",
            "llama-3.1-8b-instant",
            "mixtral-8x7b-32768",
        ],
        ProviderKind::Together => &["meta-llama/Llama-3.3-70B-Instruct-Turbo"],
        ProviderKind::Fireworks => &["accounts/fireworks/models/llama-v3p3-70b-instruct"],
        ProviderKind::DeepInfra => &["meta-llama/Llama-3.3-70B-Instruct"],
        ProviderKind::Mistral => &[
            "mistral-large-latest",
            "mistral-small-latest",
            "open-mistral-nemo",
        ],
        ProviderKind::XAI => &["grok-2", "grok-2-mini"],
        ProviderKind::DeepSeek => &["deepseek-chat", "deepseek-reasoner"],
        ProviderKind::Ollama => &["llama3.2", "mistral", "codellama"],
        ProviderKind::Cohere => &["command-r-plus", "command-r", "command-light"],
        ProviderKind::OpenaiCompat => &[],
        // Every id here was verified served by the live `/zen/go/v1` endpoint
        // (probed 2026-11-03). `kimi-k2.5` was dropped from the upstream's
        // model list (400 "Model is unavailable"); region-restricted
        // contributor models (beam-free, muse-spark-*, gpt-*-luna) are not
        // advertised either.
        ProviderKind::OpenCode => &[
            "deepseek-v4.1-flash",
            "deepseek-v4-pro",
            "glm-5.2",
            "kimi-k2.7-code",
        ],
        // commandcode.ai serves ~96 models, but this deployment deliberately
        // exposes one: `deepseek/deepseek-v4.1-flash` is the default model of
        // every commandcode provider entry (and the one the auto pool may
        // carry). The full catalogue lives upstream at
        // `GET https://api.commandcode.ai/provider/v1/models` — add a name
        // here when a deployment actually wants it advertised.
        ProviderKind::CommandCode => &["deepseek/deepseek-v4.1-flash"],
        // The OpenDesign AMR link serves an account-specific catalogue
        // (`GET /v1/models` on the live endpoint returned exactly one
        // enabled model for the probed key: `deepseek-v4.1-flash`, marked
        // `default: true`). The ids below are the chat models OpenDesign's
        // own runtime prefers, in the order it ranks them
        // (`PREFERRED_AMR_CHAT_MODEL_ORDER` in the open-design repo); a
        // deployment pins the one its account carries with `default_model`.
        ProviderKind::OpenDesign => &[
            "deepseek-v4.1-flash",
            "deepseek-v4-flash",
            "deepseek-v4-pro",
        ],
    }
}

/// Map a provider kind to its "owned_by" value for the models endpoint.
fn provider_owned_by(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::OpenAI => "openai",
        ProviderKind::Anthropic => "anthropic",
        ProviderKind::Gemini => "google",
        ProviderKind::AzureOpenai => "azure-openai",
        ProviderKind::Bedrock => "aws-bedrock",
        ProviderKind::Groq => "groq",
        ProviderKind::Together => "together",
        ProviderKind::Fireworks => "fireworks",
        ProviderKind::DeepInfra => "deepinfra",
        ProviderKind::Mistral => "mistral",
        ProviderKind::XAI => "xai",
        ProviderKind::DeepSeek => "deepseek",
        ProviderKind::Ollama => "ollama",
        ProviderKind::Cohere => "cohere",
        ProviderKind::OpenaiCompat => "custom",
        ProviderKind::OpenCode => "opencode",
        // Matches the `owned_by` field commandcode.ai publishes in its own
        // model list.
        ProviderKind::CommandCode => "command-code",
        // The OpenDesign AMR link publishes `owned_by: "vela"` in its own
        // model list; maxllm names the provider it talks to.
        ProviderKind::OpenDesign => "open-design",
    }
}

/// User-Agent injected on upstream requests to the vendors whose Cloudflare
/// edge bans script / HTTP-client fingerprints (HTTP 403 "error code: 1010",
/// browser signature banned): opencode.ai and commandcode.ai both do.
/// `curl/8.5.0` is verified allowed by both.
const SAFE_UPSTREAM_UA: &str = "curl/8.5.0";

/// True when a User-Agent string is one the Cloudflare edge of opencode.ai
/// and commandcode.ai verifiably bans (HTTP 403 "error code: 1010"). Probing
/// the live upstreams shows the ban list is narrow: only `Python-urllib/*`
/// (the default UA of Python's urllib) is blocked; curl, browsers and every
/// other HTTP-client / SDK UA tested (OpenAI/Python, node-fetch, axios,
/// python-requests, python-urllib3, ...) are accepted, so they are never
/// rewritten here.
///
/// The marker is `python-urllib/` — the real urllib UA always carries a
/// version after the slash (`Python-urllib/3.11`). The urllib3 *package*
/// sends `python-urllib3/2.0`, which is a different, allowed fingerprint and
/// must not be clobbered.
fn is_script_like_user_agent(ua_lower: &str) -> bool {
    const BANNED_UA_MARKERS: [&str; 1] = ["python-urllib/"];
    BANNED_UA_MARKERS.iter().any(|m| ua_lower.contains(m))
}

/// Default TCP connect timeout to an upstream, in seconds.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default request-level failover budget: how many providers one request may
/// walk when providers fail. Matches litellm_lb_go's
/// `maxResponsesFailoverAttempts` (six upstreams, one past the largest
/// migrated one-api channel pool).
const DEFAULT_MAX_FAILOVER_ATTEMPTS: u32 = 6;

/// Default backoff between two same-provider retries, in milliseconds.
const DEFAULT_RETRY_BACKOFF_MS: u64 = 500;

/// Resolve the upstream path of a native (provider-format) request.
///
/// Most providers serve the client's own path: Anthropic's `/v1/messages`,
/// Gemini's `/v1beta/models/…:generateContent`. A vendor that publishes the
/// Anthropic Messages contract under its own path is the exception —
/// commandcode.ai serves it at `/provider/v1/messages`, so a client speaking
/// the plain `/v1/messages` contract is rewritten onto the provider's path.
fn native_upstream_path(client_path: &str, provider_path: &str) -> String {
    if client_path.ends_with("/messages") && provider_path.ends_with("/messages") {
        return provider_path.to_string();
    }
    client_path.to_string()
}

/// Derive the native Responses path from a chat completions path.
/// `/zen/go/v1/chat/completions` → `/zen/go/v1/responses`.
fn derive_responses_path(chat_path: &str) -> Option<String> {
    let trimmed = chat_path.trim_end_matches('/');
    if let Some(prefix) = trimmed.strip_suffix("/chat/completions") {
        return Some(format!("{prefix}/responses"));
    }
    if let Some(prefix) = trimmed.strip_suffix("/completions") {
        return Some(format!("{prefix}/responses"));
    }
    None
}

/// Runtime state for a configured provider.
pub struct ProviderState {
    pub kind: ProviderKind,
    pub translator: Box<dyn ProviderTranslator>,
    /// Process-wide circuit breaker. Shared (Arc) so a config hot-reload,
    /// which rebuilds every `ProviderState`, keeps the breaker's state — an
    /// open circuit or a quota force-open must survive a reload.
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub api_key: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub sni: String,
    /// AWS region for Bedrock SigV4 signing (from the provider config).
    pub region: Option<String>,
    pub weight: u32,
    /// Arbitrary provider tags from the config. Reserved for tag-based
    /// routing; no routing rule consumes them yet.
    #[allow(dead_code)]
    pub tags: Vec<String>,
    pub default_model: Option<String>,
    /// Whether the provider speaks the Responses contract natively.
    pub native_responses: bool,
    /// Upstream path of the native Responses endpoint.
    pub responses_path: Option<String>,
    /// TCP connect timeout to the upstream.
    pub connect_timeout: Duration,
    /// Total budget for the upstream connection (TCP connect + TLS).
    pub total_connect_timeout: Option<Duration>,
    /// Fixed UTC offset (minutes east of UTC) used to read zone-less reset
    /// timestamps in this upstream's rate-limit messages. 0 = UTC.
    pub utc_offset_minutes: i32,
}

/// Hot-reloadable state — swapped atomically on config change.
pub struct HotState {
    pub providers: AHashMap<String, Arc<ProviderState>>,
    pub routes: Vec<RouteConfig>,
    /// Global session-sticky switch (the `[session_sticky]` section). A route
    /// also has to opt in with `session_sticky = true`.
    pub session_sticky_enabled: bool,
    pub metrics_enabled: bool,
    pub metrics_allow_remote: bool,
    pub global_chain: PluginChain,
    pub route_chains: Vec<PluginChain>,
    pub model_aliases: HashMap<String, String>,
    pub cost_calculator: Arc<maxllm_admin::CostCalculator>,
    pub guardrail_engine: Option<Arc<GuardrailEngine>>,
    pub models_response: Bytes,
    /// The rotation pools, in declaration order. Each pool owns one sentinel
    /// model name (`auto`, `auto_home`, …) and the (provider, model) pairs it
    /// rotates over. Entries whose provider is missing from the config are
    /// dropped at build time, so a pool can only contain usable providers.
    pub auto_pools: Vec<AutoPoolState>,
}

/// One resolved rotation pool: the sentinel model clients send plus its
/// entries.
#[derive(Debug, Clone)]
pub struct AutoPoolState {
    pub name: String,
    pub entries: Vec<AutoPoolEntry>,
    /// Whether the rotation prefers the low-latency cohort inside the
    /// preferred billing tier (see `[auto_model].prefer_fast`).
    pub prefer_fast: bool,
}

impl AutoPoolState {
    /// The pool entry for a provider, when that provider serves this pool.
    pub fn entry_for(&self, provider: &str) -> Option<&AutoPoolEntry> {
        self.entries.iter().find(|e| e.provider == provider)
    }
}

impl HotState {
    pub fn pool_named(&self, name: &str) -> Option<&AutoPoolState> {
        self.auto_pools.iter().find(|p| p.name == name)
    }

    /// The pool this request rotates over: the pool it already entered, else
    /// the sentinel carried by the model field, else — for a route that
    /// declares `auto_model` — the pool its `provider` names (falling back to
    /// the first declared pool, which keeps a single-pool config working).
    fn request_pool<'a>(
        &'a self,
        route: &RouteConfig,
        ctx: &RequestCtx,
    ) -> Option<&'a AutoPoolState> {
        if let Some(name) = ctx.auto_pool.as_deref() {
            if let Some(pool) = self.pool_named(name) {
                return Some(pool);
            }
        }
        if let Some(pool) = self.pool_named(&ctx.model) {
            return Some(pool);
        }
        if route.auto_model {
            return self
                .pool_named(&route.provider)
                .or_else(|| self.auto_pools.first());
        }
        None
    }
}

/// One entry of an auto-rotation pool, resolved against the providers.
#[derive(Debug, Clone)]
pub struct AutoPoolEntry {
    pub provider: String,
    pub model: String,
    /// Billing model: subscription entries always outrank metered ones.
    pub billing: BillingMode,
}

impl AutoPoolState {
    /// The pool entries a request may use, in billing-priority order.
    ///
    /// `usable` is the caller's per-request predicate (provider exists, its
    /// circuit is closed, it was not already tried, …). The tier is resolved
    /// against the *whole* pool: as long as one subscription entry is usable,
    /// only subscription entries are returned, so round-robin and failover
    /// can never reach a metered (pay-per-token) entry before every prepaid
    /// entry is exhausted. Only when no subscription entry is usable does the
    /// returned list fall back to the remaining (metered) entries.
    fn usable_entries<F>(&self, usable: F) -> Vec<&AutoPoolEntry>
    where
        F: Fn(&AutoPoolEntry) -> bool,
    {
        let subscription: Vec<&AutoPoolEntry> = self
            .entries
            .iter()
            .filter(|e| e.billing == BillingMode::Subscription)
            .filter(|e| usable(e))
            .collect();
        if !subscription.is_empty() {
            return subscription;
        }
        self.entries.iter().filter(|e| usable(e)).collect()
    }
}

/// The main AI gateway — implements Pingora's ProxyHttp trait.
pub struct AiGateway {
    /// Hot-reloadable state, atomically swappable via ArcSwap.
    /// Wrapped in Arc so a clone can be shared with the file watcher thread.
    pub hot: Arc<arc_swap::ArcSwap<HotState>>,
    /// Budget enforcer for per-key budget tracking.
    #[allow(dead_code)]
    pub budget_enforcer: Option<Arc<maxllm_admin::BudgetEnforcer>>,
    /// Per-key rate limiter (RPM / TPM / concurrency) for virtual keys.
    pub key_rate_limiter: Arc<crate::key_rate_limit::KeyRateLimiter>,
    /// Admin API handler (None if admin not configured).
    pub admin_api: Option<Arc<maxllm_admin::AdminApi>>,
    /// Session → provider bindings for session-sticky routing. Kept outside
    /// `HotState` so bindings survive a config hot-reload.
    pub sessions: Arc<SessionStore>,
    /// Optional JSONL request/response recorder (audit trail).
    pub recorder: Option<Arc<Recorder>>,
    /// Layered quota-reset-time resolver (headers → learned patterns → body
    /// regex → laya decision). Replaces the blind cooldown a 429 used to
    /// trigger with an exact "this provider is available again at T".
    pub rate_limit_resolver: Arc<crate::rate_limit_resolver::RateLimitResolver>,
}

/// Build the hot-reloadable state from a config.
/// Extracted so it can be called both at startup and on reload.
pub fn build_hot_state(config: &Config) -> Result<HotState, Box<dyn std::error::Error>> {
    let mut providers = AHashMap::new();

    for (name, conf) in &config.providers {
        // The upstream path the translator will use. Most kinds keep their
        // default inside the translator; commandcode.ai's default is needed
        // out here as well, because the native Responses path is derived
        // from it.
        let effective_chat_path: Option<&str> = match conf.kind {
            ProviderKind::CommandCode => Some(
                conf.upstream_path
                    .as_deref()
                    .unwrap_or(maxllm_translate::commandcode::DEFAULT_CHAT_PATH),
            ),
            // The OpenDesign link serves its Chat Completions route at
            // /v1/chat/completions; the default is needed out here as well,
            // because the native Responses path is derived from it.
            ProviderKind::OpenDesign => Some(
                conf.upstream_path
                    .as_deref()
                    .unwrap_or(maxllm_translate::open_design::DEFAULT_CHAT_PATH),
            ),
            _ => conf.upstream_path.as_deref(),
        };

        let translator: Box<dyn ProviderTranslator> = match conf.kind {
            ProviderKind::OpenAI => Box::new(maxllm_translate::openai::OpenAITranslator),
            ProviderKind::Anthropic => Box::new(maxllm_translate::anthropic::AnthropicTranslator),
            ProviderKind::Gemini => Box::new(maxllm_translate::gemini::GeminiTranslator::new()),
            ProviderKind::AzureOpenai => {
                let deployment = conf.deployment.as_deref().unwrap_or("gpt-4o");
                let api_version = conf.api_version.as_deref().unwrap_or("2024-02-01");
                Box::new(maxllm_translate::azure_openai::AzureOpenAITranslator::new(
                    deployment,
                    api_version,
                ))
            }
            ProviderKind::Bedrock => Box::new(maxllm_translate::bedrock::BedrockTranslator::new()),
            ProviderKind::Groq => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::groq())
            }
            ProviderKind::Together => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::together())
            }
            ProviderKind::Fireworks => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::fireworks())
            }
            ProviderKind::DeepInfra => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::deepinfra())
            }
            ProviderKind::Mistral => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::mistral())
            }
            ProviderKind::XAI => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::xai())
            }
            ProviderKind::DeepSeek => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::deepseek())
            }
            ProviderKind::Ollama => {
                Box::new(maxllm_translate::openai_compat::OpenAICompatTranslator::ollama())
            }
            ProviderKind::Cohere => Box::new(maxllm_translate::cohere::CohereTranslator),
            ProviderKind::OpenaiCompat => {
                let path = conf
                    .upstream_path
                    .as_deref()
                    .unwrap_or("/v1/chat/completions");
                Box::new(
                    maxllm_translate::openai_compat::OpenAICompatTranslator::new(
                        name.clone(),
                        path.to_string(),
                        maxllm_translate::openai_compat::AuthStyle::Bearer,
                    ),
                )
            }
            ProviderKind::OpenCode => {
                let path = conf
                    .upstream_path
                    .as_deref()
                    .unwrap_or("/zen/go/v1/chat/completions");
                Box::new(
                    maxllm_translate::opencode::OpenCodeTranslator::with_options(
                        name.clone(),
                        path.to_string(),
                        conf.skip_reasoning_injection.unwrap_or(false),
                    ),
                )
            }
            ProviderKind::CommandCode => {
                // commandcode.ai namespaces every route under `/provider`.
                // The default is the OpenAI Chat Completions route; a
                // provider entry whose `upstream_path` points at
                // `/provider/v1/messages` speaks the Anthropic Messages
                // contract instead — the only route that serves the vendor's
                // Claude models.
                Box::new(maxllm_translate::commandcode::CommandCodeTranslator::new(
                    name.clone(),
                    effective_chat_path
                        .unwrap_or(maxllm_translate::commandcode::DEFAULT_CHAT_PATH)
                        .to_string(),
                ))
            }
            ProviderKind::OpenDesign => {
                // The OpenDesign AMR link is OpenAI-compatible with one
                // repair (tool_choice: "any" is rejected upstream) and
                // serves the Responses contract natively.
                Box::new(maxllm_translate::open_design::OpenDesignTranslator::new(
                    name.clone(),
                    effective_chat_path
                        .unwrap_or(maxllm_translate::open_design::DEFAULT_CHAT_PATH)
                        .to_string(),
                ))
            }
        };

        let url: url::Url = conf.base_url.parse().map_err(|e: url::ParseError| {
            format!("invalid base_url for provider '{}': {}", name, e)
        })?;

        let tls = url.scheme() == "https";
        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(if tls { 443 } else { 80 });

        // A provider may serve the Responses contract natively. The path is
        // derived from the chat path (`…/chat/completions` → `…/responses`)
        // unless the config names it explicitly.
        let native_responses = conf.native_responses.unwrap_or(match conf.kind {
            ProviderKind::OpenCode => true,
            // commandcode.ai serves the Responses contract natively on
            // `/provider/v1/responses` (verified 2026-11-08: the route
            // answers 401 without a key, so it exists). A commandcode entry
            // that points at the Anthropic Messages route serves Claude
            // models instead and has no Responses route of its own.
            ProviderKind::CommandCode => {
                maxllm_translate::commandcode::CommandCodeWire::from_path(
                    effective_chat_path.unwrap_or(maxllm_translate::commandcode::DEFAULT_CHAT_PATH),
                ) == maxllm_translate::commandcode::CommandCodeWire::ChatCompletions
            }
            // The OpenDesign AMR link serves the Responses contract on
            // /v1/responses (verified 2026-11-08 against the live endpoint:
            // the route answers 200 with a Responses object and streams the
            // response.* event family).
            ProviderKind::OpenDesign => true,
            _ => false,
        });
        let responses_path = if native_responses {
            Some(
                conf.responses_path
                    .clone()
                    .or_else(|| effective_chat_path.and_then(derive_responses_path))
                    .unwrap_or_else(|| match conf.kind {
                        // The vendor's Responses route is namespaced under
                        // `/provider`, not the OpenAI default path.
                        ProviderKind::CommandCode => {
                            maxllm_translate::commandcode::DEFAULT_RESPONSES_PATH.to_string()
                        }
                        _ => "/v1/responses".to_string(),
                    }),
            )
        } else {
            None
        };

        providers.insert(
            name.clone(),
            Arc::new(ProviderState {
                kind: conf.kind,
                translator,
                circuit_breaker: breaker_for(name.as_str(), conf.max_fails, conf.fail_timeout_secs),
                api_key: conf.api_key.clone(),
                host: host.clone(),
                port,
                tls,
                sni: host,
                region: conf.region.clone(),
                weight: conf.weight,
                tags: conf.tags.clone(),
                default_model: conf.default_model.clone(),
                native_responses,
                responses_path,
                connect_timeout: Duration::from_secs(
                    conf.connect_timeout_secs
                        .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS),
                ),
                total_connect_timeout: conf.total_connect_timeout_secs.map(Duration::from_secs),
                utc_offset_minutes: conf.utc_offset_minutes(),
            }),
        );
    }

    // Build plugin instances from config
    let mut plugin_instances: AHashMap<String, Arc<dyn Plugin>> = AHashMap::new();
    for (name, plugin_conf) in &config.plugins {
        let mut full_config = plugin_conf.params.clone();
        full_config.insert(
            "category".into(),
            toml::Value::String(plugin_conf.category.clone()),
        );
        let plugin = maxllm_plugin::create_plugin(name, &full_config)
            .map_err(|e| format!("failed to create plugin '{name}': {e}"))?;
        plugin_instances.insert(name.clone(), plugin);
    }

    // The top-level [auth] and [rate_limit] sections predate the plugin
    // system. Wire them to the plugins that implement them so a config that
    // declares them is honoured instead of silently ignored. A plugin the
    // user configured explicitly always wins.
    let mut global_plugin_names: Vec<String> = config.global_plugins.clone();
    let configured_categories: std::collections::HashSet<&str> = config
        .plugins
        .values()
        .map(|p| p.category.as_str())
        .collect();
    if let Some(auth) = &config.auth {
        if !auth.api_keys.is_empty() && !configured_categories.contains("key_auth") {
            let mut t = toml::Table::new();
            t.insert("category".into(), toml::Value::String("key_auth".into()));
            t.insert("header".into(), toml::Value::String("Authorization".into()));
            t.insert("strip_prefix".into(), toml::Value::String("Bearer ".into()));
            t.insert(
                "keys".into(),
                toml::Value::Array(
                    auth.api_keys
                        .iter()
                        .map(|k| toml::Value::String(k.clone()))
                        .collect(),
                ),
            );
            let plugin = maxllm_plugin::create_plugin("[auth] api_keys", &t)
                .map_err(|e| format!("failed to build the [auth] plugin: {e}"))?;
            plugin_instances.insert("[auth] api_keys".into(), plugin);
            // Auth runs before every other plugin.
            global_plugin_names.insert(0, "[auth] api_keys".into());
        }
    }
    if let Some(rl) = &config.rate_limit {
        if !configured_categories.contains("rate_limit") {
            let mut t = toml::Table::new();
            t.insert("category".into(), toml::Value::String("rate_limit".into()));
            t.insert(
                "requests_per_minute".into(),
                toml::Value::Integer(rl.requests_per_minute as i64),
            );
            let plugin = maxllm_plugin::create_plugin("[rate_limit]", &t)
                .map_err(|e| format!("failed to build the [rate_limit] plugin: {e}"))?;
            plugin_instances.insert("[rate_limit]".into(), plugin);
            global_plugin_names.push("[rate_limit]".into());
        }
    }

    // Build global plugin chain
    let global_plugins: Vec<Arc<dyn Plugin>> = global_plugin_names
        .iter()
        .filter_map(|name| plugin_instances.get(name).cloned())
        .collect();
    let global_chain = PluginChain::new(global_plugins);

    // Build per-route plugin chains
    let route_chains: Vec<PluginChain> = config
        .routes
        .iter()
        .map(|route| {
            let route_plugins: Vec<Arc<dyn Plugin>> = route
                .plugins
                .iter()
                .filter_map(|name| plugin_instances.get(name).cloned())
                .collect();
            PluginChain::new(route_plugins)
        })
        .collect();

    // Initialize cost calculator with config overrides
    let mut cost_calculator = maxllm_admin::CostCalculator::new();
    cost_calculator.set_warn_on_missing(config.costs.warn_on_missing);
    cost_calculator.set_enabled(config.costs.enabled);
    for (model, cost_conf) in &config.model_costs {
        cost_calculator.add_cost(maxllm_admin::ModelCost {
            model_pattern: model.clone(),
            input_cost_per_1m: cost_conf.input_per_1m,
            output_cost_per_1m: cost_conf.output_per_1m,
        });
        // Optional cache-hit rate: without it cached input bills at list price.
        if let Some(rate) = cost_conf.cached_input_per_1m {
            cost_calculator.set_cache_rate(model, rate);
        }
    }
    let cost_calculator = Arc::new(cost_calculator);

    // Build guardrail engine from config
    let guardrail_engine = if config.guardrails.is_empty() {
        None
    } else {
        let engine = guardrail::build_engine(&config.guardrails)
            .map_err(|e| format!("failed to build guardrail engine: {e}"))?;
        Some(Arc::new(engine))
    };

    // The auto pools are self-contained: each lists its (provider, model)
    // pairs explicitly, so the rotation set never depends on one-api's channel
    // table (or on one-api running at all). Entries naming an unknown provider
    // are dropped — a typo must not silently shrink a pool.
    let mut auto_pools: Vec<AutoPoolState> = Vec::new();
    for (name, auto) in config.auto_pools() {
        if !auto.enabled {
            continue;
        }
        let mut entries: Vec<AutoPoolEntry> = Vec::new();
        for entry in &auto.pool {
            if providers.contains_key(&entry.provider) {
                entries.push(AutoPoolEntry {
                    provider: entry.provider.clone(),
                    model: entry.model.clone(),
                    billing: entry.billing,
                });
            } else {
                warn!(
                    pool = %name,
                    provider = %entry.provider,
                    "auto pool entry names an unknown provider — dropped"
                );
            }
        }
        if entries.is_empty() {
            warn!(pool = %name, "auto pool has no usable entries — pool disabled");
            continue;
        }
        let metered = entries
            .iter()
            .filter(|e| e.billing == BillingMode::Metered)
            .count();
        info!(
            pool = %name,
            entries = entries.len(),
            subscription = entries.len() - metered,
            metered,
            "Auto pool ready (subscription tier first, metered only as last resort)"
        );
        auto_pools.push(AutoPoolState {
            name,
            entries,
            prefer_fast: auto.prefer_fast,
        });
    }

    // Build the /v1/models response
    let models_response = {
        let mut seen = std::collections::HashSet::new();
        let mut models = Vec::new();
        let created = 0_u64;

        for (_, provider) in &providers {
            let owned_by = provider_owned_by(provider.kind);

            if let Some(ref dm) = provider.default_model {
                if seen.insert(dm.clone()) {
                    models.push(serde_json::json!({
                        "id": dm,
                        "object": "model",
                        "created": created,
                        "owned_by": owned_by,
                    }));
                }
            }

            for &model_id in well_known_models(provider.kind) {
                if seen.insert(model_id.to_string()) {
                    models.push(serde_json::json!({
                        "id": model_id,
                        "object": "model",
                        "created": created,
                        "owned_by": owned_by,
                    }));
                }
            }
        }

        for alias in config.model_aliases.keys() {
            if seen.insert(alias.clone()) {
                models.push(serde_json::json!({
                    "id": alias,
                    "object": "model",
                    "created": created,
                    "owned_by": "maxllm-alias",
                }));
            }
        }

        // Every auto pool is a model a client can ask for by name.
        for pool in &auto_pools {
            if seen.insert(pool.name.clone()) {
                models.push(serde_json::json!({
                    "id": pool.name,
                    "object": "model",
                    "created": created,
                    "owned_by": "maxllm-auto",
                }));
            }
        }

        let response = serde_json::json!({
            "object": "list",
            "data": models,
        });
        Bytes::from(serde_json::to_vec(&response).expect("serialize models"))
    };

    info!(
        global_plugins = config.global_plugins.len(),
        total_plugins = plugin_instances.len(),
        providers = providers.len(),
        routes = config.routes.len(),
        model_aliases = config.model_aliases.len(),
        guardrails = config.guardrails.len(),
        "Hot state built"
    );

    Ok(HotState {
        providers,
        session_sticky_enabled: config
            .session_sticky
            .as_ref()
            .map(|c| c.enabled)
            .unwrap_or(false),
        routes: config.routes.clone(),
        metrics_enabled: config.metrics.enabled,
        metrics_allow_remote: config.metrics.allow_remote,
        global_chain,
        route_chains,
        model_aliases: config.model_aliases.clone(),
        cost_calculator,
        guardrail_engine,
        models_response,
        auto_pools,
    })
}

/// Round-robin counter for the auto pool (shared across auto routes).
static AUTO_POOL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Auto-pool fast-cohort tuning. An entry whose EWMA latency exceeds the
/// tier's median by more than [`AUTO_POOL_COHORT_MEDIAN_MULTIPLIER`]× — clamped
/// into the absolute `[floor, cap]` band — is skipped by the rotation; it stays
/// available as a failover/last-resort target (it is never circuit-opened by
/// this filter).
///
/// The multiplier is deliberately tight (1.5×): the rotation is where a request
/// with *no* upstream cache to lose lands, so it should go to the fast half of
/// the tier rather than to whatever is merely alive. Warm sessions are a
/// separate question — see [`AUTO_POOL_PIN_ESCAPE_MULTIPLIER`], which is much
/// looser so tightening this one does not bounce them.
const AUTO_POOL_COHORT_MEDIAN_MULTIPLIER: f64 = 1.5;
/// Absolute floor for the cohort ceiling (ms): even a 200 ms provider is not
/// dropped because a 100 ms one exists.
const AUTO_POOL_COHORT_FLOOR_MS: f64 = 5_000.0;
/// Absolute cap for the cohort ceiling (ms). The multiplier alone would let a
/// uniformly-slow tier keep a 25 s member in rotation (median 10 s × 1.5); the
/// cap bounds the worst tolerated wait in absolute terms. It never bites below
/// the median itself, so at least the fast half of a measured tier always stays.
const AUTO_POOL_COHORT_CEILING_MS: f64 = 10_000.0;
/// A sticky pin is abandoned for speed only when it is *both* this many times
/// slower than the tier median *and* slower than
/// [`AUTO_POOL_PIN_ESCAPE_FLOOR_MS`] in absolute terms. Two conditions on
/// purpose: the rotation may be strict because an unpinned request has no
/// cache to lose, while a warm session moves only when waiting on it is both
/// relatively and absolutely painful — moving it throws away the upstream
/// prompt cache the conversation was built on.
const AUTO_POOL_PIN_ESCAPE_MULTIPLIER: f64 = 1.5;
/// Absolute floor for the pin escape (ms): a pin never moves for being merely
/// "not the fastest" — only for being slow enough that waiting on it hurts.
const AUTO_POOL_PIN_ESCAPE_FLOOR_MS: f64 = 10_000.0;
/// Every Nth rotation walks the *whole* preferred tier instead of the fast
/// cohort, so a provider that recovered (or a brand-new one) is re-measured
/// and can rejoin the fast cohort.
const AUTO_POOL_EXPLORE_EVERY: u64 = 50;

/// The median whole-exchange EWMA (time-to-last-byte, not TTFB) of `healthy`,
/// or `None` when nothing in the tier has been measured yet.
fn auto_pool_cohort_median(healthy: &[&AutoPoolEntry]) -> Option<f64> {
    let mut measured: Vec<f64> = Vec::with_capacity(healthy.len());
    for e in healthy.iter() {
        if let Some(ms) = crate::provider_stats::total_latency_ms(&e.provider) {
            measured.push(ms);
        }
    }
    if measured.is_empty() {
        return None;
    }
    measured.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(measured[measured.len() / 2])
}

/// The rotation ceiling for a tier whose members have a median whole-exchange
/// EWMA of `median` (ms): the multiplier, clamped into the absolute
/// `[floor, cap]` band and never allowed below the median itself.
fn auto_pool_cohort_ceiling(median: f64) -> f64 {
    (median * AUTO_POOL_COHORT_MEDIAN_MULTIPLIER)
        .max(AUTO_POOL_COHORT_FLOOR_MS)
        .min(AUTO_POOL_COHORT_CEILING_MS.max(median))
}

/// Whether a *sticky pin* may be abandoned purely for speed. Deliberately
/// looser than [`auto_pool_cohort_ceiling`] — see
/// [`AUTO_POOL_PIN_ESCAPE_MULTIPLIER`].
fn auto_pool_pin_escapable(pinned_ms: f64, median: f64) -> bool {
    pinned_ms > median * AUTO_POOL_PIN_ESCAPE_MULTIPLIER
        && pinned_ms > AUTO_POOL_PIN_ESCAPE_FLOOR_MS
}

/// The low-latency cohort of `healthy` for one rotation: every entry whose
/// whole-exchange EWMA is within [`auto_pool_cohort_ceiling`] of the tier's
/// median, plus every entry that has no measurement yet (it has not been
/// proven slow, and the rotation is what produces its first measurement). When
/// nothing has been measured — a freshly-started gateway — the whole tier is
/// returned, so the first requests behave exactly like plain round-robin.
fn auto_pool_fast_cohort<'a>(healthy: &[&'a AutoPoolEntry]) -> Vec<&'a AutoPoolEntry> {
    let Some(median) = auto_pool_cohort_median(healthy) else {
        // Nothing has been measured — a freshly-started gateway: the whole
        // tier, so the first requests behave exactly like plain round-robin.
        return healthy.to_vec();
    };
    let ceiling = auto_pool_cohort_ceiling(median);
    let cohort: Vec<&'a AutoPoolEntry> = healthy
        .iter()
        .copied()
        .filter(
            |e| match crate::provider_stats::total_latency_ms(&e.provider) {
                Some(ms) => ms <= ceiling,
                None => true,
            },
        )
        .collect();
    if cohort.is_empty() {
        healthy.to_vec()
    } else {
        cohort
    }
}

/// The entries one rotation may choose from, in tier order.
///
/// [`auto_pool_fast_cohort`] applied, except when the pool opted out
/// (`prefer_fast = false`) or this is an exploration rotation, which walks
/// the whole tier so a recovered provider can re-measure and rejoin. Every
/// entry the filter removes is counted, so the effect of the decision is
/// visible in `/metrics` and the request log is explainable.
fn auto_pool_rotation<'a>(
    pool: &AutoPoolState,
    healthy: &[&'a AutoPoolEntry],
    idx: u64,
) -> Vec<&'a AutoPoolEntry> {
    if !pool.prefer_fast || idx.is_multiple_of(AUTO_POOL_EXPLORE_EVERY) {
        return healthy.to_vec();
    }
    let cohort = auto_pool_fast_cohort(healthy);
    for e in healthy {
        if !cohort.iter().any(|c| c.provider == e.provider) {
            METRICS
                .auto_pool_slow_skips_total
                .with_label_values(&[&e.provider])
                .inc();
        }
    }
    cohort
}

impl AiGateway {
    pub fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let hot_state = build_hot_state(config)?;

        // Initialize admin API if configured
        let mut budget_enforcer: Option<Arc<maxllm_admin::BudgetEnforcer>> = None;
        let admin_api = if let Some(ref admin_config) = config.admin {
            if admin_config.enabled {
                let store: Arc<dyn maxllm_admin::AdminStore> =
                    if let Some(ref db_path) = admin_config.db_path {
                        let path = std::path::Path::new(db_path);
                        Arc::new(
                            maxllm_admin::SqliteStore::open(path)
                                .map_err(|e| format!("failed to open admin SQLite DB: {e}"))?,
                        )
                    } else {
                        Arc::new(maxllm_admin::InMemoryStore::new())
                    };
                let api = maxllm_admin::AdminApi::new(
                    Arc::clone(&store),
                    Arc::clone(&hot_state.cost_calculator),
                    &admin_config.master_key,
                );
                // Budget enforcement reads the same store: virtual keys
                // created through the admin API are authenticated here and
                // their spend is recorded by the logging hook.
                budget_enforcer = Some(Arc::new(maxllm_admin::BudgetEnforcer::new(
                    store,
                    Arc::clone(&hot_state.cost_calculator),
                )));
                info!(
                    db = admin_config.db_path.as_deref().unwrap_or("in-memory"),
                    "Admin API initialized"
                );
                Some(Arc::new(api))
            } else {
                None
            }
        } else {
            None
        };

        // Session-sticky store. The global switch is the route-level opt-in
        // (`session_sticky = true`); this section only tunes the store.
        let session_cfg = config.session_sticky.clone().unwrap_or_default();
        let sessions = Arc::new(SessionStore::new(
            session_cfg.max_entries,
            session_cfg.timeout_secs,
        ));

        // Restore the persisted resilience state: a quota force-open (429) and
        // the session → provider affinity must survive a restart, otherwise a
        // bounced gateway immediately hammers the channel that just failed
        // and every conversation gets re-routed.
        if let Some(ref admin_api) = admin_api {
            match admin_api.load_circuit_breaker_states() {
                Ok(rows) => {
                    let mut restored = 0usize;
                    let mut pruned = 0usize;
                    for row in rows {
                        // A renamed or deleted provider leaves its breaker row
                        // behind forever; a stale `force_open_until` on a dead
                        // name reads like live state to anyone inspecting the
                        // table. Prune it once at startup instead.
                        if !hot_state.providers.contains_key(&row.provider) {
                            // Deleting is the right call for a provider that was
                            // removed, but it also destroys real state when the
                            // operator renamed the provider in the config and
                            // forgot to migrate the row first (`maxllm admin
                            // rename-provider`). Say so loudly instead of
                            // counting the row away in silence.
                            if row.state != 0
                                || row.failures > 0
                                || row.quota_opens > 0
                                || row.force_open_until > crate::rate_limit::now_ms()
                            {
                                warn!(
                                    provider = %row.provider,
                                    state = row.state,
                                    failures = row.failures,
                                    quota_opens = row.quota_opens,
                                    force_open_until = row.force_open_until,
                                    "dropping circuit breaker state of a provider that is no longer in the config — \
                                     if this was a rename, migrate the row first with `maxllm admin rename-provider`"
                                );
                            }
                            match admin_api.delete_circuit_breaker_state(&row.provider) {
                                Ok(()) => pruned += 1,
                                Err(e) => warn!(
                                    provider = %row.provider,
                                    error = %e,
                                    "Failed to prune circuit breaker state for unknown provider"
                                ),
                            }
                            continue;
                        }
                        if let Some(breaker) = existing_breaker(&row.provider) {
                            breaker.restore(&row);
                            // Re-publish the gauges so a restarted process shows
                            // the restored state before the next request.
                            METRICS
                                .circuit_breaker_state
                                .with_label_values(&[&row.provider])
                                .set(breaker.state() as i64);
                            METRICS
                                .circuit_breaker_transitions_total
                                .with_label_values(&[&row.provider])
                                .set(breaker.state_transitions() as i64);
                            // Counter — a fresh process starts at 0, so inc_by
                            // brings it up to the restored value exactly.
                            METRICS
                                .circuit_breaker_quota_opens_total
                                .with_label_values(&[&row.provider])
                                .inc_by(breaker.quota_opens());
                            restored += 1;
                        }
                    }
                    if restored > 0 {
                        info!(count = restored, "Restored circuit breaker states");
                    }
                    if pruned > 0 {
                        info!(
                            count = pruned,
                            "Pruned circuit breaker rows for providers no longer in the config"
                        );
                    }
                }
                Err(e) => warn!(error = %e, "Failed to restore circuit breaker states"),
            }
            match admin_api.load_session_bindings() {
                Ok(bindings) => {
                    let mut restored = 0usize;
                    let mut pruned = 0usize;
                    for (session_id, provider) in bindings {
                        // A pin on a provider that no longer exists can never
                        // be honoured, and keeping it in memory only inflates
                        // the affinity gauges; drop the row as well so the
                        // table stops growing by one rename's worth of names.
                        if !hot_state.providers.contains_key(&provider) {
                            match admin_api.delete_session_binding(&session_id) {
                                Ok(()) => pruned += 1,
                                Err(e) => warn!(
                                    session_id = %session_id,
                                    provider = %provider,
                                    error = %e,
                                    "Failed to prune session binding for unknown provider"
                                ),
                            }
                            continue;
                        }
                        sessions.set(&session_id, &provider);
                        restored += 1;
                    }
                    METRICS.session_affinity_size.set(sessions.len() as i64);
                    if restored > 0 {
                        info!(count = restored, "Restored session bindings");
                    }
                    if pruned > 0 {
                        info!(
                            count = pruned,
                            "Pruned session bindings pinned to providers no longer in the config"
                        );
                    }
                }
                Err(e) => warn!(error = %e, "Failed to restore session bindings"),
            }
        }

        // Optional JSONL recorder.
        let recorder = match config.recording.as_ref() {
            Some(rec_cfg) if rec_cfg.enabled => match Recorder::new(rec_cfg) {
                Ok(r) => {
                    info!(dir = %rec_cfg.dir, "Request recorder enabled");
                    Some(Arc::new(r))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to initialise recorder, continuing without it");
                    None
                }
            },
            _ => None,
        };

        // Rate-limit learning resolver. Disabled by default: without it a 429
        // falls back to the plain `Retry-After` cooldown.
        let rl_cfg = config.rate_limit_learning.clone().unwrap_or_default();
        let resolver_cfg = crate::rate_limit_resolver::ResolverConfig {
            enabled: rl_cfg.enabled,
            rate_limit_threshold: rl_cfg.rate_limit_threshold,
            default_cooldown_secs: rl_cfg.default_cooldown_secs,
            laya_timeout: std::time::Duration::from_millis(rl_cfg.laya_timeout_ms),
        };
        let rate_limit_resolver = Arc::new(crate::rate_limit_resolver::RateLimitResolver::new(
            resolver_cfg,
            rl_cfg.laya_url.as_deref(),
            rl_cfg.laya_model.as_deref(),
            admin_api.clone(),
        ));
        if rl_cfg.enabled {
            info!(
                laya = rate_limit_resolver.laya_base_url().unwrap_or("(none)"),
                threshold = rl_cfg.rate_limit_threshold,
                default_cooldown_secs = rl_cfg.default_cooldown_secs,
                "Rate-limit learning enabled"
            );
        }

        Ok(Self {
            hot: Arc::new(arc_swap::ArcSwap::from_pointee(hot_state)),
            budget_enforcer,
            key_rate_limiter: Arc::new(crate::key_rate_limit::KeyRateLimiter::new()),
            admin_api,
            sessions,
            recorder,
            rate_limit_resolver,
        })
    }

    /// Reload the hot-reloadable state from a new config.
    #[allow(dead_code)]
    /// Returns a summary of what changed.
    pub fn reload(&self, config: &Config) -> Result<String, Box<dyn std::error::Error>> {
        let old = self.hot.load();
        let new_state = build_hot_state(config)?;

        // Build a summary of changes
        let mut changes = Vec::new();
        if old.providers.len() != new_state.providers.len() {
            changes.push(format!(
                "providers: {} -> {}",
                old.providers.len(),
                new_state.providers.len()
            ));
        }
        if old.routes.len() != new_state.routes.len() {
            changes.push(format!(
                "routes: {} -> {}",
                old.routes.len(),
                new_state.routes.len()
            ));
        }
        if old.model_aliases.len() != new_state.model_aliases.len() {
            changes.push(format!(
                "model_aliases: {} -> {}",
                old.model_aliases.len(),
                new_state.model_aliases.len()
            ));
        }
        if old.metrics_enabled != new_state.metrics_enabled {
            changes.push(format!(
                "metrics: {} -> {}",
                old.metrics_enabled, new_state.metrics_enabled
            ));
        }

        let summary = if changes.is_empty() {
            "config reloaded (no structural changes detected)".to_string()
        } else {
            format!("config reloaded: {}", changes.join(", "))
        };

        self.hot.store(Arc::new(new_state));
        Ok(summary)
    }

    /// Find matching route by path prefix.
    fn match_route(hot: &HotState, path: &str) -> Option<usize> {
        hot.routes.iter().position(|r| path.starts_with(&r.path))
    }

    /// The first path segment after `/v1/responses/` when it names neither a
    /// configured provider nor an auto pool. `None` means the route is valid.
    ///
    /// The segment is empty for the bare route, `responses` for a client that
    /// appends the endpoint to a `/v1/responses` base URL, and a provider or
    /// pool name for the per-provider routes. Anything else is a typo or a
    /// retired name and must not be served by the generic pool under a 200.
    /// The provider named by `/v1/responses/<provider>` when no explicit
    /// `[[routes]]` entry matched that path and the segment is a real provider.
    ///
    /// Such a path falls into the pooled `/v1/responses` route by prefix match
    /// (`match_route` takes the first prefix match in config order), because
    /// only providers that happen to have a duplicate `[[routes]]` entry pin
    /// correctly. Honouring the name here is what makes the documented
    /// `/v1/responses/<provider>` contract true for every provider, instead of
    /// silently answering with whichever provider the pool selected.
    ///
    /// Pools are not pinned here: `auto` / `auto_home` have their own routes,
    /// which match before the pooled one.
    fn responses_subroute_pin(hot: &HotState, route: &RouteConfig, path: &str) -> Option<String> {
        if route.path != "/v1/responses" {
            return None;
        }
        let rest = path.strip_prefix("/v1/responses/")?;
        let segment = rest.split('/').next().unwrap_or_default();
        if segment.is_empty() || segment == "responses" {
            return None;
        }
        hot.providers
            .contains_key(segment)
            .then(|| segment.to_string())
    }

    fn unknown_responses_subroute<'a>(
        hot: &HotState,
        route: &RouteConfig,
        path: &'a str,
    ) -> Option<&'a str> {
        if route.path != "/v1/responses" {
            return None;
        }
        let rest = path.strip_prefix("/v1/responses/")?;
        let segment = rest.split('/').next().unwrap_or_default();
        if segment.is_empty() || segment == "responses" {
            return None;
        }
        if hot.providers.contains_key(segment) || hot.pool_named(segment).is_some() {
            return None;
        }
        Some(segment)
    }

    /// The provider's blended USD price per 1M tokens — the ranking key of
    /// the cost-based strategy.
    ///
    /// A request-shaped workload is approximated by the same input/output
    /// token count (the ranking only has to order the providers, and most
    /// upstream pricing is dominated by the input side anyway). A provider
    /// whose model has no price-table entry — an OpenAI-compatible channel
    /// serving a custom name, say — is unpriced and ranked last, but stays
    /// eligible: the strategy must never leave a route unserved over a
    /// missing price line.
    fn provider_cost(hot: &HotState, provider: &Arc<ProviderState>) -> Option<f64> {
        let model = provider.default_model.as_deref()?;
        let mc = hot.cost_calculator.get_model_cost(model)?;
        Some(mc.input_cost_per_1m + mc.output_cost_per_1m)
    }

    /// Pick a (provider, model) pair from `pool` for this request: the session
    /// binding first (a conversation stays on the provider that first served
    /// it, so the upstream keeps its context), then round-robin over the
    /// healthy entries. The chosen pair is recorded in `ctx.auto_target`, so
    /// the body's `model` field is rewritten before it reaches an upstream.
    fn select_from_auto_pool(
        &self,
        hot: &HotState,
        pool: &AutoPoolState,
        route: &RouteConfig,
        ctx: &mut RequestCtx,
    ) -> Option<Arc<ProviderState>> {
        // Billing tiers first: `usable_entries` returns the healthy
        // subscription entries whenever at least one exists, and only falls
        // back to the metered entries once every subscription entry is
        // unavailable (circuit open). Everything below — session-sticky and
        // round-robin — walks that single tier, so a pay-per-token provider
        // is never picked while a prepaid plan can still serve.
        let healthy = pool.usable_entries(|e| {
            hot.providers
                .get(&e.provider)
                // Pure read: inspecting a pool entry must not consume its
                // half-open probe slot. The old `is_open()` did exactly that,
                // so the entries the round-robin *looked at* but never picked
                // were left permanently half-open and answered 503.
                .map(|p| p.circuit_breaker.allows_traffic())
                .unwrap_or(false)
        });
        if healthy.is_empty() {
            return None;
        }
        let metered_tier = healthy.iter().all(|e| e.billing == BillingMode::Metered);

        // Admission: the entry the request is actually sent to is the one
        // that drives the breaker and reserves the single half-open probe
        // slot. An entry whose slot is already occupied by another in-flight
        // probe is skipped, so `half_open_max` still bounds concurrent
        // probes per provider.
        let admits = |e: &AutoPoolEntry| -> bool {
            hot.providers
                .get(&e.provider)
                .map(|p| !p.circuit_breaker.is_open())
                .unwrap_or(false)
        };

        // Two different latency rules, on purpose.
        //
        // The round-robin rotation uses the strict fast cohort: a request with
        // no pin has no upstream cache to lose, so it may as well go to the
        // fastest healthy member of the tier.
        //
        // A sticky pin is judged by `auto_pool_pin_escapable` instead — much
        // looser, and independent of the cohort. Coupling the two made every
        // tightening of the rotation bounce warm sessions to another provider,
        // and a session that moves loses the upstream prompt cache it was
        // built on: one slow turn is cheaper than a cold conversation.
        let cohort_median = if pool.prefer_fast {
            auto_pool_cohort_median(&healthy)
        } else {
            None
        };
        let pin_is_slow = |name: &str| -> bool {
            let Some(median) = cohort_median else {
                return false;
            };
            crate::provider_stats::total_latency_ms(name)
                .is_some_and(|ms| auto_pool_pin_escapable(ms, median))
        };

        // 1. Session-sticky within the preferred tier: a session pinned to a
        //    pool provider keeps using it (a different key loses upstream
        //    context). A pin on the metered tier is not honoured once a
        //    subscription entry has recovered — the prepaid tier wins and
        //    the session re-pins on the next successful response. A pin that
        //    is *slow enough to escape* (see `auto_pool_pin_escapable`) is
        //    abandoned the same way: a provider whose typical answer takes
        //    tens of seconds costs more per turn than the cache a move loses.
        let mut chosen: Option<&AutoPoolEntry> = None;
        if ctx.path_pinned_provider.is_none() && route.session_sticky && hot.session_sticky_enabled
        {
            if let Some(session_id) = ctx.session_id.clone() {
                match self.sessions.get(&session_id) {
                    Some(pinned_name) => {
                        chosen = healthy.iter().copied().find(|e| {
                            e.provider == pinned_name && admits(e) && !pin_is_slow(&e.provider)
                        });
                        // Pinned provider unavailable (open circuit, no
                        // longer in the pool, only in a lower billing tier,
                        // its probe slot is busy, or slow enough to escape
                        // — see `auto_pool_pin_escapable`) — fall through to
                        // round-robin; the session re-pins on the next
                        // successful response. A pin that cannot be honoured
                        // is a miss, not a hit.
                        if chosen.is_some() {
                            METRICS.session_affinity_hits_total.inc();
                        } else {
                            METRICS.session_affinity_misses_total.inc();
                        }
                    }
                    None => {
                        METRICS.session_affinity_misses_total.inc();
                    }
                }
            }
        }
        // 2. Round-robin over the healthy entries of the preferred tier.
        let entry = match chosen {
            Some(entry) => entry,
            None => {
                let idx = AUTO_POOL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let rotation = auto_pool_rotation(pool, &healthy, idx);
                (0..rotation.len())
                    .map(|i| rotation[(idx as usize + i) % rotation.len()])
                    .find(|e| admits(e))?
            }
        };
        if metered_tier {
            METRICS.auto_pool_metered_selections_total.inc();
        }
        let provider = Arc::clone(hot.providers.get(&entry.provider)?);
        ctx.provider_name = entry.provider.clone();
        ctx.auto_target = Some((entry.provider.clone(), entry.model.clone()));
        ctx.model = entry.model.clone();
        ctx.plugin_ctx.model = entry.model.clone();
        // A native-Responses entry gets the request forwarded unchanged.
        ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
            && provider.native_responses
            && provider.responses_path.is_some();
        Some(provider)
    }

    /// Select provider using the route's configured strategy.
    fn select_provider(
        &self,
        hot: &HotState,
        route: &RouteConfig,
        ctx: &mut RequestCtx,
    ) -> Option<Arc<ProviderState>> {
        // An auto sentinel (`auto`, `auto_home`, …) is served from its pool on
        // *any* route, not only on routes that declare `auto_model = true`.
        //
        // When the sentinel is already known here — the path carries it
        // (`/v1/responses/auto`), the route declares `auto_model`, or an
        // earlier hop already entered a pool — the pool is entered directly,
        // with no wasted call to the route's own provider. When only the body
        // carries it the model is not known yet (the provider has to be picked
        // before the body is read), so the request takes one 404 from the
        // route's provider and the failover walks into the pool — see
        // `switch_to_fallback` for the replay that makes that safe for a
        // ~100 KB agent request.
        // A path-pinned provider (`/v1/responses/<provider>` with no route of
        // its own) is served by that provider alone: the caller named one
        // provider, so neither a pool nor the pooled chain may stand in for
        // it.
        if ctx.path_pinned_provider.is_none() {
            if let Some(pool) = hot.request_pool(route, ctx) {
                let pool_name = pool.name.clone();
                ctx.auto_pool = Some(pool_name);
                if let Some(provider) = self.select_from_auto_pool(hot, pool, route, ctx) {
                    return Some(provider);
                }
                // Every pool entry is unhealthy — fall through to the route's
                // own selection so the request still has somewhere to go.
                ctx.auto_pool = None;
            }
        }

        // Build list of all candidate providers for this route
        let mut candidates: Vec<ProviderTarget> = Vec::new();

        if let Some(pin) = ctx.path_pinned_provider.clone() {
            if let Some(provider) = hot.providers.get(&pin) {
                candidates.push(ProviderTarget {
                    name: pin,
                    cost_per_1m: Self::provider_cost(hot, provider),
                    state: Arc::clone(provider),
                    is_primary: true,
                });
            }
        } else {
            if let Some(provider) = hot.providers.get(&route.provider) {
                candidates.push(ProviderTarget {
                    name: route.provider.clone(),
                    cost_per_1m: Self::provider_cost(hot, provider),
                    state: Arc::clone(provider),
                    is_primary: true,
                });
            }

            for fb_name in &route.fallback {
                if let Some(provider) = hot.providers.get(fb_name) {
                    candidates.push(ProviderTarget {
                        name: fb_name.clone(),
                        cost_per_1m: Self::provider_cost(hot, provider),
                        state: Arc::clone(provider),
                        is_primary: false,
                    });
                }
            }
        }

        // Session-sticky: a session that already succeeded on a provider is
        // pinned to it, so a conversation keeps hitting the same API key
        // (different keys do not share upstream context). A pinned provider
        // whose circuit is open is skipped — the session is re-pinned after
        // the next successful attempt.
        if route.session_sticky && hot.session_sticky_enabled {
            if let Some(session_id) = ctx.session_id.clone() {
                match self.sessions.get(&session_id) {
                    Some(pinned_name) => {
                        if let Some(pinned) = candidates.iter().find(|c| c.name == pinned_name) {
                            if !pinned.state.circuit_breaker.is_open() {
                                METRICS.session_affinity_hits_total.inc();
                                ctx.provider_name = pinned_name.clone();
                                ctx.fallback_used = pinned_name != route.provider;
                                // The client asked for the sentinel while its
                                // base URL points at a route whose chain
                                // happens to contain auto-pool members. The
                                // pinned provider is one of them, so rewrite
                                // the body's model to that entry's model and
                                // keep the request in the pool for any later
                                // hop. Without this the literal sentinel goes
                                // upstream: strict upstreams answer 404, and
                                // llama.cpp-style servers accept any model
                                // name and quietly serve it with whatever
                                // model they happen to be hosting.
                                if let Some(pool) = hot.request_pool(route, ctx) {
                                    if let Some(entry) = pool.entry_for(&pinned_name) {
                                        ctx.auto_pool = Some(pool.name.clone());
                                        ctx.auto_target =
                                            Some((entry.provider.clone(), entry.model.clone()));
                                        ctx.model = entry.model.clone();
                                        ctx.plugin_ctx.model = entry.model.clone();
                                    }
                                }
                                if ctx.fallback_used {
                                    ctx.original_provider = Some(route.provider.clone());
                                }
                                ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
                                    && pinned.state.native_responses
                                    && pinned.state.responses_path.is_some();
                                return Some(Arc::clone(&pinned.state));
                            }
                        }
                        // Pinned provider is unavailable (open circuit, or no
                        // longer in this route's chain) — fall through to the
                        // normal selector; the session gets re-pinned on the
                        // next successful response.
                        METRICS.session_affinity_misses_total.inc();
                    }
                    None => {
                        METRICS.session_affinity_misses_total.inc();
                    }
                }
            }
        }

        let selector = ProviderSelector::new(route.strategy);
        match selector.select(&candidates, ctx) {
            Some(target) => {
                if !target.is_primary {
                    ctx.fallback_used = true;
                    ctx.original_provider = Some(route.provider.clone());
                    METRICS
                        .fallbacks_total
                        .with_label_values(&[&route.provider, &target.name])
                        .inc();
                }
                ctx.provider_name = target.name.clone();
                // A provider that speaks the Responses contract natively gets
                // the request forwarded unchanged (no chat translation round
                // trip), so the whole Responses feature set survives.
                ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
                    && target.state.native_responses
                    && target.state.responses_path.is_some();
                Some(Arc::clone(&target.state))
            }
            None => None,
        }
    }

    /// Whether no untried candidate of the route declares itself as carrying
    /// the requested model.
    ///
    /// A 400 "unknown model" has two possible cures, and the cheaper one is
    /// handing the request to a provider that does carry the name. Only when
    /// no such provider is left does it make sense to substitute the model
    /// with the current provider's `default_model` — the name may simply
    /// belong to the provider that just died (the pooled `/v1/responses`
    /// primary's `deepseek-v4.1-flash:cloud`), which no fallback knows.
    ///
    /// A candidate with no `default_model` gives no signal and is treated as
    /// possibly carrying the model, so a request is never rewritten while an
    /// unexamined provider is still in the chain.
    fn no_later_candidate_carries_model(&self, hot: &HotState, ctx: &RequestCtx) -> bool {
        let Some(route) = ctx.route_index.and_then(|i| hot.routes.get(i)) else {
            return false;
        };
        if ctx.path_pinned_provider.is_some() {
            // The pinned provider is the only candidate: no other provider can
            // serve the model, so the name may be rewritten to the pinned
            // provider's default (exactly as on a single-provider route).
            return true;
        }
        !std::iter::once(&route.provider)
            .chain(route.fallback.iter())
            .filter(|name| name.as_str() != ctx.provider_name)
            .filter(|name| !ctx.attempted_providers.iter().any(|a| a == *name))
            .any(|name| {
                hot.providers.get(name).is_some_and(|p| {
                    p.circuit_breaker.allows_traffic()
                        && p.default_model.as_deref().is_none_or(|dm| dm == ctx.model)
                })
            })
    }

    /// Whether `ctx.model` is a name this deployment actually serves — the
    /// `default_model` of some provider or the model of some pool entry.
    ///
    /// A 400 for a name no provider declares is a typo, or a model this
    /// gateway does not offer at all; substituting the current provider's
    /// default would silently answer with a different model. Only a name that
    /// belongs to *another* provider of this deployment (the pooled
    /// `/v1/responses` primary's `deepseek-v4.1-flash:cloud`) is worth
    /// rewriting when that provider is the one that failed.
    fn model_belongs_to_some_provider(hot: &HotState, ctx: &RequestCtx) -> bool {
        if ctx.model.is_empty() {
            return false;
        }
        hot.providers
            .values()
            .any(|p| p.default_model.as_deref() == Some(ctx.model.as_str()))
            || hot
                .auto_pools
                .iter()
                .flat_map(|pool| pool.entries.iter())
                .any(|e| e.model == ctx.model)
    }

    /// The model the current provider should be asked for instead of the name
    /// the client sent, when the provider plainly does not carry it.
    ///
    /// `None` when the request is not rewritable: it rotates over an auto pool
    /// (the pool entry's model is authoritative for that entry), no replayable
    /// client body was captured, the name belongs to no provider of this
    /// deployment (a typo must stay an error), a later candidate still carries
    /// the name (that candidate should serve it, not a rewrite here), or this
    /// provider already got its one rewrite (a second rejection is a genuine
    /// request fault, not a missing model).
    fn model_rewrite_target(
        &self,
        hot: &HotState,
        ctx: &RequestCtx,
        provider: &ProviderState,
    ) -> Option<String> {
        if ctx.auto_target.is_some() || ctx.client_body.is_none() {
            return None;
        }
        if ctx
            .model_rewritten_providers
            .iter()
            .any(|p| p == &ctx.provider_name)
        {
            return None;
        }
        if !self.no_later_candidate_carries_model(hot, ctx) {
            return None;
        }
        if !Self::model_belongs_to_some_provider(hot, ctx) {
            return None;
        }
        provider
            .default_model
            .as_deref()
            .filter(|dm| !dm.is_empty() && *dm != ctx.model)
            .map(str::to_string)
    }

    /// Reset the request for an in-place retry of the same provider with its
    /// own `default_model` substituted for the name the client sent, and build
    /// the pingora retry error that triggers it.
    ///
    /// Shared by the header-time 400/422 decision and the body-time 404 one
    /// (vLLM answers an unknown model with 404). The body is replayed from the
    /// client's original bytes because the translated copy of the failed
    /// attempt still carries the old model — the same state reset a failover
    /// performs, minus the provider switch.
    fn begin_model_rewrite(
        &self,
        ctx: &mut RequestCtx,
        status: u16,
        rewrite_to: String,
    ) -> Box<pingora::Error> {
        let original_model = ctx.model.clone();
        ctx.model_rewritten_providers
            .push(ctx.provider_name.clone());
        ctx.model = rewrite_to.clone();
        ctx.plugin_ctx.model = rewrite_to.clone();
        ctx.model_override = Some(rewrite_to.clone());
        ctx.body_translated = false;
        ctx.upstream_responded = false;
        ctx.request_body_buf.clear();
        ctx.replay_body = ctx.client_body.clone();
        ctx.replay_injected = false;
        ctx.keep_request_content_length = false;
        if let Ok(mut guard) = ctx.stream_translator.lock() {
            *guard = None;
        }
        METRICS
            .model_rewrite_retries_total
            .with_label_values(&[&ctx.provider_name])
            .inc();
        info!(
            status = %status,
            provider = %ctx.provider_name,
            from = %original_model,
            to = %rewrite_to,
            "upstream does not carry the requested model — rewriting to its own default and retrying it"
        );
        let mut e = pingora::Error::explain(
            pingora::ErrorType::HTTPStatus(status),
            format!(
                "provider '{}' does not carry model '{original_model}'; retrying it with '{}'",
                ctx.provider_name, rewrite_to
            ),
        );
        e.as_up();
        e.set_retry(true);
        e
    }

    /// Explain why routing produced no provider, naming every candidate and
    /// its circuit state.
    ///
    /// The flat `503 "All providers are unavailable (circuit breakers open)"`
    /// is actively misleading on a *pinned* single-provider route: the one
    /// provider is circuit-open (often a quota force-open), and an operator
    /// reading "all providers" starts hunting for a routing bug. Naming the
    /// candidates and the reason turns the same answer into a diagnosis
    /// (2026-11-08: `pool_health` reported 16/25 "All providers are
    /// unavailable" while each entry was merely circuit-open on its own).
    fn unavailable_reason(&self, hot: &HotState, route: &RouteConfig, ctx: &RequestCtx) -> String {
        let mut names: Vec<&str> = Vec::new();
        if let Some(pin) = ctx.path_pinned_provider.as_deref() {
            names.push(pin);
        } else if let Some(pool) = hot.request_pool(route, ctx) {
            names.extend(pool.entries.iter().map(|e| e.provider.as_str()));
        } else {
            names.push(&route.provider);
            names.extend(route.fallback.iter().map(String::as_str));
        }
        if names.is_empty() {
            return format!(
                "no provider available: route '{}' has no candidates",
                route.path
            );
        }
        let mut parts: Vec<String> = Vec::new();
        for name in names {
            match hot.providers.get(name) {
                None => parts.push(format!("{name}=not configured")),
                Some(p) => {
                    let cb = &p.circuit_breaker;
                    if cb.is_force_open() {
                        parts.push(format!("{name}=circuit open (quota force-open)"));
                    } else {
                        match cb.effective_state() {
                            crate::circuit_breaker::STATE_OPEN => {
                                parts.push(format!("{name}=circuit open"))
                            }
                            crate::circuit_breaker::STATE_HALF_OPEN => {
                                parts.push(format!("{name}=half-open probe in flight"))
                            }
                            _ => parts.push(format!("{name}=available")),
                        }
                    }
                }
            }
        }
        let scope = match ctx.path_pinned_provider.as_deref() {
            Some(pin) => format!("the provider '{pin}' named by the request path"),
            None => format!("every candidate of route '{}'", route.path),
        };
        format!(
            "no provider available: {} — {scope} is rejecting traffic",
            parts.join(", ")
        )
    }

    /// Resolve model aliases.
    fn resolve_model_alias(hot: &HotState, model: &str) -> String {
        hot.model_aliases
            .get(model)
            .cloned()
            .unwrap_or_else(|| model.to_string())
    }

    /// The virtual key's model allow-list denies the request. `resolved` is
    /// the model sent upstream (after alias resolution); `requested` is the
    /// raw client-supplied name, so an allow-list entry may name either the
    /// alias or the upstream model. Returns the denial message, or `None`
    /// when the model is allowed (or no allow-list is in force).
    fn model_denied(
        allowed_models: &[String],
        resolved: &str,
        requested: Option<&str>,
    ) -> Option<String> {
        if allowed_models.is_empty() {
            return None;
        }
        let matches = |pattern: &str, model: &str| {
            !model.is_empty()
                && (model == pattern || maxllm_admin::costs::glob_match(pattern, model))
        };
        let allowed = allowed_models.iter().any(|pattern| {
            matches(pattern, resolved) || requested.is_some_and(|m| matches(pattern, m))
        });
        if allowed {
            None
        } else {
            let shown = requested.filter(|m| !m.is_empty()).unwrap_or(resolved);
            Some(format!("model '{shown}' is not allowed for this API key"))
        }
    }

    /// Write a 403 response for a model the virtual key is not allowed to use.
    async fn write_model_denied(session: &mut Session, message: &str) -> pingora::Result<()> {
        let error_body = serde_json::json!({
            "error": {
                "message": message,
                "type": "model_not_allowed",
                "code": 403
            }
        });
        let error_bytes = error_body.to_string().into_bytes();
        let mut resp = ResponseHeader::build(403, Some(2))?;
        resp.insert_header("Content-Type", "application/json")?;
        resp.insert_header("Content-Length", error_bytes.len().to_string())?;
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(error_bytes)), true)
            .await?;
        session.set_keepalive(None);
        Ok(())
    }

    /// Enforce the virtual key's model allow-list at a model-resolution
    /// point. When the key denies the model, a 403 response is written to
    /// the client and `Err` is returned so the proxy pipeline stops — the
    /// same contract as [`Self::send_guardrail_block`].
    async fn enforce_model_allowlist(
        session: &mut Session,
        ctx: &mut RequestCtx,
        resolved: &str,
        requested: Option<&str>,
    ) -> pingora::Result<()> {
        let Some(message) =
            Self::model_denied(&ctx.virtual_key_allowed_models, resolved, requested)
        else {
            return Ok(());
        };
        ctx.upstream_status = 403;
        Self::write_model_denied(session, &message).await?;
        Err(pingora::Error::explain(
            pingora::ErrorType::HTTPStatus(403),
            "model not allowed",
        ))
    }

    async fn send_error_response(
        session: &mut Session,
        status: u16,
        message: &str,
    ) -> pingora::Result<bool> {
        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": "gateway_error",
                "code": status
            }
        });
        let body_bytes = body.to_string().into_bytes();
        let content_len = body_bytes.len().to_string();

        let mut resp = ResponseHeader::build(status, Some(4))?;
        resp.insert_header("Content-Type", "application/json")?;
        resp.insert_header("Content-Length", &content_len)?;

        session.set_keepalive(None);
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(body_bytes)), true)
            .await?;

        Ok(true)
    }

    /// Handle a Bedrock request end-to-end using an out-of-band HTTP client.
    ///
    /// Bedrock authenticates with AWS Signature Version 4, whose signature
    /// covers the SHA-256 of the translated request payload. The exact bytes
    /// must be known before the request headers are written. Pingora's proxy
    /// pipeline streams body chunks after the headers have already been
    /// sent, so the payload hash cannot be embedded in the `Authorization`
    /// header there. Bedrock requests are therefore read, translated, signed
    /// and sent via `reqwest` (which buffers the full body), and the
    /// response is translated back and written to the client before
    /// returning. Metrics (`requests_total`, tokens, latency) are still
    /// recorded by the `logging` hook that runs after `request_filter`
    /// returns `Ok(true)`.
    async fn handle_bedrock_request(
        &self,
        session: &mut Session,
        ctx: &mut RequestCtx,
        provider: &Arc<ProviderState>,
    ) -> pingora::Result<()> {
        // 1. Read the full client body.
        let mut client_body = Vec::new();
        while let Some(chunk) = session.read_request_body().await? {
            client_body.extend_from_slice(&chunk);
        }

        // 2. Extract the model (for metrics and alias resolution) and
        //    enforce the virtual key's model allow-list before anything is
        //    translated or signed — the request_filter context ends here,
        //    so a denial writes the 403 and stops without an Err.
        let hot = self.hot.load();
        if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&client_body) {
            if let Some(m) = parsed.get("model").and_then(|v| v.as_str()) {
                let resolved = Self::resolve_model_alias(&hot, m);
                ctx.model = resolved.clone();
                ctx.plugin_ctx.model = resolved;
                if let Some(message) =
                    Self::model_denied(&ctx.virtual_key_allowed_models, &ctx.model, Some(m))
                {
                    ctx.upstream_status = 403;
                    Self::write_model_denied(session, &message).await?;
                    return Ok(());
                }
            }
        }
        drop(hot);

        // 3. Translate OpenAI → Bedrock/Anthropic format.
        let translated = provider
            .translator
            .translate_request(&client_body, None)
            .map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(400),
                    format!("Bedrock body translation failed: {e}"),
                )
            })?;
        let is_streaming = translated.is_streaming;
        ctx.is_streaming = is_streaming;

        // 4. Build the model-specific Bedrock path.
        let model_id = provider
            .translator
            .as_any()
            .downcast_ref::<maxllm_translate::bedrock::BedrockTranslator>()
            .map(|b| b.last_model_id())
            .or_else(|| provider.default_model.clone())
            .unwrap_or_else(|| "anthropic.claude-sonnet-4-20250514-v1:0".into());
        let path = maxllm_translate::bedrock::BedrockTranslator::path_for(&model_id, is_streaming);

        // 5. Sign the request with SigV4.
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
        let region = provider
            .region
            .clone()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        let amz_date = maxllm_translate::aws_sigv4::amz_date_now();
        let credentials = maxllm_translate::aws_sigv4::AwsCredentials {
            access_key: &access_key,
            secret_key: &secret_key,
            region: &region,
        };
        let signed = maxllm_translate::aws_sigv4::sign_bedrock(
            "POST",
            &path,
            &provider.host,
            &translated.body,
            &credentials,
            &amz_date,
        );

        // 6. Build the URL.
        let scheme = if provider.tls { "https" } else { "http" };
        let default_port = if provider.tls { 443 } else { 80 };
        let url = if provider.port != default_port {
            format!("{scheme}://{}:{}{}", provider.host, provider.port, path)
        } else {
            format!("{scheme}://{}{}", provider.host, path)
        };

        // 7. Send via reqwest.
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .header("Host", &provider.host)
            .header("X-Amz-Date", &signed.amz_date)
            .header("X-Amz-Content-Sha256", &signed.content_sha256)
            .header("Authorization", &signed.authorization)
            .header("Content-Type", "application/json")
            .header("anthropic-version", "bedrock-2023-05-31")
            .body(translated.body.clone())
            .send()
            .await
            .map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::ConnectError,
                    format!("Bedrock upstream request failed: {e}"),
                )
            })?;
        let status = resp.status().as_u16();
        ctx.upstream_status = status;

        if status >= 400 {
            // Forward Bedrock error responses as-is.
            let body = resp.bytes().await.map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::ReadError,
                    format!("Bedrock error response read failed: {e}"),
                )
            })?;
            let body_bytes = body.to_vec();
            let content_len = body_bytes.len().to_string();
            let mut resp_header = ResponseHeader::build(status, Some(2))?;
            resp_header.insert_header("Content-Type", "application/json")?;
            resp_header.insert_header("Content-Length", &content_len)?;
            session
                .write_response_header(Box::new(resp_header), false)
                .await?;
            session
                .write_response_body(Some(Bytes::from(body_bytes)), true)
                .await?;
            return Ok(());
        }

        if is_streaming {
            // Streaming: forward SSE chunks through the stream translator.
            let mut stream_translator = provider.translator.streaming_translator();
            let mut resp_header = ResponseHeader::build(200, Some(3))?;
            resp_header.insert_header("Content-Type", "text/event-stream")?;
            resp_header.insert_header("Cache-Control", "no-cache")?;
            resp_header.insert_header("Connection", "keep-alive")?;
            session
                .write_response_header(Box::new(resp_header), false)
                .await?;

            let mut resp = resp;
            let mut buffer = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::ReadError,
                    format!("Bedrock stream read failed: {e}"),
                )
            })? {
                buffer.extend_from_slice(&chunk);
                let out = stream_translator.process_chunk(&buffer, false);
                if !out.is_empty() {
                    session
                        .write_response_body(Some(Bytes::from(out)), false)
                        .await?;
                }
                buffer.clear();
            }
            // Flush any remaining buffered SSE data.
            let out = stream_translator.process_chunk(&buffer, true);
            if !out.is_empty() {
                session
                    .write_response_body(Some(Bytes::from(out)), false)
                    .await?;
            }
            session
                .write_response_body(Some(Bytes::new()), true)
                .await?;
        } else {
            // Non-streaming: read the full response and translate.
            let body = resp.bytes().await.map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::ReadError,
                    format!("Bedrock response read failed: {e}"),
                )
            })?;
            let translated_resp = provider.translator.translate_response(&body).map_err(|e| {
                pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(502),
                    format!("Bedrock response translation failed: {e}"),
                )
            })?;
            // Extract tokens for metrics.
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&translated_resp) {
                if let Some(usage) = v.get("usage") {
                    copy_usage_tokens(usage, ctx);
                }
            }
            let content_len = translated_resp.len().to_string();
            let mut resp_header = ResponseHeader::build(200, Some(2))?;
            resp_header.insert_header("Content-Type", "application/json")?;
            resp_header.insert_header("Content-Length", &content_len)?;
            session
                .write_response_header(Box::new(resp_header), false)
                .await?;
            session
                .write_response_body(Some(Bytes::from(translated_resp)), true)
                .await?;
        }

        Ok(())
    }
}

/// Buffer one `request_body_filter` chunk, keeping the upstream body open
/// until the whole body has been read.
///
/// pingora treats a `None` body returned from `request_body_filter` as the
/// END of the upstream body (`upstream_end_of_body = end_of_body || data.is_none()`),
/// so a body that arrives in several 64 KiB chunks would otherwise be
/// truncated to the first chunk — the upstream then receives an empty body
/// and rejects the call (400 Bad Request). Returning an EMPTY but present
/// buffer for a non-final chunk keeps the body open while sending nothing;
/// the accumulated bytes are forwarded as one piece at `end_of_stream`.
impl AiGateway {
    /// Apply the repairs a **native Responses** request body needs before it
    /// is forwarded upstream.
    ///
    /// Two layers: the generic Responses normalisation (a chat-style nested
    /// function tool -> the official flat shape) and the provider's own fixes
    /// (the OpenDesign link answers HTTP 400 `upstream_error` for the
    /// non-standard `tool_choice: "any"` that codex-cli sends).
    fn normalize_native_responses_body(provider: &ProviderState, body: &[u8]) -> Vec<u8> {
        let normalized = maxllm_translate::responses::normalize_responses_request(body);
        provider
            .translator
            .normalize_native_responses_request(&normalized)
    }

    /// Buffer one request body chunk, keeping the upstream body open until the
    /// whole body has been read.
    ///
    /// Once the final chunk arrives, the `client_request` audit event is
    /// recorded with the full original body — before any translation rewrites
    /// `request_body_buf`.
    fn buffer_request_body_chunk(
        &self,
        ctx: &mut RequestCtx,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) {
        if let Some(data) = body.take() {
            ctx.request_body_buf.extend_from_slice(&data);
            if end_of_stream && !ctx.client_request_recorded {
                ctx.client_request_recorded = true;
                ctx.client_body = Some(ctx.request_body_buf.clone());
                self.record_event(
                    EVENT_CLIENT_REQUEST,
                    ctx,
                    None,
                    None,
                    None,
                    ctx.client_body.clone(),
                );
            }
            if !end_of_stream {
                *body = Some(Bytes::new());
            }
        }
    }

    /// Rewrite a body-level `auto` sentinel to the model of the pool entry the
    /// request has already landed on.
    ///
    /// A route that is not an auto route only reveals the sentinel in the
    /// request body (`{"model": "auto"}`), and the provider is picked before
    /// the body is read, so the sentinel cannot influence that first hop. When
    /// the picked provider is itself a pool member — the session is pinned to
    /// one from an earlier turn, or the route's own provider is listed in the
    /// pool — forwarding the literal sentinel makes the upstream answer 404 and
    /// routes every single turn through a failover to a different member,
    /// which also throws away the upstream's prompt cache for the whole
    /// conversation. Rewriting the body to the entry's model keeps the request
    /// where it already is.
    ///
    /// Only chunked requests are rewritten: a Content-Length-framed body whose
    /// length changes would be truncated by the upstream.
    async fn adopt_sentinel_model(
        &self,
        hot: &HotState,
        session: &mut Session,
        ctx: &mut RequestCtx,
    ) -> pingora::Result<()> {
        if ctx.request_body_buf.is_empty() || ctx.provider_name.is_empty() {
            return Ok(());
        }
        let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
        else {
            return Ok(());
        };
        // The body names the pool: `auto`, `auto_home`, or whatever sentinel
        // the request carries. A model that is not a pool name is forwarded
        // untouched.
        let Some(sentinel) = parsed
            .get("model")
            .and_then(|m| m.as_str())
            .map(str::to_string)
        else {
            return Ok(());
        };
        let Some(pool) = hot.pool_named(&sentinel) else {
            return Ok(());
        };
        let Some((provider, model)) = pool
            .entry_for(&ctx.provider_name)
            .map(|e| (e.provider.clone(), e.model.clone()))
        else {
            warn!(
                provider = %ctx.provider_name,
                pool = %sentinel,
                "body names an auto pool, but the selected provider is not one of its members; the sentinel will reach the upstream"
            );
            return Ok(());
        };
        if let Some(obj) = parsed.as_object_mut() {
            obj.insert("model".into(), serde_json::Value::String(model.clone()));
        }
        if let Ok(bytes) = serde_json::to_vec(&parsed) {
            ctx.request_body_buf = bytes;
        }
        ctx.auto_pool = Some(pool.name.clone());
        ctx.auto_target = Some((provider, model.clone()));
        ctx.model = model.clone();
        Self::enforce_model_allowlist(session, ctx, &model, None).await?;
        ctx.plugin_ctx.model = model;
        Ok(())
    }
}

/// Extract the model from a Gemini-style request path
/// (`/v1beta/models/gemini-2.5-flash:generateContent`). Returns `None` when
/// the path does not carry a model.
fn model_from_request_path(path: &str) -> Option<String> {
    let marker = "/models/";
    let idx = path.find(marker)?;
    let rest = &path[idx + marker.len()..];
    let end = rest.find([':', '/', '?']).unwrap_or(rest.len());
    if end == 0 {
        None
    } else {
        Some(rest[..end].to_string())
    }
}

/// Resolve the upstream time budget for a request: a route may declare
/// `timeout_secs`, which overrides the hard-coded read/write defaults.
/// Returns `None` when the route does not configure one (or is unmatched).
fn route_timeout_secs(routes: &[RouteConfig], route_index: Option<usize>) -> Option<Duration> {
    route_index
        .and_then(|i| routes.get(i))
        .and_then(|r| r.timeout_secs)
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
}

/// The route's request-level failover budget: how many providers one request
/// may walk when the current provider fails. Unset (or absent) routes keep
/// the built-in default, which matches litellm_lb_go's bound.
/// Whether this request can be sent to another provider at all.
///
/// Pingora buffers at most `BODY_BUF_LIMIT` (64 KiB) of the client body for a
/// retry and silently drops the excess. Once the whole body has been read from
/// the client, replaying that prefix hands the next upstream an incomplete
/// body while the client waits for a response — neither side can make
/// progress, so both sit there until a timeout. That is a hang, not an error,
/// and it is what a ~100 KB codex request used to do whenever its first
/// provider answered 404.
///
/// So a failover is only started when the body is either still readable from
/// the client (nothing consumed yet, e.g. a connect failure) or small enough
/// to be replayed whole.
fn body_is_replayable(session: &mut Session, ctx: &RequestCtx) -> bool {
    // The body we already read is re-injected on the retry, so the replay
    // buffer's truncation does not matter.
    if ctx.client_body.is_some() {
        return true;
    }
    let truncated = session.as_ref().retry_buffer_truncated();
    !(truncated && session.as_mut().is_body_done())
}

fn max_failover_attempts(routes: &[RouteConfig], route_index: Option<usize>) -> u32 {
    route_index
        .and_then(|i| routes.get(i))
        .map(|r| r.max_failover_attempts)
        .unwrap_or(DEFAULT_MAX_FAILOVER_ATTEMPTS)
}

/// The route's same-provider retry budget: how many times a transient
/// upstream failure (HTTP 5xx) may be retried on the SAME provider before
/// the request moves to the next provider of the fallback chain, plus the
/// backoff to wait between those retries.
///
/// Unset (or absent) routes keep the built-in default: no same-provider
/// retry at all — a failed attempt walks straight to the fallback chain.
fn same_provider_retries(routes: &[RouteConfig], route_index: Option<usize>) -> (u32, u64) {
    route_index
        .and_then(|i| routes.get(i))
        .map(|r| (r.num_retries, r.retry_backoff_ms))
        .unwrap_or((0, DEFAULT_RETRY_BACKOFF_MS))
}

/// Whether an upstream HTTP status means "this provider failed" rather than
/// "this request is wrong" — i.e. whether another provider of the pool may
/// still serve the request.
///
/// Mirrors litellm_lb_go's `responsesFailoverAllowed`: quota exhaustion, rate
/// limits, auth rejections, a missing model and server errors all fail over,
/// while 400/422 are request faults every upstream would reject the same way.
///
/// The pool needs one exception: the requested model is sent to the first
/// provider of the chain, and a provider that does not carry it answers
/// 400 (one-api's `invalid params, unknown model`). A provider whose own
/// default model differs from the requested model is treated as not carrying
/// it, so the request walks on to a provider that has the model.
fn should_failover_status(status: u16, ctx: &RequestCtx, provider: &ProviderState) -> bool {
    match status {
        // The provider cannot serve this request now or at all: quota
        // exhausted (429), auth rejected (401/403), model or endpoint
        // missing (404/405/410), timed out (408), conflicting (409/425),
        // unaffordable (402).
        401 | 402 | 403 | 404 | 405 | 408 | 409 | 410 | 425 | 429 => true,
        // The provider itself failed.
        500..=599 => true,
        // The request is wrong — unless this provider simply does not carry
        // the requested model (its own default differs from the request).
        400 | 422 => {
            !ctx.model.is_empty()
                && provider
                    .default_model
                    .as_deref()
                    .is_some_and(|default_model| default_model != ctx.model)
        }
        // 2xx/3xx and anything else: the provider answered.
        _ => false,
    }
}

/// Whether a 404 error body is the "the model does not exist" rejection of an
/// OpenAI-compatible upstream (vLLM: "The model `X` does not exist. Valid
/// aliases: …", one-api: "unknown model") rather than a missing endpoint —
/// which answers the same status with "not found" and must not be rewritten.
fn body_reports_unknown_model(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    // Phrasings that alone identify an unknown model — no bare "not found".
    const STRONG: &[&str] = &[
        "unknown model",
        "no such model",
        "model_not_found",
        "model not found",
        "model does not exist",
        "model doesn't exist",
        "does not exist. valid aliases",
        "no model named",
        "invalid model",
        "unsupported model",
        "model is not supported",
        "model not supported",
    ];
    if STRONG.iter().any(|m| lower.contains(m)) {
        return true;
    }
    // Weak phrasings ("does not exist", "not found") also describe a missing
    // endpoint, so only trust them when the body is explicitly about a model.
    let mentions_model = lower.contains("model") || lower.contains("模型");
    const WEAK: &[&str] = &[
        "does not exist",
        "doesn't exist",
        "not exist",
        "not found",
        "不存在",
        "未找到",
    ];
    mentions_model && WEAK.iter().any(|m| lower.contains(m))
}

#[async_trait]
impl ProxyHttp for AiGateway {
    type CTX = RequestCtx;

    fn new_ctx(&self) -> Self::CTX {
        RequestCtx::new()
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<bool> {
        let path = session.req_header().uri.path();
        // Record it before anything can fail: a session that never reaches the
        // router still gets its path into the logs and the recorder.
        if ctx.request_path.is_empty() {
            ctx.request_path = path.to_string();
        }

        // Health check endpoint. The body carries enough state for an
        // operator (or a monitor) to see which pools are starving and which
        // providers are circuit-open without scraping Prometheus.
        if path == "/health" || path == "/healthz" {
            // Fast path: not an application request. Set the flag before the
            // first `await` — an aborted scrape must not reach `logging`
            // unmarked (see the guard there).
            ctx.skip_logging = true;
            let hot = self.hot.load();
            let providers_total = hot.providers.len();
            let mut providers_healthy = 0usize;
            let mut open_providers: Vec<&str> = Vec::new();
            let mut half_open_providers: Vec<&str> = Vec::new();
            for (name, provider) in hot.providers.iter() {
                // A breaker whose fail window elapsed is *half-open*: it will
                // be probed by the next request that reaches it. Reporting it
                // as "open" made /health claim a dozen circuit-open providers
                // when in fact none were rejecting traffic (2026-11-08: 12 of
                // the 16 listed were merely awaiting a probe).
                match provider.circuit_breaker.effective_state() {
                    crate::circuit_breaker::STATE_CLOSED => providers_healthy += 1,
                    crate::circuit_breaker::STATE_HALF_OPEN => {
                        half_open_providers.push(name.as_str())
                    }
                    _ => open_providers.push(name.as_str()),
                }
            }
            let mut pools = serde_json::Map::new();
            let mut degraded = providers_total > 0 && providers_healthy == 0;
            for pool in &hot.auto_pools {
                let total = pool.entries.len();
                let healthy = pool
                    .entries
                    .iter()
                    .filter(|entry| {
                        hot.providers
                            .get(&entry.provider)
                            .map(|p| p.circuit_breaker.effective_state() == 0)
                            .unwrap_or(false)
                    })
                    .count();
                if total > 0 && healthy == 0 {
                    degraded = true;
                }
                pools.insert(
                    pool.name.clone(),
                    serde_json::json!({"healthy": healthy, "total": total}),
                );
            }
            open_providers.sort_unstable();
            half_open_providers.sort_unstable();
            let body = serde_json::to_vec(&serde_json::json!({
                "status": if degraded { "degraded" } else { "ok" },
                "version": env!("CARGO_PKG_VERSION"),
                "providers": {
                    "healthy": providers_healthy,
                    "total": providers_total,
                },
                "pools": pools,
                "open_providers": open_providers,
                "half_open_providers": half_open_providers,
            }))
            .unwrap_or_else(|_| br#"{"status":"degraded"}"#.to_vec());
            let content_len = body.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "application/json")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from(body)), true)
                .await?;
            return Ok(true);
        }

        // Version endpoint — a GET that used to fall through to the POST-only
        // API guard and answer 405.
        if path == "/version" {
            // Fast path: not an application request. Set the flag before the
            // first `await` — an aborted scrape must not reach `logging`
            // unmarked (see the guard there).
            ctx.skip_logging = true;
            let body = format!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION")).into_bytes();
            let content_len = body.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "application/json")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from(body)), true)
                .await?;
            return Ok(true);
        }

        // Load hot state snapshot for this request
        let hot = self.hot.load();

        // /v1/models endpoint — OpenAI-compatible model list (fast path)
        if path == "/v1/models" {
            // Fast path: not an application request. Set the flag before the
            // first `await` — an aborted scrape must not reach `logging`
            // unmarked (see the guard there).
            ctx.skip_logging = true;
            let body_bytes = hot.models_response.clone();
            let content_len = body_bytes.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "application/json")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session.write_response_body(Some(body_bytes), true).await?;
            return Ok(true);
        }

        // Metrics endpoint (bypasses plugins). The scrape is not an
        // application request: it is not counted in the active-request
        // gauge, and the exposition must not include the scrape itself —
        // otherwise the gauge could never read 0 while it is being scraped.
        if path == "/metrics" && hot.metrics_enabled {
            // Fast path: not an application request. Set the flag before the
            // first `await` — an aborted scrape must not reach `logging`
            // unmarked (see the guard there).
            ctx.skip_logging = true;
            // The exposition names every provider, model, key and price in the
            // deployment, so it is served to loopback scrapers only unless the
            // operator opts in. Nothing below the route layer is reached here,
            // so this check is the whole authorisation story for the endpoint.
            let client_ip = session
                .client_addr()
                .and_then(|addr| addr.as_inet().map(|sock| sock.ip()));
            if !metrics_visible_to(hot.metrics_allow_remote, client_ip) {
                return Self::send_error_response(
                    session,
                    403,
                    "metrics is loopback-only; set [metrics] allow_remote = true to expose it",
                )
                .await;
            }
            // The breaker gauge is otherwise only written on transitions, so
            // a provider whose fail window elapsed kept reading OPEN until a
            // request happened to touch it. Evaluate the effective state at
            // scrape time (read-only: no probe slot is consumed).
            for (name, provider) in hot.providers.iter() {
                METRICS
                    .circuit_breaker_state
                    .with_label_values(&[name])
                    .set(provider.circuit_breaker.effective_state() as i64);
            }
            // The in-flight gauge is derived from the counter the
            // least-connections router reads, so it can never drift.
            for (provider, count) in crate::provider_stats::inflight_counts() {
                METRICS
                    .provider_inflight_requests
                    .with_label_values(&[&provider])
                    .set(count as i64);
            }
            let body = METRICS.encode();
            let content_len = body.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "text/plain; version=0.0.4")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from(body)), true)
                .await?;
            return Ok(true);
        }

        METRICS.active_requests.inc();
        // Paired with the decrement in `logging`: the hook must only give back
        // the slot this request took.
        ctx.active_request_counted = true;

        // Own the path for later use (after the borrow above is done)
        let path = path.to_string();

        // Reject non-POST methods on API routes (health/metrics/models already handled above)
        let method = session.req_header().method.clone();
        if method != http::method::Method::POST
            && !path.starts_with("/admin/")
            && path != "/v1/models"
        {
            METRICS.active_requests.dec();
            ctx.skip_logging = true;
            return Self::send_error_response(
                session,
                405,
                &format!("Method {} not allowed. Use POST.", method),
            )
            .await;
        }

        // Admin API endpoints
        if path.starts_with("/admin/") {
            let handled = self.handle_admin_request(session, &path).await?;
            // The admin handler answers the request itself and already
            // decremented the active-request gauge — the logging hook must
            // not decrement it a second time.
            ctx.skip_logging = true;
            return Ok(handled);
        }

        // Extract client IP for plugins
        if let Some(addr) = session.client_addr() {
            if let Some(sock) = addr.as_inet() {
                ctx.plugin_ctx.client_ip = Some(sock.ip().to_string());
            }
        }

        // ── Virtual-key authentication ──
        // When the admin module is enabled, a bearer token that resolves to
        // a virtual key created through the admin API is validated against
        // the key store here (active, unexpired, within budget) and its key
        // id is remembered so the logging hook can record spend against it.
        // The `key_auth` plugin defers to this check via the
        // `virtual_key_auth` extension marker, so static config keys keep
        // working unchanged and keys that are neither static nor virtual
        // still hit the plugin's 401.
        if let Some(enforcer) = &self.budget_enforcer {
            let auth = session
                .req_header()
                .headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let token = auth.strip_prefix("Bearer ").unwrap_or(auth);
            if token.starts_with("sk-maxllm-") {
                match maxllm_admin::keys::validate_key(enforcer.store().as_ref(), token) {
                    Ok(Some(mut key)) => {
                        // Roll an expired budget window forward so stale
                        // spend from a previous window does not block the key.
                        if let Err(e) = enforcer.reset_key_budget_if_expired(&mut key) {
                            warn!(error = %e, "Failed to reset expired budget window");
                        }
                        match enforcer.check_budget(&key) {
                            Ok(()) => {
                                // Per-key rate limits: the RPM sliding
                                // window is consumed here; the TPM window is
                                // peeked (tokens are recorded when the
                                // response completes) and an in-flight slot
                                // is taken for the concurrency limit.
                                // The checks are lazy: a key that is
                                // already over its RPM limit is not charged
                                // for a TPM peek or a concurrency slot.
                                let denied = self
                                    .key_rate_limiter
                                    .check_request(&key.id, key.rpm_limit)
                                    .err()
                                    .map(|_| "rate limit exceeded (requests per minute)")
                                    .or_else(|| {
                                        self.key_rate_limiter
                                            .check_tokens(&key.id, key.tpm_limit)
                                            .err()
                                            .map(|_| "rate limit exceeded (tokens per minute)")
                                    })
                                    .or_else(|| {
                                        self.key_rate_limiter
                                            .acquire(&key.id, key.max_parallel_requests)
                                            .err()
                                            .map(|_| "too many parallel requests")
                                    });
                                if let Some(reason) = denied {
                                    METRICS.active_requests.dec();
                                    ctx.skip_logging = true;
                                    return Self::send_error_response(session, 429, reason).await;
                                }
                                ctx.key_in_flight =
                                    Some(crate::key_rate_limit::KeyInFlightGuard::new(
                                        Arc::clone(&self.key_rate_limiter),
                                        key.id.clone(),
                                    ));
                                ctx.plugin_ctx.client_id = Some(key.id.clone());
                                ctx.plugin_ctx
                                    .extensions
                                    .insert("virtual_key_auth".into(), "1".into());
                                ctx.virtual_key_id = Some(key.id.clone());
                                ctx.virtual_key_allowed_models = key.allowed_models.clone();
                            }
                            Err(e) => {
                                METRICS.active_requests.dec();
                                ctx.skip_logging = true;
                                return Self::send_error_response(
                                    session,
                                    e.http_status(),
                                    &e.to_string(),
                                )
                                .await;
                            }
                        }
                    }
                    Ok(None) => {
                        // Not a virtual key — fall through to the plugin
                        // chain (a static config key or an unknown token).
                    }
                    Err(maxllm_admin::keys::KeyError::Revoked) => {
                        METRICS.active_requests.dec();
                        ctx.skip_logging = true;
                        return Self::send_error_response(session, 401, "API key revoked").await;
                    }
                    Err(maxllm_admin::keys::KeyError::Expired) => {
                        METRICS.active_requests.dec();
                        ctx.skip_logging = true;
                        return Self::send_error_response(session, 401, "API key expired").await;
                    }
                    Err(maxllm_admin::keys::KeyError::BudgetExceeded) => {
                        METRICS.active_requests.dec();
                        ctx.skip_logging = true;
                        return Self::send_error_response(session, 402, "budget exceeded").await;
                    }
                    Err(e) => {
                        warn!(error = %e, "Virtual-key validation failed");
                        METRICS.active_requests.dec();
                        ctx.skip_logging = true;
                        return Self::send_error_response(session, 500, "key validation error")
                            .await;
                    }
                }
            }
        }

        // Run global plugin chain (auth, request_id, ip_restriction, etc.)
        if let Some(resp) = hot
            .global_chain
            .run_request(session, &mut ctx.plugin_ctx)
            .await?
        {
            METRICS.active_requests.dec();
            ctx.skip_logging = true;
            resp.send(session).await?;
            return Ok(true);
        }

        // Route matching
        let route_index = match Self::match_route(&hot, &path) {
            Some(idx) => idx,
            None => {
                METRICS.active_requests.dec();
                ctx.skip_logging = true;
                return Self::send_error_response(
                    session,
                    404,
                    &format!("No route matches path: {path}"),
                )
                .await;
            }
        };

        ctx.route_index = Some(route_index);
        ctx.plugin_ctx.route_path = hot.routes[route_index].path.clone();
        let route = &hot.routes[route_index];
        ctx.endpoint_type = route.endpoint_type;

        // `/v1/responses` is a prefix route on purpose: a client points its
        // base URL at `/v1/responses/<provider-or-pool>` and appends
        // `/responses`. The same prefix match silently swallowed retired
        // provider names (`/v1/responses/one_api_*`) and served them from the
        // generic pool. Reject a segment that names neither a provider nor a
        // pool, and say which names are valid.
        if let Some(segment) = Self::unknown_responses_subroute(&hot, route, &path) {
            METRICS.active_requests.dec();
            ctx.skip_logging = true;
            return Self::send_error_response(
                session,
                404,
                &format!(
                    "unknown provider or pool '{segment}' in {path}; use /v1/responses/<provider>, /v1/responses/auto or /v1/responses/auto_home"
                ),
            )
            .await;
        }

        // The segment named a real provider but no explicit route matched it,
        // so the request is sitting on the pooled `/v1/responses` route. Pin
        // it: the caller asked for that provider, not for the pool.
        if let Some(pin) = Self::responses_subroute_pin(&hot, route, &path) {
            ctx.plugin_ctx.route_path = format!("/v1/responses/{pin}");
            ctx.path_pinned_provider = Some(pin);
        }

        // For passthrough, extract the path suffix after the route prefix
        if route.endpoint_type == EndpointType::Passthrough {
            let suffix = &path[route.path.len()..];
            ctx.passthrough_path = Some(if suffix.is_empty() || suffix == "/" {
                "/".to_string()
            } else if !suffix.starts_with('/') {
                format!("/{suffix}")
            } else {
                suffix.to_string()
            });
        }

        // Store route-specific guardrail scope
        if !route.guardrails.is_empty() {
            ctx.route_guardrails = Some(route.guardrails.clone());
        }

        // Run route-specific plugin chain (rate_limit, cors, etc.)
        if let Some(resp) = hot.route_chains[route_index]
            .run_request(session, &mut ctx.plugin_ctx)
            .await?
        {
            METRICS.active_requests.dec();
            ctx.skip_logging = true;
            resp.send(session).await?;
            return Ok(true);
        }

        // Capture the request identity for the recorder and session-sticky.
        ctx.request_method = session.req_header().method.to_string();
        ctx.request_path = path.clone();
        // A synthetic health probe is served normally but kept out of every
        // production signal: metrics, request logs and breaker accounting.
        ctx.probe_request = session.req_header().headers.get("x-maxllm-probe").is_some();

        // Session-sticky: capture the session identity from the request
        // headers. codex-cli sends `session-id`, Claude Code sends
        // `X-Session-ID` / `X-Conversation-ID`; the body fallback is applied
        // later in request_body_filter.
        ctx.session_id = crate::session_sticky::resolve_from_headers(&session.req_header().headers);

        // Select provider (with circuit breaker check and routing strategy)
        let _provider = match self.select_provider(&hot, route, ctx) {
            Some(p) => {
                // The request is in flight for this provider — the
                // least-connections strategy counts it.
                crate::provider_stats::acquire(&ctx.provider_name);
                p
            }
            None => {
                METRICS.active_requests.dec();
                ctx.skip_logging = true;
                return Self::send_error_response(
                    session,
                    503,
                    &self.unavailable_reason(&hot, route, ctx),
                )
                .await;
            }
        };

        // ── Bedrock SigV4 path ──
        // Bedrock authenticates with AWS SigV4, whose signature covers the
        // SHA-256 of the translated request payload. The exact bytes must be
        // known before the request headers are written. Pingora's proxy
        // pipeline streams body chunks after the headers have been sent, so
        // the payload hash cannot be embedded in the Authorization header
        // there. Bedrock requests are therefore read, translated, signed and
        // sent via reqwest in `handle_bedrock_request` (which buffers the
        // full body), and the response is translated back before returning.
        if let Some(provider) = hot.providers.get(&ctx.provider_name) {
            if provider.kind == maxllm_config::ProviderKind::Bedrock {
                self.handle_bedrock_request(session, ctx, provider).await?;
                return Ok(true);
            }
        }

        // Sync plugin ctx with request ctx
        ctx.plugin_ctx.provider_name = ctx.provider_name.clone();

        // The `client_request` audit event is recorded by the body filter,
        // where the full request body is known — recording it here would
        // duplicate the event without the body.

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Box<HttpPeer>> {
        // Resolve the upstream hostname here instead of letting
        // `HttpPeer::new` do it: pingora's constructor calls
        // `to_socket_addrs().unwrap()` internally and PANICS the worker
        // thread when DNS fails (e.g. a corp-only host reached from a
        // non-corp network), leaving the client with a dropped connection
        // instead of a 502. Look the address up asynchronously and surface a
        // clean error. Passing a resolved `SocketAddr` to `HttpPeer::new`
        // skips the fallible lookup entirely.
        //
        // A DNS failure means this provider is unreachable *before* the
        // connect phase, so it would bypass `fail_to_connect`'s failover.
        // Treat it like a connect failure: count it against the breaker,
        // switch to the next pool/fallback provider, and resolve again —
        // bounded by the same failover budget.
        let addr = loop {
            let hot = self.hot.load();
            let provider = match hot.providers.get(&ctx.provider_name) {
                Some(p) => p,
                None => {
                    warn!(provider = %ctx.provider_name, "provider not found in upstream_peer (config may have been reloaded)");
                    return Err(pingora::Error::explain(
                        pingora::ErrorType::HTTPStatus(502),
                        format!("provider '{}' no longer exists", ctx.provider_name),
                    ));
                }
            };
            // Clone what the lookup needs: the provider borrow must not
            // cross the await point. `to_socket_addrs` accepts (String, u16).
            let host = provider.host.clone();
            let port = provider.port;
            let resolved = match tokio::net::lookup_host((host.clone(), port)).await {
                Ok(mut addrs) => addrs.next(),
                Err(_) => None,
            };
            match resolved {
                Some(a) => break a,
                None => {
                    if !self.try_dns_failover(ctx).await {
                        return Err(pingora::Error::explain(
                            pingora::ErrorType::HTTPStatus(502),
                            format!("DNS resolution failed for '{}'", host),
                        ));
                    }
                }
            }
        };
        let hot = self.hot.load();
        let provider = match hot.providers.get(&ctx.provider_name) {
            Some(p) => p,
            None => {
                return Err(pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(502),
                    format!("provider '{}' no longer exists", ctx.provider_name),
                ));
            }
        };
        let mut peer = HttpPeer::new(addr, provider.tls, provider.sni.clone());
        peer.options.connection_timeout = Some(provider.connect_timeout);
        peer.options.total_connection_timeout = provider.total_connect_timeout;
        // A route may declare its own upstream time budget (`timeout_secs`).
        // Honour it instead of silently ignoring the setting, otherwise a
        // route configured for a slow upstream is still cut off by the
        // hard-coded defaults below. Unset (or zero) keeps the defaults.
        let route_timeout = route_timeout_secs(&hot.routes, ctx.route_index);
        peer.options.read_timeout = Some(route_timeout.unwrap_or(Duration::from_secs(120)));
        peer.options.write_timeout = Some(route_timeout.unwrap_or(Duration::from_secs(30)));

        Ok(Box::new(peer))
    }

    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        // A same-provider retry carries a backoff to wait before the next
        // attempt, so a transient upstream error is not hammered again
        // immediately. Consumed here — before the request is (re)sent — and
        // cleared, so only the retried attempt waits.
        if let Some(backoff_ms) = ctx.pending_retry_backoff_ms.take() {
            if backoff_ms > 0 {
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
        let hot = self.hot.load();
        let provider = match hot.providers.get(&ctx.provider_name) {
            Some(p) => p,
            None => {
                warn!(provider = %ctx.provider_name, "provider not found in upstream_request_filter (config may have been reloaded)");
                return Err(pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(502),
                    format!("provider '{}' no longer exists", ctx.provider_name),
                ));
            }
        };

        // Set upstream path based on endpoint type
        let path = match ctx.endpoint_type {
            EndpointType::Responses => {
                if ctx.native_responses {
                    // The provider speaks the Responses contract natively: the
                    // request is forwarded unchanged to its own /responses path.
                    provider
                        .responses_path
                        .clone()
                        .unwrap_or_else(|| provider.translator.upstream_path().to_string())
                } else if provider.kind == ProviderKind::Gemini {
                    // Responses is translated into chat completions for the
                    // upstream call, so it uses the provider's chat path.
                    let model = provider
                        .default_model
                        .as_deref()
                        .unwrap_or("gemini-2.5-flash");
                    format!(
                        "/v1beta/models/{}:generateContent?key={}",
                        model, provider.api_key
                    )
                } else {
                    provider.translator.upstream_path().to_string()
                }
            }
            EndpointType::Passthrough => {
                // Use the suffix path captured from the client request
                ctx.passthrough_path
                    .clone()
                    .unwrap_or_else(|| "/".to_string())
            }
            EndpointType::Native => {
                // Use the original client request path (already in provider format).
                // For Gemini, append API key as query param.
                let original = session.req_header().uri.path().to_string();
                if provider.kind == ProviderKind::Gemini {
                    format!("{}?key={}", original, provider.api_key)
                } else {
                    native_upstream_path(&original, provider.translator.upstream_path())
                }
            }
            EndpointType::Embeddings => {
                // Embeddings endpoint: use the embeddings path for each provider
                if provider.kind == ProviderKind::Gemini {
                    let model = provider
                        .default_model
                        .as_deref()
                        .unwrap_or("text-embedding-004");
                    format!(
                        "/v1beta/models/{}:embedContent?key={}",
                        model, provider.api_key
                    )
                } else if provider.kind == ProviderKind::Cohere {
                    "/v2/embed".to_string()
                } else {
                    // OpenAI and OpenAI-compatible providers
                    "/v1/embeddings".to_string()
                }
            }
            _ => {
                // Normal (chat completions): use translator's upstream path
                if provider.kind == ProviderKind::Gemini {
                    let model = provider
                        .default_model
                        .as_deref()
                        .unwrap_or("gemini-2.5-flash");
                    // Always use generateContent (non-streaming endpoint).
                    // If client requested streaming, the gateway will convert
                    // the JSON response to SSE in upstream_response_body_filter.
                    format!(
                        "/v1beta/models/{}:generateContent?key={}",
                        model, provider.api_key
                    )
                } else {
                    provider.translator.upstream_path().to_string()
                }
            }
        };
        upstream_request.set_uri(path.parse().expect("valid path"));

        // Remove downstream auth header — providers use their own auth
        let _ = upstream_request.remove_header("Authorization");

        // Remove Accept-Encoding to prevent upstream from gzip-compressing the response.
        // The gateway needs raw bytes to translate provider responses (e.g. Gemini → OpenAI).
        // Without this, compressed responses skip translation and leak native format to clients.
        if ctx.endpoint_type != EndpointType::Passthrough {
            let _ = upstream_request.remove_header("Accept-Encoding");
        }

        // Set provider-specific headers
        for (key, value) in provider.translator.upstream_headers(&provider.api_key) {
            upstream_request.insert_header(key, &value)?;
        }

        // Set Host header
        upstream_request.insert_header("Host", &provider.host)?;

        // opencode.ai requires an `x-opencode-session` header on every request
        // for upstream routing / prompt-cache affinity; without it the
        // upstream rejects the call with 400 MissingSessionID. The opencode
        // CLI always sends it (and Pingora forwards client headers verbatim);
        // third-party clients often don't, so one is injected.
        //
        // A header that is PRESENT but EMPTY is treated exactly like a
        // missing one: the upstream rejects an empty value with the same
        // 400 MissingSessionID (verified live), and a client whose session
        // template rendered to "" still sends the header. Only a non-blank
        // client value is kept verbatim — and every injected source below is
        // held to the same rule, so a present-but-empty affinity falls through
        // to the next source instead of short-circuiting the lookup.
        let client_session = upstream_request
            .headers
            .get(maxllm_translate::opencode::SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if provider.kind == ProviderKind::OpenCode {
            // The source matters as much as the value: opencode.ai keys its
            // prompt cache on this header, so a per-request value makes every
            // turn a cold start. Measured on both opencode channels with one
            // repeated 2911-token prompt: no affinity header → 0 cached tokens
            // on both rounds; a fixed id → 2816/2911 (96.7%) on the second.
            //
            // Priority: the client's own header (it already decided what its
            // conversation is called) → an explicit affinity header → the
            // gateway's session identity → a fresh UUID. `ctx.session_id` is
            // resolved from the request headers in `request_filter`, so it is
            // known here, and it is the very id session-sticky pins on —
            // reusing it keeps the upstream conversation warm exactly when the
            // gateway keeps the request on the same provider.
            //
            // The body-carried spellings (`prompt_cache_key`,
            // `client_metadata.*`, `metadata.user_id`) are *not* available
            // here: pingora writes the upstream request header before it reads
            // the downstream body. They are also redundant in practice — across
            // every recorded request the body never produced a session id the
            // headers had not already produced.
            let (affinity_id, source) = match client_session {
                Some(owned) => (owned, "client"),
                None => {
                    let affinity_header = ["x-session-affinity", "x-opencode-session-id"]
                        .iter()
                        .filter_map(|name| session.req_header().headers.get(*name))
                        .filter_map(|v| v.to_str().ok())
                        .map(|s| s.trim().to_string())
                        .find(|s| !s.is_empty());
                    if let Some(id) = affinity_header {
                        (id, "affinity_header")
                    } else if let Some(id) = ctx
                        .session_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                    {
                        (id, "session")
                    } else {
                        (uuid::Uuid::new_v4().to_string(), "generated")
                    }
                }
            };
            METRICS
                .opencode_affinity_source_total
                .with_label_values(&[source])
                .inc();
            if source != "client" {
                upstream_request
                    .insert_header(maxllm_translate::opencode::SESSION_HEADER, &affinity_id)?;
            }
        }

        // The Cloudflare edge of opencode.ai and commandcode.ai bans requests
        // whose User-Agent looks like a script / HTTP-client fingerprint (e.g.
        // `Python-urllib/3.11`) with HTTP 403 "error code: 1010" (browser
        // signature banned). Pingora forwards the client's User-Agent
        // verbatim, so an SDK client would otherwise be banned outright.
        // Inject a safe, generic UA unless the client already sent one that
        // is not a known script fingerprint (curl / browser UAs are verified
        // allowed by both upstreams).
        if matches!(
            provider.kind,
            ProviderKind::OpenCode | ProviderKind::CommandCode
        ) {
            let ua = upstream_request
                .headers
                .get("User-Agent")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_ascii_lowercase());
            let needs_injection = match &ua {
                None => true,
                Some(s) => is_script_like_user_agent(s),
            };
            if needs_injection {
                upstream_request.insert_header("User-Agent", SAFE_UPSTREAM_UA)?;
            }
        }

        // Body framing strategy:
        // - Passthrough endpoints: keep original Content-Length (body forwarded as-is)
        // - OpenAI/Azure (body-passthrough translators): keep original Content-Length
        //   because OpenAI/Cloudflare REJECTS Transfer-Encoding: chunked with 400.
        //   The body flows through request_body_filter without modification.
        // - Other translators (Gemini, Anthropic, etc.): remove Content-Length
        //   and use chunked encoding since body size changes after translation.
        if ctx.endpoint_type == EndpointType::Passthrough {
            // Keep original Content-Length
        } else {
            // The request body is forwarded verbatim (its size must not change)
            // when the upstream already speaks the request's contract: a
            // native Responses upstream, or a chat-body passthrough to an
            // openai-like upstream. A Responses request destined for a chat-only
            // upstream must be rewritten into chat completions — it is never
            // verbatim, even when that upstream is openai-like.
            let is_native_responses =
                ctx.endpoint_type == EndpointType::Responses && ctx.native_responses;
            let is_responses_to_chat =
                ctx.endpoint_type == EndpointType::Responses && !ctx.native_responses;
            let openai_style = provider.translator.name() == "openai"
                || provider.translator.name() == "azure_openai";
            // A route that rewrites the request body's model field — the auto
            // sentinel resolution, or a model substitution after a 400
            // "unknown model" — changes the body's size, so the request must
            // travel chunked: never verbatim passthrough. Without this the
            // retry re-enters the `keep_request_content_length` branch, which
            // forwards the body untouched and the substitution never reaches
            // the upstream (2026-11-08: native-Responses providers answered
            // the same 400 twice while `ctx.model` already showed the new
            // name).
            let is_auto_route = ctx.auto_target.is_some()
                || ctx.model_override.is_some()
                || hot
                    .routes
                    .get(ctx.route_index.unwrap_or(usize::MAX))
                    .map(|r| r.auto_model)
                    .unwrap_or(false);
            // A provider whose upstream rejects part of the Responses
            // contract repairs the body before forwarding it (the OpenDesign
            // link answers 400 for the non-standard `tool_choice: "any"`).
            // A repaired body is a different size, so it cannot keep the
            // client's Content-Length framing — it travels chunked, like any
            // other body the gateway rewrites.
            let repairs_native_body =
                is_native_responses && provider.translator.repairs_native_responses_body();
            // The provider was picked before the body was read, so a pool
            // sentinel (`{"model": "auto"}`) can only be seen later, in
            // request_body_filter. A body kept byte-verbatim cannot be
            // rewritten there (changing its length would truncate it), and the
            // literal sentinel reaches the upstream — which answers 404/400 and
            // costs a failover plus the conversation's prompt cache on every
            // turn. When the provider serves an auto pool, frame the body
            // chunked like the auto route already does, so the sentinel can be
            // resolved to this entry's model.
            let provider_serves_a_pool = hot
                .auto_pools
                .iter()
                .any(|pool| pool.entry_for(&ctx.provider_name).is_some());
            let may_carry_pool_sentinel =
                ctx.endpoint_type == EndpointType::Responses && provider_serves_a_pool;
            // A Responses body rewritten into chat completions has an
            // unpredictable size (the chat form can be larger than the
            // Responses form), so it travels chunked instead of keeping the
            // original Content-Length — which would truncate a larger body.
            let verbatim = !is_auto_route
                && !may_carry_pool_sentinel
                && ((is_native_responses && !repairs_native_body)
                    || (openai_style && !is_responses_to_chat));

            let compaction_enabled = ctx
                .plugin_ctx
                .extensions
                .get("auto_compact")
                .map(|v| v.as_str())
                == Some("1");
            if verbatim && !compaction_enabled {
                // Mark as body-passthrough so request_body_filter lets chunks flow
                ctx.keep_request_content_length = true;
                // Keep original Content-Length header (don't remove, don't set chunked).
                // A native Responses body may be normalised (nested function
                // tools -> flat) in request_body_filter; remember the original
                // Content-Length so the rewritten body can be padded to match.
                if is_native_responses {
                    if let Some(cl) = upstream_request.headers.get("Content-Length") {
                        if let Ok(cl_str) = cl.to_str() {
                            ctx.plugin_ctx
                                .extensions
                                .insert("original_content_length".into(), cl_str.to_string());
                        }
                    }
                }
            } else if verbatim && compaction_enabled {
                // Compaction will modify the body but OpenAI/Cloudflare rejects
                // Transfer-Encoding: chunked. Strategy: keep the ORIGINAL
                // Content-Length, then in request_body_filter, pad the compacted
                // body with spaces to match the original size. JSON parsers
                // ignore trailing whitespace, so the upstream sees valid JSON.
                ctx.keep_request_content_length = true;
                // Store original Content-Length for padding target
                if let Some(cl) = upstream_request.headers.get("Content-Length") {
                    if let Ok(cl_str) = cl.to_str() {
                        ctx.plugin_ctx
                            .extensions
                            .insert("original_content_length".into(), cl_str.to_string());
                    }
                }
            } else {
                let _ = upstream_request.remove_header("Content-Length");
                upstream_request.insert_header("Transfer-Encoding", "chunked")?;
            }
        }

        // Run plugin chains on upstream request
        hot.global_chain
            .run_upstream_request(session, upstream_request, &mut ctx.plugin_ctx)
            .await?;
        if let Some(idx) = ctx.route_index {
            hot.route_chains[idx]
                .run_upstream_request(session, upstream_request, &mut ctx.plugin_ctx)
                .await?;
        }

        // Mark when we hand off to upstream — used to compute overhead
        ctx.upstream_send_time = Some(std::time::Instant::now());

        // Audit trail: the request was handed to the upstream.
        self.record_event(EVENT_PROVIDER_REQUEST, ctx, None, None, None, None);

        Ok(())
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        // A retried request: pingora replays at most 64 KiB of the body. Feed
        // the whole body we already read as the final chunk instead, and drop
        // the truncated remainder it replays afterwards.
        if ctx.replay_injected {
            *body = None;
            return Ok(());
        }
        let end_of_stream = match ctx.replay_body.take() {
            Some(original) => {
                ctx.replay_injected = true;
                *body = Some(Bytes::from(original));
                true
            }
            None => end_of_stream,
        };

        // Session-sticky: the session identity may also live in the request
        // body (`metadata.user_id`, `client_metadata.session_id`,
        // `prompt_cache_key`). Only buffered bodies are inspected — the
        // header path already covers codex-cli and Claude Code. Runs before
        // the passthrough shortcut so every buffered body is scanned.
        if ctx.session_id.is_none() && end_of_stream && !ctx.request_body_buf.is_empty() {
            ctx.session_id = crate::session_sticky::resolve_from_body(&ctx.request_body_buf);
        }

        // ── PASSTHROUGH: zero processing, forward chunks as-is ──
        // A virtual key with a model allow-list is the exception: the body is
        // buffered so the model (from the body's `model` field, or a
        // Gemini-style `/models/<model>:<method>` path) can be checked
        // before anything reaches the upstream.
        if ctx.endpoint_type == EndpointType::Passthrough {
            if ctx.virtual_key_allowed_models.is_empty() {
                return Ok(());
            }
            self.buffer_request_body_chunk(ctx, body, end_of_stream);
            if end_of_stream && !ctx.body_translated {
                ctx.body_translated = true;
                let hot = self.hot.load();
                let body_model = serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    .ok()
                    .and_then(|v| {
                        v.get("model")
                            .and_then(|m| m.as_str())
                            .map(|m| Self::resolve_model_alias(&hot, m))
                    });
                let model = body_model.or_else(|| {
                    ctx.passthrough_path
                        .as_deref()
                        .and_then(model_from_request_path)
                });
                if let Some(ref model) = model {
                    ctx.model = model.clone();
                    Self::enforce_model_allowlist(session, ctx, model, None).await?;
                }
                drop(hot);
            }
            *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
            return Ok(());
        }

        // ── OPENAI PASSTHROUGH: forward body as-is but extract metadata ──
        // For OpenAI/Azure providers, the body isn't translated, so we forward
        // chunks immediately to preserve Content-Length framing. We buffer a
        // copy to extract model/streaming info on end_of_stream.
        //
        // Exception: if auto-compaction is enabled, we take the body, compact it,
        // and replace it — switching to chunked framing since the body size changes.
        if ctx.keep_request_content_length {
            let compaction_enabled = !ctx.native_responses
                && ctx
                    .plugin_ctx
                    .extensions
                    .get("auto_compact")
                    .map(|v| v.as_str())
                    == Some("1");

            if compaction_enabled {
                // Take the body into our buffer so we can mutate it.
                self.buffer_request_body_chunk(ctx, body, end_of_stream);
                if end_of_stream && !ctx.body_translated {
                    // Fall through to the normal compaction + translation path.
                    // Clear keep_request_content_length so the normal path runs.
                    ctx.keep_request_content_length = false;
                    // Do NOT set body_translated here — let the normal path do it.
                    // Continue below — do NOT return here.
                } else {
                    return Ok(());
                }
            } else if ctx.native_responses {
                // NATIVE RESPONSES: buffer the body so a request carrying
                // chat-style nested function tools can be normalised to the
                // official Responses shape before the byte-for-byte forward.
                // The original Content-Length is preserved by padding with
                // spaces (JSON parsers ignore trailing whitespace), matching
                // the auto-compaction framing.
                self.buffer_request_body_chunk(ctx, body, end_of_stream);
                if end_of_stream && !ctx.body_translated {
                    ctx.body_translated = true;
                    let hot = self.hot.load();
                    if let Ok(parsed) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    {
                        if let Some(model) = parsed.get("model").and_then(|m| m.as_str()) {
                            let resolved = Self::resolve_model_alias(&hot, model);
                            ctx.model = resolved.clone();
                            Self::enforce_model_allowlist(session, ctx, &resolved, Some(model))
                                .await?;
                            ctx.plugin_ctx.model = resolved;
                        }
                        if let Some(stream) = parsed.get("stream").and_then(|v| v.as_bool()) {
                            ctx.is_streaming = stream;
                            if stream {
                                let provider = hot.providers.get(&ctx.provider_name);
                                if let Some(p) = provider {
                                    match ctx.stream_translator.lock() {
                                        Ok(mut guard) => {
                                            if ctx.native_responses {
                                                // The upstream stream already
                                                // speaks the Responses
                                                // contract — copy it verbatim.
                                                *guard = Some(Box::new(
                                                    maxllm_translate::NativePassthroughStream,
                                                ));
                                            } else {
                                                *guard = Some(p.translator.streaming_translator());
                                            }
                                        }
                                        Err(e) => {
                                            warn!("stream_translator mutex poisoned: {e}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let normalized = match hot.providers.get(&ctx.provider_name) {
                        Some(p) => Self::normalize_native_responses_body(p, &ctx.request_body_buf),
                        None => maxllm_translate::responses::normalize_responses_request(
                            &ctx.request_body_buf,
                        ),
                    };
                    let mut out = normalized;
                    if let Some(original_cl) = ctx
                        .plugin_ctx
                        .extensions
                        .get("original_content_length")
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        if out.len() < original_cl {
                            out.extend(std::iter::repeat_n(b' ', original_cl - out.len()));
                        }
                    }
                    *body = Some(Bytes::from(out));
                    ctx.request_body_buf = Vec::new();
                }
                return Ok(());
            } else {
                // A virtual key with a model allow-list must be checked
                // before the request reaches the upstream, so its body is
                // buffered (instead of being streamed through unchanged) and
                // forwarded whole once the check has run. Keys without an
                // allow-list keep the zero-copy flow: the body is forwarded
                // immediately and only a copy is buffered for metadata.
                let enforce_allowlist = !ctx.virtual_key_allowed_models.is_empty();
                if enforce_allowlist {
                    self.buffer_request_body_chunk(ctx, body, end_of_stream);
                } else if let Some(ref data) = body {
                    ctx.request_body_buf.extend_from_slice(data);
                }
                if end_of_stream && !ctx.body_translated {
                    ctx.body_translated = true;
                    let hot = self.hot.load();
                    if let Ok(parsed) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    {
                        if let Some(model) = parsed.get("model").and_then(|m| m.as_str()) {
                            let resolved = Self::resolve_model_alias(&hot, model);
                            ctx.model = resolved.clone();
                            Self::enforce_model_allowlist(session, ctx, &resolved, Some(model))
                                .await?;
                            ctx.plugin_ctx.model = resolved;
                        }
                        if let Some(stream) = parsed.get("stream").and_then(|v| v.as_bool()) {
                            ctx.is_streaming = stream;
                            if stream {
                                // Set up streaming passthrough (OpenAI SSE → client)
                                let provider = hot.providers.get(&ctx.provider_name);
                                if let Some(p) = provider {
                                    match ctx.stream_translator.lock() {
                                        Ok(mut guard) => {
                                            if ctx.native_responses {
                                                // The upstream stream already
                                                // speaks the Responses
                                                // contract — copy it verbatim.
                                                *guard = Some(Box::new(
                                                    maxllm_translate::NativePassthroughStream,
                                                ));
                                            } else {
                                                *guard = Some(p.translator.streaming_translator());
                                            }
                                        }
                                        Err(e) => {
                                            warn!("stream_translator mutex poisoned: {e}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if !ctx.client_request_recorded {
                        ctx.client_request_recorded = true;
                        ctx.client_body = Some(ctx.request_body_buf.clone());
                        self.record_event(
                            EVENT_CLIENT_REQUEST,
                            ctx,
                            None,
                            None,
                            None,
                            ctx.client_body.clone(),
                        );
                    }
                    if enforce_allowlist {
                        *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
                    } else {
                        // Free the metadata buffer (not needed anymore)
                        ctx.request_body_buf = Vec::new();
                    }
                }
                return Ok(());
            }
        }

        // ── EMBEDDINGS: forward as-is for OpenAI-compatible, extract model ──
        if ctx.endpoint_type == EndpointType::Embeddings {
            self.buffer_request_body_chunk(ctx, body, end_of_stream);
            if end_of_stream && !ctx.body_translated {
                ctx.body_translated = true;
                let hot = self.hot.load();
                if let Ok(parsed) =
                    serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                {
                    if let Some(model) = parsed.get("model").and_then(|m| m.as_str()) {
                        let resolved = Self::resolve_model_alias(&hot, model);
                        ctx.model = resolved.clone();
                        Self::enforce_model_allowlist(session, ctx, &resolved, Some(model)).await?;
                        ctx.plugin_ctx.model = resolved;
                    }
                }
                *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
            }
            return Ok(());
        }

        // Buffer incoming body chunks (both normal and native modes)
        self.buffer_request_body_chunk(ctx, body, end_of_stream);

        // Auto route: rewrite the body's `model` field to the selected pool
        // entry's model. The client sent the sentinel (e.g. "auto"); the
        // upstream only understands its own model names. Done here, before
        // any translation, so responses→chat, guardrails, and the provider
        // translation all see the rewritten model.
        if end_of_stream && !ctx.body_translated {
            if let Some((_, auto_model)) = ctx.auto_target.clone() {
                if !ctx.request_body_buf.is_empty() {
                    if let Ok(mut v) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    {
                        // The auto route is not verbatim (the model field is
                        // rewritten, so the body travels chunked), which means
                        // the `keep_request_content_length` branch above never
                        // parses `stream`. Parse it here: without it
                        // `ctx.is_streaming` stays false, no stream translator
                        // is installed, and `extract_stream_usage` never runs
                        // — every auto-pool streaming request loses its usage.
                        if let Some(stream) = v.get("stream").and_then(|s| s.as_bool()) {
                            ctx.is_streaming = stream;
                        }
                        if let Some(obj) = v.as_object_mut() {
                            obj.insert(
                                "model".into(),
                                serde_json::Value::String(auto_model.clone()),
                            );
                        }
                        if let Ok(bytes) = serde_json::to_vec(&v) {
                            ctx.request_body_buf = bytes;
                        }
                        ctx.model = auto_model.clone();
                        Self::enforce_model_allowlist(session, ctx, &auto_model, None).await?;
                        ctx.plugin_ctx.model = auto_model.clone();
                    }
                }
            } else if let Some(override_model) = ctx.model_override.take() {
                // A retry after the previous attempt answered 400/422
                // "unknown model": this provider does not carry the name the
                // client sent (it belongs to another provider of the chain,
                // e.g. the pool primary's `deepseek-v4.1-flash:cloud`).
                // Rewrite the body to this provider's own default before the
                // translation so every later step (responses→chat, guardrails,
                // provider translator) sees the substituted name. The
                // substitution only happens after the upstream said it does
                // not know the original, so a provider that *would* have
                // served it is never second-guessed.
                if !ctx.request_body_buf.is_empty() {
                    if let Ok(mut v) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    {
                        // A native Responses body is forwarded as-is, so the
                        // model rewrite is the only pass that could read the
                        // streaming flag on the retry.
                        if let Some(stream) = v.get("stream").and_then(|s| s.as_bool()) {
                            ctx.is_streaming = stream;
                        }
                        if let Some(obj) = v.as_object_mut() {
                            obj.insert(
                                "model".into(),
                                serde_json::Value::String(override_model.clone()),
                            );
                        }
                        if let Ok(bytes) = serde_json::to_vec(&v) {
                            ctx.request_body_buf = bytes;
                        }
                        ctx.model = override_model.clone();
                        Self::enforce_model_allowlist(session, ctx, &override_model, None).await?;
                        ctx.plugin_ctx.model = override_model;
                    }
                }
            } else if !ctx.keep_request_content_length {
                // The client sent the sentinel in the body only (a plain chat
                // or responses route, no `auto_model` on the route and no
                // model in the path). The provider is already picked at this
                // point; if it is a pool member, serve it with that entry's
                // model instead of forwarding "auto" upstream.
                let hot = self.hot.load();
                self.adopt_sentinel_model(&hot, session, ctx).await?;
            }
        }

        if end_of_stream && !ctx.body_translated {
            ctx.body_translated = true;

            // ── RESPONSES: convert to chat completions before the normal ──
            // path runs, so model extraction, guardrails, and compaction
            // all see the canonical chat format they already understand.
            if ctx.endpoint_type == EndpointType::Responses && !ctx.native_responses {
                match maxllm_translate::responses::responses_request_to_chat(&ctx.request_body_buf)
                {
                    Ok(translated) => {
                        if translated.is_streaming {
                            ctx.is_streaming = true;
                        }
                        ctx.request_body_buf = translated.body;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            request_id = ?ctx.plugin_ctx.request_id,
                            provider = %ctx.provider_name,
                            endpoint_type = ?ctx.endpoint_type,
                            model = %ctx.model,
                            "Failed to translate Responses request to chat format"
                        );
                        Self::send_error_response(
                            session,
                            400,
                            &format!("invalid Responses request: {e}"),
                        )
                        .await?;
                        return Err(pingora::Error::explain(
                            pingora::ErrorType::HTTPStatus(400),
                            "invalid Responses request",
                        ));
                    }
                }
            }

            let hot = self.hot.load();
            let provider = match hot.providers.get(&ctx.provider_name) {
                Some(p) => p,
                None => {
                    warn!(provider = %ctx.provider_name, "provider not found in request_body_filter (config may have been reloaded)");
                    return Err(pingora::Error::explain(
                        pingora::ErrorType::HTTPStatus(502),
                        format!("provider '{}' no longer exists", ctx.provider_name),
                    ));
                }
            };

            if ctx.endpoint_type == EndpointType::Native {
                // ── NATIVE MODE: parse for metadata only, forward body as-is ──
                let parsed =
                    serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf).ok();

                if let Some(ref parsed) = parsed {
                    // Extract model from native format
                    let provider_kind = format!("{:?}", provider.kind).to_lowercase();
                    if let Some(model) =
                        maxllm_translate::extract_native_model(&provider_kind, parsed)
                    {
                        let resolved = Self::resolve_model_alias(&hot, &model);
                        ctx.model = resolved.clone();
                        Self::enforce_model_allowlist(session, ctx, &resolved, Some(&model))
                            .await?;
                        ctx.plugin_ctx.model = resolved;
                    }

                    // Extract streaming flag from native format
                    ctx.is_streaming =
                        maxllm_translate::extract_native_streaming(&provider_kind, parsed);
                }

                // Run pre-call guardrails on native content
                if let Some(ref engine) = hot.guardrail_engine {
                    if engine.has_pre_call() {
                        let provider_kind = format!("{:?}", provider.kind).to_lowercase();
                        let content = parsed
                            .as_ref()
                            .map(|p| maxllm_translate::extract_native_content(&provider_kind, p))
                            .unwrap_or_default();

                        if !content.is_empty() {
                            let input = GuardrailInput {
                                content: &content,
                                model: &ctx.model,
                                client_id: ctx.plugin_ctx.client_id.as_deref(),
                            };

                            let (verdict, applied) = engine
                                .run_pre_call(
                                    &input,
                                    ctx.requested_guardrails.as_deref(),
                                    ctx.route_guardrails.as_deref(),
                                )
                                .await;
                            ctx.applied_guardrails = applied;

                            if let GuardrailVerdict::Block {
                                ref guardrail,
                                ref reason,
                            } = verdict
                            {
                                return self
                                    .send_guardrail_block(session, ctx, guardrail, reason)
                                    .await;
                            }
                        }
                    }
                }

                // Set up native stream passthrough (passes chunks through unchanged)
                if ctx.is_streaming {
                    match ctx.stream_translator.lock() {
                        Ok(mut guard) => {
                            *guard = Some(Box::new(maxllm_translate::NativePassthroughStream));
                        }
                        Err(e) => {
                            warn!("stream_translator mutex poisoned: {e}");
                        }
                    }
                }

                // Forward body as-is (no translation)
                *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
            } else {
                // ── NORMAL MODE: OpenAI input → provider translation ──

                // Extract and resolve model name from request
                let parsed =
                    serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf).ok();

                if let Some(ref parsed) = parsed {
                    if let Some(model) = parsed.get("model").and_then(|m| m.as_str()) {
                        let resolved = Self::resolve_model_alias(&hot, model);
                        ctx.model = resolved.clone();
                        Self::enforce_model_allowlist(session, ctx, &resolved, Some(model)).await?;
                        ctx.plugin_ctx.model = resolved;
                    }

                    // A native Responses body is forwarded as-is, so no
                    // translation reports the streaming flag — read it here,
                    // or the stream translator is never installed and the
                    // usage embedded in the final `response.completed` event
                    // is never accounted for.
                    if ctx.native_responses {
                        if let Some(stream) = parsed.get("stream").and_then(|v| v.as_bool()) {
                            ctx.is_streaming = stream;
                        }
                    }

                    // Extract client-requested guardrails from request body
                    if let Some(guardrails) = parsed.get("guardrails").and_then(|g| g.as_array()) {
                        ctx.requested_guardrails = Some(
                            guardrails
                                .iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect(),
                        );
                    }
                }

                // Run pre-call guardrails on message content
                if let Some(ref engine) = hot.guardrail_engine {
                    if engine.has_pre_call() {
                        let content = parsed
                            .as_ref()
                            .map(guardrail::extract_message_content)
                            .unwrap_or_default();

                        if !content.is_empty() {
                            let input = GuardrailInput {
                                content: &content,
                                model: &ctx.model,
                                client_id: ctx.plugin_ctx.client_id.as_deref(),
                            };

                            let (verdict, applied) = engine
                                .run_pre_call(
                                    &input,
                                    ctx.requested_guardrails.as_deref(),
                                    ctx.route_guardrails.as_deref(),
                                )
                                .await;
                            ctx.applied_guardrails = applied;

                            match verdict {
                                GuardrailVerdict::Block {
                                    ref guardrail,
                                    ref reason,
                                } => {
                                    return self
                                        .send_guardrail_block(session, ctx, guardrail, reason)
                                        .await;
                                }
                                GuardrailVerdict::Modify {
                                    content: new_content,
                                    ..
                                } => {
                                    // Replace message content with redacted version
                                    if let Some(mut parsed) = parsed.clone() {
                                        if let Some(messages) = parsed
                                            .get_mut("messages")
                                            .and_then(|m| m.as_array_mut())
                                        {
                                            for msg in messages.iter_mut().rev() {
                                                if msg.get("role").and_then(|r| r.as_str())
                                                    == Some("user")
                                                {
                                                    msg["content"] = serde_json::Value::String(
                                                        new_content.clone(),
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                        if let Some(obj) = parsed.as_object_mut() {
                                            obj.remove("guardrails");
                                        }
                                        ctx.request_body_buf = serde_json::to_vec(&parsed)
                                            .unwrap_or_else(|_| {
                                                std::mem::take(&mut ctx.request_body_buf)
                                            });
                                    }
                                }
                                _ => {
                                    // Pass or Log — remove guardrails field from body
                                    if let Some(mut parsed) = parsed.clone() {
                                        if let Some(obj) = parsed.as_object_mut() {
                                            if obj.remove("guardrails").is_some() {
                                                ctx.request_body_buf = serde_json::to_vec(&parsed)
                                                    .unwrap_or_else(|_| {
                                                        std::mem::take(&mut ctx.request_body_buf)
                                                    });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // No guardrail engine — still strip guardrails field from body
                    if let Some(mut parsed) = parsed.clone() {
                        if let Some(obj) = parsed.as_object_mut() {
                            if obj.remove("guardrails").is_some() {
                                ctx.request_body_buf = serde_json::to_vec(&parsed)
                                    .unwrap_or_else(|_| std::mem::take(&mut ctx.request_body_buf));
                            }
                        }
                    }
                }

                // ── AUTO-COMPACTION ──
                //
                // A native Responses body is forwarded verbatim upstream, so
                // compaction (which rewrites chat `messages`) must not run.

                if !ctx.native_responses
                    && ctx
                        .plugin_ctx
                        .extensions
                        .get("auto_compact")
                        .map(|v| v.as_str())
                        == Some("1")
                {
                    if let Ok(mut body_json) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.request_body_buf)
                    {
                        let strategy = ctx
                            .plugin_ctx
                            .extensions
                            .get("compact_strategy")
                            .map(|s| s.as_str())
                            .unwrap_or("truncate")
                            .to_string();
                        let threshold: usize = ctx
                            .plugin_ctx
                            .extensions
                            .get("compact_threshold")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(6000);
                        let window_size: usize = ctx
                            .plugin_ctx
                            .extensions
                            .get("compact_window_size")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(20);
                        let preserve_system = ctx
                            .plugin_ctx
                            .extensions
                            .get("compact_preserve_system")
                            .map(|s| s.as_str())
                            != Some("0");
                        let min_messages: usize = ctx
                            .plugin_ctx
                            .extensions
                            .get("compact_min_messages")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(2);

                        let result = compact_messages(
                            &mut body_json,
                            CompactionParams {
                                strategy: &strategy,
                                threshold,
                                window_size,
                                preserve_system,
                                min_messages,
                                llm_provider: ctx
                                    .plugin_ctx
                                    .extensions
                                    .get("compact_llm_provider")
                                    .map(|s| s.as_str()),
                                llm_model: ctx
                                    .plugin_ctx
                                    .extensions
                                    .get("compact_llm_model")
                                    .map(|s| s.as_str()),
                            },
                        );
                        let modified = match result {
                            CompactionResult::NoOp => false,
                            CompactionResult::Compacted => true,
                            CompactionResult::NeedsLlm(llm_data) => {
                                // api_key and base_url must come from gateway provider config.
                                // If the summarize_provider is not registered in [providers],
                                // skip LLM compaction rather than silently reading env vars.
                                match hot.providers.get(&llm_data.provider) {
                                    Some(p) => {
                                        let scheme = if p.tls { "https" } else { "http" };
                                        let api_key = p.api_key.clone();
                                        let base_url = format!("{scheme}://{}:{}", p.host, p.port);
                                        apply_llm_compaction(
                                            &mut body_json,
                                            llm_data,
                                            &api_key,
                                            &base_url,
                                            &LLM_HTTP_CLIENT,
                                        )
                                        .await
                                    }
                                    None => {
                                        tracing::warn!(
                                            provider = %llm_data.provider,
                                            "auto-compaction: summarize_provider '{}' not found \
                                             in gateway config; add a [providers.{}] section \
                                             with api_key to enable LLM-based compaction",
                                            llm_data.provider,
                                            llm_data.provider
                                        );
                                        false
                                    }
                                }
                            }
                        };
                        if modified {
                            ctx.request_body_buf = serde_json::to_vec(&body_json)
                                .unwrap_or_else(|_| std::mem::take(&mut ctx.request_body_buf));

                            // For OpenAI passthrough with compaction, the Content-Length
                            // header was already sent with the original body size.
                            // Pad the compacted body with spaces to match, so the
                            // upstream reads exactly Content-Length bytes. JSON parsers
                            // ignore trailing whitespace.
                            if let Some(original_cl) = ctx
                                .plugin_ctx
                                .extensions
                                .get("original_content_length")
                                .and_then(|s| s.parse::<usize>().ok())
                            {
                                if ctx.request_body_buf.len() < original_cl {
                                    let padding = original_cl - ctx.request_body_buf.len();
                                    ctx.request_body_buf
                                        .extend(std::iter::repeat_n(b' ', padding));
                                }
                            }
                        }
                    }
                }

                // Translate request body (OpenAI → provider format).
                //
                // A native Responses body already speaks the upstream
                // contract, so it is forwarded unchanged — apart from the
                // repairs the provider needs (nested function tools -> flat,
                // the OpenDesign link's `tool_choice: "any"`).
                let translated = if ctx.native_responses {
                    Ok(TranslatedRequest {
                        body: Self::normalize_native_responses_body(
                            provider,
                            &ctx.request_body_buf,
                        ),
                        is_streaming: ctx.is_streaming,
                    })
                } else {
                    provider
                        .translator
                        .translate_request(&ctx.request_body_buf, None)
                };
                match translated {
                    Ok(translated) => {
                        if translated.is_streaming {
                            ctx.is_streaming = true;
                            match ctx.stream_translator.lock() {
                                Ok(mut guard) => {
                                    // A native Responses upstream already emits
                                    // the Responses SSE contract. Copying it
                                    // verbatim also lets `extract_stream_usage`
                                    // read the usage embedded in the final
                                    // `response.completed` event; wrapping it in
                                    // the provider's chat translator would
                                    // corrupt the event stream.
                                    if ctx.native_responses {
                                        *guard = Some(Box::new(
                                            maxllm_translate::NativePassthroughStream,
                                        ));
                                    } else {
                                        let inner = provider.translator.streaming_translator();
                                        if ctx.endpoint_type == EndpointType::Responses {
                                            *guard = Some(Box::new(
                                                maxllm_translate::responses::ResponsesStreamTranslator::new(inner),
                                            ));
                                        } else {
                                            *guard = Some(inner);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("stream_translator mutex poisoned: {e}");
                                }
                            }
                        }
                        *body = Some(Bytes::from(translated.body));
                    }
                    Err(e) => {
                        // Without this context the warning is unattributable:
                        // it names neither the request nor the provider whose
                        // translator rejected the body, which is what makes a
                        // single untranslated request cost a full walk of the
                        // failover chain to diagnose.
                        warn!(
                            error = %e,
                            request_id = ?ctx.plugin_ctx.request_id,
                            provider = %ctx.provider_name,
                            endpoint_type = ?ctx.endpoint_type,
                            model = %ctx.model,
                            route = ctx
                                .route_index
                                .and_then(|i| hot.routes.get(i))
                                .map(|r| r.path.as_str())
                                .unwrap_or("<none>"),
                            "Failed to translate request body"
                        );
                        *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
                    }
                }
            }
        }

        Ok(())
    }

    /// The upstream answered with a status line — before anything is written
    /// to the client.
    ///
    /// This is the request-level failover point: when the provider answered
    /// with an error another provider of the pool may not have (its quota is
    /// exhausted, it rate limits, it answers 5xx, it does not carry the
    /// model), the request walks the rest of the fallback chain instead of
    /// failing the client. The provider's answer is discarded here and the
    /// whole upstream attempt is retried on the next provider, so the failed
    /// attempt stays invisible to the client — the same behaviour litellm_lb_go
    /// and one-api expose on their channel pools.
    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        let status = upstream_response.status.as_u16();
        ctx.upstream_responded = true;

        // Feed the latency-based router: a successful attempt updates the
        // provider's latency estimate (time to the upstream's first byte).
        // Failures are not counted — a provider that fails fast must not look
        // like the fastest one; the circuit breaker is what punishes it.
        if status < 400 {
            if let Some(send_time) = ctx.upstream_send_time {
                let elapsed_ms = send_time.elapsed().as_secs_f64() * 1000.0;
                crate::provider_stats::record_latency(&ctx.provider_name, elapsed_ms);
            }
        }

        // Record circuit breaker state.
        //
        // A 429 (quota exhausted) force-opens the provider until its cooldown
        // ends: retrying a quota-exhausted channel is wasted work, and the
        // upstream usually says when it resets (Retry-After). A 402 is the
        // same kind of wall (the account has no balance) and gets its own
        // force-open — without it, an out-of-credit provider stays "healthy"
        // in the breaker and every rotation pays an extra upstream round trip
        // before the failover chain finds someone else. Any other server error
        // counts against `max_fails`. Successes reset the half-open probe and
        // re-pin the session to this provider.
        let retry_after = upstream_response
            .headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_retry_after_secs);
        let quota_open = status == 429;
        let payment_required = status == 402;
        let server_error = (500..=599).contains(&status);
        let success = (200..=299).contains(&status);

        // Layer 1 (headers) of the rate-limit resolver: the header may pin
        // the exact reset instant without reading the body at all. When it
        // does, the force-open deadline is set here and the failover proceeds
        // immediately (the body never arrives on the retried attempt).
        //
        // When it does not, and the learning resolver is enabled, the 429's
        // body is awaited in `upstream_response_body_filter` — that is where
        // the layered decision (learned patterns → body regex → laya) runs.
        // Without the resolver the pre-learning behaviour applies: a plain
        // Retry-After (or the default) cooldown, decided here.
        //
        // `quota_resolved` guards the generic accounting block below: once
        // the header layer pinned the reset instant it already force-opened
        // the breaker and counted a quota open, so that block must not run
        // again (it would double-count the failure and the quota-open).
        let mut quota_resolved_by_header = false;
        if quota_open && self.rate_limit_resolver.is_enabled() {
            if let Some((reset_at_ms, source)) =
                crate::rate_limit::resolve_from_headers(&upstream_response.headers)
            {
                quota_resolved_by_header = true;
                let hot = self.hot.load();
                if let Some(p) = hot.providers.get(&ctx.provider_name) {
                    p.circuit_breaker.record_quota_until(reset_at_ms);
                    METRICS
                        .circuit_breaker_quota_opens_total
                        .with_label_values(&[&ctx.provider_name])
                        .inc();
                    self.unpin_session(ctx);
                    METRICS
                        .circuit_breaker_state
                        .with_label_values(&[&ctx.provider_name])
                        .set(p.circuit_breaker.state() as i64);
                    self.persist_circuit_breaker(&ctx.provider_name, &p.circuit_breaker);
                    info!(
                        provider = %ctx.provider_name,
                        reset_at = %reset_at_ms,
                        source = %source,
                        "429 quota force-opened until the header-declared reset instant"
                    );
                }
                // `quota_open` stays true — the failover decision below still
                // walks the fallback chain.
            } else {
                // Headers are inconclusive: the body carries the phrasing
                // ("It will reset at …"). Wait for it instead of failing over
                // at header time — pingora has not written the header to the
                // client yet, so the deferred decision is invisible.
                ctx.pending_rate_limit = Some(status);
                ctx.rate_limit_body_buf.clear();
                // Keep the headers around: the resolver's header layer may
                // still pin the reset instant from them.
                ctx.rate_limit_headers = upstream_response
                    .headers
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_string(),
                            v.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect();
                return Ok(()); // let the body flow to the body filter
            }
        }

        if (quota_open || payment_required || server_error || success)
            && !quota_resolved_by_header
            && !ctx.probe_request
        {
            let hot = self.hot.load();
            if let Some(p) = hot.providers.get(&ctx.provider_name) {
                if payment_required {
                    // Account balance, not a transient fault: pin the open
                    // circuit to a deadline instead of counting misses, so the
                    // provider leaves the rotation at once and returns on its
                    // own after the cooldown (or the next config reload, if the
                    // balance is topped up earlier).
                    let until = crate::rate_limit::now_ms()
                        + crate::circuit_breaker::PAYMENT_REQUIRED_COOLDOWN_MS;
                    p.circuit_breaker.record_quota_until(until);
                    METRICS
                        .circuit_breaker_quota_opens_total
                        .with_label_values(&[&ctx.provider_name])
                        .inc();
                    // An out-of-credit provider must not keep the session pinned.
                    self.unpin_session(ctx);
                    info!(
                        provider = %ctx.provider_name,
                        cooldown_ms = crate::circuit_breaker::PAYMENT_REQUIRED_COOLDOWN_MS,
                        "402 payment required — force-opened until the balance is topped up"
                    );
                } else if quota_open {
                    p.circuit_breaker.record_http_failure(429, retry_after);
                    METRICS
                        .circuit_breaker_quota_opens_total
                        .with_label_values(&[&ctx.provider_name])
                        .inc();
                    // A quota-exhausted provider must not keep the session pinned.
                    self.unpin_session(ctx);
                } else if server_error {
                    p.circuit_breaker.record_failure();
                    self.unpin_session(ctx);
                } else {
                    p.circuit_breaker.record_success();
                    // Pin the session to the provider that just served it.
                    self.pin_session(ctx);
                }
                METRICS
                    .circuit_breaker_state
                    .with_label_values(&[&ctx.provider_name])
                    .set(p.circuit_breaker.state() as i64);
                METRICS
                    .circuit_breaker_transitions_total
                    .with_label_values(&[&ctx.provider_name])
                    .set(p.circuit_breaker.state_transitions() as i64);
                // Persist the breaker state so a restart keeps an open circuit
                // and a 429 quota force-open.
                self.persist_circuit_breaker(&ctx.provider_name, &p.circuit_breaker);
            }
        }

        // A provider whose circuit is open must not keep the session pinned;
        // done above. Now decide whether this error belongs to the provider
        // (fail over) or to the request (return it to the client).
        let hot = self.hot.load();
        let Some(provider) = hot.providers.get(&ctx.provider_name) else {
            return Ok(());
        };
        // A 404 whose body may name the requested model as unknown: defer the
        // decision until the body arrives (a missing endpoint answers 404
        // too). Deferring is only worth it when a rewrite is actually
        // possible; otherwise the ordinary failover below proceeds at once.
        if status == 404 && self.model_rewrite_target(&hot, ctx, provider).is_some() {
            ctx.pending_model_404 = true;
            ctx.model_404_body_buf.clear();
            return Ok(());
        }
        if !should_failover_status(status, ctx, provider) {
            return Ok(());
        }
        // A body bigger than the replay buffer cannot be sent again: refuse the
        // retry rather than deadlock both sides (see `body_is_replayable`).
        if !body_is_replayable(session, ctx) {
            info!(
                provider = %ctx.provider_name,
                status = %status,
                "request body exceeds the 64 KiB replay buffer — returning the provider's error instead of a truncated retry"
            );
            ctx.attempt_trail
                .push(format!("{}=toobig", ctx.provider_name));
            self.record_event(EVENT_PROVIDER_RESPONSE, ctx, Some(status), None, None, None);
            return Ok(());
        }
        // A provider that does not carry the requested model answers 400/422
        // (one-api's `invalid params, unknown model`) — or 404 naming the
        // model as unknown (vLLM's "The model `X` does not exist. Valid
        // aliases: …"). When the name belongs to *another* provider of the
        // route — the pooled `/v1/responses` primary serves
        // `deepseek-v4.1-flash:cloud`, which no cloud fallback knows — a plain
        // failover collects the same rejection from every entry and the route
        // dies together with its primary. Rewrite the body's model to THIS
        // provider's own default and retry it once before spending a provider
        // of the chain. A second rejection after the rewrite is a genuine
        // request fault and is returned to the client (at most one rewrite
        // per provider, so the walk can never ping-pong).
        //
        // Only the plain fallback chain is rewritten: an auto pool already
        // carries a per-entry model, which is authoritative for that entry.
        //
        // A 400/422 is unambiguous, so the decision is taken here. A 404 is
        // not: a missing endpoint answers the same status, and only the body
        // can tell the two apart — that decision waits for the body filter
        // (see `pending_model_404`).
        if matches!(status, 400 | 422) {
            if let Some(rewrite_to) = self.model_rewrite_target(&hot, ctx, provider) {
                return Err(self.begin_model_rewrite(ctx, status, rewrite_to));
            }
        }
        // A 5xx is a transient provider failure: when the route allows it
        // (`num_retries`), retry the SAME provider — spaced by
        // `retry_backoff_ms` — before burning a provider of the fallback
        // chain. Deterministic rejections (429 quota, 401/403 auth, 404
        // missing model) are never retried on the same provider: the
        // upstream would answer the same way again. A connect failure fails
        // over too (`fail_to_connect`) — the provider is unreachable, not
        // flaky.
        // Pure read: the outcome was just booked; deciding whether to retry
        // the same provider must not reserve (and strand) a probe slot.
        if (500..=599).contains(&status) && provider.circuit_breaker.allows_traffic() {
            let (num_retries, backoff_ms) = same_provider_retries(&hot.routes, ctx.route_index);
            if ctx.provider_retry_count < num_retries {
                ctx.provider_retry_count += 1;
                ctx.pending_retry_backoff_ms = Some(backoff_ms);
                METRICS
                    .provider_retries_total
                    .with_label_values(&[&ctx.provider_name, &status.to_string()])
                    .inc();
                info!(
                    status = %status,
                    provider = %ctx.provider_name,
                    retry = ctx.provider_retry_count,
                    backoff_ms = %backoff_ms,
                    "upstream answered with a transient error — retrying the same provider"
                );
                let mut e = pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(status),
                    format!(
                        "provider '{}' answered {status}; retrying the same provider (retry {})",
                        ctx.provider_name, ctx.provider_retry_count
                    ),
                );
                e.as_up();
                e.set_retry(true);
                return Err(e);
            }
        }
        if ctx.failover_count >= max_failover_attempts(&hot.routes, ctx.route_index) {
            info!(
                status = %status,
                provider = %ctx.provider_name,
                attempts = ctx.failover_count,
                "upstream answered with an error and the failover budget is spent — returning the error to the client"
            );
            return Ok(());
        }
        // Audit trail: the failed provider answered (the final provider's
        // answer is recorded by `response_filter` on the surviving attempt).
        // Recorded before the switch so the provider label matches the status.
        self.record_event(EVENT_PROVIDER_RESPONSE, ctx, Some(status), None, None, None);
        ctx.attempt_trail
            .push(format!("{}={}", ctx.provider_name, status));
        let Some((failed_provider, next_provider)) = self.switch_to_fallback(ctx) else {
            return Ok(());
        };

        METRICS
            .http_failovers_total
            .with_label_values(&[&failed_provider, &next_provider, &status.to_string()])
            .inc();
        info!(
            status = %status,
            provider = %next_provider,
            failed_provider = %failed_provider,
            "upstream answered with an error — retrying the request on the fallback provider"
        );

        // The request body is still buffered in pingora's retry buffer (the
        // client has not received anything yet), so the whole upstream
        // attempt — including the request body translation — is replayed on
        // the new provider.
        let mut e = pingora::Error::explain(
            pingora::ErrorType::HTTPStatus(status),
            format!(
                "provider '{failed_provider}' answered {status}; retrying on '{next_provider}'"
            ),
        );
        e.as_up();
        e.set_retry(true);
        Err(e)
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        let hot = self.hot.load();
        let status = upstream_response.status.as_u16();
        ctx.upstream_status = status;
        let provider = hot.providers.get(&ctx.provider_name);

        // Circuit breaker bookkeeping happens in `upstream_response_filter`,
        // before the failover decision — the provider's failure must be
        // recorded even when the request walks on to the next provider.

        // Audit trail: the upstream answered.
        self.record_event(EVENT_PROVIDER_RESPONSE, ctx, Some(status), None, None, None);

        // Only remove Content-Length when the body will be translated (changes size).
        // Passthrough, native, and Embeddings keep the original Content-Length.
        // OpenAI providers skip chat-format translation, but Responses mode
        // always converts the body, so its Content-Length is removed too.
        let skip_response_translation = ctx.endpoint_type == EndpointType::Passthrough
            || ctx.endpoint_type == EndpointType::Native
            || ctx.endpoint_type == EndpointType::Embeddings
            || ctx.native_responses
            || (provider.is_some_and(|p| p.translator.name() == "openai")
                && ctx.endpoint_type != EndpointType::Responses);
        if !skip_response_translation {
            let _ = upstream_response.remove_header("Content-Length");
        }

        // Error responses (4xx/5xx) are always passed through normalize_error_response()
        // which rewrites the body into OpenAI error format, changing the body size.
        // Remove Content-Length unconditionally so we never mismatch the translated body.
        if status >= 400 {
            let _ = upstream_response.remove_header("Content-Length");
        }

        // For Gemini streaming: we got a JSON response from generateContent
        // but client wants SSE. Set the right content type.
        let is_gemini_streaming =
            ctx.is_streaming && provider.is_some_and(|p| p.kind == ProviderKind::Gemini);
        if is_gemini_streaming {
            upstream_response.insert_header("Content-Type", "text/event-stream")?;
            let _ = upstream_response.remove_header("Content-Length");
        }

        // Responses streaming always emits SSE — except error responses,
        // whose body is the normalized OpenAI error JSON (not SSE) and must
        // not be labelled as a stream.
        if ctx.endpoint_type == EndpointType::Responses
            && ctx.is_streaming
            && !is_gemini_streaming
            && status < 400
        {
            upstream_response.insert_header("Content-Type", "text/event-stream")?;
            let _ = upstream_response.remove_header("Content-Length");
        }

        // A non-streaming Responses client receives exactly one JSON object
        // (the body filter folds an SSE answer back into it), so the content
        // type must say JSON even when the upstream answered with a stream.
        // The gate mirrors the fold: only once the request body has been read
        // (`body_translated`) does `is_streaming` reflect the client's true
        // intent — in the early-answer race the stream is kept, not folded.
        if ctx.endpoint_type == EndpointType::Responses
            && !ctx.is_streaming
            && ctx.body_translated
            && status < 400
        {
            upstream_response.insert_header("Content-Type", "application/json")?;
        }

        // Add gateway headers
        upstream_response.insert_header("X-MaxLLM-Provider", &ctx.provider_name)?;
        if ctx.fallback_used {
            if let Some(original) = &ctx.original_provider {
                upstream_response.insert_header("X-MaxLLM-Fallback-From", original)?;
            }
        }

        // Add timing headers: upstream latency and gateway overhead
        if let Some(send_time) = ctx.upstream_send_time {
            let now = std::time::Instant::now();
            let upstream_ms = now.duration_since(send_time).as_millis();
            let total_ms = now.duration_since(ctx.request_start).as_millis();
            let overhead_ms = total_ms.saturating_sub(upstream_ms);

            upstream_response.insert_header("X-MaxLLM-Upstream-Ms", upstream_ms.to_string())?;
            upstream_response.insert_header("X-MaxLLM-Overhead-Ms", overhead_ms.to_string())?;
        }

        // Add applied guardrails header
        if !ctx.applied_guardrails.is_empty() {
            upstream_response.insert_header(
                "X-MaxLLM-Applied-Guardrails",
                ctx.applied_guardrails.join(", "),
            )?;
        }

        // Run plugin chains on response
        hot.global_chain
            .run_response(session, upstream_response, &mut ctx.plugin_ctx)
            .await?;
        if let Some(idx) = ctx.route_index {
            hot.route_chains[idx]
                .run_response(session, upstream_response, &mut ctx.plugin_ctx)
                .await?;
        }

        Ok(())
    }

    fn upstream_response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Option<std::time::Duration>> {
        let hot = self.hot.load();

        // The client is about to receive real bytes; a later upstream failure
        // can no longer be hidden behind a failover.
        if body.as_ref().is_some_and(|b| !b.is_empty()) {
            ctx.client_body_written = true;
        }

        // ── Pending model-lookup decision (404 that may name the model as
        // unknown) ───────────────────────────────────────────────────────
        //
        // vLLM-style upstreams reject a model they do not carry with 404
        // "The model `X` does not exist", indistinguishable from a missing
        // endpoint until the body arrives. When the body blames the model,
        // retry the SAME provider once with its own default model
        // substituted; any other 404 takes the ordinary failover path.
        if ctx.pending_model_404 {
            if let Some(data) = body.as_ref() {
                if ctx.model_404_body_buf.len() < 64 * 1024 {
                    ctx.model_404_body_buf.extend_from_slice(data);
                }
            }
            if !end_of_stream {
                return Ok(None);
            }
            ctx.pending_model_404 = false;
            let body_text =
                String::from_utf8_lossy(&std::mem::take(&mut ctx.model_404_body_buf)).to_string();
            let model_is_unknown = body_reports_unknown_model(&body_text);

            let hot = self.hot.load();
            let rewrite_to = hot
                .providers
                .get(&ctx.provider_name)
                .filter(|_| model_is_unknown)
                .and_then(|provider| self.model_rewrite_target(&hot, ctx, provider));
            if let Some(rewrite_to) = rewrite_to {
                return Err(self.begin_model_rewrite(ctx, 404, rewrite_to));
            }
            return self.fail_over_from_body_filter(
                session,
                ctx,
                404,
                "404 does not name the requested model",
            );
        }

        // ── Pending rate-limit decision (429 whose headers did not pin the
        // reset instant) ──────────────────────────────────────────────────
        //
        // The 429 body is collected here; when it is complete the layered
        // resolver runs (learned patterns → body regex → laya). The decision
        // then replaces the blind cooldown: a precise reset instant
        // force-opens the provider until that moment, and a *non*-rate-limit
        // classification takes the ordinary-failure path. Either way the
        // request walks on to the next provider of the fallback chain.
        //
        // pingora buffers the whole response (header + body) before writing
        // anything to the client, so deciding here and returning a retry
        // error is invisible to the client — the failed attempt is replaced
        // whole by the next provider's attempt.
        if let Some(status) = ctx.pending_rate_limit {
            let _ = status; // status is 429 for the deferred path; kept for logging below
            if let Some(data) = body.as_ref() {
                if ctx.rate_limit_body_buf.len() < 64 * 1024 {
                    ctx.rate_limit_body_buf.extend_from_slice(data);
                }
            }
            if !end_of_stream {
                return Ok(None);
            }

            // Full error body collected → resolve the quota reset instant.
            let status = ctx.pending_rate_limit.take().unwrap();
            let body_bytes = std::mem::take(&mut ctx.rate_limit_body_buf);

            let headers: http::HeaderMap = {
                let mut m = http::HeaderMap::new();
                for (k, v) in std::mem::take(&mut ctx.rate_limit_headers) {
                    if let (Ok(name), Ok(val)) = (
                        http::HeaderName::from_bytes(k.as_bytes()),
                        http::HeaderValue::from_str(&v),
                    ) {
                        m.append(name, val);
                    }
                }
                m
            };

            let resolver = self.rate_limit_resolver.clone();
            let provider_name = ctx.provider_name.clone();
            let utc_offset_minutes = hot
                .providers
                .get(&provider_name)
                .map(|p| p.utc_offset_minutes)
                .unwrap_or(0);
            // The Pingora body filter is sync; the resolver is async (it may
            // call laya over HTTP). Bridge the boundary with block_on on the
            // current tokio runtime — the call is bounded by the laya
            // timeout, and this is the same thread that will drive the
            // retry, so the failover decision is still on-path.
            let (decision, learned) = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current().map(|rt| {
                    rt.block_on(async {
                        resolver
                            .resolve(status, &headers, &body_bytes, utc_offset_minutes)
                            .await
                    })
                })
            })
            .unwrap_or_else(|_| {
                // No runtime context (defensive): deterministic-only decision.
                resolver.resolve_sync(status, &headers, &body_bytes, utc_offset_minutes)
            });

            // Persist what we learned (fire-and-forget; the DB write is
            // small and off the request path).
            if let Some(update) = learned {
                if update.fingerprint.len() <= 512 {
                    let resolver = resolver.clone();
                    tokio::spawn(async move {
                        resolver.persist_learned(update).await;
                    });
                }
            }

            // Apply the decision to the circuit breaker, then fail over.
            // A health probe must not move a production breaker.
            let hot = self.hot.load();
            if let Some(p) = hot
                .providers
                .get(&provider_name)
                .filter(|_| !ctx.probe_request)
            {
                match &decision {
                    crate::rate_limit::RateLimitDecision::QuotaUntil {
                        reset_at_ms,
                        source,
                    } => {
                        p.circuit_breaker.record_quota_until(*reset_at_ms);
                        METRICS
                            .circuit_breaker_quota_opens_total
                            .with_label_values(&[&provider_name])
                            .inc();
                        info!(
                            provider = %provider_name,
                            reset_at_ms = reset_at_ms,
                            source = %source,
                            "429 quota force-opened until the resolved reset instant"
                        );
                    }
                    crate::rate_limit::RateLimitDecision::QuotaUnknown => {
                        // Rate limit without a resolvable instant: default cooldown.
                        let cooldown = self.rate_limit_resolver.default_cooldown_secs();
                        p.circuit_breaker
                            .record_quota_until(crate::rate_limit::now_ms() + cooldown * 1000);
                        METRICS
                            .circuit_breaker_quota_opens_total
                            .with_label_values(&[&provider_name])
                            .inc();
                        info!(
                            provider = %provider_name,
                            cooldown_secs = cooldown,
                            "429 quota force-opened with the default cooldown (reset instant unresolved)"
                        );
                    }
                    crate::rate_limit::RateLimitDecision::OrdinaryFailure => {
                        // Not a rate limit — ordinary failure accounting.
                        p.circuit_breaker.record_failure();
                        info!(
                            provider = %provider_name,
                            "429 classified as an ordinary failure (not a rate limit)"
                        );
                    }
                }
                METRICS
                    .circuit_breaker_state
                    .with_label_values(&[&provider_name])
                    .set(p.circuit_breaker.state() as i64);
                self.persist_circuit_breaker(&provider_name, &p.circuit_breaker);
            }

            // A quota-exhausted provider must not keep the session pinned,
            // and the request walks on to the next provider of the chain.
            return self.fail_over_from_body_filter(
                session,
                ctx,
                status,
                "429 resolved via the layered rate-limit resolver",
            );
        }

        // Time-to-first-token: the first response chunk marks the moment the
        // upstream started answering.
        if ctx.ttft_ms.is_none() && !ctx.skip_logging {
            ctx.ttft_ms = Some(ctx.request_start.elapsed().as_millis() as u64);
        }

        // Capture the response body for the recorder. A non-streaming answer
        // is buffered in `response_body_buf` (plus the chunk still in `body`),
        // so the copy is the full upstream body; a streaming answer is not
        // buffered, so only its first chunk is kept — a full stream would
        // balloon the log.
        if ctx.upstream_body.is_none() {
            if ctx.is_streaming {
                if let Some(data) = body.as_ref() {
                    ctx.upstream_body = Some(data.to_vec());
                }
            } else if end_of_stream {
                let mut buf = ctx.response_body_buf.clone();
                if let Some(data) = body.as_ref() {
                    buf.extend_from_slice(data);
                }
                if !buf.is_empty() {
                    ctx.upstream_body = Some(buf);
                }
            }
        }

        // ── PASSTHROUGH and EMBEDDINGS: forward as-is, extract usage ──
        if ctx.endpoint_type == EndpointType::Passthrough
            || ctx.endpoint_type == EndpointType::Embeddings
        {
            // Extract usage from embeddings responses
            if ctx.endpoint_type == EndpointType::Embeddings && end_of_stream {
                if let Some(data) = body.as_ref() {
                    if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(data) {
                        extract_usage(&parsed, ctx);
                    }
                }
            }
            // Still run plugin chains (logging, webhooks, etc.)
            hot.global_chain.run_response_body(
                session,
                body,
                end_of_stream,
                &mut ctx.plugin_ctx,
            )?;
            if let Some(idx) = ctx.route_index {
                hot.route_chains[idx].run_response_body(
                    session,
                    body,
                    end_of_stream,
                    &mut ctx.plugin_ctx,
                )?;
            }
            return Ok(None);
        }

        // ── NATIVE MODE: forward as-is, extract usage from native format ──
        if ctx.endpoint_type == EndpointType::Native {
            if ctx.is_streaming {
                // Streaming: pass through (NativePassthroughStream just copies bytes)
                if let Some(data) = body.as_ref() {
                    match ctx.stream_translator.lock() {
                        Ok(mut guard) => {
                            if let Some(translator) = guard.as_mut() {
                                let translated = translator.process_chunk(data, end_of_stream);
                                drop(guard);
                                *body = Some(Bytes::from(translated));
                            }
                        }
                        Err(e) => {
                            warn!("stream_translator mutex poisoned in native streaming: {e}");
                        }
                    }
                }
            } else {
                // Non-streaming: buffer, extract usage, forward as-is
                if let Some(data) = body.take() {
                    ctx.response_body_buf.extend_from_slice(&data);
                }
                if end_of_stream && !ctx.response_body_buf.is_empty() {
                    // Extract usage from native response format
                    if let Ok(parsed) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf)
                    {
                        extract_usage(&parsed, ctx);

                        // Run post-call guardrails on native response content
                        if let Some(ref engine) = hot.guardrail_engine {
                            if engine.has_post_call() {
                                let provider = hot.providers.get(&ctx.provider_name);
                                let provider_kind = provider
                                    .map(|p| format!("{:?}", p.kind).to_lowercase())
                                    .unwrap_or_default();
                                let response_content =
                                    maxllm_translate::extract_native_response_content(
                                        &provider_kind,
                                        &parsed,
                                    );
                                if !response_content.is_empty() {
                                    let output = GuardrailOutput {
                                        content: &response_content,
                                        model: &ctx.model,
                                    };
                                    let (verdict, post_applied) =
                                        tokio::task::block_in_place(|| {
                                            tokio::runtime::Handle::current().block_on(
                                                engine.run_post_call(
                                                    &output,
                                                    ctx.requested_guardrails.as_deref(),
                                                    ctx.route_guardrails.as_deref(),
                                                ),
                                            )
                                        });
                                    ctx.applied_guardrails.extend(post_applied);

                                    if let GuardrailVerdict::Block { guardrail, reason } = verdict {
                                        let error_body = serde_json::json!({
                                            "error": {
                                                "message": reason,
                                                "type": "guardrail_violation",
                                                "guardrail": guardrail,
                                                "code": 400
                                            }
                                        });
                                        *body =
                                            Some(Bytes::from(error_body.to_string().into_bytes()));
                                        // Skip forwarding the original body
                                        hot.global_chain.run_response_body(
                                            session,
                                            body,
                                            end_of_stream,
                                            &mut ctx.plugin_ctx,
                                        )?;
                                        if let Some(idx) = ctx.route_index {
                                            hot.route_chains[idx].run_response_body(
                                                session,
                                                body,
                                                end_of_stream,
                                                &mut ctx.plugin_ctx,
                                            )?;
                                        }
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                    }

                    // Forward native response body as-is
                    *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                }
            }

            // Run plugin chains
            hot.global_chain.run_response_body(
                session,
                body,
                end_of_stream,
                &mut ctx.plugin_ctx,
            )?;
            if let Some(idx) = ctx.route_index {
                hot.route_chains[idx].run_response_body(
                    session,
                    body,
                    end_of_stream,
                    &mut ctx.plugin_ctx,
                )?;
            }
            return Ok(None);
        }

        // ── RESPONSES: translate provider chat → Responses format ─────────
        if ctx.endpoint_type == EndpointType::Responses {
            let provider = hot.providers.get(&ctx.provider_name);
            let is_gemini = provider.is_some_and(|p| p.kind == ProviderKind::Gemini);

            if ctx.native_responses {
                // ── NATIVE RESPONSES: forward the upstream body verbatim ──
                // The provider already speaks the Responses contract, so the
                // body (streaming or not) is passed through unchanged. Only
                // errors are normalised into the OpenAI error shape, and usage
                // is still extracted for the metrics/ledger.
                // An error body is not SSE and carries no terminal event to
                // wait for. Streaming it through the (verbatim) stream
                // translator used to leave it to the truncation judge: the
                // upstream 400 was booked as a "stream truncated" in the
                // request log (masking the real error) and a
                // gateway_stream_truncated event was appended to the JSON the
                // client was reading. A 4xx/5xx takes the buffered path
                // below, which normalizes the error instead.
                if ctx.is_streaming && ctx.upstream_status < 400 {
                    if let Some(data) = body.as_ref() {
                        let translated: Option<Bytes> = match ctx.stream_translator.lock() {
                            Ok(mut guard) => guard.as_mut().map(|translator| {
                                Bytes::from(translator.process_chunk(data, end_of_stream))
                            }),
                            Err(e) => {
                                warn!(
                                    "stream_translator mutex poisoned in native responses streaming: {e}"
                                );
                                None
                            }
                        };
                        if let Some(ref t) = translated {
                            let mut translated = t.clone();
                            observe_stream_chunk(&mut translated, ctx, end_of_stream);
                            *body = Some(translated);
                        }
                    }
                } else {
                    if let Some(data) = body.take() {
                        ctx.response_body_buf.extend_from_slice(&data);
                    }
                    if end_of_stream && !ctx.response_body_buf.is_empty() {
                        let parsed =
                            serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf);
                        if let Ok(ref parsed) = parsed {
                            extract_usage(parsed, ctx);
                        }

                        // A non-streaming client expects one Responses object.
                        // A native upstream that answered with Responses SSE
                        // anyway (ignoring `stream: false`) is folded into
                        // that single object by reading the terminal event.
                        // The gate mirrors the translated path: only once the
                        // request body has been read (`body_translated`) does
                        // `is_streaming` reflect the client's true intent —
                        // in the early-answer race the stream is kept.
                        if parsed.is_err()
                            && ctx.body_translated
                            && looks_like_responses_sse(&ctx.response_body_buf)
                        {
                            match maxllm_translate::responses::responses_sse_to_response(
                                &ctx.response_body_buf,
                            ) {
                                Some(json) => {
                                    if let Ok(parsed) =
                                        serde_json::from_slice::<serde_json::Value>(&json)
                                    {
                                        extract_usage(&parsed, ctx);
                                    }
                                    *body = Some(Bytes::from(json));
                                }
                                None => {
                                    warn!(
                                        "Failed to fold native Responses SSE into a single object"
                                    );
                                    *body = Some(Bytes::from(early_sse_failure_body()));
                                }
                            }
                        } else if ctx.upstream_status >= 400 {
                            let error_body = provider
                                .and_then(|p| {
                                    normalize_error_response(
                                        &ctx.response_body_buf,
                                        ctx.upstream_status,
                                        p.kind,
                                    )
                                })
                                .unwrap_or_else(|| ctx.response_body_buf.clone());
                            *body = Some(Bytes::from(error_body));
                        } else {
                            *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                        }
                    }
                }

                hot.global_chain.run_response_body(
                    session,
                    body,
                    end_of_stream,
                    &mut ctx.plugin_ctx,
                )?;
                if let Some(idx) = ctx.route_index {
                    hot.route_chains[idx].run_response_body(
                        session,
                        body,
                        end_of_stream,
                        &mut ctx.plugin_ctx,
                    )?;
                }
                return Ok(None);
            }

            if ctx.is_streaming && is_gemini {
                // Gemini answered generateContent with a single JSON body;
                // convert it to Responses SSE events.
                if let Some(data) = body.take() {
                    ctx.response_body_buf.extend_from_slice(&data);
                }
                if end_of_stream && !ctx.response_body_buf.is_empty() {
                    if let Some(p) = provider {
                        match p.translator.translate_response(&ctx.response_body_buf) {
                            Ok(chat_bytes) => {
                                match maxllm_translate::responses::chat_response_to_responses_sse(
                                    &chat_bytes,
                                ) {
                                    Some(sse) => {
                                        *body = Some(Bytes::from(sse));
                                    }
                                    None => {
                                        warn!("Failed to convert Gemini chat response to Responses SSE");
                                        *body = Some(Bytes::from(std::mem::take(
                                            &mut ctx.response_body_buf,
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to translate Gemini response for Responses mode");
                                *body =
                                    Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                            }
                        }
                    }
                }
            } else if ctx.is_streaming {
                // An upstream error during a streaming Responses request must
                // not be fed into the SSE translator: the error body is not
                // SSE, so the translator would emit an empty "completed"
                // response and the client would never see the real reason
                // (it just gets a 4xx with no explanation). Deliver the
                // normalized OpenAI error body instead.
                if ctx.upstream_status >= 400 {
                    if let Some(data) = body.take() {
                        ctx.response_body_buf.extend_from_slice(&data);
                    }
                    if end_of_stream && !ctx.response_body_buf.is_empty() {
                        let error_body = provider
                            .and_then(|p| {
                                normalize_error_response(
                                    &ctx.response_body_buf,
                                    ctx.upstream_status,
                                    p.kind,
                                )
                            })
                            .unwrap_or_else(|| ctx.response_body_buf.clone());
                        *body = Some(Bytes::from(error_body));
                        hot.global_chain.run_response_body(
                            session,
                            body,
                            end_of_stream,
                            &mut ctx.plugin_ctx,
                        )?;
                        if let Some(idx) = ctx.route_index {
                            hot.route_chains[idx].run_response_body(
                                session,
                                body,
                                end_of_stream,
                                &mut ctx.plugin_ctx,
                            )?;
                        }
                        return Ok(None);
                    }
                } else if let Some(data) = body.as_ref() {
                    let translated: Option<Bytes> = match ctx.stream_translator.lock() {
                        Ok(mut guard) => {
                            // Defensive: the request phase installs the stream
                            // translator, but it can be missing (the request
                            // translation failed, or the upstream answered
                            // before the request body was fully read). Raw
                            // provider SSE must never reach a Responses
                            // client, so install a translator on the fly.
                            if guard.is_none() && looks_like_chat_sse(data) {
                                if let Some(p) = provider {
                                    *guard = Some(Box::new(
                                        maxllm_translate::responses::ResponsesStreamTranslator::new(
                                            p.translator.streaming_translator(),
                                        ),
                                    ));
                                }
                            }
                            guard.as_mut().map(|translator| {
                                Bytes::from(translator.process_chunk(data, end_of_stream))
                            })
                        }
                        Err(e) => {
                            warn!("stream_translator mutex poisoned in responses streaming: {e}");
                            None
                        }
                    };
                    if let Some(ref t) = translated {
                        let mut translated = t.clone();
                        observe_stream_chunk(&mut translated, ctx, end_of_stream);
                        *body = Some(translated);
                    }
                }
            } else {
                // Non-streaming: buffer, extract usage, convert chat → responses.
                if let Some(data) = body.take() {
                    ctx.response_body_buf.extend_from_slice(&data);
                }

                if end_of_stream && !ctx.response_body_buf.is_empty() {
                    let parsed =
                        serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf);
                    if let Ok(ref parsed) = parsed {
                        extract_usage(parsed, ctx);
                    }

                    // Error responses are delivered as-is (normalized to the
                    // OpenAI error shape when the provider uses another one).
                    // They are never converted into a Responses object — a
                    // failed upstream call must not look like a completed
                    // response with empty output.
                    if ctx.upstream_status >= 400 {
                        let error_body = provider
                            .and_then(|p| {
                                normalize_error_response(
                                    &ctx.response_body_buf,
                                    ctx.upstream_status,
                                    p.kind,
                                )
                            })
                            .unwrap_or_else(|| ctx.response_body_buf.clone());
                        *body = Some(Bytes::from(error_body));
                        hot.global_chain.run_response_body(
                            session,
                            body,
                            end_of_stream,
                            &mut ctx.plugin_ctx,
                        )?;
                        if let Some(idx) = ctx.route_index {
                            hot.route_chains[idx].run_response_body(
                                session,
                                body,
                                end_of_stream,
                                &mut ctx.plugin_ctx,
                            )?;
                        }
                        return Ok(None);
                    }

                    // The upstream answered with a chat SSE stream although
                    // the client did not ask for a stream. Two shapes lead
                    // here:
                    //
                    // * the upstream ignored `stream: false` — the client
                    //   expects one JSON object and must not be handed an
                    //   SSE stream;
                    // * the upstream answered before the request body had
                    //   been fully read (the request headers are forwarded
                    //   upstream right away), so the gateway does not know
                    //   yet whether the client asked for a stream and has no
                    //   stream translator installed. Without this branch the
                    //   provider bytes would reach the Responses client
                    //   verbatim — the client then rejects the payload
                    //   ("Invalid ... stream event"), because a chat chunk
                    //   carries no Responses `type`.
                    //
                    // The chat stream is translated into Responses events in
                    // both cases. Once the request body has been read
                    // (`body_translated`) the client's intent is known: a
                    // non-streaming client gets the events folded back into
                    // the single response object it asked for, while the
                    // early-answer race keeps the stream.
                    if parsed.is_err() && looks_like_chat_sse(&ctx.response_body_buf) {
                        match provider {
                            Some(p) => {
                                let mut translator =
                                    maxllm_translate::responses::ResponsesStreamTranslator::new(
                                        p.translator.streaming_translator(),
                                    );
                                let translated =
                                    translator.process_chunk(&ctx.response_body_buf, true);
                                if translated.is_empty() {
                                    warn!("Failed to convert upstream chat SSE to Responses SSE");
                                    *body = Some(Bytes::from(early_sse_failure_body()));
                                } else {
                                    extract_stream_usage(&translated, ctx, true);
                                    if ctx.body_translated && !ctx.is_streaming {
                                        // A non-streaming client wants one
                                        // JSON object, not a stream.
                                        match maxllm_translate::responses::responses_sse_to_response(
                                            &translated,
                                        ) {
                                            Some(response) => {
                                                *body = Some(Bytes::from(response));
                                            }
                                            None => {
                                                warn!("Failed to fold upstream chat SSE into a single Responses object");
                                                *body = Some(Bytes::from(early_sse_failure_body()));
                                            }
                                        }
                                    } else {
                                        *body = Some(Bytes::from(translated));
                                    }
                                }
                            }
                            None => {
                                warn!("No provider to translate upstream chat SSE");
                                *body = Some(Bytes::from(early_sse_failure_body()));
                            }
                        }
                    } else if let Some(p) = provider {
                        match p.translator.translate_response(&ctx.response_body_buf) {
                            Ok(chat_bytes) => {
                                match maxllm_translate::responses::chat_response_to_responses(
                                    &chat_bytes,
                                ) {
                                    Ok(responses_bytes) => {
                                        *body = Some(Bytes::from(responses_bytes));
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to convert chat response to Responses format");
                                        *body = Some(Bytes::from(std::mem::take(
                                            &mut ctx.response_body_buf,
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to translate provider response for Responses mode");
                                *body =
                                    Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                            }
                        }
                    } else {
                        *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                    }
                }
            }

            // Run plugin chains
            hot.global_chain.run_response_body(
                session,
                body,
                end_of_stream,
                &mut ctx.plugin_ctx,
            )?;
            if let Some(idx) = ctx.route_index {
                hot.route_chains[idx].run_response_body(
                    session,
                    body,
                    end_of_stream,
                    &mut ctx.plugin_ctx,
                )?;
            }
            return Ok(None);
        }

        // ── NORMAL MODE: translate provider → OpenAI format ──
        if ctx.is_streaming {
            let provider = hot.providers.get(&ctx.provider_name);
            let is_gemini = provider.is_some_and(|p| p.kind == ProviderKind::Gemini);

            if is_gemini {
                // Gemini streaming: we sent a non-streaming request to Gemini
                // (because we can't know streaming status before upstream_request_filter).
                // Buffer the full JSON response, translate to OpenAI, then emit as SSE.
                if let Some(data) = body.take() {
                    ctx.response_body_buf.extend_from_slice(&data);
                }
                if end_of_stream && !ctx.response_body_buf.is_empty() {
                    if let Some(p) = provider {
                        match p.translator.translate_response(&ctx.response_body_buf) {
                            Ok(translated) => {
                                // Convert the single JSON response to SSE stream format
                                let mut sse_output = Vec::new();
                                // Emit the response as a single SSE data event
                                sse_output.extend_from_slice(b"data: ");
                                sse_output.extend_from_slice(&translated);
                                sse_output.extend_from_slice(b"\n\ndata: [DONE]\n\n");
                                *body = Some(Bytes::from(sse_output));
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to translate Gemini streaming response");
                                *body =
                                    Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                            }
                        }
                    }
                }
            } else {
                // Some upstreams end their stream with a `finish_reason` chunk
                // and no `[DONE]` frame (MiniMax). pingora's last
                // response_body_filter call then arrives with an empty body
                // and end_of_stream=true, and the translator must still be
                // given that signal — otherwise it never emits
                // `response.completed`/`[DONE]` and the Responses client
                // hangs waiting for the terminal event.
                let chunk: &[u8] = body.as_deref().unwrap_or(&[]);
                if !chunk.is_empty() || end_of_stream {
                    let translated: Option<Bytes> = match ctx.stream_translator.lock() {
                        Ok(mut guard) => guard.as_mut().map(|translator| {
                            Bytes::from(translator.process_chunk(chunk, end_of_stream))
                        }),
                        Err(e) => {
                            warn!("stream_translator mutex poisoned in streaming: {e}");
                            None
                        }
                    };
                    if let Some(ref t) = translated {
                        let mut translated = t.clone();
                        observe_stream_chunk(&mut translated, ctx, end_of_stream);
                        *body = Some(translated);
                    }
                }
            }
        } else {
            if let Some(data) = body.take() {
                ctx.response_body_buf.extend_from_slice(&data);
            }

            if end_of_stream && !ctx.response_body_buf.is_empty() {
                if let Ok(parsed) =
                    serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf)
                {
                    extract_usage(&parsed, ctx);
                }

                let provider = hot.providers.get(&ctx.provider_name);

                // Normalize error responses from non-OpenAI providers to OpenAI format
                if ctx.upstream_status >= 400 {
                    if let Some(p) = provider {
                        if let Some(normalized) = normalize_error_response(
                            &ctx.response_body_buf,
                            ctx.upstream_status,
                            p.kind,
                        ) {
                            *body = Some(Bytes::from(normalized));

                            // Run plugin chains and return early
                            hot.global_chain.run_response_body(
                                session,
                                body,
                                end_of_stream,
                                &mut ctx.plugin_ctx,
                            )?;
                            if let Some(idx) = ctx.route_index {
                                hot.route_chains[idx].run_response_body(
                                    session,
                                    body,
                                    end_of_stream,
                                    &mut ctx.plugin_ctx,
                                )?;
                            }
                            return Ok(None);
                        }
                    }
                    // If normalization returned None (already OpenAI format or unparseable),
                    // fall through to the normal translation path.
                }

                if let Some(p) = provider {
                    match p.translator.translate_response(&ctx.response_body_buf) {
                        Ok(mut translated) => {
                            // Run post-call guardrails on non-streaming response
                            if let Some(ref engine) = hot.guardrail_engine {
                                if engine.has_post_call() {
                                    if let Ok(resp_parsed) =
                                        serde_json::from_slice::<serde_json::Value>(&translated)
                                    {
                                        let response_content =
                                            guardrail::extract_response_content(&resp_parsed);
                                        if !response_content.is_empty() {
                                            let output = GuardrailOutput {
                                                content: &response_content,
                                                model: &ctx.model,
                                            };
                                            let (verdict, post_applied) =
                                                tokio::task::block_in_place(|| {
                                                    tokio::runtime::Handle::current().block_on(
                                                        engine.run_post_call(
                                                            &output,
                                                            ctx.requested_guardrails.as_deref(),
                                                            ctx.route_guardrails.as_deref(),
                                                        ),
                                                    )
                                                });
                                            ctx.applied_guardrails.extend(post_applied);

                                            match verdict {
                                                GuardrailVerdict::Block { guardrail, reason } => {
                                                    let error_body = serde_json::json!({
                                                        "error": {
                                                            "message": reason,
                                                            "type": "guardrail_violation",
                                                            "guardrail": guardrail,
                                                            "code": 400
                                                        }
                                                    });
                                                    translated =
                                                        error_body.to_string().into_bytes();
                                                }
                                                GuardrailVerdict::Modify { content, .. } => {
                                                    let mut resp_json = resp_parsed;
                                                    if let Some(choices) = resp_json
                                                        .get_mut("choices")
                                                        .and_then(|c| c.as_array_mut())
                                                    {
                                                        if let Some(choice) = choices.first_mut() {
                                                            if let Some(msg) =
                                                                choice.get_mut("message")
                                                            {
                                                                msg["content"] =
                                                                    serde_json::Value::String(
                                                                        content,
                                                                    );
                                                            }
                                                        }
                                                    }
                                                    translated = serde_json::to_vec(&resp_json)
                                                        .unwrap_or(translated);
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }

                            *body = Some(Bytes::from(translated));
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to translate response body");
                            *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                        }
                    }
                } else {
                    *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body_buf)));
                }
            }
        }

        // Run plugin chains on response body
        hot.global_chain
            .run_response_body(session, body, end_of_stream, &mut ctx.plugin_ctx)?;
        if let Some(idx) = ctx.route_index {
            hot.route_chains[idx].run_response_body(
                session,
                body,
                end_of_stream,
                &mut ctx.plugin_ctx,
            )?;
        }

        Ok(None)
    }

    /// A connection that could not be established never carried the request
    /// to the upstream, so the call can be re-sent to a fallback provider
    /// without any risk of a duplicate side effect. This is the difference
    /// between "the upstream is unreachable" (retryable) and "the upstream
    /// answered with an error" (never retried — the request was executed
    /// upstream and may have been billed).
    ///
    /// Marking the error retryable makes pingora run the whole upstream
    /// attempt again: the peer is rebuilt from `ctx.provider_name` (which now
    /// points at the fallback), the request body is re-read from the client
    /// and translated by the fallback's own translator, and the fallback's
    /// response travels the regular response path.
    fn fail_to_connect(
        &self,
        session: &mut Session,
        _peer: &HttpPeer,
        ctx: &mut Self::CTX,
        mut e: Box<pingora::Error>,
    ) -> Box<pingora::Error> {
        // A connect failure means the upstream never received the request, so
        // it is always safe to replay it on another provider. The walk is
        // bounded by the route's failover budget.
        if !AiGateway::is_connect_failure(&e) {
            return e;
        }
        // ... unless the body is already spent: a request whose body was read
        // past the replay buffer cannot be offered to another provider.
        if !body_is_replayable(session, ctx) {
            return e;
        }
        let max_attempts = {
            let hot = self.hot.load();
            max_failover_attempts(&hot.routes, ctx.route_index)
        };
        if ctx.failover_count >= max_attempts {
            return e;
        }

        // A connect failure is an upstream failure: count it against the
        // provider's circuit breaker so the fallback chain stops using it.
        self.record_provider_connect_failure(ctx);
        self.unpin_session(ctx);
        self.record_event(EVENT_ERROR, ctx, Some(502), None, Some(e.to_string()), None);
        ctx.attempt_trail
            .push(format!("{}=connect", ctx.provider_name));

        let Some((failed_provider, next_provider)) = self.switch_to_fallback(ctx) else {
            return e;
        };
        info!(
            provider = %next_provider,
            failed_provider = %failed_provider,
            "upstream connection failed — retrying the request on the fallback provider"
        );
        e.set_retry(true);
        e
    }

    /// An error on an established upstream connection.
    ///
    /// Pingora's default only replays an error of this kind when the upstream
    /// connection was *reused* (`RetryType::ReusedOnly`), because a fresh
    /// connection may have delivered the request body to a live upstream that
    /// is still working on it. That rule is right for a proxy in front of an
    /// idempotent web app and wrong for this gateway, where the failure that
    /// matters is a listener that accepts the TCP connection and then hangs up
    /// without a single response byte — a model server that crashed and is
    /// being restarted, or a supervisor that holds the port while the worker
    /// is dead. The client used to get a hard 502 from that, even though a
    /// dozen other pool members were idle, and codex then sat in its own
    /// backoff for minutes.
    ///
    /// So: when *nothing* came back (`upstream_responded == false`) and the
    /// failure is the connection dying rather than an HTTP answer, treat it
    /// exactly like a connect failure — count it against the breaker, unpin
    /// the session, and replay the request on the next provider of the pool
    /// or fallback chain, bounded by the route's failover budget.
    fn error_while_proxy(
        &self,
        peer: &HttpPeer,
        session: &mut Session,
        mut e: Box<pingora::Error>,
        ctx: &mut Self::CTX,
        client_reused: bool,
    ) -> Box<pingora::Error> {
        // Keep pingora's annotations (peer context + its reuse decision)
        // before adding ours on top.
        e = e.more_context(format!("Peer: {}", peer));
        e.retry
            .decide_reuse(client_reused && !session.as_ref().retry_buffer_truncated());

        // A `Downstream` error means the *client* closed the connection. That
        // is not the provider's fault and not ours, so give it its own counter
        // instead of letting it blend into a 200 that "carries an error" (the
        // 2026-11-08 audit read 816 of those as upstream truncations).
        //
        // `stream_terminal_seen` guards the race where the client hangs up at
        // the same instant the terminal event goes out: that exchange was
        // completed, and booking it as an abort would penalise a healthy
        // provider for a client that was already done.
        let client_gone =
            e.esource() == &pingora::ErrorSource::Downstream && !ctx.stream_terminal_seen;
        if client_gone
            && !ctx
                .client_aborted
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            METRICS
                .client_aborted_total
                .with_label_values(&[&ctx.provider_name])
                .inc();
            debug!(
                provider = %ctx.provider_name,
                error = %e,
                "client closed the connection before the response body was complete"
            );
        }

        if ctx.upstream_responded {
            // Response headers already reached the client, so this attempt
            // cannot be replayed. It is still not a success: a streaming body
            // that dies mid-flight must be booked as truncated, not as a
            // clean 200 with a partial answer.
            // A terminal event already told the client the answer was
            // complete; an error raised while the connection is torn down
            // afterwards must not rewrite that success as a truncation.
            if ctx.is_streaming
                && !ctx.stream_terminal_seen
                && tracks_stream_terminal(ctx.endpoint_type)
                && e.esource() == &pingora::ErrorSource::Upstream
            {
                mark_stream_truncated(ctx);
            }
            return e;
        }
        if e.esource() != &pingora::ErrorSource::Upstream {
            return e;
        }
        if !body_is_replayable(session, ctx) {
            return e;
        }
        let starved = matches!(
            e.etype(),
            pingora::ErrorType::ConnectionClosed
                | pingora::ErrorType::ReadError
                | pingora::ErrorType::ReadTimedout
                | pingora::ErrorType::WriteError
                | pingora::ErrorType::WriteTimedout
        );
        if !starved {
            return e;
        }
        let max_attempts = {
            let hot = self.hot.load();
            max_failover_attempts(&hot.routes, ctx.route_index)
        };
        if ctx.failover_count >= max_attempts {
            return e;
        }

        self.record_provider_connect_failure(ctx);
        self.unpin_session(ctx);
        self.record_event(EVENT_ERROR, ctx, Some(502), None, Some(e.to_string()), None);
        ctx.attempt_trail
            .push(format!("{}=closed", ctx.provider_name));

        let Some((failed_provider, next_provider)) = self.switch_to_fallback(ctx) else {
            return e;
        };
        info!(
            provider = %next_provider,
            failed_provider = %failed_provider,
            "upstream closed the connection before answering — retrying the request on the fallback provider"
        );
        e.set_retry(true);
        e
    }

    /// Pingora's default `fail_to_proxy` writes an empty-body error response,
    /// leaving clients to guess what failed (a connect timeout and a DNS
    /// failure look identical). Answer with a JSON error body in the same
    /// OpenAI error shape the gateway already uses, carrying the pingora
    /// error chain for diagnosis.
    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &pingora::Error,
        _ctx: &mut Self::CTX,
    ) -> FailToProxy {
        let code = match e.etype() {
            pingora::ErrorType::HTTPStatus(code) => *code,
            _ => match e.esource() {
                pingora::ErrorSource::Upstream => 502,
                pingora::ErrorSource::Downstream => match e.etype() {
                    pingora::ErrorType::WriteError
                    | pingora::ErrorType::ReadError
                    | pingora::ErrorType::ConnectionClosed => 0, // connection already dead
                    _ => 400,
                },
                pingora::ErrorSource::Internal | pingora::ErrorSource::Unset => 500,
            },
        };

        if code > 0 {
            // The error pingora hands over describes the *last* attempt only.
            // For a request that walked several providers that is misleading:
            // a trailing DNS failure on the last pool member used to hide the
            // real "the upstream does not know this model" answer of all the
            // others. Append the per-attempt trail so the client sees the walk.
            let mut message = e.to_string();
            if !_ctx.attempt_trail.is_empty() {
                message.push_str(&format!(" (attempts: {})", _ctx.attempt_trail.join(", ")));
            }
            let body = serde_json::json!({
                "error": {
                    "message": message,
                    "type": "gateway_error",
                    "code": code
                }
            })
            .to_string();
            // `respond_error_with_body` keeps a response that was already
            // written (e.g. our own send_error_response paths) untouched.
            if let Err(err) = session
                .respond_error_with_body(code, Bytes::from(body))
                .await
            {
                warn!(error = %err, "failed to send error response to downstream");
            }
        }

        FailToProxy {
            error_code: code,
            can_reuse_downstream: false,
        }
    }

    async fn logging(
        &self,
        session: &mut Session,
        e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) {
        if ctx.skip_logging {
            return;
        }

        // Only a request that passed the fast paths ever incremented the
        // gauge; giving it back unconditionally drove the gauge negative
        // whenever a scrape hung up before its response was written.
        if ctx.active_request_counted {
            METRICS.active_requests.dec();
        }

        // The request is done — it no longer counts towards the provider's
        // in-flight load, and it gives back the half-open probe slot it may
        // have reserved. The release is idempotent and clamped at zero, so
        // it is safe on paths that never booked a breaker outcome (a health
        // probe, a 4xx that fails over, a client that hung up mid-stream).
        if !ctx.provider_name.is_empty() {
            crate::provider_stats::release(&ctx.provider_name);
            let hot = self.hot.load();
            if let Some(p) = hot.providers.get(&ctx.provider_name) {
                p.circuit_breaker.release_probe();
            }
        }

        // A session that never reached the router has no route, no provider,
        // no model, no tokens and no cost. Booking it as a request used to
        // write a phantom `request_logs` row (provider "", endpoint type taken
        // from the default, latency 0, status 0) and a
        // `requests_total{provider="none",model="unknown",status="0"}` sample,
        // which then showed up as a "status 0" failure in every report. The
        // 2026-11-08 audit traced 88 such rows — and reproduced them by
        // resetting a socket after `GET /health` — so counting them apart
        // keeps the status-0 population honest.
        if is_unrouted_session(ctx) {
            METRICS.unrouted_sessions_total.inc();
            match e {
                // A client that hangs up between the request line and the
                // response is routine (scrapes, port checks, tab reloads).
                Some(err) if err.esource() == &pingora::ErrorSource::Downstream => {
                    debug!(error = %err, path = %ctx.request_path, "session ended before routing (client)");
                }
                // Anything else here is a gateway defect and must be visible.
                Some(err) => {
                    warn!(error = %err, path = %ctx.request_path, "session ended before routing (internal)");
                }
                None => {}
            }
            return;
        }

        if ctx.probe_request {
            info!(
                provider = %ctx.provider_name,
                status = ctx.upstream_status,
                "health probe completed (excluded from metrics and request logs)"
            );
            return;
        }

        let duration = ctx.request_start.elapsed();

        // Record metrics
        METRICS
            .request_duration_seconds
            .with_label_values(&[&ctx.provider_name])
            .observe(duration.as_secs_f64());

        // Every request that reached a provider is counted once: the provider
        // that served it, the model and the upstream status. The status is
        // `0` when the provider never answered (a connect or DNS failure).
        METRICS
            .requests_total
            .with_label_values(&[
                if ctx.provider_name.is_empty() {
                    "none"
                } else {
                    &ctx.provider_name
                },
                if ctx.model.is_empty() {
                    "unknown"
                } else {
                    &ctx.model
                },
                &ctx.upstream_status.to_string(),
            ])
            .inc();

        if ctx.tokens_in > 0 || ctx.tokens_out > 0 {
            METRICS
                .tokens_in_total
                .with_label_values(&[&ctx.provider_name, &ctx.model])
                .inc_by(ctx.tokens_in);
            METRICS
                .tokens_out_total
                .with_label_values(&[&ctx.provider_name, &ctx.model])
                .inc_by(ctx.tokens_out);
            if ctx.cached_tokens_in > 0 {
                METRICS
                    .cached_input_tokens_total
                    .with_label_values(&[&ctx.provider_name, &ctx.model])
                    .inc_by(ctx.cached_tokens_in);
            }
        }

        let hot = self.hot.load();

        // A 4xx/5xx is a failure in its own right — it cannot be a "200 cut
        // short". Bounding `truncated` here (besides the guards in
        // `mark_stream_truncated` and `observe_stream_chunk`) keeps the
        // log/metrics honest even if a future path marks the flag on an
        // error response: the real upstream error is reported, not a
        // misleading "stream truncated".
        let truncated = ctx
            .stream_truncated
            .load(std::sync::atomic::Ordering::Relaxed)
            && ctx.upstream_status < 400;

        // Stream health. A 200 whose body was cut mid-stream never reaches the
        // plain failure counter (`record_success` resets the streak, since the
        // status *was* a success), so a provider that truncates a large share
        // of its streams would keep every byte of traffic and every byte of
        // the complaint. Feed the ratio window instead; when it trips, the
        // circuit opens and routing walks to the next fallback provider.
        let judged_stream = ctx.is_streaming
            && ctx.upstream_status == 200
            && tracks_stream_terminal(ctx.endpoint_type)
            && (ctx.stream_terminal_seen || truncated);
        if judged_stream {
            METRICS
                .streams_judged_total
                .with_label_values(&[&ctx.provider_name])
                .inc();
            if let Some(p) = hot.providers.get(&ctx.provider_name) {
                let before = p.circuit_breaker.stream_opens();
                p.circuit_breaker.record_stream_outcome(truncated);
                let after = p.circuit_breaker.stream_opens();
                if after > before {
                    METRICS
                        .circuit_breaker_stream_opens_total
                        .with_label_values(&[&ctx.provider_name])
                        .inc_by(after - before);
                    METRICS
                        .circuit_breaker_state
                        .with_label_values(&[&ctx.provider_name])
                        .set(p.circuit_breaker.state() as i64);
                    METRICS
                        .circuit_breaker_transitions_total
                        .with_label_values(&[&ctx.provider_name])
                        .set(p.circuit_breaker.state_transitions() as i64);
                    // Demoting a provider is a routing change, so keep it
                    // across a restart like any other open circuit.
                    self.persist_circuit_breaker(&ctx.provider_name, &p.circuit_breaker);
                    self.unpin_session(ctx);
                    warn!(
                        provider = %ctx.provider_name,
                        "stream truncation ratio exceeded the demotion threshold — \
                         circuit force-opened so the route fails over"
                    );
                }
            }
        }

        let truncation_error = truncated.then(|| {
            "stream truncated: upstream ended the body before a terminal event".to_string()
        });
        // A 2xx/3xx that reported no tokens at all would be booked as a free
        // request; surface the gap instead of letting it hide in a zero row.
        // A client abort is excluded: the usage was never going to arrive
        // because the caller stopped listening, and counting it as "the
        // provider reported nothing" would point at the wrong party.
        if !truncated
            && !ctx
                .client_aborted
                .load(std::sync::atomic::Ordering::Relaxed)
            && ctx.upstream_status != 0
            && ctx.upstream_status < 400
            && ctx.tokens_in == 0
            && ctx.tokens_out == 0
        {
            METRICS
                .usage_missing_total
                .with_label_values(&[&ctx.provider_name, &ctx.model])
                .inc();
        }

        // Calculate cost (skip when no tokens to avoid unnecessary lookups).
        // When the price table has no entry for the model, fall back to the
        // cost the upstream itself reported (e.g. opencode.ai's
        // `usage.opencode.allowance`) instead of recording 0.
        let mut cost = if ctx.tokens_in > 0 || ctx.tokens_out > 0 {
            // Cached input bills at the model's cache rate, not the list rate:
            // this fleet's traffic is ~99% prompt-cache hits, so quoting list
            // price for every input token overbooks the biggest line roughly
            // sixfold (MiniMax M3: $0.30 list vs $0.05 cache).
            hot.cost_calculator.calculate_cost_with_cache(
                &ctx.model,
                ctx.tokens_in,
                ctx.tokens_out,
                ctx.cached_tokens_in,
            )
        } else {
            0.0
        };
        if cost == 0.0 {
            if let Some(upstream) = ctx.upstream_cost_usd {
                cost = upstream;
            }
        }
        ctx.cost_usd = cost;

        // Record the spend against the virtual key that authenticated this
        // request (per-key counters, budget window and spend log).
        if let (Some(enforcer), Some(key_id)) =
            (&self.budget_enforcer, ctx.virtual_key_id.as_deref())
        {
            if ctx.tokens_in > 0 || ctx.tokens_out > 0 {
                if let Err(err) = enforcer.record_usage(
                    key_id,
                    &ctx.model,
                    &ctx.provider_name,
                    ctx.tokens_in,
                    ctx.tokens_out,
                    ctx.plugin_ctx.request_id.as_deref(),
                ) {
                    warn!(error = %err, "Failed to record usage for virtual key");
                }
                // Feed the actual tokens into the key's TPM sliding window.
                self.key_rate_limiter
                    .record_tokens(key_id, ctx.tokens_in + ctx.tokens_out);
            }
        }

        // Run plugin logging chains
        hot.global_chain
            .run_logging(session, e, &mut ctx.plugin_ctx)
            .await;
        if let Some(idx) = ctx.route_index {
            hot.route_chains[idx]
                .run_logging(session, e, &mut ctx.plugin_ctx)
                .await;
        }

        let latency_ms = duration.as_millis() as u64;
        // Feed the auto pool's latency filter with the *whole* exchange
        // duration (time-to-last-byte). Successes only: the cohort must rank
        // by how long a client actually waits, and a failed exchange is not a
        // latency sample. TTFB keeps feeding the route latency strategy.
        if ctx.upstream_status < 400 && !ctx.provider_name.is_empty() {
            crate::provider_stats::record_total_latency(&ctx.provider_name, latency_ms as f64);
        }

        info!(
            provider = %ctx.provider_name,
            model = %ctx.model,
            request_id = ?ctx.plugin_ctx.request_id,
            tokens_in = ctx.tokens_in,
            tokens_out = ctx.tokens_out,
            cached_in = ctx.cached_tokens_in,
            cost_usd = format!("{:.6}", cost),
            latency_ms = latency_ms,
            fallback = ctx.fallback_used,
            truncated,
            "request completed"
        );

        // Write request log to admin store (fire-and-forget)
        if let Some(ref admin_api) = self.admin_api {
            let log = maxllm_admin::RequestLog {
                id: uuid::Uuid::now_v7().to_string(),
                timestamp: chrono::Utc::now(),
                provider: ctx.provider_name.clone(),
                model: ctx.model.clone(),
                tokens_in: ctx.tokens_in,
                tokens_out: ctx.tokens_out,
                cached_tokens: ctx.cached_tokens_in,
                cost_usd: cost,
                latency_ms,
                status: ctx.upstream_status,
                request_id: ctx.plugin_ctx.request_id.clone(),
                session_id: ctx.session_id.clone(),
                client_ip: ctx.plugin_ctx.client_ip.clone(),
                route_path: ctx.plugin_ctx.route_path.clone(),
                endpoint_type: format!("{:?}", ctx.endpoint_type),
                fallback_used: ctx.fallback_used,
                error: e
                    .map(|err| err.to_string())
                    .or_else(|| truncation_error.clone()),
            };
            if let Err(err) = admin_api.record_log(log) {
                warn!(error = %err, "Failed to record request log");
            }
        }

        // Audit trail: the exchange finished.
        self.record_event(
            EVENT_CLIENT_RESPONSE,
            ctx,
            Some(ctx.upstream_status),
            Some(latency_ms),
            e.map(|err| err.to_string()).or(truncation_error),
            ctx.upstream_body.clone(),
        );
    }
}

impl AiGateway {
    /// Whether the failure means the request never reached the upstream.
    ///
    /// Only connection-establishment failures qualify: the TCP handshake, the
    /// TLS handshake, DNS resolution or a refused connection. Everything else
    /// (a response that arrived, a mid-stream disconnect, a timeout while
    /// reading) means the upstream may have processed the request, so it is
    /// never re-sent.
    fn is_connect_failure(e: &pingora::Error) -> bool {
        e.esource() == &pingora::ErrorSource::Upstream
            && matches!(
                e.etype(),
                pingora::ErrorType::ConnectTimedout
                    | pingora::ErrorType::ConnectRefused
                    | pingora::ErrorType::ConnectNoRoute
                    | pingora::ErrorType::ConnectError
                    | pingora::ErrorType::TLSHandshakeFailure
                    | pingora::ErrorType::TLSHandshakeTimedout
                    | pingora::ErrorType::HandshakeError
                    | pingora::ErrorType::InvalidCert
            )
    }

    /// The next provider to try: the first healthy provider of the route's
    /// fallback chain that this request has not tried yet (skipping open
    /// circuits), so the walk is strictly forward through the chain.
    /// Returns its name and state.
    fn next_fallback_provider(&self, ctx: &RequestCtx) -> Option<(String, Arc<ProviderState>)> {
        // A path-pinned provider is served alone: the caller named it, so a
        // failure is reported to the caller instead of being papered over by
        // another provider's answer.
        if ctx.path_pinned_provider.is_some() {
            return None;
        }
        let hot = self.hot.load();
        let route = hot.routes.get(ctx.route_index?)?;

        // Admission for the entry that is actually switched to. The walks
        // below only *inspect* candidates (`allows_traffic`); the one that
        // becomes the next provider reserves its half-open probe slot here,
        // and no other entry's slot is touched.
        let admit = |provider: &str| -> bool {
            hot.providers
                .get(provider)
                .map(|p| !p.circuit_breaker.is_open())
                .unwrap_or(false)
        };

        // Auto pool: walk the entries of the pool this request rotates over
        // (still skipping attempted and open-circuit entries) rather than the
        // route's fallback chain.
        //
        // The same pool is the destination for a request that carries a
        // sentinel model on a route without `auto_model = true` — e.g. a
        // client that switched its model picker to "auto" while its base URL
        // still points at the summary route. The sentinel means "rotate"; no
        // upstream has a model by that name, so walking the route's own chain
        // would only collect 404s from every provider in it.
        if let Some(pool) = hot.request_pool(route, ctx) {
            // A genuine auto route picked its first provider in
            // `select_provider` (session-sticky, then round-robin), so this
            // walk only has to move forward to the next healthy entry.
            if route.auto_model {
                let next = pool.usable_entries(|e| {
                    e.provider != ctx.provider_name
                        && !ctx.attempted_providers.contains(&e.provider)
                        && hot
                            .providers
                            .get(&e.provider)
                            .map(|p| p.circuit_breaker.allows_traffic())
                            .unwrap_or(false)
                });
                let entry = next.into_iter().find(|e| admit(&e.provider))?;
                if entry.billing == BillingMode::Metered {
                    METRICS.auto_pool_metered_selections_total.inc();
                }
                return hot
                    .providers
                    .get(&entry.provider)
                    .map(|p| (entry.provider.clone(), Arc::clone(p)));
            }
            // Sentinel carried by a request on a non-auto route: the pool is
            // entered mid-request, so no `select_provider` slot ever rotated
            // for it. Rotate here instead — walking the pool in order would
            // pin every such request to the first working entry and leave the
            // other 24 idle, which is the opposite of what `auto` promises.
            //
            // Session binding first: a conversation that already landed on a
            // pool provider keeps it, so the upstream keeps its context.
            // Tier-aware: only entries of the preferred billing tier are
            // candidates, so the walk never reaches a metered entry while a
            // subscription entry is still untried and healthy.
            let healthy: Vec<&AutoPoolEntry> = pool.usable_entries(|e| {
                e.provider != ctx.provider_name
                    && !ctx.attempted_providers.contains(&e.provider)
                    && hot
                        .providers
                        .get(&e.provider)
                        .map(|p| p.circuit_breaker.allows_traffic())
                        .unwrap_or(false)
            });
            let metered_tier = healthy.iter().all(|e| e.billing == BillingMode::Metered);
            if let Some(session_id) = ctx.session_id.as_deref() {
                if let Some(pinned) = self.sessions.get(session_id) {
                    // Sticky only inside the preferred tier: a pin on the
                    // metered tier is abandoned as soon as a subscription
                    // entry can serve again.
                    if let Some(entry) = healthy.iter().copied().find(|e| e.provider == pinned) {
                        if let Some(pair) = hot.providers.get(&entry.provider) {
                            METRICS.session_affinity_hits_total.inc();
                            if metered_tier {
                                METRICS.auto_pool_metered_selections_total.inc();
                            }
                            return Some((entry.provider.clone(), Arc::clone(pair)));
                        }
                    }
                    METRICS.session_affinity_misses_total.inc();
                }
            }
            if healthy.is_empty() {
                return None;
            }
            let idx = AUTO_POOL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let rotation = auto_pool_rotation(pool, &healthy, idx);
            let entry = (0..rotation.len())
                .map(|i| rotation[(idx as usize + i) % rotation.len()])
                .find(|e| admit(&e.provider))?;
            if metered_tier {
                METRICS.auto_pool_metered_selections_total.inc();
            }
            return hot
                .providers
                .get(&entry.provider)
                .map(|p| (entry.provider.clone(), Arc::clone(p)));
        }

        route
            .fallback
            .iter()
            .filter(|name| *name != &ctx.provider_name)
            .filter(|name| !ctx.attempted_providers.contains(*name))
            .find_map(|name| {
                hot.providers
                    .get(name)
                    .filter(|p| !p.circuit_breaker.is_open())
                    .map(|p| (name.clone(), Arc::clone(p)))
            })
    }

    /// Count a connect failure against the provider's circuit breaker,
    /// update the breaker metrics and persist the new state, so the fallback
    /// chain stops using a provider that cannot be reached.
    fn record_provider_connect_failure(&self, ctx: &RequestCtx) {
        if ctx.probe_request {
            return;
        }
        let hot = self.hot.load();
        let Some(p) = hot.providers.get(&ctx.provider_name) else {
            return;
        };
        p.circuit_breaker.record_failure();
        METRICS
            .circuit_breaker_state
            .with_label_values(&[&ctx.provider_name])
            .set(p.circuit_breaker.state() as i64);
        self.persist_circuit_breaker(&ctx.provider_name, &p.circuit_breaker);
    }

    /// Move the request to the next healthy provider of the route's fallback
    /// chain. Returns the failed and the new provider name.
    ///
    /// The retry re-runs the whole upstream attempt, including
    /// `request_body_filter` — that is what turns a Responses body into a
    /// chat body for a fallback that only speaks chat completions (and what
    /// normalises the body into opencode's stricter schema when the fallback
    /// is an opencode provider). State left over from the failed attempt
    /// would make the retry skip that work and forward the previous body (a
    /// chat upstream then receives `{"input":…}` and rejects it):
    /// - `body_translated` marks the failed attempt's body as processed;
    /// - `request_body_buf` still holds the chunks of that attempt;
    /// - `keep_request_content_length` was derived from the failed
    ///   provider's framing (re-derived by `upstream_request_filter`
    ///   for the retry, so it must start out false);
    /// - `stream_translator` was picked for the failed provider's
    ///   response contract.
    fn switch_to_fallback(&self, ctx: &mut RequestCtx) -> Option<(String, String)> {
        let (name, fallback) = self.next_fallback_provider(ctx)?;
        let failed_provider = ctx.provider_name.clone();
        ctx.failover_count += 1;
        // The new provider starts with a fresh same-provider retry budget.
        ctx.provider_retry_count = 0;
        ctx.pending_retry_backoff_ms = None;
        ctx.attempted_providers.push(failed_provider.clone());
        ctx.fallback_used = true;
        if ctx.original_provider.is_none() {
            ctx.original_provider = Some(failed_provider.clone());
        }
        ctx.provider_name = name.clone();
        // The in-flight request moves with it: the failed provider is done
        // with this request, the next one carries it now.
        crate::provider_stats::release(&failed_provider);
        crate::provider_stats::acquire(&name);
        // The fallback may speak a different contract than the provider that
        // just failed — the request path depends on it.
        ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
            && fallback.native_responses
            && fallback.responses_path.is_some();
        // Auto pool: refresh the (provider, model) target to the new pool
        // entry so the replayed body carries the right model.
        //
        // This also covers the first switch of a request that asked for a
        // sentinel on a non-auto route: the provider just picked from the pool
        // becomes the rotation target, and `ctx.auto_pool` keeps every later
        // hop inside that same pool instead of falling back to the route's
        // chain (or leaking into another pool).
        {
            let hot = self.hot.load();
            let pool = ctx
                .auto_pool
                .as_deref()
                .and_then(|n| hot.pool_named(n))
                .or_else(|| {
                    hot.routes
                        .get(ctx.route_index.unwrap_or(usize::MAX))
                        .and_then(|route| hot.request_pool(route, ctx))
                });
            if let Some(pool) = pool {
                if let Some(e) = pool.entry_for(&name) {
                    ctx.auto_pool = Some(pool.name.clone());
                    ctx.auto_target = Some((e.provider.clone(), e.model.clone()));
                    ctx.model = e.model.clone();
                    ctx.plugin_ctx.model = e.model.clone();
                }
            }
        }
        ctx.body_translated = false;
        ctx.upstream_responded = false;
        ctx.request_body_buf.clear();
        // Re-feed the whole client body on the retry: the new provider must
        // see the request translated again (the model may change), and
        // pingora's own replay buffer only holds the first 64 KiB.
        ctx.replay_body = ctx.client_body.clone();
        ctx.replay_injected = false;
        ctx.keep_request_content_length = false;
        if let Ok(mut guard) = ctx.stream_translator.lock() {
            *guard = None;
        }
        METRICS
            .fallbacks_total
            .with_label_values(&[&failed_provider, &name])
            .inc();
        Some((failed_provider, name))
    }

    /// Persist a provider's circuit-breaker state.
    ///
    /// The admin store is the durable home of the breaker state: a gateway
    /// restart (or hot reload) keeps an open circuit and a 429 quota
    /// force-open instead of immediately hammering the failed channel.
    fn persist_circuit_breaker(&self, provider_name: &str, breaker: &CircuitBreaker) {
        let Some(admin_api) = self.admin_api.as_ref() else {
            return;
        };
        if let Err(err) = admin_api.save_circuit_breaker_state(breaker.snapshot(provider_name)) {
            warn!(error = %err, provider = %provider_name, "Failed to persist circuit breaker state");
        }
    }

    /// A provider whose hostname cannot be resolved is unreachable — the
    /// same as a connect failure. Count it against the circuit breaker,
    /// switch to the next provider of the route (fallback chain or auto
    /// pool), and report whether the switch happened so the caller re-resolves.
    /// Bounded by the route's failover budget.
    async fn try_dns_failover(&self, ctx: &mut RequestCtx) -> bool {
        let max_attempts = {
            let hot = self.hot.load();
            max_failover_attempts(&hot.routes, ctx.route_index)
        };
        if ctx.failover_count >= max_attempts {
            return false;
        }
        self.record_provider_connect_failure(ctx);
        self.unpin_session(ctx);
        ctx.attempt_trail.push(format!("{}=dns", ctx.provider_name));
        if let Some((failed_provider, next_provider)) = self.switch_to_fallback(ctx) {
            info!(
                provider = %next_provider,
                failed_provider = %failed_provider,
                "upstream DNS resolution failed — retrying the request on the fallback provider"
            );
            return true;
        }
        false
    }

    /// Append one event to the JSONL recorder, if recording is enabled.
    fn record_event(
        &self,
        event_type: &'static str,
        ctx: &RequestCtx,
        status: Option<u16>,
        duration_ms: Option<u64>,
        error: Option<String>,
        body: Option<Vec<u8>>,
    ) {
        let Some(recorder) = self.recorder.as_ref() else {
            return;
        };
        recorder.record(RecordEntry {
            event_type,
            request_uuid: ctx.request_uuid.clone(),
            session_id: ctx.session_id.clone(),
            client_ip: ctx.plugin_ctx.client_ip.clone(),
            provider: Some(ctx.provider_name.clone()),
            model: if ctx.model.is_empty() {
                None
            } else {
                Some(ctx.model.clone())
            },
            method: Some(ctx.request_method.clone()),
            path: Some(ctx.request_path.clone()),
            status_code: status,
            body,
            duration_ms,
            ttft_ms: ctx.ttft_ms,
            tokens_in: Some(ctx.tokens_in),
            tokens_out: Some(ctx.tokens_out),
            error,
        });
    }

    /// Pin the session to the provider that just served the request.
    fn pin_session(&self, ctx: &RequestCtx) {
        let hot = self.hot.load();
        let Some(route) = ctx.route_index.and_then(|i| hot.routes.get(i)) else {
            return;
        };
        if !route.session_sticky || !hot.session_sticky_enabled {
            return;
        }
        let Some(session_id) = ctx.session_id.as_deref() else {
            return;
        };
        let evicted = self.sessions.set(session_id, &ctx.provider_name);
        if evicted > 0 {
            METRICS
                .session_affinity_evictions_total
                .inc_by(evicted as u64);
        }
        METRICS
            .session_affinity_size
            .set(self.sessions.len() as i64);
        // Persist the binding so session affinity survives a gateway restart.
        if let Some(ref admin_api) = self.admin_api {
            if let Err(err) = admin_api.save_session_binding(session_id, &ctx.provider_name) {
                warn!(error = %err, session = %session_id, "Failed to persist session binding");
            }
        }
    }

    /// Drop the session binding when the pinned provider failed, so the next
    /// request is routed to a healthy provider.
    fn unpin_session(&self, ctx: &RequestCtx) {
        let hot = self.hot.load();
        let Some(route) = ctx.route_index.and_then(|i| hot.routes.get(i)) else {
            return;
        };
        if !route.session_sticky || !hot.session_sticky_enabled {
            return;
        }
        let Some(session_id) = ctx.session_id.as_deref() else {
            return;
        };
        // Only unpin when the failing provider is the pinned one.
        if self.sessions.peek(session_id).as_deref() == Some(ctx.provider_name.as_str()) {
            self.sessions.remove(session_id);
            METRICS
                .session_affinity_size
                .set(self.sessions.len() as i64);
            // Drop the persisted binding too.
            if let Some(ref admin_api) = self.admin_api {
                if let Err(err) = admin_api.delete_session_binding(session_id) {
                    warn!(error = %err, session = %session_id, "Failed to delete session binding");
                }
            }
        }
    }

    /// Walk the fallback chain from the body filter, after a decision that had
    /// to wait for the whole upstream error body (429 rate-limit resolution,
    /// 404 model lookup).
    ///
    /// `Ok(None)` delivers the upstream error to the client as-is — the body
    /// cannot be replayed, the failover budget is spent, or no fallback is
    /// left. The retry error walks on to the next provider of the chain.
    fn fail_over_from_body_filter(
        &self,
        session: &mut Session,
        ctx: &mut RequestCtx,
        status: u16,
        reason: &str,
    ) -> pingora::Result<Option<std::time::Duration>> {
        // A provider that failed must not keep the session pinned.
        self.unpin_session(ctx);

        if !body_is_replayable(session, ctx) {
            info!(
                status = status,
                provider = %ctx.provider_name,
                "failover refused — the request body exceeds the 64 KiB replay buffer"
            );
            return Ok(None);
        }

        // Same failover budget as the header-time path: when it is spent, the
        // provider's answer reaches the client as-is.
        let hot = self.hot.load();
        if ctx.failover_count >= max_failover_attempts(&hot.routes, ctx.route_index) {
            info!(
                status = status,
                provider = %ctx.provider_name,
                attempts = ctx.failover_count,
                reason = reason,
                "body-time decision reached, but the failover budget is spent — returning the error to the client"
            );
            return Ok(None);
        }

        // Audit trail: the failed provider answered (the final provider's
        // answer is recorded by `response_filter`).
        self.record_event(EVENT_PROVIDER_RESPONSE, ctx, Some(status), None, None, None);

        // Walk the fallback chain like any other failover.
        let Some((failed_provider, next_provider)) = self.switch_to_fallback(ctx) else {
            // No fallback left — the error reaches the client as-is.
            return Ok(None);
        };
        METRICS
            .http_failovers_total
            .with_label_values(&[&failed_provider, &next_provider, &status.to_string()])
            .inc();
        info!(
            status = status,
            provider = %next_provider,
            failed_provider = %failed_provider,
            reason = reason,
            "retrying on the fallback provider"
        );
        let mut e = pingora::Error::explain(
            pingora::ErrorType::HTTPStatus(status),
            format!(
                "provider '{failed_provider}' answered {status} ({reason}); retrying on '{next_provider}'"
            ),
        );
        e.as_up();
        e.set_retry(true);
        Err(e)
    }
}

impl AiGateway {
    /// Handle admin API requests (/admin/*).
    async fn handle_admin_request(
        &self,
        session: &mut Session,
        path: &str,
    ) -> pingora::Result<bool> {
        let admin_api = match &self.admin_api {
            Some(api) => api,
            None => {
                let body = serde_json::json!({
                    "error": {
                        "message": "Admin API is not enabled. Configure [admin] in maxllm.toml.",
                        "type": "gateway_error",
                        "code": 404
                    }
                });
                let body_bytes = body.to_string().into_bytes();
                let content_len = body_bytes.len().to_string();
                let mut resp = ResponseHeader::build(404, Some(2))?;
                resp.insert_header("Content-Type", "application/json")?;
                resp.insert_header("Content-Length", &content_len)?;
                session.write_response_header(Box::new(resp), false).await?;
                session
                    .write_response_body(Some(Bytes::from(body_bytes)), true)
                    .await?;
                METRICS.active_requests.dec();
                return Ok(true);
            }
        };

        // Extract method
        let method = session.req_header().method.to_string();

        // Extract auth key from Authorization header
        let auth_key = session
            .req_header()
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.strip_prefix("Bearer ").unwrap_or(v).to_string())
            .unwrap_or_default();

        // Read the request body for POST requests
        let body_bytes = if method == "POST" {
            match session.read_request_body().await? {
                Some(b) => b.to_vec(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Dispatch to admin API
        let api_resp = admin_api.handle_request(&method, path, &body_bytes, &auth_key);

        // Send response
        let content_len = api_resp.body.len().to_string();
        let mut resp = ResponseHeader::build(api_resp.status, Some(2))?;
        resp.insert_header("Content-Type", "application/json")?;
        resp.insert_header("Content-Length", &content_len)?;
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(api_resp.body)), true)
            .await?;
        METRICS.active_requests.dec();
        Ok(true)
    }
}

impl AiGateway {
    /// Send a guardrail block error response and abort the request.
    async fn send_guardrail_block(
        &self,
        session: &mut Session,
        ctx: &mut RequestCtx,
        guardrail: &str,
        reason: &str,
    ) -> pingora::Result<()> {
        let error_body = serde_json::json!({
            "error": {
                "message": reason,
                "type": "guardrail_violation",
                "guardrail": guardrail,
                "code": 400
            }
        });
        let error_bytes = error_body.to_string().into_bytes();
        let mut resp = pingora::http::ResponseHeader::build(400, Some(4))?;
        resp.insert_header("Content-Type", "application/json")?;
        resp.insert_header("Content-Length", error_bytes.len().to_string())?;
        resp.insert_header(
            "X-MaxLLM-Applied-Guardrails",
            ctx.applied_guardrails.join(", "),
        )?;
        resp.insert_header("X-MaxLLM-Guardrail-Blocked", guardrail)?;
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(error_bytes)), true)
            .await?;
        session.set_keepalive(None);
        ctx.guardrail_blocked = Some(GuardrailVerdict::Block {
            guardrail: guardrail.to_string(),
            reason: reason.to_string(),
        });

        Err(pingora::Error::explain(
            pingora::ErrorType::HTTPStatus(400),
            "guardrail blocked request",
        ))
    }
}

/// Parse a `Retry-After` header value into seconds. Supports the delay-seconds
/// form ("120") and the HTTP-date form ("Wed, 21 Oct 2015 07:28:00 GMT"),
/// resolved against the current time.
fn parse_retry_after_secs(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(secs);
    }
    chrono::DateTime::parse_from_rfc2822(trimmed)
        .ok()
        .map(|dt| {
            let delta = dt
                .with_timezone(&chrono::Utc)
                .signed_duration_since(chrono::Utc::now());
            delta.num_seconds().max(0) as u64
        })
}

/// Normalize a provider error response body to OpenAI error format.
/// Returns `Some(normalized_bytes)` if the body was translated, `None` otherwise.
fn normalize_error_response(
    body_bytes: &[u8],
    status: u16,
    provider_kind: ProviderKind,
) -> Option<Vec<u8>> {
    let parsed: serde_json::Value = serde_json::from_slice(body_bytes).ok()?;

    // Already in OpenAI error format? OpenAI errors have:
    // {"error": {"message": "...", "type": "...", "code": ...}}
    // Anthropic has top-level "type":"error", Gemini has "status" instead of "type".
    if let Some(err) = parsed.get("error") {
        if err.get("message").is_some() && err.get("type").is_some() && parsed.get("type").is_none()
        {
            return None;
        }
    }

    let (message, error_type, code) = match provider_kind {
        // Anthropic: {"type":"error","error":{"type":"...","message":"..."}}
        ProviderKind::Anthropic | ProviderKind::Bedrock => {
            let err = parsed.get("error")?;
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Anthropic error");
            let etype = err
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("api_error");
            (msg.to_string(), etype.to_string(), status)
        }
        // Gemini: {"error":{"code":N,"message":"...","status":"..."}}
        ProviderKind::Gemini => {
            if let Some(err) = parsed.get("error") {
                let msg = err
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown Gemini error");
                let etype = err
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("api_error");
                (msg.to_string(), etype.to_string(), status)
            } else {
                // Sometimes Gemini returns a plain error array
                let msg = parsed
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown Gemini error");
                (msg.to_string(), "api_error".to_string(), status)
            }
        }
        // Cohere: {"message":"..."}
        ProviderKind::Cohere => {
            let msg = parsed
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Cohere error");
            (msg.to_string(), "api_error".to_string(), status)
        }
        // Mistral and other OpenAI-compat may already be in format, but handle gracefully
        _ => {
            // Nested error objects (opencode.ai, Anthropic-style envelope):
            // {"type":"error","error":{"type":"MissingSessionID","message":"…"}}
            // — surface the real message instead of "Unknown provider error".
            if let Some(err) = parsed.get("error") {
                if let Some(msg) = err.get("message").and_then(|m| m.as_str()) {
                    let etype = err
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("api_error");
                    return Some(
                        serde_json::json!({
                            "error": {"message": msg, "type": etype, "code": status}
                        })
                        .to_string()
                        .into_bytes(),
                    );
                }
            }
            // Try to extract any "message" or "error" string
            let msg = parsed
                .get("message")
                .and_then(|m| m.as_str())
                .or_else(|| parsed.get("error").and_then(|e| e.as_str()))
                .unwrap_or("Unknown provider error");
            (msg.to_string(), "api_error".to_string(), status)
        }
    };

    let normalized = serde_json::json!({
        "error": {
            "message": message,
            "type": error_type,
            "code": code
        }
    });
    serde_json::to_vec(&normalized).ok()
}

/// Whether a response body is a chat-completions SSE stream.
///
/// The Responses contract and the chat contract share the `data:` framing, so
/// the framing alone does not tell them apart. A chat chunk is recognised by
/// its `chat.completion` object marker, which never appears in a Responses
/// event.
fn looks_like_chat_sse(data: &[u8]) -> bool {
    let head = &data[..data.len().min(4096)];
    let text = String::from_utf8_lossy(head);
    // The SSE framing ("data:" at the start of the body) plus the chat object
    // marker: a non-streaming chat response is JSON and carries no framing.
    text.trim_start().starts_with("data:") && text.contains("chat.completion")
}

/// Whether the buffered body is a Responses SSE stream.
///
/// Responses frames start with an `event:` line followed by a `data:` line
/// (chat frames start directly with `data:`), and the event payloads carry
/// the Responses `response.*` type markers — the terminal event name is the
/// distinguishing token. A plain JSON response has neither a `data:` line
/// nor the terminal marker.
fn looks_like_responses_sse(data: &[u8]) -> bool {
    let head = &data[..data.len().min(4096)];
    let text = String::from_utf8_lossy(head);
    text.contains("data:") && text.contains("response.completed")
}

/// The body sent when an upstream chat SSE stream cannot be converted into
/// Responses events. Raw provider bytes are never forwarded to a Responses
/// client, so an explicit error is sent instead.
/// Whether this client may read `/metrics`.
///
/// Loopback is always allowed — that is the local OTel collector. Everything
/// else needs `[metrics] allow_remote = true`. An unknown peer address (no
/// socket, e.g. a unix socket transport) is treated as remote: the endpoint
/// names every provider, model, key and price, so the safe default is to deny.
fn metrics_visible_to(allow_remote: bool, client: Option<std::net::IpAddr>) -> bool {
    allow_remote || client.is_some_and(|ip| ip.is_loopback())
}

fn early_sse_failure_body() -> String {
    serde_json::json!({
        "error": {
            "message": "upstream returned a chat completion stream that could not be converted to the Responses format",
            "type": "gateway_error",
            "code": 502
        }
    })
    .to_string()
}

/// Protocol markers that prove a stream reached its defined end. A stream
/// that ends without one of these was cut off mid-flight (the upstream died,
/// the proxy hop dropped, …) even though the HTTP status was 200.
const STREAM_TERMINAL_MARKERS: &[&[u8]] = &[
    b"[DONE]",
    b"response.completed",
    b"response.incomplete",
    b"response.failed",
    b"event: error",
];

/// How much of the already-scanned stream is kept to bridge a terminal marker
/// that an upstream chunk boundary cut in half (`[DO` + `NE]`).
const STREAM_SCAN_TAIL: usize = 32;

/// Whether the gateway knows this endpoint's stream grammar well enough to
/// judge a body as complete or truncated.
///
/// Only the OpenAI-compatible dialects are judged. `Native` and `Passthrough`
/// speak a provider-specific SSE grammar the gateway does not parse (Anthropic
/// ends on `message_stop`, Gemini on `finishReason`, …), so treating a missing
/// OpenAI marker as truncation there would flag every healthy stream.
/// Whether this session ended before the router was reached: it has no route
/// index and no provider, so it never had a model, a token count or a cost.
///
/// Such a session is not a *request* and must not be booked as one. The
/// logging hook used to write a `request_logs` row for it with an empty
/// provider and a status of 0 — an aborted `/health` or `/metrics` scrape was
/// the usual cause — which then read as a provider failure in every report
/// built from that table.
fn is_unrouted_session(ctx: &RequestCtx) -> bool {
    ctx.route_index.is_none() && ctx.provider_name.is_empty()
}

fn tracks_stream_terminal(endpoint: EndpointType) -> bool {
    matches!(
        endpoint,
        EndpointType::ChatCompletions | EndpointType::Completions | EndpointType::Responses
    )
}

fn stream_carries_terminal(bytes: &[u8]) -> bool {
    STREAM_TERMINAL_MARKERS
        .iter()
        .any(|marker| find_bytes(bytes, marker, 0).is_some())
}

/// Mark this exchange as truncated (idempotent) and bump the counters once.
fn mark_stream_truncated(ctx: &RequestCtx) -> bool {
    // Truncation is a *successful* stream that ended before its terminal
    // event. A 4xx/5xx body is a failure in its own right: there is no
    // terminal event to expect, and booking it here would mask the real
    // upstream error behind a "stream truncated" label in the request log
    // and the metrics.
    if ctx.upstream_status >= 400 {
        return false;
    }
    if ctx
        .stream_truncated
        .swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        return false;
    }
    METRICS
        .stream_truncated_total
        .with_label_values(&[&ctx.provider_name])
        .inc();
    if !ctx.client_body_written {
        METRICS
            .stream_truncated_before_first_byte_total
            .with_label_values(&[&ctx.provider_name])
            .inc();
    }
    warn!(
        provider = %ctx.provider_name,
        model = %ctx.model,
        client_body_written = ctx.client_body_written,
        "upstream stream ended before a terminal event — the 200 is recorded as truncated"
    );
    true
}

/// Observe one translated stream chunk: remember the terminal event, extract
/// usage, and — on the final chunk of a stream that never carried a terminal —
/// append an explicit error event so the client is not left with a silent
/// half-answer.
fn observe_stream_chunk(out: &mut Bytes, ctx: &mut RequestCtx, end_of_stream: bool) {
    if tracks_stream_terminal(ctx.endpoint_type) {
        // Scan the previous chunk's tail together with this chunk: an SSE
        // chunk boundary may fall anywhere, including inside the marker, and a
        // missed marker would silently downgrade a healthy stream to truncated.
        let mut window = Vec::with_capacity(ctx.stream_scan_tail.len() + out.len());
        window.extend_from_slice(&ctx.stream_scan_tail);
        window.extend_from_slice(out);
        if !ctx.stream_terminal_seen && stream_carries_terminal(&window) {
            ctx.stream_terminal_seen = true;
        }
        let keep = window.len().saturating_sub(STREAM_SCAN_TAIL);
        ctx.stream_scan_tail.clear();
        ctx.stream_scan_tail.extend_from_slice(&window[keep..]);
    }
    extract_stream_usage(out, ctx, end_of_stream);
    if !end_of_stream
        || ctx.stream_terminal_seen
        || !tracks_stream_terminal(ctx.endpoint_type)
        || ctx.upstream_status >= 400
    {
        return;
    }
    mark_stream_truncated(ctx);
    let event: &[u8] = if ctx.endpoint_type == EndpointType::Responses {
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"gateway_stream_truncated\",\"message\":\"upstream stream ended before response.completed; output is incomplete\",\"code\":\"stream_truncated\"}}\n\n"
    } else {
        b"data: {\"error\":{\"type\":\"gateway_stream_truncated\",\"message\":\"upstream stream ended before [DONE]; output is incomplete\",\"code\":\"stream_truncated\"}}\n\ndata: [DONE]\n\n"
    };
    let mut buf = out.to_vec();
    if !buf.ends_with(b"\n") {
        buf.push(b'\n');
    }
    buf.push(b'\n');
    buf.extend_from_slice(event);
    *out = Bytes::from(buf);
}

/// Extract token usage from a streaming SSE chunk. OpenAI chat streams carry
/// usage in the final chunk (`data: {…"usage":{…}…}`); Responses streams carry
/// it in the terminal `response.completed` / `response.incomplete` event (at
/// `.response.usage`). The scan is best-effort: only complete `data:` lines
/// are parsed, a usage line split across chunks is reassembled via
/// `ctx.sse_tail`, and a stream without any usage line changes nothing.
fn extract_stream_usage(chunk: &[u8], ctx: &mut RequestCtx, end_of_stream: bool) {
    // Fast path: a chunk that ends with a complete line and cannot carry a
    // usage payload leaves nothing to scan and no partial line to keep.
    if ctx.sse_tail.is_empty()
        && !chunk.windows(5).any(|w| w == b"usage")
        && chunk.last() == Some(&b'\n')
    {
        return;
    }

    // Reuse the tail buffer's allocation: prepend the trailing partial line of
    // the previous chunk, then scan the complete lines that follow.
    let mut buf = std::mem::take(&mut ctx.sse_tail);
    buf.extend_from_slice(chunk);

    let mut start = 0usize;
    let mut last_complete_end = 0usize;
    for (i, &b) in buf.iter().enumerate() {
        if b != b'\n' {
            continue;
        }
        let line = &buf[start..i];
        last_complete_end = i + 1;
        if line.starts_with(b"data:") && line.windows(5).any(|w| w == b"usage") {
            parse_usage_line(line, ctx);
        }
        start = i + 1;
    }

    // Recover the usage object directly from whatever we buffered on EVERY
    // call, not only when the stream ends. A terminal Responses event echoes
    // the whole output array, so its `data:` line can exceed the 16 KiB tail
    // cap (the prefix is dropped with the tail) or be split by raw newlines;
    // the usage object sits at the end of that line, so scanning for it here
    // catches it the moment it is fully present — including in the final data
    // chunk, which pingora delivers with `end_of_stream == false` and then
    // follows with a trailing `Body(None, true)` marker that carries no data.
    extract_usage_object(&buf, ctx);

    if end_of_stream {
        // A stream may end without a trailing newline — parse the final
        // partial line as the last event, then drop the tail.
        if last_complete_end < buf.len() {
            let line = &buf[last_complete_end..];
            if line.starts_with(b"data:") && line.windows(5).any(|w| w == b"usage") {
                parse_usage_line(line, ctx);
            }
        }
        ctx.sse_tail.clear();
    } else {
        let rest_len = buf.len() - last_complete_end;
        // Bounded: a stream that never emits complete lines (not SSE) must not
        // grow the tail without limit.
        if rest_len > 16 * 1024 {
            // The partial line already exceeds the cap and is about to be
            // dropped; recover any usage object it already contains in full
            // before that (the Responses terminal event's usage object sits at
            // the end of a huge data: line, so it may be fully present here
            // even though the line's `data:` prefix will be lost).
            let partial = &buf[last_complete_end..];
            extract_usage_object(partial, ctx);
            // Keep a bounded fragment of the dropped partial line so that a
            // usage object straddling this chunk boundary is still
            // reassembled: the `"usage":{` marker (and part of the object)
            // may sit in the dropped part while the closing brace only
            // arrives with the next chunk. Without this, that one request is
            // billed 0/0. The fragment is a rolling window over the huge
            // line, discarded as soon as the line ends with a newline.
            const CARRY: usize = 1024;
            let keep = CARRY.min(partial.len());
            ctx.sse_tail = partial[partial.len() - keep..].to_vec();
        } else {
            buf.copy_within(last_complete_end.., 0);
            buf.truncate(rest_len);
            ctx.sse_tail = buf;
        }
    }
}

/// Parse one SSE `data:` line (without the terminating newline) and copy any
/// token usage it carries into `ctx`.
fn parse_usage_line(line: &[u8], ctx: &mut RequestCtx) {
    let payload = line[5..].strip_prefix(b" ").unwrap_or(&line[5..]);
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) {
        let usage = value
            .get("usage")
            .or_else(|| value.get("response").and_then(|r| r.get("usage")));
        if let Some(u) = usage {
            copy_usage_tokens(u, ctx);
        }
    }
}

/// Copy token usage (and, once, the upstream cost) from a usage object into
/// `ctx`. Accepts both the chat `prompt_tokens` / `completion_tokens` and the
/// Responses `input_tokens` / `output_tokens` accounting.
fn copy_usage_tokens(usage: &serde_json::Value, ctx: &mut RequestCtx) {
    if let Some(pt) = usage.get("prompt_tokens").and_then(|v| v.as_u64()) {
        ctx.tokens_in = pt;
    }
    if let Some(ct) = usage.get("completion_tokens").and_then(|v| v.as_u64()) {
        ctx.tokens_out = ct;
    }
    // Responses accounting (input_tokens / output_tokens).
    if let Some(it) = usage.get("input_tokens").and_then(|v| v.as_u64()) {
        ctx.tokens_in = it;
    }
    if let Some(ot) = usage.get("output_tokens").and_then(|v| v.as_u64()) {
        ctx.tokens_out = ot;
    }
    if ctx.upstream_cost_usd.is_none() {
        ctx.upstream_cost_usd = extract_usage_cost(usage);
    }
    copy_cached_tokens(usage, ctx);
}

/// Copy the prompt-cache hit count out of a usage block.
///
/// Three shapes are in the wild and all of them report the cached part
/// *inside* the input count, never on top of it:
///   * chat completions / MiniMax / one-api: `prompt_tokens_details.cached_tokens`
///   * Responses: `input_tokens_details.cached_tokens`
///   * Anthropic messages: `cache_read_input_tokens`
fn copy_cached_tokens(usage: &serde_json::Value, ctx: &mut RequestCtx) {
    let cached = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
        .or_else(|| usage.get("cache_read_input_tokens"))
        .and_then(|v| v.as_u64());
    if let Some(cached) = cached {
        ctx.cached_tokens_in = cached;
    }
}

/// Find the byte index of the first occurrence of `needle` in `haystack`
/// at or after `from`.
fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from >= haystack.len() || needle.is_empty() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|i| from + i)
}

/// Find the index of the `}` that closes the JSON object opened at `open`
/// (`buf[open] == b'{'`), skipping string contents and backslash escapes.
/// Returns `None` when the object is not yet complete in `buf`.
fn find_json_object_end(buf: &[u8], open: usize) -> Option<usize> {
    if buf.get(open) != Some(&b'{') {
        return None;
    }
    let mut depth = 0u32;
    let mut in_string = false;
    let mut escaped = false;
    for (i, &b) in buf.iter().enumerate().skip(open) {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Recover token usage from a buffer that may only hold the TAIL of an SSE
/// event. The Responses API's terminal `response.completed` event carries the
/// whole output array, so its `data:` line can exceed the streaming tail cap
/// and lose its `data:` prefix; the usage object itself sits at the very end
/// of that line and is small. Scans for every `"usage":{` object and copies
/// the tokens of the last one that parses. Occurrences inside JSON strings
/// (e.g. tool descriptions) either fail to parse or carry no token fields, so
/// they cannot corrupt the accounting.
fn extract_usage_object(buf: &[u8], ctx: &mut RequestCtx) {
    let marker = b"\"usage\":{";
    let mut pos = 0;
    while let Some(rel) = find_bytes(buf, marker, pos) {
        let open = rel + marker.len() - 1; // index of the '{'
        if let Some(close) = find_json_object_end(buf, open) {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&buf[open..=close]) {
                copy_usage_tokens(&value, ctx);
            }
        }
        pos = rel + marker.len();
    }
}

/// Extract the upstream-reported cost, if any, from a usage block.
/// opencode.ai's zen endpoint reports the actual charge in
/// `usage.opencode.allowance` (falling back to `list_cost`, then `cost`).
fn extract_usage_cost(usage: &serde_json::Value) -> Option<f64> {
    if let Some(opencode) = usage.get("opencode") {
        for key in ["allowance", "list_cost"] {
            if let Some(v) = opencode.get(key).and_then(|v| v.as_f64()) {
                if v > 0.0 {
                    return Some(v);
                }
            }
        }
    }
    usage
        .get("cost")
        .and_then(|v| v.as_f64())
        .filter(|&c| c > 0.0)
}

/// Extract token usage from a response body.
fn extract_usage(body: &serde_json::Value, ctx: &mut RequestCtx) {
    // One parser for both paths: the streaming and the buffered response carry
    // the same usage object, and keeping two copies is how the cached-token
    // field would end up read on one path and ignored on the other.
    if let Some(usage) = body.get("usage") {
        copy_usage_tokens(usage, ctx);
    }

    // Extract model from response if not set
    if ctx.model.is_empty() {
        if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
            ctx.model = model.to_string();
        }
    }
}

#[cfg(test)]
mod unknown_model_body_tests {
    use super::body_reports_unknown_model;

    #[test]
    fn vllm_alias_rejection_is_an_unknown_model() {
        assert!(body_reports_unknown_model(
            r#"{"error":{"message":"The model `deepseek-v4.1-flash:cloud` does not exist. Valid aliases: minicpmv47-awq."}}"#
        ));
    }

    #[test]
    fn one_api_rejection_is_an_unknown_model() {
        assert!(body_reports_unknown_model(
            r#"{"error":{"message":"invalid params, unknown model 'x'"}}"#
        ));
        assert!(body_reports_unknown_model(
            r#"{"error":{"code":"model_not_found","message":"Model not found"}}"#
        ));
    }

    #[test]
    fn a_missing_endpoint_is_not_an_unknown_model() {
        assert!(!body_reports_unknown_model("404 page not found"));
        assert!(!body_reports_unknown_model(
            r#"{"error":{"message":"Not Found","code":404}}"#
        ));
        assert!(!body_reports_unknown_model(""));
    }

    #[test]
    fn weak_phrasings_need_a_model_mention() {
        // "not found" alone is an endpoint 404; naming the model makes it a
        // model rejection.
        assert!(!body_reports_unknown_model("resource not found"));
        assert!(body_reports_unknown_model(
            r#"{"error":{"message":"model `x` not found"}}"#
        ));
        assert!(body_reports_unknown_model(
            r#"{"error":{"message":"请求的模型不存在"}}"#
        ));
    }
}

#[cfg(test)]
mod connect_timeout_tests {
    use super::*;
    use maxllm_config::Config;

    fn hot_state(toml: &str) -> HotState {
        let config = Config::parse(toml).unwrap();
        build_hot_state(&config).unwrap()
    }

    const BASE: &str = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
"#;

    #[test]
    fn test_default_connect_timeout_is_ten_seconds() {
        let state = hot_state(BASE);
        let p = &state.providers["opencode"];
        assert_eq!(p.connect_timeout, Duration::from_secs(10));
        assert_eq!(p.total_connect_timeout, None);
    }

    #[test]
    fn test_connect_timeout_is_configurable() {
        let state = hot_state(&format!(
            "{BASE}connect_timeout_secs = 30\ntotal_connect_timeout_secs = 60\n"
        ));
        let p = &state.providers["opencode"];
        assert_eq!(p.connect_timeout, Duration::from_secs(30));
        assert_eq!(p.total_connect_timeout, Some(Duration::from_secs(60)));
    }
}

#[cfg(test)]
mod commandcode_tests {
    use super::*;
    use maxllm_config::Config;

    fn hot_state(toml: &str) -> HotState {
        let config = Config::parse(toml).unwrap();
        build_hot_state(&config).unwrap()
    }

    const BASE: &str = r#"
[server]
listen = "0.0.0.0:8080"

[providers.cmd]
kind = "commandcode"
base_url = "https://api.commandcode.ai"
api_key = "cmd-test"
default_model = "deepseek/deepseek-v4.1-flash"
"#;

    #[test]
    fn test_default_paths_are_the_vendor_routes() {
        let state = hot_state(BASE);
        let p = &state.providers["cmd"];
        assert_eq!(
            p.translator.upstream_path(),
            "/provider/v1/chat/completions"
        );
        // The Responses route is the vendor's own, not the OpenAI default.
        assert_eq!(p.responses_path.as_deref(), Some("/provider/v1/responses"));
    }

    #[test]
    fn test_responses_is_native_by_default() {
        let state = hot_state(BASE);
        let p = &state.providers["cmd"];
        assert!(
            p.native_responses,
            "commandcode.ai serves /provider/v1/responses — it must be native"
        );
    }

    #[test]
    fn test_native_responses_can_be_turned_off() {
        let state = hot_state(&format!("{BASE}native_responses = false\n"));
        let p = &state.providers["cmd"];
        assert!(!p.native_responses);
        assert_eq!(p.responses_path, None);
    }

    #[test]
    fn test_explicit_upstream_path_drives_the_responses_path() {
        let state = hot_state(&format!(
            "{BASE}upstream_path = \"/provider/v1/chat/completions\"\n"
        ));
        let p = &state.providers["cmd"];
        assert_eq!(p.responses_path.as_deref(), Some("/provider/v1/responses"));
    }

    #[test]
    fn test_messages_route_is_not_native_responses() {
        // A provider entry pointed at the Anthropic Messages route serves the
        // vendor's Claude models; that route has no Responses sibling.
        let state = hot_state(&format!(
            "{BASE}upstream_path = \"/provider/v1/messages\"\n"
        ));
        let p = &state.providers["cmd"];
        assert_eq!(p.translator.upstream_path(), "/provider/v1/messages");
        assert!(!p.native_responses);
        assert_eq!(p.responses_path, None);
    }

    #[test]
    fn test_bearer_auth_is_injected() {
        let state = hot_state(BASE);
        let p = &state.providers["cmd"];
        let headers = p.translator.upstream_headers(&p.api_key);
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer cmd-test"));
    }

    #[test]
    fn test_models_list_advertises_only_the_default_model() {
        let state = hot_state(BASE);
        let body: serde_json::Value = serde_json::from_slice(&state.models_response).unwrap();
        let ids: Vec<&str> = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|m| m["id"].as_str())
            .collect();
        assert!(ids.contains(&"deepseek/deepseek-v4.1-flash"), "{ids:?}");
        let owned: Vec<&str> = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["id"] == "deepseek/deepseek-v4.1-flash")
            .filter_map(|m| m["owned_by"].as_str())
            .collect();
        assert_eq!(owned, vec!["command-code"]);
    }

    #[test]
    fn test_native_messages_path_is_rewritten_onto_the_vendor_route() {
        // A client that speaks the plain Anthropic Messages contract.
        assert_eq!(
            native_upstream_path("/v1/messages", "/provider/v1/messages"),
            "/provider/v1/messages"
        );
        // An Anthropic provider serves the client's own path.
        assert_eq!(
            native_upstream_path("/v1/messages", "/v1/messages"),
            "/v1/messages"
        );
        // Gemini's native path is not a Messages route.
        assert_eq!(
            native_upstream_path(
                "/v1beta/models/gemini-2.5-flash:generateContent",
                "/v1/messages"
            ),
            "/v1beta/models/gemini-2.5-flash:generateContent"
        );
    }
}

#[cfg(test)]
mod route_timeout_tests {
    use super::*;
    use maxllm_config::Config;

    const BASE: &str = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
"#;

    fn routes_from(toml: &str) -> Vec<RouteConfig> {
        Config::parse(toml).unwrap().routes
    }

    #[test]
    fn test_unmatched_route_has_no_timeout() {
        let routes = routes_from(BASE);
        assert_eq!(route_timeout_secs(&routes, None), None);
        // Index outside the routes array is the same as unmatched.
        assert_eq!(route_timeout_secs(&routes, Some(3)), None);
    }

    #[test]
    fn test_unset_timeout_keeps_the_hardcoded_default() {
        let toml = format!(
            "{BASE}[[routes]]\npath = \"/v1/responses/opencode_go_1\"\nprovider = \"opencode\"\n"
        );
        let routes = routes_from(&toml);
        assert_eq!(route_timeout_secs(&routes, Some(0)), None);
    }

    #[test]
    fn test_route_timeout_is_honoured() {
        let toml = format!(
            "{BASE}[[routes]]\npath = \"/v1/responses/opencode_go_1\"\nprovider = \"opencode\"\ntimeout_secs = 300\n"
        );
        let routes = routes_from(&toml);
        assert_eq!(
            route_timeout_secs(&routes, Some(0)),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn test_zero_timeout_is_treated_as_unset() {
        let toml =
            format!("{BASE}[[routes]]\npath = \"/a\"\nprovider = \"opencode\"\ntimeout_secs = 0\n");
        let routes = routes_from(&toml);
        assert_eq!(route_timeout_secs(&routes, Some(0)), None);
    }
}

#[cfg(test)]
mod early_sse_tests {
    use super::*;

    #[test]
    fn test_chat_sse_is_detected() {
        // A chat completion chunk is identified by its object marker.
        let chat = b"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n";
        assert!(looks_like_chat_sse(chat));
        // A chat (non-stream) JSON response is not an SSE stream.
        assert!(!looks_like_chat_sse(
            br#"{"object":"chat.completion","choices":[]}"#
        ));
    }

    #[test]
    fn test_responses_sse_is_not_detected_as_chat() {
        // A Responses SSE event carries `type`, never `chat.completion`.
        let responses =
            b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{}}\n\n";
        assert!(!looks_like_chat_sse(responses));
    }

    #[test]
    fn test_responses_sse_is_detected() {
        // A native upstream answers with Responses events; the terminal
        // event name is what tells its stream apart from a chat stream.
        let responses = b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{}}\n\n";
        assert!(looks_like_responses_sse(responses));
        // A chat stream is not a Responses stream.
        let chat = b"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n";
        assert!(!looks_like_responses_sse(chat));
        // A plain JSON body is not a stream either.
        assert!(!looks_like_responses_sse(
            br#"{"object":"response","output":[]}"#
        ));
    }
}

#[cfg(test)]
mod stream_usage_tests {
    use super::*;

    fn ctx() -> RequestCtx {
        RequestCtx::new()
    }

    #[test]
    fn test_chat_stream_usage_is_extracted() {
        let mut c = ctx();
        let chunk = b"data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":31,\"completion_tokens\":7,\"total_tokens\":38}}\n\ndata: [DONE]\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 31);
        assert_eq!(c.tokens_out, 7);
    }

    /// The prompt-cache hit count arrives in three shapes, always *inside*
    /// the input count. Losing it is what makes a cost figure quote list price
    /// for tokens the vendor billed at a tenth of it.
    #[test]
    fn test_cached_input_tokens_are_extracted_on_every_shape() {
        let json = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap();

        // chat completions (also MiniMax / one-api behind a chat endpoint)
        let mut c = ctx();
        copy_usage_tokens(
            &json(
                r#"{"prompt_tokens":1000,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":896}}"#,
            ),
            &mut c,
        );
        assert_eq!(
            (c.tokens_in, c.tokens_out, c.cached_tokens_in),
            (1000, 5, 896)
        );

        // Responses
        let mut c = ctx();
        copy_usage_tokens(
            &json(
                r#"{"input_tokens":101,"output_tokens":7,"input_tokens_details":{"cached_tokens":96}}"#,
            ),
            &mut c,
        );
        assert_eq!((c.tokens_in, c.cached_tokens_in), (101, 96));

        // Anthropic messages
        let mut c = ctx();
        copy_usage_tokens(
            &json(r#"{"input_tokens":20,"output_tokens":3,"cache_read_input_tokens":18}"#),
            &mut c,
        );
        assert_eq!((c.tokens_in, c.cached_tokens_in), (20, 18));

        // A provider that reports no cache detail leaves the count at zero
        // rather than inventing one.
        let mut c = ctx();
        copy_usage_tokens(
            &json(r#"{"prompt_tokens":10,"completion_tokens":1}"#),
            &mut c,
        );
        assert_eq!(c.cached_tokens_in, 0);
    }

    /// The buffered path must book the same numbers as the streaming one — they
    /// used to be two hand-copied parsers.
    #[test]
    fn test_buffered_and_streamed_usage_agree() {
        let mut buffered = ctx();
        extract_usage(
            &serde_json::from_str::<serde_json::Value>(
                r#"{"model":"MiniMax-M3","usage":{"prompt_tokens":500,"completion_tokens":9,"prompt_tokens_details":{"cached_tokens":480}}}"#,
            )
            .unwrap(),
            &mut buffered,
        );
        let mut streamed = ctx();
        copy_usage_tokens(
            &serde_json::from_str::<serde_json::Value>(
                r#"{"prompt_tokens":500,"completion_tokens":9,"prompt_tokens_details":{"cached_tokens":480}}"#,
            )
            .unwrap(),
            &mut streamed,
        );
        assert_eq!(buffered.tokens_in, streamed.tokens_in);
        assert_eq!(buffered.tokens_out, streamed.tokens_out);
        assert_eq!(buffered.cached_tokens_in, streamed.cached_tokens_in);
        assert_eq!(buffered.model, "MiniMax-M3");
    }

    #[test]
    fn test_responses_stream_usage_is_extracted() {
        // The Responses contract nests usage under `response`.
        let mut c = ctx();
        let chunk = b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":4,\"total_tokens\":16}}}\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 12);
        assert_eq!(c.tokens_out, 4);
    }

    #[test]
    fn test_usage_split_across_chunks_is_reassembled() {
        let mut c = ctx();
        let first = b"data: {\"id\":\"1\",\"usage\":{\"prompt_tok";
        extract_stream_usage(first, &mut c, false);
        assert_eq!(c.tokens_in, 0);
        let second = b"ens\":5,\"completion_tokens\":2}}\n\n";
        extract_stream_usage(second, &mut c, true);
        assert_eq!(c.tokens_in, 5);
        assert_eq!(c.tokens_out, 2);
    }

    #[test]
    fn test_usage_split_inside_the_usage_word_is_reassembled() {
        // A chunk boundary can land anywhere, including inside the word
        // "usage" itself — the first part then carries no "usage" substring.
        let mut c = ctx();
        let first = b"data: {\"id\":\"1\",\"us";
        extract_stream_usage(first, &mut c, false);
        assert_eq!(c.tokens_in, 0);
        let second = b"age\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n";
        extract_stream_usage(second, &mut c, true);
        assert_eq!(c.tokens_in, 5);
        assert_eq!(c.tokens_out, 2);
    }

    #[test]
    fn test_final_partial_line_parsed_at_end_of_stream() {
        // Some upstreams emit the final usage event without a trailing
        // newline; the tail must still be parsed once the stream ends.
        let mut c = ctx();
        let chunk = b"data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 7);
        assert_eq!(c.tokens_out, 3);
    }

    #[test]
    fn test_stream_without_usage_leaves_tokens_zero() {
        let mut c = ctx();
        let chunk = b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 0);
        assert_eq!(c.tokens_out, 0);
    }

    #[test]
    fn test_malformed_usage_line_is_ignored() {
        let mut c = ctx();
        let chunk = b"data: {not json}\n\ndata: [DONE]\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 0);
    }

    #[test]
    fn test_extract_usage_cost_prefers_allowance() {
        let u = serde_json::json!({"prompt_tokens":1,"output_tokens":1,"cost":0,
            "opencode":{"allowance":0.00000747,"list_cost":0.00001245}});
        assert_eq!(extract_usage_cost(&u), Some(0.00000747));
    }

    #[test]
    fn test_extract_usage_cost_falls_back_to_list_cost() {
        let u = serde_json::json!({"prompt_tokens":1,"output_tokens":1,
            "opencode":{"allowance":0,"list_cost":0.00001245}});
        assert_eq!(extract_usage_cost(&u), Some(0.00001245));
    }

    #[test]
    fn test_extract_usage_cost_ignores_zero() {
        let u = serde_json::json!({"prompt_tokens":1,"output_tokens":1,"cost":0,
            "opencode":{"allowance":0,"list_cost":0}});
        assert_eq!(extract_usage_cost(&u), None);
    }

    #[test]
    fn test_extract_usage_records_upstream_cost() {
        let mut c = ctx();
        let body = serde_json::json!({"model":"m","usage":{"prompt_tokens":1,"output_tokens":1,
            "opencode":{"allowance":0.00000747,"list_cost":0.00001245}}});
        extract_usage(&body, &mut c);
        assert_eq!(c.tokens_in, 1);
        assert_eq!(c.tokens_out, 1);
        assert_eq!(c.upstream_cost_usd, Some(0.00000747));
    }

    #[test]
    fn test_usage_survives_multiple_events() {
        // Only the chunk carrying the usage block sets the counters; later
        // events without usage must not reset them.
        let mut c = ctx();
        extract_stream_usage(
            b"data: {\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":3}}\n\n",
            &mut c,
            false,
        );
        extract_stream_usage(b"data: {\"choices\":[]}\n\n", &mut c, true);
        assert_eq!(c.tokens_in, 9);
        assert_eq!(c.tokens_out, 3);
    }
    #[test]
    fn test_giant_responses_terminal_event_usage_is_extracted() {
        // The Responses API's terminal event echoes the whole output array, so
        // its `data:` line is far larger than the 16 KiB streaming tail cap:
        // the line's `data:` prefix is dropped with the tail and the
        // line-oriented scan can never see it again. The usage object sits at
        // the very end of the line and must still be recovered.
        let mut c = ctx();
        let huge = "x".repeat(120 * 1024);
        let line = format!(
            "data: {{\"response\":{{\"id\":\"r1\",\"output\":[\"{}\"],\"usage\":{{\"input_tokens\":26356,\"output_tokens\":3,\"total_tokens\":26359,\"cost\":0,\"opencode\":{{\"allowance\":0.00391757}}}}}},\"type\":\"response.completed\"}}\n\n",
            huge
        );
        assert!(line.len() > 16 * 1024);
        let bytes = line.as_bytes();

        // Feed it the way pingora delivers it: 16 KiB pieces. The last DATA
        // chunk arrives with end_of_stream == false and is followed by a
        // trailing end-of-stream marker that carries no data — the usage
        // object must be recovered from the data chunk itself.
        let mut pos = 0;
        while pos < bytes.len() {
            let end = (pos + 16 * 1024).min(bytes.len());
            extract_stream_usage(&bytes[pos..end], &mut c, false);
            pos = end;
        }
        extract_stream_usage(b"", &mut c, true);
        assert_eq!(c.tokens_in, 26356);
        assert_eq!(c.tokens_out, 3);
    }

    #[test]
    fn test_usage_is_recovered_from_a_data_chunk_before_the_end_marker() {
        // pingora delivers the final data chunk with end_of_stream == false
        // and then a trailing Body(None, true) marker. The usage object lives
        // in that data chunk, so it has to be picked up there.
        let mut c = ctx();
        let chunk = b"event: response.completed\ndata: {\"response\":{\"usage\":{\"input_tokens\":9,\"output_tokens\":4}}}\n\n";
        extract_stream_usage(chunk, &mut c, false);
        assert_eq!(c.tokens_in, 9);
        assert_eq!(c.tokens_out, 4);
        // The trailing end-of-stream marker carries no data and changes nothing.
        extract_stream_usage(b"", &mut c, true);
        assert_eq!(c.tokens_in, 9);
        assert_eq!(c.tokens_out, 4);
    }

    #[test]
    fn test_giant_terminal_event_usage_is_found_without_the_data_prefix() {
        // Same scenario, but the tail cap has already dropped the line's
        // `data:` prefix, so only the tail of the event is buffered.
        let mut c = ctx();
        let mut buf = Vec::new();
        buf.extend_from_slice(b"data: {\"response\":{\"output\":[");
        buf.extend(std::iter::repeat_n(b'y', 40 * 1024));
        buf.extend_from_slice(
            b"],\"usage\":{\"input_tokens\":7,\"output_tokens\":2}},\"type\":\"response.completed\"}\n\n",
        );
        // Skip the first 16 KiB: that is what the tail cap drops.
        let mut pos = 16 * 1024;
        while pos < buf.len() {
            let end = (pos + 16 * 1024).min(buf.len());
            let eos = end == buf.len();
            extract_stream_usage(&buf[pos..end], &mut c, eos);
            pos = end;
        }
        assert_eq!(c.tokens_in, 7);
        assert_eq!(c.tokens_out, 2);
    }

    #[test]
    fn test_usage_object_split_across_a_chunk_boundary_is_reassembled() {
        // The giant terminal event's `data:` line exceeds the 16 KiB tail
        // cap, so the tail that would carry the `"usage":{` marker is
        // dropped. When the chunk boundary lands INSIDE the usage object,
        // the marker sits in the dropped part and the closing brace only
        // arrives with the next chunk — the counters must still be
        // recovered, otherwise this request is billed as 0/0.
        let mut c = ctx();
        let line = format!(
            "data: {{\"response\":{{\"output\":[\"{}\"],\"usage\":{{\"input_tokens\":26356,\"output_tokens\":3}}}},\"type\":\"response.completed\"}}\n\n",
            "x".repeat(120 * 1024)
        );
        let bytes = line.as_bytes();
        // Split inside the usage object: the marker is before the split,
        // the closing brace after it.
        let split = bytes.len() - 60;
        extract_stream_usage(&bytes[..split], &mut c, false);
        assert_eq!(c.tokens_in, 0, "usage is not complete in the first chunk");
        extract_stream_usage(&bytes[split..], &mut c, false);
        assert_eq!(c.tokens_in, 26356);
        assert_eq!(c.tokens_out, 3);
    }

    #[test]
    fn test_usage_marker_split_across_a_chunk_boundary_is_reassembled() {
        // Same scenario, but the boundary lands inside the word "usage"
        // itself, so neither chunk carries the full `"usage":{` marker.
        let mut c = ctx();
        let line = format!(
            "data: {{\"response\":{{\"output\":[\"{}\"],\"usage\":{{\"input_tokens\":11,\"output_tokens\":5}}}},\"type\":\"response.completed\"}}\n\n",
            "x".repeat(120 * 1024)
        );
        let bytes = line.as_bytes();
        let marker = find_bytes(bytes, b"\"usage\":{", 0).expect("marker present");
        let split = marker + 4; // inside the word "usage"
        extract_stream_usage(&bytes[..split], &mut c, false);
        assert_eq!(c.tokens_in, 0);
        extract_stream_usage(&bytes[split..], &mut c, false);
        assert_eq!(c.tokens_in, 11);
        assert_eq!(c.tokens_out, 5);
    }

    #[test]
    fn test_usage_marker_inside_a_string_is_ignored() {
        // A tool description may literally contain `"usage":{`. Such an
        // occurrence is not a usage object and must not corrupt the counts.
        let mut c = ctx();
        let chunk = b"data: {\"description\":\"see \"usage\":{\"x\":1} here\",\"usage\":{\"input_tokens\":4,\"output_tokens\":1}}\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 4);
        assert_eq!(c.tokens_out, 1);
    }
}

#[cfg(test)]
mod failover_policy_tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreaker;

    fn provider(default_model: Option<&str>) -> ProviderState {
        ProviderState {
            kind: maxllm_config::ProviderKind::OpenaiCompat,
            translator: Box::new(
                maxllm_translate::openai_compat::OpenAICompatTranslator::new(
                    "p".to_string(),
                    "/v1/chat/completions".to_string(),
                    maxllm_translate::openai_compat::AuthStyle::Bearer,
                ),
            ),
            circuit_breaker: Arc::new(CircuitBreaker::new(3, 60)),
            api_key: "k".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9,
            tls: false,
            sni: "127.0.0.1".to_string(),
            region: None,
            weight: 100,
            tags: vec![],
            default_model: default_model.map(|m| m.to_string()),
            native_responses: false,
            responses_path: None,
            connect_timeout: Duration::from_secs(10),
            total_connect_timeout: None,
            utc_offset_minutes: 0,
        }
    }

    fn ctx(model: &str) -> RequestCtx {
        let mut c = RequestCtx::new();
        c.model = model.to_string();
        c
    }

    #[test]
    fn failover_eligible_statuses() {
        let p = provider(Some("MiniMax-M3"));
        for status in [
            401u16, 402, 403, 404, 405, 408, 409, 410, 425, 429, 500, 502, 503, 599,
        ] {
            assert!(
                should_failover_status(status, &ctx("anything"), &p),
                "status {status} must be failover-eligible"
            );
        }
    }

    #[test]
    fn success_and_redirect_statuses_never_fail_over() {
        let p = provider(Some("MiniMax-M3"));
        for status in [200u16, 201, 204, 301, 302, 304, 307, 308] {
            assert!(!should_failover_status(status, &ctx("anything"), &p));
        }
    }

    #[test]
    fn bad_request_with_model_mismatch_fails_over() {
        // The pool sent a model this provider does not carry — one-api answers
        // 400 "unknown model". The provider's own default differs from the
        // request, so it is treated as a model-capability failure.
        let p = provider(Some("MiniMax-M3"));
        assert!(should_failover_status(400, &ctx("deepseek-v4-flash"), &p));
        assert!(should_failover_status(422, &ctx("GLM-5.3"), &p));
    }

    #[test]
    fn bad_request_with_matching_model_stays() {
        // The provider carries the requested model (its default matches), so a
        // 400 is a genuine request fault every provider would share.
        let p = provider(Some("MiniMax-M3"));
        assert!(!should_failover_status(400, &ctx("MiniMax-M3"), &p));
        assert!(!should_failover_status(422, &ctx("MiniMax-M3"), &p));
    }

    #[test]
    fn bad_request_with_unknown_model_stays_conservative() {
        // A provider without a declared default gives no signal about which
        // models it carries — a 400 is treated as a request fault.
        let p = provider(None);
        assert!(!should_failover_status(400, &ctx("anything"), &p));
    }

    #[test]
    fn bad_request_before_model_is_known_stays() {
        // The request body has not been read yet (early answer race) — no
        // model signal, so a 400 stays.
        let p = provider(Some("MiniMax-M3"));
        assert!(!should_failover_status(400, &ctx(""), &p));
    }
}

#[cfg(test)]
mod helper_function_tests {
    use super::*;
    use maxllm_config::ProviderKind;

    /// The logging guard must fire for a context that never got a route, and
    /// only for that: a routed request keeps its journal row.
    #[test]
    fn unrouted_session_predicate_needs_both_a_route_and_a_provider() {
        let ctx = RequestCtx::new();
        assert!(
            is_unrouted_session(&ctx),
            "a fresh context has not been routed"
        );

        let mut routed = RequestCtx::new();
        routed.route_index = Some(0);
        assert!(!is_unrouted_session(&routed), "route matched");

        let mut selected = RequestCtx::new();
        selected.provider_name = "mock_openai".into();
        assert!(!is_unrouted_session(&selected), "provider selected");
    }

    /// Every provider kind maps to a non-empty `owned_by` label.
    #[test]
    fn test_provider_owned_by_labels() {
        for (kind, expected) in [
            (ProviderKind::OpenAI, "openai"),
            (ProviderKind::Anthropic, "anthropic"),
            (ProviderKind::Gemini, "google"),
            (ProviderKind::AzureOpenai, "azure-openai"),
            (ProviderKind::Bedrock, "aws-bedrock"),
            (ProviderKind::Groq, "groq"),
            (ProviderKind::Together, "together"),
            (ProviderKind::Fireworks, "fireworks"),
            (ProviderKind::DeepInfra, "deepinfra"),
            (ProviderKind::Mistral, "mistral"),
            (ProviderKind::XAI, "xai"),
            (ProviderKind::DeepSeek, "deepseek"),
            (ProviderKind::Ollama, "ollama"),
            (ProviderKind::Cohere, "cohere"),
            (ProviderKind::OpenaiCompat, "custom"),
            (ProviderKind::OpenCode, "opencode"),
        ] {
            assert_eq!(provider_owned_by(kind), expected, "{kind:?}");
        }
    }

    /// The circuit breaker registry hands out the same breaker for a name.
    #[test]
    fn test_existing_breaker_registry() {
        let name = "maxllm-test-breaker-registry";
        assert!(
            existing_breaker(name).is_none(),
            "unknown name has no breaker"
        );
        let breaker = breaker_for(name, 3, 60);
        let found = existing_breaker(name).expect("registered breaker");
        assert!(
            Arc::ptr_eq(&breaker, &found),
            "the registry hands out the same breaker"
        );
    }

    /// The model id is taken from the `/models/<id>` path segment.
    #[test]
    fn test_model_from_request_path() {
        assert_eq!(
            model_from_request_path("/v1/models/gpt-4o"),
            Some("gpt-4o".to_string())
        );
        // The id ends at the first `:`, `/` or `?`.
        assert_eq!(
            model_from_request_path("/v1/models/gpt-4o:stream"),
            Some("gpt-4o".to_string())
        );
        assert_eq!(
            model_from_request_path("/v1/models/gpt-4o/chat"),
            Some("gpt-4o".to_string())
        );
        assert_eq!(
            model_from_request_path("/v1/models/gpt-4o?x=1"),
            Some("gpt-4o".to_string())
        );
        // No marker, or an empty id.
        assert_eq!(model_from_request_path("/v1/models"), None);
        assert_eq!(model_from_request_path("/v1/models/"), None);
        assert_eq!(model_from_request_path("/v1/chat/completions"), None);
    }

    /// `Retry-After` is either delay-seconds or an HTTP date.
    #[test]
    fn test_parse_retry_after_secs() {
        assert_eq!(parse_retry_after_secs("120"), Some(120));
        assert_eq!(parse_retry_after_secs("  45  "), Some(45));
        assert_eq!(parse_retry_after_secs("not-a-date"), None);

        // A future HTTP date becomes the remaining seconds.
        let future = chrono::Utc::now() + chrono::Duration::seconds(60);
        let secs = parse_retry_after_secs(&future.to_rfc2822()).expect("http date");
        assert!((55..=60).contains(&secs), "about a minute left: {secs}");

        // A past HTTP date never goes negative.
        let past = chrono::Utc::now() - chrono::Duration::seconds(600);
        assert_eq!(parse_retry_after_secs(&past.to_rfc2822()), Some(0));
    }

    /// Provider error bodies are normalised to the OpenAI error envelope.
    #[test]
    fn test_normalize_anthropic_error() {
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"Rate limit reached"}}"#;
        let out = normalize_error_response(body, 429, ProviderKind::Anthropic).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["error"]["message"], "Rate limit reached");
        assert_eq!(v["error"]["type"], "rate_limit_error");
        assert_eq!(v["error"]["code"], 429);
    }

    #[test]
    fn test_normalize_bedrock_error() {
        let body = br#"{"type":"error","error":{"type":"throttlingException","message":"Too many tokens"}}"#;
        let out = normalize_error_response(body, 400, ProviderKind::Bedrock).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["error"]["message"], "Too many tokens");
        assert_eq!(v["error"]["type"], "throttlingException");
        assert_eq!(v["error"]["code"], 400);
    }

    #[test]
    fn test_normalize_cohere_error() {
        let body = br#"{"message":"Cohere daily limit"}"#;
        let out = normalize_error_response(body, 429, ProviderKind::Cohere).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["error"]["message"], "Cohere daily limit");
        assert_eq!(v["error"]["type"], "api_error");
        assert_eq!(v["error"]["code"], 429);
    }

    #[test]
    fn test_normalize_gemini_error() {
        let body =
            br#"{"error":{"code":429,"message":"Quota exceeded","status":"RESOURCE_EXHAUSTED"}}"#;
        let out = normalize_error_response(body, 429, ProviderKind::Gemini).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["error"]["message"], "Quota exceeded");
        assert_eq!(v["error"]["type"], "RESOURCE_EXHAUSTED");
        assert_eq!(v["error"]["code"], 429);
    }

    /// A body that already carries the OpenAI envelope is passed through.
    #[test]
    fn test_normalize_openai_error_passes_through() {
        let body = br#"{"error":{"message":"m","type":"t","code":7}}"#;
        assert!(normalize_error_response(body, 400, ProviderKind::OpenAI).is_none());
        // A body that is not JSON is passed through as well.
        assert!(normalize_error_response(b"not json", 400, ProviderKind::OpenAI).is_none());
    }

    /// `find_bytes` honours its bounds.
    #[test]
    fn test_find_bytes_bounds() {
        assert_eq!(find_bytes(b"abcabc", b"abc", 0), Some(0));
        assert_eq!(find_bytes(b"abcabc", b"abc", 3), Some(3));
        assert_eq!(find_bytes(b"abcabc", b"abc", 1), Some(3));
        assert_eq!(find_bytes(b"abc", b"xyz", 0), None);
        // `from` past the end and an empty needle are both "not found".
        assert_eq!(find_bytes(b"abc", b"a", 3), None);
        assert_eq!(find_bytes(b"abc", b"", 0), None);
    }

    /// The upstream cost may also sit in a plain `cost` field.
    #[test]
    fn test_extract_usage_cost_plain_cost_field() {
        let u = serde_json::json!({"prompt_tokens":1,"cost":0.00001245});
        assert_eq!(extract_usage_cost(&u), Some(0.00001245));
        // Zero and negative costs are not costs.
        assert_eq!(extract_usage_cost(&serde_json::json!({"cost":0})), None);
        assert_eq!(extract_usage_cost(&serde_json::json!({"cost":-1.0})), None);
        assert_eq!(
            extract_usage_cost(&serde_json::json!({"cost":"free"})),
            None
        );
    }

    /// A `data:` line is parsed for its usage payload directly.
    #[test]
    fn test_parse_usage_line_directly() {
        let mut c = RequestCtx::new();
        parse_usage_line(
            b"data: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5}}",
            &mut c,
        );
        assert_eq!(c.tokens_in, 11);
        assert_eq!(c.tokens_out, 5);

        // The Responses envelope nests the usage under `response`.
        let mut c = RequestCtx::new();
        parse_usage_line(
            b"data: {\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}",
            &mut c,
        );
        assert_eq!(c.tokens_in, 3);
        assert_eq!(c.tokens_out, 2);

        // A line that is not JSON changes nothing.
        let mut c = RequestCtx::new();
        parse_usage_line(b"data: {not json}", &mut c);
        assert_eq!(c.tokens_in, 0);
    }

    /// A usage line that is not the first one in the chunk is still found.
    #[test]
    fn test_usage_in_a_later_sse_line_is_extracted() {
        let mut c = RequestCtx::new();
        let chunk = b"data: {\"id\":\"1\",\"choices\":[]}\ndata: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 3);
        assert_eq!(c.tokens_out, 1);
    }

    /// A final partial line that is not a `data:` line is not parsed by the
    /// line path. The usage object here uses a space after the colon, so the
    /// raw `"usage":{` marker scan cannot see it either — only a data-line
    /// parse could, and this line is not a data line.
    #[test]
    fn test_final_partial_line_must_be_a_data_line() {
        let mut c = RequestCtx::new();
        let chunk = b"xxxxx{\"usage\": {\"prompt_tokens\":5,\"completion_tokens\":2}}";
        extract_stream_usage(chunk, &mut c, true);
        assert_eq!(c.tokens_in, 0, "a non-data line carries no usage");
        assert_eq!(c.tokens_out, 0);
    }

    /// The streaming tail is bounded, and a partial line at the cap is kept
    /// whole instead of being dropped.
    #[test]
    fn test_sse_tail_cap_boundary() {
        // Exactly 16 KiB of partial line: still below the cap, so the whole
        // line is kept for the next chunk.
        let mut c = RequestCtx::new();
        let partial = vec![b'x'; 16 * 1024];
        extract_stream_usage(&partial, &mut c, false);
        assert_eq!(c.sse_tail.len(), 16 * 1024, "the tail keeps the whole line");

        // One byte past the cap: the line is dropped and only the last KiB
        // is carried over.
        let mut c = RequestCtx::new();
        let mut partial = vec![b'x'; 16 * 1024 + 1];
        partial.extend_from_slice(b"TAIL-MARKER");
        extract_stream_usage(&partial, &mut c, false);
        assert_eq!(c.sse_tail.len(), 1024, "the tail keeps a bounded fragment");
        assert!(
            c.sse_tail.ends_with(b"TAIL-MARKER"),
            "the fragment is the end of the dropped line"
        );
    }
}

#[cfg(test)]
mod auth_plugin_wiring_tests {
    use super::*;
    use maxllm_config::Config;

    const BASE: &str = r#"
[server]
listen = "0.0.0.0:8080"

[providers.opencode]
kind = "opencode"
base_url = "https://opencode.ai"
api_key = "sk-test"
"#;

    fn hot_state(toml: &str) -> HotState {
        let config = Config::parse(toml).unwrap();
        build_hot_state(&config).unwrap()
    }

    /// `[auth] api_keys` becomes a `key_auth` plugin — but only when it
    /// actually carries keys.
    #[test]
    fn test_auth_section_wires_a_key_auth_plugin() {
        let state = hot_state(&format!("{BASE}\n[auth]\napi_keys = [\"sk-a\"]\n"));
        assert!(
            !state.global_chain.is_empty(),
            "the auth plugin is installed"
        );
    }

    #[test]
    fn test_empty_auth_section_installs_no_plugin() {
        let state = hot_state(&format!("{BASE}\n[auth]\n"));
        assert!(state.global_chain.is_empty(), "no keys, no auth plugin");
    }
}

#[cfg(test)]
mod stream_truncation_tests {
    use super::*;

    fn responses_ctx() -> RequestCtx {
        let mut ctx = RequestCtx::new();
        ctx.endpoint_type = EndpointType::Responses;
        ctx.is_streaming = true;
        ctx
    }

    #[test]
    fn terminal_marker_is_recognised_inside_one_chunk() {
        assert!(stream_carries_terminal(
            b"event: response.completed\ndata: {}\n\n"
        ));
        assert!(stream_carries_terminal(b"data: [DONE]\n\n"));
        assert!(!stream_carries_terminal(
            b"event: response.output_text.delta\ndata: {}\n\n"
        ));
    }

    /// An SSE chunk boundary may fall inside the marker. Scanning each chunk in
    /// isolation would miss `[DO` + `NE]` and book a healthy stream as
    /// truncated, so the detector keeps a tail window across chunks.
    #[test]
    fn terminal_marker_split_across_two_chunks_is_still_detected() {
        let mut ctx = responses_ctx();
        let mut first = Bytes::from_static(b"data: {\"delta\":\"hi\"}\n\ndata: [DO");
        observe_stream_chunk(&mut first, &mut ctx, false);
        assert!(!ctx.stream_terminal_seen, "half a marker is not a marker");

        let mut second = Bytes::from_static(b"NE]\n\n");
        observe_stream_chunk(&mut second, &mut ctx, true);
        assert!(
            ctx.stream_terminal_seen,
            "the marker spans the boundary and must be found"
        );
        assert!(
            !ctx.stream_truncated
                .load(std::sync::atomic::Ordering::Relaxed),
            "a completed stream must never be booked as truncated"
        );
        assert_eq!(second.as_ref(), b"NE]\n\n", "no error event is appended");
    }

    /// A stream that stops before its terminal event must be booked as
    /// truncated and must not leave the client with a silent half-answer.
    #[test]
    fn truncated_stream_is_flagged_and_gets_an_error_event() {
        let mut ctx = responses_ctx();
        let mut chunk = Bytes::from_static(b"event: response.output_text.delta\ndata: {}\n\n");
        observe_stream_chunk(&mut chunk, &mut ctx, true);

        assert!(ctx
            .stream_truncated
            .load(std::sync::atomic::Ordering::Relaxed));
        let text = String::from_utf8_lossy(&chunk).to_string();
        assert!(text.contains("event: error"), "{text}");
        assert!(text.contains("gateway_stream_truncated"), "{text}");
        // The original payload survives — the error is appended, not swapped in.
        assert!(
            text.starts_with("event: response.output_text.delta"),
            "{text}"
        );
    }

    /// Chat-completions streams terminate on `[DONE]`; the error event uses the
    /// chat dialect (no `event:` line, and a trailing `[DONE]` so clients stop).
    #[test]
    fn truncated_chat_stream_gets_a_chat_dialect_error_event() {
        let mut ctx = RequestCtx::new();
        ctx.endpoint_type = EndpointType::ChatCompletions;
        ctx.is_streaming = true;
        let mut chunk = Bytes::from_static(b"data: {\"choices\":[{\"delta\":{}}]}\n\n");
        observe_stream_chunk(&mut chunk, &mut ctx, true);

        assert!(ctx
            .stream_truncated
            .load(std::sync::atomic::Ordering::Relaxed));
        let text = String::from_utf8_lossy(&chunk).to_string();
        assert!(text.contains("gateway_stream_truncated"), "{text}");
        assert!(text.trim_end().ends_with("[DONE]"), "{text}");
    }

    /// `Native` and `Passthrough` speak provider-specific grammars the gateway
    /// does not parse (Anthropic ends on `message_stop`, Gemini on
    /// `finishReason`). Judging them by an OpenAI marker would flag every
    /// healthy stream as truncated.
    #[test]
    fn unknown_stream_grammars_are_never_judged() {
        for endpoint in [EndpointType::Native, EndpointType::Passthrough] {
            let mut ctx = RequestCtx::new();
            ctx.endpoint_type = endpoint;
            ctx.is_streaming = true;
            let mut chunk = Bytes::from_static(b"event: message_stop\ndata: {}\n\n");
            observe_stream_chunk(&mut chunk, &mut ctx, true);
            assert!(
                !ctx.stream_truncated
                    .load(std::sync::atomic::Ordering::Relaxed),
                "{endpoint:?} streams must not be judged by the OpenAI marker set"
            );
            assert_eq!(chunk.as_ref(), b"event: message_stop\ndata: {}\n\n");
        }
    }

    /// A 4xx/5xx is a failure in its own right, not a 200 the upstream cut
    /// short: there is no terminal event to expect. Judging it as truncation
    /// used to (a) mask the real upstream error behind "stream truncated" in
    /// the request log and (b) append a gateway_stream_truncated event to the
    /// JSON error body the client was reading (a native Responses upstream
    /// answers a streaming 4xx with plain JSON, not SSE).
    #[test]
    fn an_error_response_is_never_booked_as_a_truncated_stream() {
        let mut ctx = responses_ctx();
        ctx.upstream_status = 400;
        let body =
            b"{\"error\":{\"message\":\"content policy\",\"type\":\"invalid_request_error\"}}";
        let mut chunk = Bytes::from_static(body);

        observe_stream_chunk(&mut chunk, &mut ctx, true);

        assert!(
            !ctx.stream_truncated
                .load(std::sync::atomic::Ordering::Relaxed),
            "a 4xx body is not a truncated 200"
        );
        assert_eq!(
            chunk.as_ref(),
            body,
            "the error body must reach the client untouched"
        );
        assert!(
            !mark_stream_truncated(&ctx),
            "the truncation counter must stay untouched for a 4xx"
        );
    }

    /// Truncation is sticky and counted once, however many paths notice it.
    #[test]
    fn truncation_is_counted_once() {
        let ctx = responses_ctx();
        assert!(
            mark_stream_truncated(&ctx),
            "first call books the truncation"
        );
        assert!(
            !mark_stream_truncated(&ctx),
            "later calls are no-ops, not double counts"
        );
    }
}

#[cfg(test)]
mod auto_pool_latency_tests {
    use super::*;
    use maxllm_config::BillingMode;

    fn entry(provider: &str) -> AutoPoolEntry {
        AutoPoolEntry {
            provider: provider.to_string(),
            model: format!("m-{provider}"),
            billing: BillingMode::Subscription,
        }
    }

    /// The latency store is process-global and the tests run in parallel, so
    /// every test uses its own provider names — one test's recorded latencies
    /// must never leak into another's cohort.
    fn pool_with_names(prefer_fast: bool, names: &[&str]) -> AutoPoolState {
        AutoPoolState {
            name: "auto".to_string(),
            entries: names.iter().map(|n| entry(n)).collect(),
            prefer_fast,
        }
    }

    #[test]
    fn cohort_drops_a_provider_far_above_the_tier_median() {
        // fast-a/b ~1-2s, mid-c ~6s, slow-d ~30s: the median is ~6s, the
        // ceiling is min(max(6*1.5, 5s), max(10s, 6s)) = 9s, so only slow-d
        // is excluded.
        crate::provider_stats::record_total_latency("cohort-a", 1000.0);
        crate::provider_stats::record_total_latency("cohort-b", 2000.0);
        crate::provider_stats::record_total_latency("cohort-c", 6000.0);
        crate::provider_stats::record_total_latency("cohort-d", 30000.0);
        let pool = pool_with_names(
            true,
            &["cohort-a", "cohort-b", "cohort-c", "cohort-d", "cohort-e"],
        );
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        let cohort = auto_pool_fast_cohort(&healthy);
        let names: Vec<&str> = cohort.iter().map(|e| e.provider.as_str()).collect();
        assert!(names.contains(&"cohort-a"));
        assert!(names.contains(&"cohort-b"));
        assert!(names.contains(&"cohort-c"));
        assert!(
            !names.contains(&"cohort-d"),
            "30s vs 6s median must be excluded"
        );
        // Unmeasured entries are kept (they have not been proven slow, and the
        // rotation is what produces their first measurement).
        assert!(names.contains(&"cohort-e"));
    }

    #[test]
    fn cohort_returns_the_whole_tier_when_nothing_is_measured() {
        // A pool whose providers nobody has ever measured: the whole tier
        // must be returned, so a freshly-started gateway behaves like plain
        // round-robin. Unique names — the latency store is global.
        let pool = pool_with_names(
            true,
            &["fresh-a", "fresh-b", "fresh-c", "fresh-d", "fresh-e"],
        );
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        let cohort = auto_pool_fast_cohort(&healthy);
        assert_eq!(cohort.len(), healthy.len());
    }

    #[test]
    fn cohort_does_not_cut_a_homogeneous_cluster() {
        // A uniformly-fast cluster (all < 5s) must not be cut in half by the
        // absolute floor: ceiling = min(max(median*1.5, 5s), max(10s, median))
        // >= 5s covers all.
        crate::provider_stats::record_total_latency("cluster-a", 900.0);
        crate::provider_stats::record_total_latency("cluster-b", 1100.0);
        crate::provider_stats::record_total_latency("cluster-c", 1300.0);
        crate::provider_stats::record_total_latency("cluster-d", 1700.0);
        let pool = pool_with_names(
            true,
            &[
                "cluster-a",
                "cluster-b",
                "cluster-c",
                "cluster-d",
                "cluster-e",
            ],
        );
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        let cohort = auto_pool_fast_cohort(&healthy);
        assert_eq!(cohort.len(), healthy.len());
    }

    /// The rotation ceiling is `median × 1.5` inside an absolute `[5s, 10s]`
    /// band, and never below the median — so at least the fast half of any
    /// measured tier survives, while no entry waits on a >10 s member when the
    /// tier is quicker than that.
    #[test]
    fn cohort_ceiling_is_banded_and_never_below_the_median() {
        // A fast tier: the 5 s floor wins, so a 4 s member stays.
        assert_eq!(auto_pool_cohort_ceiling(2_000.0), 5_000.0);
        // A mid tier: the 1.5× multiplier is inside the band.
        assert_eq!(auto_pool_cohort_ceiling(4_000.0), 6_000.0);
        // A slow-ish tier: the 10 s cap bites (1.5 × 8 s = 12 s).
        assert_eq!(auto_pool_cohort_ceiling(8_000.0), 10_000.0);
        // A uniformly-slow tier: the cap must never cut below the median,
        // otherwise more than half of the tier would be rotated out.
        assert_eq!(auto_pool_cohort_ceiling(11_700.0), 11_700.0);
        assert_eq!(auto_pool_cohort_ceiling(30_000.0), 30_000.0);
    }

    /// A warm session moves only when the pin is *both* relatively and
    /// absolutely painful. Being merely "not the fastest" never moves it —
    /// that is what keeps the upstream prompt cache alive.
    #[test]
    fn pin_escape_needs_both_relative_and_absolute_pain() {
        // 12 s against a 2 s median: both conditions, escape.
        assert!(auto_pool_pin_escapable(12_000.0, 2_000.0));
        // 8 s against a 2 s median: 4× the median but under the 10 s floor —
        // stay pinned, the cache is worth more than those seconds.
        assert!(!auto_pool_pin_escapable(8_000.0, 2_000.0));
        // 5 s against a 4 s median: over the floor? no — and only 1.25×.
        assert!(!auto_pool_pin_escapable(5_000.0, 4_000.0));
        // 11 s against a 10 s median: over the floor but only 1.1× — stay.
        assert!(!auto_pool_pin_escapable(11_000.0, 10_000.0));
        // Exactly at the boundaries: strictly greater on both, so no escape.
        assert!(!auto_pool_pin_escapable(10_000.0, 10_000.0));
        // An unmeasured tier never escapes (median 0 is handled by the
        // caller returning `None`, but the predicate itself stays strict).
        assert!(!auto_pool_pin_escapable(1_000.0, 0.0));
    }

    #[test]
    fn rotation_without_prefer_fast_walks_the_whole_tier() {
        let pool = pool_with_names(false, &["rr-a", "rr-b", "rr-c", "rr-d", "rr-e"]);
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        let rotation = auto_pool_rotation(&pool, &healthy, 3);
        assert_eq!(rotation.len(), healthy.len());
    }

    #[test]
    fn exploration_rotation_walks_the_whole_tier_every_n() {
        let pool = pool_with_names(true, &["xp-a", "xp-b", "xp-c", "xp-d", "xp-e"]);
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        // idx % AUTO_POOL_EXPLORE_EVERY == 0 => full tier, even when a slow
        // provider would otherwise be filtered out.
        let rotation = auto_pool_rotation(&pool, &healthy, AUTO_POOL_EXPLORE_EVERY);
        assert_eq!(rotation.len(), healthy.len());
    }

    #[test]
    fn rotation_counts_the_skipped_provider() {
        crate::provider_stats::record_total_latency("skip-a", 1000.0);
        crate::provider_stats::record_total_latency("skip-b", 2000.0);
        crate::provider_stats::record_total_latency("skip-c", 6000.0);
        crate::provider_stats::record_total_latency("skip-d", 30000.0);
        let pool = pool_with_names(true, &["skip-a", "skip-b", "skip-c", "skip-d", "skip-e"]);
        let healthy: Vec<&AutoPoolEntry> = pool.entries.iter().collect();
        let before = METRICS
            .auto_pool_slow_skips_total
            .with_label_values(&["skip-d"])
            .get();
        let rotation = auto_pool_rotation(&pool, &healthy, 7);
        assert!(!rotation.iter().any(|e| e.provider == "skip-d"));
        let after = METRICS
            .auto_pool_slow_skips_total
            .with_label_values(&["skip-d"])
            .get();
        assert_eq!(after - before, 1);
    }
}

#[cfg(test)]
mod metrics_visibility_tests {
    use super::metrics_visible_to;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// The local OTel collector is the intended scraper, so loopback is always
    /// served — without it the whole observability path breaks.
    #[test]
    fn loopback_may_always_scrape() {
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ] {
            assert!(metrics_visible_to(false, Some(ip)), "{ip} is loopback");
            assert!(metrics_visible_to(true, Some(ip)));
        }
    }

    /// The exposition names every provider, model, key and price, so a remote
    /// client is refused unless the operator opted in.
    #[test]
    fn remote_clients_are_refused_by_default() {
        let remote = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
        assert!(!metrics_visible_to(false, Some(remote)));
        assert!(metrics_visible_to(true, Some(remote)));
    }

    /// An unknown peer (no socket address) is not evidence of locality.
    #[test]
    fn an_unknown_peer_is_treated_as_remote() {
        assert!(!metrics_visible_to(false, None));
        assert!(metrics_visible_to(true, None));
    }
}
