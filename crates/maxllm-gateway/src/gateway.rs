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
use maxllm_config::{Config, EndpointType, ProviderKind, RouteConfig};
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
use tracing::{info, warn};

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
    }
}

/// User-Agent injected on upstream requests to opencode.ai. The upstream's
/// Cloudflare edge bans script / HTTP-client fingerprints (HTTP 403 "error
/// code: 1010", browser signature banned); `curl/8.5.0` is verified allowed.
const OPENCODE_UPSTREAM_UA: &str = "curl/8.5.0";

/// True when a User-Agent string is one opencode.ai's Cloudflare edge
/// verifiably bans (HTTP 403 "error code: 1010"). Probing the live upstream
/// shows the ban list is narrow: only `Python-urllib/*` (the default UA of
/// Python's urllib) is blocked; curl, browsers and every other HTTP-client /
/// SDK UA tested (OpenAI/Python, node-fetch, axios, python-requests, ...)
/// are accepted, so they are never rewritten here.
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
    pub weight: u32,
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
}

/// Hot-reloadable state — swapped atomically on config change.
pub struct HotState {
    pub providers: AHashMap<String, Arc<ProviderState>>,
    pub routes: Vec<RouteConfig>,
    /// Global session-sticky switch (the `[session_sticky]` section). A route
    /// also has to opt in with `session_sticky = true`.
    pub session_sticky_enabled: bool,
    pub metrics_enabled: bool,
    pub global_chain: PluginChain,
    pub route_chains: Vec<PluginChain>,
    pub model_aliases: HashMap<String, String>,
    pub cost_calculator: Arc<maxllm_admin::CostCalculator>,
    pub guardrail_engine: Option<Arc<GuardrailEngine>>,
    pub models_response: Bytes,
    /// The `auto` rotation pool: (provider, model) pairs that the sentinel
    /// model rotates over. Empty when auto is disabled or not configured.
    /// Entries whose provider is missing from the config are dropped at
    /// build time, so the pool can only contain usable providers.
    pub auto_pool: Vec<AutoPoolEntry>,
}

/// One entry of the auto-rotation pool, resolved against the providers.
#[derive(Debug, Clone)]
pub struct AutoPoolEntry {
    pub provider: String,
    pub model: String,
}

/// The main AI gateway — implements Pingora's ProxyHttp trait.
pub struct AiGateway {
    /// Hot-reloadable state, atomically swappable via ArcSwap.
    /// Wrapped in Arc so a clone can be shared with the file watcher thread.
    pub hot: Arc<arc_swap::ArcSwap<HotState>>,
    /// Budget enforcer for per-key budget tracking.
    #[allow(dead_code)]
    pub budget_enforcer: Option<Arc<maxllm_admin::BudgetEnforcer>>,
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
        let native_responses = conf
            .native_responses
            .unwrap_or(matches!(conf.kind, ProviderKind::OpenCode));
        let responses_path = if native_responses {
            Some(
                conf.responses_path
                    .clone()
                    .or_else(|| {
                        conf.upstream_path
                            .as_deref()
                            .and_then(derive_responses_path)
                    })
                    .unwrap_or_else(|| "/v1/responses".to_string()),
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

    // Build global plugin chain
    let global_plugins: Vec<Arc<dyn Plugin>> = config
        .global_plugins
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
    for (model, cost_conf) in &config.model_costs {
        cost_calculator.add_cost(maxllm_admin::ModelCost {
            model_pattern: model.clone(),
            input_cost_per_1m: cost_conf.input_per_1m,
            output_cost_per_1m: cost_conf.output_per_1m,
        });
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

    // The auto pool is self-contained: it lists the (provider, model) pairs
    // explicitly, so the rotation set never depends on one-api's channel
    // table (or on one-api running at all). Entries naming an unknown
    // provider are dropped — a typo must not silently shrink the pool.
    let mut auto_pool: Vec<AutoPoolEntry> = Vec::new();
    if let Some(auto) = config.auto_model.as_ref() {
        if auto.enabled {
            for entry in &auto.pool {
                if providers.contains_key(&entry.provider) {
                    auto_pool.push(AutoPoolEntry {
                        provider: entry.provider.clone(),
                        model: entry.model.clone(),
                    });
                } else {
                    warn!(
                        provider = %entry.provider,
                        "auto_model pool entry names an unknown provider — dropped"
                    );
                }
            }
        }
    }

    Ok(HotState {
        providers,
        session_sticky_enabled: config
            .session_sticky
            .as_ref()
            .map(|c| c.enabled)
            .unwrap_or(false),
        routes: config.routes.clone(),
        metrics_enabled: config.metrics.enabled,
        global_chain,
        route_chains,
        model_aliases: config.model_aliases.clone(),
        cost_calculator,
        guardrail_engine,
        models_response,
        auto_pool,
    })
}

/// Round-robin counter for the auto pool (shared across auto routes).
static AUTO_POOL_COUNTER: AtomicU64 = AtomicU64::new(0);

impl AiGateway {
    pub fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let hot_state = build_hot_state(config)?;

        // Initialize admin API if configured
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
                    store,
                    Arc::clone(&hot_state.cost_calculator),
                    &admin_config.master_key,
                );
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
                    for row in rows {
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
                }
                Err(e) => warn!(error = %e, "Failed to restore circuit breaker states"),
            }
            match admin_api.load_session_bindings() {
                Ok(bindings) => {
                    let mut restored = 0usize;
                    for (session_id, provider) in bindings {
                        sessions.set(&session_id, &provider);
                        restored += 1;
                    }
                    METRICS.session_affinity_size.set(sessions.len() as i64);
                    if restored > 0 {
                        info!(count = restored, "Restored session bindings");
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
            budget_enforcer: None,
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

    /// Select provider using the route's configured strategy.
    fn select_provider(
        &self,
        hot: &HotState,
        route: &RouteConfig,
        ctx: &mut RequestCtx,
    ) -> Option<Arc<ProviderState>> {
        // Auto route: rotate over the [auto_model] pool instead of the
        // route's fallback chain. Selection is session-sticky first (a
        // conversation stays on the provider that first served it, so the
        // upstream keeps its context), then round-robin over the healthy
        // entries (breaker not open). The chosen (provider, model) pair is
        // remembered in ctx.auto_target so the body's model field can be
        // rewritten to the entry's model later.
        if route.auto_model && !hot.auto_pool.is_empty() {
            // 1. Session-sticky: a session pinned to a pool provider keeps
            //    using it (a different key loses upstream context), unless
            //    that provider's circuit is open.
            if route.session_sticky && hot.session_sticky_enabled {
                if let Some(session_id) = ctx.session_id.clone() {
                    match self.sessions.get(&session_id) {
                        Some(pinned_name) => {
                            if let Some(entry) =
                                hot.auto_pool.iter().find(|e| e.provider == pinned_name)
                            {
                                if let Some(provider) = hot.providers.get(&entry.provider) {
                                    if !provider.circuit_breaker.is_open() {
                                        METRICS.session_affinity_hits_total.inc();
                                        ctx.provider_name = entry.provider.clone();
                                        ctx.auto_target =
                                            Some((entry.provider.clone(), entry.model.clone()));
                                        ctx.model = entry.model.clone();
                                        ctx.plugin_ctx.model = entry.model.clone();
                                        ctx.native_responses = ctx.endpoint_type
                                            == EndpointType::Responses
                                            && provider.native_responses
                                            && provider.responses_path.is_some();
                                        return Some(Arc::clone(provider));
                                    }
                                }
                            }
                            // Pinned provider unavailable (open circuit or no
                            // longer in the pool) — fall through to
                            // round-robin; the session re-pins on the next
                            // successful response.
                        }
                        None => {
                            METRICS.session_affinity_misses_total.inc();
                        }
                    }
                }
            }
            // 2. Round-robin over the healthy pool entries.
            let healthy: Vec<&AutoPoolEntry> = hot
                .auto_pool
                .iter()
                .filter(|e| {
                    hot.providers
                        .get(&e.provider)
                        .map(|p| !p.circuit_breaker.is_open())
                        .unwrap_or(false)
                })
                .collect();
            if healthy.is_empty() {
                return None;
            }
            let idx = AUTO_POOL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;
            let entry = healthy[idx % healthy.len()];
            let provider = match hot.providers.get(&entry.provider) {
                Some(p) => Arc::clone(p),
                None => return None,
            };
            ctx.provider_name = entry.provider.clone();
            ctx.auto_target = Some((entry.provider.clone(), entry.model.clone()));
            ctx.model = entry.model.clone();
            ctx.plugin_ctx.model = entry.model.clone();
            // A native-Responses entry gets the request forwarded unchanged.
            ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
                && provider.native_responses
                && provider.responses_path.is_some();
            return Some(provider);
        }

        // Build list of all candidate providers for this route
        let mut candidates: Vec<ProviderTarget> = Vec::new();

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

        // Session-sticky: a session that already succeeded on a provider is
        // pinned to it, so a conversation keeps hitting the same API key
        // (different keys do not share upstream context). A pinned provider
        // whose circuit is open is skipped — the session is re-pinned after
        // the next successful attempt.
        if route.session_sticky && hot.session_sticky_enabled {
            if let Some(session_id) = ctx.session_id.clone() {
                match self.sessions.get(&session_id) {
                    Some(pinned_name) => {
                        METRICS.session_affinity_hits_total.inc();
                        if let Some(pinned) = candidates.iter().find(|c| c.name == pinned_name) {
                            if !pinned.state.circuit_breaker.is_open() {
                                ctx.provider_name = pinned_name.clone();
                                ctx.fallback_used = pinned_name != route.provider;
                                if ctx.fallback_used {
                                    ctx.original_provider = Some(route.provider.clone());
                                }
                                ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
                                    && pinned.state.native_responses
                                    && pinned.state.responses_path.is_some();
                                return Some(Arc::clone(&pinned.state));
                            }
                        }
                        // Pinned provider is unavailable (open circuit) — fall
                        // through to the normal selector; the session gets
                        // re-pinned on the next successful response.
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

    /// Resolve model aliases.
    fn resolve_model_alias(hot: &HotState, model: &str) -> String {
        hot.model_aliases
            .get(model)
            .cloned()
            .unwrap_or_else(|| model.to_string())
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
fn max_failover_attempts(routes: &[RouteConfig], route_index: Option<usize>) -> u32 {
    route_index
        .and_then(|i| routes.get(i))
        .map(|r| r.max_failover_attempts)
        .unwrap_or(DEFAULT_MAX_FAILOVER_ATTEMPTS)
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

        // Health check endpoint — fast path, no allocations, no metrics overhead
        if path == "/health" || path == "/healthz" {
            static HEALTH_BODY: &[u8] = br#"{"status":"ok"}"#;
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "application/json")?;
            resp.insert_header("Content-Length", "15")?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from_static(HEALTH_BODY)), true)
                .await?;
            ctx.skip_logging = true;
            return Ok(true);
        }

        // Load hot state snapshot for this request
        let hot = self.hot.load();

        // /v1/models endpoint — OpenAI-compatible model list (fast path)
        if path == "/v1/models" {
            let body_bytes = hot.models_response.clone();
            let content_len = body_bytes.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "application/json")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session.write_response_body(Some(body_bytes), true).await?;
            ctx.skip_logging = true;
            return Ok(true);
        }

        // Metrics endpoint (bypasses plugins). The scrape is not an
        // application request: it is not counted in the active-request
        // gauge, and the exposition must not include the scrape itself —
        // otherwise the gauge could never read 0 while it is being scraped.
        if path == "/metrics" && hot.metrics_enabled {
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
            ctx.skip_logging = true;
            return Ok(true);
        }

        METRICS.active_requests.inc();

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
                    "All providers are unavailable (circuit breakers open)",
                )
                .await;
            }
        };

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
                    original
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
        // third-party clients often don't, so inject one:
        // prefer the client's own session-affinity id so cache routing stays
        // sticky across turns, otherwise generate a fresh UUID per request.
        //
        // A header that is PRESENT but EMPTY is treated exactly like a
        // missing one: the upstream rejects an empty value with the same
        // 400 MissingSessionID (verified live), and a client whose session
        // template rendered to "" still sends the header. Only a non-blank
        // client value is kept verbatim.
        let client_session = upstream_request
            .headers
            .get(maxllm_translate::opencode::SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if provider.kind == ProviderKind::OpenCode && client_session.is_none() {
            // The affinity id is also subject to the blank-value rule: an
            // empty `x-session-affinity` must not be injected as an empty
            // session (the upstream rejects it). The sources are scanned in
            // order and the first NON-BLANK one wins, so a present-but-empty
            // affinity falls through to `x-opencode-session-id` instead of
            // short-circuiting the lookup.
            let session_id = ["x-session-affinity", "x-opencode-session-id"]
                .iter()
                .filter_map(|name| session.req_header().headers.get(*name))
                .filter_map(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
                .find(|s| !s.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            upstream_request
                .insert_header(maxllm_translate::opencode::SESSION_HEADER, &session_id)?;
        }

        // opencode.ai's Cloudflare edge bans requests whose User-Agent looks
        // like a script / HTTP-client fingerprint (e.g. `Python-urllib/3.11`)
        // with HTTP 403 "error code: 1010" (browser signature banned). Pingora
        // forwards the client's User-Agent verbatim, so an SDK client would
        // otherwise be banned outright. Inject a safe, generic UA unless the
        // client already sent one that is not a known script fingerprint
        // (curl / browser UAs are verified allowed by the upstream).
        if provider.kind == ProviderKind::OpenCode {
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
                upstream_request.insert_header("User-Agent", OPENCODE_UPSTREAM_UA)?;
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
            // An auto route rewrites the request body's model field to the
            // selected pool entry's model, so the body size changes and the
            // request must travel chunked — never verbatim passthrough.
            let is_auto_route = hot
                .routes
                .get(ctx.route_index.unwrap_or(usize::MAX))
                .map(|r| r.auto_model)
                .unwrap_or(false);
            // A Responses body rewritten into chat completions has an
            // unpredictable size (the chat form can be larger than the
            // Responses form), so it travels chunked instead of keeping the
            // original Content-Length — which would truncate a larger body.
            let verbatim =
                !is_auto_route && (is_native_responses || (openai_style && !is_responses_to_chat));

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
        // Session-sticky: the session identity may also live in the request
        // body (`metadata.user_id`, `client_metadata.session_id`,
        // `prompt_cache_key`). Only buffered bodies are inspected — the
        // header path already covers codex-cli and Claude Code. Runs before
        // the passthrough shortcut so every buffered body is scanned.
        if ctx.session_id.is_none() && end_of_stream && !ctx.request_body_buf.is_empty() {
            ctx.session_id = crate::session_sticky::resolve_from_body(&ctx.request_body_buf);
        }

        // ── PASSTHROUGH: zero processing, forward chunks as-is ──
        if ctx.endpoint_type == EndpointType::Passthrough {
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
                    let normalized = maxllm_translate::responses::normalize_responses_request(
                        &ctx.request_body_buf,
                    );
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
                // Buffer a copy for metadata extraction, but DON'T take the body —
                // let it flow through to upstream unchanged.
                if let Some(ref data) = body {
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
                    // Free the metadata buffer (not needed anymore)
                    ctx.request_body_buf = Vec::new();
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
                        ctx.plugin_ctx.model = auto_model.clone();
                    }
                }
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
                        warn!(error = %e, "Failed to translate Responses request to chat format");
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
                        ctx.plugin_ctx.model = resolved;
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
                // contract, so it is forwarded unchanged.
                let translated = if ctx.native_responses {
                    Ok(TranslatedRequest {
                        body: std::mem::take(&mut ctx.request_body_buf),
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
                                    let inner = provider.translator.streaming_translator();
                                    if ctx.endpoint_type == EndpointType::Responses
                                        && !ctx.native_responses
                                    {
                                        *guard = Some(Box::new(
                                            maxllm_translate::responses::ResponsesStreamTranslator::new(inner),
                                        ));
                                    } else {
                                        *guard = Some(inner);
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
                        warn!(error = %e, "Failed to translate request body");
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
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        let status = upstream_response.status.as_u16();

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
        // upstream usually says when it resets (Retry-After). Any other
        // server error counts against `max_fails`. Successes reset the
        // half-open probe and re-pin the session to this provider.
        let retry_after = upstream_response
            .headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_retry_after_secs);
        let quota_open = status == 429;
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

        if (quota_open || server_error || success) && !quota_resolved_by_header {
            let hot = self.hot.load();
            if let Some(p) = hot.providers.get(&ctx.provider_name) {
                if quota_open {
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
        if !should_failover_status(status, ctx, provider) {
            return Ok(());
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
            // The Pingora body filter is sync; the resolver is async (it may
            // call laya over HTTP). Bridge the boundary with block_on on the
            // current tokio runtime — the call is bounded by the laya
            // timeout, and this is the same thread that will drive the
            // retry, so the failover decision is still on-path.
            let (decision, learned) = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current().map(|rt| {
                    rt.block_on(async { resolver.resolve(status, &headers, &body_bytes).await })
                })
            })
            .unwrap_or_else(|_| {
                // No runtime context (defensive): deterministic-only decision.
                resolver.resolve_sync(status, &headers, &body_bytes)
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
            let hot = self.hot.load();
            if let Some(p) = hot.providers.get(&provider_name) {
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

            // A quota-exhausted provider must not keep the session pinned.
            self.unpin_session(ctx);

            // Same failover budget as the header-time path: when it is spent,
            // the 429 reaches the client as-is.
            let hot = self.hot.load();
            if ctx.failover_count >= max_failover_attempts(&hot.routes, ctx.route_index) {
                info!(
                    status = status,
                    provider = %provider_name,
                    attempts = ctx.failover_count,
                    "429 rate-limit resolved but the failover budget is spent — returning the error to the client"
                );
                return Ok(None);
            }

            // Audit trail: the failed provider answered 429 (the final
            // provider's answer is recorded by `response_filter`).
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
                "429 resolved via the layered rate-limit resolver — retrying on the fallback provider"
            );
            let mut e = pingora::Error::explain(
                pingora::ErrorType::HTTPStatus(status),
                format!(
                    "provider '{failed_provider}' answered 429; rate-limit resolved; retrying on '{next_provider}'"
                ),
            );
            e.as_up();
            e.set_retry(true);
            return Err(e);
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
                if ctx.is_streaming {
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
                            extract_stream_usage(t, ctx, end_of_stream);
                            *body = Some(t.clone());
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
                        extract_stream_usage(t, ctx, end_of_stream);
                        *body = Some(t.clone());
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
                        extract_stream_usage(t, ctx, end_of_stream);
                        *body = Some(t.clone());
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
        _session: &mut Session,
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
            let message = e.to_string();
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

        METRICS.active_requests.dec();

        // The request is done — it no longer counts towards the provider's
        // in-flight load.
        if !ctx.provider_name.is_empty() {
            crate::provider_stats::release(&ctx.provider_name);
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
        }

        let hot = self.hot.load();

        // Calculate cost (skip when no tokens to avoid unnecessary lookups).
        // When the price table has no entry for the model, fall back to the
        // cost the upstream itself reported (e.g. opencode.ai's
        // `usage.opencode.allowance`) instead of recording 0.
        let mut cost = if ctx.tokens_in > 0 || ctx.tokens_out > 0 {
            hot.cost_calculator
                .calculate_cost(&ctx.model, ctx.tokens_in, ctx.tokens_out)
        } else {
            0.0
        };
        if cost == 0.0 {
            if let Some(upstream) = ctx.upstream_cost_usd {
                cost = upstream;
            }
        }
        ctx.cost_usd = cost;

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

        info!(
            provider = %ctx.provider_name,
            model = %ctx.model,
            request_id = ?ctx.plugin_ctx.request_id,
            tokens_in = ctx.tokens_in,
            tokens_out = ctx.tokens_out,
            cost_usd = format!("{:.6}", cost),
            latency_ms = latency_ms,
            fallback = ctx.fallback_used,
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
                cost_usd: cost,
                latency_ms,
                status: ctx.upstream_status,
                request_id: ctx.plugin_ctx.request_id.clone(),
                client_ip: ctx.plugin_ctx.client_ip.clone(),
                route_path: ctx.plugin_ctx.route_path.clone(),
                endpoint_type: format!("{:?}", ctx.endpoint_type),
                fallback_used: ctx.fallback_used,
                error: e.map(|err| err.to_string()),
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
            e.map(|err| err.to_string()),
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
        let hot = self.hot.load();
        let route = hot.routes.get(ctx.route_index?)?;

        // Auto route: walk the [auto_model] pool (still skipping attempted
        // and open-circuit entries) rather than the route's fallback chain.
        if route.auto_model {
            return hot
                .auto_pool
                .iter()
                .filter(|e| e.provider != ctx.provider_name)
                .filter(|e| !ctx.attempted_providers.contains(&e.provider))
                .find_map(|e| {
                    hot.providers
                        .get(&e.provider)
                        .filter(|p| !p.circuit_breaker.is_open())
                        .map(|p| (e.provider.clone(), Arc::clone(p)))
                });
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
        // Auto route: refresh the (provider, model) target to the new pool
        // entry so the replayed body carries the right model.
        if ctx.auto_target.is_some() {
            let hot = self.hot.load();
            if let Some(e) = hot.auto_pool.iter().find(|e| e.provider == name) {
                ctx.auto_target = Some((e.provider.clone(), e.model.clone()));
                ctx.model = e.model.clone();
                ctx.plugin_ctx.model = e.model.clone();
            }
        }
        ctx.body_translated = false;
        ctx.request_body_buf.clear();
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
    if let Some(usage) = body.get("usage") {
        // OpenAI format
        if let Some(pt) = usage.get("prompt_tokens").and_then(|v| v.as_u64()) {
            ctx.tokens_in = pt;
        }
        if let Some(ct) = usage.get("completion_tokens").and_then(|v| v.as_u64()) {
            ctx.tokens_out = ct;
        }
        // Anthropic format
        if let Some(it) = usage.get("input_tokens").and_then(|v| v.as_u64()) {
            ctx.tokens_in = it;
        }
        if let Some(ot) = usage.get("output_tokens").and_then(|v| v.as_u64()) {
            ctx.tokens_out = ot;
        }
        if ctx.upstream_cost_usd.is_none() {
            ctx.upstream_cost_usd = extract_usage_cost(usage);
        }
    }

    // Extract model from response if not set
    if ctx.model.is_empty() {
        if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
            ctx.model = model.to_string();
        }
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
            weight: 100,
            tags: vec![],
            default_model: default_model.map(|m| m.to_string()),
            native_responses: false,
            responses_path: None,
            connect_timeout: Duration::from_secs(10),
            total_connect_timeout: None,
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
