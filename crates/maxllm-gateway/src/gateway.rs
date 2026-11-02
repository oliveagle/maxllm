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
use crate::routing::{ProviderSelector, ProviderTarget};

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
use maxllm_translate::{ProviderTranslator, TranslatedRequest};
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::proxy::{FailToProxy, ProxyHttp, Session};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Shared HTTP client for LLM-based auto-compaction summarization calls.
static LLM_HTTP_CLIENT: once_cell::sync::Lazy<reqwest::Client> = once_cell::sync::Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("reqwest client init failed")
});

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
        ProviderKind::OpenCode => &["deepseek-v4.1-flash", "kimi-k2.5", "glm-5.2"],
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
fn is_script_like_user_agent(ua_lower: &str) -> bool {
    const BANNED_UA_MARKERS: [&str; 1] = ["python-urllib"];
    BANNED_UA_MARKERS.iter().any(|m| ua_lower.contains(m))
}

/// Default TCP connect timeout to an upstream, in seconds.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

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
    pub circuit_breaker: CircuitBreaker,
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
    pub metrics_enabled: bool,
    pub global_chain: PluginChain,
    pub route_chains: Vec<PluginChain>,
    pub model_aliases: HashMap<String, String>,
    pub cost_calculator: Arc<maxllm_admin::CostCalculator>,
    pub guardrail_engine: Option<Arc<GuardrailEngine>>,
    pub models_response: Bytes,
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
                circuit_breaker: CircuitBreaker::new(conf.max_fails, conf.fail_timeout_secs),
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

    Ok(HotState {
        providers,
        routes: config.routes.clone(),
        metrics_enabled: config.metrics.enabled,
        global_chain,
        route_chains,
        model_aliases: config.model_aliases.clone(),
        cost_calculator,
        guardrail_engine,
        models_response,
    })
}

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

        Ok(Self {
            hot: Arc::new(arc_swap::ArcSwap::from_pointee(hot_state)),
            budget_enforcer: None,
            admin_api,
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

    /// Select provider using the route's configured strategy.
    fn select_provider(
        hot: &HotState,
        route: &RouteConfig,
        ctx: &mut RequestCtx,
    ) -> Option<Arc<ProviderState>> {
        // Build list of all candidate providers for this route
        let mut candidates: Vec<ProviderTarget> = Vec::new();

        if let Some(provider) = hot.providers.get(&route.provider) {
            candidates.push(ProviderTarget {
                name: route.provider.clone(),
                state: Arc::clone(provider),
                is_primary: true,
            });
        }

        for fb_name in &route.fallback {
            if let Some(provider) = hot.providers.get(fb_name) {
                candidates.push(ProviderTarget {
                    name: fb_name.clone(),
                    state: Arc::clone(provider),
                    is_primary: false,
                });
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

        METRICS.active_requests.inc();

        // Metrics endpoint (bypasses plugins)
        if path == "/metrics" && hot.metrics_enabled {
            let body = METRICS.encode();
            let content_len = body.len().to_string();
            let mut resp = ResponseHeader::build(200, Some(2))?;
            resp.insert_header("Content-Type", "text/plain; version=0.0.4")?;
            resp.insert_header("Content-Length", &content_len)?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from(body)), true)
                .await?;
            METRICS.active_requests.dec();
            ctx.skip_logging = true;
            return Ok(true);
        }

        // Own the path for later use (after the borrow above is done)
        let path = path.to_string();

        // Reject non-POST methods on API routes (health/metrics/models already handled above)
        let method = session.req_header().method.clone();
        if method != http::method::Method::POST
            && !path.starts_with("/admin/")
            && path != "/v1/models"
        {
            METRICS.active_requests.dec();
            return Self::send_error_response(
                session,
                405,
                &format!("Method {} not allowed. Use POST.", method),
            )
            .await;
        }

        // Admin API endpoints
        if path.starts_with("/admin/") {
            return self.handle_admin_request(session, &path).await;
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
            resp.send(session).await?;
            return Ok(true);
        }

        // Route matching
        let route_index = match Self::match_route(&hot, &path) {
            Some(idx) => idx,
            None => {
                METRICS.active_requests.dec();
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
            resp.send(session).await?;
            return Ok(true);
        }

        // Select provider (with circuit breaker check and routing strategy)
        let _provider = match Self::select_provider(&hot, route, ctx) {
            Some(p) => p,
            None => {
                METRICS.active_requests.dec();
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

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Box<HttpPeer>> {
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

        // Resolve the upstream hostname here instead of letting
        // `HttpPeer::new` do it: pingora's constructor calls
        // `to_socket_addrs().unwrap()` internally and PANICS the worker
        // thread when DNS fails (e.g. a corp-only host reached from a
        // non-corp network), leaving the client with a dropped connection
        // instead of a 502. Look the address up asynchronously and surface a
        // clean error. Passing a resolved `SocketAddr` to `HttpPeer::new`
        // skips the fallible lookup entirely.
        let addr = match tokio::net::lookup_host((provider.host.as_str(), provider.port)).await {
            Ok(mut addrs) => match addrs.next() {
                Some(a) => a,
                None => {
                    return Err(pingora::Error::explain(
                        pingora::ErrorType::HTTPStatus(502),
                        format!("no address resolved for '{}'", provider.host),
                    ));
                }
            },
            Err(e) => {
                return Err(pingora::Error::explain(
                    pingora::ErrorType::HTTPStatus(502),
                    format!("DNS resolution failed for '{}': {}", provider.host, e),
                ));
            }
        };

        let mut peer = HttpPeer::new(addr, provider.tls, provider.sni.clone());
        peer.options.connection_timeout = Some(provider.connect_timeout);
        peer.options.total_connection_timeout = provider.total_connect_timeout;
        peer.options.read_timeout = Some(std::time::Duration::from_secs(120));
        peer.options.write_timeout = Some(std::time::Duration::from_secs(30));

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
            let session_id = session
                .req_header()
                .headers
                .get("x-session-affinity")
                .or_else(|| session.req_header().headers.get("x-opencode-session-id"))
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
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
            // A Responses body rewritten into chat completions has an
            // unpredictable size (the chat form can be larger than the
            // Responses form), so it travels chunked instead of keeping the
            // original Content-Length — which would truncate a larger body.
            let verbatim = is_native_responses || (openai_style && !is_responses_to_chat);

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

        Ok(())
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
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
                if let Some(data) = body.take() {
                    ctx.request_body_buf.extend_from_slice(&data);
                }
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
                if let Some(data) = body.take() {
                    ctx.request_body_buf.extend_from_slice(&data);
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
                    // Free the metadata buffer (not needed anymore)
                    ctx.request_body_buf = Vec::new();
                }
                return Ok(());
            }
        }

        // ── EMBEDDINGS: forward as-is for OpenAI-compatible, extract model ──
        if ctx.endpoint_type == EndpointType::Embeddings {
            if let Some(data) = body.take() {
                ctx.request_body_buf.extend_from_slice(&data);
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
                }
                *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body_buf)));
            }
            return Ok(());
        }

        // Buffer incoming body chunks (both normal and native modes)
        if let Some(data) = body.take() {
            ctx.request_body_buf.extend_from_slice(&data);
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

        // Record circuit breaker state
        if let Some(p) = provider {
            if (500..=599).contains(&status) || status == 429 {
                p.circuit_breaker.record_failure();
            } else if (200..=299).contains(&status) {
                p.circuit_breaker.record_success();
            }
        }

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

        // Responses streaming always emits SSE.
        if ctx.endpoint_type == EndpointType::Responses && ctx.is_streaming && !is_gemini_streaming
        {
            upstream_response.insert_header("Content-Type", "text/event-stream")?;
            let _ = upstream_response.remove_header("Content-Length");
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
                        if let Ok(parsed) =
                            serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf)
                        {
                            extract_usage(&parsed, ctx);
                        }

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
                // Non-Gemini: stream_translator is already a
                // ResponsesStreamTranslator wrapping the provider's chat
                // stream translator, so chunks flow through as Responses SSE.
                if let Some(data) = body.as_ref() {
                    let translated: Option<Bytes> = match ctx.stream_translator.lock() {
                        Ok(mut guard) => guard.as_mut().map(|translator| {
                            Bytes::from(translator.process_chunk(data, end_of_stream))
                        }),
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
                    if let Ok(parsed) =
                        serde_json::from_slice::<serde_json::Value>(&ctx.response_body_buf)
                    {
                        extract_usage(&parsed, ctx);
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

                    if let Some(p) = provider {
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
            } else if let Some(data) = body.as_ref() {
                let translated: Option<Bytes> = match ctx.stream_translator.lock() {
                    Ok(mut guard) => guard.as_mut().map(|translator| {
                        Bytes::from(translator.process_chunk(data, end_of_stream))
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
        if ctx.retry_attempted || !AiGateway::is_connect_failure(&e) {
            return e;
        }
        let Some((name, fallback)) = self.next_fallback_provider(ctx) else {
            return e;
        };
        ctx.retry_attempted = true;
        ctx.fallback_used = true;
        if ctx.original_provider.is_none() {
            ctx.original_provider = Some(ctx.provider_name.clone());
        }
        let failed_provider = ctx.provider_name.clone();
        ctx.provider_name = name.clone();
        // The fallback may speak a different contract than the provider that
        // just failed — the request path depends on it.
        ctx.native_responses = ctx.endpoint_type == EndpointType::Responses
            && fallback.native_responses
            && fallback.responses_path.is_some();

        // The retry re-runs the whole upstream attempt, including
        // `request_body_filter` — that is what turns a Responses body into a
        // chat body for a fallback that only speaks chat completions. State
        // left over from the failed attempt would make the retry skip that
        // work and forward the untranslated body (a chat upstream then
        // receives `{"input":…}` and rejects it):
        // - `body_translated` marks the failed attempt's body as processed;
        // - `request_body_buf` still holds the chunks of that attempt;
        // - `keep_request_content_length` was derived from the failed
        //   provider's framing (re-derived by `upstream_request_filter`
        //   for the retry, so it must start out false);
        // - `stream_translator` was picked for the failed provider's
        //   response contract.
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
        info!(
            provider = %name,
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

        let duration = ctx.request_start.elapsed();

        // Record metrics
        METRICS
            .request_duration_seconds
            .with_label_values(&[&ctx.provider_name])
            .observe(duration.as_secs_f64());

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

    /// The next provider to try after a connect failure: the first healthy
    /// provider in the route's fallback list, skipping the provider whose
    /// connection just failed. Returns its name and state.
    fn next_fallback_provider(&self, ctx: &RequestCtx) -> Option<(String, Arc<ProviderState>)> {
        let hot = self.hot.load();
        let route = hot.routes.get(ctx.route_index?)?;
        route
            .fallback
            .iter()
            .filter(|name| *name != &ctx.provider_name)
            .find_map(|name| {
                hot.providers
                    .get(name)
                    .filter(|p| !p.circuit_breaker.is_open())
                    .map(|p| (name.clone(), Arc::clone(p)))
            })
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
            ctx.sse_tail = Vec::new();
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
            if let Some(pt) = u.get("prompt_tokens").and_then(|v| v.as_u64()) {
                ctx.tokens_in = pt;
            }
            if let Some(ct) = u.get("completion_tokens").and_then(|v| v.as_u64()) {
                ctx.tokens_out = ct;
            }
            // Responses accounting (input_tokens / output_tokens).
            if let Some(it) = u.get("input_tokens").and_then(|v| v.as_u64()) {
                ctx.tokens_in = it;
            }
            if let Some(ot) = u.get("output_tokens").and_then(|v| v.as_u64()) {
                ctx.tokens_out = ot;
            }
            if ctx.upstream_cost_usd.is_none() {
                ctx.upstream_cost_usd = extract_usage_cost(u);
            }
        }
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
}
