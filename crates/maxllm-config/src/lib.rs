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
    /// Model cost overrides (supplement built-in costs).
    #[serde(default)]
    pub model_costs: HashMap<String, ModelCostConfig>,
    /// Guardrail definitions (content-level guardrails).
    #[serde(default)]
    pub guardrails: Vec<GuardrailConfig>,
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelCostConfig {
    pub input_per_1m: f64,
    pub output_per_1m: f64,
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
    /// Weight for load balancing (higher = more traffic).
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Tags for tag-based routing.
    #[serde(default)]
    pub tags: Vec<String>,
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
    /// Number of retries before falling back.
    #[serde(default)]
    pub num_retries: u32,
    /// Retry backoff in milliseconds.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

fn default_retry_backoff_ms() -> u64 {
    500
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

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(default)]
    pub api_keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u64,
    pub tokens_per_minute: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub listen: Option<SocketAddr>,
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
        for route in &self.routes {
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
}
