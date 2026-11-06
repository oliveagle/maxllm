// Copyright 2025 MaxLLM Contributors.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

const STARTER_CONFIG: &str = r#"# MaxLLM Gateway Configuration
# See https://github.com/yourorg/maxllm for documentation.

# Global plugins run on every request. This is a TOP-LEVEL key: in TOML it
# must come before any table header, otherwise it is parsed as
# `server.global_plugins` and silently ignored (and no auth runs).
global_plugins = ["request_id", "api_auth"]

[server]
listen = "0.0.0.0:8080"
threads = 4

[metrics]
enabled = true

# --- Authentication ---
[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["sk-maxllm-CHANGEME"]

# --- Rate Limiting (optional) ---
# [plugins.rate_limiter]
# category = "rate_limit"
# requests_per_minute = 60
# key = "client_ip"

# --- CORS (optional) ---
# [plugins.cors]
# category = "cors"
# allow_origin = "*"
# allow_methods = "GET, POST, OPTIONS"
# allow_headers = "Content-Type, Authorization"

# --- Providers ---
# Uncomment and configure the providers you want to use.

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "${OPENAI_API_KEY}"
default_model = "gpt-4o"

# [providers.anthropic]
# kind = "anthropic"
# base_url = "https://api.anthropic.com"
# api_key = "${ANTHROPIC_API_KEY}"
# default_model = "claude-sonnet-4-20250514"

# [providers.gemini]
# kind = "gemini"
# base_url = "https://generativelanguage.googleapis.com"
# api_key = "${GEMINI_API_KEY}"
# default_model = "gemini-2.5-flash"

# [providers.groq]
# kind = "groq"
# base_url = "https://api.groq.com/openai"
# api_key = "${GROQ_API_KEY}"
# default_model = "llama-3.3-70b-versatile"

# [providers.ollama]
# kind = "ollama"
# base_url = "http://127.0.0.1:11434"
# default_model = "llama3.2"

# --- Routes ---
[[routes]]
path = "/v1/chat/completions"
provider = "openai"
# fallback = ["anthropic", "groq"]
# strategy = "fallback"

# --- Model Aliases ---
[model_aliases]
"gpt-4" = "gpt-4o"
# "claude" = "claude-sonnet-4-20250514"

# --- Admin API (optional) ---
# [admin]
# master_key = "${MAXLLM_ADMIN_KEY}"
# enabled = true
"#;

/// Generate a starter configuration file.
pub fn run(output: PathBuf) -> i32 {
    if output.exists() {
        eprintln!(
            "Error: {} already exists. Remove it first or choose a different path.",
            output.display()
        );
        return 1;
    }

    match std::fs::write(&output, STARTER_CONFIG) {
        Ok(()) => {
            println!("Created {}", output.display());
            println!();
            println!("Next steps:");
            println!("  1. Edit {} and set your API keys", output.display());
            println!("  2. Run: maxllm config check");
            println!("  3. Run: maxllm start");
            0
        }
        Err(e) => {
            eprintln!("Error writing {}: {e}", output.display());
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The starter config must actually enable the plugins it declares:
    /// `global_plugins` is a top-level key, so it has to appear before the
    /// first table header — inside `[server]` it would be parsed as a
    /// `server.global_plugins` key and silently ignored, leaving the
    /// gateway with no auth plugin wired.
    #[test]
    fn starter_config_declares_global_plugins_at_the_top_level() {
        let config = maxllm_config::Config::parse(STARTER_CONFIG).expect("starter config parses");
        assert_eq!(config.global_plugins, vec!["request_id", "api_auth"]);
        for name in &config.global_plugins {
            assert!(
                config.plugins.contains_key(name),
                "plugin {name} must be defined by the starter config"
            );
        }
    }
}
