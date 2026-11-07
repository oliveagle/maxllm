// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::factory::PluginError;
use crate::{HttpResponse, Plugin, PluginCtx, RequestAction};
use async_trait::async_trait;
use pingora::proxy::Session;
use regex::Regex;

/// Extension key signaling that secret scanning is enabled for the request body.
const EXT_SECRET_SCAN_CHECK: &str = "secret_scan_check";
/// Extension key storing the configured action.
const EXT_SECRET_SCAN_ACTION: &str = "secret_scan_action";

/// What to do when a secret is detected.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SecretScanAction {
    Block,
    Redact,
    LogOnly,
}

impl SecretScanAction {
    fn as_str(&self) -> &'static str {
        match self {
            SecretScanAction::Block => "block",
            SecretScanAction::Redact => "redact",
            SecretScanAction::LogOnly => "log_only",
        }
    }
}

/// A named secret detection pattern.
struct SecretPattern {
    name: String,
    regex: Regex,
    replacement: String,
}

/// A match result from secret scanning.
#[derive(Debug, Clone)]
pub struct SecretMatch {
    pub pattern_name: String,
    pub matched_text: String,
    pub start: usize,
    pub end: usize,
}

/// Secret detection plugin.
///
/// Scans request content for leaked credentials and secrets:
/// - AWS access keys (AKIA...)
/// - AWS secret keys
/// - Generic API keys (long hex/base64 strings with key-like prefixes)
/// - JWT tokens
/// - Private keys (RSA, EC, etc.)
/// - GitHub/GitLab tokens
/// - Slack tokens
/// - Password patterns in URLs
/// - Generic high-entropy bearer tokens
///
/// Can block, redact, or log when secrets are found.
pub struct SecretScanPlugin {
    name: String,
    patterns: Vec<SecretPattern>,
    action: SecretScanAction,
}

impl SecretScanPlugin {
    pub fn from_config(name: &str, config: &toml::Table) -> Result<Self, PluginError> {
        let action = match config
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("block")
        {
            "block" => SecretScanAction::Block,
            "redact" => SecretScanAction::Redact,
            "log_only" => SecretScanAction::LogOnly,
            other => {
                return Err(PluginError::Config(format!(
                    "secret_scan action must be 'block', 'redact', or 'log_only', got '{other}'"
                )));
            }
        };

        // Collect enabled built-in pattern names.
        let enabled_patterns: Vec<String> = config
            .get("patterns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_else(|| {
                vec![
                    "aws_access_key".into(),
                    "aws_secret_key".into(),
                    "github_token".into(),
                    "gitlab_token".into(),
                    "slack_token".into(),
                    "jwt".into(),
                    "private_key".into(),
                    "password_in_url".into(),
                    "generic_api_key".into(),
                ]
            });

        let mut patterns = Vec::new();
        for pattern_name in &enabled_patterns {
            if let Some(p) = Self::builtin_pattern(pattern_name) {
                patterns.push(p);
            } else {
                tracing::warn!(
                    plugin = name,
                    pattern = pattern_name.as_str(),
                    "unknown built-in secret pattern, skipping"
                );
            }
        }

        // Add custom patterns.
        if let Some(custom) = config.get("custom_patterns").and_then(|v| v.as_array()) {
            for entry in custom {
                if let Some(table) = entry.as_table() {
                    let pat_name = table
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("custom");
                    let pat_regex =
                        table.get("regex").and_then(|v| v.as_str()).ok_or_else(|| {
                            PluginError::Config(
                                "custom_patterns entries require a 'regex' field".into(),
                            )
                        })?;
                    let pat_replacement = table
                        .get("replacement")
                        .and_then(|v| v.as_str())
                        .unwrap_or("[REDACTED]");

                    let regex = Regex::new(pat_regex).map_err(|e| {
                        PluginError::Config(format!(
                            "invalid regex for custom pattern '{pat_name}': {e}"
                        ))
                    })?;

                    patterns.push(SecretPattern {
                        name: pat_name.to_string(),
                        regex,
                        replacement: pat_replacement.to_string(),
                    });
                }
            }
        }

        if patterns.is_empty() {
            return Err(PluginError::Config(
                "secret_scan requires at least one pattern".into(),
            ));
        }

        Ok(Self {
            name: name.to_string(),
            patterns,
            action,
        })
    }

    /// Return a built-in secret detection pattern by name.
    fn builtin_pattern(name: &str) -> Option<SecretPattern> {
        let (regex_str, replacement) = match name {
            // AWS access key IDs (always start with AKIA).
            "aws_access_key" => (r"(?:AKIA|ASIA)[0-9A-Z]{16}", "[AWS_ACCESS_KEY]"),
            // AWS secret access keys (40-char base64).
            "aws_secret_key" => (
                r"(?i)(?:aws_secret_access_key|aws_secret_key|secret_key)\s*[=:]\s*[A-Za-z0-9/+=]{40}",
                "[AWS_SECRET_KEY]",
            ),
            // GitHub personal access tokens and fine-grained tokens.
            "github_token" => (
                r"(?:ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{82}|gho_[A-Za-z0-9]{36}|ghu_[A-Za-z0-9]{36}|ghs_[A-Za-z0-9]{36}|ghr_[A-Za-z0-9]{36})",
                "[GITHUB_TOKEN]",
            ),
            // GitLab personal/project/group access tokens.
            "gitlab_token" => (r"glpat-[A-Za-z0-9\-]{20,}", "[GITLAB_TOKEN]"),
            // Slack bot/user/webhook tokens.
            "slack_token" => (r"xox[bporas]-[A-Za-z0-9\-]{10,}", "[SLACK_TOKEN]"),
            // JSON Web Tokens (three base64url segments).
            "jwt" => (
                r"eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
                "[JWT_TOKEN]",
            ),
            // Private key blocks (RSA, EC, DSA, etc.).
            "private_key" => (
                r"-----BEGIN\s+(?:RSA\s+)?(?:EC\s+)?(?:DSA\s+)?(?:OPENSSH\s+)?PRIVATE\s+KEY-----",
                "[PRIVATE_KEY]",
            ),
            // Passwords in URLs (basic auth).
            "password_in_url" => (r"://[^:\s]+:[^@\s]+@[^\s]+", "://[CREDENTIALS]@"),
            // Generic API key patterns (sk-..., api_key=..., etc.).
            "generic_api_key" => (
                r#"(?i)(?:sk-[A-Za-z0-9]{20,}|(?:api[_-]?key|apikey|access[_-]?token)\s*[=:]\s*['"]?[A-Za-z0-9\-_]{20,}['"]?)"#,
                "[API_KEY]",
            ),
            _ => return None,
        };

        Some(SecretPattern {
            name: name.to_string(),
            regex: Regex::new(regex_str).expect("built-in secret regex must compile"),
            replacement: replacement.to_string(),
        })
    }

    /// Scan text for secrets. Returns all matches found.
    /// Public so the gateway can call it on request bodies.
    pub fn scan_text(&self, text: &str) -> Vec<SecretMatch> {
        let mut matches = Vec::new();
        for pattern in &self.patterns {
            for m in pattern.regex.find_iter(text) {
                matches.push(SecretMatch {
                    pattern_name: pattern.name.clone(),
                    matched_text: m.as_str().to_string(),
                    start: m.start(),
                    end: m.end(),
                });
            }
        }
        matches
    }

    /// Redact all secrets in the given text.
    pub fn redact_text(&self, text: &str) -> String {
        let mut result = text.to_string();
        for pattern in &self.patterns {
            result = pattern
                .regex
                .replace_all(&result, pattern.replacement.as_str())
                .into_owned();
        }
        result
    }

    /// Returns the configured action.
    pub fn action(&self) -> SecretScanAction {
        self.action
    }
}

#[async_trait]
impl Plugin for SecretScanPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_request(
        &self,
        session: &mut Session,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        // Set flags so the gateway runs secret checks during body processing.
        ctx.extensions
            .insert(EXT_SECRET_SCAN_CHECK.into(), "enabled".into());
        ctx.extensions
            .insert(EXT_SECRET_SCAN_ACTION.into(), self.action.as_str().into());

        // Check query string for secrets.
        if let Some(query) = session.req_header().uri.query() {
            let matches = self.scan_text(query);
            if !matches.is_empty() {
                let pattern_names: Vec<&str> =
                    matches.iter().map(|m| m.pattern_name.as_str()).collect();
                tracing::warn!(
                    plugin = self.name.as_str(),
                    patterns = ?pattern_names,
                    "secret detected in query string"
                );

                if self.action == SecretScanAction::Block {
                    return Ok(RequestAction::Respond(HttpResponse::json_error(
                        400,
                        &format!(
                            "Request blocked: credential/secret detected ({})",
                            pattern_names.join(", ")
                        ),
                    )));
                }
            }
        }

        Ok(RequestAction::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_plugin() -> SecretScanPlugin {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        SecretScanPlugin::from_config("secret_scan", &config).unwrap()
    }

    #[test]
    fn test_from_config_defaults() {
        let plugin = default_plugin();
        assert_eq!(plugin.action, SecretScanAction::Block);
        assert_eq!(plugin.patterns.len(), 9);
    }

    #[test]
    fn test_from_config_redact() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        config.insert("action".into(), "redact".into());
        let plugin = SecretScanPlugin::from_config("ss", &config).unwrap();
        assert_eq!(plugin.action, SecretScanAction::Redact);
    }

    #[test]
    fn test_invalid_action_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        config.insert("action".into(), "nuke".into());
        assert!(SecretScanPlugin::from_config("ss", &config).is_err());
    }

    #[test]
    fn test_detect_aws_access_key() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("my key is AKIAIOSFODNN7EXAMPLE");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "aws_access_key");
    }

    #[test]
    fn test_detect_aws_temp_key() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("temporary key ASIA1234567890ABCDEF");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "aws_access_key");
    }

    #[test]
    fn test_detect_github_pat() {
        let plugin = default_plugin();
        // ghp_ tokens are exactly 36 alphanumeric chars after prefix
        let matches = plugin.scan_text("token: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "github_token");
    }

    #[test]
    fn test_detect_gitlab_token() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("use glpat-xxxxxxxxxxxxxxxxxxxx");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "gitlab_token");
    }

    #[test]
    fn test_detect_slack_token() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("slack: xoxb-1234567890-abcdefghij");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "slack_token");
    }

    #[test]
    fn test_detect_jwt() {
        let plugin = default_plugin();
        let matches = plugin.scan_text(
            "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "jwt");
    }

    #[test]
    fn test_detect_private_key() {
        let plugin = default_plugin();
        let matches =
            plugin.scan_text("here is my key:\n-----BEGIN RSA PRIVATE KEY-----\nMIIEow...");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "private_key");
    }

    #[test]
    fn test_detect_password_in_url() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("connect to https://admin:p@ssw0rd@db.example.com/mydb");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "password_in_url");
    }

    #[test]
    fn test_detect_generic_api_key() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("use this: sk-abcdefghijklmnopqrstuvwxyz1234");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "generic_api_key");
    }

    #[test]
    fn test_detect_api_key_assignment() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("api_key = 'abcdef1234567890abcdef1234567890'");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "generic_api_key");
    }

    #[test]
    fn test_no_match_clean_text() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("What is the weather like today?");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_no_match_short_strings() {
        let plugin = default_plugin();
        // Short strings shouldn't trigger false positives.
        let matches = plugin.scan_text("key=abc123");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_redact_text() {
        let plugin = default_plugin();
        let redacted = plugin.redact_text("my key is AKIAIOSFODNN7EXAMPLE");
        assert!(redacted.contains("[AWS_ACCESS_KEY]"));
        assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_multiple_secrets() {
        let plugin = default_plugin();
        let text = "AWS: AKIAIOSFODNN7EXAMPLE, GitHub: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij";
        let matches = plugin.scan_text(text);
        assert!(matches.len() >= 2);
    }

    #[test]
    fn test_custom_pattern() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        config.insert(
            "patterns".into(),
            toml::Value::Array(vec!["aws_access_key".into()]),
        );

        let mut custom = toml::Table::new();
        custom.insert("name".into(), "internal_token".into());
        custom.insert("regex".into(), r"mxl_[A-Za-z0-9]{32}".into());
        custom.insert("replacement".into(), "[INTERNAL_TOKEN]".into());

        config.insert(
            "custom_patterns".into(),
            toml::Value::Array(vec![toml::Value::Table(custom)]),
        );

        let plugin = SecretScanPlugin::from_config("ss", &config).unwrap();
        assert_eq!(plugin.patterns.len(), 2);

        let matches = plugin.scan_text("token: mxl_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "internal_token");
    }

    #[test]
    fn test_selective_patterns() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        config.insert(
            "patterns".into(),
            toml::Value::Array(vec!["aws_access_key".into(), "jwt".into()]),
        );

        let plugin = SecretScanPlugin::from_config("ss", &config).unwrap();
        assert_eq!(plugin.patterns.len(), 2);
    }

    #[test]
    fn test_empty_patterns_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "secret_scan".into());
        config.insert("patterns".into(), toml::Value::Array(vec![]));
        assert!(SecretScanPlugin::from_config("ss", &config).is_err());
    }

    #[test]
    fn test_secret_scan_action_as_str_is_exact() {
        assert_eq!(SecretScanAction::Block.as_str(), "block");
        assert_eq!(SecretScanAction::Redact.as_str(), "redact");
        assert_eq!(SecretScanAction::LogOnly.as_str(), "log_only");
    }
}
