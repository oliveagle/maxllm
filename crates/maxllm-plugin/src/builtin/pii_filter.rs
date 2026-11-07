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

/// Extension key signaling that PII checking is enabled for the request body.
/// The gateway should call `PiiFilterPlugin::scan_text()` on the parsed body
/// and act according to the configured action.
const EXT_PII_CHECK: &str = "pii_check";
/// Extension key storing the configured action as a string ("block", "redact", "log_only").
const EXT_PII_ACTION: &str = "pii_action";
/// Extension key storing the configured mode as a string ("pre_call", "post_call", "both").
const EXT_PII_MODE: &str = "pii_mode";

/// What to do when PII is detected.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PiiAction {
    Block,
    Redact,
    LogOnly,
}

impl PiiAction {
    fn as_str(&self) -> &'static str {
        match self {
            PiiAction::Block => "block",
            PiiAction::Redact => "redact",
            PiiAction::LogOnly => "log_only",
        }
    }
}

/// When to run the PII check.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GuardrailMode {
    PreCall,
    PostCall,
    Both,
}

impl GuardrailMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            GuardrailMode::PreCall => "pre_call",
            GuardrailMode::PostCall => "post_call",
            GuardrailMode::Both => "both",
        }
    }
}

/// A named PII pattern with its compiled regex and replacement text.
pub struct PiiPattern {
    pub name: String,
    pub regex: Regex,
    pub replacement: String,
}

/// A match result from scanning text for PII.
#[derive(Debug, Clone)]
pub struct PiiMatch {
    pub pattern_name: String,
    pub matched_text: String,
    pub start: usize,
    pub end: usize,
}

/// PII detection and filtering plugin.
///
/// Provides regex-based PII detection that can block, redact, or log
/// requests containing sensitive data. Since the request body is not
/// available in the plugin's `on_request` hook, this plugin sets flags
/// in `ctx.extensions` and exposes the `scan_text()` utility for the
/// gateway to call during body processing.
///
/// Built-in patterns: email, SSN, credit card, US phone, IP address.
pub struct PiiFilterPlugin {
    name: String,
    patterns: Vec<PiiPattern>,
    action: PiiAction,
    mode: GuardrailMode,
}

impl PiiFilterPlugin {
    pub fn from_config(name: &str, config: &toml::Table) -> Result<Self, PluginError> {
        let action = match config
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("block")
        {
            "block" => PiiAction::Block,
            "redact" => PiiAction::Redact,
            "log_only" => PiiAction::LogOnly,
            other => {
                return Err(PluginError::Config(format!(
                    "pii_filter action must be 'block', 'redact', or 'log_only', got '{other}'"
                )));
            }
        };

        let mode = match config
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("pre_call")
        {
            "pre_call" => GuardrailMode::PreCall,
            "post_call" => GuardrailMode::PostCall,
            "both" => GuardrailMode::Both,
            other => {
                return Err(PluginError::Config(format!(
                    "pii_filter mode must be 'pre_call', 'post_call', or 'both', got '{other}'"
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
                    "email".into(),
                    "ssn".into(),
                    "credit_card".into(),
                    "phone".into(),
                    "ip_address".into(),
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
                    "unknown built-in PII pattern, skipping"
                );
            }
        }

        // Add custom regex patterns.
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

                    patterns.push(PiiPattern {
                        name: pat_name.to_string(),
                        regex,
                        replacement: pat_replacement.to_string(),
                    });
                }
            }
        }

        if patterns.is_empty() {
            return Err(PluginError::Config(
                "pii_filter requires at least one pattern".into(),
            ));
        }

        Ok(Self {
            name: name.to_string(),
            patterns,
            action,
            mode,
        })
    }

    /// Return a built-in PII pattern by name, or None if unknown.
    fn builtin_pattern(name: &str) -> Option<PiiPattern> {
        let (regex_str, replacement) = match name {
            "email" => (r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "[EMAIL]"),
            "ssn" => (r"\b\d{3}-\d{2}-\d{4}\b", "[SSN]"),
            "credit_card" => (r"\b(?:\d{4}[-\s]?){3}\d{4}\b", "[CREDIT_CARD]"),
            "phone" => (
                r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
                "[PHONE]",
            ),
            "ip_address" => (r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", "[IP_ADDRESS]"),
            _ => return None,
        };

        Some(PiiPattern {
            name: name.to_string(),
            regex: Regex::new(regex_str).expect("built-in PII regex must compile"),
            replacement: replacement.to_string(),
        })
    }

    /// Scan text for PII matches against all configured patterns.
    /// Returns a list of matches found. This method is public so the
    /// gateway can call it on request/response bodies.
    pub fn scan_text(&self, text: &str) -> Vec<PiiMatch> {
        let mut matches = Vec::new();
        for pattern in &self.patterns {
            for m in pattern.regex.find_iter(text) {
                matches.push(PiiMatch {
                    pattern_name: pattern.name.clone(),
                    matched_text: m.as_str().to_string(),
                    start: m.start(),
                    end: m.end(),
                });
            }
        }
        matches
    }

    /// Redact all PII in the given text, replacing matches with their
    /// configured replacement strings.
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
    pub fn action(&self) -> PiiAction {
        self.action
    }

    /// Returns the configured guardrail mode.
    pub fn mode(&self) -> GuardrailMode {
        self.mode
    }
}

#[async_trait]
impl Plugin for PiiFilterPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_request(
        &self,
        session: &mut Session,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        // Set flags in extensions so the gateway knows to run PII checks
        // during body processing.
        ctx.extensions
            .insert(EXT_PII_CHECK.into(), "enabled".into());
        ctx.extensions
            .insert(EXT_PII_ACTION.into(), self.action.as_str().into());
        ctx.extensions
            .insert(EXT_PII_MODE.into(), self.mode.as_str().into());

        // Check query string for PII (path and query params are available).
        if let Some(query) = session.req_header().uri.query() {
            let matches = self.scan_text(query);
            if !matches.is_empty() {
                let pattern_names: Vec<&str> =
                    matches.iter().map(|m| m.pattern_name.as_str()).collect();
                tracing::warn!(
                    plugin = self.name.as_str(),
                    patterns = ?pattern_names,
                    "PII detected in query string"
                );

                if self.action == PiiAction::Block {
                    return Ok(RequestAction::Respond(HttpResponse::json_error(
                        400,
                        &format!(
                            "Request blocked: PII detected in query parameters ({})",
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

    fn default_plugin() -> PiiFilterPlugin {
        let mut config = toml::Table::new();
        config.insert("category".into(), "pii_filter".into());
        config.insert(
            "patterns".into(),
            toml::Value::Array(vec![
                "email".into(),
                "ssn".into(),
                "credit_card".into(),
                "phone".into(),
                "ip_address".into(),
            ]),
        );
        PiiFilterPlugin::from_config("pii", &config).unwrap()
    }

    #[test]
    fn test_from_config_defaults() {
        let plugin = default_plugin();
        assert_eq!(plugin.action, PiiAction::Block);
        assert_eq!(plugin.mode, GuardrailMode::PreCall);
        assert_eq!(plugin.patterns.len(), 5);
    }

    #[test]
    fn test_from_config_redact_mode() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "pii_filter".into());
        config.insert("action".into(), "redact".into());
        config.insert("mode".into(), "both".into());
        config.insert("patterns".into(), toml::Value::Array(vec!["email".into()]));

        let plugin = PiiFilterPlugin::from_config("pii", &config).unwrap();
        assert_eq!(plugin.action, PiiAction::Redact);
        assert_eq!(plugin.mode, GuardrailMode::Both);
    }

    #[test]
    fn test_scan_email() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("Contact me at user@example.com for details");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "email");
        assert_eq!(matches[0].matched_text, "user@example.com");
    }

    #[test]
    fn test_scan_ssn() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("My SSN is 123-45-6789");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "ssn");
    }

    #[test]
    fn test_scan_credit_card() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("Card: 4111-1111-1111-1111");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "credit_card");
    }

    #[test]
    fn test_scan_phone() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("Call me at (555) 123-4567");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "phone");
    }

    #[test]
    fn test_scan_ip_address() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("Server at 192.168.1.100");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "ip_address");
    }

    #[test]
    fn test_scan_multiple_matches() {
        let plugin = default_plugin();
        let text = "Email: test@example.com, SSN: 123-45-6789, IP: 10.0.0.1";
        let matches = plugin.scan_text(text);
        assert!(matches.len() >= 3);
    }

    #[test]
    fn test_scan_no_pii() {
        let plugin = default_plugin();
        let matches = plugin.scan_text("This is a perfectly clean message.");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_redact_text() {
        let plugin = default_plugin();
        let redacted = plugin.redact_text("Email me at user@example.com");
        assert!(redacted.contains("[EMAIL]"));
        assert!(!redacted.contains("user@example.com"));
    }

    #[test]
    fn test_invalid_action_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "pii_filter".into());
        config.insert("action".into(), "destroy".into());
        config.insert("patterns".into(), toml::Value::Array(vec!["email".into()]));

        assert!(PiiFilterPlugin::from_config("pii", &config).is_err());
    }

    #[test]
    fn test_empty_patterns_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "pii_filter".into());
        config.insert("patterns".into(), toml::Value::Array(vec![]));

        assert!(PiiFilterPlugin::from_config("pii", &config).is_err());
    }

    #[test]
    fn test_custom_pattern() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "pii_filter".into());
        config.insert("patterns".into(), toml::Value::Array(vec!["email".into()]));

        let mut custom = toml::Table::new();
        custom.insert("name".into(), "aws_key".into());
        custom.insert("regex".into(), r"AKIA[0-9A-Z]{16}".into());
        custom.insert("replacement".into(), "[AWS_KEY]".into());

        config.insert(
            "custom_patterns".into(),
            toml::Value::Array(vec![toml::Value::Table(custom)]),
        );

        let plugin = PiiFilterPlugin::from_config("pii", &config).unwrap();
        assert_eq!(plugin.patterns.len(), 2);

        let matches = plugin.scan_text("Key: AKIAIOSFODNN7EXAMPLE");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "aws_key");
    }

    #[test]
    fn test_pii_action_as_str_is_exact() {
        assert_eq!(PiiAction::Block.as_str(), "block");
        assert_eq!(PiiAction::Redact.as_str(), "redact");
        assert_eq!(PiiAction::LogOnly.as_str(), "log_only");
    }

    #[test]
    fn test_guardrail_mode_as_str_is_exact() {
        assert_eq!(GuardrailMode::PreCall.as_str(), "pre_call");
        assert_eq!(GuardrailMode::PostCall.as_str(), "post_call");
        assert_eq!(GuardrailMode::Both.as_str(), "both");
    }
}
