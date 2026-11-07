// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::builtin::pii_filter::GuardrailMode;
use crate::factory::PluginError;
use crate::{HttpResponse, Plugin, PluginCtx, RequestAction};
use async_trait::async_trait;
use pingora::proxy::Session;
use regex::Regex;

/// Extension key signaling that regex guard checking is enabled.
const EXT_REGEX_GUARD_CHECK: &str = "regex_guard_check";
/// Extension key storing the configured action.
const EXT_REGEX_GUARD_ACTION: &str = "regex_guard_action";
/// Extension key storing the configured mode.
const EXT_REGEX_GUARD_MODE: &str = "regex_guard_mode";

/// What to do when a pattern matches.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RegexGuardAction {
    Block,
    Redact,
    LogOnly,
}

impl RegexGuardAction {
    fn as_str(&self) -> &'static str {
        match self {
            RegexGuardAction::Block => "block",
            RegexGuardAction::Redact => "redact",
            RegexGuardAction::LogOnly => "log_only",
        }
    }
}

/// A user-defined regex rule.
struct RegexRule {
    name: String,
    regex: Regex,
    replacement: String,
}

/// A match result from regex guard scanning.
#[derive(Debug, Clone)]
pub struct RegexGuardMatch {
    pub rule_name: String,
    pub matched_text: String,
    pub start: usize,
    pub end: usize,
}

/// User-defined regex guardrail plugin.
///
/// Allows operators to define arbitrary regex patterns for content filtering.
/// More flexible than keyword_block (supports regex) and more general than
/// pii_filter (not limited to PII categories). Useful for:
/// - Custom compliance rules
/// - Domain-specific content filtering
/// - Brand/trademark protection
/// - Preventing specific data patterns from leaking
///
/// Supports pre_call (request), post_call (response), or both modes.
pub struct RegexGuardPlugin {
    name: String,
    rules: Vec<RegexRule>,
    action: RegexGuardAction,
    mode: GuardrailMode,
}

impl RegexGuardPlugin {
    pub fn from_config(name: &str, config: &toml::Table) -> Result<Self, PluginError> {
        let action = match config
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("block")
        {
            "block" => RegexGuardAction::Block,
            "redact" => RegexGuardAction::Redact,
            "log_only" => RegexGuardAction::LogOnly,
            other => {
                return Err(PluginError::Config(format!(
                    "regex_guard action must be 'block', 'redact', or 'log_only', got '{other}'"
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
                    "regex_guard mode must be 'pre_call', 'post_call', or 'both', got '{other}'"
                )));
            }
        };

        // Rules are required — there are no built-ins.
        let rules_arr = config
            .get("rules")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                PluginError::Config(
                    "regex_guard requires a 'rules' array with at least one rule".into(),
                )
            })?;

        if rules_arr.is_empty() {
            return Err(PluginError::Config(
                "regex_guard requires at least one rule".into(),
            ));
        }

        let mut rules = Vec::with_capacity(rules_arr.len());
        for entry in rules_arr {
            if let Some(table) = entry.as_table() {
                let rule_name = table
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unnamed");
                let rule_regex = table.get("regex").and_then(|v| v.as_str()).ok_or_else(|| {
                    PluginError::Config(format!(
                        "regex_guard rule '{rule_name}' requires a 'regex' field"
                    ))
                })?;
                let rule_replacement = table
                    .get("replacement")
                    .and_then(|v| v.as_str())
                    .unwrap_or("[REDACTED]");

                let regex = Regex::new(rule_regex).map_err(|e| {
                    PluginError::Config(format!("invalid regex for rule '{rule_name}': {e}"))
                })?;

                rules.push(RegexRule {
                    name: rule_name.to_string(),
                    regex,
                    replacement: rule_replacement.to_string(),
                });
            }
        }

        if rules.is_empty() {
            return Err(PluginError::Config(
                "regex_guard requires at least one valid rule".into(),
            ));
        }

        Ok(Self {
            name: name.to_string(),
            rules,
            action,
            mode,
        })
    }

    /// Scan text for matches against all configured rules.
    /// Public so the gateway can call it on request/response bodies.
    pub fn scan_text(&self, text: &str) -> Vec<RegexGuardMatch> {
        let mut matches = Vec::new();
        for rule in &self.rules {
            for m in rule.regex.find_iter(text) {
                matches.push(RegexGuardMatch {
                    rule_name: rule.name.clone(),
                    matched_text: m.as_str().to_string(),
                    start: m.start(),
                    end: m.end(),
                });
            }
        }
        matches
    }

    /// Redact all matches in the given text.
    pub fn redact_text(&self, text: &str) -> String {
        let mut result = text.to_string();
        for rule in &self.rules {
            result = rule
                .regex
                .replace_all(&result, rule.replacement.as_str())
                .into_owned();
        }
        result
    }

    /// Returns the configured action.
    pub fn action(&self) -> RegexGuardAction {
        self.action
    }

    /// Returns the configured mode.
    pub fn mode(&self) -> GuardrailMode {
        self.mode
    }
}

#[async_trait]
impl Plugin for RegexGuardPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_request(
        &self,
        session: &mut Session,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        // Set flags so the gateway runs regex checks during body processing.
        ctx.extensions
            .insert(EXT_REGEX_GUARD_CHECK.into(), "enabled".into());
        ctx.extensions
            .insert(EXT_REGEX_GUARD_ACTION.into(), self.action.as_str().into());
        ctx.extensions
            .insert(EXT_REGEX_GUARD_MODE.into(), self.mode.as_str().into());

        // Check query string.
        if let Some(query) = session.req_header().uri.query() {
            let matches = self.scan_text(query);
            if !matches.is_empty() {
                let rule_names: Vec<&str> = matches.iter().map(|m| m.rule_name.as_str()).collect();
                tracing::warn!(
                    plugin = self.name.as_str(),
                    rules = ?rule_names,
                    "regex guard match detected in query string"
                );

                if self.action == RegexGuardAction::Block {
                    return Ok(RequestAction::Respond(HttpResponse::json_error(
                        400,
                        &format!(
                            "Request blocked: content policy violation ({})",
                            rule_names.join(", ")
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

    fn make_rules_config(rules: Vec<(&str, &str)>) -> toml::Table {
        let mut config = toml::Table::new();
        config.insert("category".into(), "regex_guard".into());

        let rules_arr: Vec<toml::Value> = rules
            .into_iter()
            .map(|(name, regex)| {
                let mut table = toml::Table::new();
                table.insert("name".into(), name.into());
                table.insert("regex".into(), regex.into());
                toml::Value::Table(table)
            })
            .collect();
        config.insert("rules".into(), toml::Value::Array(rules_arr));
        config
    }

    fn make_plugin(rules: Vec<(&str, &str)>) -> RegexGuardPlugin {
        let config = make_rules_config(rules);
        RegexGuardPlugin::from_config("rg", &config).unwrap()
    }

    #[test]
    fn test_from_config() {
        let plugin = make_plugin(vec![(
            "sql_injection",
            r"(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|DROP)\b.*\b(?:FROM|INTO|TABLE|SET)\b",
        )]);
        assert_eq!(plugin.action, RegexGuardAction::Block);
        assert_eq!(plugin.mode, GuardrailMode::PreCall);
        assert_eq!(plugin.rules.len(), 1);
    }

    #[test]
    fn test_from_config_redact_post_call() {
        let mut config = make_rules_config(vec![("test", r"secret\d+")]);
        config.insert("action".into(), "redact".into());
        config.insert("mode".into(), "post_call".into());

        let plugin = RegexGuardPlugin::from_config("rg", &config).unwrap();
        assert_eq!(plugin.action, RegexGuardAction::Redact);
        assert_eq!(plugin.mode, GuardrailMode::PostCall);
    }

    #[test]
    fn test_invalid_action_fails() {
        let mut config = make_rules_config(vec![("test", r"test")]);
        config.insert("action".into(), "nuke".into());
        assert!(RegexGuardPlugin::from_config("rg", &config).is_err());
    }

    #[test]
    fn test_missing_rules_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "regex_guard".into());
        assert!(RegexGuardPlugin::from_config("rg", &config).is_err());
    }

    #[test]
    fn test_empty_rules_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "regex_guard".into());
        config.insert("rules".into(), toml::Value::Array(vec![]));
        assert!(RegexGuardPlugin::from_config("rg", &config).is_err());
    }

    #[test]
    fn test_invalid_regex_fails() {
        let config = make_rules_config(vec![("bad", r"[invalid")]);
        assert!(RegexGuardPlugin::from_config("rg", &config).is_err());
    }

    #[test]
    fn test_scan_sql_injection() {
        let plugin = make_plugin(vec![(
            "sql_injection",
            r"(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|DROP)\b.*\b(?:FROM|INTO|TABLE|SET)\b",
        )]);

        let matches = plugin.scan_text("SELECT * FROM users WHERE id = 1");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].rule_name, "sql_injection");
    }

    #[test]
    fn test_scan_no_match() {
        let plugin = make_plugin(vec![(
            "sql_injection",
            r"(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|DROP)\b.*\b(?:FROM|INTO|TABLE|SET)\b",
        )]);
        let matches = plugin.scan_text("What is the weather like today?");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_scan_multiple_rules() {
        let plugin = make_plugin(vec![
            ("profanity", r"(?i)\bbadword\b"),
            ("internal_code", r"INTERNAL-\d{6}"),
        ]);

        let matches = plugin.scan_text("The badword project INTERNAL-123456 is ready");
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_redact_text() {
        let mut config = make_rules_config(vec![("internal_id", r"PROJ-\d+")]);
        config.insert("action".into(), "redact".into());

        let mut rule_table = toml::Table::new();
        rule_table.insert("name".into(), "internal_id".into());
        rule_table.insert("regex".into(), r"PROJ-\d+".into());
        rule_table.insert("replacement".into(), "[INTERNAL_ID]".into());

        let mut config2 = toml::Table::new();
        config2.insert("category".into(), "regex_guard".into());
        config2.insert("action".into(), "redact".into());
        config2.insert(
            "rules".into(),
            toml::Value::Array(vec![toml::Value::Table(rule_table)]),
        );

        let plugin = RegexGuardPlugin::from_config("rg", &config2).unwrap();
        let redacted = plugin.redact_text("Issue PROJ-42 is critical");
        assert_eq!(redacted, "Issue [INTERNAL_ID] is critical");
    }

    #[test]
    fn test_replacement_default() {
        // When no replacement is specified, defaults to [REDACTED].
        let plugin = make_plugin(vec![("test", r"secret\d+")]);
        let redacted = plugin.redact_text("value is secret123");
        assert_eq!(redacted, "value is [REDACTED]");
    }

    #[test]
    fn test_mode_both() {
        let mut config = make_rules_config(vec![("test", r"test")]);
        config.insert("mode".into(), "both".into());
        let plugin = RegexGuardPlugin::from_config("rg", &config).unwrap();
        assert_eq!(plugin.mode, GuardrailMode::Both);
    }

    #[test]
    fn test_regex_guard_action_as_str_is_exact() {
        assert_eq!(RegexGuardAction::Block.as_str(), "block");
        assert_eq!(RegexGuardAction::Redact.as_str(), "redact");
        assert_eq!(RegexGuardAction::LogOnly.as_str(), "log_only");
    }
}
