// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Guardrail framework for content-level inspection.
//!
//! This module provides:
//! - **Content-level inspection** — operates on parsed message content, not raw HTTP
//! - **Built-in providers** — prompt injection, PII, secrets, keywords, regex
//! - **External providers** — generic HTTP webhook, Lakera AI
//! - **Request-level selection** — clients choose which guardrails via `guardrails` param
//! - **default_on** — some guardrails always run regardless of client request
//! - **Pre/post-call lifecycle** — check input before LLM, check output after
//! - **Response headers** — `X-MaxLLM-Applied-Guardrails` shows which ran

pub mod builtin;
pub mod cel;
pub mod webhook;

use async_trait::async_trait;
use maxllm_config::GuardrailConfig;
use std::sync::Arc;

// ─── Types ──────────────────────────────────────────────────────────────

/// When the guardrail runs in the request lifecycle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GuardrailMode {
    /// Before sending to LLM — checks user input.
    PreCall,
    /// After receiving from LLM — checks model output.
    PostCall,
    /// Both pre and post call.
    Both,
}

impl GuardrailMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "post_call" => Self::PostCall,
            "both" => Self::Both,
            _ => Self::PreCall,
        }
    }
}

/// What to do when a guardrail triggers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GuardrailAction {
    Block,
    Redact,
    LogOnly,
}

impl GuardrailAction {
    pub fn parse(s: &str) -> Self {
        match s {
            "redact" => Self::Redact,
            "log_only" => Self::LogOnly,
            _ => Self::Block,
        }
    }
}

/// Result of a guardrail check.
#[derive(Debug, Clone)]
pub enum GuardrailVerdict {
    /// Content is allowed.
    Pass,
    /// Content is blocked.
    Block { guardrail: String, reason: String },
    /// Content was modified (redacted).
    Modify {
        guardrail: String,
        content: String,
        reason: String,
    },
    /// Findings logged but not blocked.
    Log {
        guardrail: String,
        findings: Vec<String>,
    },
}

/// Input to a pre-call guardrail check.
pub struct GuardrailInput<'a> {
    /// Concatenated message content from the request.
    pub content: &'a str,
    /// Model being requested.
    pub model: &'a str,
    /// Client ID (from auth plugin).
    pub client_id: Option<&'a str>,
}

/// Input to a post-call guardrail check.
pub struct GuardrailOutput<'a> {
    /// LLM response content.
    pub content: &'a str,
    /// Model that generated the response.
    pub model: &'a str,
}

// ─── Trait ──────────────────────────────────────────────────────────────

/// Content-level guardrail provider.
///
/// Unlike plugins (which operate on raw HTTP), guardrails operate on parsed
/// message content. They inspect the actual text being sent to/from the LLM.
#[async_trait]
pub trait Guardrail: Send + Sync {
    /// Provider name (e.g., "prompt_guard", "webhook").
    fn name(&self) -> &str;

    /// Check input content before sending to LLM.
    async fn check_input(&self, input: &GuardrailInput<'_>) -> GuardrailVerdict;

    /// Check output content after receiving from LLM.
    async fn check_output(&self, output: &GuardrailOutput<'_>) -> GuardrailVerdict;
}

// ─── Engine ─────────────────────────────────────────────────────────────

/// A configured guardrail instance with its metadata.
pub struct GuardrailEntry {
    pub name: String,
    pub guardrail: Arc<dyn Guardrail>,
    pub mode: GuardrailMode,
    pub default_on: bool,
    pub action: GuardrailAction,
}

/// Orchestrates guardrail execution across the request lifecycle.
///
/// The engine manages multiple guardrail instances and runs them based on:
/// - Their configured mode (pre_call, post_call, both)
/// - Whether they're default_on
/// - Which guardrails the client requested
pub struct GuardrailEngine {
    entries: Vec<GuardrailEntry>,
}

impl GuardrailEngine {
    pub fn new(entries: Vec<GuardrailEntry>) -> Self {
        Self { entries }
    }

    /// Returns true if there are any pre-call guardrails configured.
    pub fn has_pre_call(&self) -> bool {
        self.entries
            .iter()
            .any(|e| e.mode == GuardrailMode::PreCall || e.mode == GuardrailMode::Both)
    }

    /// Returns true if there are any post-call guardrails configured.
    pub fn has_post_call(&self) -> bool {
        self.entries
            .iter()
            .any(|e| e.mode == GuardrailMode::PostCall || e.mode == GuardrailMode::Both)
    }

    /// Get the list of guardrails that should run for this request.
    ///
    /// Selection logic:
    /// - `route_guardrails`: If the route specifies guardrail names, only those are eligible
    ///   (scopes which guardrails CAN run for this route).
    /// - `requested`: Client-requested guardrails from the request body `guardrails` field.
    /// - `default_on`: Guardrails with `default_on = true` always run (within route scope).
    ///
    /// Priority: route scope → then default_on + client-requested within that scope.
    fn active_entries<'a>(
        &'a self,
        requested: Option<&[String]>,
        route_guardrails: Option<&[String]>,
        mode_filter: impl Fn(GuardrailMode) -> bool,
    ) -> Vec<&'a GuardrailEntry> {
        self.entries
            .iter()
            .filter(|e| {
                // Mode must match
                if !mode_filter(e.mode) {
                    return false;
                }

                // If route specifies guardrails, entry must be in that list
                if let Some(route_names) = route_guardrails {
                    if !route_names.is_empty() && !route_names.iter().any(|n| n == &e.name) {
                        return false;
                    }
                }

                // default_on guardrails always run (within route scope)
                if e.default_on {
                    return true;
                }
                // Otherwise, must be explicitly requested by client
                if let Some(names) = requested {
                    names.iter().any(|n| n == &e.name)
                } else {
                    false
                }
            })
            .collect()
    }

    /// Run pre-call guardrails on input content.
    ///
    /// Returns the first blocking verdict, or Pass if all pass.
    /// For redact action, returns Modify with the progressively redacted content.
    ///
    /// - `requested`: Client-requested guardrail names from request body.
    /// - `route_guardrails`: Route-specific guardrail scope (if empty, all are eligible).
    pub async fn run_pre_call(
        &self,
        input: &GuardrailInput<'_>,
        requested: Option<&[String]>,
        route_guardrails: Option<&[String]>,
    ) -> (GuardrailVerdict, Vec<String>) {
        let entries = self.active_entries(requested, route_guardrails, |m| {
            m == GuardrailMode::PreCall || m == GuardrailMode::Both
        });

        let mut applied = Vec::new();
        let mut current_content = input.content.to_string();

        for entry in &entries {
            applied.push(entry.name.clone());

            let check_input = GuardrailInput {
                content: &current_content,
                model: input.model,
                client_id: input.client_id,
            };

            let verdict = entry.guardrail.check_input(&check_input).await;

            match (&verdict, entry.action) {
                (GuardrailVerdict::Block { .. }, GuardrailAction::Block) => {
                    return (verdict, applied);
                }
                (GuardrailVerdict::Block { reason, .. }, GuardrailAction::Redact) => {
                    // For redact action, treat block as "found something, redact it"
                    // The provider should return Modify instead for redaction
                    tracing::warn!(
                        guardrail = entry.name.as_str(),
                        reason = reason.as_str(),
                        "guardrail triggered (action=redact, treating as log)"
                    );
                }
                (GuardrailVerdict::Block { reason, .. }, GuardrailAction::LogOnly) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        reason = reason.as_str(),
                        "guardrail triggered (action=log_only)"
                    );
                }
                (
                    GuardrailVerdict::Modify {
                        content, reason, ..
                    },
                    _,
                ) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        reason = reason.as_str(),
                        "guardrail modified content"
                    );
                    current_content = content.clone();
                }
                (GuardrailVerdict::Log { findings, .. }, _) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        findings = ?findings,
                        "guardrail findings"
                    );
                }
                (GuardrailVerdict::Pass, _) => {}
            }
        }

        // If content was modified, return Modify verdict
        if current_content != input.content {
            return (
                GuardrailVerdict::Modify {
                    guardrail: "engine".into(),
                    content: current_content,
                    reason: "content redacted by guardrails".into(),
                },
                applied,
            );
        }

        (GuardrailVerdict::Pass, applied)
    }

    /// Run post-call guardrails on output content.
    pub async fn run_post_call(
        &self,
        output: &GuardrailOutput<'_>,
        requested: Option<&[String]>,
        route_guardrails: Option<&[String]>,
    ) -> (GuardrailVerdict, Vec<String>) {
        let entries = self.active_entries(requested, route_guardrails, |m| {
            m == GuardrailMode::PostCall || m == GuardrailMode::Both
        });

        let mut applied = Vec::new();
        let mut current_content = output.content.to_string();

        for entry in &entries {
            applied.push(entry.name.clone());

            let check_output = GuardrailOutput {
                content: &current_content,
                model: output.model,
            };

            let verdict = entry.guardrail.check_output(&check_output).await;

            match (&verdict, entry.action) {
                (GuardrailVerdict::Block { .. }, GuardrailAction::Block) => {
                    return (verdict, applied);
                }
                (GuardrailVerdict::Block { reason, .. }, GuardrailAction::LogOnly) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        reason = reason.as_str(),
                        "post-call guardrail triggered (action=log_only)"
                    );
                }
                (
                    GuardrailVerdict::Modify {
                        content, reason, ..
                    },
                    _,
                ) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        reason = reason.as_str(),
                        "post-call guardrail modified content"
                    );
                    current_content = content.clone();
                }
                (GuardrailVerdict::Log { findings, .. }, _) => {
                    tracing::info!(
                        guardrail = entry.name.as_str(),
                        findings = ?findings,
                        "post-call guardrail findings"
                    );
                }
                _ => {}
            }
        }

        if current_content != output.content {
            return (
                GuardrailVerdict::Modify {
                    guardrail: "engine".into(),
                    content: current_content,
                    reason: "output redacted by guardrails".into(),
                },
                applied,
            );
        }

        (GuardrailVerdict::Pass, applied)
    }
}

// ─── Factory ────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum GuardrailError {
    #[error("unknown guardrail provider: {0}")]
    UnknownProvider(String),
    #[error("guardrail config error: {0}")]
    Config(String),
}

/// Create a guardrail instance from config.
pub fn create_guardrail(config: &GuardrailConfig) -> Result<Arc<dyn Guardrail>, GuardrailError> {
    match config.provider.as_str() {
        "prompt_guard" => Ok(Arc::new(builtin::PromptGuardGuardrail::from_config(
            config,
        )?)),
        "pii_filter" => Ok(Arc::new(builtin::PiiFilterGuardrail::from_config(config)?)),
        "secret_scan" => Ok(Arc::new(builtin::SecretScanGuardrail::from_config(config)?)),
        "keyword_block" => Ok(Arc::new(builtin::KeywordBlockGuardrail::from_config(
            config,
        )?)),
        "regex_guard" => Ok(Arc::new(builtin::RegexGuardGuardrail::from_config(config)?)),
        "webhook" => Ok(Arc::new(webhook::WebhookGuardrail::from_config(config)?)),
        "lakera" => Ok(Arc::new(webhook::LakeraGuardrail::from_config(config)?)),
        "cel" => Ok(Arc::new(cel::CelGuardrail::from_config(config)?)),
        other => Err(GuardrailError::UnknownProvider(other.to_string())),
    }
}

/// Build a GuardrailEngine from a list of guardrail configs.
pub fn build_engine(configs: &[GuardrailConfig]) -> Result<GuardrailEngine, GuardrailError> {
    let mut entries = Vec::with_capacity(configs.len());

    for config in configs {
        let guardrail = create_guardrail(config)?;
        entries.push(GuardrailEntry {
            name: config.name.clone(),
            guardrail,
            mode: GuardrailMode::parse(&config.mode),
            default_on: config.default_on,
            action: GuardrailAction::parse(&config.action),
        });
    }

    Ok(GuardrailEngine::new(entries))
}

/// Extract concatenated message content from a parsed request JSON body.
/// Handles OpenAI-style `messages` array format.
pub fn extract_message_content(body: &serde_json::Value) -> String {
    let mut content = String::new();

    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if let Some(text) = msg.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(text);
            }
            // Handle array content (multimodal messages)
            if let Some(parts) = msg.get("content").and_then(|c| c.as_array()) {
                for part in parts {
                    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                        if !content.is_empty() {
                            content.push('\n');
                        }
                        content.push_str(text);
                    }
                }
            }
        }
    }

    // Also check prompt field (completions API)
    if content.is_empty() {
        if let Some(prompt) = body.get("prompt").and_then(|p| p.as_str()) {
            content.push_str(prompt);
        }
    }

    content
}

/// Extract response content from an OpenAI-format chat completion response.
pub fn extract_response_content(body: &serde_json::Value) -> String {
    let mut content = String::new();

    if let Some(choices) = body.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            // Non-streaming: message.content
            if let Some(text) = choice
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
            {
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(text);
            }
            // Streaming: delta.content
            if let Some(text) = choice
                .get("delta")
                .and_then(|d| d.get("content"))
                .and_then(|c| c.as_str())
            {
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(text);
            }
        }
    }

    content
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_message_content_openai() {
        let body = serde_json::json!({
            "model": "gpt-4",
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "What is 2+2?"}
            ]
        });
        let content = extract_message_content(&body);
        assert_eq!(content, "You are helpful.\nWhat is 2+2?");
    }

    #[test]
    fn test_extract_message_content_multimodal() {
        let body = serde_json::json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "Describe this image"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,..."}}
                ]}
            ]
        });
        let content = extract_message_content(&body);
        assert_eq!(content, "Describe this image");
    }

    #[test]
    fn test_extract_message_content_prompt() {
        let body = serde_json::json!({
            "model": "gpt-3.5-turbo-instruct",
            "prompt": "Complete this: The quick brown"
        });
        let content = extract_message_content(&body);
        assert_eq!(content, "Complete this: The quick brown");
    }

    #[test]
    fn test_extract_message_content_empty() {
        let body = serde_json::json!({"model": "gpt-4"});
        let content = extract_message_content(&body);
        assert!(content.is_empty());
    }

    #[test]
    fn test_guardrail_mode_from_str() {
        assert_eq!(GuardrailMode::parse("pre_call"), GuardrailMode::PreCall);
        assert_eq!(GuardrailMode::parse("post_call"), GuardrailMode::PostCall);
        assert_eq!(GuardrailMode::parse("both"), GuardrailMode::Both);
        assert_eq!(GuardrailMode::parse("unknown"), GuardrailMode::PreCall);
    }

    #[test]
    fn test_guardrail_action_from_str() {
        assert_eq!(GuardrailAction::parse("block"), GuardrailAction::Block);
        assert_eq!(GuardrailAction::parse("redact"), GuardrailAction::Redact);
        assert_eq!(GuardrailAction::parse("log_only"), GuardrailAction::LogOnly);
        assert_eq!(GuardrailAction::parse("unknown"), GuardrailAction::Block);
    }

    #[tokio::test]
    async fn test_engine_default_on() {
        // A default_on guardrail should run even without explicit request
        struct AlwaysPass;
        #[async_trait]
        impl Guardrail for AlwaysPass {
            fn name(&self) -> &str {
                "always_pass"
            }
            async fn check_input(&self, _: &GuardrailInput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Pass
            }
            async fn check_output(&self, _: &GuardrailOutput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Pass
            }
        }

        let engine = GuardrailEngine::new(vec![GuardrailEntry {
            name: "test".into(),
            guardrail: Arc::new(AlwaysPass),
            mode: GuardrailMode::PreCall,
            default_on: true,
            action: GuardrailAction::Block,
        }]);

        let input = GuardrailInput {
            content: "hello",
            model: "gpt-4",
            client_id: None,
        };

        let (verdict, applied) = engine.run_pre_call(&input, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert_eq!(applied, vec!["test"]);
    }

    #[tokio::test]
    async fn test_engine_not_default_on_requires_request() {
        struct AlwaysBlock;
        #[async_trait]
        impl Guardrail for AlwaysBlock {
            fn name(&self) -> &str {
                "blocker"
            }
            async fn check_input(&self, _: &GuardrailInput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Block {
                    guardrail: "blocker".into(),
                    reason: "blocked".into(),
                }
            }
            async fn check_output(&self, _: &GuardrailOutput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Pass
            }
        }

        let engine = GuardrailEngine::new(vec![GuardrailEntry {
            name: "blocker".into(),
            guardrail: Arc::new(AlwaysBlock),
            mode: GuardrailMode::PreCall,
            default_on: false,
            action: GuardrailAction::Block,
        }]);

        let input = GuardrailInput {
            content: "hello",
            model: "gpt-4",
            client_id: None,
        };

        // Without requesting, should not run
        let (verdict, applied) = engine.run_pre_call(&input, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert!(applied.is_empty());

        // With request, should block
        let requested = vec!["blocker".to_string()];
        let (verdict, applied) = engine.run_pre_call(&input, Some(&requested), None).await;
        assert!(matches!(verdict, GuardrailVerdict::Block { .. }));
        assert_eq!(applied, vec!["blocker"]);
    }

    #[tokio::test]
    async fn test_engine_mode_filtering() {
        struct PostOnlyGuard;
        #[async_trait]
        impl Guardrail for PostOnlyGuard {
            fn name(&self) -> &str {
                "post_only"
            }
            async fn check_input(&self, _: &GuardrailInput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Block {
                    guardrail: "post_only".into(),
                    reason: "should not run".into(),
                }
            }
            async fn check_output(&self, _: &GuardrailOutput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Pass
            }
        }

        let engine = GuardrailEngine::new(vec![GuardrailEntry {
            name: "post_only".into(),
            guardrail: Arc::new(PostOnlyGuard),
            mode: GuardrailMode::PostCall,
            default_on: true,
            action: GuardrailAction::Block,
        }]);

        let input = GuardrailInput {
            content: "hello",
            model: "gpt-4",
            client_id: None,
        };

        // Pre-call should NOT run post-call-only guardrail
        let (verdict, applied) = engine.run_pre_call(&input, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert!(applied.is_empty());
    }

    #[tokio::test]
    async fn test_engine_route_scoping() {
        struct BlockAll;
        #[async_trait]
        impl Guardrail for BlockAll {
            fn name(&self) -> &str {
                "block_all"
            }
            async fn check_input(&self, _: &GuardrailInput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Block {
                    guardrail: "block_all".into(),
                    reason: "blocked".into(),
                }
            }
            async fn check_output(&self, _: &GuardrailOutput<'_>) -> GuardrailVerdict {
                GuardrailVerdict::Pass
            }
        }

        let engine = GuardrailEngine::new(vec![
            GuardrailEntry {
                name: "guard-a".into(),
                guardrail: Arc::new(BlockAll),
                mode: GuardrailMode::PreCall,
                default_on: true,
                action: GuardrailAction::Block,
            },
            GuardrailEntry {
                name: "guard-b".into(),
                guardrail: Arc::new(BlockAll),
                mode: GuardrailMode::PreCall,
                default_on: true,
                action: GuardrailAction::Block,
            },
        ]);

        let input = GuardrailInput {
            content: "hello",
            model: "gpt-4",
            client_id: None,
        };

        // No route scope — both run, first blocks
        let (verdict, applied) = engine.run_pre_call(&input, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Block { .. }));
        assert_eq!(applied, vec!["guard-a"]);

        // Route scoped to guard-b only — guard-a should NOT run
        let route_guards = vec!["guard-b".to_string()];
        let (verdict, applied) = engine.run_pre_call(&input, None, Some(&route_guards)).await;
        assert!(matches!(verdict, GuardrailVerdict::Block { .. }));
        assert_eq!(applied, vec!["guard-b"]);

        // Route scoped to non-existent guard — nothing runs
        let route_guards = vec!["guard-c".to_string()];
        let (verdict, applied) = engine.run_pre_call(&input, None, Some(&route_guards)).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert!(applied.is_empty());
    }
    // -----------------------------------------------------------------------
    // Mutation coverage: engine mode predicates, post-call dispatch and the
    // response content extractor.
    // -----------------------------------------------------------------------

    fn mock_entry(
        name: &str,
        mode: GuardrailMode,
        default_on: bool,
        action: GuardrailAction,
        verdict: GuardrailVerdict,
    ) -> GuardrailEntry {
        struct Mock {
            v: GuardrailVerdict,
        }
        #[async_trait]
        impl Guardrail for Mock {
            fn name(&self) -> &str {
                "mock"
            }
            async fn check_input(&self, _: &GuardrailInput<'_>) -> GuardrailVerdict {
                self.v.clone()
            }
            async fn check_output(&self, _: &GuardrailOutput<'_>) -> GuardrailVerdict {
                self.v.clone()
            }
        }
        GuardrailEntry {
            name: name.into(),
            guardrail: Arc::new(Mock { v: verdict }),
            mode,
            default_on,
            action,
        }
    }

    #[test]
    fn test_has_pre_call_reflects_the_configured_modes() {
        let empty = GuardrailEngine::new(vec![]);
        assert!(!empty.has_pre_call());
        assert!(!empty.has_post_call());

        let pre_only = GuardrailEngine::new(vec![mock_entry(
            "g",
            GuardrailMode::PreCall,
            true,
            GuardrailAction::Block,
            GuardrailVerdict::Pass,
        )]);
        assert!(pre_only.has_pre_call());
        assert!(!pre_only.has_post_call());

        let post_only = GuardrailEngine::new(vec![mock_entry(
            "g",
            GuardrailMode::PostCall,
            true,
            GuardrailAction::Block,
            GuardrailVerdict::Pass,
        )]);
        assert!(!post_only.has_pre_call());
        assert!(post_only.has_post_call());

        let both = GuardrailEngine::new(vec![mock_entry(
            "g",
            GuardrailMode::Both,
            true,
            GuardrailAction::Block,
            GuardrailVerdict::Pass,
        )]);
        assert!(both.has_pre_call());
        assert!(both.has_post_call());
    }

    #[tokio::test]
    async fn test_run_post_call_runs_post_call_and_both_guardrails() {
        let engine = GuardrailEngine::new(vec![
            mock_entry(
                "post",
                GuardrailMode::PostCall,
                true,
                GuardrailAction::Block,
                GuardrailVerdict::Pass,
            ),
            mock_entry(
                "both",
                GuardrailMode::Both,
                true,
                GuardrailAction::Block,
                GuardrailVerdict::Pass,
            ),
            mock_entry(
                "pre-only",
                GuardrailMode::PreCall,
                true,
                GuardrailAction::Block,
                GuardrailVerdict::Block {
                    guardrail: "pre-only".into(),
                    reason: "must not run".into(),
                },
            ),
        ]);
        let output = GuardrailOutput {
            content: "hello",
            model: "gpt-4",
        };
        let (verdict, applied) = engine.run_post_call(&output, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert_eq!(applied, vec!["post".to_string(), "both".to_string()]);
    }

    #[tokio::test]
    async fn test_run_post_call_returns_the_first_block() {
        let engine = GuardrailEngine::new(vec![mock_entry(
            "blocker",
            GuardrailMode::PostCall,
            true,
            GuardrailAction::Block,
            GuardrailVerdict::Block {
                guardrail: "blocker".into(),
                reason: "nope".into(),
            },
        )]);
        let output = GuardrailOutput {
            content: "hello",
            model: "gpt-4",
        };
        let (verdict, applied) = engine.run_post_call(&output, None, None).await;
        match verdict {
            GuardrailVerdict::Block { guardrail, reason } => {
                assert_eq!(guardrail, "blocker");
                assert_eq!(reason, "nope");
            }
            other => panic!("expected Block, got {:?}", other),
        }
        assert_eq!(applied, vec!["blocker".to_string()]);
    }

    #[tokio::test]
    async fn test_run_post_call_applies_content_modification() {
        let engine = GuardrailEngine::new(vec![mock_entry(
            "redactor",
            GuardrailMode::PostCall,
            true,
            GuardrailAction::Redact,
            GuardrailVerdict::Modify {
                guardrail: "redactor".into(),
                content: "REDACTED".into(),
                reason: "pii".into(),
            },
        )]);
        let output = GuardrailOutput {
            content: "secret text",
            model: "gpt-4",
        };
        let (verdict, applied) = engine.run_post_call(&output, None, None).await;
        match verdict {
            GuardrailVerdict::Modify { content, .. } => assert_eq!(content, "REDACTED"),
            other => panic!("expected Modify, got {:?}", other),
        }
        assert_eq!(applied, vec!["redactor".to_string()]);
    }

    #[tokio::test]
    async fn test_run_post_call_passes_unmodified_content_through() {
        let engine = GuardrailEngine::new(vec![mock_entry(
            "passer",
            GuardrailMode::PostCall,
            true,
            GuardrailAction::Block,
            GuardrailVerdict::Pass,
        )]);
        let output = GuardrailOutput {
            content: "unchanged",
            model: "gpt-4",
        };
        let (verdict, applied) = engine.run_post_call(&output, None, None).await;
        assert!(matches!(verdict, GuardrailVerdict::Pass));
        assert_eq!(applied, vec!["passer".to_string()]);
    }

    #[test]
    fn test_extract_response_content_joins_message_contents() {
        let body = serde_json::json!({
            "choices": [
                {"message": {"content": "first"}},
                {"message": {"content": "second"}},
            ]
        });
        assert_eq!(extract_response_content(&body), "first\nsecond");
        assert_eq!(extract_response_content(&serde_json::json!({})), "");
    }

    #[test]
    fn test_extract_response_content_joins_delta_contents() {
        let body = serde_json::json!({
            "choices": [
                {"delta": {"content": "chunk1"}},
                {"delta": {"content": "chunk2"}},
            ]
        });
        assert_eq!(extract_response_content(&body), "chunk1\nchunk2");
    }

    #[test]
    fn test_extract_response_content_reads_both_shapes() {
        let body = serde_json::json!({
            "choices": [
                {"message": {"content": "full"}},
                {"delta": {"content": "part"}},
            ]
        });
        assert_eq!(extract_response_content(&body), "full\npart");
    }
}
