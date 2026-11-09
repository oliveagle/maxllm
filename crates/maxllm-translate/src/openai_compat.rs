// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Configurable passthrough translator for OpenAI-compatible providers.
//!
//! Many providers (Groq, Together AI, Fireworks, DeepInfra, Mistral, xAI,
//! DeepSeek, Ollama) expose an OpenAI-compatible chat completions API. This
//! module provides a single translator that can be configured with the
//! provider name, upstream path, and authentication style.

use crate::formats::OpenAIChatRequest;
use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};

/// Authentication style for an OpenAI-compatible provider.
#[derive(Debug, Clone)]
pub enum AuthStyle {
    /// `Authorization: Bearer {key}`
    Bearer,
    /// Custom header: `{header_name}: {key}`
    ApiKey(String),
    /// Query parameter: `?{param}={key}` (appended by the gateway)
    QueryParam(String),
    /// No authentication (e.g., local Ollama).
    None,
}

/// A configurable passthrough translator for OpenAI-compatible providers.
///
/// Request and response bodies are passed through as-is (they already use
/// OpenAI format), with optional model override support. Only the upstream
/// path and authentication headers differ.
pub struct OpenAICompatTranslator {
    provider_name: String,
    upstream_path: String,
    auth_style: AuthStyle,
    native_responses_repair: NativeResponsesRepair,
}

/// A repair a provider needs on its **native Responses** request body before
/// the gateway forwards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeResponsesRepair {
    /// Forward the body verbatim (the default).
    None,
    /// Drop trailing assistant text turns.
    ///
    /// Volcengine Ark's Responses prefill validation rejects an `input` list
    /// whose last item is an assistant text turn, answering
    /// `400 {"code":"MissingParameter","param":"partial", …}` — a misleading
    /// error, because `partial` is not a request parameter (adding
    /// `{"partial":true}` changes nothing). Codex remote compaction ends its
    /// input exactly that way, so a long session 400s on every compaction.
    /// Dropping the trailing text turn restores the last user turn as the
    /// prefill anchor; mid-history assistant turns and tool turns are left
    /// alone.
    ArkTrailingAssistantTurn,
}

impl OpenAICompatTranslator {
    pub fn new(
        provider_name: impl Into<String>,
        upstream_path: impl Into<String>,
        auth_style: AuthStyle,
    ) -> Self {
        Self {
            provider_name: provider_name.into(),
            upstream_path: upstream_path.into(),
            auth_style,
            native_responses_repair: NativeResponsesRepair::None,
        }
    }

    /// Opt this provider into a native-Responses body repair.
    pub fn with_native_responses_repair(mut self, repair: NativeResponsesRepair) -> Self {
        self.native_responses_repair = repair;
        self
    }

    /// Pre-configured translator for Groq.
    pub fn groq() -> Self {
        Self::new("groq", "/openai/v1/chat/completions", AuthStyle::Bearer)
    }

    /// Pre-configured translator for Together AI.
    pub fn together() -> Self {
        Self::new("together", "/v1/chat/completions", AuthStyle::Bearer)
    }

    /// Pre-configured translator for Fireworks AI.
    pub fn fireworks() -> Self {
        Self::new(
            "fireworks",
            "/inference/v1/chat/completions",
            AuthStyle::Bearer,
        )
    }

    /// Pre-configured translator for DeepInfra.
    pub fn deepinfra() -> Self {
        Self::new(
            "deepinfra",
            "/v1/openai/chat/completions",
            AuthStyle::Bearer,
        )
    }

    /// Pre-configured translator for Mistral.
    pub fn mistral() -> Self {
        Self::new("mistral", "/v1/chat/completions", AuthStyle::Bearer)
    }

    /// Pre-configured translator for xAI (Grok).
    pub fn xai() -> Self {
        Self::new("xai", "/v1/chat/completions", AuthStyle::Bearer)
    }

    /// Pre-configured translator for DeepSeek.
    pub fn deepseek() -> Self {
        Self::new("deepseek", "/chat/completions", AuthStyle::Bearer)
    }

    /// Pre-configured translator for local Ollama.
    pub fn ollama() -> Self {
        Self::new("ollama", "/v1/chat/completions", AuthStyle::None)
    }
}

impl ProviderTranslator for OpenAICompatTranslator {
    fn name(&self) -> &str {
        &self.provider_name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn translate_request(
        &self,
        body: &[u8],
        model_override: Option<&str>,
    ) -> Result<TranslatedRequest, TranslateError> {
        if let Some(model) = model_override {
            let mut req: OpenAIChatRequest = serde_json::from_slice(body)?;
            req.model = model.to_string();
            let is_streaming = req.stream.unwrap_or(false);
            Ok(TranslatedRequest {
                body: serde_json::to_vec(&req)?,
                is_streaming,
            })
        } else {
            let req: OpenAIChatRequest = serde_json::from_slice(body)?;
            let is_streaming = req.stream.unwrap_or(false);
            Ok(TranslatedRequest {
                body: body.to_vec(),
                is_streaming,
            })
        }
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        // Already in OpenAI format.
        Ok(body.to_vec())
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        Box::new(OpenAICompatPassthroughStream)
    }

    fn upstream_path(&self) -> &str {
        &self.upstream_path
    }

    fn normalize_native_responses_request(&self, body: &[u8]) -> Vec<u8> {
        match self.native_responses_repair {
            NativeResponsesRepair::None => body.to_vec(),
            NativeResponsesRepair::ArkTrailingAssistantTurn => drop_trailing_assistant_turns(body),
        }
    }

    fn repairs_native_responses_body(&self) -> bool {
        self.native_responses_repair != NativeResponsesRepair::None
    }

    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)> {
        let mut headers = vec![("Content-Type".to_string(), "application/json".to_string())];
        match &self.auth_style {
            AuthStyle::Bearer => {
                headers.push(("Authorization".to_string(), format!("Bearer {api_key}")));
            }
            AuthStyle::ApiKey(header_name) => {
                headers.push((header_name.clone(), api_key.to_string()));
            }
            AuthStyle::QueryParam(_) => {
                // Query params are appended by the gateway to the URL, not as headers.
            }
            AuthStyle::None => {}
        }
        headers
    }
}

/// Passthrough SSE stream (already in OpenAI format).
struct OpenAICompatPassthroughStream;

impl StreamTranslator for OpenAICompatPassthroughStream {
    fn process_chunk(&mut self, data: &[u8], _end_of_stream: bool) -> Vec<u8> {
        data.to_vec()
    }
}

/// Remove trailing assistant message items from a native Responses `input`
/// list — the repair ark's prefill validator needs.
///
/// Returns the input bytes unchanged when the body is not JSON, when `input`
/// is absent or not an array (the plain-string prompt form), when the last
/// item is not an assistant message, or when dropping would empty the list
/// (the upstream would then reject an empty input, a worse failure than the
/// one being repaired).
///
/// Only the **last** item matters upstream: an assistant turn followed by a
/// user turn, a tool result or even a reasoning item is accepted, so completed
/// turns in the middle of the history are never touched.
pub fn drop_trailing_assistant_turns(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.to_vec();
    };
    let Some(obj) = root.as_object_mut() else {
        return body.to_vec();
    };
    let Some(items) = obj.get_mut("input").and_then(|v| v.as_array_mut()) else {
        return body.to_vec();
    };
    let original_len = items.len();
    while items.last().map(is_assistant_message_item).unwrap_or(false) {
        items.pop();
    }
    if items.len() == original_len || items.is_empty() {
        return body.to_vec();
    }
    serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
}

/// Whether one Responses input item is an assistant **message** — the only
/// shape ark's prefill validator refuses to end on.
///
/// The content is deliberately not inspected. Probed against the live endpoint
/// on 2026-11-09, ark rejects every trailing assistant content form, with two
/// equally fatal dialects:
///
/// * text / plain string / `[]` / `null` / absent -> `MissingParameter partial`
/// * refusal part, image part, text+refusal      -> `MissingParameter input.content.text`
///
/// `function_call`, `function_call_output` and `reasoning` carry no `role`, so
/// they never match and are left in place (a trailing one is accepted upstream).
fn is_assistant_message_item(item: &serde_json::Value) -> bool {
    let Some(obj) = item.as_object() else {
        return false;
    };
    let msg_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let role = obj.get("role").and_then(|v| v.as_str()).unwrap_or("");
    // Responses message items carry type "message"; tolerate a missing type
    // when the role is unambiguous.
    role == "assistant" && (msg_type.is_empty() || msg_type == "message")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough_request() {
        let body = br#"{"model":"llama-3.1-70b","messages":[{"role":"user","content":"Hi"}]}"#;
        let t = OpenAICompatTranslator::groq();
        let result = t.translate_request(body, None).unwrap();
        assert_eq!(result.body, body.to_vec());
        assert!(!result.is_streaming);
    }

    #[test]
    fn test_model_override() {
        let body = br#"{"model":"llama-3.1-70b","messages":[{"role":"user","content":"Hi"}]}"#;
        let t = OpenAICompatTranslator::together();
        let result = t.translate_request(body, Some("llama-3.1-8b")).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(parsed["model"], "llama-3.1-8b");
    }

    #[test]
    fn test_bearer_headers() {
        let t = OpenAICompatTranslator::groq();
        let headers = t.upstream_headers("gsk_test");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer gsk_test"));
    }

    #[test]
    fn test_api_key_header() {
        let t = OpenAICompatTranslator::new(
            "custom",
            "/v1/chat",
            AuthStyle::ApiKey("X-Api-Key".to_string()),
        );
        let headers = t.upstream_headers("my-key");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "X-Api-Key" && v == "my-key"));
        assert!(!headers.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn test_no_auth_headers() {
        let t = OpenAICompatTranslator::ollama();
        let headers = t.upstream_headers("");
        // Should only have Content-Type
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "Content-Type");
    }

    #[test]
    fn test_provider_names() {
        assert_eq!(OpenAICompatTranslator::groq().name(), "groq");
        assert_eq!(OpenAICompatTranslator::together().name(), "together");
        assert_eq!(OpenAICompatTranslator::fireworks().name(), "fireworks");
        assert_eq!(OpenAICompatTranslator::deepinfra().name(), "deepinfra");
        assert_eq!(OpenAICompatTranslator::mistral().name(), "mistral");
        assert_eq!(OpenAICompatTranslator::xai().name(), "xai");
        assert_eq!(OpenAICompatTranslator::deepseek().name(), "deepseek");
        assert_eq!(OpenAICompatTranslator::ollama().name(), "ollama");
    }

    #[test]
    fn test_upstream_paths() {
        assert_eq!(
            OpenAICompatTranslator::groq().upstream_path(),
            "/openai/v1/chat/completions"
        );
        assert_eq!(
            OpenAICompatTranslator::deepseek().upstream_path(),
            "/chat/completions"
        );
    }

    #[test]
    fn test_streaming_detection() {
        let body =
            br#"{"model":"mixtral","messages":[{"role":"user","content":"Hi"}],"stream":true}"#;
        let t = OpenAICompatTranslator::mistral();
        let result = t.translate_request(body, None).unwrap();
        assert!(result.is_streaming);
    }

    fn ark() -> OpenAICompatTranslator {
        OpenAICompatTranslator::new(
            "volc_1",
            "/api/coding/v3/chat/completions",
            AuthStyle::Bearer,
        )
        .with_native_responses_repair(NativeResponsesRepair::ArkTrailingAssistantTurn)
    }

    fn input_of(body: &[u8]) -> serde_json::Value {
        let parsed: serde_json::Value = serde_json::from_slice(body).unwrap();
        parsed["input"].clone()
    }

    #[test]
    fn test_ark_repair_drops_trailing_assistant_text_turn() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"summarise"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"draft"}]}
        ]}"#;
        let t = ark();
        assert!(t.repairs_native_responses_body());
        let out = t.normalize_native_responses_request(body);
        let input = input_of(&out);
        assert_eq!(input.as_array().unwrap().len(), 1);
        assert_eq!(input[0]["role"], "user");
    }

    #[test]
    fn test_ark_repair_drops_string_form_assistant_turn() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":"go"},
            {"type":"message","role":"assistant","content":"draft"}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(input_of(&out).as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_ark_repair_is_noop_for_mid_history_assistant_turn() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},
            {"type":"message","role":"user","content":[{"type":"input_text","text":"again"}]}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_ark_repair_is_noop_for_trailing_tool_result() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"function_call","call_id":"c1","name":"shell","arguments":"{}"},
            {"type":"function_call_output","call_id":"c1","output":"ok"}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_ark_repair_drops_trailing_refusal_turn() {
        // Probed live 2026-11-09: a trailing assistant turn whose part is a
        // refusal is rejected too, with the sibling
        // MissingParameter input.content.text. Every assistant content shape
        // must therefore be dropped, not just text.
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"no"}]}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(input_of(&out).as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_ark_repair_drops_trailing_image_turn() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"message","role":"assistant","content":[{"type":"input_image","image_url":"https://example.test/a.png"}]}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(input_of(&out).as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_ark_repair_is_noop_for_trailing_reasoning() {
        // ark accepts an assistant turn as long as something else trails it,
        // and a reasoning item alone is accepted — so it must survive.
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},
            {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"t"}],"content":[]}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_ark_repair_never_empties_the_input_list() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}
        ]}"#;
        let out = ark().normalize_native_responses_request(body);
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_default_translator_leaves_native_body_verbatim() {
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}
        ]}"#;
        let t = OpenAICompatTranslator::deepseek();
        assert!(!t.repairs_native_responses_body());
        assert_eq!(t.normalize_native_responses_request(body), body.to_vec());
    }
}
