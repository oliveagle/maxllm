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
    /// Make every reasoning item carry a `reasoning_text`, and make every
    /// step that carries a tool call lead with one.
    ///
    /// DeepSeek's native Responses endpoint reads one step of the model
    /// (its reasoning, what it said and the calls it made, between a user
    /// message or a tool output and the next) as a single assistant message
    /// whose thinking is the reasoning item the step *starts* with. A step
    /// that starts with anything else — a call, or what the model said
    /// before the call — is refused with
    /// `400 The `reasoning_text` in the thinking mode must be passed back to
    /// the API` (verified live against `api.deepseek.com` on 2026-11-21:
    /// `[message, function_call]` answers 400, `[reasoning, message,
    /// function_call]` answers 200).
    ///
    /// The same refusal comes from a reasoning item that carries no
    /// reasoning text at all: codex replays a sealed item (only
    /// `encrypted_content`), a summary-only item, or a step whose reasoning
    /// belongs to another model in a mixed group. The repair gives every
    /// reasoning item a `reasoning_text` (its own text when it has one, its
    /// summary otherwise, a single space when it has neither) and moves the
    /// step's first reasoning item to its front, adding a placeholder item
    /// when the step has none.
    DeepSeekReasoningLead,
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
            NativeResponsesRepair::DeepSeekReasoningLead => with_reasoning_lead(body),
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

/// The reasoning text a step that has no reasoning of its own leads with.
///
/// A single space: the upstream only checks that the text is there and
/// non-empty, and a marker word would leak into the model's context.
const NO_REASONING: &str = " ";

/// Give every reasoning item of a native Responses request a `reasoning_text`,
/// and make every step that carries a tool call (or that follows a tool
/// output) lead with one.
///
/// DeepSeek's native Responses endpoint refuses a request whose step does not
/// start with a reasoning item that carries text — the reasoning of the
/// turn being continued has to be passed back — answering
/// `400 The `reasoning_text` in the thinking mode must be passed back to the
/// API`. Three shapes reach it that way:
///
/// * a step that starts with an assistant message or a tool call, because
///   codex replays the model's turns as separate items and puts the
///   reasoning item after the message it belongs to;
/// * a reasoning item that carries no text — codex replays it as it got it,
///   and it got it from another model in a mixed group (summary only), from
///   a sealed backend (`encrypted_content` only) or from a model that did
///   not think at all;
/// * a step that follows a tool output and does not start with a reasoning
///   item.
///
/// The repair gives every reasoning item a `reasoning_text` (its own text
/// when it has one, its summary otherwise, a single space when it has
/// neither) and moves the step's first reasoning item to the front of the
/// step, adding a placeholder item when the step has none.
///
/// A body that needs neither change comes back byte for byte.
pub fn with_reasoning_lead(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.to_vec();
    };
    let Some(obj) = root.as_object_mut() else {
        return body.to_vec();
    };
    let Some(items) = obj.get_mut("input").and_then(|v| v.as_array_mut()) else {
        return body.to_vec();
    };

    let mut changed = false;
    let mut kinds: Vec<StepKind> = Vec::with_capacity(items.len());
    for item in items.iter_mut() {
        let (kind, is_reasoning) = step_kind_of(item);
        kinds.push(kind);
        if !is_reasoning {
            continue;
        }
        if has_reasoning_text(item) {
            continue;
        }
        let text = summary_text(item).unwrap_or_else(|| NO_REASONING.to_string());
        if let Some(item_obj) = item.as_object_mut() {
            item_obj.insert(
                "content".to_string(),
                serde_json::json!([{ "type": "reasoning_text", "text": text }]),
            );
            changed = true;
        }
    }

    if let Some(led) = reason_first(items, &kinds) {
        *items = led;
        changed = true;
    }

    if !changed {
        return body.to_vec();
    }
    serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
}

/// What one Responses input item is to a step of the model's turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepKind {
    /// The model's own reasoning.
    Reasoning,
    /// Something the model said.
    Said,
    /// A call the model made.
    Call,
    /// What answered a call.
    Output,
    /// A message of the user (or the system) — the start of a turn.
    Turn,
    /// Anything else.
    Other,
}

/// Classify one Responses input item.
fn step_kind_of(item: &serde_json::Value) -> (StepKind, bool) {
    let Some(obj) = item.as_object() else {
        return (StepKind::Other, false);
    };
    let item_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let role = obj.get("role").and_then(|v| v.as_str()).unwrap_or("");
    let kind = match item_type {
        "reasoning" => StepKind::Reasoning,
        t if t.ends_with("_output") => StepKind::Output,
        t if t.ends_with("_call") => StepKind::Call,
        "message" => {
            if role == "assistant" {
                StepKind::Said
            } else {
                StepKind::Turn
            }
        }
        "" => {
            if role == "assistant" {
                StepKind::Said
            } else if !role.is_empty() {
                StepKind::Turn
            } else {
                StepKind::Other
            }
        }
        _ => StepKind::Other,
    };
    (kind, item_type == "reasoning")
}

/// Whether a reasoning item already carries a non-empty `reasoning_text`.
fn has_reasoning_text(item: &serde_json::Value) -> bool {
    item.get("content")
        .and_then(|c| c.as_array())
        .map(|parts| {
            parts.iter().any(|part| {
                part.get("type").and_then(|t| t.as_str()) == Some("reasoning_text")
                    && part
                        .get("text")
                        .and_then(|t| t.as_str())
                        .map(|t| !t.is_empty())
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// The summary text of a reasoning item, when it has one.
fn summary_text(item: &serde_json::Value) -> Option<String> {
    let summary = item.get("summary")?.as_array()?;
    let mut text = String::new();
    for part in summary {
        if let Some(t) = part.get("text").and_then(|t| t.as_str()) {
            text.push_str(t);
        }
    }
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// The reasoning item a step leads with when it has none of its own.
fn placeholder_reasoning_item() -> serde_json::Value {
    serde_json::json!({
        "type": "reasoning",
        "summary": [],
        "content": [{ "type": "reasoning_text", "text": NO_REASONING }],
    })
}

/// Move the first reasoning item of every step that needs one to the front
/// of that step.
///
/// A step is a run of the model's reasoning, what it said and the calls it
/// made, between a user message or a tool output and the next. A step that
/// makes a call, or that follows a tool output, has to lead with a
/// reasoning item: the upstream reads the step as one assistant message
/// whose thinking is the item it starts with. The step's own first
/// reasoning item is moved ahead of the rest; a step without one gets the
/// placeholder.
///
/// Returns `None` when every such step already leads with a reasoning item.
fn reason_first(items: &[serde_json::Value], kinds: &[StepKind]) -> Option<Vec<serde_json::Value>> {
    let is_step =
        |kind: StepKind| matches!(kind, StepKind::Reasoning | StepKind::Said | StepKind::Call);
    let mut led: Vec<serde_json::Value> = Vec::with_capacity(items.len() + 1);
    let mut changed = false;
    let mut after_output = false;
    let mut i = 0usize;
    while i < kinds.len() {
        if !is_step(kinds[i]) {
            match kinds[i] {
                StepKind::Output => after_output = true,
                StepKind::Turn => after_output = false,
                _ => {}
            }
            if changed {
                led.push(items[i].clone());
            }
            i += 1;
            continue;
        }
        let start = i;
        let mut end = i;
        let mut has_call = false;
        let mut first_reasoning: Option<usize> = None;
        while end < kinds.len() && is_step(kinds[end]) {
            has_call = has_call || kinds[end] == StepKind::Call;
            if kinds[end] == StepKind::Reasoning && first_reasoning.is_none() {
                first_reasoning = Some(end);
            }
            end += 1;
        }
        let needs_lead = (has_call || after_output) && first_reasoning != Some(start);
        if needs_lead {
            if !changed {
                led = items[..start].to_vec();
                changed = true;
            }
            match first_reasoning {
                None => led.push(placeholder_reasoning_item()),
                Some(idx) => led.push(items[idx].clone()),
            }
            for (x, item) in items.iter().enumerate().take(end).skip(start) {
                if Some(x) != first_reasoning {
                    led.push(item.clone());
                }
            }
        } else if changed {
            led.extend_from_slice(&items[start..end]);
        }
        i = end;
    }
    if changed {
        Some(led)
    } else {
        None
    }
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
    fn test_deepseek_reasoning_lead_puts_a_reasoning_item_in_front() {
        // The shape DeepSeek refuses: the step starts with what the model
        // said and only then makes the call.
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"let me look"}]},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"}
        ]}"#;
        let out = with_reasoning_lead(body);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let input = parsed["input"].as_array().unwrap();
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["content"][0]["type"], "reasoning_text");
        assert_eq!(input[1]["content"][0]["text"], " ");
        assert_eq!(input[2]["type"], "message");
        assert_eq!(input[3]["type"], "function_call");
    }

    #[test]
    fn test_deepseek_reasoning_lead_moves_an_existing_reasoning_item_ahead() {
        // Codex replays the model's turn with the reasoning item behind the
        // message it belongs to; the upstream wants it in front.
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"let me look"}]},
            {"type":"reasoning","id":"rs_1","summary":[],"content":[{"type":"reasoning_text","text":"thinking"}]},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"}
        ]}"#;
        let out = with_reasoning_lead(body);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let input = parsed["input"].as_array().unwrap();
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["content"][0]["text"], "thinking");
        assert_eq!(input[2]["type"], "message");
        assert_eq!(input[3]["type"], "function_call");
    }

    #[test]
    fn test_deepseek_reasoning_lead_gives_a_summary_only_item_its_text() {
        // A summary-only item (translated from another model) has no
        // reasoning_text; the upstream refuses the whole request over it.
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"thinking"}]},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"}
        ]}"#;
        let out = with_reasoning_lead(body);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let input = parsed["input"].as_array().unwrap();
        assert_eq!(input[1]["content"][0]["type"], "reasoning_text");
        assert_eq!(input[1]["content"][0]["text"], "thinking");
    }

    #[test]
    fn test_deepseek_reasoning_lead_gives_a_sealed_item_a_placeholder() {
        // A sealed item (encrypted_content only) is refused as well.
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"opaque"},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"}
        ]}"#;
        let out = with_reasoning_lead(body);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let input = parsed["input"].as_array().unwrap();
        assert_eq!(input[1]["content"][0]["type"], "reasoning_text");
        assert_eq!(input[1]["content"][0]["text"], " ");
        assert_eq!(input[1]["encrypted_content"], "opaque");
    }

    #[test]
    fn test_deepseek_reasoning_lead_leads_a_step_after_a_tool_output() {
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"},
            {"type":"function_call_output","call_id":"call_1","output":"12:00"},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"it is noon"}]}
        ]}"#;
        let out = with_reasoning_lead(body);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let input = parsed["input"].as_array().unwrap();
        // The call step leads with a reasoning item, and so does the step
        // that follows the tool output.
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[4]["type"], "reasoning");
        assert_eq!(input[5]["type"], "message");
    }

    #[test]
    fn test_deepseek_reasoning_lead_leaves_a_plain_body_alone() {
        let body = br#"{"model":"deepseek-flash","input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#;
        assert_eq!(with_reasoning_lead(body), body.to_vec());
    }

    #[test]
    fn test_deepseek_reasoning_lead_leaves_a_leading_reasoning_step_alone() {
        let body = br#"{"model":"deepseek-flash","input":[
            {"role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"reasoning","id":"rs_1","summary":[],"content":[{"type":"reasoning_text","text":"thinking"}]},
            {"type":"function_call","name":"get_time","arguments":"{}","call_id":"call_1"}
        ]}"#;
        assert_eq!(with_reasoning_lead(body), body.to_vec());
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
