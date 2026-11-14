// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

pub mod anthropic;

use serde_json::Value;
pub mod anthropic_stream;
pub mod aws_sigv4;
pub mod azure_openai;
pub mod bedrock;
pub mod cohere;
pub mod cohere_stream;
pub mod commandcode;
pub mod formats;
pub mod gemini;
pub mod gemini_stream;
pub mod moonshot;
pub mod open_design;
pub mod openai;
pub mod openai_compat;
pub mod opencode;
pub mod responses;
/// Extract human-readable message content from a provider-native request body.
/// Used by pre-call guardrails.

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("translation error: {0}")]
    Translation(String),
}

pub struct TranslatedRequest {
    pub body: Vec<u8>,
    pub is_streaming: bool,
}

/// Trait for translating requests/responses between OpenAI canonical format
/// and a provider's native format.
pub trait ProviderTranslator: Send + Sync {
    fn name(&self) -> &str;

    /// Downcast support for provider-specific operations.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Translate OpenAI-format request body → provider-native request bytes.
    fn translate_request(
        &self,
        body: &[u8],
        model_override: Option<&str>,
    ) -> Result<TranslatedRequest, TranslateError>;

    /// Translate provider-native response bytes → OpenAI-format response bytes.
    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError>;

    /// Create a streaming translator for SSE chunks (provider → OpenAI SSE).
    fn streaming_translator(&self) -> Box<dyn StreamTranslator>;

    /// Provider-specific upstream path (e.g. "/v1/messages" for Anthropic).
    fn upstream_path(&self) -> &str;

    /// Provider-specific headers to set on the upstream request.
    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)>;

    /// Provider-specific repairs applied to a **native Responses** request
    /// body before it is forwarded upstream.
    ///
    /// A native Responses body already speaks the upstream's own contract,
    /// so the default is the identity function: the body is forwarded as it
    /// arrived. A provider whose upstream rejects part of the Responses
    /// contract (e.g. the non-standard `tool_choice: "any"`) overrides this
    /// to repair the body before it is sent.
    ///
    /// A repair may change the body's size, so the gateway forwards a
    /// repaired body chunked instead of with the client's Content-Length —
    /// see [`Self::repairs_native_responses_body`].
    fn normalize_native_responses_request(&self, body: &[u8]) -> Vec<u8> {
        body.to_vec()
    }

    /// Whether this provider repairs native Responses request bodies.
    ///
    /// The gateway must pick the request framing (Content-Length vs
    /// chunked) before it has read the body, so it cannot learn about the
    /// repair from the body itself: a provider that repairs the body
    /// declares it here, and the gateway forwards that provider's native
    /// Responses requests chunked — the repaired body no longer matches the
    /// client's Content-Length.
    fn repairs_native_responses_body(&self) -> bool {
        false
    }
}

/// Incremental SSE stream translator.
pub trait StreamTranslator: Send {
    fn process_chunk(&mut self, data: &[u8], end_of_stream: bool) -> Vec<u8>;
}

/// Pass-through stream translator for native mode — forwards SSE chunks unchanged.
pub struct NativePassthroughStream;

impl StreamTranslator for NativePassthroughStream {
    fn process_chunk(&mut self, data: &[u8], _end_of_stream: bool) -> Vec<u8> {
        data.to_vec()
    }
}

/// The sentinel an orphaned tool call is answered with.
///
/// Truthful, not inventive: the call's result was never recorded, so the
/// conversation says so instead of fabricating one.
const ORPHAN_TOOL_CALL_SENTINEL: &str = "[tool call interrupted: no result was recorded]";

/// Repair a history that carries an orphaned tool call.
///
/// An upstream that validates tool-call pairing (Kimi:
/// `an assistant message with 'tool_calls' must be followed by tool messages
/// responding to each 'tool_call_id'`) answers a body whose assistant call
/// was never answered with a **400** — a request fault, and one that ends an
/// agent's turn even though the fault was produced by the conversation's
/// own history (a compaction that kept the call and dropped its result).
///
/// The repair answers every orphaned call with a sentinel output, so the
/// upstream accepts the history and the turn continues. Returns `None` when
/// the body has no orphan to repair.
///
/// Supported shapes: the Responses contract (`input` items, `function_call`
/// / `function_call_output` and their `custom_tool_call` twins) and the Chat
/// contract (`messages` with assistant `tool_calls` and `role: "tool"`
/// replies). A body without either array is left alone.
pub fn repair_orphan_tool_calls(body: &[u8]) -> Option<Vec<u8>> {
    let Ok(mut root) = serde_json::from_slice::<serde_json::Value>(body) else {
        return None;
    };

    if let Some(items) = root.get_mut("input").and_then(|i| i.as_array_mut()) {
        if repair_responses_items(items) {
            return serde_json::to_vec(&root).ok();
        }
        return None;
    }
    if let Some(messages) = root.get_mut("messages").and_then(|m| m.as_array_mut()) {
        if repair_chat_messages(messages) {
            return serde_json::to_vec(&root).ok();
        }
        return None;
    }
    None
}

/// Shrink a Responses-shaped history by removing orphaned tool calls.
///
/// The last resort of [`shrink_orphan_tool_calls`]: an unanswered call is
/// dropped outright, so the pairing rule has nothing left to reject.
pub fn remove_orphan_tool_calls(body: &[u8]) -> Option<Vec<u8>> {
    let Ok(mut root) = serde_json::from_slice::<serde_json::Value>(body) else {
        return None;
    };

    if let Some(items) = root.get_mut("input").and_then(|i| i.as_array_mut()) {
        let orphans = orphaned_responses_calls(items);
        if orphans.is_empty() {
            return None;
        }
        for index in orphans.into_iter().rev() {
            items.remove(index);
        }
        return serde_json::to_vec(&root).ok();
    }
    None
}

/// Shrink a history with orphaned tool calls: first answer the orphans, and
/// when the answered history would not fit the frame the upstream reads
/// (a native Responses body must not grow), drop the orphans instead.
///
/// Returns the smallest repair that fits `max_len`, or `None` when the body
/// has no orphan at all.
pub fn shrink_orphan_tool_calls(body: &[u8], max_len: usize) -> Option<Vec<u8>> {
    if let Some(repaired) = repair_orphan_tool_calls(body) {
        if repaired.len() <= max_len {
            return Some(repaired);
        }
    }
    remove_orphan_tool_calls(body).filter(|repaired| repaired.len() <= max_len)
}

/// The indexes of the Responses items whose call was never answered.
///
/// A call is answered by a later output carrying the same `call_id`; the
/// walk keeps the ids of the calls still waiting for an answer.
fn orphaned_responses_calls(items: &[Value]) -> Vec<usize> {
    let mut pending: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut calls: Vec<usize> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call" | "custom_tool_call") => {
                calls.push(index);
                if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                    pending.insert(id.to_string());
                }
            }
            Some("function_call_output" | "custom_tool_call_output") => {
                if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                    pending.remove(id);
                }
            }
            _ => {}
        }
    }
    calls
        .into_iter()
        .filter(|index| {
            items[*index]
                .get("call_id")
                .and_then(Value::as_str)
                .is_some_and(|id| pending.contains(id))
        })
        .collect()
}

/// Answer the orphaned calls of a Responses `input` item list.
///
/// A call is orphaned when no later item answers its `call_id`; the repair
/// appends the sentinel output directly after it.
fn repair_responses_items(items: &mut Vec<Value>) -> bool {
    let orphan_indexes = orphaned_responses_calls(items);
    if orphan_indexes.is_empty() {
        return false;
    }
    let mut repaired: Vec<Value> = Vec::with_capacity(items.len() + orphan_indexes.len());
    let mut remaining = orphan_indexes.as_slice();
    for (index, item) in items.iter().enumerate() {
        repaired.push(item.clone());
        if remaining.first() == Some(&index) {
            remaining = &remaining[1..];
            let id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
            repaired.push(match item.get("type").and_then(Value::as_str) {
                Some("custom_tool_call") => serde_json::json!({
                    "type": "custom_tool_call_output",
                    "call_id": id,
                    "output": ORPHAN_TOOL_CALL_SENTINEL,
                }),
                _ => serde_json::json!({
                    "type": "function_call_output",
                    "call_id": id,
                    "output": ORPHAN_TOOL_CALL_SENTINEL,
                }),
            });
        }
    }
    *items = repaired;
    true
}

/// Answer the orphaned calls of a chat `messages` list.
///
/// An assistant `tool_calls` entry whose `id` no later `role: "tool"`
/// message repeats is answered with a sentinel tool message right after it.
fn repair_chat_messages(messages: &mut Vec<Value>) -> bool {
    let mut pending: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut orphans: Vec<(usize, String)> = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if message.get("role").and_then(Value::as_str) == Some("assistant") {
            if let Some(calls) = message.get("tool_calls").and_then(|c| c.as_array()) {
                for call in calls {
                    if let Some(id) = call.get("id").and_then(Value::as_str) {
                        orphans.push((index, id.to_string()));
                        pending.insert(id.to_string());
                    }
                }
            }
        } else if message.get("role").and_then(Value::as_str) == Some("tool") {
            if let Some(id) = message.get("tool_call_id").and_then(Value::as_str) {
                pending.remove(id);
            }
        }
    }
    let orphan_ids: Vec<(usize, String)> = orphans
        .into_iter()
        .filter(|(_, id)| pending.contains(id))
        .collect();
    if orphan_ids.is_empty() {
        return false;
    }
    let mut repaired: Vec<Value> = Vec::with_capacity(messages.len() + orphan_ids.len());
    let mut remaining = orphan_ids.as_slice();
    for (index, message) in messages.iter().enumerate() {
        repaired.push(message.clone());
        while remaining.first().map(|(i, _)| i) == Some(&index) {
            let (_, id) = remaining.first().expect("the index matched above").clone();
            remaining = &remaining[1..];
            repaired.push(serde_json::json!({
                "role": "tool",
                "tool_call_id": id,
                "content": ORPHAN_TOOL_CALL_SENTINEL,
            }));
        }
    }
    *messages = repaired;
    true
}

/// Whether an upstream error body blames an orphaned tool call.
///
/// The strict-upstream phrasings answer a history whose assistant call was
/// never answered — the exact fault [`repair_orphan_tool_calls`] repairs:
///
/// * Kimi: `an assistant message with 'tool_calls' must be followed by tool
///   messages responding to each 'tool_call_id'. The following tool_call_ids
///   did not have response messages: …`
/// * Kimi (flat form): `tool_call_id … is not found`
/// * OpenAI: `No tool output found for function call …`
///
/// Deliberately narrow: a genuine request fault reaches the client exactly
/// as before.
pub fn body_reports_orphan_tool_call(body: &str) -> bool {
    const MARKERS: &[&str] = &[
        "did not have response messages",
        "tool_call_id",
        "no tool output found for function call",
    ];
    let lower = body.to_ascii_lowercase();
    MARKERS.iter().any(|m| lower.contains(m))
}

// ─── Native format helpers ──────────────────────────────────────────────

/// Extract model name from a provider-native request body.
pub fn extract_native_model(kind: &str, body: &serde_json::Value) -> Option<String> {
    match kind {
        // Gemini: model is in the URL path, not the body. Return None here.
        "gemini" => None,
        // Most providers (Anthropic, Cohere, OpenAI-compat) use "model" field.
        _ => body.get("model").and_then(|m| m.as_str()).map(String::from),
    }
}

/// Extract streaming flag from a provider-native request body.
pub fn extract_native_streaming(kind: &str, body: &serde_json::Value) -> bool {
    match kind {
        // Gemini: streaming is indicated by the endpoint suffix (:streamGenerateContent),
        // not a body field. Default to false; the gateway checks the URL separately.
        "gemini" => false,
        // Most providers use a "stream" boolean field.
        _ => body
            .get("stream")
            .and_then(|s| s.as_bool())
            .unwrap_or(false),
    }
}

/// Extract human-readable message content from a provider-native request body.
/// Used by guardrails to inspect content regardless of input format.
pub fn extract_native_content(kind: &str, body: &serde_json::Value) -> String {
    match kind {
        "anthropic" => extract_anthropic_content(body),
        "gemini" => extract_gemini_content(body),
        "cohere" => extract_cohere_content(body),
        // commandcode.ai speaks two contracts on one vendor: the OpenAI
        // Chat Completions shape and the Anthropic Messages shape (the route
        // that serves its Claude models). The body decides which one this
        // is: an Anthropic Messages body has "messages" and no "input".
        "commandcode" => {
            if body.get("messages").is_some() && body.get("input").is_none() {
                extract_anthropic_content(body)
            } else {
                extract_openai_content(body)
            }
        }
        // OpenAI-format and compatible providers
        _ => extract_openai_content(body),
    }
}

/// Extract content from Anthropic Messages API format.
/// Messages: `[{role, content: string | [{type: "text", text: "..."}]}]`
fn extract_anthropic_content(body: &serde_json::Value) -> String {
    let mut parts = Vec::new();

    // System prompt (string or array of content blocks)
    if let Some(system) = body.get("system") {
        if let Some(s) = system.as_str() {
            parts.push(s.to_string());
        } else if let Some(blocks) = system.as_array() {
            for block in blocks {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    parts.push(text.to_string());
                }
            }
        }
    }

    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if let Some(content) = msg.get("content") {
                if let Some(text) = content.as_str() {
                    parts.push(text.to_string());
                } else if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            parts.push(text.to_string());
                        }
                    }
                }
            }
        }
    }

    parts.join("\n")
}

/// Extract content from Gemini generateContent format.
/// Contents: `[{role, parts: [{text: "..."}]}]`
fn extract_gemini_content(body: &serde_json::Value) -> String {
    let mut parts = Vec::new();

    if let Some(contents) = body.get("contents").and_then(|c| c.as_array()) {
        for content in contents {
            if let Some(content_parts) = content.get("parts").and_then(|p| p.as_array()) {
                for part in content_parts {
                    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                        parts.push(text.to_string());
                    }
                }
            }
        }
    }

    // Also check systemInstruction
    if let Some(si) = body.get("systemInstruction") {
        if let Some(si_parts) = si.get("parts").and_then(|p| p.as_array()) {
            for part in si_parts {
                if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                    parts.push(text.to_string());
                }
            }
        }
    }

    parts.join("\n")
}

/// Extract content from Cohere chat format.
/// Messages: `[{role, content: "..."}]` or legacy `message` field.
fn extract_cohere_content(body: &serde_json::Value) -> String {
    let mut parts = Vec::new();

    // V2 format: messages array
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if let Some(text) = msg.get("content").and_then(|c| c.as_str()) {
                parts.push(text.to_string());
            }
        }
    }

    // Legacy format: single message field
    if let Some(message) = body.get("message").and_then(|m| m.as_str()) {
        parts.push(message.to_string());
    }

    // Preamble (system prompt)
    if let Some(preamble) = body.get("preamble").and_then(|p| p.as_str()) {
        parts.push(preamble.to_string());
    }

    parts.join("\n")
}

/// Extract content from OpenAI chat format.
/// Messages: `[{role, content: string | [{type: "text", text: "..."}]}]`
fn extract_openai_content(body: &serde_json::Value) -> String {
    let mut parts = Vec::new();

    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if let Some(content) = msg.get("content") {
                if let Some(text) = content.as_str() {
                    parts.push(text.to_string());
                } else if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            parts.push(text.to_string());
                        }
                    }
                }
            }
        }
    }

    // Also handle legacy "prompt" field
    if let Some(prompt) = body.get("prompt").and_then(|p| p.as_str()) {
        parts.push(prompt.to_string());
    }

    parts.join("\n")
}

/// Extract human-readable content from a provider-native response body.
/// Used by post-call guardrails.
pub fn extract_native_response_content(kind: &str, body: &serde_json::Value) -> String {
    match kind {
        "anthropic" => {
            // Anthropic: {content: [{type: "text", text: "..."}]}
            let mut parts = Vec::new();
            if let Some(content) = body.get("content").and_then(|c| c.as_array()) {
                for block in content {
                    if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                        parts.push(text.to_string());
                    }
                }
            }
            parts.join("\n")
        }
        "gemini" => {
            // Gemini: {candidates: [{content: {parts: [{text: "..."}]}}]}
            let mut parts = Vec::new();
            if let Some(candidates) = body.get("candidates").and_then(|c| c.as_array()) {
                for candidate in candidates {
                    if let Some(content_parts) = candidate
                        .get("content")
                        .and_then(|c| c.get("parts"))
                        .and_then(|p| p.as_array())
                    {
                        for part in content_parts {
                            if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                parts.push(text.to_string());
                            }
                        }
                    }
                }
            }
            parts.join("\n")
        }
        "cohere" => {
            // Cohere v2: {message: {content: [{type: "text", text: "..."}]}}
            let mut parts = Vec::new();
            if let Some(msg) = body.get("message") {
                if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                    for block in content {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            parts.push(text.to_string());
                        }
                    }
                }
            }
            // Legacy: {text: "..."}
            if let Some(text) = body.get("text").and_then(|t| t.as_str()) {
                parts.push(text.to_string());
            }
            parts.join("\n")
        }
        // OpenAI and compatible: {choices: [{message: {content: "..."}}]}
        _ => {
            let mut parts = Vec::new();
            if let Some(choices) = body.get("choices").and_then(|c| c.as_array()) {
                for choice in choices {
                    if let Some(text) = choice
                        .get("message")
                        .and_then(|m| m.get("content"))
                        .and_then(|c| c.as_str())
                    {
                        parts.push(text.to_string());
                    }
                }
            }
            parts.join("\n")
        }
    }
}

#[cfg(test)]
mod orphan_tool_call_tests {
    use super::*;

    #[test]
    fn an_unanswered_responses_call_is_answered() {
        // The shape Kimi refuses: the assistant called a tool and the
        // result was never recorded.
        let body = br#"{"model":"m","input":[
            {"type":"message","role":"user","content":"run it"},
            {"type":"function_call","call_id":"f:0","name":"shell","arguments":"{}"}
        ]}"#;

        let repaired = repair_orphan_tool_calls(body).expect("the orphan is repaired");
        let out: Value = serde_json::from_slice(&repaired).unwrap();
        let items = out["input"].as_array().unwrap();
        assert_eq!(items.len(), 3, "the sentinel output follows the call");
        assert_eq!(items[2]["type"], "function_call_output");
        assert_eq!(items[2]["call_id"], "f:0");
        assert_eq!(items[2]["output"], ORPHAN_TOOL_CALL_SENTINEL);

        // The repair is idempotent: a repaired history has no orphan left.
        assert!(repair_orphan_tool_calls(&repaired).is_none());
    }

    #[test]
    fn a_paired_call_is_left_alone() {
        let body = br#"{"model":"m","input":[
            {"type":"function_call","call_id":"f:0","name":"shell","arguments":"{}"},
            {"type":"function_call_output","call_id":"f:0","output":"done"}
        ]}"#;
        assert!(repair_orphan_tool_calls(body).is_none());
    }

    #[test]
    fn an_unanswered_chat_call_is_answered() {
        let body = br#"{"model":"m","messages":[
            {"role":"user","content":"run it"},
            {"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"shell","arguments":"{}"}}]}
        ]}"#;

        let repaired = repair_orphan_tool_calls(body).expect("the orphan is repaired");
        let out: Value = serde_json::from_slice(&repaired).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_1");
        assert_eq!(messages[2]["content"], ORPHAN_TOOL_CALL_SENTINEL);
    }

    #[test]
    fn a_paired_chat_call_is_left_alone() {
        let body = br#"{"model":"m","messages":[
            {"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"shell","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"call_1","content":"done"}
        ]}"#;
        assert!(repair_orphan_tool_calls(body).is_none());
    }

    #[test]
    fn a_body_without_a_history_is_left_alone() {
        assert!(repair_orphan_tool_calls(br#"{"model":"m"}"#).is_none());
        assert!(repair_orphan_tool_calls(b"not json").is_none());
        assert!(repair_orphan_tool_calls(br#"{"input":"hello"}"#).is_none());
    }

    #[test]
    fn the_last_resort_drops_the_orphan_instead_of_growing_the_body() {
        let body = br#"{"model":"m","input":[
            {"type":"function_call","call_id":"f:0","name":"shell","arguments":"{}"}
        ]}"#;

        // The answered repair grows the body — it does not fit the frame a
        // native Responses upstream reads, so the orphan is dropped.
        let shrunk = remove_orphan_tool_calls(body).expect("the orphan is dropped");
        let out: Value = serde_json::from_slice(&shrunk).unwrap();
        assert!(out["input"].as_array().unwrap().is_empty());

        let fitted = shrink_orphan_tool_calls(body, 32).expect("the smallest repair fits");
        assert!(fitted.len() <= 32, "the repair fits the frame");
        let out: Value = serde_json::from_slice(&fitted).unwrap();
        assert!(out["input"].as_array().unwrap().is_empty());
    }

    #[test]
    fn the_shrunk_repair_keeps_a_body_that_already_fits() {
        let body = br#"{"model":"m","input":[
            {"type":"function_call","call_id":"f:0","name":"shell","arguments":"{}"}
        ]}"#;
        let repaired = shrink_orphan_tool_calls(body, 4096).expect("the repair fits");
        let out: Value = serde_json::from_slice(&repaired).unwrap();
        assert_eq!(out["input"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn the_orphan_markers_are_narrow() {
        // Kimi's phrasing.
        assert!(body_reports_orphan_tool_call(
            "Invalid request: an assistant message with 'tool_calls' must be followed by tool messages responding to each 'tool_call_id'. The following tool_call_ids did not have response messages: f:0"
        ));
        assert!(body_reports_orphan_tool_call(
            "{\"error\":{\"message\":\"Invalid request: tool_call_id  is not found\"}}"
        ));
        assert!(body_reports_orphan_tool_call(
            "No tool output found for function call call_abc"
        ));
        // A genuine request fault is never mistaken for one.
        assert!(!body_reports_orphan_tool_call(
            "Invalid request: missing 'messages' parameter"
        ));
        assert!(!body_reports_orphan_tool_call(
            "tools.function.parameters is not a valid moonshot flavored json schema"
        ));
        assert!(!body_reports_orphan_tool_call(""));
    }

    #[test]
    fn the_marker_keeps_the_schema_rejection_distinct() {
        // The two repair classes must not shadow each other: the schema
        // rejection names the schemas, the orphan rejection names the call
        // ids.
        let schema = "tools.function.parameters is not a valid moonshot flavored json schema, details: <At path 'properties.show': detected infinite recursion without termination condition>";
        assert!(!body_reports_orphan_tool_call(schema));
    }
}
