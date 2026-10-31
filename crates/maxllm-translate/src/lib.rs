// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

pub mod anthropic;
pub mod anthropic_stream;
pub mod azure_openai;
pub mod bedrock;
pub mod cohere;
pub mod cohere_stream;
pub mod formats;
pub mod gemini;
pub mod gemini_stream;
pub mod openai;
pub mod openai_compat;
pub mod opencode;
pub mod responses;

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
