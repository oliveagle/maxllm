// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Translator for AWS Bedrock (Claude models).
//!
//! Bedrock hosts Anthropic Claude models and uses the Anthropic Messages API
//! format for the request/response body. The key differences from direct
//! Anthropic access are:
//!
//! - **Endpoint**: `/model/{model_id}/invoke` or `/model/{model_id}/invoke-with-response-stream`
//! - **Auth**: AWS SigV4 signing (not API key). The signature covers the
//!   SHA-256 of the translated request body, so it can only be produced once
//!   the whole body is known — the gateway signs the request after
//!   translating it (see `maxllm_translate::aws_sigv4`).
//! - **Body format**: Identical to Anthropic Messages API (minus the `model` field
//!   in the body, since the model is in the URL).
//!
//! This translator reuses the Anthropic body translation logic internally.

use crate::anthropic_stream::AnthropicToOpenAIStream;
use crate::formats::*;
use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};
use serde_json::Value;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MODEL_ID: &str = "anthropic.claude-sonnet-4-20250514-v1:0";
const DEFAULT_MAX_TOKENS: u64 = 4096;
const ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";

/// Translator for AWS Bedrock (Anthropic Claude models).
///
/// The body translation reuses the same OpenAI-to-Anthropic logic as the
/// `AnthropicTranslator`. Authentication (AWS SigV4) is NOT handled here —
/// the gateway signs the translated request before sending it upstream,
/// because the signature covers the payload hash.
pub struct BedrockTranslator {
    /// Stores the last model ID for path construction.
    last_model_id: Mutex<String>,
    /// Default upstream path.
    default_path: String,
}

impl BedrockTranslator {
    pub fn new() -> Self {
        let default_path = format!("/model/{DEFAULT_MODEL_ID}/invoke");
        Self {
            last_model_id: Mutex::new(DEFAULT_MODEL_ID.to_string()),
            default_path,
        }
    }

    /// Returns the last model ID seen during request translation.
    pub fn last_model_id(&self) -> String {
        self.last_model_id.lock().unwrap().clone()
    }

    /// The Bedrock Runtime API path for a model.
    ///
    /// Non-streaming: `/model/{model_id}/invoke`
    /// Streaming: `/model/{model_id}/invoke-with-response-stream`
    pub fn path_for(model_id: &str, streaming: bool) -> String {
        let suffix = if streaming {
            "invoke-with-response-stream"
        } else {
            "invoke"
        };
        format!("/model/{model_id}/{suffix}")
    }
}

impl Default for BedrockTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderTranslator for BedrockTranslator {
    fn name(&self) -> &str {
        "bedrock"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn translate_request(
        &self,
        body: &[u8],
        model_override: Option<&str>,
    ) -> Result<TranslatedRequest, TranslateError> {
        let req: OpenAIChatRequest = serde_json::from_slice(body)?;
        let model = model_override.unwrap_or(&req.model);

        // Store model ID for the gateway to construct the path.
        if let Ok(mut m) = self.last_model_id.lock() {
            *m = normalize_bedrock_model(model);
        }

        let anthropic_req = openai_to_bedrock_anthropic(&req)?;
        let is_streaming = req.stream.unwrap_or(false);

        Ok(TranslatedRequest {
            body: serde_json::to_vec(&anthropic_req)?,
            is_streaming,
        })
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        let resp: AnthropicResponse = serde_json::from_slice(body)?;
        let openai_resp = bedrock_anthropic_to_openai(&resp);
        Ok(serde_json::to_vec(&openai_resp)?)
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        // Bedrock streaming for Claude uses the same Anthropic SSE format.
        Box::new(AnthropicToOpenAIStream::new())
    }

    /// The default path. The gateway replaces it with the model-specific
    /// path ([`BedrockTranslator::path_for`]) once the request body has been
    /// translated and the model is known.
    fn upstream_path(&self) -> &str {
        &self.default_path
    }

    fn upstream_headers(&self, _api_key: &str) -> Vec<(String, String)> {
        // NOTE: AWS SigV4 signing is NOT handled here — the signature covers
        // the payload hash and can only be produced once the whole body is
        // known. The gateway signs the translated request itself (see
        // `maxllm_translate::aws_sigv4`). The api_key parameter is ignored;
        // AWS credentials come from the environment or an IAM role.
        vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
        ]
    }
}

fn normalize_bedrock_model(model: &str) -> String {
    if let Some(pos) = model.find('/') {
        if model.starts_with('@') {
            return model[pos + 1..].to_string();
        }
    }
    model.to_string()
}

/// Convert OpenAI request to Bedrock Anthropic body format.
///
/// Bedrock Anthropic format is similar to direct Anthropic, but without the
/// `model` field (it is in the URL path).
fn openai_to_bedrock_anthropic(req: &OpenAIChatRequest) -> Result<Value, TranslateError> {
    let mut system: Option<Value> = None;
    let mut messages: Vec<Value> = Vec::new();

    for msg in &req.messages {
        if msg.role == "system" {
            if let Some(content) = &msg.content {
                system = Some(content.clone());
            }
            continue;
        }

        if msg.role == "tool" {
            let tool_call_id = msg.tool_call_id.as_deref().unwrap_or("").to_string();
            let content_text = msg
                .content
                .as_ref()
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            messages.push(serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": content_text,
                }]
            }));
            continue;
        }

        if msg.role == "assistant" {
            if let Some(tool_calls) = &msg.tool_calls {
                let mut blocks: Vec<Value> = Vec::new();
                if let Some(content) = &msg.content {
                    if let Some(text) = content.as_str() {
                        if !text.is_empty() {
                            blocks.push(serde_json::json!({"type": "text", "text": text}));
                        }
                    }
                }
                for tc in tool_calls {
                    let input: Value = serde_json::from_str(&tc.function.arguments)
                        .unwrap_or(Value::Object(serde_json::Map::new()));
                    blocks.push(serde_json::json!({
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.function.name,
                        "input": input,
                    }));
                }
                messages.push(serde_json::json!({
                    "role": "assistant",
                    "content": blocks,
                }));
                continue;
            }
        }

        let content = msg.content.clone().unwrap_or(Value::String(String::new()));
        messages.push(serde_json::json!({
            "role": msg.role,
            "content": content,
        }));
    }

    let max_tokens = req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

    let mut body = serde_json::json!({
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        "messages": messages,
    });

    if let Some(sys) = system {
        body["system"] = sys;
    }
    if let Some(temp) = req.temperature {
        body["temperature"] = serde_json::json!(temp);
    }
    if let Some(tp) = req.top_p {
        body["top_p"] = serde_json::json!(tp);
    }
    if let Some(stop) = &req.stop {
        body["stop_sequences"] = serde_json::json!(stop);
    }
    if req.stream == Some(true) {
        body["stream"] = serde_json::json!(true);
    }

    // Translate tools
    if let Some(tools) = &req.tools {
        let anthropic_tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.function.name,
                    "description": t.function.description,
                    "input_schema": t.function.parameters.clone().unwrap_or(serde_json::json!({"type": "object"})),
                })
            })
            .collect();
        body["tools"] = serde_json::json!(anthropic_tools);
    }

    Ok(body)
}

fn bedrock_anthropic_to_openai(resp: &AnthropicResponse) -> OpenAIChatResponse {
    let mut text_parts: Vec<String> = Vec::new();
    let mut tool_calls: Vec<OpenAIToolCall> = Vec::new();

    for block in &resp.content {
        match block.block_type.as_str() {
            "text" => {
                if let Some(text) = &block.text {
                    text_parts.push(text.clone());
                }
            }
            "tool_use" => {
                let arguments = block
                    .input
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "{}".to_string());
                tool_calls.push(OpenAIToolCall {
                    id: block.id.clone().unwrap_or_default(),
                    call_type: "function".to_string(),
                    function: OpenAIFunctionCall {
                        name: block.name.clone().unwrap_or_default(),
                        arguments,
                    },
                });
            }
            _ => {}
        }
    }

    let content = if text_parts.is_empty() {
        None
    } else {
        Some(Value::String(text_parts.join("")))
    };

    let finish_reason = resp.stop_reason.as_deref().map(|sr| {
        match sr {
            "end_turn" => "stop",
            "max_tokens" => "length",
            "pause_turn" | "tool_use" => "tool_calls",
            "stop_sequence" => "stop",
            other => other,
        }
        .to_string()
    });

    let message = OpenAIMessage {
        role: "assistant".to_string(),
        content,
        tool_calls: if tool_calls.is_empty() {
            None
        } else {
            Some(tool_calls)
        },
        tool_call_id: None,
        extra: serde_json::Map::new(),
    };

    let usage = resp.usage.as_ref().map(|u| OpenAIUsage {
        prompt_tokens: u.input_tokens,
        completion_tokens: u.output_tokens,
        total_tokens: u.input_tokens + u.output_tokens,
    });

    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    OpenAIChatResponse {
        id: resp.id.clone(),
        object: "chat.completion".to_string(),
        created,
        model: resp.model.clone(),
        choices: vec![OpenAIChoice {
            index: 0,
            message,
            finish_reason,
        }],
        usage,
        extra: serde_json::Map::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_request_translation() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "anthropic.claude-sonnet-4-20250514-v1:0",
            "messages": [
                {"role": "system", "content": "Be helpful."},
                {"role": "user", "content": "Hello!"}
            ],
            "max_tokens": 1024
        }))
        .unwrap();

        let t = BedrockTranslator::new();
        let result = t.translate_request(&body, None).unwrap();
        let parsed: Value = serde_json::from_slice(&result.body).unwrap();

        assert_eq!(parsed["max_tokens"], 1024);
        assert_eq!(parsed["system"], "Be helpful.");
        assert_eq!(parsed["messages"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["messages"][0]["role"], "user");
        // model should NOT be in the body
        assert!(parsed.get("model").is_none());
    }

    #[test]
    fn test_response_translation() {
        let resp = serde_json::to_vec(&serde_json::json!({
            "id": "msg_bedrock_01",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello from Bedrock!"}],
            "model": "anthropic.claude-sonnet-4-20250514-v1:0",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 8, "output_tokens": 4}
        }))
        .unwrap();

        let t = BedrockTranslator::new();
        let result = t.translate_response(&resp).unwrap();
        let parsed: OpenAIChatResponse = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed.object, "chat.completion");
        assert_eq!(parsed.choices[0].finish_reason, Some("stop".to_string()));
        assert_eq!(
            parsed.choices[0].message.content,
            Some(Value::String("Hello from Bedrock!".to_string()))
        );
    }

    #[test]
    fn test_model_stored() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "anthropic.claude-haiku-4-5-20251001-v1:0",
            "messages": [{"role": "user", "content": "Hi"}]
        }))
        .unwrap();

        let t = BedrockTranslator::new();
        t.translate_request(&body, None).unwrap();
        assert_eq!(
            t.last_model_id(),
            "anthropic.claude-haiku-4-5-20251001-v1:0"
        );
    }

    #[test]
    fn test_headers_no_auth() {
        let t = BedrockTranslator::new();
        let headers = t.upstream_headers("ignored");
        // Should NOT have Authorization or api-key
        assert!(!headers.iter().any(|(k, _)| k == "Authorization"));
        assert!(headers.iter().any(|(k, _)| k == "anthropic-version"));
    }

    #[test]
    fn test_name() {
        assert_eq!(BedrockTranslator::new().name(), "bedrock");
    }

    #[test]
    fn test_path_for_uses_the_model_id() {
        assert_eq!(
            BedrockTranslator::path_for("anthropic.claude-sonnet-4-20250514-v1:0", false),
            "/model/anthropic.claude-sonnet-4-20250514-v1:0/invoke"
        );
        assert_eq!(
            BedrockTranslator::path_for("anthropic.claude-sonnet-4-20250514-v1:0", true),
            "/model/anthropic.claude-sonnet-4-20250514-v1:0/invoke-with-response-stream"
        );
    }

    #[test]
    fn test_model_is_normalized_for_the_path() {
        // The `@org/model` form is Bedrock's inference-profile syntax; the
        // path carries the bare model id.
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "@anthropic/claude-3-5-sonnet-20241022-v2:0",
            "messages": [{"role": "user", "content": "Hi"}]
        }))
        .unwrap();

        let t = BedrockTranslator::new();
        t.translate_request(&body, None).unwrap();
        assert_eq!(t.last_model_id(), "claude-3-5-sonnet-20241022-v2:0");
    }

    #[test]
    fn test_streaming_flag() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "anthropic.claude-sonnet-4-20250514-v1:0",
            "messages": [{"role": "user", "content": "Hi"}],
            "stream": true
        }))
        .unwrap();

        let t = BedrockTranslator::new();
        let result = t.translate_request(&body, None).unwrap();
        assert!(result.is_streaming);
    }
}
