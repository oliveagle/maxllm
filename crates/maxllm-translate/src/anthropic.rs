// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::anthropic_stream::AnthropicToOpenAIStream;
use crate::formats::*;
use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_TOKENS: u64 = 4096;
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Translator between OpenAI canonical format and Anthropic Messages API.
pub struct AnthropicTranslator;

impl ProviderTranslator for AnthropicTranslator {
    fn name(&self) -> &str {
        "anthropic"
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
        let mut anthropic_req = openai_to_anthropic_request(&req)?;
        if let Some(model) = model_override {
            anthropic_req.model = model.to_string();
        }
        let is_streaming = anthropic_req.stream.unwrap_or(false);
        Ok(TranslatedRequest {
            body: serde_json::to_vec(&anthropic_req)?,
            is_streaming,
        })
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        let resp: AnthropicResponse = serde_json::from_slice(body)?;
        let openai_resp = anthropic_to_openai_response(&resp);
        Ok(serde_json::to_vec(&openai_resp)?)
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        Box::new(AnthropicToOpenAIStream::new())
    }

    fn upstream_path(&self) -> &str {
        "/v1/messages"
    }

    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)> {
        vec![
            ("x-api-key".to_string(), api_key.to_string()),
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
            ("Content-Type".to_string(), "application/json".to_string()),
        ]
    }
}

/// Strip `@provider/` prefix from model string if present.
fn normalize_model(model: &str) -> String {
    if let Some(pos) = model.find('/') {
        if model.starts_with('@') {
            return model[pos + 1..].to_string();
        }
    }
    model.to_string()
}

pub(crate) fn openai_to_anthropic_request(
    req: &OpenAIChatRequest,
) -> Result<AnthropicRequest, TranslateError> {
    let mut system: Option<Value> = None;
    let mut messages = Vec::with_capacity(req.messages.len());

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
            let block = serde_json::json!([{
                "type": "tool_result",
                "tool_use_id": tool_call_id,
                "content": content_text,
            }]);
            messages.push(AnthropicMessage {
                role: "user".to_string(),
                content: block,
            });
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
                messages.push(AnthropicMessage {
                    role: "assistant".to_string(),
                    content: Value::Array(blocks),
                });
                continue;
            }
        }

        let content = msg.content.clone().unwrap_or(Value::String(String::new()));
        messages.push(AnthropicMessage {
            role: msg.role.clone(),
            content,
        });
    }

    let tools = req.tools.as_ref().map(|openai_tools| {
        openai_tools
            .iter()
            .map(|t| AnthropicTool {
                name: t.function.name.clone(),
                description: t.function.description.clone(),
                input_schema: t
                    .function
                    .parameters
                    .clone()
                    .unwrap_or(serde_json::json!({"type": "object"})),
            })
            .collect()
    });

    let tool_choice = req.tool_choice.as_ref().map(|tc| match tc.as_str() {
        Some("auto") => serde_json::json!({"type": "auto"}),
        Some("none") => serde_json::json!({"type": "none"}),
        Some("required") => serde_json::json!({"type": "any"}),
        _ => {
            if let Some(func) = tc.get("function") {
                if let Some(name) = func.get("name") {
                    return serde_json::json!({"type": "tool", "name": name});
                }
            }
            tc.clone()
        }
    });

    let model = normalize_model(&req.model);
    let max_tokens = req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

    Ok(AnthropicRequest {
        model,
        max_tokens,
        messages,
        system,
        temperature: req.temperature,
        top_p: req.top_p,
        stream: req.stream,
        stop_sequences: req.stop.clone(),
        tools,
        tool_choice,
        metadata: None,
        extra: serde_json::Map::new(),
    })
}

pub(crate) fn anthropic_to_openai_response(resp: &AnthropicResponse) -> OpenAIChatResponse {
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
    fn test_translate_basic_request() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "system", "content": "Be helpful."},
                {"role": "user", "content": "Hello!"}
            ],
            "max_tokens": 1024
        }))
        .unwrap();

        let t = AnthropicTranslator;
        let result = t.translate_request(&body, None).unwrap();
        let parsed: AnthropicRequest = serde_json::from_slice(&result.body).unwrap();

        assert_eq!(parsed.model, "claude-sonnet-4-20250514");
        assert_eq!(parsed.max_tokens, 1024);
        assert_eq!(
            parsed.system,
            Some(Value::String("Be helpful.".to_string()))
        );
        assert_eq!(parsed.messages.len(), 1);
        assert_eq!(parsed.messages[0].role, "user");
    }

    #[test]
    fn test_translate_response() {
        let resp = serde_json::to_vec(&serde_json::json!({
            "id": "msg_01",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello!"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }))
        .unwrap();

        let t = AnthropicTranslator;
        let result = t.translate_response(&resp).unwrap();
        let parsed: OpenAIChatResponse = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed.id, "msg_01");
        assert_eq!(parsed.object, "chat.completion");
        assert_eq!(parsed.choices[0].finish_reason, Some("stop".to_string()));
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 5);
    }

    #[test]
    fn test_model_override() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hi"}]
        }))
        .unwrap();

        let t = AnthropicTranslator;
        let result = t
            .translate_request(&body, Some("claude-haiku-4-5-20251001"))
            .unwrap();
        let parsed: AnthropicRequest = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(parsed.model, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn test_tool_translation() {
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Weather?"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {"type": "object", "properties": {"loc": {"type": "string"}}}
                }
            }],
            "tool_choice": "auto"
        }))
        .unwrap();

        let t = AnthropicTranslator;
        let result = t.translate_request(&body, None).unwrap();
        let parsed: AnthropicRequest = serde_json::from_slice(&result.body).unwrap();

        let tools = parsed.tools.unwrap();
        assert_eq!(tools[0].name, "get_weather");
        assert_eq!(
            parsed.tool_choice,
            Some(serde_json::json!({"type": "auto"}))
        );
    }

    #[test]
    fn test_headers() {
        let t = AnthropicTranslator;
        let headers = t.upstream_headers("sk-ant-test");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "x-api-key" && v == "sk-ant-test"));
        assert!(headers.iter().any(|(k, _)| k == "anthropic-version"));
    }
}
