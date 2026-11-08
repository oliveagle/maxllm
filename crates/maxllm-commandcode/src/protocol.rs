// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The OpenAI Chat Completions contract, as the bridge speaks it.
//!
//! MaxLLM's own Responses layer translates the Responses contract into this
//! one, so the bridge only ever has to speak chat completions.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// A chat completions request.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
}

/// One message of a chat completions request.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<Value>,
}

impl ChatMessage {
    /// The message's text, flattened out of the string / content-part forms.
    pub fn text(&self) -> String {
        match &self.content {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Array(parts)) => parts
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .filter(|_| part.get("type").and_then(Value::as_str) != Some("image_url"))
                })
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        }
    }
}

/// Build the prompt the CLI is asked to answer.
///
/// A single user turn is passed through verbatim — that is what the CLI
/// expects and it keeps the model's answer clean. A longer conversation is
/// rendered as a labelled transcript, and the system prompt is prefixed to
/// it, because the CLI takes exactly one prompt string.
pub fn build_prompt(messages: &[ChatMessage]) -> String {
    let system: Vec<String> = messages
        .iter()
        .filter(|m| matches!(m.role.as_str(), "system" | "developer"))
        .map(|m| m.text())
        .filter(|t| !t.trim().is_empty())
        .collect();

    let turns: Vec<&ChatMessage> = messages
        .iter()
        .filter(|m| !matches!(m.role.as_str(), "system" | "developer"))
        .collect();

    if system.is_empty() && turns.len() == 1 && turns[0].role == "user" {
        return turns[0].text();
    }

    let mut prompt = String::new();
    for instruction in &system {
        prompt.push_str("System instructions:\n");
        prompt.push_str(instruction);
        prompt.push_str("\n\n");
    }
    for message in turns {
        let label = match message.role.as_str() {
            "assistant" => "Assistant",
            "tool" => "Tool result",
            _ => "User",
        };
        let text = message.text();
        prompt.push_str(label);
        prompt.push_str(": ");
        prompt.push_str(&text);
        prompt.push('\n');
    }
    prompt.trim_end().to_string()
}

/// Token usage of one run, in the OpenAI chat shape.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

/// The `usage` object of a chat completion.
#[derive(Debug, Clone, Serialize)]
pub struct UsageDetails {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(rename = "prompt_tokens_details")]
    pub prompt_tokens_details: PromptTokensDetails,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromptTokensDetails {
    pub cached_tokens: u64,
}

impl Usage {
    /// Build the chat `usage` object, including the cached-input breakdown.
    pub fn details(self, cached_tokens: u64) -> UsageDetails {
        UsageDetails {
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            total_tokens: self.total_tokens,
            prompt_tokens_details: PromptTokensDetails { cached_tokens },
        }
    }
}

/// A completion id in the OpenAI shape.
pub fn completion_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("chatcmpl-cmd{nanos:x}")
}

/// The wall-clock seconds used as a completion's `created` stamp.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One streamed chat completion chunk.
pub fn chunk(
    id: &str,
    model: &str,
    delta: Value,
    finish_reason: Option<&str>,
    usage: Option<Value>,
) -> String {
    let mut chunk = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": now_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
    });
    if let Some(usage) = usage {
        chunk["usage"] = usage;
    }
    format!("data: {chunk}\n\n")
}

/// The non-streamed chat completion object.
pub fn completion(
    id: &str,
    model: &str,
    content: &str,
    finish_reason: &str,
    usage: Value,
) -> Value {
    json!({
        "id": id,
        "object": "chat.completion",
        "created": now_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": finish_reason,
        }],
        "usage": usage,
    })
}

/// An OpenAI-shaped error object.
pub fn error(message: &str, code: &str, kind: &str) -> Value {
    json!({
        "error": {
            "message": message,
            "type": kind,
            "code": code,
            "param": Value::Null,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(role: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: role.to_string(),
            content: Some(Value::String(content.to_string())),
        }
    }

    #[test]
    fn a_single_user_turn_is_passed_through_verbatim() {
        let prompt = build_prompt(&[message("user", "hello")]);
        assert_eq!(prompt, "hello");
    }

    #[test]
    fn a_system_prompt_is_prefixed() {
        let prompt = build_prompt(&[message("system", "be terse"), message("user", "hello")]);
        assert!(prompt.starts_with("System instructions:\nbe terse"));
        assert!(prompt.ends_with("User: hello"));
    }

    #[test]
    fn a_conversation_becomes_a_transcript() {
        let prompt = build_prompt(&[
            message("user", "one"),
            message("assistant", "two"),
            message("user", "three"),
        ]);
        assert_eq!(prompt, "User: one\nAssistant: two\nUser: three");
    }

    #[test]
    fn content_parts_are_flattened_to_text() {
        let mut msg = message("user", "");
        msg.content = Some(json!([
            {"type": "text", "text": "hello"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA"}},
        ]));
        assert_eq!(msg.text(), "hello");
    }
}
