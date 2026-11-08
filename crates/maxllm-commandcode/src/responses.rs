// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The OpenAI Responses contract, as the bridge speaks it.
//!
//! The bridge answers `POST /v1/responses` with the Responses contract
//! itself — the `response.*` event family when the client streams, and a
//! `response` object when it does not. The event shapes are the ones the
//! OpenAI Responses API emits (and the ones MaxLLM's own Responses layer
//! emits), so a Responses client needs no translation on the way in.
//!
//! The CLI behind the bridge has no function-calling channel, so a request
//! that carries `tools` is answered without them: the client's tools are
//! dropped, the CLI's own agent tools are not exposed.

use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A Responses API request (the subset the bridge serves).
#[derive(Debug, Clone, Deserialize)]
pub struct ResponsesRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub input: Option<Value>,
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub tools: Option<Value>,
    /// The reasoning block. Only `effort` is honoured: it becomes the CLI's
    /// `--effort` flag.
    #[serde(default)]
    pub reasoning: Option<Value>,
}

impl ResponsesRequest {
    /// The reasoning effort the request asks for, if any.
    pub fn effort(&self) -> Option<String> {
        self.reasoning
            .as_ref()
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(Value::as_str)
            .map(str::to_string)
    }
}

/// One turn of the request, as the CLI would see it.
#[derive(Debug, Clone)]
struct Turn {
    role: &'static str,
    text: String,
}

/// Build the prompt the CLI is asked to answer.
///
/// A single user turn is passed through verbatim; anything else (instructions,
/// a multi-turn transcript, tool results) is rendered as a labelled
/// transcript, because the CLI takes exactly one prompt string.
pub fn build_prompt(request: &ResponsesRequest) -> Result<String, String> {
    let mut turns: Vec<Turn> = Vec::new();

    match &request.input {
        Some(Value::String(text)) => {
            if !text.trim().is_empty() {
                turns.push(Turn {
                    role: "user",
                    text: text.clone(),
                });
            }
        }
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(turn) = item_turn(item) {
                    turns.push(turn);
                }
            }
        }
        Some(Value::Object(item)) => {
            if let Some(turn) = item_turn(&Value::Object(item.clone())) {
                turns.push(turn);
            }
        }
        Some(Value::Null) | None => {}
        Some(other) => {
            return Err(format!(
                "`input` must be a string or an array of items, got {}",
                kind_of(other)
            ));
        }
    }

    if turns.is_empty() {
        return Err("`input` must not be empty".to_string());
    }

    let instructions = request
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty());

    if instructions.is_none() && turns.len() == 1 && turns[0].role == "user" {
        return Ok(turns[0].text.clone());
    }

    let mut prompt = String::new();
    if let Some(instructions) = instructions {
        prompt.push_str("System instructions:\n");
        prompt.push_str(instructions);
        prompt.push_str("\n\n");
    }
    for turn in &turns {
        let label = match turn.role {
            "assistant" => "Assistant",
            "tool" => "Tool result",
            "system" | "developer" => "System",
            _ => "User",
        };
        prompt.push_str(label);
        prompt.push_str(": ");
        prompt.push_str(&turn.text);
        prompt.push('\n');
    }
    Ok(prompt.trim_end().to_string())
}

/// Turn one input item into a transcript turn.
fn item_turn(item: &Value) -> Option<Turn> {
    let item_type = item.get("type").and_then(Value::as_str);

    // A tool call / tool result the client sent earlier: keep it in the
    // transcript so the answer has the context.
    if item_type == Some("function_call") {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("function");
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return Some(Turn {
            role: "assistant",
            text: format!("Tool call {name}({arguments})"),
        });
    }
    if item_type == Some("function_call_output") {
        let output = item
            .get("output")
            .map(|value| match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        return Some(Turn {
            role: "tool",
            text: output,
        });
    }
    // A reasoning item carries no new instructions for the CLI.
    if item_type == Some("reasoning") {
        return None;
    }

    let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
    let role = match role {
        "assistant" => "assistant",
        "system" | "developer" => "system",
        "tool" => "tool",
        _ => "user",
    };
    let text = item.get("content").map(content_text).unwrap_or_default();
    if text.trim().is_empty() {
        return None;
    }
    Some(Turn { role, text })
}

/// Flatten a content value (string or content-part array) into text.
fn content_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                let kind = part.get("type").and_then(Value::as_str).unwrap_or("text");
                match kind {
                    "input_text" | "output_text" | "text" | "summary_text" | "refusal" => {
                        part.get("text").and_then(Value::as_str).map(str::to_string)
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// The JSON type name of a value, for error messages.
fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Identifiers and the response object
// ---------------------------------------------------------------------------

/// Generate a sortable, unique identifier with the given prefix.
pub fn generate_id(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{nanos:016x}{seq:08x}")
}

/// The current unix timestamp in seconds.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The `usage` object of a Responses answer.
pub fn usage(input_tokens: u64, cached_tokens: u64, output_tokens: u64) -> Value {
    json!({
        "input_tokens": input_tokens,
        "input_tokens_details": { "cached_tokens": cached_tokens },
        "output_tokens": output_tokens,
        "output_tokens_details": { "reasoning_tokens": 0 },
        "total_tokens": input_tokens + output_tokens,
    })
}

/// Build the `response` object.
pub fn response_object(
    id: &str,
    model: &str,
    created_at: u64,
    status: &str,
    output: Vec<Value>,
    usage: Value,
    error: Value,
) -> Value {
    json!({
        "id": id,
        "object": "response",
        "created_at": created_at,
        "status": status,
        "error": error,
        "incomplete_details": Value::Null,
        "model": model,
        "output": output,
        "parallel_tool_calls": true,
        "previous_response_id": Value::Null,
        "tool_choice": "auto",
        "tools": [],
        "store": false,
        "metadata": {},
        "usage": usage,
    })
}

/// The message item of a completed answer.
pub fn message_item(id: &str, text: &str, status: &str) -> Value {
    json!({
        "type": "message",
        "id": id,
        "status": status,
        "role": "assistant",
        "content": [{
            "type": "output_text",
            "text": text,
            "annotations": [],
        }],
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

// ---------------------------------------------------------------------------
// The event stream
// ---------------------------------------------------------------------------

/// Serialize one Responses SSE event.
///
/// Every event carries the monotonic `sequence_number` the OpenAI Responses
/// API stamps on its frames — the AI SDK's schema requires it on
/// `response.failed`.
pub fn event(name: &str, payload: Value, sequence: u64) -> String {
    let mut payload = payload;
    if let Some(object) = payload.as_object_mut() {
        object.insert("sequence_number".into(), json!(sequence));
    }
    format!("event: {name}\ndata: {payload}\n\n")
}

/// The state of one streamed answer.
pub struct Stream {
    pub response_id: String,
    pub message_id: String,
    pub model: String,
    pub created_at: u64,
    sequence: u64,
    text: String,
    open: bool,
}

impl Stream {
    /// Start a stream for one answer.
    pub fn new(model: &str) -> Self {
        Self {
            response_id: generate_id("resp_"),
            message_id: generate_id("msg_"),
            model: model.to_string(),
            created_at: now_secs(),
            sequence: 0,
            text: String::new(),
            open: false,
        }
    }

    /// The next sequence number.
    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// The response object as it stands right now.
    pub fn response(&self, status: &str, usage: Value, error: Value) -> Value {
        let output = if self.open || !self.text.is_empty() {
            vec![message_item(
                &self.message_id,
                &self.text,
                if status == "in_progress" {
                    "in_progress"
                } else {
                    "completed"
                },
            )]
        } else {
            Vec::new()
        };
        response_object(
            &self.response_id,
            &self.model,
            self.created_at,
            status,
            output,
            usage,
            error,
        )
    }

    /// `response.created` + `response.in_progress`.
    pub fn start(&mut self) -> String {
        let mut out = String::new();
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.created",
            json!({
                "type": "response.created",
                "response": self.response("in_progress", usage(0, 0, 0), Value::Null),
            }),
            sequence,
        ));
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.in_progress",
            json!({
                "type": "response.in_progress",
                "response": self.response("in_progress", usage(0, 0, 0), Value::Null),
            }),
            sequence,
        ));
        out
    }

    /// Open the assistant message item (once).
    fn open_message(&mut self) -> String {
        if self.open {
            return String::new();
        }
        self.open = true;
        let mut out = String::new();
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.output_item.added",
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "message",
                    "id": self.message_id,
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [],
                },
            }),
            sequence,
        ));
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.content_part.added",
            json!({
                "type": "response.content_part.added",
                "item_id": self.message_id,
                "output_index": 0,
                "content_index": 0,
                "part": { "type": "output_text", "text": "", "annotations": [] },
            }),
            sequence,
        ));
        out
    }

    /// One chunk of assistant text.
    pub fn text_delta(&mut self, delta: &str) -> String {
        let mut out = self.open_message();
        self.text.push_str(delta);
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta",
                "item_id": self.message_id,
                "output_index": 0,
                "content_index": 0,
                "delta": delta,
            }),
            sequence,
        ));
        out
    }

    /// Close the message item and finish the stream.
    pub fn finish(&mut self, usage: Value) -> String {
        let mut out = String::new();
        if self.open {
            self.open = false;
            let sequence = self.next_sequence();
            out.push_str(&event(
                "response.output_text.done",
                json!({
                    "type": "response.output_text.done",
                    "item_id": self.message_id,
                    "output_index": 0,
                    "content_index": 0,
                    "text": self.text,
                }),
                sequence,
            ));
            let sequence = self.next_sequence();
            out.push_str(&event(
                "response.content_part.done",
                json!({
                    "type": "response.content_part.done",
                    "item_id": self.message_id,
                    "output_index": 0,
                    "content_index": 0,
                    "part": { "type": "output_text", "text": self.text, "annotations": [] },
                }),
                sequence,
            ));
            let sequence = self.next_sequence();
            out.push_str(&event(
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": message_item(&self.message_id, &self.text, "completed"),
                }),
                sequence,
            ));
        }
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.completed",
            json!({
                "type": "response.completed",
                "response": self.response("completed", usage, Value::Null),
            }),
            sequence,
        ));
        out.push_str("data: [DONE]\n\n");
        out
    }

    /// Fail the stream.
    pub fn fail(&mut self, message: &str, code: &str) -> String {
        let mut out = String::new();
        let sequence = self.next_sequence();
        out.push_str(&event(
            "response.failed",
            json!({
                "type": "response.failed",
                "response": self.response(
                    "failed",
                    usage(0, 0, 0),
                    json!({ "code": code, "message": message }),
                ),
            }),
            sequence,
        ));
        out.push_str("data: [DONE]\n\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(input: Value) -> ResponsesRequest {
        ResponsesRequest {
            model: Some("deepseek/deepseek-v4.1-flash".to_string()),
            input: Some(input),
            instructions: None,
            stream: false,
            tools: None,
            reasoning: None,
        }
    }

    #[test]
    fn a_single_string_input_is_passed_through_verbatim() {
        let prompt = build_prompt(&request(json!("hello"))).unwrap();
        assert_eq!(prompt, "hello");
    }

    #[test]
    fn instructions_are_prefixed() {
        let mut req = request(json!("hello"));
        req.instructions = Some("be terse".to_string());
        let prompt = build_prompt(&req).unwrap();
        assert!(prompt.starts_with("System instructions:\nbe terse"));
        assert!(prompt.ends_with("User: hello"));
    }

    #[test]
    fn a_conversation_becomes_a_transcript() {
        let prompt = build_prompt(&request(json!([
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "two"},
            {"role": "user", "content": "three"},
        ])))
        .unwrap();
        assert_eq!(prompt, "User: one\nAssistant: two\nUser: three");
    }

    #[test]
    fn content_parts_are_flattened_to_text() {
        let prompt = build_prompt(&request(json!([
            {"role": "user", "content": [
                {"type": "input_text", "text": "hello"},
                {"type": "input_image", "image_url": "data:image/png;base64,AA"},
            ]},
        ])))
        .unwrap();
        assert_eq!(prompt, "hello");
    }

    #[test]
    fn tool_results_are_kept_in_the_transcript() {
        let prompt = build_prompt(&request(json!([
            {"type": "function_call", "name": "shell", "arguments": "{\"cmd\":\"ls\"}"},
            {"type": "function_call_output", "output": "a.txt"},
        ])))
        .unwrap();
        assert!(prompt.contains("Tool call shell"));
        assert!(prompt.contains("Tool result: a.txt"));
    }

    #[test]
    fn the_reasoning_effort_is_read_from_the_request() {
        let mut req = request(json!("hi"));
        assert_eq!(req.effort(), None);
        req.reasoning = Some(json!({"effort": "high"}));
        assert_eq!(req.effort().as_deref(), Some("high"));
    }

    #[test]
    fn an_empty_input_is_rejected() {
        let err = build_prompt(&request(json!([]))).unwrap_err();
        assert!(err.contains("input"));
    }

    #[test]
    fn the_stream_emits_the_responses_event_family() {
        let mut stream = Stream::new("deepseek/deepseek-v4.1-flash");
        let mut out = stream.start();
        out.push_str(&stream.text_delta("Hi"));
        out.push_str(&stream.finish(usage(10, 2, 3)));

        assert!(out.contains("event: response.created"));
        assert!(out.contains("event: response.in_progress"));
        assert!(out.contains("event: response.output_item.added"));
        assert!(out.contains("event: response.content_part.added"));
        assert!(out.contains("event: response.output_text.delta"));
        assert!(out.contains("event: response.output_text.done"));
        assert!(out.contains("event: response.content_part.done"));
        assert!(out.contains("event: response.output_item.done"));
        assert!(out.contains("event: response.completed"));
        assert!(out.ends_with("data: [DONE]\n\n"));
        // Every event carries a sequence number.
        assert_eq!(out.matches("sequence_number").count(), 9);
    }

    #[test]
    fn a_failed_stream_ends_with_response_failed() {
        let mut stream = Stream::new("m");
        let out = stream.fail("boom", "upstream_error");
        assert!(out.contains("event: response.failed"));
        assert!(out.contains("boom"));
        assert!(out.ends_with("data: [DONE]\n\n"));
    }
}
