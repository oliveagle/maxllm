// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! OpenAI Responses API contract translation.
//!
//! The Responses API (`POST /v1/responses`) is a different contract from chat
//! completions: the request carries `input`/`instructions` instead of
//! `messages`, and the response carries a list of `output` items instead of
//! `choices`.
//!
//! This module converts between the two so that *any* provider reachable
//! through the gateway can serve Responses API clients:
//!
//! * request: Responses → chat completions (canonical form the provider
//!   translators already understand)
//! * response: chat completions → Responses
//! * stream: chat completion SSE → Responses SSE events
//!
//! The conversion is stateless: `previous_response_id` chaining is not
//! resolved, because the gateway keeps no response store.

use crate::{StreamTranslator, TranslateError, TranslatedRequest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Request: Responses → chat completions
// ---------------------------------------------------------------------------

/// A Responses API request as accepted by `POST /v1/responses`.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponsesRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub input: Option<Value>,
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub tools: Option<Value>,
    #[serde(default)]
    pub tool_choice: Option<Value>,
    /// Fields the chat completions contract also understands. Anything else
    /// (`store`, `include`, `metadata`, `previous_response_id`, …) is
    /// Responses-specific and dropped.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Chat completions request fields that are valid to forward verbatim.
const PASSTHROUGH_CHAT_FIELDS: &[&str] = &[
    "stop",
    "seed",
    "frequency_penalty",
    "presence_penalty",
    "logit_bias",
    "logprobs",
    "top_logprobs",
    "n",
];

/// Translate a Responses API request body into a chat completions body.
pub fn responses_request_to_chat(body: &[u8]) -> Result<TranslatedRequest, TranslateError> {
    let req: ResponsesRequest = serde_json::from_slice(body)?;

    let model = req
        .model
        .as_deref()
        .filter(|m| !m.is_empty())
        .ok_or_else(|| {
            TranslateError::Translation("responses request is missing 'model'".to_string())
        })?;

    let mut messages: Vec<Value> = Vec::new();

    // `instructions` is the Responses spelling of a system prompt.
    if let Some(instructions) = req.instructions.as_deref().filter(|s| !s.is_empty()) {
        messages.push(json!({ "role": "system", "content": instructions }));
    }

    match req.input.as_ref() {
        Some(input) => messages.extend(input_to_messages(input)?),
        None => {
            return Err(TranslateError::Translation(
                "responses request is missing 'input'".to_string(),
            ))
        }
    }

    if messages.is_empty() {
        return Err(TranslateError::Translation(
            "responses request has no usable input items".to_string(),
        ));
    }

    let mut chat = Map::new();
    chat.insert("model".into(), Value::String(model.to_string()));
    chat.insert("messages".into(), Value::Array(messages));

    if let Some(v) = req.max_output_tokens {
        chat.insert("max_tokens".into(), json!(v));
    }
    if let Some(v) = req.temperature {
        chat.insert("temperature".into(), json!(v));
    }
    if let Some(v) = req.top_p {
        chat.insert("top_p".into(), json!(v));
    }
    if req.stream == Some(true) {
        chat.insert("stream".into(), Value::Bool(true));
        // Responses clients expect usage on `response.completed`; many
        // OpenAI-compatible providers only include usage in a stream when
        // asked. Spec-compliant (OpenAI, ollama, vLLM, Groq, DeepSeek, …).
        chat.insert("stream_options".into(), json!({ "include_usage": true }));
    }
    if let Some(tools) = req.tools.as_ref() {
        chat.insert("tools".into(), responses_tools_to_chat(tools));
    }
    if let Some(tool_choice) = req.tool_choice.as_ref() {
        chat.insert("tool_choice".into(), tool_choice.clone());
    }
    for field in PASSTHROUGH_CHAT_FIELDS {
        if let Some(v) = req.extra.get(*field) {
            chat.insert((*field).to_string(), v.clone());
        }
    }

    Ok(TranslatedRequest {
        body: serde_json::to_vec(&Value::Object(chat))?,
        is_streaming: req.stream == Some(true),
    })
}

/// Expand the `input` field into chat completion messages.
fn input_to_messages(input: &Value) -> Result<Vec<Value>, TranslateError> {
    match input {
        // Plain text input is a single user message.
        Value::String(text) => Ok(vec![json!({ "role": "user", "content": text })]),
        Value::Array(items) => {
            let mut messages = Vec::new();
            for item in items {
                if let Some(msg) = parse_input_item(item) {
                    messages.push(msg);
                }
            }
            Ok(messages)
        }
        _ => Err(TranslateError::Translation(
            "responses 'input' must be a string or an array".to_string(),
        )),
    }
}

/// Convert a single Responses input item into a chat completion message.
///
/// Returns `None` for items that carry no chat equivalent (reasoning items,
/// unknown types) — they are dropped rather than failing the request.
fn parse_input_item(item: &Value) -> Option<Value> {
    let item_type = item.get("type").and_then(|t| t.as_str());

    match item_type {
        // Explicit message item.
        Some("message") => {
            let role = normalize_role(item.get("role").and_then(|r| r.as_str()));
            let content = item.get("content")?;
            Some(json!({ "role": role, "content": content_to_text(content) }))
        }
        // Model-issued function call.
        Some("function_call") => {
            let call_id = item
                .get("call_id")
                .and_then(|v| v.as_str())
                .or_else(|| item.get("id").and_then(|v| v.as_str()))
                .unwrap_or("");
            let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let arguments = item
                .get("arguments")
                .and_then(|v| v.as_str())
                .unwrap_or("{}");
            Some(json!({
                "role": "assistant",
                "content": Value::Null,
                "tool_calls": [{
                    "id": call_id,
                    "type": "function",
                    "function": { "name": name, "arguments": arguments }
                }]
            }))
        }
        // Tool result for a previous function call.
        Some("function_call_output") => {
            let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
            let output = match item.get("output") {
                Some(Value::String(s)) => s.clone(),
                Some(other) => serde_json::to_string(other).unwrap_or_default(),
                None => String::new(),
            };
            Some(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": output
            }))
        }
        // Reasoning items are model-internal; the chat contract has no equivalent.
        Some("reasoning") => None,
        Some(_) => None,
        // No explicit type: a chat-style `{role, content}` message.
        None => {
            let role = normalize_role(item.get("role").and_then(|r| r.as_str()));
            let content = item.get("content")?;
            Some(json!({ "role": role, "content": content_to_text(content) }))
        }
    }
}

/// Map a Responses role onto a chat completion role.
///
/// The Responses contract added `developer`, which is the system prompt of
/// the new API generation; chat completions only know `system`.
fn normalize_role(role: Option<&str>) -> &'static str {
    match role {
        Some("assistant") => "assistant",
        Some("system") => "system",
        Some("developer") => "system",
        Some("tool") => "tool",
        _ => "user",
    }
}

/// Flatten message content into plain text.
fn content_to_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| match part.get("type").and_then(|t| t.as_str()) {
                Some("text") | Some("input_text") | Some("output_text") | None => {
                    part.get("text").and_then(|t| t.as_str()).map(String::from)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Rewrite Responses tool definitions into chat completions tool definitions.
///
/// Responses uses a flat shape (`{"type":"function","name":…}`) while chat
/// completions nests the function object (`{"type":"function","function":{…}}`).
fn responses_tools_to_chat(tools: &Value) -> Value {
    let Value::Array(items) = tools else {
        return tools.clone();
    };

    Value::Array(
        items
            .iter()
            .map(|tool| {
                let is_flat_function = tool.get("type").and_then(|t| t.as_str())
                    == Some("function")
                    && tool.get("function").is_none();
                if !is_flat_function {
                    return tool.clone();
                }

                let mut func = Map::new();
                for field in ["name", "description", "parameters"] {
                    if let Some(v) = tool.get(field) {
                        func.insert(field.to_string(), v.clone());
                    }
                }
                let mut out = Map::new();
                out.insert("type".into(), Value::String("function".into()));
                out.insert("function".into(), Value::Object(func));
                Value::Object(out)
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Request normalisation for native Responses upstreams
// ---------------------------------------------------------------------------

/// Normalise a Responses request body for strict upstreams that serve the
/// Responses contract natively (the opencode.ai "zen" `/zen/go/v1/responses`
/// endpoint).
///
/// The endpoint accepts the official Responses tool shape
/// (`{"type":"function","name":…,"parameters":…}`) but rejects the
/// chat-completions nesting (`{"type":"function","function":{…}}`) with a 503
/// — verified live. Third-party clients that convert a chat request into a
/// Responses one (or echo Responses items built from a chat session) can send
/// the nested shape, so it is flattened back to the official form before the
/// byte-for-byte native passthrough forwards it.
///
/// Only `tools[]` entries and `tool_choice` carrying a nested `function`
/// object are rewritten. Everything else — including the standard flat shape —
/// is left untouched, and a body with nothing to rewrite is returned
/// unchanged so the native passthrough stays byte-for-byte.
pub fn normalize_responses_request(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    let Some(obj) = root.as_object_mut() else {
        return body.to_vec();
    };
    let mut changed = false;

    if let Some(tools) = obj.get_mut("tools").and_then(|t| t.as_array_mut()) {
        for tool in tools.iter_mut() {
            if let Some(flattened) = flatten_nested_function(tool) {
                *tool = flattened;
                changed = true;
            }
        }
    }
    if let Some(choice) = obj.get_mut("tool_choice") {
        if let Some(flattened) = flatten_nested_function(choice) {
            *choice = flattened;
            changed = true;
        }
    }

    if changed {
        serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
    } else {
        body.to_vec()
    }
}

/// Flatten a chat-style nested-function tool (`{"type":"function","function":
/// {"name":…,…}}`) into the official Responses shape (`{"type":"function",
/// "name":…,…}`). Returns `None` when the value is not a nested-function tool.
fn flatten_nested_function(value: &Value) -> Option<Value> {
    let tool = value.as_object()?;
    if tool.get("type").and_then(|t| t.as_str()) != Some("function") {
        return None;
    }
    let Some(inner) = tool.get("function").and_then(|f| f.as_object()) else {
        return None;
    };
    // The inner function object already carries the official flat fields
    // (name / description / parameters / strict); re-add the type marker.
    let mut flat = inner.clone();
    flat.insert("type".into(), Value::String("function".into()));
    // Preserve any extra top-level fields (e.g. a top-level "strict").
    for (k, v) in tool {
        if k != "function" {
            flat.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    Some(Value::Object(flat))
}

// ---------------------------------------------------------------------------
// Response: chat completions → Responses
// ---------------------------------------------------------------------------

/// Translate a chat completions response body into a Responses API body.
pub fn chat_response_to_responses(body: &[u8]) -> Result<Vec<u8>, TranslateError> {
    let chat: Value = serde_json::from_slice(body)?;
    let finish_reason = chat
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|c| c.first())
        .and_then(|c| c.get("finish_reason"))
        .and_then(|v| v.as_str());
    let (status, incomplete_details) = status_from_finish_reason(finish_reason);

    let mut response = build_responses_object(&chat, status);
    if let Some(obj) = response.as_object_mut() {
        obj.insert(
            "incomplete_details".to_string(),
            if status == ResponseStatus::Incomplete {
                incomplete_details
            } else {
                Value::Null
            },
        );
    }
    Ok(serde_json::to_vec(&response)?)
}

/// The terminal state of a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseStatus {
    InProgress,
    Completed,
    Incomplete,
}

impl ResponseStatus {
    fn as_str(self) -> &'static str {
        match self {
            ResponseStatus::InProgress => "in_progress",
            ResponseStatus::Completed => "completed",
            ResponseStatus::Incomplete => "incomplete",
        }
    }
}

/// Map a chat completion `finish_reason` onto a response status.
fn status_from_finish_reason(finish_reason: Option<&str>) -> (ResponseStatus, Value) {
    match finish_reason {
        Some("length") | Some("max_tokens") => (
            ResponseStatus::Incomplete,
            json!({ "reason": "max_output_tokens" }),
        ),
        Some("content_filter") => (
            ResponseStatus::Incomplete,
            json!({ "reason": "content_filter" }),
        ),
        _ => (ResponseStatus::Completed, Value::Null),
    }
}

/// Build a Responses API object from a chat completion object.
fn build_responses_object(chat: &Value, status: ResponseStatus) -> Value {
    let model = chat
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let created_at = chat
        .get("created")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_secs);

    let choice = chat
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|c| c.first());
    let message = choice.and_then(|c| c.get("message"));

    let mut output: Vec<Value> = Vec::new();

    // Reasoning content (DeepSeek-style `reasoning_content`, or an explicit
    // `reasoning` object) becomes a reasoning output item.
    if let Some(reasoning) = extract_reasoning(message) {
        output.push(json!({
            "type": "reasoning",
            "id": generate_id("rs_"),
            "summary": [{ "type": "summary_text", "text": reasoning }],
            "status": "completed"
        }));
    }

    let text = message
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    let tool_calls = message
        .and_then(|m| m.get("tool_calls"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    // A message item is emitted unless the model only produced tool calls.
    if !text.is_empty() || tool_calls.is_empty() {
        output.push(json!({
            "type": "message",
            "id": generate_id("msg_"),
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": []
            }]
        }));
    }

    for call in &tool_calls {
        let call_id = call.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let function = call.get("function");
        let name = function
            .and_then(|f| f.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let arguments = function
            .and_then(|f| f.get("arguments"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        output.push(json!({
            "type": "function_call",
            "id": generate_id("fc_"),
            "status": "completed",
            "call_id": call_id,
            "name": name,
            "arguments": arguments
        }));
    }

    json!({
        "id": generate_id("resp_"),
        "object": "response",
        "created_at": created_at,
        "status": status.as_str(),
        "error": Value::Null,
        "incomplete_details": Value::Null,
        "model": model,
        "output": output,
        "parallel_tool_calls": true,
        "previous_response_id": Value::Null,
        "usage": convert_usage(chat.get("usage")),
    })
}

/// Extract reasoning text from a chat completion message, if any.
fn extract_reasoning(message: Option<&Value>) -> Option<String> {
    let message = message?;
    if let Some(text) = message.get("reasoning_content").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    match message.get("reasoning") {
        Some(Value::String(text)) if !text.is_empty() => Some(text.clone()),
        Some(obj) => {
            let summary = obj.get("summary").and_then(|s| s.as_array())?;
            let parts: Vec<String> = summary
                .iter()
                .filter_map(|s| s.get("text").and_then(|t| t.as_str()).map(String::from))
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        None => None,
    }
}

/// Convert chat completion usage into Responses usage.
fn convert_usage(usage: Option<&Value>) -> Value {
    let empty = json!({
        "input_tokens": 0,
        "input_tokens_details": { "cached_tokens": 0 },
        "output_tokens": 0,
        "output_tokens_details": { "reasoning_tokens": 0 },
        "total_tokens": 0
    });
    let Some(usage) = usage else {
        return empty;
    };

    let prompt_tokens = usage
        .get("prompt_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let completion_tokens = usage
        .get("completion_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(prompt_tokens + completion_tokens);
    let cached_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let reasoning_tokens = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    json!({
        "input_tokens": prompt_tokens,
        "input_tokens_details": { "cached_tokens": cached_tokens },
        "output_tokens": completion_tokens,
        "output_tokens_details": { "reasoning_tokens": reasoning_tokens },
        "total_tokens": total_tokens
    })
}

// ---------------------------------------------------------------------------
// Streaming: chat completion SSE → Responses SSE
// ---------------------------------------------------------------------------

/// A function call accumulated from chat completion stream deltas.
#[derive(Debug, Default, Clone)]
struct ToolCallState {
    call_id: String,
    name: String,
    arguments: String,
    item_id: String,
    output_index: usize,
    announced: bool,
}

/// Translates chat completion SSE into Responses API SSE events.
pub struct ResponsesStreamTranslator {
    /// Produces chat completion SSE from the provider's native stream.
    inner: Box<dyn StreamTranslator>,
    /// Partial chat SSE waiting for a complete event.
    buffer: Vec<u8>,
    /// Whether `response.created` was emitted.
    started: bool,
    /// Whether the terminal event was emitted.
    finished: bool,
    response_id: String,
    message_item_id: String,
    model: String,
    created_at: u64,
    /// Accumulated assistant text.
    text: String,
    /// Whether the message item / content part were announced.
    text_open: bool,
    /// Accumulated tool calls, keyed by the chat delta index.
    tool_calls: Vec<(u64, ToolCallState)>,
    /// Usage reported by the provider, if any.
    usage: Value,
    finish_reason: Option<String>,
    /// Id of the reasoning output item.
    reasoning_item_id: String,
    /// Accumulated reasoning text (from `delta.reasoning_content` and friends).
    reasoning_text: String,
    /// Whether the reasoning output item is currently open.
    reasoning_open: bool,
    /// Output index allocated to the reasoning item.
    reasoning_output_index: Option<usize>,
    /// Monotonic output-index allocator: every output item that opens gets a
    /// unique index, so a client can key items by index without collisions.
    next_output_index: usize,
    /// Output index allocated to the assistant message item.
    message_output_index: usize,
}

impl ResponsesStreamTranslator {
    pub fn new(inner: Box<dyn StreamTranslator>) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            started: false,
            finished: false,
            response_id: generate_id("resp_"),
            message_item_id: generate_id("msg_"),
            reasoning_item_id: generate_id("rs_"),
            reasoning_text: String::new(),
            reasoning_open: false,
            reasoning_output_index: None,
            next_output_index: 0,
            message_output_index: 0,
            model: String::new(),
            created_at: now_secs(),
            text: String::new(),
            text_open: false,
            tool_calls: Vec::new(),
            usage: convert_usage(None),
            finish_reason: None,
        }
    }

    /// The response object as it stands right now — used for `response.created`
    /// and `response.completed`.
    fn response_object(&self, status: ResponseStatus) -> Value {
        let mut output: Vec<Value> = Vec::new();

        // The reasoning item always precedes the message and the calls, the
        // order the items were opened in the stream.
        if !self.reasoning_text.is_empty() {
            output.push(json!({
                "type": "reasoning",
                "id": self.reasoning_item_id,
                "status": if status == ResponseStatus::InProgress {
                    "in_progress"
                } else {
                    "completed"
                },
                "summary": [{ "type": "summary_text", "text": self.reasoning_text }]
            }));
        }

        if self.text_open || !self.text.is_empty() {
            output.push(json!({
                "type": "message",
                "id": self.message_item_id,
                "status": if status == ResponseStatus::InProgress {
                    "in_progress"
                } else {
                    "completed"
                },
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": self.text,
                    "annotations": []
                }]
            }));
        }

        for (_, call) in &self.tool_calls {
            if !call.announced {
                continue;
            }
            output.push(json!({
                "type": "function_call",
                "id": call.item_id,
                "status": if status == ResponseStatus::InProgress {
                    "in_progress"
                } else {
                    "completed"
                },
                "call_id": call.call_id,
                "name": call.name,
                "arguments": call.arguments
            }));
        }

        json!({
            "id": self.response_id,
            "object": "response",
            "created_at": self.created_at,
            "status": status.as_str(),
            "error": Value::Null,
            "incomplete_details": Value::Null,
            "model": self.model,
            "output": output,
            "parallel_tool_calls": true,
            "previous_response_id": Value::Null,
            "usage": if status == ResponseStatus::InProgress {
                convert_usage(None)
            } else {
                self.usage.clone()
            },
        })
    }

    /// Handle one chat completion chunk.
    fn handle_chunk(&mut self, chunk: &Value, out: &mut Vec<u8>) {
        if let Some(model) = chunk.get("model").and_then(|v| v.as_str()) {
            if !model.is_empty() {
                self.model = model.to_string();
            }
        }

        if !self.started {
            self.started = true;
            emit_event(
                out,
                "response.created",
                json!({
                    "type": "response.created",
                    "response": self.response_object(ResponseStatus::InProgress),
                }),
            );
        }

        if let Some(usage) = chunk.get("usage") {
            if !usage.is_null() {
                self.usage = convert_usage(Some(usage));
            }
        }

        let choice = chunk
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|c| c.first());

        if let Some(finish_reason) = choice
            .and_then(|c| c.get("finish_reason"))
            .and_then(|v| v.as_str())
        {
            if !finish_reason.is_empty() {
                self.finish_reason = Some(finish_reason.to_string());
            }
        }

        let delta = choice.and_then(|c| c.get("delta"));

        // Reasoning deltas. Thinking models (deepseek, glm, ...) stream their
        // reasoning before any content or tool call, as `reasoning_content`
        // (DeepSeek style), `reasoning` or `reasoning_text`. The reasoning
        // item is opened first and closed before the next item opens — a
        // Responses client treats an interleaved reasoning start as a broken
        // stream.
        let reasoning_delta = delta
            .and_then(|d| {
                d.get("reasoning_content")
                    .or_else(|| d.get("reasoning"))
                    .or_else(|| d.get("reasoning_text"))
            })
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        if let Some(reasoning) = reasoning_delta {
            if !self.reasoning_open {
                self.close_reasoning(out);
                self.reasoning_open = true;
                self.reasoning_output_index = Some(self.next_output_index);
                self.next_output_index += 1;
                let output_index = self.reasoning_output_index.unwrap_or(0);
                emit_event(
                    out,
                    "response.output_item.added",
                    json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": {
                            "type": "reasoning",
                            "id": self.reasoning_item_id,
                            "status": "in_progress",
                            "summary": []
                        }
                    }),
                );
                emit_event(
                    out,
                    "response.reasoning_summary_part.added",
                    json!({
                        "type": "response.reasoning_summary_part.added",
                        "item_id": self.reasoning_item_id,
                        "output_index": output_index,
                        "summary_index": 0,
                        "part": { "type": "summary_text", "text": "" }
                    }),
                );
            }
            self.reasoning_text.push_str(reasoning);
            emit_event(
                out,
                "response.reasoning_summary_text.delta",
                json!({
                    "type": "response.reasoning_summary_text.delta",
                    "item_id": self.reasoning_item_id,
                    "output_index": self.reasoning_output_index.unwrap_or(0),
                    "summary_index": 0,
                    "delta": reasoning
                }),
            );
        }

        // Assistant text.
        if let Some(text) = delta
            .and_then(|d| d.get("content"))
            .and_then(|c| c.as_str())
        {
            if !text.is_empty() {
                if !self.text_open {
                    // The reasoning item must be closed before the message
                    // item opens.
                    self.close_reasoning(out);
                    self.text_open = true;
                    self.message_output_index = self.next_output_index;
                    self.next_output_index += 1;
                    emit_event(
                        out,
                        "response.output_item.added",
                        json!({
                            "type": "response.output_item.added",
                            "output_index": self.message_output_index,
                            "item": {
                                "type": "message",
                                "id": self.message_item_id,
                                "status": "in_progress",
                                "role": "assistant",
                                "content": []
                            }
                        }),
                    );
                    emit_event(
                        out,
                        "response.content_part.added",
                        json!({
                            "type": "response.content_part.added",
                            "item_id": self.message_item_id,
                            "output_index": self.message_output_index,
                            "content_index": 0,
                            "part": { "type": "output_text", "text": "", "annotations": [] }
                        }),
                    );
                }
                self.text.push_str(text);
                emit_event(
                    out,
                    "response.output_text.delta",
                    json!({
                        "type": "response.output_text.delta",
                        "item_id": self.message_item_id,
                        "output_index": self.message_output_index,
                        "content_index": 0,
                        "delta": text
                    }),
                );
            }
        }

        // Function calls.
        if let Some(calls) = delta
            .and_then(|d| d.get("tool_calls"))
            .and_then(|t| t.as_array())
        {
            for call in calls {
                let index = call.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let slot = match self.tool_calls.iter_mut().find(|(i, _)| *i == index) {
                    Some((_, state)) => state,
                    None => {
                        // The reasoning item (if any) is closed before the
                        // first call opens, so the call gets the next index.
                        self.close_reasoning(out);
                        let output_index = self.next_output_index;
                        self.next_output_index += 1;
                        let state = ToolCallState {
                            item_id: generate_id("fc_"),
                            output_index,
                            ..Default::default()
                        };
                        self.tool_calls.push((index, state));
                        &mut self.tool_calls.last_mut().expect("just pushed").1
                    }
                };

                if let Some(id) = call.get("id").and_then(|v| v.as_str()) {
                    slot.call_id = id.to_string();
                }
                let function = call.get("function");
                if let Some(name) = function
                    .and_then(|f| f.get("name"))
                    .and_then(|v| v.as_str())
                {
                    slot.name = name.to_string();
                }
                if let Some(arguments) = function
                    .and_then(|f| f.get("arguments"))
                    .and_then(|v| v.as_str())
                {
                    if !arguments.is_empty() {
                        if !slot.announced {
                            slot.announced = true;
                            let item_id = slot.item_id.clone();
                            let output_index = slot.output_index;
                            let call_id = slot.call_id.clone();
                            let name = slot.name.clone();
                            emit_event(
                                out,
                                "response.output_item.added",
                                json!({
                                    "type": "response.output_item.added",
                                    "output_index": output_index,
                                    "item": {
                                        "type": "function_call",
                                        "id": item_id,
                                        "status": "in_progress",
                                        "call_id": call_id,
                                        "name": name,
                                        "arguments": ""
                                    }
                                }),
                            );
                        }
                        slot.arguments.push_str(arguments);
                        let item_id = slot.item_id.clone();
                        let output_index = slot.output_index;
                        emit_event(
                            out,
                            "response.function_call_arguments.delta",
                            json!({
                                "type": "response.function_call_arguments.delta",
                                "item_id": item_id,
                                "output_index": output_index,
                                "delta": arguments
                            }),
                        );
                    }
                }
            }
        }
    }

    /// Emit the terminal events.
    fn finalize(&mut self, out: &mut Vec<u8>) {
        if self.finished {
            return;
        }
        self.finished = true;

        if !self.started {
            self.started = true;
            emit_event(
                out,
                "response.created",
                json!({
                    "type": "response.created",
                    "response": self.response_object(ResponseStatus::InProgress),
                }),
            );
        }

        // A stream must not end with an open output item.
        self.close_reasoning(out);

        if self.text_open {
            emit_event(
                out,
                "response.output_text.done",
                json!({
                    "type": "response.output_text.done",
                    "item_id": self.message_item_id,
                    "output_index": self.message_output_index,
                    "content_index": 0,
                    "text": self.text
                }),
            );
            emit_event(
                out,
                "response.content_part.done",
                json!({
                    "type": "response.content_part.done",
                    "item_id": self.message_item_id,
                    "output_index": self.message_output_index,
                    "content_index": 0,
                    "part": { "type": "output_text", "text": self.text, "annotations": [] }
                }),
            );
            emit_event(
                out,
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": self.message_output_index,
                    "item": {
                        "type": "message",
                        "id": self.message_item_id,
                        "status": "completed",
                        "role": "assistant",
                        "content": [{
                            "type": "output_text",
                            "text": self.text,
                            "annotations": []
                        }]
                    }
                }),
            );
        }

        let calls: Vec<ToolCallState> = self
            .tool_calls
            .iter()
            .filter(|(_, state)| state.announced)
            .map(|(_, state)| state.clone())
            .collect();
        for call in calls {
            emit_event(
                out,
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": call.output_index,
                    "item": {
                        "type": "function_call",
                        "id": call.item_id,
                        "status": "completed",
                        "call_id": call.call_id,
                        "name": call.name,
                        "arguments": call.arguments
                    }
                }),
            );
        }

        let (status, incomplete_details) = status_from_finish_reason(self.finish_reason.as_deref());
        let mut response = self.response_object(status);
        if let Some(obj) = response.as_object_mut() {
            obj.insert(
                "incomplete_details".to_string(),
                if status == ResponseStatus::Incomplete {
                    incomplete_details
                } else {
                    Value::Null
                },
            );
        }

        let event_name = if status == ResponseStatus::Incomplete {
            "response.incomplete"
        } else {
            "response.completed"
        };
        emit_event(
            out,
            event_name,
            json!({
                "type": event_name,
                "response": response
            }),
        );

        out.extend_from_slice(b"data: [DONE]\n\n");
    }
}

impl ResponsesStreamTranslator {
    /// Close the open reasoning item, emitting its summary done events. The
    /// Responses stream contract forbids opening a new output item while the
    /// reasoning item is still open.
    fn close_reasoning(&mut self, out: &mut Vec<u8>) {
        if !self.reasoning_open {
            return;
        }
        self.reasoning_open = false;
        let text = self.reasoning_text.clone();
        let output_index = self.reasoning_output_index.unwrap_or(0);
        emit_event(
            out,
            "response.reasoning_summary_text.done",
            json!({
                "type": "response.reasoning_summary_text.done",
                "item_id": self.reasoning_item_id,
                "output_index": output_index,
                "summary_index": 0,
                "text": text
            }),
        );
        emit_event(
            out,
            "response.reasoning_summary_part.done",
            json!({
                "type": "response.reasoning_summary_part.done",
                "item_id": self.reasoning_item_id,
                "output_index": output_index,
                "summary_index": 0,
                "part": { "type": "summary_text", "text": text }
            }),
        );
        emit_event(
            out,
            "response.output_item.done",
            json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": {
                    "type": "reasoning",
                    "id": self.reasoning_item_id,
                    "status": "completed",
                    "summary": [{ "type": "summary_text", "text": text }]
                }
            }),
        );
    }
}

impl StreamTranslator for ResponsesStreamTranslator {
    fn process_chunk(&mut self, data: &[u8], end_of_stream: bool) -> Vec<u8> {
        let chat_sse = self.inner.process_chunk(data, end_of_stream);
        self.buffer.extend_from_slice(&chat_sse);

        let mut out = Vec::new();
        loop {
            let buffer = String::from_utf8_lossy(&self.buffer).to_string();
            let Some(boundary) = buffer.find("\n\n") else {
                break;
            };
            let event: String = buffer[..boundary].to_string();
            self.buffer = self.buffer[boundary + 2..].to_vec();

            let data_line = event.lines().find_map(|line| {
                line.strip_prefix("data:")
                    .map(|d| d.trim_start().to_string())
            });
            let Some(data_line) = data_line else {
                continue;
            };
            if data_line.is_empty() {
                continue;
            }
            if data_line == "[DONE]" {
                self.finalize(&mut out);
                continue;
            }
            if let Ok(chunk) = serde_json::from_str::<Value>(&data_line) {
                self.handle_chunk(&chunk, &mut out);
            }
        }

        if end_of_stream {
            self.finalize(&mut out);
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialize one Responses SSE event.
fn emit_event(out: &mut Vec<u8>, event: &str, payload: Value) {
    out.extend_from_slice(format!("event: {event}\n").as_bytes());
    out.extend_from_slice(format!("data: {payload}\n\n").as_bytes());
}

/// Current unix timestamp in seconds.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Generate a sortable, unique identifier with the given prefix.
fn generate_id(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{nanos:016x}{seq:08x}")
}

/// Build a Responses SSE stream from a complete chat completion body.
///
/// Used for providers that answer a streaming request with a single JSON
/// document (Gemini), where there is no SSE to convert.
pub fn chat_response_to_responses_sse(body: &[u8]) -> Option<Vec<u8>> {
    let chat: Value = serde_json::from_slice(body).ok()?;
    let mut translator = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
    let mut chunk = chat.clone();
    if let Some(obj) = chunk.as_object_mut() {
        obj.insert(
            "object".into(),
            Value::String("chat.completion.chunk".into()),
        );
    }
    // A complete chat completion stores the assistant turn as `message`;
    // the stream translator reads `delta`, so move it across.
    if let Some(choices) = chunk.get_mut("choices").and_then(|c| c.as_array_mut()) {
        for choice in choices {
            let Some(obj) = choice.as_object_mut() else {
                continue;
            };
            let Some(message) = obj.get("message") else {
                continue;
            };
            let delta = json!({
                "role": message.get("role"),
                "content": message.get("content"),
                "tool_calls": message.get("tool_calls"),
            });
            obj.insert("delta".into(), delta);
            obj.remove("message");
        }
    }
    let mut sse = Vec::new();
    sse.extend_from_slice(b"data: ");
    sse.extend_from_slice(&serde_json::to_vec(&chunk).ok()?);
    sse.extend_from_slice(b"\n\ndata: [DONE]\n\n");
    Some(translator.process_chunk(&sse, true))
}

/// Serialize a Responses request for logging or debugging.
#[derive(Debug, Serialize)]
pub struct ResponsesRequestSummary {
    pub model: String,
    pub has_instructions: bool,
    pub input_items: usize,
    pub streaming: bool,
}

impl ResponsesRequest {
    /// Summarize a request without retaining its content.
    pub fn summarize(&self) -> ResponsesRequestSummary {
        let input_items = match self.input.as_ref() {
            Some(Value::Array(items)) => items.len(),
            Some(Value::String(_)) => 1,
            _ => 0,
        };
        ResponsesRequestSummary {
            model: self.model.clone().unwrap_or_default(),
            has_instructions: self.instructions.is_some(),
            input_items,
            streaming: self.stream == Some(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── request translation ─────────────────────────────────────────────

    #[test]
    fn test_string_input_becomes_user_message() {
        let body = br#"{"model":"gpt-4o","input":"hello"}"#;
        let out = responses_request_to_chat(body).unwrap();
        assert!(!out.is_streaming);
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "gpt-4o");
        assert_eq!(v["messages"][0]["role"], "user");
        assert_eq!(v["messages"][0]["content"], "hello");
    }

    #[test]
    fn test_instructions_become_system_message() {
        let body = br#"{"model":"gpt-4o","instructions":"Be terse.","input":"hi"}"#;
        let out = responses_request_to_chat(body).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["messages"][0]["role"], "system");
        assert_eq!(v["messages"][0]["content"], "Be terse.");
        assert_eq!(v["messages"][1]["role"], "user");
    }

    #[test]
    fn test_message_item_array_input() {
        let body = br#"{
            "model":"gpt-4o",
            "input":[
                {"type":"message","role":"user","content":[{"type":"input_text","text":"one"}]},
                {"type":"message","role":"assistant","content":[{"type":"output_text","text":"two"}]},
                {"type":"message","role":"user","content":[{"type":"input_text","text":"three"}]}
            ]
        }"#;
        let out = responses_request_to_chat(body).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        let msgs = v["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["content"], "one");
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["content"], "two");
        assert_eq!(msgs[2]["content"], "three");
    }

    #[test]
    fn test_function_call_round_trip_items() {
        let body = br#"{
            "model":"gpt-4o",
            "input":[
                {"type":"message","role":"user","content":[{"type":"input_text","text":"weather?"}]},
                {"type":"function_call","call_id":"call_1","name":"get_weather","arguments":"{\"city\":\"Paris\"}"},
                {"type":"function_call_output","call_id":"call_1","output":"sunny"}
            ]
        }"#;
        let out = responses_request_to_chat(body).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        let msgs = v["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(msgs[1]["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "call_1");
        assert_eq!(msgs[2]["content"], "sunny");
    }

    #[test]
    fn test_reasoning_items_are_dropped() {
        let body = br#"{
            "model":"gpt-4o",
            "input":[
                {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"thinking"}]},
                {"type":"message","role":"user","content":"hi"}
            ]
        }"#;
        let out = responses_request_to_chat(body).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        let msgs = v["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
    }

    #[test]
    fn test_stream_and_sampling_params() {
        let body = br#"{
            "model":"gpt-4o","input":"hi","stream":true,
            "temperature":0.5,"top_p":0.9,"max_output_tokens":128,
            "stop":["END"],"seed":42
        }"#;
        let out = responses_request_to_chat(body).unwrap();
        assert!(out.is_streaming);
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["stream"], true);
        assert_eq!(v["temperature"], 0.5);
        assert_eq!(v["top_p"], 0.9);
        assert_eq!(v["max_tokens"], 128);
        assert_eq!(v["stop"][0], "END");
        assert_eq!(v["seed"], 42);
        // Streaming asks the upstream for usage so `response.completed` can
        // carry token counts.
        assert_eq!(v["stream_options"]["include_usage"], true);
    }

    #[test]
    fn test_flat_tools_are_nested() {
        let body = br#"{
            "model":"gpt-4o","input":"hi",
            "tools":[{"type":"function","name":"fn","description":"d","parameters":{"type":"object"}}],
            "tool_choice":"auto"
        }"#;
        let out = responses_request_to_chat(body).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        let tool = &v["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "fn");
        assert_eq!(tool["function"]["description"], "d");
        assert_eq!(v["tool_choice"], "auto");
    }

    #[test]
    fn test_missing_model_errors() {
        let body = br#"{"input":"hi"}"#;
        assert!(responses_request_to_chat(body).is_err());
    }

    #[test]
    fn test_missing_input_errors() {
        let body = br#"{"model":"gpt-4o"}"#;
        assert!(responses_request_to_chat(body).is_err());
    }

    // ── response translation ────────────────────────────────────────────

    #[test]
    fn test_chat_response_to_responses() {
        let chat = br#"{
            "id":"chatcmpl-1","object":"chat.completion","created":1700000000,
            "model":"gpt-4o","choices":[{"index":0,"finish_reason":"stop",
                "message":{"role":"assistant","content":"pong"}}],
            "usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}
        }"#;
        let out = chat_response_to_responses(chat).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["object"], "response");
        assert_eq!(v["status"], "completed");
        assert_eq!(v["created_at"], 1700000000);
        assert_eq!(v["model"], "gpt-4o");
        assert!(v["id"].as_str().unwrap().starts_with("resp_"));
        let output = v["output"].as_array().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["type"], "message");
        assert_eq!(output[0]["role"], "assistant");
        assert_eq!(output[0]["content"][0]["type"], "output_text");
        assert_eq!(output[0]["content"][0]["text"], "pong");
        assert_eq!(v["usage"]["input_tokens"], 5);
        assert_eq!(v["usage"]["output_tokens"], 7);
        assert_eq!(v["usage"]["total_tokens"], 12);
    }

    #[test]
    fn test_tool_calls_become_function_call_items() {
        let chat = br#"{
            "id":"chatcmpl-1","object":"chat.completion","created":1700000000,
            "model":"gpt-4o","choices":[{"index":0,"finish_reason":"tool_calls",
                "message":{"role":"assistant","content":null,
                    "tool_calls":[{"id":"call_9","type":"function",
                        "function":{"name":"get_weather","arguments":"{\"city\":\"Rome\"}"}}]}}],
            "usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}
        }"#;
        let out = chat_response_to_responses(chat).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        let output = v["output"].as_array().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["type"], "function_call");
        assert_eq!(output[0]["call_id"], "call_9");
        assert_eq!(output[0]["name"], "get_weather");
        assert_eq!(output[0]["arguments"], "{\"city\":\"Rome\"}");
    }

    #[test]
    fn test_length_finish_reason_becomes_incomplete() {
        let chat = br#"{
            "id":"chatcmpl-1","object":"chat.completion","created":1700000000,
            "model":"gpt-4o","choices":[{"index":0,"finish_reason":"length",
                "message":{"role":"assistant","content":"partial"}}],
            "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}
        }"#;
        let out = chat_response_to_responses(chat).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["status"], "incomplete");
        assert_eq!(v["incomplete_details"]["reason"], "max_output_tokens");
    }

    #[test]
    fn test_reasoning_content_becomes_reasoning_item() {
        let chat = br#"{
            "id":"chatcmpl-1","object":"chat.completion","created":1700000000,
            "model":"deepseek-reasoner","choices":[{"index":0,"finish_reason":"stop",
                "message":{"role":"assistant","content":"answer","reasoning_content":"let me think"}}],
            "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}
        }"#;
        let out = chat_response_to_responses(chat).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        let output = v["output"].as_array().unwrap();
        assert_eq!(output[0]["type"], "reasoning");
        assert_eq!(output[0]["summary"][0]["text"], "let me think");
        assert_eq!(output[1]["type"], "message");
    }

    // ── streaming ───────────────────────────────────────────────────────

    fn chat_sse(chunks: &[&str]) -> String {
        let mut out = String::new();
        for c in chunks {
            out.push_str("data: ");
            out.push_str(c);
            out.push_str("\n\n");
        }
        out.push_str("data: [DONE]\n\n");
        out
    }

    #[test]
    fn test_stream_text_events() {
        let chunks = [
            r#"{"id":"chatcmpl-s","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-s","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"po"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-s","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"ng"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-s","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0}],"finish_reason":"stop","usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();

        assert!(s.contains("event: response.created"));
        assert!(s.contains("event: response.output_item.added"));
        assert!(s.contains("event: response.content_part.added"));
        assert!(s.contains(r#""delta":"po""#));
        assert!(s.contains(r#""delta":"ng""#));
        assert!(s.contains("event: response.output_text.done"));
        assert!(s.contains(r#""text":"pong""#));
        assert!(s.contains("event: response.content_part.done"));
        assert!(s.contains("event: response.output_item.done"));
        assert!(s.contains("event: response.completed"));
        assert!(s.contains(r#""input_tokens":2"#));
        assert!(s.contains(r#""output_tokens":2"#));
        assert!(s.contains("data: [DONE]"));
    }

    #[test]
    fn test_stream_tool_call_events() {
        let chunks = [
            r#"{"id":"chatcmpl-t","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-t","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"fn","arguments":""}}]},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-t","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"x\":1}"}}]},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-t","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0}],"finish_reason":"tool_calls","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();

        assert!(s.contains(r#""type":"function_call""#));
        assert!(s.contains(r#""call_id":"call_1""#));
        assert!(s.contains(r#""name":"fn""#));
        assert!(s.contains("event: response.function_call_arguments.delta"));
        assert!(s.contains(r#""arguments":"{\"x\":1}""#));
        assert!(s.contains("event: response.completed"));
    }

    #[test]
    fn test_stream_incomplete_on_length() {
        let chunks = [
            r#"{"id":"chatcmpl-l","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"abc"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-l","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"finish_reason":"length"}],"usage":{"prompt_tokens":1,"completion_tokens":3,"total_tokens":4}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("event: response.incomplete"));
        assert!(s.contains(r#""reason":"max_output_tokens""#));
    }

    #[test]
    fn test_stream_chunked_delivery() {
        // One logical stream split across arbitrary byte boundaries.
        let full = chat_sse(&[
            r#"{"id":"chatcmpl-c","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-c","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0}],"finish_reason":"stop","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
        ]);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let mut collected = String::new();
        for byte in full.as_bytes().chunks(7) {
            let end = byte.len() < 7;
            let out = t.process_chunk(byte, end);
            collected.push_str(&String::from_utf8(out).unwrap());
        }
        assert!(collected.contains("event: response.created"));
        assert!(collected.contains(r#""delta":"hi""#));
        assert!(collected.contains("event: response.completed"));
        assert!(collected.contains("data: [DONE]"));
    }

    #[test]
    fn test_chat_response_to_responses_sse_bridge() {
        // Gemini answers a streaming request with a single JSON body; the
        // bridge converts it into Responses SSE events.
        let chat = br#"{
            "id":"chatcmpl-g","object":"chat.completion","created":1700000000,
            "model":"gemini-2.5-flash","choices":[{"index":0,"finish_reason":"stop",
                "message":{"role":"assistant","content":"hello"}}],
            "usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}
        }"#;
        let sse = chat_response_to_responses_sse(chat).unwrap();
        let s = String::from_utf8(sse).unwrap();
        assert!(s.contains("event: response.created"));
        assert!(s.contains(r#""delta":"hello""#));
        assert!(s.contains("event: response.output_text.done"));
        assert!(s.contains(r#""text":"hello""#));
        assert!(s.contains("event: response.completed"));
        assert!(s.contains(r#""input_tokens":3"#));
        assert!(s.contains("data: [DONE]"));
    }

    /// Parse the `data:` payloads of a Responses SSE stream into JSON values.
    fn sse_payloads(sse: &str) -> Vec<serde_json::Value> {
        sse.lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .filter(|d| *d != "[DONE]")
            .filter_map(|d| serde_json::from_str::<serde_json::Value>(d).ok())
            .collect()
    }

    /// The item type and `output_index` of every `response.output_item.added`.
    fn added_output_indexes(sse: &str) -> Vec<(String, u64)> {
        sse_payloads(sse)
            .into_iter()
            .filter(|e| e["type"] == "response.output_item.added")
            .map(|e| {
                (
                    e["item"]["type"].as_str().unwrap_or_default().to_string(),
                    e["output_index"].as_u64().unwrap_or_default(),
                )
            })
            .collect()
    }

    #[test]
    fn test_reasoning_deltas_open_and_close_reasoning_item() {
        // A thinking model streams reasoning first, then the answer.
        let chunks = [
            r#"{"id":"chatcmpl-r","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"think"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-r","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"reasoning_content":"ing"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-r","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-r","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0}],"finish_reason":"stop","usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();

        let types: Vec<String> = sse_payloads(&s)
            .iter()
            .map(|e| e["type"].as_str().unwrap_or_default().to_string())
            .collect();
        let pos_reasoning_added = types
            .iter()
            .position(|t| t == "response.output_item.added")
            .expect("an item is added");
        let pos_reasoning_delta = types
            .iter()
            .position(|t| t == "response.reasoning_summary_text.delta")
            .expect("a reasoning delta");
        let pos_reasoning_done = types
            .iter()
            .position(|t| t == "response.reasoning_summary_text.done")
            .expect("a reasoning done");
        let pos_message_added = types
            .iter()
            .enumerate()
            .filter(|(_, t)| *t == "response.output_item.added")
            .map(|(i, _)| i)
            .nth(1)
            .expect("the message item is added");
        assert!(pos_reasoning_added < pos_reasoning_delta);
        assert!(pos_reasoning_delta < pos_reasoning_done);
        assert!(
            pos_reasoning_done < pos_message_added,
            "reasoning must close before the message item opens"
        );

        // The accumulated reasoning is carried in the summary events.
        assert!(s.contains(r#""text":"thinking""#));
        assert!(s.contains("event: response.reasoning_summary_part.done"));
    }

    #[test]
    fn test_reasoning_only_stream_still_produces_output() {
        // Reasoning without any content or tool call: the response must still
        // carry the reasoning item instead of an empty output.
        let chunks = [
            r#"{"id":"chatcmpl-o","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"reasoning_content":"only thinking"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-o","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0}],"finish_reason":"stop","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();

        let payloads = sse_payloads(&s);
        let completed = payloads
            .iter()
            .find(|e| e["type"] == "response.completed")
            .expect("response.completed");
        let output = completed["response"]["output"].as_array().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["type"], "reasoning");
        assert_eq!(output[0]["summary"][0]["text"], "only thinking");
    }

    #[test]
    fn test_output_indexes_are_unique() {
        // Reasoning, message and a function call must not share an index.
        let chunks = [
            r#"{"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"reasoning_content":"hmm"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"content":"text"},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0}],"finish_reason":"tool_calls","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();

        let added = added_output_indexes(&s);
        let kinds: Vec<&str> = added.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["reasoning", "message", "function_call"]);
        let indexes: Vec<u64> = added.iter().map(|(_, i)| *i).collect();
        assert_eq!(indexes, vec![0, 1, 2], "output indexes must be unique");
    }

    #[test]
    fn test_message_output_index_without_reasoning() {
        // Without reasoning the message keeps index 0 — the pre-reasoning
        // contract other clients rely on.
        let chunks = [
            r#"{"id":"chatcmpl-y","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();
        let added = added_output_indexes(&s);
        assert_eq!(added, vec![("message".to_string(), 0)]);
    }

    #[test]
    fn test_reasoning_closes_at_stream_end() {
        // A stream that ends while the reasoning item is still open must
        // close it before the terminal event.
        let chunks = [
            r#"{"id":"chatcmpl-z","object":"chat.completion.chunk","created":1700000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"delta":{"reasoning_content":"cut off"},"finish_reason":null}]}"#,
        ];
        let sse = chat_sse(&chunks);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();
        let pos_done = s
            .find("event: response.reasoning_summary_text.done")
            .unwrap();
        let pos_completed = s.find("event: response.completed").unwrap();
        assert!(pos_done < pos_completed);
        assert!(s.contains("event: response.output_item.done"));
    }

    #[test]
    fn test_nested_function_tools_are_flattened() {
        let body = json!({
            "model": "m",
            "input": "hi",
            "tools": [
                {"type": "function", "function": {"name": "get_weather", "description": "d", "parameters": {"type": "object"}, "strict": true}}
            ]
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        let out = normalize_responses_request(&bytes);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        let tool = &parsed["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["name"], "get_weather");
        assert_eq!(tool["description"], "d");
        assert_eq!(tool["parameters"]["type"], "object");
        assert_eq!(tool["strict"], true);
        assert!(
            tool.get("function").is_none(),
            "nested wrapper must be removed"
        );
    }

    #[test]
    fn test_nested_tool_choice_is_flattened() {
        let body = json!({
            "model": "m",
            "input": "hi",
            "tool_choice": {"type": "function", "function": {"name": "get_weather"}}
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        let out = normalize_responses_request(&bytes);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["tool_choice"]["type"], "function");
        assert_eq!(parsed["tool_choice"]["name"], "get_weather");
        assert!(parsed["tool_choice"].get("function").is_none());
    }

    #[test]
    fn test_flat_tools_are_untouched_byte_for_byte() {
        let body = json!({
            "model": "m",
            "input": "hi",
            "tools": [{"type": "function", "name": "get_weather", "description": "d", "parameters": {"type": "object"}}]
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        assert_eq!(normalize_responses_request(&bytes), bytes);
    }

    #[test]
    fn test_non_function_tool_types_are_untouched() {
        let body = json!({
            "model": "m",
            "input": "hi",
            "tools": [
                {"type": "web_search", "max_results": 3},
                {"type": "custom", "name": "c"}
            ]
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        assert_eq!(normalize_responses_request(&bytes), bytes);
    }

    #[test]
    fn test_invalid_json_passthrough() {
        let out = normalize_responses_request(b"not json");
        assert_eq!(out, b"not json");
    }

    #[test]
    fn test_stream_no_content() {
        // Some providers send usage-only chunks and never open a text part.
        let sse = chat_sse(&[
            r#"{"id":"chatcmpl-e","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[],"usage":null}"#,
            r#"{"id":"chatcmpl-e","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o","choices":[{"index":0}],"finish_reason":"stop","usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1}}"#,
        ]);
        let mut t = ResponsesStreamTranslator::new(Box::new(crate::NativePassthroughStream));
        let out = t.process_chunk(sse.as_bytes(), true);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("event: response.created"));
        assert!(s.contains("event: response.completed"));
        assert!(s.contains("data: [DONE]"));
    }
}
