// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! OpenCode (opencode.ai "zen") compatibility translator.
//!
//! The opencode.ai gateway (`/zen/go/v1/chat/completions`) is OpenAI-compatible
//! but has stricter validation than most providers. Every rule below was
//! verified against the live endpoint (`deepseek-v4.1-flash`):
//!
//! * **Session header** — every request must carry an `x-opencode-session`
//!   header (used for upstream routing / prompt-cache affinity). Without it
//!   the upstream rejects the call with `400 MissingSessionID`
//!   (`{"type":"error","error":{"type":"MissingSessionID",…}}`). The opencode
//!   CLI sends it; third-party clients often don't. The gateway injects a
//!   session id when the client did not provide one (see `SESSION_HEADER`).
//! * **tool_choice** — only `"none"` / `"auto"` / `"required"` or a tool
//!   choice object; the non-standard `"any"` (sent by codex-cli) is rejected
//!   with a 400 invalid-parameter error.
//! * **tool call arguments** — `tool_calls[].function.arguments` must be a
//!   JSON *string*; object/array values are rejected with
//!   `cannot unmarshal object into … function.arguments of type string`.
//!   Missing `arguments` / `type` fields are also rejected.
//! * **function names** — empty `tool_calls[].function.name` is rejected with
//!   a missing-parameter 400 (a client can produce it when a streaming
//!   `id: null` continuation fragment clobbers the captured name).
//! * **tool_choice without tools** — a `tool_choice` sent while `tools` is
//!   empty/absent is rejected by strict OpenAI-style upstreams; it is dropped.
//!
//! This module also carries the conversation-history repairs proven by the
//! one-api opencode channel (tool_call id / orphan repair, reasoning_content
//! echo collapse + placeholder injection). They are harmless against the
//! upstream (all shapes are accepted) and protect clients whose persisted
//! history was damaged by an earlier upstream.
//!
//! The Responses API is served natively: `/zen/go/v1/responses` accepts the
//! OpenAI Responses contract (verified), so `kind = "opencode"` providers
//! default to `native_responses = true` and `/v1/responses` requests are
//! forwarded byte-for-byte.

use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};
use serde_json::{Map, Value};

/// Header opencode.ai requires on every `/zen` request.
pub const SESSION_HEADER: &str = "x-opencode-session";

/// Placeholder injected into thinking-mode assistant messages that made tool
/// calls but lost their `reasoning_content` (dropped by many chat emitters).
/// The upstream accepts any non-empty string; only *absence* triggers the
/// `reasoning_content must be passed back` 400 on stricter models.
pub const REASONING_PLACEHOLDER: &str = "Tool execution in progress";

/// Content of the synthetic tool response inserted when a client lost a
/// tool result (e.g. after session resume or context truncation).
pub const SYNTHETIC_TOOL_CONTENT: &str = "Tool execution was not recorded";

// ---------------------------------------------------------------------------
// Request normalisation
// ---------------------------------------------------------------------------

/// Apply every opencode-specific request repair to a chat completions body,
/// with reasoning-placeholder injection enabled (the safe default).
///
/// Returns the (possibly unchanged) original bytes. A body that does not
/// parse as JSON is returned as-is — the gateway forwards it and the upstream
/// produces the (accurate) parse error rather than the gateway guessing.
pub fn normalize_request(body: &[u8]) -> Vec<u8> {
    normalize_request_opts(body, false)
}

/// Apply every opencode-specific request repair to a chat completions body.
///
/// `skip_reasoning_injection` disables the `reasoning_content` placeholder
/// injection for thinking-mode turns (echo collapse always runs). Channels
/// whose upstream does not require `reasoning_content` back can set it to
/// avoid feeding a fake reasoning marker into the model context.
///
/// Returns the (possibly unchanged) original bytes.
pub fn normalize_request_opts(body: &[u8], skip_reasoning_injection: bool) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };

    let mut changed = false;
    changed |= normalize_message_roles(&mut root);
    changed |= normalize_content_part_types(&mut root);
    changed |= normalize_tool_call_arguments(&mut root);
    changed |= normalize_tool_choice(&mut root);
    changed |= repair_tool_call_ids(&mut root);
    changed |= repair_tool_call_function_names(&mut root);
    changed |= repair_orphaned_tool_calls(&mut root);
    changed |= normalize_reasoning_content(&mut root, skip_reasoning_injection);

    if changed {
        serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
    } else {
        body.to_vec()
    }
}

fn messages_mut(root: &mut Value) -> Option<&mut Vec<Value>> {
    root.get_mut("messages").and_then(|m| m.as_array_mut())
}

/// Map `role: "developer"` back to `"system"` (codex-cli / o-series SDKs).
/// Returns true if any role was rewritten.
pub fn normalize_message_roles(root: &mut Value) -> bool {
    let mut modified = false;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            if msg.get("role").and_then(|r| r.as_str()) == Some("developer") {
                if let Some(obj) = msg.as_object_mut() {
                    obj.insert("role".into(), Value::String("system".into()));
                    modified = true;
                }
            }
        }
    }
    modified
}

/// Rename Responses-origin content part types (`input_text` / `output_text`)
/// to their chat equivalents (`text`). Returns true if any was rewritten.
pub fn normalize_content_part_types(root: &mut Value) -> bool {
    let mut modified = false;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            let Some(parts) = msg.get_mut("content").and_then(|c| c.as_array_mut()) else {
                continue;
            };
            for part in parts.iter_mut() {
                let Some(obj) = part.as_object_mut() else {
                    continue;
                };
                if let Some(t) = obj.get("type").and_then(|t| t.as_str()) {
                    if t == "input_text" || t == "output_text" {
                        obj.insert("type".into(), Value::String("text".into()));
                        modified = true;
                    }
                }
            }
        }
    }
    modified
}

/// Make every `tool_calls[]` entry schema-complete for the strict upstream:
///
/// * `type` — insert `"function"` when missing (the OpenAI schema requires it
///   and the upstream rejects entries without it).
/// * `function.arguments` — must be a JSON *string*. codex-cli serialises
///   them as objects; the upstream rejects that with an unmarshal 400. A
///   missing field is inserted as `"{}"`; `null` becomes `"{}"` too (an
///   explicit `"null"` would reach the model as JSON null arguments).
///
/// Returns true if any entry was rewritten.
pub fn normalize_tool_call_arguments(root: &mut Value) -> bool {
    let mut modified = false;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            let Some(tool_calls) = msg.get_mut("tool_calls").and_then(|t| t.as_array_mut()) else {
                continue;
            };
            for tc in tool_calls.iter_mut() {
                let Some(obj) = tc.as_object_mut() else {
                    continue;
                };
                if !obj.contains_key("type") {
                    obj.insert("type".into(), Value::String("function".into()));
                    modified = true;
                }
                let function = obj
                    .entry("function")
                    .or_insert_with(|| Value::Object(Map::new()));
                let Some(fobj) = function.as_object_mut() else {
                    continue;
                };
                match fobj.get("arguments") {
                    None => {
                        fobj.insert("arguments".into(), Value::String("{}".into()));
                        modified = true;
                    }
                    Some(v) if !v.is_string() => {
                        let s = if v.is_null() {
                            "{}".to_string()
                        } else {
                            serde_json::to_string(v).unwrap_or_default()
                        };
                        fobj.insert("arguments".into(), Value::String(s));
                        modified = true;
                    }
                    Some(_) => {}
                }
            }
        }
    }
    modified
}

/// Normalise `tool_choice` to a value the upstream accepts: `"none"`,
/// `"auto"`, `"required"` or an object. Any other string (e.g. the
/// non-standard `"any"` sent by codex-cli) is mapped to `"required"`.
///
/// A `tool_choice` sent while `tools` is empty or absent is dropped
/// entirely — strict OpenAI-style upstreams reject `tool_choice` without
/// any tool defined, and dropping it is semantically equivalent (there is
/// nothing to call).
///
/// Returns true if it was rewritten.
pub fn normalize_tool_choice(root: &mut Value) -> bool {
    let Some(obj) = root.as_object_mut() else {
        return false;
    };
    let Some(choice) = obj.get("tool_choice") else {
        return false;
    };
    let has_tools = obj
        .get("tools")
        .and_then(|t| t.as_array())
        .is_some_and(|a| !a.is_empty());
    if !has_tools {
        obj.remove("tool_choice");
        return true;
    }
    if !choice.is_string() || choice.as_str().is_none() {
        return false; // object — leave as-is
    }
    let tc = choice.as_str().unwrap_or_default();
    if tc == "none" || tc == "auto" || tc == "required" {
        return false;
    }
    obj.insert("tool_choice".into(), Value::String("required".into()));
    true
}

/// Assign synthetic ids to assistant tool_calls with EMPTY ids, and rewrite
/// the paired tool responses' `tool_call_id` so the pairing survives.
/// Pairing is positional: the tool messages immediately following an
/// assistant message respond to its calls in order.
/// Returns true if any ids were rewritten.
pub fn repair_tool_call_ids(root: &mut Value) -> bool {
    let Some(messages) = messages_mut(root) else {
        return false;
    };
    let mut modified = false;
    let mut seq: usize = 0;
    for i in 0..messages.len() {
        if messages[i].get("role").and_then(|r| r.as_str()) != Some("assistant") {
            continue;
        }
        let Some(tool_calls) = messages[i]
            .get_mut("tool_calls")
            .and_then(|t| t.as_array_mut())
        else {
            continue;
        };
        let mut pending: Vec<String> = Vec::new();
        for tc in tool_calls.iter_mut() {
            let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or_default();
            if id.trim().is_empty() {
                seq += 1;
                let new_id = format!("call_maxllm_{seq}");
                if let Some(obj) = tc.as_object_mut() {
                    obj.insert("id".into(), Value::String(new_id.clone()));
                    pending.push(new_id);
                    modified = true;
                }
            }
        }
        if pending.is_empty() {
            continue;
        }
        // Rewrite empty tool_call_ids in the tool messages that follow,
        // consuming the synthetic ids in order.
        let mut next = 0usize;
        let mut k = i + 1;
        while k < messages.len() {
            if messages[k].get("role").and_then(|r| r.as_str()) != Some("tool") {
                break;
            }
            if next < pending.len() {
                let tc_id = messages[k]
                    .get("tool_call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if tc_id.trim().is_empty() {
                    if let Some(obj) = messages[k].as_object_mut() {
                        obj.insert("tool_call_id".into(), Value::String(pending[next].clone()));
                        next += 1;
                        modified = true;
                    }
                }
            }
            k += 1;
        }
    }
    modified
}

/// Give assistant tool_calls with EMPTY function names a usable name.
/// When the request carries exactly one tool a nameless call is that tool
/// with near certainty and inherits its name; otherwise a distinguishable
/// placeholder (`unknown_tool_<n>`) is assigned.
/// Returns true if any names were rewritten.
pub fn repair_tool_call_function_names(root: &mut Value) -> bool {
    let single_tool = root
        .get("tools")
        .and_then(|t| t.as_array())
        .filter(|t| t.len() == 1)
        .and_then(|t| t[0].get("function"))
        .and_then(|f| f.get("name"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string());

    let mut modified = false;
    let mut seq: usize = 0;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            let Some(tool_calls) = msg.get_mut("tool_calls").and_then(|t| t.as_array_mut()) else {
                continue;
            };
            for tc in tool_calls.iter_mut() {
                let name = tc
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default();
                if name.trim().is_empty() {
                    seq += 1;
                    let new_name = single_tool
                        .clone()
                        .unwrap_or_else(|| format!("unknown_tool_{seq}"));
                    if let Some(f) = tc
                        .as_object_mut()
                        .and_then(|o| o.get_mut("function"))
                        .and_then(|f| f.as_object_mut())
                    {
                        f.insert("name".into(), Value::String(new_name));
                        modified = true;
                    }
                }
            }
        }
    }
    modified
}

/// Fix conversation histories where tool calls and tool responses diverge:
///
/// 1. Missing tool responses — insert a synthetic `"tool"` message for each
///    `tool_call_id` the assistant issued but the client never answered
///    (counting duplicates, so N identical calls get N responses).
/// 2. Orphaned tool responses — drop `"tool"` messages whose
///    `tool_call_id` is empty, unknown, or references a call issued *later*
///    in the conversation (a forward reference). Validity is positional:
///    a tool response may only answer a call issued before it.
///
/// Returns true if any messages were inserted or dropped.
pub fn repair_orphaned_tool_calls(root: &mut Value) -> bool {
    let Some(messages) = messages_mut(root) else {
        return false;
    };
    let mut cleaned: Vec<Value> = Vec::with_capacity(messages.len());
    // Ids issued by assistant messages seen so far — a tool response is only
    // valid when it answers a call that precedes it.
    let mut valid_ids: Vec<String> = Vec::new();
    let mut modified = false;

    let mut i = 0usize;
    while i < messages.len() {
        let msg = messages[i].clone();
        let role = msg.get("role").and_then(|r| r.as_str());

        if role == Some("assistant") {
            let calls: Vec<String> = msg
                .get("tool_calls")
                .and_then(|t| t.as_array())
                .map(|t| {
                    t.iter()
                        .filter_map(|tc| {
                            tc.get("id").and_then(|v| v.as_str()).map(|s| s.to_string())
                        })
                        .filter(|s| !s.trim().is_empty())
                        .collect()
                })
                .unwrap_or_default();

            cleaned.push(msg);
            for id in &calls {
                valid_ids.push(id.clone());
            }

            // Consume the tool responses that immediately follow, keeping only
            // those that answer a call issued so far.
            i += 1;
            let mut responded: Vec<String> = Vec::new();
            while i < messages.len() {
                let m = messages[i].clone();
                if m.get("role").and_then(|r| r.as_str()) != Some("tool") {
                    break;
                }
                let tid = m.get("tool_call_id").and_then(|v| v.as_str()).unwrap_or("");
                if !tid.trim().is_empty() {
                    responded.push(tid.to_string());
                }
                if valid_ids.contains(&tid.to_string()) {
                    cleaned.push(m);
                } else {
                    modified = true; // empty / unknown / forward-reference id
                }
                i += 1;
            }

            // Insert synthetic responses for calls nobody answered. Counted
            // per unique id: N identical ids need N responses, but the
            // shortfall must be inserted once per id, not once per occurrence.
            let mut seen: Vec<&String> = Vec::new();
            for id in &calls {
                if seen.contains(&id) {
                    continue;
                }
                seen.push(id);
                let needed = calls.iter().filter(|c| *c == id).count();
                let have = responded.iter().filter(|r| *r == id).count();
                for _ in have..needed {
                    cleaned.push(serde_json::json!({
                        "role": "tool",
                        "tool_call_id": id,
                        "content": SYNTHETIC_TOOL_CONTENT,
                    }));
                    modified = true;
                }
            }
            continue;
        }

        if role == Some("tool") {
            // A tool message that does not immediately follow an assistant
            // tool-call message. Keep it only if it answers a call already
            // issued earlier in the conversation.
            let tid = msg
                .get("tool_call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !tid.trim().is_empty() && valid_ids.contains(&tid.to_string()) {
                cleaned.push(msg);
            } else {
                modified = true;
            }
            i += 1;
            continue;
        }

        cleaned.push(msg);
        i += 1;
    }

    if modified {
        if let Some(arr) = messages_mut(root) {
            *arr = cleaned;
        }
    }
    modified
}

/// Normalise `reasoning_content` on assistant messages for thinking-mode
/// upstreams:
///
/// 1. **Echo collapse (always)** — strip every occurrence of the injected
///    placeholder from `reasoning_content`. Thinking upstreams echo the
///    placeholder back as part of their own reasoning, clients persist that,
///    and each subsequent turn replays and re-echoes it (one measured
///    session snowballed to 745 repeats). Content that was only placeholders
///    is dropped to `null`.
/// 2. **Dialect mirror** — the opencode client may carry reasoning under
///    `reasoning` or `reasoning_text` (its chat schema accepts all three).
///    When `reasoning_content` is empty but one of the other variants holds
///    text, mirror it into `reasoning_content`, the field the strict
///    upstream requires.
/// 3. **Placeholder injection (unless `skip_injection`)** — every
///    thinking-mode assistant message (tool_calls + no reasoning) gets a
///    stable placeholder, because the strict upstream rejects thinking turns
///    that lost their `reasoning_content` with
///    `the reasoning_content in the thinking mode must be passed back`.
///
/// Returns true if any message was rewritten.
pub fn normalize_reasoning_content(root: &mut Value, skip_injection: bool) -> bool {
    let mut modified = false;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            if msg.get("role").and_then(|r| r.as_str()) != Some("assistant") {
                continue;
            }
            let Some(obj) = msg.as_object_mut() else {
                continue;
            };
            // Echo collapse — strip every placeholder occurrence.
            if let Some(rc) = obj.get("reasoning_content").and_then(|v| v.as_str()) {
                if rc != REASONING_PLACEHOLDER && rc.contains(REASONING_PLACEHOLDER) {
                    // Drop the placeholder and tidy the whitespace it left
                    // behind, so the echoed marker cannot accumulate.
                    let stripped = rc.replace(REASONING_PLACEHOLDER, " ");
                    let collapsed = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
                    if collapsed.is_empty() {
                        obj.insert("reasoning_content".into(), Value::Null);
                    } else {
                        obj.insert("reasoning_content".into(), Value::String(collapsed));
                    }
                    modified = true;
                }
            }
            // Mirror alternate reasoning dialects into `reasoning_content`.
            let has_reasoning = match obj.get("reasoning_content") {
                None | Some(Value::Null) => false,
                Some(Value::String(s)) => !s.trim().is_empty(),
                _ => true,
            };
            if !has_reasoning {
                for variant in ["reasoning", "reasoning_text"] {
                    if let Some(text) = obj
                        .get(variant)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.trim().is_empty())
                    {
                        obj.insert("reasoning_content".into(), Value::String(text.to_string()));
                        modified = true;
                        break;
                    }
                }
            }
            // Placeholder injection for thinking turns without reasoning.
            if skip_injection {
                continue;
            }
            let has_calls = obj
                .get("tool_calls")
                .and_then(|t| t.as_array())
                .is_some_and(|t| !t.is_empty());
            if has_calls {
                let reasoning_empty = match obj.get("reasoning_content") {
                    None | Some(Value::Null) => true,
                    Some(Value::String(s)) => s.trim().is_empty(),
                    _ => false,
                };
                if reasoning_empty {
                    obj.insert(
                        "reasoning_content".into(),
                        Value::String(REASONING_PLACEHOLDER.to_string()),
                    );
                    modified = true;
                }
            }
        }
    }
    modified
}

// ---------------------------------------------------------------------------
// ProviderTranslator
// ---------------------------------------------------------------------------

/// OpenAI-compatible passthrough translator for opencode.ai, with the
/// opencode-specific request normalisation applied.
pub struct OpenCodeTranslator {
    provider_name: String,
    upstream_path: String,
    skip_reasoning_injection: bool,
}

impl OpenCodeTranslator {
    pub fn new(provider_name: impl Into<String>, upstream_path: impl Into<String>) -> Self {
        Self::with_options(provider_name, upstream_path, false)
    }

    /// Construct with `skip_reasoning_injection`: when true the
    /// `reasoning_content` placeholder is never injected (echo collapse still
    /// runs). Set it on channels whose upstream does not require
    /// `reasoning_content` back.
    pub fn with_options(
        provider_name: impl Into<String>,
        upstream_path: impl Into<String>,
        skip_reasoning_injection: bool,
    ) -> Self {
        Self {
            provider_name: provider_name.into(),
            upstream_path: upstream_path.into(),
            skip_reasoning_injection,
        }
    }
}

impl ProviderTranslator for OpenCodeTranslator {
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
        let normalized = normalize_request_opts(body, self.skip_reasoning_injection);
        if let Some(model) = model_override {
            let mut req: serde_json::Value = serde_json::from_slice(&normalized)?;
            if let Some(obj) = req.as_object_mut() {
                obj.insert("model".into(), Value::String(model.to_string()));
            }
            let is_streaming = req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
            Ok(TranslatedRequest {
                body: serde_json::to_vec(&req)?,
                is_streaming,
            })
        } else {
            let req: serde_json::Value = serde_json::from_slice(&normalized)?;
            let is_streaming = req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
            Ok(TranslatedRequest {
                body: normalized,
                is_streaming,
            })
        }
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        // Already in OpenAI format.
        Ok(body.to_vec())
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        Box::new(OpenCodePassthroughStream)
    }

    fn upstream_path(&self) -> &str {
        &self.upstream_path
    }

    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)> {
        vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Authorization".to_string(), format!("Bearer {api_key}")),
        ]
    }
}

/// Passthrough SSE stream (already in OpenAI format).
struct OpenCodePassthroughStream;

impl StreamTranslator for OpenCodePassthroughStream {
    fn process_chunk(&mut self, data: &[u8], _end_of_stream: bool) -> Vec<u8> {
        data.to_vec()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn norm(body: Value) -> Value {
        let bytes = serde_json::to_vec(&body).unwrap();
        let out = normalize_request(&bytes);
        serde_json::from_slice(&out).unwrap()
    }

    #[test]
    fn test_no_op_normalisation() {
        let body = json!({"model":"m","messages":[{"role":"user","content":"hi"}]});
        let bytes = serde_json::to_vec(&body).unwrap();
        let out = normalize_request(&bytes);
        assert_eq!(out, bytes);
    }

    #[test]
    fn test_invalid_json_passthrough() {
        let out = normalize_request(b"not json");
        assert_eq!(out, b"not json");
    }

    #[test]
    fn test_developer_role() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"developer","content":"be terse"},{"role":"user","content":"hi"}]}));
        assert_eq!(out["messages"][0]["role"], "system");
    }

    #[test]
    fn test_content_part_types() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":[{"type":"input_text","text":"a"},{"type":"output_text","text":"b"}]}]}));
        assert_eq!(out["messages"][0]["content"][0]["type"], "text");
        assert_eq!(out["messages"][0]["content"][1]["type"], "text");
    }

    #[test]
    fn test_tool_choice_any() {
        // "any" with tools declared maps to "required".
        let mut out = norm(json!({"model":"m","messages":[],"tools":[
            {"type":"function","function":{"name":"f","parameters":{}}}],"tool_choice":"any"}));
        assert_eq!(out["tool_choice"], "required");
        for ok in ["none", "auto", "required"] {
            out = norm(json!({"model":"m","messages":[],"tools":[
                {"type":"function","function":{"name":"f","parameters":{}}}],"tool_choice":ok}));
            assert_eq!(out["tool_choice"], ok);
        }
        // Object is left untouched.
        out = norm(json!({"model":"m","messages":[],"tools":[
                {"type":"function","function":{"name":"f","parameters":{}}}],
                "tool_choice":{"type":"function","function":{"name":"f"}}}));
        assert!(out["tool_choice"].is_object());
    }

    #[test]
    fn test_tool_choice_dropped_without_tools() {
        // A tool_choice sent with no tools is rejected by strict upstreams;
        // dropping it is semantically equivalent.
        let out = norm(json!({"model":"m","messages":[],"tool_choice":"any"}));
        assert!(out.get("tool_choice").is_none());
        let out = norm(json!({"model":"m","messages":[],
            "tool_choice":{"type":"function","function":{"name":"f"}}}));
        assert!(out.get("tool_choice").is_none());
    }

    #[test]
    fn test_tool_call_arguments_object_to_string() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":{"city":"BJ"}}}]}]}));
        let args = out["messages"][0]["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .unwrap();
        let parsed: Value = serde_json::from_str(args).unwrap();
        assert_eq!(parsed["city"], "BJ");
    }

    #[test]
    fn test_empty_function_name_single_tool() {
        let out = norm(json!({"model":"m","tools":[
            {"type":"function","function":{"name":"get_weather","parameters":{}}}],
            "messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"","arguments":"{}"}}]}]}));
        assert_eq!(
            out["messages"][0]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
    }

    #[test]
    fn test_empty_function_name_multiple_tools() {
        let out = norm(json!({"model":"m","tools":[
            {"type":"function","function":{"name":"a"}},{"type":"function","function":{"name":"b"}}],
            "messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"","arguments":"{}"}},
                {"id":"c2","type":"function","function":{"name":"","arguments":"{}"}}]}]}));
        assert_eq!(
            out["messages"][0]["tool_calls"][0]["function"]["name"],
            "unknown_tool_1"
        );
        assert_eq!(
            out["messages"][0]["tool_calls"][1]["function"]["name"],
            "unknown_tool_2"
        );
    }

    #[test]
    fn test_repair_empty_tool_call_ids() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"","type":"function","function":{"name":"f","arguments":"{}"}},
                {"id":"c2","type":"function","function":{"name":"f","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"","content":"a"},
            {"role":"tool","tool_call_id":"c2","content":"b"}]}));
        assert_eq!(out["messages"][1]["tool_calls"][0]["id"], "call_maxllm_1");
        assert_eq!(out["messages"][2]["tool_call_id"], "call_maxllm_1");
        assert_eq!(out["messages"][1]["tool_calls"][1]["id"], "c2");
        assert_eq!(out["messages"][3]["tool_call_id"], "c2");
    }

    #[test]
    fn test_insert_missing_tool_response() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        assert_eq!(out["messages"].as_array().unwrap().len(), 3);
        assert_eq!(out["messages"][2]["role"], "tool");
        assert_eq!(out["messages"][2]["tool_call_id"], "c1");
        assert_eq!(out["messages"][2]["content"], SYNTHETIC_TOOL_CONTENT);
    }

    #[test]
    fn test_drop_orphaned_tool_response() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"tool","tool_call_id":"call_zzz","content":"sunny"}]}));
        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_reasoning_content_injection() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"c1","content":"a"}]}));
        assert_eq!(
            out["messages"][1]["reasoning_content"],
            REASONING_PLACEHOLDER
        );
    }

    #[test]
    fn test_reasoning_content_echo_collapse() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":"pre Tool execution in progress mid Tool execution in progress post",
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        let rc = out["messages"][0]["reasoning_content"].as_str().unwrap();
        assert!(!rc.contains(REASONING_PLACEHOLDER));
        assert_eq!(rc, "pre mid post");
    }

    #[test]
    fn test_reasoning_content_only_placeholder() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":REASONING_PLACEHOLDER,
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        // Re-injected once (placeholder-only collapses to empty → injected).
        assert_eq!(
            out["messages"][0]["reasoning_content"],
            REASONING_PLACEHOLDER
        );
    }

    #[test]
    fn test_reasoning_content_untouched_when_present() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":"my thoughts",
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        assert_eq!(out["messages"][0]["reasoning_content"], "my thoughts");
    }

    #[test]
    fn test_translator_roundtrip() {
        let t = OpenCodeTranslator::new("opencode_go_1", "/zen/go/v1/chat/completions");
        let body = json!({"model":"deepseek-v4.1-flash","messages":[
            {"role":"user","content":"hi"}],"tools":[
            {"type":"function","function":{"name":"f","parameters":{}}}],
            "tool_choice":"any","max_tokens":16});
        let bytes = serde_json::to_vec(&body).unwrap();
        let result = t.translate_request(&bytes, None).unwrap();
        assert!(!result.is_streaming);
        let parsed: Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(parsed["tool_choice"], "required");
    }

    #[test]
    fn test_translator_streaming_detection() {
        let t = OpenCodeTranslator::new("test", "/zen/go/v1/chat/completions");
        let body = json!({"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true});
        let bytes = serde_json::to_vec(&body).unwrap();
        let result = t.translate_request(&bytes, None).unwrap();
        assert!(result.is_streaming);
    }

    #[test]
    fn test_translator_model_override() {
        let t = OpenCodeTranslator::new("test", "/zen/go/v1/chat/completions");
        let body = json!({"model":"orig","messages":[{"role":"user","content":"hi"}]});
        let bytes = serde_json::to_vec(&body).unwrap();
        let result = t
            .translate_request(&bytes, Some("deepseek-v4.1-flash"))
            .unwrap();
        let parsed: Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(parsed["model"], "deepseek-v4.1-flash");
    }

    #[test]
    fn test_translator_headers() {
        let t = OpenCodeTranslator::new("test", "/zen/go/v1/chat/completions");
        let headers = t.upstream_headers("oc_sk_x");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer oc_sk_x"));
    }

    #[test]
    fn test_tool_call_missing_arguments_and_type() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","function":{"name":"f"}}]}]}));
        let tc = &out["messages"][0]["tool_calls"][0];
        assert_eq!(tc["type"], "function");
        assert_eq!(tc["function"]["arguments"], "{}");
    }

    #[test]
    fn test_tool_call_null_arguments_becomes_empty_object() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":null}}]}]}));
        assert_eq!(
            out["messages"][0]["tool_calls"][0]["function"]["arguments"],
            "{}"
        );
    }

    #[test]
    fn test_orphan_forward_reference_dropped() {
        // A tool response referencing a call issued LATER is a forward
        // reference — it must be dropped, not kept.
        let out = norm(json!({"model":"m","messages":[
            {"role":"tool","tool_call_id":"call_later","content":"a"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"call_later","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2, "orphan dropped, synthetic inserted");
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(msgs[1]["role"], "tool");
        assert_eq!(msgs[1]["tool_call_id"], "call_later");
        assert_eq!(msgs[1]["content"], SYNTHETIC_TOOL_CONTENT);
    }

    #[test]
    fn test_orphan_empty_id_dropped() {
        // A standalone tool message with an empty tool_call_id cannot be
        // paired with anything — drop it.
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"tool","tool_call_id":"","content":"a"}]}));
        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
        assert_eq!(out["messages"][0]["role"], "user");
    }

    #[test]
    fn test_reasoning_variant_mirrored_to_reasoning_content() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning":"my reasoning",
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        assert_eq!(out["messages"][0]["reasoning_content"], "my reasoning");
    }

    #[test]
    fn test_reasoning_text_variant_mirrored() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_text":"r",
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        assert_eq!(out["messages"][0]["reasoning_content"], "r");
    }

    #[test]
    fn test_reasoning_injection_skipped() {
        let body = json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]});
        let bytes = serde_json::to_vec(&body).unwrap();
        let out = normalize_request_opts(&bytes, true);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["messages"][0].get("reasoning_content").is_none());
    }

    #[test]
    fn test_synthetic_response_inserted_for_duplicate_calls() {
        // Two identical call ids need two tool responses; only one exists.
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}},
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"c1","content":"a"}]}));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "c1");
        assert_eq!(msgs[2]["content"], SYNTHETIC_TOOL_CONTENT);
    }

    #[test]
    fn test_normalization_idempotent() {
        let body = json!({"model":"m","messages":[
            {"role":"developer","content":"sys"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"","type":"function","function":{"name":"","arguments":{"x":1}}}]},
            {"role":"tool","tool_call_id":"","content":"a"}],
            "tool_choice":"any"});
        let bytes = serde_json::to_vec(&body).unwrap();
        let once = normalize_request(&bytes);
        let twice = normalize_request(&once);
        assert_eq!(once, twice, "normalization must be idempotent");
    }

    #[test]
    fn test_streaming_passthrough() {
        let t = OpenCodeTranslator::new("test", "/p");
        let mut st = t.streaming_translator();
        let out = st.process_chunk(b"data: hello\n\n", false);
        assert_eq!(out, b"data: hello\n\n");
    }

    // ── mutation-test hardening: argument/branch pins ──────────────────

    #[test]
    fn test_placeholder_only_reasoning_is_kept() {
        // `reasoning_content` that IS exactly the placeholder is not an echo
        // (there is nothing around it to collapse); it must survive.
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":REASONING_PLACEHOLDER}]}));
        assert_eq!(
            out["messages"][0]["reasoning_content"],
            REASONING_PLACEHOLDER
        );
    }

    #[test]
    fn test_whitespace_reasoning_is_replaced_by_the_dialect_variant() {
        // Whitespace-only `reasoning_content` counts as missing, so the
        // mirrored variant text must replace it.
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":"   ","reasoning":"real thinking"}]}));
        assert_eq!(out["messages"][0]["reasoning_content"], "real thinking");
    }

    #[test]
    fn test_present_reasoning_is_not_overwritten_by_the_variant() {
        // Real reasoning content must win over the alternate dialect.
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":"keep","reasoning":"other"}]}));
        assert_eq!(out["messages"][0]["reasoning_content"], "keep");
    }

    #[test]
    fn test_whitespace_reasoning_with_tool_calls_gets_the_placeholder() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","reasoning_content":"  ",
             "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}]}));
        assert_eq!(
            out["messages"][0]["reasoning_content"],
            REASONING_PLACEHOLDER
        );
    }

    #[test]
    fn test_valid_tool_response_survives_the_orphan_repair() {
        // A tool response that answers a call issued before it must be kept.
        let out = norm(json!({"model":"m","messages":[
            {"role":"user","content":"q"},
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
            {"role":"tool","tool_call_id":"c1","content":"ok"}]}));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "c1");
        assert_eq!(msgs[2]["content"], "ok");
    }

    #[test]
    fn test_translator_name_and_response_passthrough() {
        let t = OpenCodeTranslator::new("opencode_go", "/zen/go/v1/chat/completions");
        assert_eq!(t.name(), "opencode_go");
        assert_eq!(t.upstream_path(), "/zen/go/v1/chat/completions");
        let body = br#"{"id":"x","object":"chat.completion","choices":[]}"#;
        let out = t.translate_response(body).unwrap();
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_orphan_tool_response_to_an_earlier_call_is_kept() {
        // The tool response does not *immediately* follow its assistant call
        // (a user message sits in between), but it still answers a call that
        // was issued earlier, so the orphan repair must keep it. (The
        // assistant branch also inserts a synthetic answer for the call it
        // saw as unanswered — that is separate and unchanged.)
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
            {"role":"user","content":"q"},
            {"role":"tool","tool_call_id":"c1","content":"ok"}]}));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 4);
        assert_eq!(
            msgs[1]["role"], "tool",
            "synthetic answer for the unseen call"
        );
        assert_eq!(msgs[2]["role"], "user");
        assert_eq!(
            msgs[3]["role"], "tool",
            "the earlier-issued call keeps its response"
        );
        assert_eq!(msgs[3]["tool_call_id"], "c1");
        assert_eq!(msgs[3]["content"], "ok");
    }
}
