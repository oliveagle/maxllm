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
//! but has stricter validation than most providers:
//!
//! * **Session header** — every request must carry an `x-opencode-session`
//!   header (used for upstream routing / prompt-cache affinity). The opencode
//!   CLI sends it; third-party clients often don't, and the upstream rejects
//!   those with `400 MissingSessionID`. The gateway injects a session id when
//!   the client did not provide one (see `SESSION_HEADER`).
//! * **tool_choice** — only `"none"` / `"auto"` / `"required"` or a tool
//!   choice object; the non-standard `"any"` (sent by codex-cli) is rejected.
//! * **tool call arguments** — `tool_calls[].function.arguments` must be a
//!   JSON *string*; object/array values are rejected with an unmarshal error.
//! * **function names** — empty `tool_calls[].function.name` is rejected with
//!   a missing-parameter 400 (a client can produce it when a streaming
//!   `id: null` continuation fragment clobbers the captured name).
//!
//! This module also carries the conversation-history repairs proven by the
//! one-api opencode channel (tool_call id / orphan repair, reasoning_content
//! echo collapse + placeholder injection). They are harmless against the
//! upstream (all shapes are accepted) and protect clients whose persisted
//! history was damaged by an earlier upstream.

use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};
use serde_json::Value;

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

/// Apply every opencode-specific request repair to a chat completions body.
///
/// Returns the (possibly unchanged) original bytes. A body that does not
/// parse as JSON is returned as-is — the gateway forwards it and the upstream
/// produces the (accurate) parse error rather than the gateway guessing.
pub fn normalize_request(body: &[u8]) -> Vec<u8> {
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
    changed |= normalize_reasoning_content(&mut root);

    if changed {
        serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
    } else {
        body.to_vec()
    }
}

fn messages_mut(root: &mut Value) -> Option<&mut Vec<Value>> {
    root.get_mut("messages")
        .and_then(|m| m.as_array_mut())
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

/// Coerce every `tool_calls[].function.arguments` to a JSON string.
/// codex-cli serialises them as objects; the upstream rejects that.
/// Returns true if any arguments were rewritten.
pub fn normalize_tool_call_arguments(root: &mut Value) -> bool {
    let mut modified = false;
    if let Some(messages) = messages_mut(root) {
        for msg in messages.iter_mut() {
            let Some(tool_calls) = msg.get_mut("tool_calls").and_then(|t| t.as_array_mut()) else {
                continue;
            };
            for tc in tool_calls.iter_mut() {
                let Some(args) = tc
                    .get_mut("function")
                    .and_then(|f| f.get_mut("arguments"))
                else {
                    continue;
                };
                if !args.is_string() {
                    let s = serde_json::to_string(&args).unwrap_or_default();
                    *args = Value::String(s);
                    modified = true;
                }
            }
        }
    }
    modified
}

/// Normalise `tool_choice` to a value the upstream accepts: `"none"`,
/// `"auto"`, `"required"` or an object. Any other string (e.g. the
/// non-standard `"any"` sent by codex-cli) is mapped to `"required"`.
/// Returns true if it was rewritten.
pub fn normalize_tool_choice(root: &mut Value) -> bool {
    let Some(obj) = root.as_object_mut() else {
        return false;
    };
    let Some(choice) = obj.get("tool_choice") else {
        return false;
    };
    if !choice.is_string() || choice.as_str().is_none() {
        return false; // object / absent — leave as-is
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
        let Some(tool_calls) = messages[i].get_mut("tool_calls").and_then(|t| t.as_array_mut())
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
///    `tool_call_id` the assistant issued but the client never answered.
/// 2. Orphaned tool responses — drop `"tool"` messages whose
///    `tool_call_id` does not match any assistant call in the conversation.
///
/// Returns true if any messages were inserted or dropped.
pub fn repair_orphaned_tool_calls(root: &mut Value) -> bool {
    let Some(messages) = messages_mut(root) else {
        return false;
    };
    let mut repaired: Vec<Value> = Vec::with_capacity(messages.len());
    let mut modified = false;

    let mut i = 0usize;
    while i < messages.len() {
        let msg = messages[i].clone();
        repaired.push(msg.clone());

        if msg.get("role").and_then(|r| r.as_str()) == Some("assistant") {
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

            if !calls.is_empty() {
                // Consume the tool responses that immediately follow.
                i += 1;
                let tool_start = repaired.len();
                while i < messages.len() {
                    if messages[i].get("role").and_then(|r| r.as_str()) != Some("tool") {
                        break;
                    }
                    repaired.push(messages[i].clone());
                    i += 1;
                }
                // Which of the calls were answered?
                let responded: Vec<String> = repaired[tool_start..]
                    .iter()
                    .filter_map(|m| {
                        m.get("tool_call_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();
                for id in &calls {
                    if !responded.contains(id) {
                        repaired.push(serde_json::json!({
                            "role": "tool",
                            "tool_call_id": id,
                            "content": SYNTHETIC_TOOL_CONTENT,
                        }));
                        modified = true;
                    }
                }
                continue;
            }
        }
        i += 1;
    }

    // Drop orphaned tool responses whose tool_call_id matches no call.
    let mut valid: Vec<String> = Vec::new();
    for m in repaired.iter() {
        if m.get("role").and_then(|r| r.as_str()) != Some("assistant") {
            continue;
        }
        if let Some(calls) = m.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in calls {
                if let Some(id) = tc.get("id").and_then(|v| v.as_str()) {
                    if !id.trim().is_empty() {
                        valid.push(id.to_string());
                    }
                }
            }
        }
    }

    let before = repaired.len();
    let cleaned: Vec<Value> = repaired
        .into_iter()
        .filter(|m| {
            if m.get("role").and_then(|r| r.as_str()) != Some("tool") {
                return true;
            }
            let id = m.get("tool_call_id").and_then(|v| v.as_str()).unwrap_or("");
            id.trim().is_empty() || valid.contains(&id.to_string())
        })
        .collect();
    if cleaned.len() != before {
        modified = true;
    }

    if modified {
        if let Some(arr) = messages_mut(root) {
            *arr = cleaned;
        }
    }
    modified
}

/// Ensure every thinking-mode assistant message (tool_calls + empty content)
/// carries a `reasoning_content`, and collapse any echoes of the injected
/// placeholder from the conversation history (clients persist the upstream's
/// echoed placeholder and it would otherwise snowball turn over turn).
/// Returns true if any message was rewritten.
pub fn normalize_reasoning_content(root: &mut Value) -> bool {
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
                    let collapsed = stripped
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    if collapsed.is_empty() {
                        obj.insert("reasoning_content".into(), Value::Null);
                    } else {
                        obj.insert("reasoning_content".into(), Value::String(collapsed));
                    }
                    modified = true;
                }
            }
            // Placeholder injection for thinking turns without reasoning.
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
}

impl OpenCodeTranslator {
    pub fn new(provider_name: impl Into<String>, upstream_path: impl Into<String>) -> Self {
        Self {
            provider_name: provider_name.into(),
            upstream_path: upstream_path.into(),
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
        let normalized = normalize_request(body);
        if let Some(model) = model_override {
            let mut req: serde_json::Value = serde_json::from_slice(&normalized)?;
            if let Some(obj) = req.as_object_mut() {
                obj.insert("model".into(), Value::String(model.to_string()));
            }
            let is_streaming = req
                .get("stream")
                .and_then(|s| s.as_bool())
                .unwrap_or(false);
            Ok(TranslatedRequest {
                body: serde_json::to_vec(&req)?,
                is_streaming,
            })
        } else {
            let req: serde_json::Value = serde_json::from_slice(&normalized)?;
            let is_streaming = req
                .get("stream")
                .and_then(|s| s.as_bool())
                .unwrap_or(false);
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
        let mut out = norm(json!({"model":"m","messages":[],"tool_choice":"any"}));
        assert_eq!(out["tool_choice"], "required");
        for ok in ["none", "auto", "required"] {
            out = norm(json!({"model":"m","messages":[],"tool_choice":ok}));
            assert_eq!(out["tool_choice"], ok);
        }
        // Object is left untouched.
        out = norm(json!({"model":"m","messages":[],"tool_choice":{"type":"function","function":{"name":"f"}}}));
        assert!(out["tool_choice"].is_object());
    }

    #[test]
    fn test_tool_call_arguments_object_to_string() {
        let out = norm(json!({"model":"m","messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"f","arguments":{"city":"BJ"}}}]}]}));
        let args = out["messages"][0]["tool_calls"][0]["function"]["arguments"].as_str().unwrap();
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
        assert_eq!(out["messages"][0]["tool_calls"][0]["function"]["name"], "get_weather");
    }

    #[test]
    fn test_empty_function_name_multiple_tools() {
        let out = norm(json!({"model":"m","tools":[
            {"type":"function","function":{"name":"a"}},{"type":"function","function":{"name":"b"}}],
            "messages":[
            {"role":"assistant","content":"","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"","arguments":"{}"}},
                {"id":"c2","type":"function","function":{"name":"","arguments":"{}"}}]}]}));
        assert_eq!(out["messages"][0]["tool_calls"][0]["function"]["name"], "unknown_tool_1");
        assert_eq!(out["messages"][0]["tool_calls"][1]["function"]["name"], "unknown_tool_2");
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
        assert_eq!(out["messages"][1]["reasoning_content"], REASONING_PLACEHOLDER);
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
        assert_eq!(out["messages"][0]["reasoning_content"], REASONING_PLACEHOLDER);
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
            {"role":"user","content":"hi"}],"tool_choice":"any","max_tokens":16});
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
        let result = t.translate_request(&bytes, Some("deepseek-v4.1-flash")).unwrap();
        let parsed: Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(parsed["model"], "deepseek-v4.1-flash");
    }

    #[test]
    fn test_translator_headers() {
        let t = OpenCodeTranslator::new("test", "/zen/go/v1/chat/completions");
        let headers = t.upstream_headers("oc_sk_x");
        assert!(headers.iter().any(|(k, v)| k == "Authorization" && v == "Bearer oc_sk_x"));
    }

    #[test]
    fn test_streaming_passthrough() {
        let t = OpenCodeTranslator::new("test", "/p");
        let mut st = t.streaming_translator();
        let out = st.process_chunk(b"data: hello\n\n", false);
        assert_eq!(out, b"data: hello\n\n");
    }
}
