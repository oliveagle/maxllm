// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! OpenDesign (open-design.ai) provider translator — the "AMR link" endpoint.
//!
//! OpenDesign is a collaborative AI design-agent workspace. Its cloud runtime
//! ("AMR", the vela CLI's ACP mode) reaches the model vendors through an
//! OpenAI-compatible *link* endpoint that the workspace authenticates with an
//! `sk-amr-…` key:
//!
//! | Path | Contract | Serves |
//! |------|----------|--------|
//! | `/v1/models` | OpenAI model list (+ OpenDesign extensions) | the account's catalogue |
//! | `/v1/chat/completions` | OpenAI Chat Completions | every chat model |
//! | `/v1/responses` | OpenAI Responses | every chat model (native, streaming) |
//!
//! This module implements that contract **natively in Rust** — no CLI is
//! spawned, no `od` / `vela` binary is shelled out to. The gateway talks to
//! the link endpoint over plain HTTP exactly like any other provider.
//!
//! ## What the link actually speaks (verified against the live endpoint)
//!
//! Everything below was probed against `https://amr-link.open-design.ai/v1`
//! on 2026-11-08 with a live `sk-amr-…` key:
//!
//! * **Responses is native.** `POST /v1/responses` answers the OpenAI
//!   Responses contract — object shape, `output[]` items, `usage.input_tokens`
//!   / `output_tokens` — and `stream: true` emits the `response.*` SSE event
//!   family (`response.created`, `response.output_item.added`,
//!   `response.reasoning_summary_text.delta`, `response.output_text.delta`,
//!   `response.completed`, …). `store`, `previous_response_id`, `include`,
//!   `reasoning.effort` and `text.verbosity` are all accepted, so a
//!   `kind = "open_design"` provider defaults to `native_responses = true` and
//!   `/v1/responses` requests are forwarded byte-for-byte.
//! * **`tool_choice: "any"` is rejected.** The link answers HTTP 400
//!   `{"error":{"code":"upstream_error",…}}` for the non-standard `"any"`
//!   value (codex-cli sends it), while `"auto"`, `"none"` and `"required"`
//!   are accepted. The request normaliser rewrites `"any"` to `"required"`.
//! * **Unknown models answer 404.** `{"error":{"code":"model_not_found",…}}`
//!   — which the gateway already treats as a failover trigger, so a request
//!   for a model the account does not carry walks the fallback chain.
//! * **The assistant message carries `reasoning_details[]`** (the OpenRouter
//!   shape: `{"index":0,"type":"reasoning.summary","summary":"…"}`) and the
//!   usage block carries `prompt_tokens_details.cached_read_tokens` and
//!   `completion_tokens_details.reasoning_tokens`. Both are passed through
//!   verbatim — a client that understands them keeps working, and the
//!   gateway's own accounting reads `prompt_tokens` / `completion_tokens`
//!   (and `input_tokens` / `output_tokens` on the Responses route).
//! * **Everything else is plain OpenAI.** `developer` roles, `input_text`
//!   content parts, object-valued `tool_calls[].function.arguments` in the
//!   history and a `tool_choice` sent without any `tools` are all accepted.
//!
//! ## Configuration
//!
//! ```toml
//! [providers.open_design]
//! kind = "open_design"
//! base_url = "https://amr-link.open-design.ai/v1"
//! api_key = "${OPEN_DESIGN_API_KEY}"
//! default_model = "deepseek-v4.1-flash"
//! ```

use crate::{ProviderTranslator, StreamTranslator, TranslateError, TranslatedRequest};
use serde_json::Value;

/// Default upstream path of the Chat Completions endpoint.
pub const DEFAULT_CHAT_PATH: &str = "/v1/chat/completions";

/// Default upstream path of the Responses endpoint — served natively.
pub const DEFAULT_RESPONSES_PATH: &str = "/v1/responses";

/// Default upstream path of the model catalogue.
pub const DEFAULT_MODELS_PATH: &str = "/v1/models";

/// The `tool_choice` value OpenAI never defined but codex-cli sends.
const TOOL_CHOICE_ANY: &str = "any";

/// The OpenAI value that means the same thing and the link accepts.
const TOOL_CHOICE_REQUIRED: &str = "required";

/// Apply the OpenDesign-specific request repairs to a chat completions body.
///
/// The link is OpenAI-compatible but rejects the non-standard
/// `tool_choice: "any"` (HTTP 400 `upstream_error`), so it is rewritten to
/// `"required"`, the OpenAI value with the same meaning. Every other field
/// is left alone: the body is round-tripped through `serde_json::Value`, so
/// vendor extensions (`reasoning_effort`, `thinking`, …) and unknown fields
/// survive untouched.
///
/// A body that does not parse as JSON is returned as-is — the gateway forwards
/// it and the upstream produces the (accurate) parse error rather than the
/// gateway guessing.
pub fn normalize_request(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };

    if !normalize_tool_choice(&mut root) {
        return body.to_vec();
    }
    serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
}

/// Rewrite `tool_choice: "any"` to `"required"`. Returns true when the body
/// was changed.
fn normalize_tool_choice(root: &mut Value) -> bool {
    let Some(obj) = root.as_object_mut() else {
        return false;
    };
    let is_any = obj.get("tool_choice").and_then(|v| v.as_str()) == Some(TOOL_CHOICE_ANY);
    if !is_any {
        return false;
    }
    obj.insert(
        "tool_choice".into(),
        Value::String(TOOL_CHOICE_REQUIRED.into()),
    );
    true
}

/// Read the `stream` flag out of a request body.
fn request_is_streaming(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
        .unwrap_or(false)
}

/// Translator for the OpenDesign AMR link endpoint.
pub struct OpenDesignTranslator {
    provider_name: String,
    upstream_path: String,
}

impl OpenDesignTranslator {
    /// Build a translator for `upstream_path` (defaults to
    /// [`DEFAULT_CHAT_PATH`] when empty).
    pub fn new(provider_name: impl Into<String>, upstream_path: impl Into<String>) -> Self {
        let upstream_path = upstream_path.into();
        let upstream_path = if upstream_path.is_empty() {
            DEFAULT_CHAT_PATH.to_string()
        } else {
            upstream_path
        };
        Self {
            provider_name: provider_name.into(),
            upstream_path,
        }
    }

    /// The model-catalogue path of the link endpoint.
    pub fn models_path() -> &'static str {
        DEFAULT_MODELS_PATH
    }
}

impl ProviderTranslator for OpenDesignTranslator {
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
        // The link speaks the OpenAI Chat Completions contract, so the body
        // only needs the OpenDesign-specific repairs (and a rewritten model
        // when a pool / alias picked one).
        let mut normalized = normalize_request(body);
        if let Some(model) = model_override {
            if let Ok(mut root) = serde_json::from_slice::<Value>(&normalized) {
                if let Some(obj) = root.as_object_mut() {
                    obj.insert("model".into(), Value::String(model.to_string()));
                    normalized = serde_json::to_vec(&root)?;
                }
            }
        }
        let is_streaming = request_is_streaming(&normalized);
        Ok(TranslatedRequest {
            body: normalized,
            is_streaming,
        })
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        // Already in OpenAI format. `reasoning_details` and the extended
        // usage details are passed through verbatim.
        Ok(body.to_vec())
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        Box::new(crate::NativePassthroughStream)
    }

    fn upstream_path(&self) -> &str {
        &self.upstream_path
    }

    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)> {
        vec![
            // The link authenticates with a bearer token on every route.
            ("Authorization".to_string(), format!("Bearer {api_key}")),
            ("Content-Type".to_string(), "application/json".to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn translator() -> OpenDesignTranslator {
        OpenDesignTranslator::new("open_design", DEFAULT_CHAT_PATH)
    }

    #[test]
    fn test_default_paths() {
        assert_eq!(translator().upstream_path(), "/v1/chat/completions");
        assert_eq!(DEFAULT_RESPONSES_PATH, "/v1/responses");
        assert_eq!(OpenDesignTranslator::models_path(), "/v1/models");
        // An empty path falls back to the default.
        assert_eq!(
            OpenDesignTranslator::new("x", "").upstream_path(),
            "/v1/chat/completions"
        );
    }

    #[test]
    fn test_bearer_auth_header() {
        let headers = translator().upstream_headers("sk-amr-test");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk-amr-test"));
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Content-Type" && v == "application/json"));
    }

    #[test]
    fn test_tool_choice_any_is_rewritten_to_required() {
        // The live link answers 400 upstream_error for "any".
        let body = json!({
            "model": "deepseek-v4.1-flash",
            "messages": [{"role": "user", "content": "hi"}],
            "tool_choice": "any",
        });
        let out = translator()
            .translate_request(&serde_json::to_vec(&body).unwrap(), None)
            .unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["tool_choice"], "required");
    }

    #[test]
    fn test_other_tool_choice_values_are_untouched() {
        for choice in ["auto", "none", "required"] {
            let body = json!({
                "model": "m",
                "messages": [],
                "tool_choice": choice,
            });
            let raw = serde_json::to_vec(&body).unwrap();
            let out = translator().translate_request(&raw, None).unwrap();
            let v: Value = serde_json::from_slice(&out.body).unwrap();
            assert_eq!(v["tool_choice"], choice);
        }
    }

    #[test]
    fn test_tool_choice_object_form_is_untouched() {
        let body = json!({
            "model": "m",
            "messages": [],
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "tool_choice": {"type": "function", "function": {"name": "f"}},
        });
        let raw = serde_json::to_vec(&body).unwrap();
        let out = translator().translate_request(&raw, None).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["tool_choice"]["function"]["name"], "f");
    }

    #[test]
    fn test_request_is_passed_through_otherwise() {
        let body =
            br#"{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}"#;
        let out = translator().translate_request(body, None).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "deepseek-v4.1-flash");
        assert_eq!(v["messages"][0]["content"], "hi");
        assert!(!out.is_streaming);
    }

    #[test]
    fn test_vendor_extensions_survive() {
        let body = json!({
            "model": "deepseek-v4.1-flash",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "high",
            "unknown_vendor_field": {"a": 1},
        });
        let out = translator()
            .translate_request(&serde_json::to_vec(&body).unwrap(), None)
            .unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["reasoning_effort"], "high");
        assert_eq!(v["unknown_vendor_field"]["a"], 1);
    }

    #[test]
    fn test_model_override_is_applied() {
        let body = br#"{"model":"deepseek-v4.1-flash","messages":[]}"#;
        let out = translator()
            .translate_request(body, Some("deepseek-v4-pro"))
            .unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "deepseek-v4-pro");
    }

    #[test]
    fn test_streaming_flag_is_detected() {
        let body = br#"{"model":"m","stream":true,"messages":[]}"#;
        assert!(
            translator()
                .translate_request(body, None)
                .unwrap()
                .is_streaming
        );
    }

    #[test]
    fn test_non_json_body_is_forwarded_unchanged() {
        let body = b"not json at all";
        let out = translator().translate_request(body, None).unwrap();
        assert_eq!(out.body, body.to_vec());
        assert!(!out.is_streaming);
    }

    #[test]
    fn test_response_is_passed_through_with_reasoning_details() {
        // The link answers with the OpenRouter-style reasoning_details array.
        let body = br#"{"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi","reasoning_details":[{"index":0,"type":"reasoning.summary","summary":"thinking"}]}}],"usage":{"prompt_tokens":31,"completion_tokens":17}}"#;
        let out = translator().translate_response(body).unwrap();
        assert_eq!(out, body.to_vec());
    }

    #[test]
    fn test_stream_translator_is_a_passthrough() {
        let mut st = translator().streaming_translator();
        let chunk = b"data: {\"id\":\"1\"}\n\n";
        assert_eq!(st.process_chunk(chunk, false), chunk.to_vec());
    }
}
