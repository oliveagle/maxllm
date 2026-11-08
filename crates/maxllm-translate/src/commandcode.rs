// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Command Code (commandcode.ai "provider") compatibility translator.
//!
//! commandcode.ai is a multi-vendor gateway: one key, one credit balance, and
//! every top model (Claude, GPT, Gemini and the open models) behind it. The
//! Provider API lives under `https://api.commandcode.ai/provider` and serves
//! three wire contracts:
//!
//! | Path | Contract | Serves |
//! |------|----------|--------|
//! | `/provider/v1/chat/completions` | OpenAI Chat Completions | every non-Claude model |
//! | `/provider/v1/responses` | OpenAI Responses | every non-Claude model that lists `/responses` |
//! | `/provider/v1/messages` | Anthropic Messages | the Claude models **only** |
//!
//! All three authenticate with `Authorization: Bearer <CMD_API_KEY>` — *not*
//! Anthropic's `x-api-key` header, even on the Anthropic-shaped route.
//!
//! Which contract a provider entry speaks is decided by its upstream path: a
//! path ending in `/messages` is the Anthropic Messages contract (so the
//! gateway translates OpenAI chat into Anthropic Messages and the Anthropic
//! answer back), anything else is OpenAI Chat Completions. That lets one
//! deployment expose both halves of the catalogue:
//!
//! ```toml
//! [providers.cmd_open]                       # open models + GPT + Gemini
//! kind = "commandcode"
//! base_url = "https://api.commandcode.ai"
//!
//! [providers.cmd_claude]                     # Claude models
//! kind = "commandcode"
//! base_url = "https://api.commandcode.ai"
//! upstream_path = "/provider/v1/messages"
//! ```
//!
//! The live model catalogue (ids, context lengths and the endpoints each
//! model is served on) is published at `GET /provider/v1/models`.

use crate::anthropic::{anthropic_to_openai_response, openai_to_anthropic_request};
use crate::anthropic_stream::AnthropicToOpenAIStream;
use crate::formats::{AnthropicResponse, OpenAIChatRequest};
use crate::{
    NativePassthroughStream, ProviderTranslator, StreamTranslator, TranslateError,
    TranslatedRequest,
};

/// Default upstream path of the OpenAI Chat Completions endpoint.
pub const DEFAULT_CHAT_PATH: &str = "/provider/v1/chat/completions";

/// Default upstream path of the OpenAI Responses endpoint.
pub const DEFAULT_RESPONSES_PATH: &str = "/provider/v1/responses";

/// Default upstream path of the Anthropic Messages endpoint — the only route
/// that serves commandcode.ai's Claude models.
pub const DEFAULT_MESSAGES_PATH: &str = "/provider/v1/messages";

/// `anthropic-version` header sent on the Anthropic Messages route.
///
/// commandcode.ai mirrors Anthropic's Messages contract; Anthropic's own API
/// rejects a request without this header, so it is sent unconditionally on
/// that route (the OpenAI-shaped routes ignore it).
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The wire contract a Command Code upstream speaks on a given path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandCodeWire {
    /// OpenAI Chat Completions (`/provider/v1/chat/completions`).
    ChatCompletions,
    /// Anthropic Messages (`/provider/v1/messages`).
    AnthropicMessages,
}

impl CommandCodeWire {
    /// Infer the contract from an upstream path: a `/messages` path speaks the
    /// Anthropic Messages contract, everything else is Chat Completions.
    pub fn from_path(path: &str) -> Self {
        let trimmed = path.trim_end_matches('/');
        if trimmed.ends_with("/messages") {
            Self::AnthropicMessages
        } else {
            Self::ChatCompletions
        }
    }
}

/// Translator for the commandcode.ai Provider API.
pub struct CommandCodeTranslator {
    provider_name: String,
    upstream_path: String,
    wire: CommandCodeWire,
}

impl CommandCodeTranslator {
    /// Build a translator for `upstream_path`; the wire contract is inferred
    /// from the path (see [`CommandCodeWire::from_path`]).
    pub fn new(provider_name: impl Into<String>, upstream_path: impl Into<String>) -> Self {
        let upstream_path = upstream_path.into();
        let wire = CommandCodeWire::from_path(&upstream_path);
        Self {
            provider_name: provider_name.into(),
            upstream_path,
            wire,
        }
    }

    /// The wire contract this translator speaks.
    pub fn wire(&self) -> CommandCodeWire {
        self.wire
    }
}

impl ProviderTranslator for CommandCodeTranslator {
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
        let req: OpenAIChatRequest = serde_json::from_slice(body)?;
        match self.wire {
            CommandCodeWire::ChatCompletions => {
                // Already the upstream's contract: pass it through, only
                // rewriting the model when the pool/alias picked one.
                let is_streaming = req.stream.unwrap_or(false);
                let body = match model_override {
                    Some(model) => {
                        let mut req = req;
                        req.model = model.to_string();
                        serde_json::to_vec(&req)?
                    }
                    None => body.to_vec(),
                };
                Ok(TranslatedRequest { body, is_streaming })
            }
            CommandCodeWire::AnthropicMessages => {
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
        }
    }

    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>, TranslateError> {
        match self.wire {
            CommandCodeWire::ChatCompletions => Ok(body.to_vec()),
            CommandCodeWire::AnthropicMessages => {
                let resp: AnthropicResponse = serde_json::from_slice(body)?;
                let openai_resp = anthropic_to_openai_response(&resp);
                Ok(serde_json::to_vec(&openai_resp)?)
            }
        }
    }

    fn streaming_translator(&self) -> Box<dyn StreamTranslator> {
        match self.wire {
            CommandCodeWire::ChatCompletions => Box::new(NativePassthroughStream),
            CommandCodeWire::AnthropicMessages => Box::new(AnthropicToOpenAIStream::new()),
        }
    }

    fn upstream_path(&self) -> &str {
        &self.upstream_path
    }

    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)> {
        let mut headers = vec![
            // commandcode.ai authenticates with a bearer token on *every*
            // route — including the Anthropic-shaped one.
            ("Authorization".to_string(), format!("Bearer {api_key}")),
            ("Content-Type".to_string(), "application/json".to_string()),
        ];
        if self.wire == CommandCodeWire::AnthropicMessages {
            headers.push((
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ));
        }
        headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat() -> CommandCodeTranslator {
        CommandCodeTranslator::new("cmd_open", DEFAULT_CHAT_PATH)
    }

    fn claude() -> CommandCodeTranslator {
        CommandCodeTranslator::new("cmd_claude", DEFAULT_MESSAGES_PATH)
    }

    #[test]
    fn test_wire_is_inferred_from_the_path() {
        assert_eq!(chat().wire(), CommandCodeWire::ChatCompletions);
        assert_eq!(claude().wire(), CommandCodeWire::AnthropicMessages);
        // A trailing slash does not change the verdict.
        assert_eq!(
            CommandCodeTranslator::new("c", "/provider/v1/messages/").wire(),
            CommandCodeWire::AnthropicMessages
        );
    }

    #[test]
    fn test_default_paths_are_the_documented_ones() {
        assert_eq!(chat().upstream_path(), "/provider/v1/chat/completions");
        assert_eq!(claude().upstream_path(), "/provider/v1/messages");
    }

    #[test]
    fn test_chat_request_is_passed_through() {
        let body = br#"{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}"#;
        let out = chat().translate_request(body, None).unwrap();
        assert_eq!(out.body, body.to_vec());
        assert!(!out.is_streaming);
    }

    #[test]
    fn test_chat_request_honours_the_model_override() {
        let body = br#"{"model":"auto","messages":[{"role":"user","content":"hi"}]}"#;
        let out = chat()
            .translate_request(body, Some("google/gemini-3.5-flash"))
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "google/gemini-3.5-flash");
    }

    #[test]
    fn test_chat_streaming_flag_is_detected() {
        let body = br#"{"model":"m","stream":true,"messages":[]}"#;
        assert!(chat().translate_request(body, None).unwrap().is_streaming);
    }

    #[test]
    fn test_chat_response_is_passed_through() {
        let body = br#"{"id":"x","object":"chat.completion","choices":[]}"#;
        assert_eq!(chat().translate_response(body).unwrap(), body.to_vec());
    }

    #[test]
    fn test_bearer_auth_on_every_route() {
        let headers = chat().upstream_headers("cmd-key");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer cmd-key"));
        // The OpenAI-shaped route carries no anthropic-version header.
        assert!(!headers.iter().any(|(k, _)| k == "anthropic-version"));

        let headers = claude().upstream_headers("cmd-key");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer cmd-key"));
        assert!(headers
            .iter()
            .any(|(k, v)| k == "anthropic-version" && v == ANTHROPIC_VERSION));
    }

    #[test]
    fn test_messages_request_is_translated_to_anthropic() {
        let body = json!({
            "model": "claude-sonnet-5-5",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 64,
        });
        let out = claude()
            .translate_request(&serde_json::to_vec(&body).unwrap(), None)
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "claude-sonnet-5-5");
        assert_eq!(v["max_tokens"], 64);
        assert_eq!(v["messages"][0]["role"], "user");
        // The Anthropic contract has no "messages[].role == system" — the
        // system prompt moves to the top-level "system" field.
        assert!(v.get("system").is_none());
    }

    #[test]
    fn test_messages_system_prompt_is_extracted() {
        let body = json!({
            "model": "claude-sonnet-5-5",
            "messages": [
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "hi"},
            ],
        });
        let out = claude()
            .translate_request(&serde_json::to_vec(&body).unwrap(), None)
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["system"], "be terse");
        assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_messages_request_honours_the_model_override() {
        let body = json!({
            "model": "auto",
            "messages": [{"role": "user", "content": "hi"}],
        });
        let out = claude()
            .translate_request(&serde_json::to_vec(&body).unwrap(), Some("claude-opus-5-5"))
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "claude-opus-5-5");
    }

    #[test]
    fn test_messages_response_is_translated_back_to_openai() {
        let anthropic = json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-5-5",
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 7, "output_tokens": 3},
        });
        let out = claude()
            .translate_response(&serde_json::to_vec(&anthropic).unwrap())
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["object"], "chat.completion");
        assert_eq!(v["choices"][0]["message"]["content"], "hello");
        assert_eq!(v["usage"]["prompt_tokens"], 7);
        assert_eq!(v["usage"]["completion_tokens"], 3);
    }

    #[test]
    fn test_messages_stream_translator_converts_sse() {
        let mut st = claude().streaming_translator();
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude-sonnet-5-5\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n"
        );
        let out = st.process_chunk(sse.as_bytes(), false);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("chat.completion.chunk"), "{text}");
        assert!(text.contains("\"hi\""), "{text}");
    }

    #[test]
    fn test_chat_stream_translator_is_a_passthrough() {
        let mut st = chat().streaming_translator();
        let chunk = b"data: {\"id\":\"1\"}\n\n";
        assert_eq!(st.process_chunk(chunk, false), chunk.to_vec());
    }
}
