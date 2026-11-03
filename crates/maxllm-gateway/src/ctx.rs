// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use maxllm_config::EndpointType;
use maxllm_plugin::guardrail::GuardrailVerdict;
use maxllm_plugin::PluginCtx;
use maxllm_translate::StreamTranslator;
use std::sync::Mutex;
use std::time::Instant;

/// Per-request context carried through the Pingora ProxyHttp lifecycle.
pub struct RequestCtx {
    /// Index into the config routes array (-1 = unmatched).
    pub route_index: Option<usize>,
    /// The selected provider name for this request.
    pub provider_name: String,
    /// Whether the request is a streaming request.
    pub is_streaming: bool,
    /// SSE stream translator (provider → OpenAI format).
    /// Wrapped in Mutex for Sync bound required by Pingora's CTX.
    pub stream_translator: Mutex<Option<Box<dyn StreamTranslator>>>,
    /// Buffer for incoming request body chunks (accumulated in request_body_filter).
    pub request_body_buf: Vec<u8>,
    /// Whether body translation is done.
    pub body_translated: bool,
    /// Buffer for incoming response body chunks (for non-streaming translation).
    pub response_body_buf: Vec<u8>,
    /// When the request started.
    pub request_start: Instant,
    /// Token usage extracted from the response.
    pub tokens_in: u64,
    pub tokens_out: u64,
    /// Model used for the request.
    pub model: String,
    /// Whether a fallback provider was used.
    pub fallback_used: bool,
    /// Original provider that was tried first (if fallback was used).
    pub original_provider: Option<String>,
    /// Per-request cost in USD.
    pub cost_usd: f64,
    /// Cost reported by the upstream itself (e.g. opencode.ai's
    /// `usage.opencode.allowance`), used when the price table has no entry.
    pub upstream_cost_usd: Option<f64>,
    /// When the request was sent to upstream (set at end of upstream_request_filter).
    pub upstream_send_time: Option<Instant>,
    /// Skip the logging hook (health/metrics endpoints).
    pub skip_logging: bool,
    /// Plugin context visible to all plugins.
    pub plugin_ctx: PluginCtx,
    /// Client-requested guardrail names (from request body `guardrails` field).
    pub requested_guardrails: Option<Vec<String>>,
    /// Route-specific guardrail scope (from route config `guardrails` field).
    pub route_guardrails: Option<Vec<String>>,
    /// Set when a pre-call guardrail blocks the request.
    pub guardrail_blocked: Option<GuardrailVerdict>,
    /// Names of guardrails that were applied to this request.
    pub applied_guardrails: Vec<String>,
    /// Endpoint type for this request (normal, native, passthrough).
    pub endpoint_type: EndpointType,
    /// For passthrough mode: the path suffix after the route prefix to forward upstream.
    /// e.g., request to `/passthrough/anthropic/v1/messages` → suffix `/v1/messages`.
    pub passthrough_path: Option<String>,
    /// Upstream response HTTP status code (set in response_filter).
    pub upstream_status: u16,
    /// Whether to preserve/update Content-Length on the upstream request
    /// (for providers like OpenAI that reject chunked encoding).
    pub keep_request_content_length: bool,
    /// The selected provider speaks the Responses contract natively, so the
    /// gateway forwards the request and response bodies unchanged.
    pub native_responses: bool,
    /// Rolling tail of the streaming SSE body, kept so a token-usage line
    /// split across chunks is still parsed once the rest arrives.
    pub sse_tail: Vec<u8>,
    /// How many providers this request already walked through. Every
    /// failover (an HTTP error from the upstream or a connect failure)
    /// switches to the next provider of the route's fallback chain and
    /// counts here; the route's `max_failover_attempts` bounds the walk.
    pub failover_count: u32,
    /// Providers this request already tried (mirrors litellm_lb_go's
    /// `attempted` list), so the fallback walk never goes backwards through
    /// the chain — the walk is strictly `primary → fb1 → fb2 → …`.
    pub attempted_providers: Vec<String>,
    /// Session identity for session-sticky routing (from headers and/or body).
    pub session_id: Option<String>,
    /// Per-request UUID, used to correlate recording events of one exchange.
    pub request_uuid: String,
    /// Time-to-first-token (ms) for streaming responses, set when the first
    /// response chunk arrives.
    pub ttft_ms: Option<u64>,
    /// Client request method and path, captured early for the recorder.
    pub request_method: String,
    pub request_path: String,
    /// Original client request body, captured once the full body is read
    /// (before translation). Used by the recorder's `client_request` event.
    pub client_body: Option<Vec<u8>>,
    /// Whether the `client_request` recorder event has been emitted for this
    /// request (dedupe across body-chunk calls).
    pub client_request_recorded: bool,
    /// Upstream response body (non-streaming: full body; streaming: first
    /// chunk). Used by the recorder's `client_response` event.
    pub upstream_body: Option<Vec<u8>>,
}

impl RequestCtx {
    pub fn new() -> Self {
        Self {
            route_index: None,
            provider_name: String::new(),
            is_streaming: false,
            stream_translator: Mutex::new(None),
            request_body_buf: Vec::new(),
            body_translated: false,
            response_body_buf: Vec::new(),
            request_start: Instant::now(),
            tokens_in: 0,
            tokens_out: 0,
            model: String::new(),
            fallback_used: false,
            original_provider: None,
            cost_usd: 0.0,
            upstream_cost_usd: None,
            upstream_send_time: None,
            skip_logging: false,
            plugin_ctx: PluginCtx::new(),
            requested_guardrails: None,
            route_guardrails: None,
            guardrail_blocked: None,
            applied_guardrails: Vec::new(),
            endpoint_type: EndpointType::default(),
            passthrough_path: None,
            upstream_status: 0,
            keep_request_content_length: false,
            native_responses: false,
            sse_tail: Vec::new(),
            failover_count: 0,
            attempted_providers: Vec::new(),
            session_id: None,
            request_uuid: uuid::Uuid::now_v7().to_string(),
            ttft_ms: None,
            request_method: String::new(),
            request_path: String::new(),
            client_body: None,
            client_request_recorded: false,
            upstream_body: None,
        }
    }
}
