// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::key_rate_limit::KeyInFlightGuard;
use maxllm_config::EndpointType;
use maxllm_plugin::guardrail::GuardrailVerdict;
use maxllm_plugin::PluginCtx;
use maxllm_translate::StreamTranslator;
use std::sync::Mutex;
use std::time::Instant;

/// The gateway's own answer to a request whose provider failed and could not
/// be replaced by another provider.
///
/// The upstream's error is the *provider's* business: a Zhipu quota message
/// ("您的限额将在 2026-11-10 22:58:54 重置"), an upstream request id and an
/// upstream's wording say nothing the client can act on, and passing them
/// through makes a healthy client look broken. The gateway answers with its
/// own 503 instead, naming the provider that failed and — when it knows it —
/// when that provider will be back.
#[derive(Debug, Clone)]
pub struct GatewayError {
    /// The status the client sees (503).
    pub status: u16,
    /// The message the client sees.
    pub message: String,
    /// The upstream status that caused it (audit trail, metrics).
    pub upstream_status: u16,
    /// The provider that failed.
    pub upstream_provider: String,
    /// How long the client should wait before retrying.
    pub retry_after_secs: Option<u64>,
}

impl GatewayError {
    /// The OpenAI-shaped error body the client receives.
    pub fn render(&self) -> Vec<u8> {
        serde_json::json!({
            "error": {
                "message": self.message,
                "type": "gateway_error",
                "code": self.status,
            }
        })
        .to_string()
        .into_bytes()
    }
}

/// A request the deployment boundary refused.
///
/// The pool that fenced the request, the scope it declared and the provider
/// that sat on the wrong side. The 503 names all three, so the answer says
/// *which* boundary was crossed instead of reporting a dead provider.
#[derive(Debug, Clone)]
pub struct ScopeRefusal {
    /// The pool that declared the scope (`auto_home`, …).
    pub pool: String,
    /// The scope, as it is spelled in the config (`self-hosted`, `external
    /// vendor`).
    pub scope: &'static str,
    /// The provider that was on the wrong side of the boundary.
    pub provider: String,
}

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
    /// The part of `tokens_in` the upstream served from its prompt cache. It is
    /// *included* in `tokens_in`, not additional: vendors bill cached input at
    /// a fraction of the list price (MiniMax M3 ~$0.05/M vs $0.30/M), so
    /// keeping it separate is what makes a cost figure meaningful.
    pub cached_tokens_in: u64,
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
    /// Whether this request incremented `active_requests`. Only a request that
    /// passed the fast paths does, and the logging hook may only decrement the
    /// gauge it actually incremented — an aborted `/health` scrape used to
    /// decrement it without ever incrementing it, so the gauge drifted
    /// negative under a monitor that hung up early.
    pub active_request_counted: bool,
    /// Plugin context visible to all plugins.
    pub plugin_ctx: PluginCtx,
    /// ID of the virtual key that authenticated this request (set by the
    /// virtual-key path in `request_filter`; empty when the request used a
    /// static key or no key was required).
    pub virtual_key_id: Option<String>,
    /// The virtual key's model allow-list (empty = every model permitted).
    /// Cached at authentication time so the per-request enforcement in
    /// `request_body_filter` does not hit the key store again.
    pub virtual_key_allowed_models: Vec<String>,
    /// In-flight slot for the virtual key's `max_parallel_requests` limit.
    /// Dropping the guard (the context is dropped when the request ends)
    /// releases the slot on every exit path.
    pub key_in_flight: Option<KeyInFlightGuard>,
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
    /// How many times this request already retried the SAME provider after a
    /// transient upstream error (HTTP 5xx). The route's `num_retries` bounds
    /// it; the counter is reset when the request moves to the next provider
    /// of the fallback chain (`switch_to_fallback`).
    pub provider_retry_count: u32,
    /// Backoff (ms) to wait before the next upstream attempt. Set when a
    /// same-provider retry is scheduled; consumed (and cleared) by
    /// `upstream_request_filter` on the retried attempt.
    pub pending_retry_backoff_ms: Option<u64>,
    /// Providers this request already tried (mirrors litellm_lb_go's
    /// `attempted` list), so the fallback walk never goes backwards through
    /// the chain — the walk is strictly `primary → fb1 → fb2 → …`.
    pub attempted_providers: Vec<String>,
    /// Session identity for session-sticky routing (from headers and/or body).
    pub session_id: Option<String>,
    /// The request body, read in `request_filter` before the provider was
    /// picked, so the session identity could be resolved from it.
    ///
    /// The body is re-injected by the body filter as the request's single
    /// chunk, so the upstream sees exactly the bytes the client sent. It is
    /// `None` again once the body filter has taken it.
    pub prefetched_body: Option<bytes::Bytes>,
    /// Whether this request *continues* a conversation the provider has
    /// already served — i.e. the session was pinned to this provider and the
    /// pin was honoured.
    ///
    /// This is what makes the prompt-cache measurement meaningful. The first
    /// turn of a conversation is cold by nature (the upstream has never seen
    /// the prefix), so a provider that caches perfectly still reports a miss
    /// on it. Measuring the cache ratio over *every* request therefore
    /// convicts a brand-new account of being a bad cache before it has had
    /// the chance to keep one — and the pool would then starve it of the
    /// very sessions that would fill that cache. Only a continuation could
    /// have hit the cache, so only a continuation is evidence.
    pub session_continuation: bool,
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
    /// Set when a 429 arrived whose headers did not pin the quota reset
    /// instant, so the decision waits for the error body (the body carries
    /// the phrasing, e.g. "It will reset at 2026-11-06 23:59:59").
    pub pending_rate_limit: Option<u16>,
    /// The 429 error body collected while `pending_rate_limit` is set.
    pub rate_limit_body_buf: Vec<u8>,
    /// The upstream response headers of the pending 429 (needed by the
    /// resolver's header layer once the body arrives).
    pub rate_limit_headers: Vec<(String, String)>,
    /// Set when a 404 arrived that may be a "the model does not exist"
    /// rejection rather than a missing endpoint. Both are 404, so the
    /// decision waits for the error body: an upstream that names the model
    /// as unknown gets one same-provider retry with its own default model
    /// substituted (the 400/422 rewrite, decided at body time).
    pub pending_model_404: bool,
    /// The 404 error body collected while `pending_model_404` is set.
    pub model_404_body_buf: Vec<u8>,
    /// The auto-rotation target selected for this request: (provider, model).
    /// Set when the route carries the `auto` sentinel; the request body's
    /// model field is rewritten to this model before it is sent upstream.
    pub auto_target: Option<(String, String)>,
    /// Set when the deployment boundary refused this request.
    ///
    /// `None` for every request that was served; set when a path-pinned
    /// provider sat on the wrong side of a scoped pool's fence, so the 503
    /// can name the boundary instead of the (healthy) provider.
    pub scope_refusal: Option<ScopeRefusal>,
    /// The auto pool this request is rotating over, by sentinel name (`auto`,
    /// `auto_home`, …). Set as soon as the sentinel is recognised — from the
    /// path, from the body (adopted before the first upstream hop), or from a
    /// route that declares `auto_model`. Once set, every later hop walks this
    /// pool instead of the route's fallback chain, so a request never leaks
    /// from one pool into another. `None` means "no pool: use the route".
    pub auto_pool: Option<String>,
    /// A model name the replayed request body must carry instead of the one
    /// the client sent (and the one the failed attempt used).
    ///
    /// Set when a provider answers 400/422 because it does not carry the
    /// requested model: the name may be another provider's model (the pool's
    /// primary, e.g. `deepseek-v4.1-flash:cloud`), which no later provider of
    /// the chain knows either, so a plain failover would collect the same 400
    /// from every entry and the route would die with the primary. The retry
    /// on the SAME provider rewrites the body to that provider's own
    /// `default_model` before translation. Consumed by the next
    /// `request_body_filter` pass.
    pub model_override: Option<String>,
    /// Providers already given the "rewrite the model to my default and try
    /// again" retry for this request. Keeps the rewrite to one attempt per
    /// provider: a second 400 after the rewrite is a genuine request fault
    /// and is returned to the client instead of looping.
    pub model_rewritten_providers: Vec<String>,
    /// One entry per failed upstream attempt, in order: `provider=status`,
    /// `provider=connect`, `provider=dns`. Appended to the error handed back
    /// to the client so a 502 names every provider that failed instead of
    /// only the last one (a trailing DNS failure used to mask the real
    /// "model not found" answer of the whole chain).
    pub attempt_trail: Vec<String>,
    /// Set as soon as this attempt's upstream produced response headers.
    ///
    /// `error_while_proxy` uses it to tell "the upstream answered and then
    /// broke" (never replayed — the request may have been executed and
    /// billed) from "the upstream accepted the connection and hung up before
    /// saying anything" (safe to replay on the next provider). Reset by
    /// `switch_to_fallback` so every attempt starts out unanswered.
    pub upstream_responded: bool,
    /// The original client body, re-fed on a retry.
    ///
    /// Pingora keeps at most 64 KiB of the downstream body for a replay and
    /// silently drops the rest, so replaying a real agent request (~100 KB of
    /// instructions and tool schemas) hands the next upstream a truncated body
    /// while the client waits for a response. The body we already read is
    /// re-injected as a single final chunk instead, and translated again for
    /// the new provider (the auto pool rewrites the model, so the bytes of the
    /// failed attempt are not reusable).
    pub replay_body: Option<Vec<u8>>,
    /// Set once `replay_body` has been re-injected: the chunks that follow are
    /// the replay buffer's truncated prefix and are dropped.
    pub replay_injected: bool,
    /// The **first-byte deadline** of this request.
    ///
    /// The upstream must produce its first byte within this budget, or the
    /// attempt is given up on and another pool member is asked. The budget
    /// can only be decided once the request body has been read — that is
    /// when it is known whether the answer arrives as a stream (whose first
    /// byte comes quickly) or as one document (whose first byte is the whole
    /// answer) — and the upstream request is written before that. The proxy
    /// therefore waits for the decision in this cell before it reads the
    /// upstream's response.
    pub first_byte_budget: pingora::proxy::FirstByteBudget,
    /// Whether the translated stream already carried its protocol terminal
    /// event (`response.completed` / `[DONE]`). A stream that ends without one
    /// was cut off upstream; the gateway appends an error event and books the
    /// exchange as truncated instead of a clean 200.
    pub stream_terminal_seen: bool,
    /// Sticky truncation flag, shared with the stream-translator wrapper so
    /// the `logging` hook can see it after the body filter has returned.
    pub stream_truncated: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The downstream walked away mid-exchange (`ErrorSource::Downstream`).
    ///
    /// This is neither a provider failure nor a gateway failure, so it must not
    /// be booked as either: `logging` uses it to keep such a request out of
    /// `usage_missing_total`, and `client_aborted_total` reports it on its own.
    /// Set once, in `error_while_proxy`.
    pub client_aborted: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Tail of the stream bytes already inspected for a terminal marker, so a
    /// marker split across two upstream chunks is still recognised.
    pub stream_scan_tail: Vec<u8>,
    /// Whether at least one non-empty response body chunk was handed back to
    /// pingora (i.e. the client already received stream data).
    pub client_body_written: bool,
    /// A synthetic health probe (`X-MaxLLM-Probe: 1`). It is served normally
    /// but excluded from metrics, request logs and circuit-breaker accounting,
    /// so a monitor cannot poison the production signals it measures.
    pub probe_request: bool,
    /// The gateway's own answer, set when the upstream failed and no failover
    /// was possible. The response status was already rewritten to 503 at
    /// header time; the body filter replaces the upstream's body with this
    /// error, so the client never sees the provider's own error text.
    pub own_error: Option<GatewayError>,
    /// Whether the gateway's own error body was already handed to the client
    /// (the upstream's remaining body chunks are dropped after that).
    pub own_error_sent: bool,
    /// The provider named by the request path itself
    /// (`/v1/responses/<provider>`), when that provider has no `[[routes]]`
    /// entry of its own.
    ///
    /// `/v1/responses` is a prefix route, so such a path lands on the pooled
    /// route by prefix match. The name used to be validated and then ignored,
    /// and the request was served by whichever provider the pool picked — a
    /// caller that named one provider got another one, after the whole
    /// fallback chain had been walked. A pinned request is served by that
    /// provider alone.
    pub path_pinned_provider: Option<String>,
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
            cached_tokens_in: 0,
            model: String::new(),
            fallback_used: false,
            original_provider: None,
            cost_usd: 0.0,
            upstream_cost_usd: None,
            upstream_send_time: None,
            skip_logging: false,
            active_request_counted: false,
            virtual_key_id: None,
            virtual_key_allowed_models: Vec::new(),
            key_in_flight: None,
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
            provider_retry_count: 0,
            pending_retry_backoff_ms: None,
            attempted_providers: Vec::new(),
            session_id: None,
            prefetched_body: None,
            session_continuation: false,
            request_uuid: uuid::Uuid::now_v7().to_string(),
            ttft_ms: None,
            request_method: String::new(),
            request_path: String::new(),
            client_body: None,
            client_request_recorded: false,
            upstream_body: None,
            pending_rate_limit: None,
            rate_limit_body_buf: Vec::new(),
            rate_limit_headers: Vec::new(),
            pending_model_404: false,
            model_404_body_buf: Vec::new(),
            auto_target: None,
            auto_pool: None,
            scope_refusal: None,
            model_override: None,
            model_rewritten_providers: Vec::new(),
            attempt_trail: Vec::new(),
            upstream_responded: false,
            replay_body: None,
            replay_injected: false,
            first_byte_budget: pingora::proxy::FirstByteBudget::new(),
            stream_terminal_seen: false,
            stream_scan_tail: Vec::new(),
            stream_truncated: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            client_aborted: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            client_body_written: false,
            probe_request: false,
            path_pinned_provider: None,
            own_error: None,
            own_error_sent: false,
        }
    }
}
