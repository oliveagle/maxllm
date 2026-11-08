// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

pub static METRICS: Lazy<GatewayMetrics> = Lazy::new(GatewayMetrics::new);

pub struct GatewayMetrics {
    pub registry: Registry,
    #[allow(dead_code)]
    pub requests_total: IntCounterVec,
    pub tokens_in_total: IntCounterVec,
    pub tokens_out_total: IntCounterVec,
    pub request_duration_seconds: HistogramVec,
    pub active_requests: IntGauge,
    pub fallbacks_total: IntCounterVec,
    /// Auto-pool selections that had to leave the subscription tier because
    /// every subscription entry was unavailable (circuit open) — i.e. requests
    /// that fell back to a pay-per-token provider.
    pub auto_pool_metered_selections_total: IntCounter,
    /// Session-sticky affinity cache hit/miss counters.
    pub session_affinity_hits_total: IntCounter,
    pub session_affinity_misses_total: IntCounter,
    pub session_affinity_evictions_total: IntCounter,
    pub session_affinity_size: IntGauge,
    /// Circuit breaker state per provider: 0=closed, 1=open, 2=half-open.
    pub circuit_breaker_state: IntGaugeVec,
    /// Total transitions per provider (absolute snapshot → gauge).
    pub circuit_breaker_transitions_total: IntGaugeVec,
    /// Force-opens caused by HTTP 429 (quota exhaustion) per provider.
    pub circuit_breaker_quota_opens_total: IntCounterVec,
    /// Streaming responses whose upstream ended the body before the protocol
    /// terminal event (`response.completed` / `[DONE]`). The gateway appends
    /// an explicit error event and records the exchange as truncated instead
    /// of counting the 200 as a success.
    pub stream_truncated_total: IntCounterVec,
    /// The subset of `stream_truncated_total` where not a single response
    /// body byte had been handed to the client yet. These are the only
    /// truncations a future provider could still have rescued, so the gap
    /// between the two counters quantifies the unrecoverable class.
    pub stream_truncated_before_first_byte_total: IntCounterVec,
    /// Responses that reached a successful status but carried no token usage
    /// (the ledger would book them as free). Labelled by provider + model.
    pub usage_missing_total: IntCounterVec,
    /// Streaming responses whose completeness the gateway could determine
    /// (the protocol terminal event was seen, or its absence proved a
    /// truncation). `stream_truncated_total / streams_judged_total` is the
    /// provider's live truncation ratio.
    pub streams_judged_total: IntCounterVec,
    /// Responses the **client** abandoned mid-exchange (`ErrorSource::
    /// Downstream`), i.e. it closed the connection while the gateway was still
    /// writing the body. Kept apart from `stream_truncated_total` (a provider
    /// fault) and from ordinary failures: a client that walks away is the
    /// caller's decision, and counting it as either would make a healthy
    /// provider look broken — see docs/ops_review_20261108.md §1.1.
    pub client_aborted_total: IntCounterVec,
    /// Circuit force-opens caused by an excessive stream-truncation ratio.
    pub circuit_breaker_stream_opens_total: IntCounterVec,
    /// Failovers caused by an upstream HTTP error (the request is retried on
    /// the next provider of the route's fallback chain).
    pub http_failovers_total: IntCounterVec,
    /// Same-provider retries after a transient upstream HTTP error (5xx),
    /// per provider and status. The route's `num_retries` bounds them.
    pub provider_retries_total: IntCounterVec,
    /// Same-provider retries that rewrote the body's `model` to the
    /// provider's own `default_model` after a 400/422 "unknown model".
    ///
    /// A heterogeneous fallback chain forwards one provider's model name to
    /// every entry; without the rewrite the whole chain 400s and the route
    /// dies with its primary (docs/ops_review_20261108.md §828).
    pub model_rewrite_retries_total: IntCounterVec,
    /// Requests currently in flight per provider — the live view of the
    /// least-connections routing decision.
    pub provider_inflight_requests: IntGaugeVec,
    /// Recorded bodies (or error strings) whose content the recorder scrubbed
    /// before writing. Non-zero means clients are pasting credentials or
    /// personal data through the gateway; the bytes never reach the log store.
    pub recording_redactions_total: IntCounter,
    /// The part of the input tokens the upstream served from its prompt cache,
    /// per provider and model. Cached input is billed at a fraction of the list
    /// price, so `cached_tokens_in / tokens_in_total` is the discount factor a
    /// cost figure is missing.
    pub cached_input_tokens_total: IntCounterVec,
    /// Sessions that ended before the router was reached: no route, no
    /// provider, no tokens, no cost. They are not requests (an aborted
    /// `/health` scrape is the common case) and must not be booked as
    /// status-0 provider failures.
    pub unrouted_sessions_total: IntCounter,
}

impl GatewayMetrics {
    fn new() -> Self {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new("maxllm_requests_total", "Total number of requests"),
            &["provider", "model", "status"],
        )
        .expect("metric");
        registry
            .register(Box::new(requests_total.clone()))
            .expect("register");

        let tokens_in_total = IntCounterVec::new(
            Opts::new("maxllm_tokens_in_total", "Total input tokens"),
            &["provider", "model"],
        )
        .expect("metric");
        registry
            .register(Box::new(tokens_in_total.clone()))
            .expect("register");

        let tokens_out_total = IntCounterVec::new(
            Opts::new("maxllm_tokens_out_total", "Total output tokens"),
            &["provider", "model"],
        )
        .expect("metric");
        registry
            .register(Box::new(tokens_out_total.clone()))
            .expect("register");

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new("maxllm_request_duration_seconds", "Request latency")
                .buckets(vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .expect("register");

        let active_requests =
            IntGauge::new("maxllm_active_requests", "Currently active requests").expect("metric");
        registry
            .register(Box::new(active_requests.clone()))
            .expect("register");

        let fallbacks_total = IntCounterVec::new(
            Opts::new("maxllm_fallbacks_total", "Total fallback invocations"),
            &["from_provider", "to_provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(fallbacks_total.clone()))
            .expect("register");

        let auto_pool_metered_selections_total = IntCounter::new(
            "maxllm_auto_pool_metered_selections_total",
            "Auto-pool selections that fell back to the metered (pay-per-token) tier \
             because every subscription entry was unavailable",
        )
        .expect("metric");
        registry
            .register(Box::new(auto_pool_metered_selections_total.clone()))
            .expect("register");

        let session_affinity_hits_total = IntCounter::new(
            "maxllm_session_affinity_hits_total",
            "Session affinity cache hits",
        )
        .expect("metric");
        registry
            .register(Box::new(session_affinity_hits_total.clone()))
            .expect("register");

        let session_affinity_misses_total = IntCounter::new(
            "maxllm_session_affinity_misses_total",
            "Session affinity cache misses",
        )
        .expect("metric");
        registry
            .register(Box::new(session_affinity_misses_total.clone()))
            .expect("register");

        let session_affinity_evictions_total = IntCounter::new(
            "maxllm_session_affinity_evictions_total",
            "Session affinity cache evictions (LRU)",
        )
        .expect("metric");
        registry
            .register(Box::new(session_affinity_evictions_total.clone()))
            .expect("register");

        let session_affinity_size = IntGauge::new(
            "maxllm_session_affinity_size",
            "Current number of session bindings",
        )
        .expect("metric");
        registry
            .register(Box::new(session_affinity_size.clone()))
            .expect("register");

        let circuit_breaker_state = IntGaugeVec::new(
            Opts::new(
                "maxllm_circuit_breaker_state",
                "Circuit breaker state per provider (0=closed 1=open 2=half-open)",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(circuit_breaker_state.clone()))
            .expect("register");

        let circuit_breaker_transitions_total = IntGaugeVec::new(
            Opts::new(
                "maxllm_circuit_breaker_transitions_total",
                "Circuit breaker state transitions per provider",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(circuit_breaker_transitions_total.clone()))
            .expect("register");

        let circuit_breaker_quota_opens_total = IntCounterVec::new(
            Opts::new(
                "maxllm_circuit_breaker_quota_opens_total",
                "Circuit breaker force-opens caused by HTTP 429 per provider",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(circuit_breaker_quota_opens_total.clone()))
            .expect("register");

        let stream_truncated_total = IntCounterVec::new(
            Opts::new(
                "maxllm_stream_truncated_total",
                "Streaming responses truncated before the protocol terminal event",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(stream_truncated_total.clone()))
            .expect("register");

        let stream_truncated_before_first_byte_total = IntCounterVec::new(
            Opts::new(
                "maxllm_stream_truncated_before_first_byte_total",
                "Streaming responses truncated before the first body byte reached the client",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(stream_truncated_before_first_byte_total.clone()))
            .expect("register");

        let usage_missing_total = IntCounterVec::new(
            Opts::new(
                "maxllm_usage_missing_total",
                "Successful responses that carried no token usage",
            ),
            &["provider", "model"],
        )
        .expect("metric");
        registry
            .register(Box::new(usage_missing_total.clone()))
            .expect("register");

        let streams_judged_total = IntCounterVec::new(
            Opts::new(
                "maxllm_streams_judged_total",
                "Streaming responses whose completeness the gateway could determine",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(streams_judged_total.clone()))
            .expect("register");

        let client_aborted_total = IntCounterVec::new(
            Opts::new(
                "maxllm_client_aborted_total",
                "Responses the client abandoned before the gateway finished the body",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(client_aborted_total.clone()))
            .expect("register");

        let circuit_breaker_stream_opens_total = IntCounterVec::new(
            Opts::new(
                "maxllm_circuit_breaker_stream_opens_total",
                "Circuit force-opens caused by an excessive stream-truncation ratio",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(circuit_breaker_stream_opens_total.clone()))
            .expect("register");

        let http_failovers_total = IntCounterVec::new(
            Opts::new(
                "maxllm_http_failovers_total",
                "Failovers triggered by an upstream HTTP error",
            ),
            &["from_provider", "to_provider", "status"],
        )
        .expect("metric");
        registry
            .register(Box::new(http_failovers_total.clone()))
            .expect("register");

        let provider_retries_total = IntCounterVec::new(
            Opts::new(
                "maxllm_provider_retries_total",
                "Same-provider retries after a transient upstream HTTP error",
            ),
            &["provider", "status"],
        )
        .expect("metric");
        registry
            .register(Box::new(provider_retries_total.clone()))
            .expect("register");

        let model_rewrite_retries_total = IntCounterVec::new(
            Opts::new(
                "maxllm_model_rewrite_retries_total",
                "Same-provider retries after rewriting an unknown model name to the provider default",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(model_rewrite_retries_total.clone()))
            .expect("register");

        let provider_inflight_requests = IntGaugeVec::new(
            Opts::new(
                "maxllm_provider_inflight_requests",
                "Requests currently in flight per provider",
            ),
            &["provider"],
        )
        .expect("metric");
        registry
            .register(Box::new(provider_inflight_requests.clone()))
            .expect("register");

        let recording_redactions_total = IntCounter::new(
            "maxllm_recording_redactions_total",
            "Recorded bodies/errors scrubbed by the recorder redactor",
        )
        .expect("metric");
        registry
            .register(Box::new(recording_redactions_total.clone()))
            .expect("register");

        let cached_input_tokens_total = IntCounterVec::new(
            Opts::new(
                "maxllm_cached_input_tokens_total",
                "Input tokens the upstream served from its prompt cache",
            ),
            &["provider", "model"],
        )
        .expect("metric");
        registry
            .register(Box::new(cached_input_tokens_total.clone()))
            .expect("register");

        let unrouted_sessions_total = IntCounter::new(
            "maxllm_unrouted_sessions_total",
            "Sessions that ended before the router was reached (no route or provider)",
        )
        .expect("metric");
        registry
            .register(Box::new(unrouted_sessions_total.clone()))
            .expect("register");

        Self {
            registry,
            requests_total,
            tokens_in_total,
            tokens_out_total,
            request_duration_seconds,
            active_requests,
            fallbacks_total,
            stream_truncated_total,
            stream_truncated_before_first_byte_total,
            usage_missing_total,
            streams_judged_total,
            client_aborted_total,
            circuit_breaker_stream_opens_total,
            auto_pool_metered_selections_total,
            session_affinity_hits_total,
            session_affinity_misses_total,
            session_affinity_evictions_total,
            session_affinity_size,
            circuit_breaker_state,
            circuit_breaker_transitions_total,
            circuit_breaker_quota_opens_total,
            http_failovers_total,
            provider_retries_total,
            model_rewrite_retries_total,
            provider_inflight_requests,
            recording_redactions_total,
            cached_input_tokens_total,
            unrouted_sessions_total,
        }
    }

    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .expect("encode");
        String::from_utf8(buffer).expect("utf8")
    }
}
