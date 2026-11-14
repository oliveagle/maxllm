// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Advanced routing strategies for provider selection.

use crate::ctx::RequestCtx;
use crate::gateway::ProviderState;
use maxllm_config::RoutingStrategy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A candidate provider for routing.
pub struct ProviderTarget {
    pub name: String,
    pub state: Arc<ProviderState>,
    pub is_primary: bool,
    /// Blended USD price per 1M tokens (input + output) — the ranking key of
    /// the cost-based strategy. `None` when the provider has no price data
    /// (no explicit per-provider pricing and no price-table entry for its
    /// default model); such a provider is eligible but ranked last.
    pub cost_per_1m: Option<f64>,
}

/// Provider selection engine.
pub struct ProviderSelector {
    strategy: RoutingStrategy,
}

/// Global round-robin counter (shared across all routes using round_robin strategy).
static ROUND_ROBIN_COUNTER: AtomicU64 = AtomicU64::new(0);

impl ProviderSelector {
    pub fn new(strategy: RoutingStrategy) -> Self {
        Self { strategy }
    }

    /// Select a provider from the candidate list.
    ///
    /// The candidate walk is a *read-only* inspection: `allows_traffic` never
    /// advances a breaker's state machine nor reserves a half-open probe
    /// slot, so the rejected candidates of a route stay untouched. Only the
    /// provider that is actually selected drives the state machine — the
    /// single `is_open()` admission below. If that provider's probe slot is
    /// already occupied by a concurrent request (`half_open_max`), the next
    /// healthy candidate is tried instead of reporting the whole route
    /// unavailable.
    pub fn select<'a>(
        &self,
        candidates: &'a [ProviderTarget],
        _ctx: &mut RequestCtx,
    ) -> Option<&'a ProviderTarget> {
        // Filter to healthy candidates (pure read — does not consume probe
        // slots of the candidates it rejects).
        let healthy: Vec<&ProviderTarget> = candidates
            .iter()
            .filter(|c| c.state.circuit_breaker.allows_traffic())
            .collect();

        if healthy.is_empty() {
            return None;
        }

        let chosen = match self.strategy {
            RoutingStrategy::Fallback => {
                // Simple priority: try primary first, then fallbacks in order
                healthy.first().copied()
            }
            RoutingStrategy::Weighted => self.weighted_select(&healthy),
            RoutingStrategy::RoundRobin => {
                let idx = ROUND_ROBIN_COUNTER.fetch_add(1, Ordering::Relaxed) as usize;
                healthy.get(idx % healthy.len()).copied()
            }
            RoutingStrategy::LeastConnections => {
                // The healthy provider with the fewest requests currently
                // routed to it. The counter is per provider and process-wide
                // (see `provider_stats`), so it survives a config reload.
                healthy
                    .iter()
                    .copied()
                    .min_by_key(|c| crate::provider_stats::inflight(&c.name))
            }
            RoutingStrategy::LatencyBased => {
                // The healthy provider with the lowest observed upstream
                // latency (an EWMA over the successful attempts). A provider
                // without a measurement is ranked last — but still eligible
                // when no candidate has been measured yet, so a fresh
                // provider is never locked out.
                let mut measured: Vec<(&ProviderTarget, f64)> = healthy
                    .iter()
                    .filter_map(|c| crate::provider_stats::latency_ms(&c.name).map(|ms| (*c, ms)))
                    .collect();
                if measured.is_empty() {
                    healthy.first().copied()
                } else {
                    // Stable sort: equal latency keeps the candidate order,
                    // so the primary still wins a tie.
                    measured
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                    Some(measured[0].0)
                }
            }
            RoutingStrategy::CostBased => {
                // The cheapest healthy provider, by blended USD price per 1M
                // tokens. A provider with no price data at all ranks last —
                // but stays eligible when nothing is priced, so the route
                // still serves traffic.
                let mut priced: Vec<(&ProviderTarget, f64)> = healthy
                    .iter()
                    .filter_map(|c| c.cost_per_1m.map(|cost| (*c, cost)))
                    .collect();
                if priced.is_empty() {
                    healthy.first().copied()
                } else {
                    // Stable sort: equal price keeps the candidate order, so
                    // the primary still wins a tie.
                    priced
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                    Some(priced[0].0)
                }
            }
        };
        let chosen = chosen?;

        // Admission: this is the provider the request will actually use, so
        // (and only so) drive the breaker and reserve the probe slot.
        if !chosen.state.circuit_breaker.is_open() {
            return Some(chosen);
        }
        // The chosen candidate's probe slot is occupied by another request in
        // flight: fall back to the next healthy candidate that admits.
        healthy
            .iter()
            .copied()
            .find(|c| !c.state.circuit_breaker.is_open())
    }

    fn weighted_select<'a>(&self, healthy: &[&'a ProviderTarget]) -> Option<&'a ProviderTarget> {
        let total_weight: u32 = healthy.iter().map(|c| c.state.weight).sum();
        if total_weight == 0 {
            return healthy.first().copied();
        }

        // Simple deterministic selection based on counter (not truly random, but
        // distributes traffic proportionally over time without needing rand crate)
        let counter = ROUND_ROBIN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let target = (counter % total_weight as u64) as u32;

        let mut cumulative = 0u32;
        for candidate in healthy {
            cumulative += candidate.state.weight;
            if target < cumulative {
                return Some(candidate);
            }
        }

        healthy.last().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreaker;

    fn make_target(name: &str, weight: u32, is_primary: bool) -> ProviderTarget {
        ProviderTarget {
            name: name.to_string(),
            state: Arc::new(ProviderState {
                kind: maxllm_config::ProviderKind::OpenAI,
                translator: Box::new(maxllm_translate::openai::OpenAITranslator),
                circuit_breaker: Arc::new(CircuitBreaker::new(3, 60)),
                api_key: String::new(),
                host: "localhost".to_string(),
                port: 443,
                tls: true,
                sni: "localhost".to_string(),
                region: None,
                weight,
                tags: vec![],
                default_model: None,
                native_responses: false,
                responses_path: None,
                moonshot_tool_schema: false,
            max_concurrency: None,
                connect_timeout: std::time::Duration::from_secs(10),
                total_connect_timeout: None,
                read_timeout: None,
                first_byte_timeout: None,
                utc_offset_minutes: 0,
                self_hosted: true,
            }),
            is_primary,
            cost_per_1m: None,
        }
    }

    #[test]
    fn test_fallback_strategy() {
        let candidates = vec![
            make_target("primary", 100, true),
            make_target("fallback", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::Fallback);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "primary");
    }

    #[test]
    fn test_fallback_skips_open_circuit() {
        let candidates = vec![
            make_target("primary", 100, true),
            make_target("fallback", 100, false),
        ];
        // Trip the primary's circuit breaker
        for _ in 0..3 {
            candidates[0].state.circuit_breaker.record_failure();
        }
        let selector = ProviderSelector::new(RoutingStrategy::Fallback);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "fallback");
    }

    /// A candidate whose breaker has a 1-second fail window, already tripped
    /// `failures` times — half-open-eligible once that window elapses.
    fn make_probe_target(name: &str, failures: u32) -> ProviderTarget {
        let breaker = Arc::new(CircuitBreaker::new(1, 1));
        for _ in 0..failures {
            breaker.record_failure();
        }
        ProviderTarget {
            name: name.to_string(),
            state: Arc::new(ProviderState {
                kind: maxllm_config::ProviderKind::OpenAI,
                translator: Box::new(maxllm_translate::openai::OpenAITranslator),
                circuit_breaker: breaker,
                api_key: String::new(),
                host: "localhost".to_string(),
                port: 443,
                tls: true,
                sni: "localhost".to_string(),
                region: None,
                weight: 100,
                tags: vec![],
                default_model: None,
                native_responses: false,
                responses_path: None,
                moonshot_tool_schema: false,
            max_concurrency: None,
                connect_timeout: std::time::Duration::from_secs(10),
                total_connect_timeout: None,
                read_timeout: None,
                first_byte_timeout: None,
                utc_offset_minutes: 0,
                self_hosted: true,
            }),
            is_primary: false,
            cost_per_1m: None,
        }
    }

    /// Regression for the 2026-11-08 outage: selecting one provider out of a
    /// healthy list must reserve the half-open probe slot of *that* provider
    /// only. Every candidate the selector inspected and rejected has to stay
    /// untouched — the old filter called `is_open()` on all of them, which
    /// transitioned each one to half-open and reserved its only probe slot for
    /// a request that never came.
    #[test]
    fn test_selection_only_reserves_the_chosen_providers_probe_slot() {
        let candidates: Vec<ProviderTarget> = (0..3)
            .map(|i| make_probe_target(&format!("probe-{i}"), 1))
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let selector = ProviderSelector::new(RoutingStrategy::Fallback);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "probe-0");

        // Only the selected provider entered half-open (the raw state, not
        // the projected `effective_state`, which reports half-open for every
        // eligible breaker).
        assert_eq!(
            candidates[0].state.circuit_breaker.state(),
            crate::circuit_breaker::STATE_HALF_OPEN
        );
        for c in &candidates[1..] {
            assert_eq!(
                c.state.circuit_breaker.state(),
                crate::circuit_breaker::STATE_CLOSED,
                "an inspected-but-rejected candidate must not be transitioned"
            );
        }

        // The chosen provider's probe slot is held by the request...
        assert!(candidates[0].state.circuit_breaker.is_open());
        // ...while the rejected candidates still admit a probe, i.e. the
        // inspection never consumed their slots.
        assert!(!candidates[1].state.circuit_breaker.is_open());
        assert!(!candidates[2].state.circuit_breaker.is_open());
    }

    /// A candidate with an explicit blended price (cost-based ranking key).
    fn make_priced_target(name: &str, cost_per_1m: f64) -> ProviderTarget {
        let mut t = make_target(name, 100, false);
        t.cost_per_1m = Some(cost_per_1m);
        t
    }

    #[test]
    fn test_latency_based_picks_the_lowest_latency() {
        // Distinct provider names: the latency stats are process-wide.
        crate::provider_stats::record_latency("lat-slow", 900.0);
        crate::provider_stats::record_latency("lat-fast", 120.0);

        let candidates = vec![
            make_target("lat-slow", 100, true),
            make_target("lat-fast", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::LatencyBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "lat-fast");
    }

    #[test]
    fn test_latency_based_keeps_the_primary_on_a_tie() {
        crate::provider_stats::record_latency("lat-tie-a", 250.0);
        crate::provider_stats::record_latency("lat-tie-b", 250.0);

        let candidates = vec![
            make_target("lat-tie-a", 100, true),
            make_target("lat-tie-b", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::LatencyBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "lat-tie-a");
    }

    #[test]
    fn test_latency_based_falls_back_to_the_primary_without_measurements() {
        // No measurement for either candidate: the route must still serve,
        // using the primary (the cold-start order).
        let candidates = vec![
            make_target("lat-cold-a", 100, true),
            make_target("lat-cold-b", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::LatencyBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "lat-cold-a");
    }

    #[test]
    fn test_cost_based_picks_the_cheapest() {
        let candidates = vec![
            make_priced_target("cost-expensive", 12.0),
            make_priced_target("cost-cheap", 0.5),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::CostBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "cost-cheap");
    }

    #[test]
    fn test_cost_based_ranks_an_unpriced_provider_last() {
        let candidates = vec![
            make_target("cost-unpriced", 100, true),
            make_priced_target("cost-priced", 3.0),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::CostBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "cost-priced");
    }

    #[test]
    fn test_cost_based_falls_back_to_the_primary_without_prices() {
        let candidates = vec![
            make_target("cost-none-a", 100, true),
            make_target("cost-none-b", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::CostBased);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "cost-none-a");
    }

    #[test]
    fn test_least_connections_picks_the_least_busy() {
        crate::provider_stats::acquire("busy-provider");
        crate::provider_stats::acquire("busy-provider");

        let candidates = vec![
            make_target("busy-provider", 100, true),
            make_target("idle-provider", 100, false),
        ];
        let selector = ProviderSelector::new(RoutingStrategy::LeastConnections);
        let mut ctx = RequestCtx::new();
        let selected = selector.select(&candidates, &mut ctx).unwrap();
        assert_eq!(selected.name, "idle-provider");

        crate::provider_stats::release("busy-provider");
        crate::provider_stats::release("busy-provider");
    }

    #[test]
    fn test_round_robin() {
        let candidates = vec![make_target("a", 100, true), make_target("b", 100, false)];
        let selector = ProviderSelector::new(RoutingStrategy::RoundRobin);
        let mut ctx = RequestCtx::new();

        let mut counts = std::collections::HashMap::new();
        for _ in 0..100 {
            let selected = selector.select(&candidates, &mut ctx).unwrap();
            *counts.entry(selected.name.clone()).or_insert(0) += 1;
        }
        assert_eq!(counts["a"], 50);
        assert_eq!(counts["b"], 50);
    }
}
