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
    pub fn select<'a>(
        &self,
        candidates: &'a [ProviderTarget],
        _ctx: &mut RequestCtx,
    ) -> Option<&'a ProviderTarget> {
        // Filter to healthy candidates (circuit breaker not open)
        let healthy: Vec<&ProviderTarget> = candidates
            .iter()
            .filter(|c| !c.state.circuit_breaker.is_open())
            .collect();

        if healthy.is_empty() {
            return None;
        }

        match self.strategy {
            RoutingStrategy::Fallback => {
                // Simple priority: try primary first, then fallbacks in order
                healthy.into_iter().next()
            }
            RoutingStrategy::Weighted => self.weighted_select(&healthy),
            RoutingStrategy::RoundRobin => {
                let idx = ROUND_ROBIN_COUNTER.fetch_add(1, Ordering::Relaxed) as usize;
                Some(healthy[idx % healthy.len()])
            }
            RoutingStrategy::LeastConnections => {
                // Use failure count as proxy for "busy" (real impl would track active conns)
                healthy
                    .into_iter()
                    .min_by_key(|c| c.state.circuit_breaker.failure_count())
            }
            RoutingStrategy::LatencyBased => {
                // TODO: Track per-provider latency histogram
                // For now, use the same as fallback (primary first)
                healthy.into_iter().next()
            }
            RoutingStrategy::CostBased => {
                // TODO: Look up model cost and select cheapest provider
                // For now, use the same as fallback (primary first)
                healthy.into_iter().next()
            }
        }
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
                weight,
                tags: vec![],
                default_model: None,
                native_responses: false,
                responses_path: None,
                connect_timeout: std::time::Duration::from_secs(10),
                total_connect_timeout: None,
            }),
            is_primary,
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
