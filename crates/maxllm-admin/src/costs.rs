// Copyright 2024 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! Model cost definitions and per-request cost calculation.
//!
//! The [`CostCalculator`] holds a list of [`ModelCost`] entries and supports
//! both exact-name and glob-style pattern matching (e.g. `gpt-4o*`).

use crate::models::ModelCost;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum CostError {
    #[error("failed to parse cost config: {0}")]
    ParseError(String),
}

// ---------------------------------------------------------------------------
// Glob matching (simple, no external crate)
// ---------------------------------------------------------------------------

/// Match a string against a pattern that supports `*` (any chars) and `?`
/// (single char).  This is intentionally simple — it covers the patterns we
/// care about (e.g. `gpt-4o*`, `claude-*-sonnet`).
///
/// Public because the virtual-key allow-list uses the same wildcard syntax.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    glob_match_inner(&pat, &txt)
}

fn glob_match_inner(pat: &[char], txt: &[char]) -> bool {
    match (pat.first(), txt.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // '*' matches zero or more characters.
            glob_match_inner(&pat[1..], txt)
                || (!txt.is_empty() && glob_match_inner(pat, &txt[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &txt[1..]),
        (Some(p), Some(t)) if *p == *t => glob_match_inner(&pat[1..], &txt[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// TOML config shape
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct CostConfig {
    #[serde(default)]
    models: Vec<ModelCostEntry>,
}

#[derive(serde::Deserialize)]
struct ModelCostEntry {
    pattern: String,
    input_cost_per_1m: f64,
    output_cost_per_1m: f64,
}

// ---------------------------------------------------------------------------
// CostCalculator
// ---------------------------------------------------------------------------

/// Calculates the USD cost of an LLM request based on model and token counts.
#[derive(Debug, Clone)]
pub struct CostCalculator {
    costs: Vec<ModelCost>,
    /// Exact-match index for O(1) lookup of non-glob patterns.
    exact: ahash::AHashMap<String, usize>,
    /// Per-model prompt-cache (cache-hit) input rates, USD per 1M tokens,
    /// keyed by the same patterns as `costs`. A model without an entry is
    /// billed entirely at the list input rate (the pre-cache behaviour).
    cache_rates: ahash::AHashMap<String, f64>,
    /// Whether a missing price entry logs a warning. False for deployments
    /// that know their table is partial (or have disabled cost accounting).
    warn_on_missing: bool,
    /// Whether to compute costs at all.
    enabled: bool,
}

impl CostCalculator {
    /// Create a calculator pre-loaded with default pricing for popular models.
    pub fn new() -> Self {
        let costs = default_costs();
        let exact = Self::build_exact_index(&costs);
        Self {
            costs,
            exact,
            cache_rates: default_cache_rates(),
            warn_on_missing: true,
            enabled: true,
        }
    }

    /// Silence the per-request warning for models outside the price table.
    pub fn set_warn_on_missing(&mut self, enabled: bool) {
        self.warn_on_missing = enabled;
    }

    /// Turn cost computation off entirely (every request books 0).
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn build_exact_index(costs: &[ModelCost]) -> ahash::AHashMap<String, usize> {
        let mut map = ahash::AHashMap::new();
        for (i, mc) in costs.iter().enumerate() {
            if !mc.model_pattern.contains('*') && !mc.model_pattern.contains('?') {
                map.insert(mc.model_pattern.clone(), i);
            }
        }
        map
    }

    /// Load cost definitions from a TOML string.
    ///
    /// ```toml
    /// [[models]]
    /// pattern = "gpt-4o*"
    /// input_cost_per_1m = 2.50
    /// output_cost_per_1m = 10.00
    /// ```
    pub fn load_from_toml(toml_str: &str) -> Result<Self, CostError> {
        let config: CostConfig =
            toml::from_str(toml_str).map_err(|e| CostError::ParseError(e.to_string()))?;
        let costs: Vec<ModelCost> = config
            .models
            .into_iter()
            .map(|e| ModelCost {
                model_pattern: e.pattern,
                input_cost_per_1m: e.input_cost_per_1m,
                output_cost_per_1m: e.output_cost_per_1m,
            })
            .collect();
        let exact = Self::build_exact_index(&costs);
        Ok(Self {
            costs,
            exact,
            cache_rates: default_cache_rates(),
            warn_on_missing: true,
            enabled: true,
        })
    }

    /// Set (or replace) the prompt-cache input rate for a model pattern.
    pub fn set_cache_rate(&mut self, model_pattern: &str, rate_per_1m: f64) {
        self.cache_rates
            .insert(model_pattern.to_string(), rate_per_1m);
    }

    /// The prompt-cache input rate for a model: exact match first, then glob
    /// patterns in insertion order. `None` means "bill cached input at the
    /// list input price".
    pub fn cache_rate_for(&self, model: &str) -> Option<f64> {
        if let Some(&r) = self.cache_rates.get(model) {
            return Some(r);
        }
        self.cache_rates.iter().find_map(|(pat, &rate)| {
            if (pat.contains('*') || pat.contains('?')) && glob_match(pat, model) {
                Some(rate)
            } else {
                None
            }
        })
    }

    /// Add or replace cost entries (useful for runtime updates).
    pub fn add_cost(&mut self, cost: ModelCost) {
        let idx = self.costs.len();
        if !cost.model_pattern.contains('*') && !cost.model_pattern.contains('?') {
            self.exact.insert(cost.model_pattern.clone(), idx);
        }
        self.costs.push(cost);
    }

    /// Calculate the cost in USD for a request.
    ///
    /// Returns `0.0` if no matching model cost is found.
    pub fn calculate_cost(&self, model: &str, tokens_in: u64, tokens_out: u64) -> f64 {
        self.calculate_cost_with_cache(model, tokens_in, tokens_out, 0)
    }

    /// Calculate the cost when `cached_in` of the input tokens were served from
    /// the vendor's prompt cache. Those are billed at the model's cache rate
    /// (default: the list rate, i.e. no discount) and are never allowed to
    /// exceed the input count.
    pub fn calculate_cost_with_cache(
        &self,
        model: &str,
        tokens_in: u64,
        tokens_out: u64,
        cached_in: u64,
    ) -> f64 {
        if !self.enabled {
            return 0.0;
        }
        match self.find_cost(model) {
            Some(mc) => {
                let cached = cached_in.min(tokens_in);
                let fresh = tokens_in - cached;
                let input = (fresh as f64 / 1_000_000.0) * mc.input_cost_per_1m;
                let cached_input = (cached as f64 / 1_000_000.0)
                    * self.cache_rate_for(model).unwrap_or(mc.input_cost_per_1m);
                let output = (tokens_out as f64 / 1_000_000.0) * mc.output_cost_per_1m;
                input + cached_input + output
            }
            None => {
                if self.warn_on_missing {
                    tracing::warn!(model = %model, "no cost entry found, returning 0");
                }
                0.0
            }
        }
    }

    /// Find the first matching [`ModelCost`] for a model name.
    ///
    /// Exact matches are preferred; glob matches are tried in definition order.
    pub fn get_model_cost(&self, model: &str) -> Option<&ModelCost> {
        self.find_cost(model)
    }

    /// Return all registered cost entries.
    pub fn all_costs(&self) -> &[ModelCost] {
        &self.costs
    }

    fn find_cost(&self, model: &str) -> Option<&ModelCost> {
        // O(1) exact match via hashmap.
        if let Some(&idx) = self.exact.get(model) {
            return Some(&self.costs[idx]);
        }
        // Fallback: glob patterns only (skip exact entries already checked).
        self.costs.iter().find(|c| {
            (c.model_pattern.contains('*') || c.model_pattern.contains('?'))
                && glob_match(&c.model_pattern, model)
        })
    }
}

impl Default for CostCalculator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Default pricing
// ---------------------------------------------------------------------------

/// Prompt-cache input rates for models whose vendor publishes a cache price
/// and that this fleet actually uses. A rate here is a discount on the *list*
/// input price, so it only appears when the number is public and the channel
/// reports cache hits. MiniMax M3 is the fleet's biggest cost line and its
/// traffic is ~99% cache hits (measured 2026-11-08): its cache-read rate is
/// $0.05/1M against a $0.30/1M list price, so list-price booking overstated
/// that line roughly sixfold.
fn default_cache_rates() -> ahash::AHashMap<String, f64> {
    let mut rates = ahash::AHashMap::new();
    rates.insert("MiniMax-M3".into(), 0.05);
    rates
}

fn default_costs() -> Vec<ModelCost> {
    vec![
        // OpenAI
        ModelCost {
            model_pattern: "gpt-4o".into(),
            input_cost_per_1m: 2.50,
            output_cost_per_1m: 10.00,
        },
        ModelCost {
            model_pattern: "gpt-4o-*".into(),
            input_cost_per_1m: 2.50,
            output_cost_per_1m: 10.00,
        },
        ModelCost {
            model_pattern: "gpt-4o-mini".into(),
            input_cost_per_1m: 0.15,
            output_cost_per_1m: 0.60,
        },
        ModelCost {
            model_pattern: "gpt-4o-mini-*".into(),
            input_cost_per_1m: 0.15,
            output_cost_per_1m: 0.60,
        },
        ModelCost {
            model_pattern: "gpt-4-turbo".into(),
            input_cost_per_1m: 10.00,
            output_cost_per_1m: 30.00,
        },
        ModelCost {
            model_pattern: "gpt-4-turbo-*".into(),
            input_cost_per_1m: 10.00,
            output_cost_per_1m: 30.00,
        },
        // Anthropic
        ModelCost {
            model_pattern: "claude-sonnet-4-20250514".into(),
            input_cost_per_1m: 3.00,
            output_cost_per_1m: 15.00,
        },
        ModelCost {
            model_pattern: "claude-sonnet-4-*".into(),
            input_cost_per_1m: 3.00,
            output_cost_per_1m: 15.00,
        },
        ModelCost {
            model_pattern: "claude-haiku-4-5-20251001".into(),
            input_cost_per_1m: 0.80,
            output_cost_per_1m: 4.00,
        },
        ModelCost {
            model_pattern: "claude-haiku-4-5-*".into(),
            input_cost_per_1m: 0.80,
            output_cost_per_1m: 4.00,
        },
        ModelCost {
            model_pattern: "claude-opus-4-20250514".into(),
            input_cost_per_1m: 15.00,
            output_cost_per_1m: 75.00,
        },
        ModelCost {
            model_pattern: "claude-opus-4-*".into(),
            input_cost_per_1m: 15.00,
            output_cost_per_1m: 75.00,
        },
        // Google
        ModelCost {
            model_pattern: "gemini-2.0-flash".into(),
            input_cost_per_1m: 0.10,
            output_cost_per_1m: 0.40,
        },
        ModelCost {
            model_pattern: "gemini-2.0-flash-*".into(),
            input_cost_per_1m: 0.10,
            output_cost_per_1m: 0.40,
        },
        ModelCost {
            model_pattern: "gemini-1.5-pro".into(),
            input_cost_per_1m: 1.25,
            output_cost_per_1m: 5.00,
        },
        ModelCost {
            model_pattern: "gemini-1.5-pro-*".into(),
            input_cost_per_1m: 1.25,
            output_cost_per_1m: 5.00,
        },
        // Cohere
        ModelCost {
            model_pattern: "command-r-plus".into(),
            input_cost_per_1m: 2.50,
            output_cost_per_1m: 10.00,
        },
        ModelCost {
            model_pattern: "command-r-plus-*".into(),
            input_cost_per_1m: 2.50,
            output_cost_per_1m: 10.00,
        },
        // Mistral
        ModelCost {
            model_pattern: "mistral-large".into(),
            input_cost_per_1m: 2.00,
            output_cost_per_1m: 6.00,
        },
        ModelCost {
            model_pattern: "mistral-large-*".into(),
            input_cost_per_1m: 2.00,
            output_cost_per_1m: 6.00,
        },
        // ── Production channels ──────────────────────────────────────────
        //
        // List prices (USD per 1M tokens), not contract prices: the gateway
        // ships them so a channel is never silently booked at 0. A deployment
        // with a negotiated rate overrides the row in `[model_costs.<model>]`
        // — the config entry replaces the built-in one because exact matches
        // win over the entries above (and `add_cost` appends later entries,
        // which `find_cost` checks first).
        //
        // DeepSeek V4 Flash / Flash (cache-miss list price, blended).
        ModelCost {
            model_pattern: "deepseek-v4-flash".into(),
            input_cost_per_1m: 0.10,
            output_cost_per_1m: 0.28,
        },
        ModelCost {
            model_pattern: "deepseek-flash".into(),
            input_cost_per_1m: 0.10,
            output_cost_per_1m: 0.28,
        },
        // MiniMax M3.
        ModelCost {
            model_pattern: "MiniMax-M3".into(),
            input_cost_per_1m: 0.30,
            output_cost_per_1m: 1.20,
        },
        // Xiaomi MiMo V2.6 Flash.
        ModelCost {
            model_pattern: "mimo-v2.6-flash".into(),
            input_cost_per_1m: 0.10,
            output_cost_per_1m: 0.30,
        },
        // Zhipu GLM 5.3 Flash.
        ModelCost {
            model_pattern: "glm-5.3-flash".into(),
            input_cost_per_1m: 0.02,
            output_cost_per_1m: 0.08,
        },
        // ── commandcode.ai (Provider plan) ───────────────────────────────
        //
        // Pay-as-you-go at the underlying API rate (no markup), so these are
        // real money and must be booked. The vendor's DeepSeek V4.1 Flash
        // rate is time-banded: the row below is the off-peak rate (17h/day);
        // the peak rate is $0.30 / $1.20 (01-04 and 06-10 UTC, Mon-Fri).
        // A deployment that wants the peak rate booked overrides the row in
        // `[model_costs."deepseek/deepseek-v4.1-flash"]`.
        //
        // The model ids are namespaced by the vendor (`deepseek/…`,
        // `google/…`, `xai/…`), so they never collide with the un-namespaced
        // ids of the direct channels above.
        ModelCost {
            model_pattern: "deepseek/deepseek-v4.1-flash".into(),
            input_cost_per_1m: 0.15,
            output_cost_per_1m: 0.60,
        },
        ModelCost {
            model_pattern: "deepseek/deepseek-v4.1-flash-fast".into(),
            input_cost_per_1m: 0.16,
            output_cost_per_1m: 0.58,
        },
        ModelCost {
            model_pattern: "deepseek/deepseek-v4.1-flash-ultrafast".into(),
            input_cost_per_1m: 2.40,
            output_cost_per_1m: 9.60,
        },
        // ── Zero-marginal-cost channels ──────────────────────────────────
        //
        // Prepaid coding plans and self-hosted endpoints are not billed per
        // token. An explicit 0/0 row keeps them out of the "no cost entry"
        // warning and lets the upstream-reported cost (when present) win.
        ModelCost {
            model_pattern: "kimi-for-coding".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "deepseek-v4.1-flash:cloud".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        // DeepSeek V4.1 Flash behind a prepaid plan — the OpenDesign AMR
        // link (the design workspace's credit balance) and the opencode.ai
        // zen plan. Neither reports a per-request USD cost, and both are
        // covered by a prepaid allowance, so the marginal token price is
        // zero. A deployment that wants the credit draw booked overrides
        // the row in `[model_costs."deepseek-v4.1-flash"]`.
        ModelCost {
            model_pattern: "deepseek-v4.1-flash".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "qwen3.8-27b*".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "qwen3-embedding-*".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "qwen3-reranker-*".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "minicpm*".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
        ModelCost {
            model_pattern: "humanizer-*".into(),
            input_cost_per_1m: 0.0,
            output_cost_per_1m: 0.0,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("gpt-4o*", "gpt-4o"));
        assert!(glob_match("gpt-4o*", "gpt-4o-2024-08-06"));
        assert!(glob_match("gpt-4o-mini*", "gpt-4o-mini-2024-07-18"));
        assert!(!glob_match("gpt-4o", "gpt-4o-mini"));
        assert!(glob_match("claude-*-sonnet", "claude-3.5-sonnet"));
        assert!(glob_match("?at", "cat"));
        assert!(!glob_match("?at", "chat"));
    }

    #[test]
    fn default_calculator_known_models() {
        let calc = CostCalculator::new();

        // Exact model name.
        let cost = calc.calculate_cost("gpt-4o", 1_000_000, 1_000_000);
        assert!((cost - 12.50).abs() < 0.01); // 2.50 + 10.00

        // Versioned model name matched by glob.
        let cost = calc.calculate_cost("gpt-4o-2024-08-06", 1_000_000, 0);
        assert!((cost - 2.50).abs() < 0.01);

        // Claude
        let cost = calc.calculate_cost("claude-opus-4-20250514", 1_000_000, 1_000_000);
        assert!((cost - 90.0).abs() < 0.01); // 15.00 + 75.00
    }

    #[test]
    fn production_channels_have_a_price_or_an_explicit_zero() {
        let calc = CostCalculator::new();
        for model in [
            "deepseek-v4-flash",
            "deepseek-flash",
            "MiniMax-M3",
            "mimo-v2.6-flash",
            "glm-5.3-flash",
            "kimi-for-coding",
            "deepseek-v4.1-flash:cloud",
            "deepseek-v4.1-flash",
            "qwen3.8-27b-ct",
            "minicpmv47-awq",
            "humanizer-12b",
        ] {
            assert!(
                calc.get_model_cost(model).is_some(),
                "{model} must have an explicit cost row"
            );
        }
        // A metered channel is billed, not silently zero.
        let cost = calc.calculate_cost("deepseek-v4-flash", 1_000_000, 1_000_000);
        assert!((cost - 0.38).abs() < 1e-9, "got {cost}");
        // A subscription/self-hosted channel is explicitly free.
        assert_eq!(
            calc.calculate_cost("kimi-for-coding", 1_000_000, 1_000_000),
            0.0
        );
        // The OpenDesign AMR link draws down a prepaid credit balance.
        assert_eq!(
            calc.calculate_cost("deepseek-v4.1-flash", 1_000_000, 1_000_000),
            0.0
        );
    }

    #[test]
    fn unknown_model_returns_zero() {
        let calc = CostCalculator::new();
        assert_eq!(calc.calculate_cost("unknown-model", 1000, 1000), 0.0);
    }

    #[test]
    fn small_token_counts() {
        let calc = CostCalculator::new();
        // 100 input + 50 output tokens of gpt-4o
        let cost = calc.calculate_cost("gpt-4o", 100, 50);
        let expected = (100.0 / 1_000_000.0) * 2.50 + (50.0 / 1_000_000.0) * 10.00;
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn load_from_toml_string() {
        let toml = r#"
[[models]]
pattern = "my-model"
input_cost_per_1m = 1.0
output_cost_per_1m = 3.0

[[models]]
pattern = "my-model-v*"
input_cost_per_1m = 1.5
output_cost_per_1m = 4.0
"#;
        let calc = CostCalculator::load_from_toml(toml).unwrap();
        assert_eq!(calc.all_costs().len(), 2);

        let cost = calc.calculate_cost("my-model", 1_000_000, 1_000_000);
        assert!((cost - 4.0).abs() < 0.01);

        let cost = calc.calculate_cost("my-model-v2", 1_000_000, 0);
        assert!((cost - 1.5).abs() < 0.01);
    }

    /// Cached input is billed at the cache rate, the rest at the list rate.
    #[test]
    fn cached_input_is_billed_at_the_cache_rate() {
        let calc = CostCalculator::new();
        // MiniMax-M3: $0.30/M in, $1.20/M out, $0.05/M cached in.
        let cost = calc.calculate_cost_with_cache("MiniMax-M3", 1_000_000, 1_000_000, 900_000);
        let expected = 100_000.0 / 1_000_000.0 * 0.30 // fresh input, list price
            + 900_000.0 / 1_000_000.0 * 0.05 // cached input, cache rate
            + 1.20; // output
        assert!(
            (cost - expected).abs() < 1e-9,
            "got {cost}, want {expected}"
        );
        // Without the cached count the same request books list price for all
        // of it — the behaviour every model without a cache rate keeps.
        let plain = calc.calculate_cost("MiniMax-M3", 1_000_000, 1_000_000);
        assert!((plain - (0.30 + 1.20)).abs() < 1e-9);
    }

    /// A provider that reports more cached tokens than input tokens (or a
    /// parser underflow) must not produce a negative charge.
    #[test]
    fn cached_tokens_are_clamped_to_the_input_count() {
        let calc = CostCalculator::new();
        let cost = calc.calculate_cost_with_cache("MiniMax-M3", 1_000, 0, 5_000);
        let expected = 1_000.0 / 1_000_000.0 * 0.05;
        assert!(
            (cost - expected).abs() < 1e-12,
            "got {cost}, want {expected}"
        );
        assert!(cost >= 0.0);
    }

    /// Models with no published cache price keep billing cached input at list;
    /// a per-deployment override can add one (glob patterns included).
    #[test]
    fn cache_rate_override_and_glob() {
        let mut calc = CostCalculator::new();
        assert_eq!(calc.cache_rate_for("deepseek-v4-flash"), None);
        let list = calc.calculate_cost_with_cache("deepseek-v4-flash", 1_000_000, 0, 800_000);
        assert!((list - 0.10).abs() < 1e-9, "no rate => list price: {list}");

        calc.set_cache_rate("deepseek-*", 0.003);
        assert_eq!(calc.cache_rate_for("deepseek-v4-flash"), Some(0.003));
        let discounted = calc.calculate_cost_with_cache("deepseek-v4-flash", 1_000_000, 0, 800_000);
        let expected = 200_000.0 / 1_000_000.0 * 0.10 + 800_000.0 / 1_000_000.0 * 0.003;
        assert!((discounted - expected).abs() < 1e-9, "got {discounted}");
    }

    #[test]
    fn exact_match_preferred_over_glob() {
        // gpt-4o-mini should match the exact "gpt-4o-mini" entry (0.15/0.60),
        // not the "gpt-4o-*" glob entry (2.50/10.00).
        let calc = CostCalculator::new();
        let cost = calc.calculate_cost("gpt-4o-mini", 1_000_000, 1_000_000);
        assert!((cost - 0.75).abs() < 0.01); // 0.15 + 0.60
    }
}
