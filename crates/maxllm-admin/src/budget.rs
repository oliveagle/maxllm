// Copyright 2024 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! Budget tracking and enforcement.
//!
//! [`BudgetEnforcer`] ties together the store, cost calculator, and key/team
//! budgets.  It is called on every proxied request to:
//!
//! 1. **Pre-request** — verify the key (and team) have remaining budget.
//! 2. **Post-request** — record token usage and update spend counters.
//! 3. **Periodic** — reset expired rolling budgets.

use std::sync::Arc;

use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::costs::CostCalculator;
use crate::models::*;
use crate::store::{AdminStore, StoreError};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum BudgetError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("key budget exceeded: spent ${spent:.4} of ${limit:.4}")]
    KeyBudgetExceeded { spent: f64, limit: f64 },
    #[error("team budget exceeded: spent ${spent:.4} of ${limit:.4}")]
    TeamBudgetExceeded { spent: f64, limit: f64 },
    #[error("key not found: {0}")]
    KeyNotFound(String),
}

impl BudgetError {
    /// The HTTP status the gateway should answer with.
    pub fn http_status(&self) -> u16 {
        match self {
            BudgetError::KeyBudgetExceeded { .. } | BudgetError::TeamBudgetExceeded { .. } => 402,
            _ => 500,
        }
    }
}

// ---------------------------------------------------------------------------
// BudgetEnforcer
// ---------------------------------------------------------------------------

/// Coordinates budget checks and spend recording across keys and teams.
pub struct BudgetEnforcer {
    store: Arc<dyn AdminStore>,
    cost_calculator: Arc<CostCalculator>,
}

impl BudgetEnforcer {
    pub fn new(store: Arc<dyn AdminStore>, cost_calculator: Arc<CostCalculator>) -> Self {
        Self {
            store,
            cost_calculator,
        }
    }

    /// The store this enforcer reads keys and teams from.
    pub fn store(&self) -> &Arc<dyn AdminStore> {
        &self.store
    }

    /// Check whether the key (and its team, if any) still has budget remaining.
    ///
    /// Call this **before** forwarding the request to the upstream provider.
    pub fn check_budget(&self, key: &VirtualKey) -> Result<(), BudgetError> {
        // Key-level budget.
        if let Some(max) = key.max_budget_usd {
            if key.budget_spent_usd >= max {
                return Err(BudgetError::KeyBudgetExceeded {
                    spent: key.budget_spent_usd,
                    limit: max,
                });
            }
        }

        // Team-level budget.
        if let Some(ref team_id) = key.team_id {
            if let Some(team) = self.store.get_team(team_id)? {
                if let Some(max) = team.max_budget_usd {
                    if team.budget_spent_usd >= max {
                        return Err(BudgetError::TeamBudgetExceeded {
                            spent: team.budget_spent_usd,
                            limit: max,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Record usage after a successful LLM response.
    ///
    /// Updates the key's lifetime counters, the key's budget window spend,
    /// the team's budget spend (if applicable), and writes a spend log entry.
    ///
    /// Returns the cost in USD for this request.
    pub fn record_usage(
        &self,
        key_id: &str,
        model: &str,
        provider: &str,
        tokens_in: u64,
        tokens_out: u64,
        request_id: Option<&str>,
    ) -> Result<f64, BudgetError> {
        let cost = self
            .cost_calculator
            .calculate_cost(model, tokens_in, tokens_out);

        // Update key counters.
        let mut key = self
            .store
            .get_key_by_id(key_id)?
            .ok_or_else(|| BudgetError::KeyNotFound(key_id.to_string()))?;

        key.total_requests += 1;
        key.total_tokens_in += tokens_in;
        key.total_tokens_out += tokens_out;
        key.total_spend_usd += cost;
        key.budget_spent_usd += cost;
        key.last_used_at = Some(Utc::now());
        let team_id = key.team_id.clone();
        self.store.update_key(key)?;

        // Update team counters.
        if let Some(ref tid) = team_id {
            if let Some(mut team) = self.store.get_team(tid)? {
                team.budget_spent_usd += cost;
                self.store.update_team(team)?;
            }
        }

        // Write spend log.
        let record = SpendRecord {
            id: Uuid::new_v4().to_string(),
            key_id: key_id.to_string(),
            team_id,
            model: model.to_string(),
            provider: provider.to_string(),
            tokens_in,
            tokens_out,
            cost_usd: cost,
            request_id: request_id.map(String::from),
            timestamp: Utc::now(),
        };
        self.store.record_spend(record)?;

        tracing::debug!(
            key_id = %key_id,
            model = %model,
            tokens_in = tokens_in,
            tokens_out = tokens_out,
            cost_usd = cost,
            "usage recorded"
        );

        Ok(cost)
    }

    /// Reset budget counters for keys whose rolling window has expired.
    ///
    /// Call this periodically (e.g. once per minute from a background task).
    /// Returns the number of keys that were reset.
    pub fn reset_expired_budgets(&self) -> Result<usize, BudgetError> {
        let keys = self.store.list_keys(0, usize::MAX)?;
        let mut reset_count = 0;

        for mut key in keys {
            reset_count += usize::from(self.reset_key_budget_if_expired(&mut key)?);
        }

        Ok(reset_count)
    }

    /// Reset a single key's budget window if it has expired.
    ///
    /// Returns `true` when the window was reset. The request path calls this
    /// on the key it is authenticating so an expired-window key is not
    /// blocked on stale spend from the previous window — without scanning
    /// every key in the store.
    pub fn reset_key_budget_if_expired(&self, key: &mut VirtualKey) -> Result<bool, BudgetError> {
        let now = Utc::now();
        if let (Some(reset_at), Some(reset_days)) = (key.budget_reset_at, key.budget_reset_days) {
            if now >= reset_at {
                tracing::info!(
                    key_id = %key.id,
                    old_spend = key.budget_spent_usd,
                    "resetting expired budget window"
                );
                key.budget_spent_usd = 0.0;
                key.budget_reset_at = Some(now + Duration::days(i64::from(reset_days)));
                self.store.update_key(key.clone())?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys;
    use crate::models::KeyCreateRequest;
    use crate::store::InMemoryStore;

    fn setup() -> (Arc<InMemoryStore>, BudgetEnforcer) {
        let store = Arc::new(InMemoryStore::new());
        let calc = Arc::new(CostCalculator::new());
        let enforcer = BudgetEnforcer::new(store.clone(), calc);
        (store, enforcer)
    }

    fn default_request(name: &str) -> KeyCreateRequest {
        KeyCreateRequest {
            name: name.into(),
            team_id: None,
            allowed_models: vec![],
            max_budget_usd: None,
            budget_reset_days: None,
            rpm_limit: None,
            tpm_limit: None,
            max_parallel_requests: None,
            expires_in_days: None,
            metadata: None,
        }
    }

    #[test]
    fn record_usage_updates_counters() {
        let (store, enforcer) = setup();
        let resp = keys::generate_key(store.as_ref(), default_request("test")).unwrap();

        let cost = enforcer
            .record_usage(&resp.key_id, "gpt-4o", "openai", 1000, 500, Some("req-1"))
            .unwrap();
        assert!(cost > 0.0);

        let key = store.get_key_by_id(&resp.key_id).unwrap().unwrap();
        assert_eq!(key.total_requests, 1);
        assert_eq!(key.total_tokens_in, 1000);
        assert_eq!(key.total_tokens_out, 500);
        assert!((key.total_spend_usd - cost).abs() < 1e-10);
    }

    #[test]
    fn check_budget_blocks_over_limit() {
        let (store, enforcer) = setup();
        let req = KeyCreateRequest {
            max_budget_usd: Some(0.001),
            ..default_request("budget")
        };
        let resp = keys::generate_key(store.as_ref(), req).unwrap();

        // Spend enough to exceed.
        enforcer
            .record_usage(&resp.key_id, "gpt-4o", "openai", 1_000_000, 1_000_000, None)
            .unwrap();

        let key = store.get_key_by_id(&resp.key_id).unwrap().unwrap();
        match enforcer.check_budget(&key) {
            Err(BudgetError::KeyBudgetExceeded { .. }) => {}
            other => panic!("expected KeyBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn team_budget_enforcement() {
        let (store, enforcer) = setup();

        // Create team with tiny budget.
        let team = crate::teams::create_team(
            store.as_ref(),
            crate::models::TeamCreateRequest {
                name: "eng".into(),
                max_budget_usd: Some(0.001),
                metadata: None,
            },
        )
        .unwrap();

        let req = KeyCreateRequest {
            team_id: Some(team.id.clone()),
            ..default_request("k")
        };
        let resp = keys::generate_key(store.as_ref(), req).unwrap();

        // Add key to team.
        crate::teams::add_member(store.as_ref(), &team.id, &resp.key_id).unwrap();

        // Spend over team budget.
        enforcer
            .record_usage(&resp.key_id, "gpt-4o", "openai", 1_000_000, 1_000_000, None)
            .unwrap();

        let key = store.get_key_by_id(&resp.key_id).unwrap().unwrap();
        match enforcer.check_budget(&key) {
            Err(BudgetError::TeamBudgetExceeded { .. }) => {}
            other => panic!("expected TeamBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn reset_expired_budgets() {
        let (store, enforcer) = setup();
        let req = KeyCreateRequest {
            max_budget_usd: Some(100.0),
            budget_reset_days: Some(1),
            ..default_request("rolling")
        };
        let resp = keys::generate_key(store.as_ref(), req).unwrap();

        // Simulate spending.
        let mut key = store.get_key_by_id(&resp.key_id).unwrap().unwrap();
        key.budget_spent_usd = 50.0;
        // Set reset time to the past.
        key.budget_reset_at = Some(Utc::now() - Duration::hours(1));
        store.update_key(key).unwrap();

        let count = enforcer.reset_expired_budgets().unwrap();
        assert_eq!(count, 1);

        let key = store.get_key_by_id(&resp.key_id).unwrap().unwrap();
        assert!((key.budget_spent_usd - 0.0).abs() < 1e-10);
        assert!(key.budget_reset_at.unwrap() > Utc::now());
    }
}
