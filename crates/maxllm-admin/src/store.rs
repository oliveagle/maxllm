// Copyright 2024 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage abstraction for the admin subsystem.
//!
//! The [`AdminStore`] trait defines the contract that any backend (in-memory,
//! SQLite, Postgres, …) must implement.  [`InMemoryStore`] is the default
//! implementation shipped with this crate.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::models::*;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during store operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("entity not found: {0}")]
    NotFound(String),
    #[error("duplicate entity: {0}")]
    Duplicate(String),
    #[error("internal store error: {0}")]
    Internal(String),
    #[error("lock poisoned")]
    LockPoisoned,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Pluggable persistence layer for admin data.
pub trait AdminStore: Send + Sync {
    // -- Keys ---------------------------------------------------------------
    fn create_key(&self, key: VirtualKey) -> Result<(), StoreError>;
    fn get_key_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError>;
    fn get_key_by_id(&self, id: &str) -> Result<Option<VirtualKey>, StoreError>;
    fn list_keys(&self, offset: usize, limit: usize) -> Result<Vec<VirtualKey>, StoreError>;
    fn update_key(&self, key: VirtualKey) -> Result<(), StoreError>;
    fn delete_key(&self, id: &str) -> Result<bool, StoreError>;

    // -- Teams --------------------------------------------------------------
    fn create_team(&self, team: Team) -> Result<(), StoreError>;
    fn get_team(&self, id: &str) -> Result<Option<Team>, StoreError>;
    fn list_teams(&self) -> Result<Vec<Team>, StoreError>;
    fn update_team(&self, team: Team) -> Result<(), StoreError>;
    fn delete_team(&self, id: &str) -> Result<bool, StoreError>;

    // -- Spend --------------------------------------------------------------
    fn record_spend(&self, record: SpendRecord) -> Result<(), StoreError>;
    fn get_spend_logs(
        &self,
        key_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SpendRecord>, StoreError>;
    fn get_spend_summary(&self, key_id: Option<&str>) -> Result<SpendReport, StoreError>;

    // -- Request logs -------------------------------------------------------
    fn record_request_log(&self, log: RequestLog) -> Result<(), StoreError>;
    fn get_request_logs(
        &self,
        limit: usize,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<Vec<RequestLog>, StoreError>;

    // -- Resilience state ----------------------------------------------------
    /// Persist the circuit-breaker state of one provider (upsert).
    fn save_circuit_breaker_state(&self, row: CircuitBreakerStateRow) -> Result<(), StoreError>;
    /// Load every persisted circuit-breaker state (used on startup).
    fn load_circuit_breaker_states(&self) -> Result<Vec<CircuitBreakerStateRow>, StoreError>;
    /// Persist a session → provider affinity binding (upsert).
    fn save_session_binding(&self, session_id: &str, provider: &str) -> Result<(), StoreError>;
    /// Load every persisted session binding (used on startup).
    fn load_session_bindings(&self) -> Result<Vec<(String, String)>, StoreError>;
    /// Persist a learned rate-limit pattern (upsert by fingerprint).
    fn save_rate_limit_pattern(&self, row: RateLimitPatternRow) -> Result<(), StoreError>;
    /// Look up one learned rate-limit pattern by fingerprint.
    fn get_rate_limit_pattern(
        &self,
        fingerprint: &str,
    ) -> Result<Option<RateLimitPatternRow>, StoreError>;
    /// Drop a session binding (the pinned provider became unhealthy).
    fn delete_session_binding(&self, session_id: &str) -> Result<(), StoreError>;
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// A simple in-memory store backed by [`RwLock`]-protected [`HashMap`]s.
///
/// Suitable for development and single-node deployments.  For production use
/// swap in a durable backend (SQLite, Postgres) behind the same trait.
pub struct InMemoryStore {
    /// Keyed by `VirtualKey.id`.
    keys: RwLock<HashMap<String, VirtualKey>>,
    /// Secondary index: `key_hash -> key_id` for O(1) auth lookups.
    hash_index: RwLock<HashMap<String, String>>,
    /// Keyed by `Team.id`.
    teams: RwLock<HashMap<String, Team>>,
    /// Spend records stored in insertion order.
    spend_logs: RwLock<Vec<SpendRecord>>,
    /// Request logs stored in insertion order.
    request_logs: RwLock<Vec<RequestLog>>,
    /// Circuit-breaker state per provider.
    circuit_breakers: RwLock<HashMap<String, CircuitBreakerStateRow>>,
    /// Session → provider affinity bindings.
    session_bindings: RwLock<HashMap<String, String>>,
    /// Learned rate-limit message patterns keyed by shape fingerprint.
    rate_limit_patterns: RwLock<HashMap<String, RateLimitPatternRow>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            hash_index: RwLock::new(HashMap::new()),
            teams: RwLock::new(HashMap::new()),
            spend_logs: RwLock::new(Vec::new()),
            request_logs: RwLock::new(Vec::new()),
            circuit_breakers: RwLock::new(HashMap::new()),
            session_bindings: RwLock::new(HashMap::new()),
            rate_limit_patterns: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminStore for InMemoryStore {
    // -- Keys ---------------------------------------------------------------

    fn create_key(&self, key: VirtualKey) -> Result<(), StoreError> {
        let mut keys = self.keys.write().map_err(|_| StoreError::LockPoisoned)?;
        if keys.contains_key(&key.id) {
            return Err(StoreError::Duplicate(format!("key {}", key.id)));
        }
        let mut idx = self
            .hash_index
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        idx.insert(key.key_hash.clone(), key.id.clone());
        keys.insert(key.id.clone(), key);
        Ok(())
    }

    fn get_key_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError> {
        let idx = self
            .hash_index
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;
        let id = match idx.get(key_hash) {
            Some(id) => id.clone(),
            None => return Ok(None),
        };
        drop(idx);
        self.get_key_by_id(&id)
    }

    fn get_key_by_id(&self, id: &str) -> Result<Option<VirtualKey>, StoreError> {
        let keys = self.keys.read().map_err(|_| StoreError::LockPoisoned)?;
        Ok(keys.get(id).cloned())
    }

    fn list_keys(&self, offset: usize, limit: usize) -> Result<Vec<VirtualKey>, StoreError> {
        let keys = self.keys.read().map_err(|_| StoreError::LockPoisoned)?;
        let mut all: Vec<VirtualKey> = keys.values().cloned().collect();
        // Stable ordering by creation time (newest first).
        all.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    fn update_key(&self, key: VirtualKey) -> Result<(), StoreError> {
        let mut keys = self.keys.write().map_err(|_| StoreError::LockPoisoned)?;
        if !keys.contains_key(&key.id) {
            return Err(StoreError::NotFound(format!("key {}", key.id)));
        }
        keys.insert(key.id.clone(), key);
        Ok(())
    }

    fn delete_key(&self, id: &str) -> Result<bool, StoreError> {
        let mut keys = self.keys.write().map_err(|_| StoreError::LockPoisoned)?;
        let removed = keys.remove(id);
        if let Some(ref k) = removed {
            let mut idx = self
                .hash_index
                .write()
                .map_err(|_| StoreError::LockPoisoned)?;
            idx.remove(&k.key_hash);
        }
        Ok(removed.is_some())
    }

    // -- Teams --------------------------------------------------------------

    fn create_team(&self, team: Team) -> Result<(), StoreError> {
        let mut teams = self.teams.write().map_err(|_| StoreError::LockPoisoned)?;
        if teams.contains_key(&team.id) {
            return Err(StoreError::Duplicate(format!("team {}", team.id)));
        }
        teams.insert(team.id.clone(), team);
        Ok(())
    }

    fn get_team(&self, id: &str) -> Result<Option<Team>, StoreError> {
        let teams = self.teams.read().map_err(|_| StoreError::LockPoisoned)?;
        Ok(teams.get(id).cloned())
    }

    fn list_teams(&self) -> Result<Vec<Team>, StoreError> {
        let teams = self.teams.read().map_err(|_| StoreError::LockPoisoned)?;
        let mut all: Vec<Team> = teams.values().cloned().collect();
        all.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(all)
    }

    fn update_team(&self, team: Team) -> Result<(), StoreError> {
        let mut teams = self.teams.write().map_err(|_| StoreError::LockPoisoned)?;
        if !teams.contains_key(&team.id) {
            return Err(StoreError::NotFound(format!("team {}", team.id)));
        }
        teams.insert(team.id.clone(), team);
        Ok(())
    }

    fn delete_team(&self, id: &str) -> Result<bool, StoreError> {
        let mut teams = self.teams.write().map_err(|_| StoreError::LockPoisoned)?;
        Ok(teams.remove(id).is_some())
    }

    // -- Spend --------------------------------------------------------------

    fn record_spend(&self, record: SpendRecord) -> Result<(), StoreError> {
        let mut logs = self
            .spend_logs
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        logs.push(record);
        Ok(())
    }

    fn get_spend_logs(
        &self,
        key_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SpendRecord>, StoreError> {
        let logs = self
            .spend_logs
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;
        let iter = logs.iter().rev(); // newest first
        let filtered: Vec<SpendRecord> = match key_id {
            Some(kid) => iter
                .filter(|r| r.key_id == kid)
                .take(limit)
                .cloned()
                .collect(),
            None => iter.take(limit).cloned().collect(),
        };
        Ok(filtered)
    }

    fn get_spend_summary(&self, key_id: Option<&str>) -> Result<SpendReport, StoreError> {
        let logs = self
            .spend_logs
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;

        let records: Vec<&SpendRecord> = match key_id {
            Some(kid) => logs.iter().filter(|r| r.key_id == kid).collect(),
            None => logs.iter().collect(),
        };

        let mut total_spend = 0.0_f64;
        let mut total_requests = 0_u64;
        let mut total_in = 0_u64;
        let mut total_out = 0_u64;

        let mut by_model: HashMap<String, SpendByGroup> = HashMap::new();
        let mut by_provider: HashMap<String, SpendByGroup> = HashMap::new();
        let mut by_key: HashMap<String, SpendByGroup> = HashMap::new();

        for r in &records {
            total_spend += r.cost_usd;
            total_requests += 1;
            total_in += r.tokens_in;
            total_out += r.tokens_out;

            for (map, name) in [
                (&mut by_model, &r.model),
                (&mut by_provider, &r.provider),
                (&mut by_key, &r.key_id),
            ] {
                let entry = map.entry(name.clone()).or_insert_with(|| SpendByGroup {
                    name: name.clone(),
                    spend_usd: 0.0,
                    requests: 0,
                    tokens_in: 0,
                    tokens_out: 0,
                });
                entry.spend_usd += r.cost_usd;
                entry.requests += 1;
                entry.tokens_in += r.tokens_in;
                entry.tokens_out += r.tokens_out;
            }
        }

        let collect = |m: HashMap<String, SpendByGroup>| -> Vec<SpendByGroup> {
            let mut v: Vec<SpendByGroup> = m.into_values().collect();
            v.sort_by(|a, b| {
                b.spend_usd
                    .partial_cmp(&a.spend_usd)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            v
        };

        Ok(SpendReport {
            total_spend_usd: total_spend,
            total_requests,
            total_tokens_in: total_in,
            total_tokens_out: total_out,
            by_model: collect(by_model),
            by_provider: collect(by_provider),
            by_key: collect(by_key),
        })
    }

    // -- Request logs -------------------------------------------------------

    fn record_request_log(&self, log: RequestLog) -> Result<(), StoreError> {
        let mut logs = self
            .request_logs
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        logs.push(log);
        Ok(())
    }

    fn get_request_logs(
        &self,
        limit: usize,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<Vec<RequestLog>, StoreError> {
        let logs = self
            .request_logs
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;
        let iter = logs.iter().rev(); // newest first
        let filtered: Vec<RequestLog> = iter
            .filter(|r| {
                provider.is_none_or(|p| r.provider == p) && model.is_none_or(|m| r.model == m)
            })
            .take(limit)
            .cloned()
            .collect();
        Ok(filtered)
    }
fn save_circuit_breaker_state(&self, row: CircuitBreakerStateRow) -> Result<(), StoreError> {
        let mut map = self.circuit_breakers.write().map_err(|_| StoreError::LockPoisoned)?;
        map.insert(row.provider.clone(), row);
        Ok(())
    }

    fn load_circuit_breaker_states(&self) -> Result<Vec<CircuitBreakerStateRow>, StoreError> {
        let map = self.circuit_breakers.read().map_err(|_| StoreError::LockPoisoned)?;
        Ok(map.values().cloned().collect())
    }

    fn save_session_binding(&self, session_id: &str, provider: &str) -> Result<(), StoreError> {
        let mut map = self
            .session_bindings
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        map.insert(session_id.to_string(), provider.to_string());
        Ok(())
    }

    fn load_session_bindings(&self) -> Result<Vec<(String, String)>, StoreError> {
        let map = self
            .session_bindings
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;
        Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    fn delete_session_binding(&self, session_id: &str) -> Result<(), StoreError> {
        let mut map = self
            .session_bindings
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        map.remove(session_id);
        Ok(())
    }

    fn save_rate_limit_pattern(&self, row: RateLimitPatternRow) -> Result<(), StoreError> {
        let mut map = self
            .rate_limit_patterns
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        // Upsert: keep the max hit count and freshest updated_at.
        let merged = match map.get(&row.fingerprint) {
            Some(existing) => RateLimitPatternRow {
                hits: existing.hits.max(row.hits),
                updated_at: row.updated_at.max(existing.updated_at),
                ..row
            },
            None => row,
        };
        map.insert(merged.fingerprint.clone(), merged);
        Ok(())
    }

    fn get_rate_limit_pattern(
        &self,
        fingerprint: &str,
    ) -> Result<Option<RateLimitPatternRow>, StoreError> {
        let map = self
            .rate_limit_patterns
            .read()
            .map_err(|_| StoreError::LockPoisoned)?;
        Ok(map.get(fingerprint).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_key(id: &str, hash: &str) -> VirtualKey {
        VirtualKey {
            id: id.to_string(),
            key_hash: hash.to_string(),
            key_prefix: "sk-maxllm-te".to_string(),
            name: format!("test-key-{id}"),
            team_id: None,
            allowed_models: vec![],
            max_budget_usd: None,
            budget_reset_days: None,
            budget_spent_usd: 0.0,
            budget_reset_at: None,
            rpm_limit: None,
            tpm_limit: None,
            max_parallel_requests: None,
            expires_at: None,
            created_at: Utc::now(),
            last_used_at: None,
            is_active: true,
            metadata: serde_json::Map::new(),
            total_requests: 0,
            total_tokens_in: 0,
            total_tokens_out: 0,
            total_spend_usd: 0.0,
        }
    }

    #[test]
    fn create_and_get_key() {
        let store = InMemoryStore::new();
        let key = make_key("k1", "hash1");
        store.create_key(key.clone()).unwrap();

        let fetched = store.get_key_by_id("k1").unwrap().unwrap();
        assert_eq!(fetched.id, "k1");

        let by_hash = store.get_key_by_hash("hash1").unwrap().unwrap();
        assert_eq!(by_hash.id, "k1");
    }

    #[test]
    fn duplicate_key_rejected() {
        let store = InMemoryStore::new();
        let key = make_key("k1", "hash1");
        store.create_key(key.clone()).unwrap();
        assert!(store.create_key(key).is_err());
    }

    #[test]
    fn delete_key_removes_hash_index() {
        let store = InMemoryStore::new();
        store.create_key(make_key("k1", "hash1")).unwrap();
        assert!(store.delete_key("k1").unwrap());
        assert!(store.get_key_by_hash("hash1").unwrap().is_none());
        assert!(!store.delete_key("k1").unwrap());
    }

    #[test]
    fn list_keys_pagination() {
        let store = InMemoryStore::new();
        for i in 0..5 {
            store
                .create_key(make_key(&format!("k{i}"), &format!("h{i}")))
                .unwrap();
        }
        let page = store.list_keys(1, 2).unwrap();
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn spend_summary() {
        let store = InMemoryStore::new();
        store
            .record_spend(SpendRecord {
                id: "s1".into(),
                key_id: "k1".into(),
                team_id: None,
                model: "gpt-4o".into(),
                provider: "openai".into(),
                tokens_in: 1000,
                tokens_out: 500,
                cost_usd: 0.01,
                request_id: None,
                timestamp: Utc::now(),
            })
            .unwrap();
        store
            .record_spend(SpendRecord {
                id: "s2".into(),
                key_id: "k1".into(),
                team_id: None,
                model: "gpt-4o".into(),
                provider: "openai".into(),
                tokens_in: 2000,
                tokens_out: 1000,
                cost_usd: 0.02,
                request_id: None,
                timestamp: Utc::now(),
            })
            .unwrap();

        let report = store.get_spend_summary(None).unwrap();
        assert_eq!(report.total_requests, 2);
        assert!((report.total_spend_usd - 0.03).abs() < 1e-9);
        assert_eq!(report.by_model.len(), 1);
        assert_eq!(report.by_model[0].name, "gpt-4o");
    }

    #[test]
    fn team_crud() {
        let store = InMemoryStore::new();
        let team = Team {
            id: "t1".into(),
            name: "engineering".into(),
            max_budget_usd: Some(100.0),
            budget_spent_usd: 0.0,
            members: vec![],
            created_at: Utc::now(),
            metadata: serde_json::Map::new(),
        };
        store.create_team(team).unwrap();
        assert_eq!(store.list_teams().unwrap().len(), 1);
        assert!(store.delete_team("t1").unwrap());
        assert!(store.list_teams().unwrap().is_empty());
    }
}
