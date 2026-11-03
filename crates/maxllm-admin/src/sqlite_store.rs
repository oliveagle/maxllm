// Copyright 2025 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite-backed implementation of [`AdminStore`].

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::models::*;
use crate::store::{AdminStore, StoreError};

/// A durable [`AdminStore`] backed by SQLite.
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (or create) a SQLite database at the given path.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)
            .map_err(|e| StoreError::Internal(format!("failed to open SQLite: {e}")))?;

        Self::set_pragmas(&conn)?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_tables()?;
        Ok(store)
    }

    /// Create an in-memory SQLite store (useful for tests).
    pub fn in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| StoreError::Internal(format!("failed to open in-memory SQLite: {e}")))?;

        Self::set_pragmas(&conn)?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_tables()?;
        Ok(store)
    }

    /// Set connection-level pragmas.
    fn set_pragmas(conn: &Connection) -> Result<(), StoreError> {
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )
        .map_err(|e| StoreError::Internal(format!("failed to set pragmas: {e}")))?;
        Ok(())
    }

    fn init_tables(&self) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS virtual_keys (
                id TEXT PRIMARY KEY,
                key_hash TEXT NOT NULL UNIQUE,
                key_prefix TEXT NOT NULL,
                name TEXT NOT NULL,
                team_id TEXT,
                allowed_models TEXT NOT NULL DEFAULT '[]',
                max_budget_usd REAL,
                budget_reset_days INTEGER,
                budget_spent_usd REAL NOT NULL DEFAULT 0,
                budget_reset_at TEXT,
                rpm_limit INTEGER,
                tpm_limit INTEGER,
                max_parallel_requests INTEGER,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                metadata TEXT NOT NULL DEFAULT '{}',
                total_requests INTEGER NOT NULL DEFAULT 0,
                total_tokens_in INTEGER NOT NULL DEFAULT 0,
                total_tokens_out INTEGER NOT NULL DEFAULT 0,
                total_spend_usd REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS teams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                max_budget_usd REAL,
                budget_spent_usd REAL NOT NULL DEFAULT 0,
                members TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS spend_logs (
                id TEXT PRIMARY KEY,
                key_id TEXT NOT NULL,
                team_id TEXT,
                model TEXT NOT NULL,
                provider TEXT NOT NULL,
                tokens_in INTEGER NOT NULL,
                tokens_out INTEGER NOT NULL,
                cost_usd REAL NOT NULL,
                request_id TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (key_id) REFERENCES virtual_keys(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS request_logs (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                tokens_in INTEGER NOT NULL DEFAULT 0,
                tokens_out INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0,
                latency_ms INTEGER NOT NULL DEFAULT 0,
                status INTEGER NOT NULL DEFAULT 0,
                request_id TEXT,
                client_ip TEXT,
                route_path TEXT NOT NULL DEFAULT '',
                endpoint_type TEXT NOT NULL DEFAULT '',
                fallback_used INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_keys_hash ON virtual_keys(key_hash);
            CREATE INDEX IF NOT EXISTS idx_spend_key ON spend_logs(key_id);
            CREATE INDEX IF NOT EXISTS idx_spend_ts ON spend_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_reqlog_ts ON request_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_reqlog_provider ON request_logs(provider);
            CREATE TABLE IF NOT EXISTS circuit_breaker_state (
                provider TEXT PRIMARY KEY,
                state INTEGER NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0,
                last_failure_at INTEGER NOT NULL DEFAULT 0,
                force_open_until INTEGER NOT NULL DEFAULT 0,
                successes_half_open INTEGER NOT NULL DEFAULT 0,
                transitions INTEGER NOT NULL DEFAULT 0,
                quota_opens INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS session_bindings (
                session_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            ",
        )
        .map_err(|e| StoreError::Internal(format!("failed to create tables: {e}")))?;
        Ok(())
    }
}

// Helper to serialize/deserialize JSON fields
fn to_json_string<T: serde::Serialize>(val: &T) -> String {
    serde_json::to_string(val).unwrap_or_else(|_| "[]".to_string())
}

fn key_from_row(row: &rusqlite::Row) -> rusqlite::Result<VirtualKey> {
    let allowed_models_str: String = row.get("allowed_models")?;
    let metadata_str: String = row.get("metadata")?;
    let budget_reset_at_str: Option<String> = row.get("budget_reset_at")?;
    let expires_at_str: Option<String> = row.get("expires_at")?;
    let created_at_str: String = row.get("created_at")?;
    let last_used_at_str: Option<String> = row.get("last_used_at")?;

    Ok(VirtualKey {
        id: row.get("id")?,
        key_hash: row.get("key_hash")?,
        key_prefix: row.get("key_prefix")?,
        name: row.get("name")?,
        team_id: row.get("team_id")?,
        allowed_models: serde_json::from_str(&allowed_models_str).unwrap_or_default(),
        max_budget_usd: row.get("max_budget_usd")?,
        budget_reset_days: row.get("budget_reset_days")?,
        budget_spent_usd: row.get("budget_spent_usd")?,
        budget_reset_at: budget_reset_at_str
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        rpm_limit: row.get("rpm_limit")?,
        tpm_limit: row.get("tpm_limit")?,
        max_parallel_requests: row.get("max_parallel_requests")?,
        expires_at: expires_at_str
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        created_at: chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        last_used_at: last_used_at_str
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        is_active: row.get::<_, i32>("is_active")? != 0,
        metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
        total_requests: row.get::<_, i64>("total_requests")? as u64,
        total_tokens_in: row.get::<_, i64>("total_tokens_in")? as u64,
        total_tokens_out: row.get::<_, i64>("total_tokens_out")? as u64,
        total_spend_usd: row.get("total_spend_usd")?,
    })
}

fn team_from_row(row: &rusqlite::Row) -> rusqlite::Result<Team> {
    let members_str: String = row.get("members")?;
    let metadata_str: String = row.get("metadata")?;
    let created_at_str: String = row.get("created_at")?;

    Ok(Team {
        id: row.get("id")?,
        name: row.get("name")?,
        max_budget_usd: row.get("max_budget_usd")?,
        budget_spent_usd: row.get("budget_spent_usd")?,
        members: serde_json::from_str(&members_str).unwrap_or_default(),
        created_at: chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
    })
}

impl AdminStore for SqliteStore {
    fn create_key(&self, key: VirtualKey) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO virtual_keys (
                id, key_hash, key_prefix, name, team_id, allowed_models,
                max_budget_usd, budget_reset_days, budget_spent_usd, budget_reset_at,
                rpm_limit, tpm_limit, max_parallel_requests, expires_at,
                created_at, last_used_at, is_active, metadata,
                total_requests, total_tokens_in, total_tokens_out, total_spend_usd
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                key.id,
                key.key_hash,
                key.key_prefix,
                key.name,
                key.team_id,
                to_json_string(&key.allowed_models),
                key.max_budget_usd,
                key.budget_reset_days,
                key.budget_spent_usd,
                key.budget_reset_at.map(|dt| dt.to_rfc3339()),
                key.rpm_limit,
                key.tpm_limit,
                key.max_parallel_requests,
                key.expires_at.map(|dt| dt.to_rfc3339()),
                key.created_at.to_rfc3339(),
                key.last_used_at.map(|dt| dt.to_rfc3339()),
                key.is_active as i32,
                to_json_string(&key.metadata),
                key.total_requests as i64,
                key.total_tokens_in as i64,
                key.total_tokens_out as i64,
                key.total_spend_usd,
            ],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    return StoreError::Duplicate(format!("key {}", key.id));
                }
            }
            StoreError::Internal(e.to_string())
        })?;
        Ok(())
    }

    fn get_key_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT * FROM virtual_keys WHERE key_hash = ?1")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let result = stmt
            .query_row(params![key_hash], key_from_row)
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(result)
    }

    fn get_key_by_id(&self, id: &str) -> Result<Option<VirtualKey>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT * FROM virtual_keys WHERE id = ?1")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let result = stmt
            .query_row(params![id], key_from_row)
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(result)
    }

    fn list_keys(&self, offset: usize, limit: usize) -> Result<Vec<VirtualKey>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT * FROM virtual_keys ORDER BY created_at DESC LIMIT ?1 OFFSET ?2")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let keys = stmt
            .query_map(params![limit as i64, offset as i64], key_from_row)
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(keys)
    }

    fn update_key(&self, key: VirtualKey) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let rows = conn
            .execute(
                "UPDATE virtual_keys SET
                    key_hash = ?2, key_prefix = ?3, name = ?4, team_id = ?5,
                    allowed_models = ?6, max_budget_usd = ?7, budget_reset_days = ?8,
                    budget_spent_usd = ?9, budget_reset_at = ?10, rpm_limit = ?11,
                    tpm_limit = ?12, max_parallel_requests = ?13, expires_at = ?14,
                    last_used_at = ?15, is_active = ?16, metadata = ?17,
                    total_requests = ?18, total_tokens_in = ?19, total_tokens_out = ?20,
                    total_spend_usd = ?21
                WHERE id = ?1",
                params![
                    key.id,
                    key.key_hash,
                    key.key_prefix,
                    key.name,
                    key.team_id,
                    to_json_string(&key.allowed_models),
                    key.max_budget_usd,
                    key.budget_reset_days,
                    key.budget_spent_usd,
                    key.budget_reset_at.map(|dt| dt.to_rfc3339()),
                    key.rpm_limit,
                    key.tpm_limit,
                    key.max_parallel_requests,
                    key.expires_at.map(|dt| dt.to_rfc3339()),
                    key.last_used_at.map(|dt| dt.to_rfc3339()),
                    key.is_active as i32,
                    to_json_string(&key.metadata),
                    key.total_requests as i64,
                    key.total_tokens_in as i64,
                    key.total_tokens_out as i64,
                    key.total_spend_usd,
                ],
            )
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        if rows == 0 {
            return Err(StoreError::NotFound(format!("key {}", key.id)));
        }
        Ok(())
    }

    fn delete_key(&self, id: &str) -> Result<bool, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let rows = conn
            .execute("DELETE FROM virtual_keys WHERE id = ?1", params![id])
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows > 0)
    }

    fn create_team(&self, team: Team) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO teams (id, name, max_budget_usd, budget_spent_usd, members, created_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                team.id,
                team.name,
                team.max_budget_usd,
                team.budget_spent_usd,
                to_json_string(&team.members),
                team.created_at.to_rfc3339(),
                to_json_string(&team.metadata),
            ],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    return StoreError::Duplicate(format!("team {}", team.id));
                }
            }
            StoreError::Internal(e.to_string())
        })?;
        Ok(())
    }

    fn get_team(&self, id: &str) -> Result<Option<Team>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT * FROM teams WHERE id = ?1")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let result = stmt
            .query_row(params![id], team_from_row)
            .optional()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(result)
    }

    fn list_teams(&self) -> Result<Vec<Team>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT * FROM teams ORDER BY created_at DESC")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let teams = stmt
            .query_map([], team_from_row)
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(teams)
    }

    fn update_team(&self, team: Team) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let rows = conn
            .execute(
                "UPDATE teams SET name = ?2, max_budget_usd = ?3, budget_spent_usd = ?4,
                 members = ?5, metadata = ?6 WHERE id = ?1",
                params![
                    team.id,
                    team.name,
                    team.max_budget_usd,
                    team.budget_spent_usd,
                    to_json_string(&team.members),
                    to_json_string(&team.metadata),
                ],
            )
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        if rows == 0 {
            return Err(StoreError::NotFound(format!("team {}", team.id)));
        }
        Ok(())
    }

    fn delete_team(&self, id: &str) -> Result<bool, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let rows = conn
            .execute("DELETE FROM teams WHERE id = ?1", params![id])
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows > 0)
    }

    fn record_spend(&self, record: SpendRecord) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO spend_logs (id, key_id, team_id, model, provider, tokens_in, tokens_out, cost_usd, request_id, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.id,
                record.key_id,
                record.team_id,
                record.model,
                record.provider,
                record.tokens_in as i64,
                record.tokens_out as i64,
                record.cost_usd,
                record.request_id,
                record.timestamp.to_rfc3339(),
            ],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    fn get_spend_logs(
        &self,
        key_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SpendRecord>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;

        if let Some(kid) = key_id {
            let mut stmt = conn
                .prepare(
                    "SELECT * FROM spend_logs WHERE key_id = ?1 ORDER BY timestamp DESC LIMIT ?2",
                )
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let rows = stmt
                .query_map(params![kid, limit as i64], spend_from_row)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
            }
            Ok(records)
        } else {
            let mut stmt = conn
                .prepare("SELECT * FROM spend_logs ORDER BY timestamp DESC LIMIT ?1")
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let rows = stmt
                .query_map(params![limit as i64], spend_from_row)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
            }
            Ok(records)
        }
    }

    fn get_spend_summary(&self, key_id: Option<&str>) -> Result<SpendReport, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;

        // Build WHERE clause for optional key_id filter
        let where_clause = if key_id.is_some() {
            "WHERE key_id = ?1"
        } else {
            ""
        };

        // Totals
        let totals_sql = format!(
            "SELECT COALESCE(SUM(cost_usd), 0) as total_spend,
                    COUNT(*) as total_requests,
                    COALESCE(SUM(tokens_in), 0) as total_in,
                    COALESCE(SUM(tokens_out), 0) as total_out
             FROM spend_logs {where_clause}"
        );
        let mut stmt = conn
            .prepare(&totals_sql)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let bind_params: Vec<Box<dyn rusqlite::types::ToSql>> = if let Some(kid) = key_id {
            vec![Box::new(kid.to_string())]
        } else {
            vec![]
        };
        let bind_refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_params.iter().map(|p| p.as_ref()).collect();
        let (total_spend, total_requests, total_in, total_out) = stmt
            .query_row(bind_refs.as_slice(), |row| {
                Ok((
                    row.get::<_, f64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        // Helper to query GROUP BY aggregations
        let query_group = |group_col: &str| -> Result<Vec<SpendByGroup>, StoreError> {
            let sql = format!(
                "SELECT {group_col} as name,
                        COALESCE(SUM(cost_usd), 0) as spend_usd,
                        COUNT(*) as requests,
                        COALESCE(SUM(tokens_in), 0) as tokens_in,
                        COALESCE(SUM(tokens_out), 0) as tokens_out
                 FROM spend_logs {where_clause}
                 GROUP BY {group_col}
                 ORDER BY spend_usd DESC"
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let rows = stmt
                .query_map(bind_refs.as_slice(), |row| {
                    Ok(SpendByGroup {
                        name: row.get(0)?,
                        spend_usd: row.get(1)?,
                        requests: row.get(2)?,
                        tokens_in: row.get(3)?,
                        tokens_out: row.get(4)?,
                    })
                })
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| StoreError::Internal(e.to_string()))
        };

        let by_model = query_group("model")?;
        let by_provider = query_group("provider")?;
        let by_key = query_group("key_id")?;

        Ok(SpendReport {
            total_spend_usd: total_spend,
            total_requests,
            total_tokens_in: total_in,
            total_tokens_out: total_out,
            by_model,
            by_provider,
            by_key,
        })
    }

    fn record_request_log(&self, log: RequestLog) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO request_logs (
                id, timestamp, provider, model, tokens_in, tokens_out,
                cost_usd, latency_ms, status, request_id, client_ip,
                route_path, endpoint_type, fallback_used, error
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                log.id,
                log.timestamp.to_rfc3339(),
                log.provider,
                log.model,
                log.tokens_in as i64,
                log.tokens_out as i64,
                log.cost_usd,
                log.latency_ms as i64,
                log.status as i32,
                log.request_id,
                log.client_ip,
                log.route_path,
                log.endpoint_type,
                log.fallback_used as i32,
                log.error,
            ],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    fn get_request_logs(
        &self,
        limit: usize,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<Vec<RequestLog>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            match (provider, model) {
                (Some(p), Some(m)) => (
                    "SELECT * FROM request_logs WHERE provider = ?1 AND model = ?2 ORDER BY timestamp DESC LIMIT ?3".to_string(),
                    vec![Box::new(p.to_string()), Box::new(m.to_string()), Box::new(limit as i64)],
                ),
                (Some(p), None) => (
                    "SELECT * FROM request_logs WHERE provider = ?1 ORDER BY timestamp DESC LIMIT ?2".to_string(),
                    vec![Box::new(p.to_string()), Box::new(limit as i64)],
                ),
                (None, Some(m)) => (
                    "SELECT * FROM request_logs WHERE model = ?1 ORDER BY timestamp DESC LIMIT ?2".to_string(),
                    vec![Box::new(m.to_string()), Box::new(limit as i64)],
                ),
                (None, None) => (
                    "SELECT * FROM request_logs ORDER BY timestamp DESC LIMIT ?1".to_string(),
                    vec![Box::new(limit as i64)],
                ),
            };

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), request_log_from_row)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(|e| StoreError::Internal(e.to_string()))?);
        }
        Ok(records)
    }

    fn save_circuit_breaker_state(&self, row: CircuitBreakerStateRow) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO circuit_breaker_state (
                provider, state, failures, last_failure_at, force_open_until,
                successes_half_open, transitions, quota_opens, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(provider) DO UPDATE SET
                state = excluded.state,
                failures = excluded.failures,
                last_failure_at = excluded.last_failure_at,
                force_open_until = excluded.force_open_until,
                successes_half_open = excluded.successes_half_open,
                transitions = excluded.transitions,
                quota_opens = excluded.quota_opens,
                updated_at = excluded.updated_at",
            params![
                row.provider,
                row.state as i32,
                row.failures as i32,
                row.last_failure_at as i64,
                row.force_open_until as i64,
                row.successes_half_open as i32,
                row.transitions as i64,
                row.quota_opens as i64,
                row.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    fn load_circuit_breaker_states(&self) -> Result<Vec<CircuitBreakerStateRow>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare(
                "SELECT provider, state, failures, last_failure_at, force_open_until,
                        successes_half_open, transitions, quota_opens, updated_at
                 FROM circuit_breaker_state",
            )
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let provider: String = row.get("provider")?;
                let state: i32 = row.get("state")?;
                let failures: i32 = row.get("failures")?;
                let last_failure_at: i64 = row.get("last_failure_at")?;
                let force_open_until: i64 = row.get("force_open_until")?;
                let successes_half_open: i32 = row.get("successes_half_open")?;
                let transitions: i64 = row.get("transitions")?;
                let quota_opens: i64 = row.get("quota_opens")?;
                let updated_at: String = row.get("updated_at")?;
                Ok(CircuitBreakerStateRow {
                    provider,
                    state: state.clamp(0, 2) as u8,
                    failures: failures.max(0) as u32,
                    last_failure_at: last_failure_at.max(0) as u64,
                    force_open_until: force_open_until.max(0) as u64,
                    successes_half_open: successes_half_open.max(0) as u32,
                    transitions: transitions.max(0) as u64,
                    quota_opens: quota_opens.max(0) as u64,
                    updated_at: chrono::DateTime::parse_from_rfc3339(&updated_at)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now()),
                })
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows)
    }

    fn save_session_binding(&self, session_id: &str, provider: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "INSERT INTO session_bindings (session_id, provider, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(session_id) DO UPDATE SET
                provider = excluded.provider,
                updated_at = excluded.updated_at",
            params![session_id, provider, chrono::Utc::now().to_rfc3339()],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    fn load_session_bindings(&self) -> Result<Vec<(String, String)>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut stmt = conn
            .prepare("SELECT session_id, provider FROM session_bindings")
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let session_id: String = row.get("session_id")?;
                let provider: String = row.get("provider")?;
                Ok((session_id, provider))
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows)
    }

    fn delete_session_binding(&self, session_id: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::LockPoisoned)?;
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

}

fn request_log_from_row(row: &rusqlite::Row) -> rusqlite::Result<RequestLog> {
    let ts_str: String = row.get("timestamp")?;
    Ok(RequestLog {
        id: row.get("id")?,
        timestamp: chrono::DateTime::parse_from_rfc3339(&ts_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        provider: row.get("provider")?,
        model: row.get("model")?,
        tokens_in: row.get::<_, i64>("tokens_in")? as u64,
        tokens_out: row.get::<_, i64>("tokens_out")? as u64,
        cost_usd: row.get("cost_usd")?,
        latency_ms: row.get::<_, i64>("latency_ms")? as u64,
        status: row.get::<_, i32>("status")? as u16,
        request_id: row.get("request_id")?,
        client_ip: row.get("client_ip")?,
        route_path: row.get("route_path")?,
        endpoint_type: row.get("endpoint_type")?,
        fallback_used: row.get::<_, i32>("fallback_used")? != 0,
        error: row.get("error")?,
    })
}

fn spend_from_row(row: &rusqlite::Row) -> rusqlite::Result<SpendRecord> {
    let ts_str: String = row.get("timestamp")?;
    Ok(SpendRecord {
        id: row.get("id")?,
        key_id: row.get("key_id")?,
        team_id: row.get("team_id")?,
        model: row.get("model")?,
        provider: row.get("provider")?,
        tokens_in: row.get::<_, i64>("tokens_in")? as u64,
        tokens_out: row.get::<_, i64>("tokens_out")? as u64,
        cost_usd: row.get("cost_usd")?,
        request_id: row.get("request_id")?,
        timestamp: chrono::DateTime::parse_from_rfc3339(&ts_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
    })
}

// We need the optional() method from rusqlite
use rusqlite::OptionalExtension;

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
    fn sqlite_create_and_get_key() {
        let store = SqliteStore::in_memory().unwrap();
        let key = make_key("k1", "hash1");
        store.create_key(key.clone()).unwrap();

        let fetched = store.get_key_by_id("k1").unwrap().unwrap();
        assert_eq!(fetched.id, "k1");
        assert_eq!(fetched.name, "test-key-k1");

        let by_hash = store.get_key_by_hash("hash1").unwrap().unwrap();
        assert_eq!(by_hash.id, "k1");
    }

    #[test]
    fn sqlite_duplicate_key_rejected() {
        let store = SqliteStore::in_memory().unwrap();
        let key = make_key("k1", "hash1");
        store.create_key(key.clone()).unwrap();
        assert!(store.create_key(key).is_err());
    }

    #[test]
    fn sqlite_delete_key() {
        let store = SqliteStore::in_memory().unwrap();
        store.create_key(make_key("k1", "hash1")).unwrap();
        assert!(store.delete_key("k1").unwrap());
        assert!(store.get_key_by_hash("hash1").unwrap().is_none());
        assert!(!store.delete_key("k1").unwrap());
    }

    #[test]
    fn sqlite_list_keys_pagination() {
        let store = SqliteStore::in_memory().unwrap();
        for i in 0..5 {
            store
                .create_key(make_key(&format!("k{i}"), &format!("h{i}")))
                .unwrap();
        }
        let page = store.list_keys(1, 2).unwrap();
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn sqlite_team_crud() {
        let store = SqliteStore::in_memory().unwrap();
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

    #[test]
    fn sqlite_spend_summary() {
        let store = SqliteStore::in_memory().unwrap();
        // Create the referenced key first (FK constraint)
        store.create_key(make_key("k1", "hash1")).unwrap();
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

        let report = store.get_spend_summary(None).unwrap();
        assert_eq!(report.total_requests, 1);
        assert!((report.total_spend_usd - 0.01).abs() < 1e-9);
    }

    #[test]
    fn sqlite_api_integration() {
        use crate::api::AdminApi;
        use crate::costs::CostCalculator;
        use std::sync::Arc;

        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let calc = Arc::new(CostCalculator::new());
        let api = AdminApi::new(store, calc, "test-master-key");

        // Create key
        let body = serde_json::to_vec(&serde_json::json!({"name": "test"})).unwrap();
        let resp = api.handle_request("POST", "/admin/keys", &body, "test-master-key");
        assert_eq!(resp.status, 201);

        // List keys
        let resp = api.handle_request("GET", "/admin/keys", &[], "test-master-key");
        assert_eq!(resp.status, 200);
        let keys: Vec<serde_json::Value> = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn sqlite_request_logs() {
        let store = SqliteStore::in_memory().unwrap();

        // Record a request log
        store
            .record_request_log(RequestLog {
                id: "req1".into(),
                timestamp: Utc::now(),
                provider: "openai".into(),
                model: "gpt-4o".into(),
                tokens_in: 100,
                tokens_out: 50,
                cost_usd: 0.005,
                latency_ms: 230,
                status: 200,
                request_id: Some("rid-1".into()),
                client_ip: Some("127.0.0.1".into()),
                route_path: "/v1/chat/completions".into(),
                endpoint_type: "ChatCompletions".into(),
                fallback_used: false,
                error: None,
            })
            .unwrap();

        store
            .record_request_log(RequestLog {
                id: "req2".into(),
                timestamp: Utc::now(),
                provider: "anthropic".into(),
                model: "claude-sonnet-4-20250514".into(),
                tokens_in: 200,
                tokens_out: 100,
                cost_usd: 0.01,
                latency_ms: 450,
                status: 200,
                request_id: Some("rid-2".into()),
                client_ip: None,
                route_path: "/v1/chat/completions".into(),
                endpoint_type: "ChatCompletions".into(),
                fallback_used: true,
                error: None,
            })
            .unwrap();

        // Get all logs
        let logs = store.get_request_logs(100, None, None).unwrap();
        assert_eq!(logs.len(), 2);
        // Newest first
        assert_eq!(logs[0].id, "req2");
        assert_eq!(logs[1].id, "req1");

        // Filter by provider
        let logs = store.get_request_logs(100, Some("openai"), None).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].provider, "openai");

        // Filter by model
        let logs = store.get_request_logs(100, None, Some("gpt-4o")).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].model, "gpt-4o");

        // Verify fields round-trip correctly
        assert_eq!(logs[0].tokens_in, 100);
        assert_eq!(logs[0].tokens_out, 50);
        assert_eq!(logs[0].latency_ms, 230);
        assert_eq!(logs[0].status, 200);
        assert!(!logs[0].fallback_used);
        assert_eq!(logs[0].request_id, Some("rid-1".into()));
    }

    #[test]
    fn circuit_breaker_state_round_trip() {
        let store = SqliteStore::in_memory().unwrap();

        let row = CircuitBreakerStateRow {
            provider: "OA-VOLC-1".into(),
            state: 1, // open
            failures: 3,
            last_failure_at: 1_700_000_000_000,
            force_open_until: 1_700_000_060_000,
            successes_half_open: 0,
            transitions: 2,
            quota_opens: 1,
            updated_at: Utc::now(),
        };
        store.save_circuit_breaker_state(row.clone()).unwrap();

        // Upsert replaces the row.
        let updated = CircuitBreakerStateRow {
            force_open_until: 1_700_000_120_000,
            ..row.clone()
        };
        store.save_circuit_breaker_state(updated).unwrap();

        let rows = store.load_circuit_breaker_states().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider, "OA-VOLC-1");
        assert_eq!(rows[0].state, 1);
        assert_eq!(rows[0].failures, 3);
        assert_eq!(rows[0].force_open_until, 1_700_000_120_000);
        assert_eq!(rows[0].quota_opens, 1);
    }

    #[test]
    fn session_binding_round_trip() {
        let store = SqliteStore::in_memory().unwrap();

        store
            .save_session_binding("thread-abc-123", "OA-VOLC-1")
            .unwrap();
        store
            .save_session_binding("thread-xyz", "OA-MINIMAX")
            .unwrap();
        // Upsert moves an existing binding to another provider.
        store
            .save_session_binding("thread-abc-123", "OA-ZHIPU-1")
            .unwrap();

        let mut bindings = store.load_session_bindings().unwrap();
        bindings.sort();
        assert_eq!(bindings.len(), 2);
        assert!(bindings.contains(&("thread-abc-123".into(), "OA-ZHIPU-1".into())));
        assert!(bindings.contains(&("thread-xyz".into(), "OA-MINIMAX".into())));

        store.delete_session_binding("thread-abc-123").unwrap();
        let bindings = store.load_session_bindings().unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].0, "thread-xyz");
    }
}
