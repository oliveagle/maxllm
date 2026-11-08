// Copyright 2024 MaxLLM Contributors
// SPDX-License-Identifier: Apache-2.0

//! Admin REST API handler layer.
//!
//! [`AdminApi`] provides a framework-agnostic request dispatcher.  The gateway
//! integration layer calls [`AdminApi::handle_request`] with raw HTTP
//! primitives; this module routes the request to the appropriate handler and
//! returns a status code + JSON body.

use std::sync::Arc;

use serde::Serialize;

use crate::costs::CostCalculator;
use crate::keys::{self, hash_key};
use crate::models::*;
use crate::store::AdminStore;
use crate::teams;

// ---------------------------------------------------------------------------
// Response type
// ---------------------------------------------------------------------------

/// A framework-agnostic HTTP response.
pub struct ApiResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl ApiResponse {
    fn json<T: Serialize>(status: u16, body: &T) -> Self {
        Self {
            status,
            body: serde_json::to_vec(body).unwrap_or_default(),
        }
    }

    fn error(status: u16, message: &str) -> Self {
        Self::json(status, &serde_json::json!({ "error": message }))
    }
}

// ---------------------------------------------------------------------------
// AdminApi
// ---------------------------------------------------------------------------

/// The admin API dispatcher.
pub struct AdminApi {
    store: Arc<dyn AdminStore>,
    cost_calculator: Arc<CostCalculator>,
    /// SHA-256 hex digest of the master admin key.
    master_key_hash: String,
}

impl AdminApi {
    /// Create a new admin API.
    ///
    /// `master_key` is the raw master admin key used to authenticate admin
    /// requests.  It is hashed immediately; the raw value is not stored.
    pub fn new(
        store: Arc<dyn AdminStore>,
        cost_calculator: Arc<CostCalculator>,
        master_key: &str,
    ) -> Self {
        Self {
            store,
            cost_calculator,
            master_key_hash: hash_key(master_key),
        }
    }

    /// Dispatch an admin API request.
    ///
    /// * `method` — HTTP method (e.g. `"GET"`, `"POST"`, `"DELETE"`).
    /// * `path` — Request path, expected to start with `/admin/`.
    /// * `body` — Raw request body bytes (may be empty for GET/DELETE).
    /// * `auth_key` — The bearer token / API key from the `Authorization` header.
    pub fn handle_request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        auth_key: &str,
    ) -> ApiResponse {
        // Authenticate.
        if hash_key(auth_key) != self.master_key_hash {
            return ApiResponse::error(401, "invalid admin key");
        }

        // Normalize path: strip trailing slash, lowercase method.
        let path = path.trim_end_matches('/');
        let method = method.to_uppercase();

        // Route.
        match (method.as_str(), path) {
            // -- Keys -------------------------------------------------------
            ("POST", "/admin/keys") => self.handle_create_key(body),
            ("GET", "/admin/keys") => self.handle_list_keys(),
            ("DELETE", p) if p.starts_with("/admin/keys/") => {
                let id = &p["/admin/keys/".len()..];
                self.handle_delete_key(id)
            }
            ("GET", p) if p.starts_with("/admin/keys/") => {
                let id = &p["/admin/keys/".len()..];
                self.handle_get_key(id)
            }

            // -- Teams ------------------------------------------------------
            ("POST", p) if p.starts_with("/admin/teams/") && p.ends_with("/members") => {
                let rest = &p["/admin/teams/".len()..];
                let team_id = &rest[..rest.len() - "/members".len()];
                self.handle_add_member(team_id, body)
            }
            ("DELETE", p) if p.starts_with("/admin/teams/") && p.contains("/members/") => {
                let rest = &p["/admin/teams/".len()..];
                if let Some((team_id, key_id)) = rest.split_once("/members/") {
                    self.handle_remove_member(team_id, key_id)
                } else {
                    ApiResponse::error(404, "not found")
                }
            }
            ("POST", "/admin/teams") => self.handle_create_team(body),
            ("GET", "/admin/teams") => self.handle_list_teams(),

            // -- Spend ------------------------------------------------------
            ("GET", "/admin/spend/logs") => self.handle_spend_logs(),
            ("GET", "/admin/spend/report") => self.handle_spend_report(),

            // -- Request logs -----------------------------------------------
            ("GET", "/admin/logs") => self.handle_request_logs(),

            // -- Model costs ------------------------------------------------
            ("GET", "/admin/models/costs") => self.handle_model_costs(),

            _ => ApiResponse::error(404, "not found"),
        }
    }

    /// Record a request log entry (called from the gateway logging hook).
    pub fn record_log(&self, log: RequestLog) -> Result<(), crate::store::StoreError> {
        self.store.record_request_log(log)
    }

    // -- Resilience state (called from the gateway so a restart keeps it) ----

    /// Persist one provider's circuit-breaker state (upsert).
    pub fn save_circuit_breaker_state(
        &self,
        row: CircuitBreakerStateRow,
    ) -> Result<(), crate::store::StoreError> {
        self.store.save_circuit_breaker_state(row)
    }

    /// Load every persisted circuit-breaker state (startup restore).
    pub fn load_circuit_breaker_states(
        &self,
    ) -> Result<Vec<CircuitBreakerStateRow>, crate::store::StoreError> {
        self.store.load_circuit_breaker_states()
    }

    /// Drop a provider's persisted breaker state (provider removed/renamed).
    pub fn delete_circuit_breaker_state(
        &self,
        provider: &str,
    ) -> Result<(), crate::store::StoreError> {
        self.store.delete_circuit_breaker_state(provider)
    }

    /// Persist a session → provider affinity binding (upsert).
    pub fn save_session_binding(
        &self,
        session_id: &str,
        provider: &str,
    ) -> Result<(), crate::store::StoreError> {
        self.store.save_session_binding(session_id, provider)
    }

    /// Load every persisted session binding (startup restore).
    pub fn load_session_bindings(&self) -> Result<Vec<(String, String)>, crate::store::StoreError> {
        self.store.load_session_bindings()
    }

    /// Drop a session binding (the pinned provider became unhealthy).
    pub fn delete_session_binding(&self, session_id: &str) -> Result<(), crate::store::StoreError> {
        self.store.delete_session_binding(session_id)
    }

    /// Persist a learned rate-limit pattern (upsert by fingerprint).
    pub fn save_rate_limit_pattern(
        &self,
        row: crate::models::RateLimitPatternRow,
    ) -> Result<(), crate::store::StoreError> {
        self.store.save_rate_limit_pattern(row)
    }

    /// Look up one learned rate-limit pattern by fingerprint.
    pub fn get_rate_limit_pattern(
        &self,
        fingerprint: &str,
    ) -> Result<Option<crate::models::RateLimitPatternRow>, crate::store::StoreError> {
        self.store.get_rate_limit_pattern(fingerprint)
    }

    // -- Key handlers -------------------------------------------------------

    fn handle_create_key(&self, body: &[u8]) -> ApiResponse {
        let request: KeyCreateRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ApiResponse::error(400, &format!("invalid request body: {e}")),
        };
        match keys::generate_key(self.store.as_ref(), request) {
            Ok(resp) => ApiResponse::json(201, &resp),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_list_keys(&self) -> ApiResponse {
        match keys::list_keys(self.store.as_ref(), 0, 100) {
            Ok(keys) => ApiResponse::json(200, &keys),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_get_key(&self, id: &str) -> ApiResponse {
        match keys::get_key_info(self.store.as_ref(), id) {
            Ok(Some(key)) => ApiResponse::json(200, &key),
            Ok(None) => ApiResponse::error(404, "key not found"),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_delete_key(&self, id: &str) -> ApiResponse {
        match keys::revoke_key(self.store.as_ref(), id) {
            Ok(true) => ApiResponse::json(200, &serde_json::json!({ "deleted": true })),
            Ok(false) => ApiResponse::error(404, "key not found"),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    // -- Team handlers ------------------------------------------------------

    fn handle_create_team(&self, body: &[u8]) -> ApiResponse {
        let request: TeamCreateRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ApiResponse::error(400, &format!("invalid request body: {e}")),
        };
        match teams::create_team(self.store.as_ref(), request) {
            Ok(team) => ApiResponse::json(201, &team),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_list_teams(&self) -> ApiResponse {
        match teams::list_teams(self.store.as_ref()) {
            Ok(teams) => ApiResponse::json(200, &teams),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_add_member(&self, team_id: &str, body: &[u8]) -> ApiResponse {
        let request: TeamMemberRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ApiResponse::error(400, &format!("invalid request body: {e}")),
        };
        match teams::add_member(self.store.as_ref(), team_id, &request.key_id) {
            Ok(()) => ApiResponse::json(200, &serde_json::json!({ "ok": true })),
            Err(e) => ApiResponse::error(400, &e.to_string()),
        }
    }

    fn handle_remove_member(&self, team_id: &str, key_id: &str) -> ApiResponse {
        match teams::remove_member(self.store.as_ref(), team_id, key_id) {
            Ok(()) => ApiResponse::json(200, &serde_json::json!({ "ok": true })),
            Err(e) => ApiResponse::error(400, &e.to_string()),
        }
    }

    // -- Spend handlers -----------------------------------------------------

    fn handle_spend_logs(&self) -> ApiResponse {
        match self.store.get_spend_logs(None, 100) {
            Ok(logs) => ApiResponse::json(200, &logs),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    fn handle_spend_report(&self) -> ApiResponse {
        match self.store.get_spend_summary(None) {
            Ok(report) => ApiResponse::json(200, &report),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    // -- Request log handlers -----------------------------------------------

    fn handle_request_logs(&self) -> ApiResponse {
        match self.store.get_request_logs(100, None, None) {
            Ok(logs) => ApiResponse::json(200, &logs),
            Err(e) => ApiResponse::error(500, &e.to_string()),
        }
    }

    // -- Model cost handlers ------------------------------------------------

    fn handle_model_costs(&self) -> ApiResponse {
        ApiResponse::json(200, &self.cost_calculator.all_costs())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryStore;

    const MASTER_KEY: &str = "sk-admin-master-test-key";

    fn setup() -> AdminApi {
        let store = Arc::new(InMemoryStore::new());
        let calc = Arc::new(CostCalculator::new());
        AdminApi::new(store, calc, MASTER_KEY)
    }

    fn parse_json(resp: &ApiResponse) -> serde_json::Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn auth_required() {
        let api = setup();
        let resp = api.handle_request("GET", "/admin/keys", &[], "wrong-key");
        assert_eq!(resp.status, 401);
    }

    #[test]
    fn create_and_list_keys() {
        let api = setup();

        let body = serde_json::to_vec(&serde_json::json!({
            "name": "test-key",
            "allowed_models": ["gpt-4o"],
        }))
        .unwrap();

        let resp = api.handle_request("POST", "/admin/keys", &body, MASTER_KEY);
        assert_eq!(resp.status, 201);
        let json = parse_json(&resp);
        assert!(json["key"].as_str().unwrap().starts_with("sk-maxllm-"));

        let resp = api.handle_request("GET", "/admin/keys", &[], MASTER_KEY);
        assert_eq!(resp.status, 200);
        let keys: Vec<serde_json::Value> = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn get_and_delete_key() {
        let api = setup();

        let body = serde_json::to_vec(&serde_json::json!({ "name": "k1" })).unwrap();
        let resp = api.handle_request("POST", "/admin/keys", &body, MASTER_KEY);
        let key_id = parse_json(&resp)["key_id"].as_str().unwrap().to_string();

        // GET
        let resp = api.handle_request("GET", &format!("/admin/keys/{key_id}"), &[], MASTER_KEY);
        assert_eq!(resp.status, 200);
        assert_eq!(parse_json(&resp)["id"].as_str().unwrap(), key_id);

        // DELETE (revoke)
        let resp = api.handle_request("DELETE", &format!("/admin/keys/{key_id}"), &[], MASTER_KEY);
        assert_eq!(resp.status, 200);

        // Key should be inactive now.
        let resp = api.handle_request("GET", &format!("/admin/keys/{key_id}"), &[], MASTER_KEY);
        let json = parse_json(&resp);
        assert_eq!(json["is_active"].as_bool(), Some(false));
    }

    #[test]
    fn resilience_store_passthrough() {
        let api = setup();

        // Circuit-breaker state round-trips through the store.
        let row = crate::models::CircuitBreakerStateRow {
            provider: "p1".into(),
            state: 1,
            failures: 3,
            last_failure_at: 1234,
            force_open_until: 99999,
            successes_half_open: 0,
            transitions: 2,
            quota_opens: 1,
            updated_at: chrono::Utc::now(),
        };
        api.save_circuit_breaker_state(row).unwrap();
        let rows = api.load_circuit_breaker_states().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider, "p1");
        assert_eq!(rows[0].state, 1);
        assert_eq!(rows[0].failures, 3);

        // Session bindings round-trip and can be dropped.
        api.save_session_binding("sess-1", "provider-a").unwrap();
        let bindings = api.load_session_bindings().unwrap();
        assert_eq!(
            bindings,
            vec![("sess-1".to_string(), "provider-a".to_string())]
        );
        api.delete_session_binding("sess-1").unwrap();
        assert!(api.load_session_bindings().unwrap().is_empty());

        // Rate-limit patterns round-trip by fingerprint.
        let row = crate::models::RateLimitPatternRow {
            fingerprint: "fp-1".into(),
            is_rate_limit: true,
            reset_expression: Some("duration".into()),
            sample_message: "rate limit 30s".into(),
            hits: 1,
            updated_at: chrono::Utc::now(),
        };
        api.save_rate_limit_pattern(row).unwrap();
        let found = api
            .get_rate_limit_pattern("fp-1")
            .unwrap()
            .expect("pattern");
        assert!(found.is_rate_limit);
        assert!(api.get_rate_limit_pattern("missing").unwrap().is_none());

        // Request logs round-trip.
        let log = crate::models::RequestLog {
            id: "r1".into(),
            timestamp: chrono::Utc::now(),
            provider: "p1".into(),
            model: "m".into(),
            tokens_in: 1,
            tokens_out: 2,
            cost_usd: 0.0,
            latency_ms: 5,
            status: 200,
            request_id: None,
            client_ip: None,
            route_path: "/v1/chat".into(),
            endpoint_type: "chat".into(),
            fallback_used: false,
            error: None,
        };
        api.record_log(log).unwrap();
    }

    #[test]
    fn team_member_removal_routes() {
        let api = setup();

        // Create a team and a key, add a member, then remove it via the
        // `/admin/teams/{team}/members/{key}` path.
        let body = serde_json::to_vec(&serde_json::json!({ "name": "eng" })).unwrap();
        let resp = api.handle_request("POST", "/admin/teams", &body, MASTER_KEY);
        let team_id = parse_json(&resp)["id"].as_str().unwrap().to_string();

        let body = serde_json::to_vec(&serde_json::json!({ "name": "k1" })).unwrap();
        let resp = api.handle_request("POST", "/admin/keys", &body, MASTER_KEY);
        let key_id = parse_json(&resp)["key_id"].as_str().unwrap().to_string();

        let body = serde_json::to_vec(&serde_json::json!({ "key_id": key_id })).unwrap();
        let resp = api.handle_request(
            "POST",
            &format!("/admin/teams/{team_id}/members"),
            &body,
            MASTER_KEY,
        );
        assert_eq!(resp.status, 200);

        let resp = api.handle_request(
            "DELETE",
            &format!("/admin/teams/{team_id}/members/{key_id}"),
            &[],
            MASTER_KEY,
        );
        assert_eq!(resp.status, 200);

        // An unmatched teams path is a 404.
        let resp = api.handle_request("DELETE", "/admin/teams/x", &[], MASTER_KEY);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn unknown_admin_route_is_404() {
        let api = setup();
        for (method, path) in [
            ("GET", "/admin/nope"),
            ("POST", "/admin/spend"),
            ("PUT", "/admin/keys"),
        ] {
            let resp = api.handle_request(method, path, &[], MASTER_KEY);
            assert_eq!(resp.status, 404, "{method} {path}");
        }
        // Trailing slashes are normalised.
        let body = serde_json::to_vec(&serde_json::json!({ "name": "t" })).unwrap();
        let resp = api.handle_request("POST", "/admin/teams/", &body, MASTER_KEY);
        assert_eq!(resp.status, 201);
    }

    #[test]
    fn create_and_list_teams() {
        let api = setup();

        let body = serde_json::to_vec(&serde_json::json!({
            "name": "engineering",
            "max_budget_usd": 1000.0,
        }))
        .unwrap();

        let resp = api.handle_request("POST", "/admin/teams", &body, MASTER_KEY);
        assert_eq!(resp.status, 201);

        let resp = api.handle_request("GET", "/admin/teams", &[], MASTER_KEY);
        assert_eq!(resp.status, 200);
        let teams: Vec<serde_json::Value> = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(teams.len(), 1);
    }

    #[test]
    fn team_membership() {
        let api = setup();

        // Create key.
        let body = serde_json::to_vec(&serde_json::json!({ "name": "k1" })).unwrap();
        let resp = api.handle_request("POST", "/admin/keys", &body, MASTER_KEY);
        let key_id = parse_json(&resp)["key_id"].as_str().unwrap().to_string();

        // Create team.
        let body = serde_json::to_vec(&serde_json::json!({ "name": "eng" })).unwrap();
        let resp = api.handle_request("POST", "/admin/teams", &body, MASTER_KEY);
        let team_id = parse_json(&resp)["id"].as_str().unwrap().to_string();

        // Add member.
        let body = serde_json::to_vec(&serde_json::json!({ "key_id": key_id })).unwrap();
        let resp = api.handle_request(
            "POST",
            &format!("/admin/teams/{team_id}/members"),
            &body,
            MASTER_KEY,
        );
        assert_eq!(resp.status, 200);

        // Remove member.
        let resp = api.handle_request(
            "DELETE",
            &format!("/admin/teams/{team_id}/members/{key_id}"),
            &[],
            MASTER_KEY,
        );
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn model_costs_endpoint() {
        let api = setup();
        let resp = api.handle_request("GET", "/admin/models/costs", &[], MASTER_KEY);
        assert_eq!(resp.status, 200);
        let costs: Vec<serde_json::Value> = serde_json::from_slice(&resp.body).unwrap();
        assert!(!costs.is_empty());
    }

    #[test]
    fn spend_endpoints() {
        let api = setup();

        let resp = api.handle_request("GET", "/admin/spend/logs", &[], MASTER_KEY);
        assert_eq!(resp.status, 200);

        let resp = api.handle_request("GET", "/admin/spend/report", &[], MASTER_KEY);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn not_found_route() {
        let api = setup();
        let resp = api.handle_request("GET", "/admin/nonexistent", &[], MASTER_KEY);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn trailing_slash_normalized() {
        let api = setup();
        let resp = api.handle_request("GET", "/admin/keys/", &[], MASTER_KEY);
        // Should match GET /admin/keys.
        assert_eq!(resp.status, 200);
    }
}
