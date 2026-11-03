// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Session-sticky routing — port of litellm_lb_go's session affinity.
//!
//! A conversation is bound to one upstream provider so every turn of the same
//! session hits the same API key. Different keys do not share provider-side
//! context, so a session that hops between channels loses the thread and the
//! answer quality drops.
//!
//! To honour that, the gateway has to know which session a request belongs
//! to. Clients spell "which session is this" in several ways, and all of them
//! are accepted (same precedence as litellm_lb_go):
//!
//! - `X-Session-ID` / `X-Conversation-ID` — the Anthropic-style spelling used
//!   by Claude Code and the OpenAI clients that mimic it.
//! - `session-id` / `thread-id` — the spelling codex-cli sends; its thread id
//!   carries the same conversation id as its session id.
//! - body `metadata.user_id` — the Claude Code per-conversation user id.
//! - body `client_metadata.session_id` / `client_metadata.thread_id` — the
//!   bag codex-cli adds to the request body.
//! - body `prompt_cache_key` — the Responses field clients keep stable for
//!   the whole conversation.
//!
//! The checks run most-explicit first and the first non-empty value wins, so
//! a client that sends several identifiers never ends up with two bindings for
//! the same conversation.

use http::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Header names that may carry a session identity, in precedence order.
const SESSION_HEADERS: [&str; 4] = [
    "x-session-id",
    "x-conversation-id",
    "session-id",
    "thread-id",
];

/// A single session binding.
#[derive(Clone, Debug)]
pub struct SessionEntry {
    /// Provider the session is pinned to.
    pub provider: String,
    /// Last time the session was used (ns epoch).
    pub last_accessed: u64,
}

/// In-memory session → provider binding store with LRU eviction.
pub struct SessionStore {
    entries: Mutex<HashMap<String, SessionEntry>>,
    /// Max number of live bindings; the least recently used one is evicted.
    max_entries: usize,
    /// Binding lifetime since last use, in seconds.
    timeout_secs: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    /// When the expired entries were last swept (ms epoch).
    last_sweep: AtomicU64,
}

impl SessionStore {
    pub fn new(max_entries: usize, timeout_secs: u64) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries: max_entries.max(1),
            timeout_secs,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            last_sweep: AtomicU64::new(0),
        }
    }

    /// Look up the provider a session is pinned to. Refreshes the entry's
    /// last-access time so a busy session is never evicted.
    pub fn get(&self, session_id: &str) -> Option<String> {
        let now = now_ns();
        let mut entries = self.entries.lock().ok()?;
        match entries.get_mut(session_id) {
            Some(entry) => {
                if self.is_expired(entry.last_accessed, now) {
                    entries.remove(session_id);
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                entry.last_accessed = now;
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.provider.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Look up a binding without refreshing access time or counters. Used to
    /// check whether a failing provider is the one a session is pinned to.
    pub fn peek(&self, session_id: &str) -> Option<String> {
        let entries = self.entries.lock().ok()?;
        entries.get(session_id).map(|e| e.provider.clone())
    }

    /// Bind a session to a provider. Re-binding overwrites the previous
    /// provider (the old one tripped or expired). Returns how many entries
    /// were evicted (LRU) by this insert, so the caller can feed the metrics.
    pub fn set(&self, session_id: &str, provider: &str) -> usize {
        let now = now_ns();
        let mut entries = match self.entries.lock() {
            Ok(e) => e,
            Err(_) => return 0,
        };

        // Opportunistic expiry sweep: run at most once per timeout_secs/2 so a
        // busy session store is not scanned on every request.
        if self.timeout_secs > 0 {
            let last = self.last_sweep.load(Ordering::Relaxed);
            if now.saturating_sub(last) >= self.timeout_secs * 500_000_000 {
                entries.retain(|_, e| !self.is_expired(e.last_accessed, now));
                self.last_sweep.store(now, Ordering::Relaxed);
            }
        }

        entries.insert(
            session_id.to_string(),
            SessionEntry {
                provider: provider.to_string(),
                last_accessed: now,
            },
        );
        // Evict the least recently used entry when over the cap.
        let mut evicted = 0usize;
        while entries.len() > self.max_entries {
            let oldest = entries
                .iter()
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(k, _)| k.clone());
            match oldest {
                Some(key) => {
                    entries.remove(&key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    evicted += 1;
                }
                None => break,
            }
        }
        evicted
    }

    /// Drop a binding (e.g. after the pinned provider kept failing).
    pub fn remove(&self, session_id: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(session_id);
        }
    }

    /// Remove bindings that were not used within the timeout.
    #[allow(dead_code)]
    pub fn clear_expired(&self) -> usize {
        let now = now_ns();
        let mut entries = match self.entries.lock() {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let before = entries.len();
        entries.retain(|_, e| !self.is_expired(e.last_accessed, now));
        before - entries.len()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    fn is_expired(&self, last_accessed: u64, now: u64) -> bool {
        if self.timeout_secs == 0 {
            return false;
        }
        now.saturating_sub(last_accessed) >= self.timeout_secs * 1_000_000_000
    }
}

/// Resolve the session id carried by the request headers.
pub fn resolve_from_headers(headers: &HeaderMap) -> Option<String> {
    for name in SESSION_HEADERS {
        if let Some(value) = headers.get(name) {
            if let Ok(text) = value.to_str() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

/// Resolve the session id carried by the request body.
pub fn resolve_from_body(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_slice(body).ok()?;

    // metadata.user_id (Claude Code per-conversation user id)
    if let Some(uid) = parsed
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .and_then(|v| v.as_str())
    {
        let trimmed = uid.trim();
        if !trimmed.is_empty() {
            return Some(format!("user:{trimmed}"));
        }
    }

    // client_metadata.session_id / client_metadata.thread_id (codex-cli)
    if let Some(meta) = parsed.get("client_metadata") {
        for key in ["session_id", "thread_id"] {
            if let Some(value) = meta.get(key).and_then(|v| v.as_str()) {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }

    // prompt_cache_key (stable for the whole conversation in Responses)
    if let Some(key) = parsed.get("prompt_cache_key").and_then(|v| v.as_str()) {
        let trimmed = key.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    None
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn test_store_binds_and_returns() {
        let store = SessionStore::new(10, 3600);
        assert!(store.get("s1").is_none());
        store.set("s1", "provider-a");
        assert_eq!(store.get("s1").as_deref(), Some("provider-a"));
        assert_eq!(store.hits(), 1);
        assert_eq!(store.misses(), 1);
    }

    #[test]
    fn test_store_evicts_lru() {
        let store = SessionStore::new(2, 3600);
        store.set("s1", "p1");
        store.set("s2", "p2");
        // Touch s1 so s2 becomes the least recently used.
        store.get("s1");
        store.set("s3", "p3");
        assert_eq!(store.len(), 2);
        assert!(store.get("s2").is_none());
        assert_eq!(store.get("s1").as_deref(), Some("p1"));
        assert_eq!(store.evictions(), 1);
    }

    #[test]
    fn test_store_expires_entries() {
        let store = SessionStore::new(10, 0);
        store.set("s1", "p1");
        // timeout 0 means "never expires"
        assert!(store.get("s1").is_some());

        let store = SessionStore::new(10, 1);
        store.set("s1", "p1");
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(store.get("s1").is_none());
    }

    #[test]
    fn test_header_precedence() {
        let h = headers(&[
            ("x-session-id", "sid"),
            ("session-id", "codex-sid"),
            ("thread-id", "thread"),
        ]);
        assert_eq!(resolve_from_headers(&h).as_deref(), Some("sid"));

        let h = headers(&[("session-id", "codex-sid"), ("thread-id", "thread")]);
        assert_eq!(resolve_from_headers(&h).as_deref(), Some("codex-sid"));

        let h = headers(&[("thread-id", "thread")]);
        assert_eq!(resolve_from_headers(&h).as_deref(), Some("thread"));

        assert!(resolve_from_headers(&headers(&[])).is_none());
    }

    #[test]
    fn test_body_resolution() {
        let body = br#"{"metadata":{"user_id":"u-1"}}"#;
        assert_eq!(resolve_from_body(body).as_deref(), Some("user:u-1"));

        let body = br#"{"client_metadata":{"session_id":"sess-9"}}"#;
        assert_eq!(resolve_from_body(body).as_deref(), Some("sess-9"));

        let body = br#"{"client_metadata":{"thread_id":"th-9"}}"#;
        assert_eq!(resolve_from_body(body).as_deref(), Some("th-9"));

        let body = br#"{"prompt_cache_key":"pc-9"}"#;
        assert_eq!(resolve_from_body(body).as_deref(), Some("pc-9"));

        assert!(resolve_from_body(b"not json").is_none());
        assert!(resolve_from_body(b"{}").is_none());
    }
}
