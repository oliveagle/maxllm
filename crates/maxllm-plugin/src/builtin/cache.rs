// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use crate::factory::PluginError;
use crate::{HttpResponse, Plugin, PluginCtx, RequestAction};
use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::ResponseHeader;
use pingora::proxy::Session;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A cached response entry with TTL support.
struct CachedResponse {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    cached_at: Instant,
    ttl: Duration,
}

impl CachedResponse {
    fn is_expired(&self) -> bool {
        expired_after(self.cached_at.elapsed(), self.ttl)
    }
}

/// Whether an elapsed duration is past the TTL. A response that has been
/// cached for *exactly* the TTL is not yet expired.
fn expired_after(elapsed: Duration, ttl: Duration) -> bool {
    elapsed > ttl
}

/// Simple bounded cache with TTL. Evicts the oldest entry when full.
struct BoundedCache {
    entries: HashMap<String, CachedResponse>,
    insertion_order: Vec<String>,
    max_entries: usize,
}

impl BoundedCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(max_entries),
            insertion_order: Vec::with_capacity(max_entries),
            max_entries,
        }
    }

    fn get(&mut self, key: &str) -> Option<&CachedResponse> {
        // Remove expired entry if present.
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired() {
                self.entries.remove(key);
                self.insertion_order.retain(|k| k != key);
                return None;
            }
        }
        self.entries.get(key)
    }

    fn insert(&mut self, key: String, entry: CachedResponse) {
        // If already present, remove old entry from order tracking.
        if self.entries.contains_key(&key) {
            self.insertion_order.retain(|k| k != &key);
        }

        // Evict oldest entries until we have room.
        while self.entries.len() >= self.max_entries && !self.insertion_order.is_empty() {
            let oldest = self.insertion_order.remove(0);
            self.entries.remove(&oldest);
        }

        self.insertion_order.push(key.clone());
        self.entries.insert(key, entry);
    }
}

/// In-memory response cache plugin for LLM responses.
///
/// Caches non-streaming 200 OK responses using a SHA-256 hash of the
/// request body fields (model, messages, temperature, top_p, max_tokens)
/// as the cache key. Respects `x-maxllm-no-cache: true` to bypass.
pub struct CachePlugin {
    name: String,
    cache: Arc<Mutex<BoundedCache>>,
    default_ttl: Duration,
}

/// Extension key for accumulating response body chunks.
const EXT_CACHE_BODY: &str = "_cache_body";
/// Extension key for the cache key computed during on_request.
const EXT_CACHE_KEY: &str = "_cache_key";
/// Extension key for cached response headers.
const EXT_CACHE_HEADERS: &str = "_cache_headers";
/// Extension key signaling that caching is enabled for this request.
const EXT_CACHE_ENABLED: &str = "_cache_enabled";

/// Whether an `x-maxllm-no-cache` header value opts out of caching.
fn no_cache_header(value: &str) -> bool {
    value == "true"
}

/// Whether an `Accept` header value indicates a streaming request.
fn streaming_accept_header(value: &str) -> bool {
    value.contains("text/event-stream")
}

/// Whether an upstream status code is cacheable.
fn cacheable_status(status: u16) -> bool {
    status == 200
}

impl CachePlugin {
    pub fn from_config(name: &str, config: &toml::Table) -> Result<Self, PluginError> {
        let max_entries = config
            .get("max_entries")
            .and_then(|v| v.as_integer())
            .unwrap_or(10_000) as usize;

        let default_ttl_secs = config
            .get("default_ttl_secs")
            .and_then(|v| v.as_integer())
            .unwrap_or(3600) as u64;

        Ok(Self {
            name: name.to_string(),
            cache: Arc::new(Mutex::new(BoundedCache::new(max_entries))),
            default_ttl: Duration::from_secs(default_ttl_secs),
        })
    }

    /// Compute a cache key from a request body by hashing relevant fields.
    /// The body is expected to be JSON with model, messages, temperature,
    /// top_p, and max_tokens fields.
    pub fn compute_cache_key(body: &[u8], model: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(model.as_bytes());
        hasher.update(body);
        hex::encode(hasher.finalize())
    }
}

#[async_trait]
impl Plugin for CachePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_request(
        &self,
        session: &mut Session,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        // Skip if client opted out of caching.
        let no_cache = session
            .req_header()
            .headers
            .get("x-maxllm-no-cache")
            .and_then(|v| v.to_str().ok())
            .map(no_cache_header)
            .unwrap_or(false);

        if no_cache {
            return Ok(RequestAction::Continue);
        }

        // Skip streaming requests (Accept: text/event-stream).
        let is_streaming = session
            .req_header()
            .headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .map(streaming_accept_header)
            .unwrap_or(false);

        if is_streaming {
            return Ok(RequestAction::Continue);
        }

        // Mark caching as enabled for downstream hooks.
        ctx.extensions
            .insert(EXT_CACHE_ENABLED.into(), "true".into());

        // We cannot read the body in on_request, so the cache key will be
        // set by the gateway after body parsing. The gateway should call
        // `compute_cache_key()` and store it in ctx.extensions[EXT_CACHE_KEY].
        // If the gateway has already set the key (e.g., from a prior filter),
        // check the cache now.
        if let Some(key) = ctx.extensions.get(EXT_CACHE_KEY) {
            let key = key.clone();
            let mut cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.get(&key) {
                let mut resp = HttpResponse {
                    status: 200,
                    headers: cached.headers.clone(),
                    body: cached.body.clone(),
                };
                resp.headers.push(("X-MaxLLM-Cache".into(), "HIT".into()));
                return Ok(RequestAction::Respond(resp));
            }
        }

        Ok(RequestAction::Continue)
    }

    async fn on_response(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<()> {
        // Only cache 200 OK responses.
        if !cacheable_status(upstream_response.status.as_u16()) {
            ctx.extensions.remove(EXT_CACHE_ENABLED);
            return Ok(());
        }

        // Skip if caching was not enabled.
        if ctx.extensions.get(EXT_CACHE_ENABLED).is_none() {
            return Ok(());
        }

        // Collect response headers for caching.
        let mut header_pairs = Vec::new();
        for (name, value) in upstream_response.headers.iter() {
            if let Ok(v) = value.to_str() {
                header_pairs.push((name.as_str().to_string(), v.to_string()));
            }
        }
        ctx.extensions.insert(
            EXT_CACHE_HEADERS.into(),
            serde_json::to_string(&header_pairs).unwrap_or_default(),
        );

        // Add cache MISS header.
        upstream_response.insert_header("X-MaxLLM-Cache", "MISS")?;

        Ok(())
    }

    fn on_response_body(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<()> {
        // Only accumulate if caching is enabled for this request.
        if ctx.extensions.get(EXT_CACHE_ENABLED).is_none() {
            return Ok(());
        }

        // Accumulate body chunks in extensions (base64 would be safer but
        // for simplicity we concatenate raw bytes as a hex string).
        if let Some(chunk) = body {
            let existing = ctx
                .extensions
                .get(EXT_CACHE_BODY)
                .cloned()
                .unwrap_or_default();
            let mut buf = hex::decode(&existing).unwrap_or_default();
            buf.extend_from_slice(chunk);
            ctx.extensions
                .insert(EXT_CACHE_BODY.into(), hex::encode(&buf));
        }

        if end_of_stream {
            let cache_key = match ctx.extensions.get(EXT_CACHE_KEY) {
                Some(k) => k.clone(),
                None => {
                    // No cache key was set; compute from accumulated body + model.
                    let body_hex = ctx
                        .extensions
                        .get(EXT_CACHE_BODY)
                        .cloned()
                        .unwrap_or_default();
                    let body_bytes = hex::decode(&body_hex).unwrap_or_default();
                    let key = Self::compute_cache_key(&body_bytes, &ctx.model);
                    ctx.extensions.insert(EXT_CACHE_KEY.into(), key.clone());
                    key
                }
            };

            let body_hex = ctx.extensions.remove(EXT_CACHE_BODY).unwrap_or_default();
            let body_bytes = hex::decode(&body_hex).unwrap_or_default();

            let headers: Vec<(String, String)> = ctx
                .extensions
                .get(EXT_CACHE_HEADERS)
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            let entry = CachedResponse {
                body: body_bytes,
                headers,
                cached_at: Instant::now(),
                ttl: self.default_ttl,
            };

            let mut cache = self.cache.lock().unwrap();
            cache.insert(cache_key, entry);

            // Clean up extensions.
            ctx.extensions.remove(EXT_CACHE_HEADERS);
            ctx.extensions.remove(EXT_CACHE_ENABLED);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_config_defaults() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "cache".into());

        let plugin = CachePlugin::from_config("cache", &config).unwrap();
        assert_eq!(plugin.default_ttl, Duration::from_secs(3600));
    }

    #[test]
    fn test_from_config_custom() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "cache".into());
        config.insert("max_entries".into(), 500.into());
        config.insert("default_ttl_secs".into(), 120.into());

        let plugin = CachePlugin::from_config("cache", &config).unwrap();
        assert_eq!(plugin.default_ttl, Duration::from_secs(120));
    }

    #[test]
    fn test_compute_cache_key_deterministic() {
        let body = b"{\"model\":\"gpt-4\",\"messages\":[]}";
        let k1 = CachePlugin::compute_cache_key(body, "gpt-4");
        let k2 = CachePlugin::compute_cache_key(body, "gpt-4");
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_compute_cache_key_differs_by_model() {
        let body = b"{\"messages\":[]}";
        let k1 = CachePlugin::compute_cache_key(body, "gpt-4");
        let k2 = CachePlugin::compute_cache_key(body, "gpt-3.5");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_bounded_cache_eviction() {
        let mut cache = BoundedCache::new(2);
        let entry = |data: &[u8]| CachedResponse {
            body: data.to_vec(),
            headers: vec![],
            cached_at: Instant::now(),
            ttl: Duration::from_secs(3600),
        };

        cache.insert("a".into(), entry(b"aaa"));
        cache.insert("b".into(), entry(b"bbb"));
        cache.insert("c".into(), entry(b"ccc"));

        // "a" should have been evicted.
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn test_cached_response_expiry() {
        let entry = CachedResponse {
            body: vec![],
            headers: vec![],
            cached_at: Instant::now() - Duration::from_secs(10),
            ttl: Duration::from_secs(5),
        };
        assert!(entry.is_expired());

        let fresh = CachedResponse {
            body: vec![],
            headers: vec![],
            cached_at: Instant::now(),
            ttl: Duration::from_secs(3600),
        };
        assert!(!fresh.is_expired());
    }

    #[test]
    fn test_expired_after_boundary() {
        let ttl = Duration::from_secs(60);
        assert!(expired_after(ttl + Duration::from_nanos(1), ttl));
        assert!(!expired_after(ttl - Duration::from_nanos(1), ttl));
        // Exactly-at-TTL is not expired (elapsed > ttl, not >=).
        assert!(!expired_after(ttl, ttl));
    }

    #[test]
    fn test_cache_name_is_configured_name() {
        let plugin = CachePlugin::from_config("resp-cache", &toml::Table::new()).unwrap();
        assert_eq!(plugin.name(), "resp-cache");
    }

    #[test]
    fn test_bounded_cache_get_removes_expired_entry_from_order() {
        let mut cache = BoundedCache::new(8);
        let ttl = Duration::from_millis(1);
        cache.insert(
            "k".into(),
            CachedResponse {
                body: vec![],
                headers: vec![],
                cached_at: Instant::now() - Duration::from_secs(10),
                ttl,
            },
        );
        assert!(cache.get("k").is_none());
        // The stale key must be dropped from the insertion order too.
        assert!(cache.insertion_order.is_empty());
    }

    #[test]
    fn test_bounded_cache_reinsert_keeps_a_single_order_entry() {
        let mut cache = BoundedCache::new(8);
        let entry = || CachedResponse {
            body: vec![],
            headers: vec![],
            cached_at: Instant::now(),
            ttl: Duration::from_secs(60),
        };
        cache.insert("k".into(), entry());
        cache.insert("k".into(), entry());
        assert_eq!(cache.insertion_order, vec!["k".to_string()]);
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn test_bounded_cache_evicts_oldest_first() {
        let mut cache = BoundedCache::new(2);
        let entry = || CachedResponse {
            body: vec![],
            headers: vec![],
            cached_at: Instant::now(),
            ttl: Duration::from_secs(60),
        };
        cache.insert("a".into(), entry());
        cache.insert("b".into(), entry());
        cache.insert("c".into(), entry());
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
        assert_eq!(
            cache.insertion_order,
            vec!["b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_no_cache_header_parsing() {
        assert!(no_cache_header("true"));
        assert!(!no_cache_header("false"));
        assert!(!no_cache_header("1"));
        assert!(!no_cache_header(""));
    }

    #[test]
    fn test_streaming_accept_header_parsing() {
        assert!(streaming_accept_header("text/event-stream"));
        assert!(streaming_accept_header(
            "application/json, text/event-stream"
        ));
        assert!(!streaming_accept_header("application/json"));
        assert!(!streaming_accept_header(""));
    }

    #[test]
    fn test_cacheable_status_parsing() {
        assert!(cacheable_status(200));
        assert!(!cacheable_status(201));
        assert!(!cacheable_status(404));
        assert!(!cacheable_status(500));
    }
}
