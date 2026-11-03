// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Request/response recorder — a JSONL audit trail of every client and
//! provider exchange. Port of litellm_lb_go's `internal/infrastructure/recording`.
//!
//! Events are written as one JSON object per line, rotated hourly (with a
//! `-N` size suffix when a shard exceeds `max_file_bytes`), bodies truncated
//! to `max_body_bytes`, and shards older than `retention_hours` pruned.

use maxllm_config::RecordingConfig;
use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Event kinds, mirroring litellm_lb_go's `EventType`.
pub const EVENT_CLIENT_REQUEST: &str = "client_request";
pub const EVENT_CLIENT_RESPONSE: &str = "client_response";
pub const EVENT_PROVIDER_REQUEST: &str = "provider_request";
pub const EVENT_PROVIDER_RESPONSE: &str = "provider_response";
pub const EVENT_ERROR: &str = "error";

/// One recording entry. Fields mirror litellm_lb_go's `RecordEntry`.
#[derive(Clone, Debug, Default)]
pub struct RecordEntry {
    pub event_type: &'static str,
    pub request_uuid: String,
    pub session_id: Option<String>,
    pub client_ip: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status_code: Option<u16>,
    /// Raw body bytes (truncated to `max_body_bytes` at write time).
    pub body: Option<Vec<u8>>,
    pub duration_ms: Option<u64>,
    pub ttft_ms: Option<u64>,
    pub tokens_in: Option<u64>,
    pub tokens_out: Option<u64>,
    pub error: Option<String>,
}

/// JSONL recorder with hourly rotation, size sharding, body truncation and
/// retention pruning. Cheap to construct; `record` is a quick lock + append.
pub struct Recorder {
    dir: PathBuf,
    max_body_bytes: usize,
    max_file_bytes: u64,
    retention_hours: u64,
    /// Entries written since the last opportunistic cleanup.
    since_cleanup: Mutex<u32>,
    inner: Mutex<Inner>,
}

struct Inner {
    file: Option<File>,
    /// Current shard stem, e.g. `2026-11-03T19` (or with `-1` size suffix).
    shard_stem: String,
    written_bytes: u64,
}

impl Recorder {
    pub fn new(cfg: &RecordingConfig) -> std::io::Result<Self> {
        let dir = PathBuf::from(&cfg.dir);
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            max_body_bytes: cfg.max_body_bytes,
            max_file_bytes: cfg.max_file_bytes.max(1024),
            retention_hours: cfg.retention_hours,
            since_cleanup: Mutex::new(0),
            inner: Mutex::new(Inner {
                file: None,
                shard_stem: String::new(),
                written_bytes: 0,
            }),
        })
    }

    /// Append one event. Best-effort: a failed write is logged as a warning,
    /// never allowed to fail the request being recorded.
    pub fn record(&self, entry: RecordEntry) {
        let now_ms = now_ms();
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        let shard = shard_stem(now_ms);
        // Rotate when the hour changes or the current shard exceeded its size.
        if inner.file.is_none() || inner.shard_stem != shard {
            match self.open_shard(&shard, 0) {
                Ok((f, stem)) => {
                    inner.file = Some(f);
                    inner.shard_stem = stem;
                    inner.written_bytes = 0;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "recorder: failed to open shard, dropping entry");
                    return;
                }
            }
        } else if inner.written_bytes >= self.max_file_bytes {
            let mut suffix = 1;
            let mut stem = format!("{shard}-{suffix}");
            // Find the next free size suffix.
            loop {
                let path = self.dir.join(format!("{stem}.jsonl"));
                if !path.exists() {
                    break;
                }
                suffix += 1;
                stem = format!("{shard}-{suffix}");
            }
            match self.open_shard(&shard, suffix) {
                Ok((f, stem)) => {
                    inner.file = Some(f);
                    inner.shard_stem = stem;
                    inner.written_bytes = 0;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "recorder: failed to rotate shard, dropping entry");
                    return;
                }
            }
        }

        let mut line = self.entry_to_json(&entry, now_ms);
        line.push('\n');
        let bytes = line.as_bytes();
        if let Some(file) = inner.file.as_mut() {
            let written = file.write(bytes).map_err(|e| {
                tracing::warn!(error = %e, "recorder: write failed");
                e
            });
            if let Ok(n) = written {
                inner.written_bytes += n as u64;
            }
        }

        // Opportunistic retention pruning every ~500 writes.
        let mut counter = match self.since_cleanup.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        *counter += 1;
        if *counter >= 500 {
            *counter = 0;
            let dir = self.dir.clone();
            let retention = self.retention_hours;
            drop(inner);
            Self::prune_old_shards(&dir, retention);
        }
    }

    fn open_shard(&self, stem: &str, size_suffix: u32) -> std::io::Result<(File, String)> {
        let file_stem = if size_suffix > 0 {
            format!("{stem}-{size_suffix}")
        } else {
            stem.to_string()
        };
        let path = self.dir.join(format!("{file_stem}.jsonl"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((file, file_stem))
    }

    fn entry_to_json(&self, entry: &RecordEntry, ts_ms: i64) -> String {
        let mut body: Option<Value> = None;
        let mut body_truncated = false;
        if let Some(raw) = &entry.body {
            if raw.len() > self.max_body_bytes {
                let head = &raw[..self.max_body_bytes];
                // Keep it parseable when the body is JSON: cut at a `}` boundary
                // if one exists in the truncated window.
                let cut = head
                    .iter()
                    .rposition(|b| *b == b'}')
                    .map(|i| i + 1)
                    .unwrap_or(head.len());
                body = serde_json::from_slice(&head[..cut]).ok();
                body_truncated = true;
            } else {
                body = serde_json::from_slice(raw).ok();
            }
        }

        let mut obj = json!({
            "timestamp_ms": ts_ms,
            "event_type": entry.event_type,
            "request_uuid": entry.request_uuid,
        });
        add_opt(&mut obj, "session_id", entry.session_id.as_ref().map(|s| s.as_str()));
        add_opt(&mut obj, "client_ip", entry.client_ip.as_ref().map(|s| s.as_str()));
        add_opt(&mut obj, "provider", entry.provider.as_ref().map(|s| s.as_str()));
        add_opt(&mut obj, "model", entry.model.as_ref().map(|s| s.as_str()));
        add_opt(&mut obj, "method", entry.method.as_ref().map(|s| s.as_str()));
        add_opt(&mut obj, "path", entry.path.as_ref().map(|s| s.as_str()));
        if let Some(code) = entry.status_code {
            obj["status_code"] = json!(code);
        }
        if let Some(body_val) = body {
            obj["body"] = body_val;
        }
        if body_truncated {
            obj["body_truncated"] = json!(true);
        }
        if let Some(v) = entry.duration_ms {
            obj["duration_ms"] = json!(v);
        }
        if let Some(v) = entry.ttft_ms {
            obj["ttft_ms"] = json!(v);
        }
        if let Some(v) = entry.tokens_in {
            obj["tokens_in"] = json!(v);
        }
        if let Some(v) = entry.tokens_out {
            obj["tokens_out"] = json!(v);
        }
        if let Some(v) = entry.error.as_ref().map(|s| s.as_str()) {
            obj["error"] = json!(v);
        }
        obj.to_string()
    }

    fn prune_old_shards(dir: &Path, retention_hours: u64) {
        let cutoff_ms = now_ms() - (retention_hours as i64) * 3600 * 1000;
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_jsonl = path.extension().map(|e| e == "jsonl").unwrap_or(false);
            if !is_jsonl {
                continue;
            }
            // Shard stem looks like `2026-11-03T19` — parse the hour prefix.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let hour = stem.split('-').next().unwrap_or_default();
            let parsed = parse_shard_ts(hour);
            if parsed > 0 && parsed < cutoff_ms {
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!(error = %e, path = %path.display(), "recorder: prune failed");
                }
            }
        }
    }
}

fn add_opt(obj: &mut Value, key: &str, value: Option<&str>) {
    if let Some(v) = value {
        obj[key] = json!(v);
    }
}

/// Hourly shard stem for a timestamp, e.g. `2026-11-03T19`.
fn shard_stem(ts_ms: i64) -> String {
    let secs = (ts_ms / 1000) as i64;
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let hour = rem / 3600;
    // Civil date from Unix days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}T{:02}", y, m, d, hour)
}

/// Parse a `YYYY-MM-DDTHH` shard stem into epoch ms; 0 when unparseable.
fn parse_shard_ts(stem: &str) -> i64 {
    // `YYYY-MM-DDTHH` → civil → unix.
    let bytes = stem.as_bytes();
    if bytes.len() < 13 {
        return 0;
    }
    let y = parse_num(&bytes[0..4]);
    let m = parse_num(&bytes[5..7]);
    let d = parse_num(&bytes[8..10]);
    let h = parse_num(&bytes[11..13]);
    if y < 1970 || !(1..=12).contains(&m) || !(1..=31).contains(&d) || h > 23 {
        return 0;
    }
    let days = days_from_civil(y, m, d);
    (days * 86400 + h as i64 * 3600) * 1000
}

fn parse_num(b: &[u8]) -> i64 {
    let mut v: i64 = 0;
    for byte in b {
        if !byte.is_ascii_digit() {
            return -1;
        }
        v = v * 10 + (byte - b'0') as i64;
    }
    v
}

/// Days since epoch for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_stem_and_parse_roundtrip() {
        let stem = shard_stem(1_731_088_800_000); // 2024-11-08T18:00:00Z
        assert_eq!(stem, "2024-11-08T18");
        let parsed = parse_shard_ts(&stem);
        assert!(parsed > 0);
        assert_eq!(shard_stem(parsed), stem);
    }

    #[test]
    fn test_record_writes_jsonl() {
        let dir = std::env::temp_dir().join(format!(
            "maxllm-recorder-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cfg = RecordingConfig {
            enabled: true,
            dir: dir.display().to_string(),
            max_body_bytes: 32,
            max_file_bytes: 4096,
            retention_hours: 24,
        };
        let rec = Recorder::new(&cfg).unwrap();
        rec.record(RecordEntry {
            event_type: EVENT_CLIENT_REQUEST,
            request_uuid: "r1".into(),
            session_id: Some("sess-1".into()),
            provider: Some("p1".into()),
            model: Some("m1".into()),
            method: Some("POST".into()),
            path: Some("/v1/responses".into()),
            status_code: Some(200),
            body: Some(br#"{"a":"b"}"#.to_vec()),
            duration_ms: Some(12),
            ..Default::default()
        });

        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .collect();
        assert_eq!(files.len(), 1);
        let content = fs::read_to_string(&files[0]).unwrap();
        assert!(content.contains("\"event_type\":\"client_request\""));
        assert!(content.contains("\"session_id\":\"sess-1\""));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_body_truncation_keeps_json_parseable() {
        let dir = std::env::temp_dir().join(format!(
            "maxllm-recorder-trunc-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cfg = RecordingConfig {
            enabled: true,
            dir: dir.display().to_string(),
            max_body_bytes: 16,
            max_file_bytes: 4096,
            retention_hours: 24,
        };
        let rec = Recorder::new(&cfg).unwrap();
        let big = format!("{{\"msg\":\"{}\"}}", "x".repeat(200));
        rec.record(RecordEntry {
            event_type: EVENT_CLIENT_RESPONSE,
            request_uuid: "r2".into(),
            body: Some(big.into_bytes()),
            ..Default::default()
        });
        let files: Vec<_> = fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(files.len(), 1);
        let content = fs::read_to_string(&files[0].path()).unwrap();
        assert!(content.contains("\"body_truncated\":true"));
        let _ = fs::remove_dir_all(&dir);
    }
}
