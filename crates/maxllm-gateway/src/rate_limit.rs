// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Rate-limit error classification and quota-reset-time resolution.
//!
//! Layered resolution, cheapest first:
//! 1. **Headers** (deterministic, zero network): `Retry-After`,
//!    `x-ratelimit-reset-requests`, `x-ratelimit-reset-tokens`,
//!    `x-ratelimit-reset`, `retry-after-ms`, `anthropic-ratelimit-*-reset`.
//! 2. **Learned patterns** (deterministic, zero network): a SQLite-backed
//!    fingerprint table that maps a normalised message shape to the parse
//!    result learned the first time that shape was seen — so after one
//!    successful parse, the same expression form never needs laya again.
//! 3. **Regex on body text** (deterministic, zero network): generic time
//!    expression matching (absolute timestamps in multiple locales, relative
//!    durations, epoch seconds), not hardcoded per-provider phrasings.
//! 4. **Laya decision** (one small HTTP call, ~30–200 ms): a typed `noul`
//!    question classifies whether the text is a rate-limit error at all, and a
//!    `choice` question selects the true reset instant from the candidates
//!    found by step 3. Fail-open: an unreachable or slow laya falls back to
//!    the deterministic default.
//!
//! If every layer agrees the error is **not** a rate-limit error, the caller
//! takes the ordinary-failure path (normal circuit-breaker accounting, no
//! quota force-open).

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};

/// The resolver's verdict for one upstream error observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// Confirmed quota/rate-limit error; the provider is unavailable until
    /// `reset_at_ms` (epoch milliseconds).
    QuotaUntil {
        reset_at_ms: u64,
        source: &'static str,
    },
    /// Confirmed quota/rate-limit error, but the reset instant is unknown —
    /// use the route's default cooldown.
    QuotaUnknown,
    /// Not a rate-limit error — ordinary upstream failure, no force-open.
    OrdinaryFailure,
}

/// A candidate reset instant extracted from the error text.
#[derive(Debug, Clone)]
pub struct ResetCandidate {
    /// Absolute epoch ms.
    pub at_ms: u64,
    /// The raw substring that produced this candidate (for laya's choice
    /// question and for audit trails).
    pub raw: String,
}

/// Parse rate-limit headers into an absolute epoch-ms reset instant.
///
/// Only the most trustworthy header wins — checked in order of increasing
/// imprecision (absolute timestamps before relative durations).
pub fn resolve_from_headers(headers: &http::HeaderMap) -> Option<(u64, &'static str)> {
    let now_ms = now_ms();

    // Absolute reset instants (most precise first).
    // RFC 3339 / ISO 8601 (Anthropic, some OpenRouter channels).
    for name in &[
        "anthropic-ratelimit-requests-reset",
        "anthropic-ratelimit-tokens-reset",
        "anthropic-ratelimit-unified-reset",
    ] {
        if let Some(v) = header_str(headers, name) {
            if let Some(ms) = parse_iso8601_ms(&v) {
                return Some((ms, "anthropic_reset_header"));
            }
        }
    }

    // Epoch seconds (OpenAI x-ratelimit-reset, GitHub-style).
    if let Some(v) = header_str(headers, "x-ratelimit-reset") {
        if let Some(ms) = parse_epoch_seconds_ms(&v) {
            return Some((ms, "x_ratelimit_reset"));
        }
    }

    // OpenAI / Azure durations: "1s", "6m0s", "1h2m3s", "120ms".
    for name in &["x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"] {
        if let Some(v) = header_str(headers, name) {
            if let Some(ms) = parse_duration_ms(&v) {
                return Some((now_ms + ms, name_static(name)));
            }
        }
    }

    // Milliseconds (Azure `x-ms-retry-after-ms`, OpenAI `retry-after-ms`).
    for name in &["retry-after-ms", "x-ms-retry-after-ms"] {
        if let Some(v) = header_str(headers, name) {
            if let Ok(ms) = v.trim().parse::<u64>() {
                return Some((now_ms + ms, "retry_after_ms"));
            }
        }
    }

    // Standard `Retry-After`: delay-seconds or HTTP-date.
    if let Some(v) = header_str(headers, "retry-after") {
        if let Ok(secs) = v.trim().parse::<u64>() {
            return Some((now_ms + secs * 1000, "retry_after_seconds"));
        }
        if let Some(ms) = parse_http_date_ms(&v) {
            return Some((ms, "retry_after_http_date"));
        }
    }

    // IETF draft `ratelimit-reset` (delay-seconds until reset).
    if let Some(v) = header_str(headers, "ratelimit-reset") {
        if let Ok(secs) = v.trim().parse::<u64>() {
            return Some((now_ms + secs * 1000, "ratelimit_reset"));
        }
    }

    None
}

fn name_static(name: &str) -> &'static str {
    match name {
        "x-ratelimit-reset-requests" => "x_ratelimit_reset_requests",
        "x-ratelimit-reset-tokens" => "x_ratelimit_reset_tokens",
        _ => "x_ratelimit_reset",
    }
}

fn header_str<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}

/// Parse the error body text for rate-limit patterns and reset candidates.
///
/// Returns `(is_rate_limit, candidates)` where `is_rate_limit` is `Some(true)`
/// when a deterministic rate-limit phrase was found, `Some(false)` when a
/// deterministic non-rate-limit phrase was found, `None` when inconclusive.
pub fn parse_body_text(text: &str) -> (Option<bool>, Vec<ResetCandidate>) {
    let lower = text.to_lowercase();

    // Quota / rate-limit signals (English + Chinese).
    let quota_phrases: &[&str] = &[
        "rate limit",
        "ratelimit",
        "rate-limit",
        "quota",
        "throttl",
        "too many requests",
        "使用上限",
        "限额",
        "额度",
        "已达到",
        "已用完",
        "配额",
        "限流",
        "超频",
    ];
    let has_quota_phrase = quota_phrases.iter().any(|p| lower.contains(p));

    // Absolute-time candidates: ISO 8601 / RFC 3339, "YYYY-MM-DD
    // HH:MM:SS ±HHMM|Z", date-only shapes — scanned positionally so a
    // timestamp embedded in prose (with or without a timezone suffix) is
    // found without depending on whitespace tokenisation.
    let mut candidates = find_absolute_times(text);

    // Relative durations: "in 30s", "resets in 5m30s", "12m50s", "5分钟".
    for m in regex_lite::find_durations(&lower) {
        candidates.push(ResetCandidate {
            at_ms: now_ms() + m,
            raw: format!("duration:{m}"),
        });
    }

    // Bare epoch seconds (10-digit numbers that look like a plausible future timestamp).
    for m in regex_lite::find_epoch_numbers(&lower) {
        // Epoch 10 digits (seconds) between 2020 and 2100.
        if (1577836800..=4102444800).contains(&m) {
            let at_ms = m * 1000;
            // Only keep if it's in the future (reset times are always future).
            if at_ms > now_ms() {
                candidates.push(ResetCandidate {
                    at_ms,
                    raw: format!("epoch:{m}"),
                });
            }
        }
    }

    let is_rate_limit = if has_quota_phrase {
        Some(true)
    } else if lower.contains("insufficient")
        || lower.contains("billing")
        || lower.contains("余额不足")
        || lower.contains("欠费")
    {
        // Billing / balance errors are often confused with quota — but they
        // are not reset-on-a-timer. Ordinary failure.
        Some(false)
    } else {
        None
    };

    // Deduplicate candidates by at_ms, keep the earliest (soonest recovery).
    candidates.sort_by_key(|c| c.at_ms);
    candidates.dedup_by(|a, b| a.at_ms == b.at_ms);

    (is_rate_limit, candidates)
}

/// Normalise a message text into a shape fingerprint: digits → `#`,
/// long alphanumerics → `<V>`, whitespace → single space. Two messages that
/// express the same quota rule share a fingerprint even when their values
/// differ — that's the learned-pattern key.
pub fn fingerprint(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            // Collapse runs of digits.
            while chars.peek().is_some_and(|n| n.is_ascii_digit()) {
                chars.next();
            }
            out.push('#');
        } else if c.is_alphanumeric() && c.is_ascii() {
            // Collapse long ASCII identifier runs (API keys, model names).
            let mut run = String::new();
            run.push(c);
            while chars
                .peek()
                .is_some_and(|n| n.is_alphanumeric() || *n == '-' || *n == '_')
            {
                run.push(chars.next().unwrap());
            }
            if run.len() > 12 {
                out.push_str("<V>");
            } else {
                // Digits inside a short run are values too ("12m50s" and
                // "5m0s" are the same shape) — collapse them as well.
                out.push_str(&collapse_digits(&run).to_lowercase());
            }
        } else if c.is_whitespace() {
            if !out.ends_with(' ') {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
    }
    out.trim().to_string()
}

/// Collapse every digit run inside `s` to a single `#`.
fn collapse_digits(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_digit = false;
    for c in s.chars() {
        let is_digit = c.is_ascii_digit();
        if is_digit {
            if !prev_digit {
                out.push('#');
            }
        } else {
            out.push(c);
        }
        prev_digit = is_digit;
    }
    out
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_iso8601_ms(s: &str) -> Option<u64> {
    DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis() as u64)
}

fn parse_epoch_seconds_ms(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok().map(|secs| secs * 1000)
}

fn parse_http_date_ms(s: &str) -> Option<u64> {
    DateTime::parse_from_rfc2822(s.trim())
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis() as u64)
}

/// Parse Go-style durations ("1h30m", "6m0s", "12ms", "45s") into milliseconds.
fn parse_duration_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total: u64 = 0;
    let mut num = String::new();
    let mut matched = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            i += 1;
            continue;
        }
        let val: f64 = num.parse().ok()?;
        num.clear();
        // `ms` must be tested before `m` — otherwise "120ms" reads as
        // 120 minutes and the trailing `s` breaks the parse.
        let factor = if c == 'm' && bytes.get(i + 1) == Some(&b's') {
            i += 1;
            1.0
        } else {
            match c {
                'h' => 3_600_000.0,
                'm' => 60_000.0,
                's' => 1_000.0,
                _ => return None,
            }
        };
        total += (val * factor) as u64;
        matched = true;
        i += 1;
    }
    if matched {
        Some(total)
    } else {
        None
    }
}

/// Scan text for absolute timestamps and return their epoch-ms instants.
///
/// Recognised shapes, cheapest first:
/// * `YYYY-MM-DDTHH:MM:SS(.fff)?(Z|±HH:MM|±HHMM)?` (RFC 3339 / ISO 8601)
/// * `YYYY-MM-DD HH:MM:SS(.fff)?(Z|±HH:MM|±HHMM)?` (the common prose form;
///   the `±HHMM` variant with a trailing zone name — `+0800 CST` — is the
///   Volcengine / ByteDance production shape that a whitespace-tokenised
///   scan misses)
/// * `YYYY-MM-DD` (midnight, interpreted as UTC)
///
/// Naive timestamps without a timezone are interpreted as UTC — a bounded
/// approximation that keeps the parse deterministic across deployment
/// timezones. The laya layer still double-checks the chosen candidate.
fn find_absolute_times(text: &str) -> Vec<ResetCandidate> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 10 <= bytes.len() {
        // The date shape always starts with `YYYY-MM-DD`.
        let is_date = bytes[i].is_ascii_digit()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
            && bytes[i + 4] == b'-'
            && bytes[i + 5].is_ascii_digit()
            && bytes[i + 6].is_ascii_digit()
            && bytes[i + 7] == b'-'
            && bytes[i + 8].is_ascii_digit()
            && bytes[i + 9].is_ascii_digit();
        if is_date {
            if let Some((at_ms, consumed, raw)) = parse_date_shape(&text[i..]) {
                out.push(ResetCandidate { at_ms, raw });
                i += consumed;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// Parse one absolute timestamp starting at the beginning of `s` (which
/// starts with the `YYYY-MM-DD` shape). Returns `(epoch_ms, consumed_bytes,
/// raw_text)`; `None` when the tail does not form a complete timestamp.
fn parse_date_shape(s: &str) -> Option<(u64, usize, String)> {
    let b = s.as_bytes();

    // Split the fixed date prefix.
    let date = &s[..10];
    let mut idx = 10;

    // Optional time: `T`, `t` or one space separates the date from it.
    let mut time = String::new();
    let mut has_time = false;
    if idx < b.len() && (b[idx] == b'T' || b[idx] == b't' || b[idx] == b' ') {
        let sep_len = if b[idx] == b' ' { 1 } else { 1 };
        let t = &s[idx + sep_len..];
        if let Some((parsed, used)) = parse_time_shape(t) {
            time = parsed;
            idx += sep_len + used;
            has_time = true;
        }
    }

    // A date-only shape is midnight of that day.
    if !has_time {
        let dt =
            NaiveDateTime::parse_from_str(&format!("{date} 00:00:00"), "%Y-%m-%d %H:%M:%S").ok()?;
        return Some((dt.and_utc().timestamp_millis() as u64, 10, date.to_string()));
    }

    // Optional timezone suffix. A zone may be separated from the time by a
    // space (`23:59:59 +0800 CST`) — the production Volcengine shape.
    let mut tz = String::new();
    let mut tz_start = idx;
    if tz_start < b.len() && b[tz_start] == b' ' {
        tz_start += 1;
    }
    if tz_start < b.len() {
        let first = b[tz_start];
        if first == b'Z' || first == b'z' {
            tz = "Z".to_string();
            idx = tz_start + 1;
        } else if first == b'+' || first == b'-' {
            // `±HH`, `±HHMM` or `±HH:MM` — the compact `+0800` form is the
            // production Volcengine shape (`+0800 CST`).
            let sign = &s[tz_start..tz_start + 1];
            let mut hh = "";
            let mut mm = "";
            let mut k = tz_start;
            if tz_start + 3 <= b.len()
                && b[tz_start + 1].is_ascii_digit()
                && b[tz_start + 2].is_ascii_digit()
            {
                hh = &s[tz_start + 1..tz_start + 3];
                k = tz_start + 3;
                if k < b.len() && b[k] == b':' {
                    let m = k + 1;
                    if m + 2 <= b.len() && b[m].is_ascii_digit() && b[m + 1].is_ascii_digit() {
                        mm = &s[m..m + 2];
                        k = m + 2;
                    }
                } else if k + 2 <= b.len() && b[k].is_ascii_digit() && b[k + 1].is_ascii_digit() {
                    mm = &s[k..k + 2];
                    k += 2;
                }
            }
            if !hh.is_empty() {
                // Normalise to the RFC 3339 `±HH:MM` form chrono's strict
                // parser requires.
                let canonical_mm = if mm.is_empty() { "00" } else { mm };
                tz = format!("{sign}{hh}:{canonical_mm}");
                idx = k;
            }
        }
    }

    // Assemble a canonical timestamp and parse it.
    let canonical = format!("{date}T{time}");
    let dt = if tz.is_empty() {
        // Naive local time — interpret as UTC (deterministic approximation).
        NaiveDateTime::parse_from_str(&canonical, "%Y-%m-%dT%H:%M:%S")
            .ok()?
            .and_utc()
    } else {
        DateTime::parse_from_rfc3339(&format!("{canonical}{tz}"))
            .ok()?
            .with_timezone(&Utc)
    };
    let raw = s[..idx].trim_end().to_string();
    Some((dt.timestamp_millis() as u64, idx, raw))
}

/// Parse `HH:MM`, `HH:MM:SS` or `HH:MM:SS.fff` from the start of `s`.
/// Returns `(canonical_time, consumed_bytes)`; `None` when the tail does
/// not form a complete time.
fn parse_time_shape(s: &str) -> Option<(String, usize)> {
    let b = s.as_bytes();
    // HH:MM — the mandatory minimum shape.
    if b.len() < 5
        || !b[0].is_ascii_digit()
        || !b[1].is_ascii_digit()
        || b[2] != b':'
        || !b[3].is_ascii_digit()
        || !b[4].is_ascii_digit()
    {
        return None;
    }
    let hh: u32 = s[0..2].parse().ok()?;
    let mm: u32 = s[3..5].parse().ok()?;
    if hh > 23 || mm > 59 {
        return None;
    }
    let mut time = format!("{:02}:{:02}", hh, mm);
    let mut idx = 5;

    // Optional seconds. `<=` (not `<`): a timestamp may end exactly at the
    // end of the message ("…resets at 2099-06-15 12:00:00").
    if idx + 1 < b.len() && b[idx] == b':' && b[idx + 1].is_ascii_digit() {
        if idx + 3 <= b.len() && b[idx + 2].is_ascii_digit() {
            let ss: u32 = s[idx + 1..idx + 3].parse().ok()?;
            if ss > 59 {
                return None;
            }
            time.push_str(&format!(":{:02}", ss));
            idx += 3;

            // Optional fractional seconds.
            if idx < b.len() && b[idx] == b'.' {
                let frac_start = idx + 1;
                let mut frac_end = frac_start;
                while frac_end < b.len() && b[frac_end].is_ascii_digit() {
                    frac_end += 1;
                }
                if frac_end > frac_start {
                    let frac = &s[frac_start..frac_end.min(frac_start + 3)];
                    time.push('.');
                    time.push_str(frac);
                    idx = frac_end;
                }
            }
        }
    }
    Some((time, idx))
}

/// Tiny regex helpers — avoids pulling the `regex` crate (already vendored
/// `regex-lite` via deps; if unavailable, falls back to scanning).
mod regex_lite {
    /// Find duration patterns like "in 30s", "resets in 5m30s", "12m50s",
    /// "5分钟" (treated as 5 minutes), "3小时".
    pub fn find_durations(text: &str) -> Vec<u64> {
        let mut out = Vec::new();
        let bytes = text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            // Scan for a digit start.
            if bytes[i].is_ascii_digit() {
                let start = i;
                let mut total_secs: u64 = 0;
                let mut j = i;
                let mut found = false;
                // Parse sequences like 1h30m, 5m30s, 12ms.
                while j < bytes.len() {
                    let mut num = 0u64;
                    let num_start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        num = num * 10 + (bytes[j] - b'0') as u64;
                        j += 1;
                    }
                    if j == num_start {
                        break;
                    } // no digits, stop
                      // Look for a unit suffix.
                    let unit = if j + 1 < bytes.len() && bytes[j] == b'm' && bytes[j + 1] == b's' {
                        j += 2;
                        0u64 // milliseconds
                    } else if j < bytes.len() && bytes[j] == b'h' {
                        j += 1;
                        3600u64
                    } else if j < bytes.len() && bytes[j] == b'm' {
                        j += 1;
                        60u64
                    } else if j < bytes.len() && bytes[j] == b's' {
                        j += 1;
                        1u64
                    } else {
                        break; // digits without a unit — not a duration
                    };
                    total_secs += num * unit;
                    found = true;
                }
                if found && total_secs > 0 && total_secs < 7 * 24 * 3600 {
                    out.push(total_secs * 1000);
                }
                i = if j > start { j } else { start + 1 };
            } else {
                i += 1;
            }
        }
        out
    }

    /// Find 10-digit numbers that look like epoch-seconds timestamps.
    pub fn find_epoch_numbers(text: &str) -> Vec<u64> {
        let mut out = Vec::new();
        let bytes = text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i].is_ascii_digit() {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let len = i - start;
                if len == 10 {
                    if let Ok(v) = text[start..i].parse::<u64>() {
                        out.push(v);
                    }
                }
            } else {
                i += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut m = http::HeaderMap::new();
        for (k, v) in pairs {
            m.insert(
                http::HeaderName::from_lowercase(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        m
    }

    #[test]
    fn retry_after_seconds() {
        let h = headers(&[("retry-after", "120")]);
        let (ms, src) = resolve_from_headers(&h).unwrap();
        assert_eq!(src, "retry_after_seconds");
        assert!(ms > now_ms() + 119_000 && ms <= now_ms() + 121_000);
    }

    #[test]
    fn retry_after_http_date() {
        let future = Utc::now() + chrono::Duration::seconds(30);
        let h = headers(&[("retry-after", &future.to_rfc2822())]);
        let (ms, src) = resolve_from_headers(&h).unwrap();
        assert_eq!(src, "retry_after_http_date");
        assert!(ms > now_ms());
    }

    #[test]
    fn x_ratelimit_reset_requests_duration() {
        let h = headers(&[("x-ratelimit-reset-requests", "6m0s")]);
        let (ms, src) = resolve_from_headers(&h).unwrap();
        assert_eq!(src, "x_ratelimit_reset_requests");
        assert!(ms >= now_ms() + 359_000);
    }

    #[test]
    fn anthropic_iso_reset() {
        let future = (Utc::now() + chrono::Duration::seconds(60))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let h = headers(&[("anthropic-ratelimit-requests-reset", &future)]);
        let (ms, src) = resolve_from_headers(&h).unwrap();
        assert_eq!(src, "anthropic_reset_header");
        assert!(ms > now_ms());
    }

    #[test]
    fn no_headers_no_reset() {
        let h = headers(&[("content-type", "application/json")]);
        assert!(resolve_from_headers(&h).is_none());
    }

    #[test]
    fn body_chinese_reset_time() {
        let (is_rl, cands) = parse_body_text("配额已用完，将在 2099-01-01 00:00:00 重置");
        assert_eq!(is_rl, Some(true));
        assert!(!cands.is_empty());
        assert!(cands[0].at_ms > now_ms());
    }

    #[test]
    fn body_datetime_with_numeric_offset_and_zone_name() {
        // The production Volcengine shape: "…It will reset at 2026-11-06
        // 23:59:59 +0800 CST…" — a whitespace-tokenised scan splits the
        // timestamp into pieces and misses it entirely.
        let (is_rl, cands) = parse_body_text(
            "You have exceeded the monthly usage quota. It will reset at 2099-11-06 23:59:59 +0800 CST. Request id: 021793713952",
        );
        assert_eq!(is_rl, Some(true));
        assert_eq!(cands.len(), 1, "exactly one reset instant");
        // 2099-11-06 23:59:59 +0800 = 2099-11-06 15:59:59 UTC.
        let expect_utc = chrono::NaiveDate::from_ymd_opt(2099, 11, 6)
            .unwrap()
            .and_hms_opt(15, 59, 59)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64;
        assert_eq!(cands[0].at_ms, expect_utc);
        assert_eq!(cands[0].raw, "2099-11-06 23:59:59 +0800");
    }

    #[test]
    fn body_datetime_rfc3339_with_zone() {
        let (is_rl, cands) = parse_body_text("Quota resets at 2099-03-01T12:30:45+08:00 exactly");
        assert_eq!(is_rl, Some(true));
        assert_eq!(cands.len(), 1);
        let expect_utc = chrono::NaiveDate::from_ymd_opt(2099, 3, 1)
            .unwrap()
            .and_hms_opt(4, 30, 45)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64;
        assert_eq!(cands[0].at_ms, expect_utc);
    }

    #[test]
    fn body_datetime_without_zone_is_utc() {
        let (is_rl, cands) = parse_body_text("Rate limit resets at 2099-06-15 12:00:00 (UTC)");
        assert_eq!(is_rl, Some(true));
        let expect_utc = chrono::NaiveDate::from_ymd_opt(2099, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64;
        assert_eq!(cands[0].at_ms, expect_utc);
    }

    #[test]
    fn body_iso_with_fraction_and_z() {
        let (is_rl, cands) =
            parse_body_text("Rate limit reached. Retry after 2099-08-08T10:20:30.123Z");
        assert_eq!(is_rl, Some(true));
        assert_eq!(cands.len(), 1);
        let expect_utc = chrono::NaiveDate::from_ymd_opt(2099, 8, 8)
            .unwrap()
            .and_hms_opt(10, 20, 30)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64
            + 123;
        assert_eq!(cands[0].at_ms, expect_utc);
    }

    #[test]
    fn body_english_reset_time() {
        let (is_rl, cands) =
            parse_body_text("Rate limit exceeded. Quota resets at 2099-06-15 12:00:00");
        assert_eq!(is_rl, Some(true));
        assert!(!cands.is_empty());
    }

    #[test]
    fn body_duration_relative() {
        let (is_rl, cands) = parse_body_text("Too many requests, please try again in 30s");
        assert_eq!(is_rl, Some(true));
        assert!(cands
            .iter()
            .any(|c| c.at_ms > now_ms() + 25_000 && c.at_ms < now_ms() + 35_000));
    }

    #[test]
    fn body_not_rate_limit() {
        let (is_rl, _) = parse_body_text("Insufficient balance: your account has 余额不足");
        assert_eq!(is_rl, Some(false));
    }

    #[test]
    fn body_inconclusive() {
        let (is_rl, _) = parse_body_text("Internal server error");
        assert_eq!(is_rl, None);
    }

    #[test]
    fn fingerprint_stable_across_values() {
        let a = fingerprint(
            "Rate limit reached for tp-abc123456789xyz. Limit: 20 RPM. Please try again in 12m50s.",
        );
        let b = fingerprint(
            "Rate limit reached for tp-xyz987654321abc. Limit: 30 RPM. Please try again in 5m0s.",
        );
        assert_eq!(a, b, "same shape must share a fingerprint");
    }

    #[test]
    fn fingerprint_differs_across_shapes() {
        let a = fingerprint("Rate limit exceeded. Please try again in 30s.");
        let b = fingerprint("Model not found: gpt-4o");
        assert_ne!(a, b);
    }

    #[test]
    fn duration_parse_go_style() {
        assert_eq!(parse_duration_ms("6m0s"), Some(360_000));
        assert_eq!(parse_duration_ms("1h30m"), Some(5_400_000));
        assert_eq!(parse_duration_ms("120ms"), Some(120));
        assert_eq!(parse_duration_ms("45s"), Some(45_000));
        assert_eq!(parse_duration_ms("not-a-duration"), None);
    }
}
