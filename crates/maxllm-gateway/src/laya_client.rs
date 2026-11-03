// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Laya System-One decision client.
//!
//! The gateway uses laya as the *last* resolver for rate-limit errors whose
//! reset time the deterministic layers could not pin down, and as the
//! authority on whether a 4xx body is a rate-limit error at all.
//!
//! Two question types are used:
//! * `noul` — "is this a rate-limit / quota error?" → a probability. Above
//!   the configured threshold the error is treated as a rate limit; below it
//!   the error takes the ordinary-failure path (no quota force-open).
//! * `choice` — "which of these candidate instants is the reset time?" →
//!   selects the true reset moment among the candidates the regex layer
//!   extracted, so a message that merely *mentions* several timestamps still
//!   resolves to the right one.
//!
//! Fail-open by design: an unreachable laya, a timeout or a malformed
//! answer falls back to the deterministic default instead of failing the
//! request. laya is an assistant to the recovery-time computation, never a
//! dependency of the failover path.

use std::time::Duration;

use serde_json::{json, Value};

/// Default decision-service origin (per the laya-decide skill).
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:18405";

/// Decision-service client.
#[derive(Debug, Clone)]
pub struct LayaClient {
    base_url: String,
    model: String,
    timeout: Duration,
}

/// What laya answered about one error observation.
#[derive(Debug, Clone)]
pub struct LayaVerdict {
    /// Probability that the error is a rate-limit / quota error.
    pub is_rate_limit: Option<f64>,
    /// The chosen reset instant (epoch ms) when laya picked a candidate.
    pub chosen_reset_at_ms: Option<u64>,
    /// Raw answers, for audit trails.
    #[allow(dead_code)]
    pub raw: Value,
}

impl LayaClient {
    pub fn new(base_url: Option<&str>, model: Option<&str>, timeout: Duration) -> Self {
        Self {
            base_url: base_url
                .map(|s| s.trim_end_matches('/').to_string())
                .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            model: model.unwrap_or("winwinwinbb/soft-decider-421m").to_string(),
            timeout,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Ask laya to classify an error and pick the reset instant.
    ///
    /// `candidates` are the reset instants the deterministic layer found in
    /// the message, as `(epoch_ms, raw_text)` pairs. When the list is
    /// non-empty, laya answers a `choice` question over it; when it is empty
    /// only the classification question is asked.
    pub async fn classify_rate_limit(
        &self,
        status: u16,
        message: &str,
        candidates: &[(u64, String)],
    ) -> Option<LayaVerdict> {
        let body = self.build_request_body(status, message, candidates);

        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .ok()?;

        let resp = client
            .post(format!("{}/v1/systemone", self.base_url))
            .json(&body)
            .send()
            .await
            .ok()?;

        if !resp.status().is_success() {
            return None;
        }
        let raw: Value = resp.json().await.ok()?;
        Some(self.parse_verdict(&raw, candidates))
    }

    /// Build the `/v1/systemone` request body.
    ///
    /// Two questions are asked:
    /// * `noul` — "is this a rate limit?" → a probability.
    /// * `choice` — "which of these instants is the reset?" → only when the
    ///   deterministic layer found candidates.
    ///
    /// The Jev protocol expects a choice question's `criteria` to be a *map*
    /// of option-id → option-meaning; the decider server rejects an array
    /// with a 500. The answer's `choice` is then the id (epoch ms) of the
    /// selected instant.
    fn build_request_body(
        &self,
        status: u16,
        message: &str,
        candidates: &[(u64, String)],
    ) -> Value {
        let mut questions = serde_json::Map::new();
        questions.insert(
            "is_rate_limit".to_string(),
            json!({
                "type": "noul",
                "instructions": "Is this error response a rate limit or quota exhaustion \
                 (a temporary limit that resets over time), as opposed to a permanent \
                 request error such as an invalid model, bad parameters, or an \
                 insufficient balance?",
            }),
        );

        if !candidates.is_empty() {
            let options: serde_json::Map<String, Value> = candidates
                .iter()
                .map(|(ms, raw)| (ms.to_string(), json!(raw)))
                .collect();
            questions.insert(
                "reset_time".to_string(),
                json!({
                    "type": "choice",
                    "instructions": "Which of these times is the moment when the rate limit \
                     or quota resets and the request may be retried? Choose the exact one \
                     the message states. If none of them is a reset time, choose the first.",
                    "criteria": Value::Object(options),
                }),
            );
        }

        json!({
            "model": self.model,
            "state": {
                "http_status": status,
                "error_message": message,
            },
            "questions": Value::Object(questions),
        })
    }

    /// Extract the typed answers from a Jev-compatible response.
    fn parse_verdict(&self, raw: &Value, candidates: &[(u64, String)]) -> LayaVerdict {
        let answers = raw.get("answers").and_then(|v| v.as_object());

        let is_rate_limit = answers
            .and_then(|a| a.get("is_rate_limit"))
            .and_then(|ans| {
                ans.get("noul")
                    .and_then(|v| v.as_f64())
                    .or_else(|| ans.get("score").and_then(|v| v.as_f64()))
            });

        let chosen_reset_at_ms = answers
            .and_then(|a| a.get("reset_time"))
            .and_then(|ans| ans.get("choice"))
            .and_then(|choice| {
                let id = match choice {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    _ => return None,
                };
                id.parse::<u64>().ok()
            })
            .or_else(|| {
                // Some servers answer a choice with the option's meaning
                // instead of its id; fall back to matching by raw text.
                let raw_text = answers
                    .and_then(|a| a.get("reset_time"))
                    .and_then(|ans| ans.get("choice"))
                    .and_then(|c| c.as_str())?;
                candidates
                    .iter()
                    .find(|(_, raw)| raw == raw_text)
                    .map(|(ms, _)| *ms)
            });

        LayaVerdict {
            is_rate_limit,
            chosen_reset_at_ms,
            raw: raw.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_noul_and_choice() {
        let client = LayaClient::new(None, None, Duration::from_secs(1));
        let raw = json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.91 },
                "reset_time": { "type": "choice", "choice": "1893456000000" }
            }
        });
        let candidates = vec![(1_893_456_000_000u64, "2029-01-01 00:00:00".to_string())];
        let v = client.parse_verdict(&raw, &candidates);
        assert_eq!(v.is_rate_limit, Some(0.91));
        assert_eq!(v.chosen_reset_at_ms, Some(1_893_456_000_000));
    }

    #[test]
    fn choice_by_meaning_fallback() {
        let client = LayaClient::new(None, None, Duration::from_secs(1));
        let raw = json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.8 },
                "reset_time": { "type": "choice", "choice": "2029-01-01 00:00:00" }
            }
        });
        let candidates = vec![(1_893_456_000_000u64, "2029-01-01 00:00:00".to_string())];
        let v = client.parse_verdict(&raw, &candidates);
        assert_eq!(v.chosen_reset_at_ms, Some(1_893_456_000_000));
    }

    #[test]
    fn choice_criteria_is_a_map_not_an_array() {
        // The decider server rejects an array-form `criteria` with a 500 —
        // the protocol wants a map of option-id → option-meaning.
        let client = LayaClient::new(None, None, Duration::from_secs(1));
        let candidates = vec![
            (1_893_456_000_000u64, "2029-01-01 00:00:00".to_string()),
            (1_893_456_060_000u64, "in 60s".to_string()),
        ];
        let body = client.build_request_body(429, "quota exceeded", &candidates);
        let criteria = &body["questions"]["reset_time"]["criteria"];
        assert!(criteria.is_object(), "criteria must be a JSON object");
        assert_eq!(criteria["1893456000000"], "2029-01-01 00:00:00");
        assert_eq!(criteria["1893456060000"], "in 60s");
        // The classification question is always present.
        assert_eq!(body["questions"]["is_rate_limit"]["type"], "noul");
        assert_eq!(body["state"]["http_status"], 429);
    }

    #[test]
    fn no_choice_question_without_candidates() {
        let client = LayaClient::new(None, None, Duration::from_secs(1));
        let body = client.build_request_body(500, "boom", &[]);
        assert!(body["questions"].get("reset_time").is_none());
        assert_eq!(body["questions"]["is_rate_limit"]["type"], "noul");
    }

    #[test]
    fn missing_answers_is_none() {
        let client = LayaClient::new(None, None, Duration::from_secs(1));
        let raw = json!({ "answers": {} });
        let v = client.parse_verdict(&raw, &[]);
        assert_eq!(v.is_rate_limit, None);
        assert_eq!(v.chosen_reset_at_ms, None);
    }
}
