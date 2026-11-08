// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Driving the Command Code CLI (`cmd`) in its non-interactive print mode.
//!
//! commandcode.ai serves two API surfaces:
//!
//! * `/provider/v1/*` — the OpenAI-compatible **Provider API**. It is a paid
//!   add-on: on the Go plan every call answers HTTP 403 `upgrade_required`.
//! * `/alpha/*` — the **CLI protocol** the `cmd` binary itself speaks. The
//!   Go plan covers it, but the vendor states that the endpoint serves the
//!   CLI only and that continued proxying of a subscription violates their
//!   terms.
//!
//! This bridge therefore never speaks the vendor's wire protocol itself. It
//! runs the vendor's own client — the `cmd` binary they ship and the user
//! already pays for — in the non-interactive mode the CLI documents for
//! automation (`cmd -p <query> --output-format json`), and translates that
//! output into the OpenAI Chat Completions contract.
//!
//! The print mode writes one JSON document per line (NDJSON):
//!
//! ```text
//! {"type":"event","event":{"type":"text_delta","delta":"Hi"}}
//! {"type":"event","event":{"type":"model_request_end","usage":{…},"stopReason":"stop"}}
//! {"type":"event","event":{"type":"run_end","result":{"finalText":"Hi",…}}}
//! {"type":"result","subtype":"success","finalText":"Hi","usage":{…}}
//! ```

use serde_json::Value;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;

/// One event of the CLI's print-mode event stream.
#[derive(Debug, Clone, PartialEq)]
pub enum CliEvent {
    /// A chunk of assistant text.
    TextDelta(String),
    /// Token usage reported when a model request finished.
    Usage {
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_write_tokens: u64,
    },
    /// The run ended; carries the final text when the CLI reported one.
    Finished {
        text: Option<String>,
        stop_reason: Option<String>,
    },
    /// The run failed.
    Error { message: String },
}

/// How the bridge invokes the CLI.
#[derive(Debug, Clone)]
pub struct CliConfig {
    /// Path to the `cmd` binary.
    pub bin: String,
    /// Model the bridge serves when the client does not name an allowed one.
    ///
    /// This is the id the client asks for and the id the answer reports. It
    /// is deliberately *not* the CLI's own model id: the CLI plan is a flat
    /// subscription, so the served id must not match a per-token price entry
    /// in the gateway's cost table (otherwise subscription traffic is
    /// booked as if it were metered).
    pub default_model: String,
    /// The model the CLI actually runs.
    pub cli_model: String,
    /// Models this bridge serves. Anything else is rejected before the CLI
    /// is started.
    pub allowed_models: Vec<String>,
    /// Directory the CLI runs in. It is a scratch directory: the agent must
    /// never see (or touch) a real workspace.
    pub workspace: PathBuf,
    /// Cap on the CLI's internal conversation turns.
    pub max_turns: u32,
    /// Default reasoning effort passed to the CLI, if any. A request may
    /// override it (`reasoning.effort` in the Responses contract).
    pub effort: Option<String>,
}

/// Parse one NDJSON line of the CLI's print-mode output.
///
/// Returns `None` for lines that carry no event we act on.
pub fn parse_event(line: &str) -> Option<CliEvent> {
    let value: Value = serde_json::from_str(line.trim()).ok()?;
    match value.get("type").and_then(Value::as_str) {
        Some("event") => parse_agent_event(value.get("event")?),
        Some("result") => parse_result_line(&value),
        _ => None,
    }
}

fn parse_agent_event(event: &Value) -> Option<CliEvent> {
    match event.get("type").and_then(Value::as_str) {
        Some("text_delta") => event
            .get("delta")
            .and_then(Value::as_str)
            .map(|d| CliEvent::TextDelta(d.to_string())),
        Some("model_request_end") => event.get("usage").map(parse_usage),
        Some("run_end") => {
            let result = event.get("result");
            Some(CliEvent::Finished {
                text: result
                    .and_then(|r| r.get("finalText"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                stop_reason: result
                    .and_then(|r| r.get("stopReason"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        }
        Some("run_error") => Some(CliEvent::Error {
            message: error_message(event),
        }),
        _ => None,
    }
}

fn parse_result_line(value: &Value) -> Option<CliEvent> {
    let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
    match subtype {
        "success" => Some(CliEvent::Finished {
            text: value
                .get("finalText")
                .and_then(Value::as_str)
                .map(str::to_string),
            stop_reason: value
                .get("stopReason")
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
        _ => Some(CliEvent::Error {
            message: value
                .get("error")
                .map(error_message)
                .unwrap_or_else(|| format!("the Command Code CLI reported `{subtype}`")),
        }),
    }
}

/// The CLI's usage object: `{inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens}`.
fn parse_usage(usage: &Value) -> CliEvent {
    let get = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    CliEvent::Usage {
        input_tokens: get("inputTokens"),
        output_tokens: get("outputTokens"),
        cache_read_tokens: get("cacheReadTokens"),
        cache_write_tokens: get("cacheWriteTokens"),
    }
}

fn error_message(value: &Value) -> String {
    for key in ["message", "error", "reason"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            return text.to_string();
        }
        if let Some(nested) = value
            .get(key)
            .and_then(|v| v.get("message"))
            .and_then(Value::as_str)
        {
            return nested.to_string();
        }
    }
    value.to_string()
}

/// Build the `cmd` invocation for one prompt.
///
/// The flags are the ones the CLI documents for automation:
///
/// * `-p` — non-interactive, print the answer and exit;
/// * `--output-format json` — the NDJSON event stream;
/// * `--no-session` — do not persist the conversation to disk;
/// * `--no-auto-update` — never let the CLI rewrite itself mid-request;
/// * `--skip-onboarding` — never prompt for taste onboarding;
/// * `--permission-mode dont-ask` — never prompt; anything that would need
///   approval is denied instead, so a headless run can never block;
/// * `--trust` — skip the initial project-trust prompt.
pub fn build_command(
    config: &CliConfig,
    prompt: &str,
    model: &str,
    effort: Option<&str>,
) -> Command {
    let mut command = Command::new(&config.bin);
    command
        .arg("-p")
        .arg(prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--no-session")
        .arg("--no-auto-update")
        .arg("--skip-onboarding")
        .arg("--trust")
        .arg("--permission-mode")
        .arg("dont-ask")
        .arg("--max-turns")
        .arg(config.max_turns.to_string())
        .arg("--model")
        .arg(model)
        .current_dir(&config.workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(effort) = effort {
        command.arg("--effort").arg(effort);
    }
    command.kill_on_drop(true);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_text_delta() {
        let line = r#"{"type":"event","event":{"type":"text_delta","delta":"PONG"}}"#;
        assert_eq!(
            parse_event(line),
            Some(CliEvent::TextDelta("PONG".to_string()))
        );
    }

    #[test]
    fn parses_usage_from_model_request_end() {
        let line = r#"{"type":"event","event":{"type":"model_request_end","model":"deepseek/deepseek-v4.1-flash","usage":{"inputTokens":291,"outputTokens":10,"cacheReadTokens":256,"cacheWriteTokens":0},"stopReason":"stop"}}"#;
        assert_eq!(
            parse_event(line),
            Some(CliEvent::Usage {
                input_tokens: 291,
                output_tokens: 10,
                cache_read_tokens: 256,
                cache_write_tokens: 0,
            })
        );
    }

    #[test]
    fn parses_the_run_end_event() {
        let line = r#"{"type":"event","event":{"type":"run_end","result":{"finalText":"PONG","stopReason":"end_turn","usage":{"inputTokens":5,"outputTokens":1}}}}"#;
        match parse_event(line) {
            Some(CliEvent::Finished { text, stop_reason }) => {
                assert_eq!(text.as_deref(), Some("PONG"));
                assert_eq!(stop_reason.as_deref(), Some("end_turn"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_the_result_line() {
        let line = r#"{"type":"result","subtype":"success","sessionId":"x","stopReason":"end_turn","usage":{"inputTokens":5,"outputTokens":1},"durationMs":42,"finalText":"PONG"}"#;
        match parse_event(line) {
            Some(CliEvent::Finished { text, .. }) => assert_eq!(text.as_deref(), Some("PONG")),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_a_failed_result_line_as_an_error() {
        let line =
            r#"{"type":"result","subtype":"error_max_turns","error":{"message":"too many turns"}}"#;
        match parse_event(line) {
            Some(CliEvent::Error { message }) => assert!(message.contains("too many turns")),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn ignores_unrelated_lines() {
        assert_eq!(parse_event("not json"), None);
        assert_eq!(
            parse_event(r#"{"type":"event","event":{"type":"turn_start"}}"#),
            None
        );
    }
}
