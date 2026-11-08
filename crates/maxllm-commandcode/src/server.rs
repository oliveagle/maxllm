// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The bridge's HTTP surface: an OpenAI Chat Completions endpoint in front
//! of the Command Code CLI.

use crate::cli::{self, CliConfig, CliEvent};
use crate::protocol::{self, ChatRequest};
use crate::responses;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{channel::Channel, BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::ChildStdout;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

pub type BridgeBody = BoxBody<Bytes, Infallible>;

/// Everything the request handler needs.
#[derive(Debug)]
pub struct State {
    pub cli: CliConfig,
    /// Wall-clock budget for one CLI run.
    pub timeout: Duration,
}

/// Handle one request.
pub async fn handle(state: Arc<State>, request: Request<Incoming>) -> Response<BridgeBody> {
    let path = request.uri().path().to_string();
    match (request.method().as_str(), path.as_str()) {
        ("GET", "/health") => json_response(StatusCode::OK, json!({"status": "ok"})),
        ("GET", "/v1/models") => models_response(state.clone()),
        ("POST", "/v1/chat/completions") => chat(state.clone(), request).await,
        ("POST", "/v1/responses") => responses_endpoint(state.clone(), request).await,
        ("GET", "/") => json_response(
            StatusCode::OK,
            json!({
                "service": "maxllm-commandcode",
                "backend": "Command Code CLI (cmd -p, non-interactive print mode)",
                "endpoints": ["/v1/responses", "/v1/chat/completions", "/v1/models", "/health"],
            }),
        ),
        _ => json_response(
            StatusCode::NOT_FOUND,
            protocol::error("unknown endpoint", "not_found", "invalid_request_error"),
        ),
    }
}

/// The models this bridge serves.
fn models_response(state: Arc<State>) -> Response<BridgeBody> {
    let created = protocol::now_secs();
    let data: Vec<Value> = state
        .cli
        .allowed_models
        .iter()
        .map(|model| {
            json!({
                "id": model,
                "object": "model",
                "created": created,
                "owned_by": "command-code",
            })
        })
        .collect();
    json_response(StatusCode::OK, json!({"object": "list", "data": data}))
}

fn json_response(status: StatusCode, body: Value) -> Response<BridgeBody> {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(bytes)).boxed())
        .unwrap()
}

/// A model the bridge does not serve.
struct UnknownModel(String);

/// Resolve the model to run: the client's choice when it is allowed, the
/// default otherwise. A model the bridge does not serve is a 404.
fn resolve_model(state: &State, requested: Option<&str>) -> Result<String, UnknownModel> {
    match requested {
        None | Some("") => Ok(state.cli.default_model.clone()),
        Some(model) => {
            if state.cli.allowed_models.iter().any(|m| m == model) {
                Ok(model.to_string())
            } else {
                Err(UnknownModel(format!(
                    "model `{model}` is not served by this bridge; served models: {}",
                    state.cli.allowed_models.join(", ")
                )))
            }
        }
    }
}

/// Serve one chat completions request.
async fn chat(state: Arc<State>, request: Request<Incoming>) -> Response<BridgeBody> {
    let body = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                protocol::error(
                    &format!("cannot read request body: {err}"),
                    "bad_request",
                    "invalid_request_error",
                ),
            )
        }
    };

    let parsed: ChatRequest = match serde_json::from_slice(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                protocol::error(
                    &format!("invalid chat request: {err}"),
                    "bad_request",
                    "invalid_request_error",
                ),
            )
        }
    };

    if parsed.messages.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            protocol::error(
                "`messages` must not be empty",
                "bad_request",
                "invalid_request_error",
            ),
        );
    }

    let model = match resolve_model(&state, parsed.model.as_deref()) {
        Ok(model) => model,
        Err(err) => {
            return json_response(
                StatusCode::NOT_FOUND,
                protocol::error(&err.0, "model_not_found", "invalid_request_error"),
            )
        }
    };

    let prompt = protocol::build_prompt(&parsed.messages);
    let id = protocol::completion_id();
    let effort = state.cli.effort.clone();

    if parsed.stream {
        stream_response(state, id, model, prompt, effort)
    } else {
        buffered_response(state, id, model, prompt, effort).await
    }
}

/// A streaming chat completion: the CLI's NDJSON events become chat SSE.
fn stream_response(
    state: Arc<State>,
    id: String,
    model: String,
    prompt: String,
    effort: Option<String>,
) -> Response<BridgeBody> {
    let (mut sender, body) = Channel::<Bytes, Infallible>::new(16);

    tokio::spawn(async move {
        let role = protocol::chunk(
            &id,
            &model,
            json!({"role": "assistant", "content": ""}),
            None,
            None,
        );
        if sender.send_data(Bytes::from(role)).await.is_err() {
            return;
        }

        let (events, mut rx) = unbounded_channel::<CliEvent>();
        let runner = tokio::spawn({
            let state = state.clone();
            let prompt = prompt.clone();
            let effort = effort.clone();
            async move { run_cli(&state, &prompt, effort, events).await }
        });

        let mut usage = protocol::Usage::default();
        let mut cached_tokens = 0u64;
        let mut stop_reason = "stop".to_string();

        while let Some(event) = rx.recv().await {
            match event {
                CliEvent::TextDelta(text) => {
                    let chunk = protocol::chunk(&id, &model, json!({"content": text}), None, None);
                    if sender.send_data(Bytes::from(chunk)).await.is_err() {
                        break;
                    }
                }
                CliEvent::Usage {
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    ..
                } => {
                    usage = protocol::Usage {
                        prompt_tokens: input_tokens,
                        completion_tokens: output_tokens,
                        total_tokens: input_tokens + output_tokens,
                    };
                    cached_tokens = cache_read_tokens;
                }
                CliEvent::Finished {
                    stop_reason: reason,
                    ..
                } => {
                    if let Some(reason) = reason {
                        stop_reason = match reason.as_str() {
                            "tool_calls" | "tool_use" => "tool_calls".to_string(),
                            "max_tokens" | "length" => "length".to_string(),
                            _ => "stop".to_string(),
                        };
                    }
                }
                CliEvent::Error { message } => {
                    let error = protocol::error(&message, "upstream_error", "server_error");
                    let _ = sender
                        .send_data(Bytes::from(format!("data: {error}\n\n")))
                        .await;
                }
            }
        }

        let _ = runner.await;

        let final_chunk = protocol::chunk(
            &id,
            &model,
            json!({}),
            Some(&stop_reason),
            Some(serde_json::to_value(usage.details(cached_tokens)).unwrap_or(Value::Null)),
        );
        let _ = sender.send_data(Bytes::from(final_chunk)).await;
        let _ = sender
            .send_data(Bytes::from_static(b"data: [DONE]\n\n"))
            .await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(body.boxed())
        .unwrap()
}

/// The outcome of one CLI run.
struct RunOutcome {
    text: String,
    usage: protocol::Usage,
    cached_tokens: u64,
    failure: Option<String>,
}

/// Run the CLI once and collect its answer.
async fn collect_run(state: Arc<State>, prompt: &str, effort: Option<String>) -> RunOutcome {
    let (events, mut rx) = unbounded_channel::<CliEvent>();
    let runner = tokio::spawn({
        let state = state.clone();
        let prompt = prompt.to_string();
        async move { run_cli(&state, &prompt, effort, events).await }
    });

    let mut outcome = RunOutcome {
        text: String::new(),
        usage: protocol::Usage::default(),
        cached_tokens: 0,
        failure: None,
    };

    while let Some(event) = rx.recv().await {
        match event {
            CliEvent::TextDelta(delta) => outcome.text.push_str(&delta),
            CliEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                ..
            } => {
                outcome.usage = protocol::Usage {
                    prompt_tokens: input_tokens,
                    completion_tokens: output_tokens,
                    total_tokens: input_tokens + output_tokens,
                };
                outcome.cached_tokens = cache_read_tokens;
            }
            CliEvent::Error { message } => outcome.failure = Some(message),
            _ => {}
        }
    }

    if let Err(err) = runner.await {
        outcome.failure = Some(format!("the Command Code CLI run failed: {err}"));
    }

    outcome
}

/// The buffered (non-streaming) chat completion.
async fn buffered_response(
    state: Arc<State>,
    id: String,
    model: String,
    prompt: String,
    effort: Option<String>,
) -> Response<BridgeBody> {
    let outcome = collect_run(state.clone(), &prompt, effort).await;
    let RunOutcome {
        text,
        usage,
        cached_tokens,
        failure,
    } = outcome;

    match failure {
        Some(message) => json_response(
            StatusCode::BAD_GATEWAY,
            protocol::error(&message, "upstream_error", "server_error"),
        ),
        None => {
            let body = protocol::completion(
                &id,
                &model,
                &text,
                "stop",
                serde_json::to_value(usage.details(cached_tokens)).unwrap_or(Value::Null),
            );
            json_response(StatusCode::OK, body)
        }
    }
}

/// Run the CLI once and publish its events.
///
/// Every failure mode — the binary missing, a timeout, a non-zero exit — is
/// published as an error event, so both consumers see the same thing.
async fn run_cli(
    state: &State,
    prompt: &str,
    effort: Option<String>,
    events: UnboundedSender<CliEvent>,
) -> Result<(), String> {
    // The CLI runs its own model id; the served id only labels the answer.
    let cli_model = state.cli.cli_model.clone();
    let mut command = cli::build_command(&state.cli, prompt, &cli_model, effort.as_deref());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let message = format!("cannot start the Command Code CLI: {err}");
            let _ = events.send(CliEvent::Error {
                message: message.clone(),
            });
            return Err(message);
        }
    };

    let stdout: ChildStdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let message = "the Command Code CLI produced no output".to_string();
            let _ = events.send(CliEvent::Error {
                message: message.clone(),
            });
            return Err(message);
        }
    };

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let mut failed = false;

    let read = async {
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .await
                .map_err(|err| format!("cannot read the Command Code CLI output: {err}"))?;
            if read == 0 {
                break;
            }
            if let Some(event) = cli::parse_event(&line) {
                if matches!(event, CliEvent::Error { .. }) {
                    failed = true;
                }
                if events.send(event).is_err() {
                    break;
                }
            }
        }
        Ok::<(), String>(())
    };

    let read = tokio::time::timeout(state.timeout, read).await;
    if !matches!(read, Ok(Ok(()))) {
        let _ = child.kill().await;
        let message = match read {
            // The CLI's output could not be read.
            Ok(Err(message)) => message,
            // The CLI ran past its budget.
            Err(_) => format!(
                "the Command Code CLI did not finish within {} seconds",
                state.timeout.as_secs()
            ),
            // The read succeeded; something else went wrong.
            Ok(Ok(())) => "the Command Code CLI run failed".to_string(),
        };
        let _ = events.send(CliEvent::Error {
            message: message.clone(),
        });
        return Err(message);
    }

    let status = child.wait().await;

    if !failed {
        if let Ok(status) = status {
            if !status.success() {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr).await;
                }
                let detail = stderr.trim();
                let message = if detail.is_empty() {
                    format!("the Command Code CLI exited with {status}")
                } else {
                    format!("the Command Code CLI failed: {detail}")
                };
                let _ = events.send(CliEvent::Error {
                    message: message.clone(),
                });
                return Err(message);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// The Responses contract
// ---------------------------------------------------------------------------

/// Serve one Responses request.
async fn responses_endpoint(state: Arc<State>, request: Request<Incoming>) -> Response<BridgeBody> {
    let body = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                responses::error(
                    &format!("cannot read request body: {err}"),
                    "bad_request",
                    "invalid_request_error",
                ),
            )
        }
    };

    let parsed: responses::ResponsesRequest = match serde_json::from_slice(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                responses::error(
                    &format!("invalid responses request: {err}"),
                    "bad_request",
                    "invalid_request_error",
                ),
            )
        }
    };

    let model = match resolve_model(&state, parsed.model.as_deref()) {
        Ok(model) => model,
        Err(err) => {
            return json_response(
                StatusCode::NOT_FOUND,
                responses::error(&err.0, "model_not_found", "invalid_request_error"),
            )
        }
    };

    let prompt = match responses::build_prompt(&parsed) {
        Ok(prompt) => prompt,
        Err(message) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                responses::error(&message, "bad_request", "invalid_request_error"),
            )
        }
    };

    if parsed.tools.as_ref().is_some_and(|tools| !tools.is_null()) {
        tracing::warn!(
            "a Responses request carried tools; the Command Code CLI has no function-calling channel, so they are ignored"
        );
    }

    let effort = parsed.effort().or_else(|| state.cli.effort.clone());

    if parsed.stream {
        responses_stream_response(state, model, prompt, effort)
    } else {
        responses_buffered_response(state, model, prompt, effort).await
    }
}

/// A streaming Responses answer: the CLI's NDJSON events become the
/// `response.*` SSE event family.
fn responses_stream_response(
    state: Arc<State>,
    model: String,
    prompt: String,
    effort: Option<String>,
) -> Response<BridgeBody> {
    let (mut sender, body) = Channel::<Bytes, Infallible>::new(16);

    tokio::spawn(async move {
        let mut stream = responses::Stream::new(&model);
        if sender.send_data(Bytes::from(stream.start())).await.is_err() {
            return;
        }

        let (events, mut rx) = unbounded_channel::<CliEvent>();
        let runner = tokio::spawn({
            let state = state.clone();
            let prompt = prompt.clone();
            let effort = effort.clone();
            async move { run_cli(&state, &prompt, effort, events).await }
        });

        let mut usage = protocol::Usage::default();
        let mut cached_tokens = 0u64;
        let mut failure: Option<String> = None;

        while let Some(event) = rx.recv().await {
            match event {
                CliEvent::TextDelta(text) => {
                    if sender
                        .send_data(Bytes::from(stream.text_delta(&text)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                CliEvent::Usage {
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    ..
                } => {
                    usage = protocol::Usage {
                        prompt_tokens: input_tokens,
                        completion_tokens: output_tokens,
                        total_tokens: input_tokens + output_tokens,
                    };
                    cached_tokens = cache_read_tokens;
                }
                CliEvent::Error { message } => failure = Some(message),
                _ => {}
            }
        }

        let _ = runner.await;

        match failure {
            Some(message) => {
                let failed = stream.fail(&message, "upstream_error");
                let _ = sender.send_data(Bytes::from(failed)).await;
            }
            None => {
                let finished = stream.finish(responses::usage(
                    usage.prompt_tokens,
                    cached_tokens,
                    usage.completion_tokens,
                ));
                let _ = sender.send_data(Bytes::from(finished)).await;
            }
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(body.boxed())
        .unwrap()
}

/// The buffered (non-streaming) Responses answer.
async fn responses_buffered_response(
    state: Arc<State>,
    model: String,
    prompt: String,
    effort: Option<String>,
) -> Response<BridgeBody> {
    let outcome = collect_run(state.clone(), &prompt, effort).await;
    let RunOutcome {
        text,
        usage,
        cached_tokens,
        failure,
    } = outcome;

    let response = responses::response_object(
        &responses::generate_id("resp_"),
        &model,
        responses::now_secs(),
        if failure.is_some() {
            "failed"
        } else {
            "completed"
        },
        if failure.is_some() {
            Vec::new()
        } else {
            vec![responses::message_item(
                &responses::generate_id("msg_"),
                &text,
                "completed",
            )]
        },
        responses::usage(usage.prompt_tokens, cached_tokens, usage.completion_tokens),
        match &failure {
            Some(message) => json!({ "code": "upstream_error", "message": message }),
            None => Value::Null,
        },
    );

    json_response(StatusCode::OK, response)
}
