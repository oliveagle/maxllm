// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Layer-1 control API.
//!
//! Kept on a separate loopback listener because these endpoints can replace
//! the worker binary and rewrite the route table. The public listener never
//! exposes this surface.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use maxllm_config::SupervisorRouteConfig;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::state::Supervisor;

pub async fn serve(sup: Arc<Supervisor>, listener: TcpListener) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    tracing::info!(%addr, "layer-1 control API listening");
    loop {
        let (stream, _peer) = listener.accept().await?;
        let sup = sup.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req| {
                let sup = sup.clone();
                async move { Ok::<_, Infallible>(handle(sup, req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                tracing::debug!(error = %e, "control connection closed");
            }
        });
    }
}

async fn handle(sup: Arc<Supervisor>, req: Request<Incoming>) -> Response<Full<Bytes>> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    match (method, path.as_str()) {
        (Method::GET, "/healthz") => json(
            StatusCode::OK,
            serde_json::json!({"status": "ok", "layer": 1, "role": "supervisor"}),
        ),
        (Method::GET, "/status") | (Method::GET, "/") => json(StatusCode::OK, sup.status_json()),
        (Method::GET, "/routes") => {
            let table = sup.routes.load();
            json(
                StatusCode::OK,
                serde_json::json!({
                    "routes": table.routes.iter().map(|r| serde_json::json!({
                        "path": r.path,
                        "backend": r.backend,
                    })).collect::<Vec<_>>(),
                }),
            )
        }
        (Method::PUT, "/routes") | (Method::POST, "/routes") => {
            let body = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(e) => return error(StatusCode::BAD_REQUEST, format!("cannot read body: {e}")),
            };
            let value: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")),
            };
            let routes_value = if value.is_array() {
                value
            } else {
                value.get("routes").cloned().unwrap_or_default()
            };
            let routes: Vec<SupervisorRouteConfig> = match serde_json::from_value(routes_value) {
                Ok(r) => r,
                Err(e) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid routes array: {e}"),
                    )
                }
            };
            let sup_task = sup.clone();
            match tokio::task::spawn_blocking(move || sup_task.set_overlay_routes(routes)).await {
                Ok(Ok(())) => {
                    let table = sup.routes.load();
                    json(
                        StatusCode::OK,
                        serde_json::json!({
                            "status": "ok",
                            "routes": table.routes.iter().map(|r| serde_json::json!({
                                "path": r.path,
                                "backend": r.backend,
                            })).collect::<Vec<_>>(),
                        }),
                    )
                }
                Ok(Err(e)) => error(StatusCode::BAD_REQUEST, e),
                Err(e) => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task failed: {e}"),
                ),
            }
        }
        (Method::POST, "/upgrade") => {
            let body = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(e) => return error(StatusCode::BAD_REQUEST, format!("cannot read body: {e}")),
            };
            let value: serde_json::Value = if body.is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_slice(&body) {
                    Ok(v) => v,
                    Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")),
                }
            };
            let backend = value
                .get("backend")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let binary = value
                .get("binary")
                .and_then(|v| v.as_str())
                .map(PathBuf::from);
            let env: BTreeMap<String, String> = value
                .get("env")
                .and_then(|v| v.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                        .collect()
                })
                .unwrap_or_default();

            let sup_task = sup.clone();
            let backend_task = backend.clone();
            match tokio::task::spawn_blocking(move || sup_task.upgrade(&backend_task, binary, env))
                .await
            {
                Ok(Ok(gen)) => json(
                    StatusCode::OK,
                    serde_json::json!({
                        "status": "upgraded",
                        "backend": backend,
                        "generation": gen.id,
                        "pid": gen.pid,
                        "version": gen.version,
                        "build_id": gen.build_id,
                        "old_generations_draining": true,
                    }),
                ),
                Ok(Err(e)) => error(StatusCode::CONFLICT, e),
                Err(e) => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task failed: {e}"),
                ),
            }
        }
        (Method::POST, "/reload") => {
            let sup = sup.clone();
            match tokio::task::spawn_blocking(move || sup.reload_from_config()).await {
                Ok(Ok(summary)) => json(
                    StatusCode::OK,
                    serde_json::json!({"status": "reloaded", "summary": summary}),
                ),
                Ok(Err(e)) => error(StatusCode::CONFLICT, e),
                Err(e) => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task failed: {e}"),
                ),
            }
        }
        _ => error(
            StatusCode::NOT_FOUND,
            format!(
                "unknown control endpoint: {} {}",
                req.method(),
                req.uri().path()
            ),
        ),
    }
}

fn json(status: StatusCode, value: serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&value).unwrap_or_default();
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Content-Length", body.len())
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn error(status: StatusCode, message: impl Into<String>) -> Response<Full<Bytes>> {
    json(
        status,
        serde_json::json!({"status": "error", "error": message.into()}),
    )
}
