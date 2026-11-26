// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The layer-1 reverse proxy.
//!
//! It owns the public TCP listener, resolves the path against the supervisor
//! route table, and forwards the request untouched to the selected worker
//! generation over its private Unix socket. Body and response streaming stay
//! byte-for-byte transparent: there is no buffering or translation here.

use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};
use std::sync::Arc;

use crate::state::{InflightGuard, Supervisor};

pub struct Layer1Proxy {
    sup: Arc<Supervisor>,
}

impl Layer1Proxy {
    pub fn new(sup: Arc<Supervisor>) -> Self {
        Self { sup }
    }

    async fn respond_json(
        session: &mut Session,
        status: u16,
        body: serde_json::Value,
    ) -> pingora::Result<()> {
        let body = serde_json::to_vec(&body).unwrap_or_default();
        let len = body.len().to_string();
        let mut resp = ResponseHeader::build(status, Some(2))?;
        resp.insert_header("Content-Type", "application/json")?;
        resp.insert_header("Content-Length", &len)?;
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(body)), true)
            .await?;
        Ok(())
    }
}

pub struct Layer1Ctx {
    /// Holds the generation chosen for this request and decrements its
    /// in-flight counter on drop.
    pub generation: Option<InflightGuard>,
    pub route_path: Option<String>,
}

#[async_trait]
impl ProxyHttp for Layer1Proxy {
    type CTX = Layer1Ctx;

    fn new_ctx(&self) -> Self::CTX {
        Layer1Ctx {
            generation: None,
            route_path: None,
        }
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<bool> {
        let path = session.req_header().uri.path().to_string();
        let routes = self.sup.routes.load();
        let Some(route) = routes.resolve(&path) else {
            return {
                Self::respond_json(
                    session,
                    404,
                    serde_json::json!({
                        "error": {
                            "message": format!("no layer-1 route registered for path '{path}'"),
                            "type": "route_not_registered",
                        }
                    }),
                )
                .await?;
                Ok(true)
            };
        };

        let Some(backend) = self.sup.backend(&route.backend) else {
            return {
                Self::respond_json(
                    session,
                    503,
                    serde_json::json!({
                        "error": {
                            "message": format!(
                                "layer-1 route '{}' points at backend '{}', which is not running",
                                route.path, route.backend
                            ),
                            "type": "backend_unavailable",
                        }
                    }),
                )
                .await?;
                Ok(true)
            };
        };

        let generation = backend.active.load_full();
        if generation.state.load(std::sync::atomic::Ordering::SeqCst) == crate::state::GEN_EXITED {
            return {
                Self::respond_json(
                    session,
                    503,
                    serde_json::json!({
                        "error": {
                            "message": format!("worker generation {} for backend '{}' has exited", generation.id, route.backend),
                            "type": "backend_unavailable",
                        }
                    }),
                )
                .await?;
                Ok(true)
            };
        }

        ctx.route_path = Some(route.path.clone());
        ctx.generation = Some(InflightGuard::new(generation));

        // Preserve the real client address for the worker's plugins. The
        // worker connects to loopback/UDS, so without this header every client
        // would look like 127.0.0.1.
        let client_ip = session
            .client_addr()
            .and_then(|addr| addr.as_inet().map(|sock| sock.ip().to_string()));
        if let Some(ip) = client_ip {
            let existing = session
                .req_header()
                .headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let value = match existing {
                Some(previous) if !previous.is_empty() => format!("{previous}, {ip}"),
                _ => ip,
            };
            let _ = session
                .req_header_mut()
                .insert_header("X-Forwarded-For", value);
        }

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Box<HttpPeer>> {
        let generation = ctx
            .generation
            .as_ref()
            .map(|g| g.generation().clone())
            .ok_or_else(|| {
                pingora::Error::explain(
                    pingora::ErrorType::InternalError,
                    "request reached upstream_peer without a layer-2 generation",
                )
            })?;
        let socket = generation.socket_path.to_string_lossy().to_string();
        let peer = HttpPeer::new_uds(&socket, false, String::new()).map_err(|e| {
            pingora::Error::because(
                pingora::ErrorType::InternalError,
                format!("invalid worker socket {}", generation.socket_path.display()),
                e,
            )
        })?;
        Ok(Box::new(peer))
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        if let Some(guard) = ctx.generation.as_ref() {
            let gen = guard.generation();
            let _ = upstream_response.insert_header("X-MaxLLM-Layer", "1");
            let _ = upstream_response.insert_header("X-MaxLLM-Generation", gen.id.to_string());
            let _ = upstream_response.insert_header("X-MaxLLM-Layer2-Version", gen.version.clone());
            if let Some(build) = gen.build_id.as_ref() {
                let _ = upstream_response.insert_header("X-MaxLLM-Build-Id", build.clone());
            }
        }
        Ok(())
    }

    async fn logging(
        &self,
        _session: &mut Session,
        e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) {
        // Release the generation slot as soon as the request is over rather
        // than waiting for the context to be dropped. Dropping the guard is
        // idempotent (it can only run once).
        if let Some(gen) = ctx.generation.take() {
            if let Some(e) = e {
                tracing::debug!(
                    backend = %gen.generation().backend,
                    generation = gen.generation().id,
                    error = %e,
                    "layer-1 request ended with an error"
                );
            }
        }
    }
}
