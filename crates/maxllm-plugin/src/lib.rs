// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

pub mod builtin;
pub mod chain;
pub mod context;
pub mod factory;
pub mod guardrail;

pub use chain::PluginChain;
pub use context::PluginCtx;
pub use factory::{create_plugin, PluginError};

use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::proxy::Session;

/// Result of a plugin's request hook.
pub enum RequestAction {
    /// Continue to next plugin in chain.
    Continue,
    /// Short-circuit: send this response immediately.
    Respond(HttpResponse),
}

/// A minimal HTTP response for short-circuit plugin responses.
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Build a JSON error response.
    pub fn json_error(status: u16, message: &str) -> Self {
        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": "plugin_error",
                "code": status
            }
        });
        Self {
            status,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: body.to_string().into_bytes(),
        }
    }

    /// Write this response to the session.
    pub async fn send(&self, session: &mut Session) -> pingora::Result<()> {
        let content_len = self.body.len().to_string();
        let mut resp = ResponseHeader::build(self.status, Some(4))?;
        for (k, v) in &self.headers {
            resp.insert_header(k.clone(), v.as_str())?;
        }
        resp.insert_header("Content-Length", &content_len)?;
        session.set_keepalive(None);
        session.write_response_header(Box::new(resp), false).await?;
        session
            .write_response_body(Some(Bytes::from(self.body.clone())), true)
            .await?;
        Ok(())
    }
}

/// The core plugin trait. Each method maps to a Pingora ProxyHttp lifecycle hook.
/// Default implementations are no-ops so plugins only override what they need.
// Clippy 1.99's double_must_use flags async_trait-generated boxed futures; the
// #[must_use] on them is intentional, so silence the false positive here.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Plugin name, e.g. "key_auth".
    fn name(&self) -> &str;

    /// Called during request_filter, before route matching.
    /// Can short-circuit with RequestAction::Respond.
    async fn on_request(
        &self,
        _session: &mut Session,
        _ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        Ok(RequestAction::Continue)
    }

    /// Called during upstream_request_filter.
    /// Can mutate upstream request headers.
    async fn on_upstream_request(
        &self,
        _session: &mut Session,
        _upstream_request: &mut RequestHeader,
        _ctx: &mut PluginCtx,
    ) -> pingora::Result<()> {
        Ok(())
    }

    /// Called during response_filter.
    /// Can mutate upstream response headers.
    async fn on_response(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut PluginCtx,
    ) -> pingora::Result<()> {
        Ok(())
    }

    /// Called during upstream_response_body_filter (sync).
    /// Can mutate response body chunks.
    fn on_response_body(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut PluginCtx,
    ) -> pingora::Result<()> {
        Ok(())
    }

    /// Called during logging phase.
    async fn on_logging(
        &self,
        _session: &mut Session,
        _error: Option<&pingora::Error>,
        _ctx: &mut PluginCtx,
    ) {
    }
}
