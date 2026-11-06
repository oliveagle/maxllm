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
use pingora::proxy::Session;
use std::collections::HashSet;

/// API key authentication plugin.
/// Validates Bearer tokens from the Authorization header against a configured key set.
pub struct KeyAuthPlugin {
    name: String,
    header: String,
    strip_prefix: String,
    keys: HashSet<String>,
    hide_credentials: bool,
}

impl KeyAuthPlugin {
    pub fn from_config(name: &str, config: &toml::Table) -> Result<Self, PluginError> {
        let keys: HashSet<String> = config
            .get("keys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if keys.is_empty() {
            return Err(PluginError::Config(
                "key_auth plugin requires at least one key".into(),
            ));
        }

        let header = config
            .get("header")
            .and_then(|v| v.as_str())
            .unwrap_or("Authorization")
            .to_string();

        let strip_prefix = config
            .get("strip_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("Bearer ")
            .to_string();

        let hide_credentials = config
            .get("hide_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            name: name.to_string(),
            header,
            strip_prefix,
            keys,
            hide_credentials,
        })
    }
}

#[async_trait]
impl Plugin for KeyAuthPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_request(
        &self,
        session: &mut Session,
        ctx: &mut PluginCtx,
    ) -> pingora::Result<RequestAction> {
        // A request that already passed the gateway's virtual-key
        // authentication (see the `request_filter` hook) is trusted: the
        // static key set configured here does not know about keys created
        // through the admin API.
        if ctx.extensions.get("virtual_key_auth").map(|v| v.as_str()) == Some("1") {
            return Ok(RequestAction::Continue);
        }
        let header_value = session
            .req_header()
            .headers
            .get(&self.header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let key = match header_value {
            Some(ref val) => {
                if val.starts_with(&self.strip_prefix) {
                    &val[self.strip_prefix.len()..]
                } else {
                    val.as_str()
                }
            }
            None => {
                return Ok(RequestAction::Respond(HttpResponse::json_error(
                    401,
                    "Missing API key",
                )));
            }
        };

        if self.keys.contains(key) {
            ctx.client_id = Some(key.to_string());

            if self.hide_credentials {
                let _ = session.req_header_mut().remove_header(&self.header);
            }

            Ok(RequestAction::Continue)
        } else {
            Ok(RequestAction::Respond(HttpResponse::json_error(
                401,
                "Invalid API key",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_config() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "key_auth".into());
        config.insert(
            "keys".into(),
            toml::Value::Array(vec!["sk-test".into(), "sk-prod".into()]),
        );

        let plugin = KeyAuthPlugin::from_config("auth", &config).unwrap();
        assert_eq!(plugin.keys.len(), 2);
        assert_eq!(plugin.header, "Authorization");
        assert_eq!(plugin.strip_prefix, "Bearer ");
    }

    #[test]
    fn test_empty_keys_fails() {
        let mut config = toml::Table::new();
        config.insert("category".into(), "key_auth".into());
        config.insert("keys".into(), toml::Value::Array(vec![]));

        assert!(KeyAuthPlugin::from_config("auth", &config).is_err());
    }
}
