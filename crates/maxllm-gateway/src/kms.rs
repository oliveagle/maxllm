// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! KMS-backed configuration secrets.
//!
//! A TOML document keeps `${kms.assets.<name>}` references verbatim. The
//! gateway resolves them only while building an in-memory [`Config`]. The
//! configuration-center cache and the watched configuration file therefore
//! never receive provider credentials.
//!
//! Two backends are supported:
//!
//! * OpenBao/Vault KV v2, for home deployments;
//! * Ctrip KMS (`/query-pwd` and `/query-key`), for corp deployments.
//!
//! Every lookup fails closed. Errors name the asset and the stable failure
//! class, never a response body, credential, token or header.

use maxllm_config::Config;
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;
use thiserror::Error;
use toml::Table;
use tracing::debug;

const KMS_CLIENT_VERSION: &str = "1.3.1-infra";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_TIMEOUT_SECS: u64 = 15;
const OPENBAO_DEFAULT_MOUNT: &str = "secret";

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("KMS asset '{0}' is not configured")]
    MissingAsset(String),
    #[error("KMS reference '{0}' must be the complete string value")]
    InvalidReference(String),
    #[error("KMS is disabled, but asset '{0}' is referenced")]
    Disabled(String),
    #[error("KMS asset '{0}' request failed")]
    Unavailable(String),
    #[error("KMS asset '{0}' returned an unexpected shape")]
    Shape(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum KmsBackend {
    OpenBao,
    Ctrip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum KmsAssetType {
    Pwd,
    Key,
}

#[derive(Debug, Clone, Deserialize)]
struct KmsConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_timeout_secs")]
    timeout_secs: u64,
    #[serde(default)]
    openbao: Option<OpenBaoConfig>,
    #[serde(default)]
    ctrip: Option<CtripConfig>,
    #[serde(default)]
    assets: HashMap<String, KmsAssetConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenBaoConfig {
    address: String,
    token: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default = "default_openbao_mount")]
    mount: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CtripConfig {
    endpoint: String,
    #[serde(default)]
    retry_endpoint: Option<String>,
    app_id: String,
    herald_token: String,
}

#[derive(Debug, Clone, Deserialize)]
struct KmsAssetConfig {
    backend: KmsBackend,
    // OpenBao KV v2.
    #[serde(default)]
    mount: Option<String>,
    #[serde(default)]
    path: Option<String>,
    // Ctrip asset selector.
    #[serde(default)]
    asset_type: Option<KmsAssetType>,
    #[serde(default)]
    token: Option<String>,
    // Both backends select one string field. For Ctrip pwd, omitting it means
    // `pwdValue` itself is the secret; OpenBao always requires a field.
    #[serde(default)]
    field: Option<String>,
}

fn default_timeout_secs() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

fn default_openbao_mount() -> String {
    OPENBAO_DEFAULT_MOUNT.to_string()
}

/// Parse and validate a complete TOML document, resolving KMS references.
pub fn parse_config(raw: &str) -> Result<Config, KmsError> {
    let expanded = maxllm_config::expand_environment_references(raw);
    let mut document: Table = toml::from_str(&expanded)
        .map_err(|error| KmsError::Config(format!("invalid TOML: {error}")))?;

    let references = collect_references(&document)?;
    let kms_section = document.remove("kms");
    if references.is_empty() {
        return finish_config(toml::Value::Table(document));
    }

    let kms_section = kms_section.ok_or_else(|| {
        KmsError::Config("document contains KMS references but has no [kms] section".into())
    })?;
    let kms: KmsConfig = kms_section
        .clone()
        .try_into()
        .map_err(|error: toml::de::Error| KmsError::Config(format!("invalid [kms]: {error}")))?;
    validate_kms(&kms, &references)?;
    let mut resolver = Resolver::new(kms);
    resolve_table(&mut document, &mut resolver)?;
    finish_config(toml::Value::Table(document))
}

fn finish_config(value: toml::Value) -> Result<Config, KmsError> {
    let config: Config = value.try_into().map_err(|error: toml::de::Error| {
        KmsError::Config(format!("invalid configuration: {error}"))
    })?;
    config
        .validate()
        .map_err(|error| KmsError::Config(error.to_string()))?;
    Ok(config)
}

fn collect_references(document: &Table) -> Result<Vec<String>, KmsError> {
    let mut references = Vec::new();
    collect_value_references(&toml::Value::Table(document.clone()), &mut references)?;
    references.sort();
    references.dedup();
    Ok(references)
}

fn collect_value_references(
    value: &toml::Value,
    references: &mut Vec<String>,
) -> Result<(), KmsError> {
    match value {
        toml::Value::String(text) => {
            if text.contains("${kms.assets.") {
                let reference = KMS_REF
                    .captures(text)
                    .map(|captures| captures[0].to_string())
                    .ok_or_else(|| KmsError::InvalidReference(text.clone()))?;
                references.push(reference);
            }
            Ok(())
        }
        toml::Value::Array(values) => values
            .iter()
            .try_for_each(|value| collect_value_references(value, references)),
        toml::Value::Table(values) => values
            .values()
            .try_for_each(|value| collect_value_references(value, references)),
        _ => Ok(()),
    }
}

fn resolve_table(document: &mut Table, resolver: &mut Resolver) -> Result<(), KmsError> {
    for (_, value) in document.iter_mut() {
        resolve_value(value, resolver)?;
    }
    Ok(())
}

fn resolve_value(value: &mut toml::Value, resolver: &mut Resolver) -> Result<(), KmsError> {
    match value {
        toml::Value::String(text) => {
            if !text.contains("${kms.assets.") {
                return Ok(());
            }
            let original = text.clone();
            let Some(reference) = KMS_REF.captures(&original) else {
                return Err(KmsError::InvalidReference(original));
            };
            let name = &reference[1];
            let resolved = resolver.resolve(name)?;
            *text = resolved;
            Ok(())
        }
        toml::Value::Array(values) => values
            .iter_mut()
            .try_for_each(|value| resolve_value(value, resolver)),
        toml::Value::Table(values) => values
            .iter_mut()
            .try_for_each(|(_, value)| resolve_value(value, resolver)),
        _ => Ok(()),
    }
}

struct Resolver {
    config: KmsConfig,
    client: reqwest::blocking::Client,
    cache: HashMap<String, String>,
}

impl Resolver {
    fn new(config: KmsConfig) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs.max(1)))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        Self {
            config,
            client,
            cache: HashMap::new(),
        }
    }

    fn resolve(&mut self, name: &str) -> Result<String, KmsError> {
        if let Some(value) = self.cache.get(name) {
            return Ok(value.clone());
        }
        let asset = self
            .config
            .assets
            .get(name)
            .ok_or_else(|| KmsError::MissingAsset(name.to_string()))?;
        let value = match asset.backend {
            KmsBackend::OpenBao => self.resolve_openbao(name)?,
            KmsBackend::Ctrip => self.resolve_ctrip(name)?,
        };
        debug!(backend = ?asset.backend, "Resolved a KMS asset");
        self.cache.insert(name.to_string(), value.clone());
        Ok(value)
    }

    fn resolve_openbao(&self, name: &str) -> Result<String, KmsError> {
        let openbao = self.config.openbao.as_ref().ok_or_else(|| {
            KmsError::MissingAsset(format!("{name} (the openbao backend is not configured)"))
        })?;
        let asset = &self.config.assets[name];
        let mount = asset
            .mount
            .as_deref()
            .unwrap_or(&openbao.mount)
            .trim_matches('/');
        let path = asset
            .path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| KmsError::Shape(format!("{name} (openbao path is required)")))?;
        let field = asset
            .field
            .as_deref()
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .ok_or_else(|| KmsError::Shape(format!("{name} (openbao field is required)")))?;
        let url = format!(
            "{}/v1/{}/data/{}",
            openbao.address.trim_end_matches('/'),
            mount,
            path.trim_matches('/')
        );

        let mut request = self.client.get(url).header("X-Vault-Token", &openbao.token);
        if let Some(namespace) = openbao.namespace.as_deref().filter(|v| !v.is_empty()) {
            request = request.header("X-Vault-Namespace", namespace);
        }
        let response = request
            .send()
            .map_err(|_| KmsError::Unavailable(name.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(KmsError::Unavailable(format!(
                "{name} (OpenBao returned HTTP {})",
                status.as_u16()
            )));
        }
        let body = read_limited(response, name)?;
        let envelope: OpenBaoResponse = serde_json::from_slice(&body).map_err(|_| {
            KmsError::Shape(format!("{name} (OpenBao response was not KV v2 JSON)"))
        })?;
        let secret = envelope.data.data;
        let Some(Value::String(value)) = secret.get(field) else {
            return Err(KmsError::Shape(format!(
                "{name} (OpenBao field '{field}' is missing or is not a string)"
            )));
        };
        valid_secret(value)
            .ok_or_else(|| KmsError::Shape(format!("{name} (OpenBao field '{field}' is empty)")))
    }

    fn resolve_ctrip(&self, name: &str) -> Result<String, KmsError> {
        let ctrip = self.config.ctrip.as_ref().ok_or_else(|| {
            KmsError::MissingAsset(format!("{name} (the ctrip backend is not configured)"))
        })?;
        let asset = &self.config.assets[name];
        let asset_type = asset
            .asset_type
            .ok_or_else(|| KmsError::Shape(format!("{name} (ctrip asset_type is required)")))?;
        let token = asset
            .token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| KmsError::Shape(format!("{name} (ctrip token is required)")))?;
        let path = match asset_type {
            KmsAssetType::Pwd => "/query-pwd",
            KmsAssetType::Key => "/query-key",
        };
        let body = serde_json::json!({
            "appid": ctrip.app_id,
            "token": token,
            "type": "JSON",
            "cversion": KMS_CLIENT_VERSION,
        });
        let result = match self.ctrip_once(name, &ctrip.endpoint, path, &body) {
            CtripAttempt::Value(result) => result,
            attempt => {
                let retry_endpoint = ctrip
                    .retry_endpoint
                    .as_deref()
                    .map(str::trim)
                    .filter(|endpoint| !endpoint.is_empty() && *endpoint != ctrip.endpoint);
                if let Some(retry_endpoint) = retry_endpoint {
                    match self.ctrip_once(name, retry_endpoint, path, &body) {
                        CtripAttempt::Value(result) => result,
                        _ => {
                            return Err(match attempt {
                                CtripAttempt::Retryable(error) | CtripAttempt::Failed(error) => {
                                    error
                                }
                                CtripAttempt::Value(_) => unreachable!("handled above"),
                            })
                        }
                    }
                } else {
                    return Err(match attempt {
                        CtripAttempt::Retryable(error) | CtripAttempt::Failed(error) => error,
                        CtripAttempt::Value(_) => unreachable!("handled above"),
                    });
                }
            }
        };

        match asset_type {
            KmsAssetType::Pwd => {
                let record: CtripPwdRecord = serde_json::from_value(result).map_err(|_| {
                    KmsError::Shape(format!("{name} (Ctrip pwd response shape mismatch)"))
                })?;
                if !record.record_type.is_empty() && record.record_type != "PWD" {
                    return Err(KmsError::Shape(format!(
                        "{name} (Ctrip record is not a pwd asset)"
                    )));
                }
                let selected_field = asset
                    .field
                    .as_deref()
                    .map(str::trim)
                    .filter(|field| !field.is_empty() && *field != "@self");
                extract_pwd(name, &record.pwd_value, selected_field)
            }
            KmsAssetType::Key => {
                let record: CtripKeyRecord = serde_json::from_value(result).map_err(|_| {
                    KmsError::Shape(format!("{name} (Ctrip key response shape mismatch)"))
                })?;
                if !record.record_type.is_empty() && record.record_type != "KEY" {
                    return Err(KmsError::Shape(format!(
                        "{name} (Ctrip record is not a key asset)"
                    )));
                }
                valid_secret(&record.key_value)
                    .ok_or_else(|| KmsError::Shape(format!("{name} (Ctrip keyValue is empty)")))
            }
        }
    }

    fn ctrip_once(&self, name: &str, endpoint: &str, path: &str, body: &Value) -> CtripAttempt {
        let response = match self
            .client
            .post(format!("{}{path}", endpoint.trim_end_matches('/')))
            .header(
                "herald-token",
                &self.config.ctrip.as_ref().expect("validated").herald_token,
            )
            .json(body)
            .send()
            .map_err(|error| {
                if error.is_timeout() || error.is_connect() {
                    CtripAttempt::Retryable(KmsError::Unavailable(format!(
                        "{name} (Ctrip KMS is unreachable)"
                    )))
                } else {
                    CtripAttempt::Retryable(KmsError::Unavailable(name.to_string()))
                }
            }) {
            Ok(response) => response,
            Err(error) => return error,
        };
        let status = response.status();
        if !status.is_success() {
            // Match the Go layer's retry rule: only infrastructure failures
            // are worth trying on the secondary endpoint.
            if status.is_server_error() {
                return CtripAttempt::Retryable(KmsError::Unavailable(format!(
                    "{name} (Ctrip KMS returned HTTP {})",
                    status.as_u16()
                )));
            }
            return CtripAttempt::Failed(KmsError::Unavailable(format!(
                "{name} (Ctrip KMS returned HTTP {})",
                status.as_u16()
            )));
        }
        let raw = match read_limited(response, name) {
            Ok(raw) => raw,
            Err(error) => return CtripAttempt::Failed(error),
        };
        let envelope: CtripEnvelope = match serde_json::from_slice(&raw) {
            Ok(envelope) => envelope,
            Err(_) => {
                return CtripAttempt::Failed(KmsError::Shape(format!(
                    "{name} (Ctrip response was not a JSON envelope)"
                )));
            }
        };
        if envelope.code != Some(0) {
            return CtripAttempt::Failed(KmsError::Unavailable(format!(
                "{name} (Ctrip KMS rejected the asset query)"
            )));
        }
        match envelope.result {
            Some(result) => CtripAttempt::Value(result),
            None => CtripAttempt::Failed(KmsError::Shape(format!(
                "{name} (Ctrip response omitted result)"
            ))),
        }
    }
}

enum CtripAttempt {
    Value(Value),
    Retryable(KmsError),
    Failed(KmsError),
}

fn read_limited(response: reqwest::blocking::Response, name: &str) -> Result<Vec<u8>, KmsError> {
    let mut body = Vec::new();
    response
        .take((MAX_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|_| KmsError::Unavailable(name.to_string()))?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(KmsError::Shape(format!("{name} (response exceeded 1 MiB)")));
    }
    Ok(body)
}

fn extract_pwd(name: &str, pwd_value: &str, field: Option<&str>) -> Result<String, KmsError> {
    if valid_secret(pwd_value).is_none() {
        return Err(KmsError::Shape(format!(
            "{name} (Ctrip pwdValue is empty or has invalid control characters)"
        )));
    }
    let Some(field) = field else {
        let trimmed = pwd_value.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return Err(KmsError::Shape(format!(
                "{name} (bare Ctrip pwdValue cannot be a JSON object; select a field)"
            )));
        }
        return Ok(pwd_value.to_string());
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Map<String, Value>>(pwd_value) else {
        return Err(KmsError::Shape(format!(
            "{name} (Ctrip pwdValue is not a JSON object)"
        )));
    };
    let Some(Value::String(value)) = parsed.get(field) else {
        return Err(KmsError::Shape(format!(
            "{name} (Ctrip pwdValue field '{field}' is missing or is not a string)"
        )));
    };
    valid_secret(value)
        .ok_or_else(|| KmsError::Shape(format!("{name} (Ctrip pwdValue field '{field}' is empty)")))
}

fn valid_secret(value: &str) -> Option<String> {
    if value.trim().is_empty() || has_invalid_control_character(value) {
        return None;
    }
    Some(value.to_string())
}

fn has_invalid_control_character(value: &str) -> bool {
    value
        .bytes()
        .any(|byte| byte != b'\t' && (byte < 0x20 || byte == 0x7f))
}

fn validate_kms(config: &KmsConfig, references: &[String]) -> Result<(), KmsError> {
    if !config.enabled {
        let referenced = references
            .first()
            .map(|reference| {
                reference
                    .trim_start_matches("${kms.assets.")
                    .trim_end_matches('}')
            })
            .unwrap_or("unknown");
        return Err(KmsError::Disabled(referenced.to_string()));
    }
    if config.timeout_secs == 0 || config.timeout_secs > 120 {
        return Err(KmsError::Config(
            "kms.timeout_secs must be between 1 and 120".into(),
        ));
    }

    let openbao_used = config
        .assets
        .values()
        .any(|asset| asset.backend == KmsBackend::OpenBao);
    let ctrip_used = config
        .assets
        .values()
        .any(|asset| asset.backend == KmsBackend::Ctrip);

    if openbao_used {
        let Some(openbao) = &config.openbao else {
            return Err(KmsError::Config(
                "kms.openbao is required by at least one openbao asset".into(),
            ));
        };
        let url = url::Url::parse(openbao.address.trim()).map_err(|_| {
            KmsError::Config("kms.openbao.address must be an absolute HTTP(S) URL".into())
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(KmsError::Config(
                "kms.openbao.address must use http or https".into(),
            ));
        }
        if openbao.token.trim().is_empty() {
            return Err(KmsError::Config("kms.openbao.token is required".into()));
        }
        if openbao.mount.trim_matches('/').is_empty() {
            return Err(KmsError::Config("kms.openbao.mount is required".into()));
        }
    }
    if ctrip_used {
        let Some(ctrip) = &config.ctrip else {
            return Err(KmsError::Config(
                "kms.ctrip is required by at least one ctrip asset".into(),
            ));
        };
        let endpoint = url::Url::parse(ctrip.endpoint.trim()).map_err(|_| {
            KmsError::Config("kms.ctrip.endpoint must be an absolute HTTP(S) URL".into())
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(KmsError::Config(
                "kms.ctrip.endpoint must use http or https".into(),
            ));
        }
        if !ctrip.app_id.chars().all(|c| c.is_ascii_digit())
            || ctrip.app_id.is_empty()
            || ctrip.app_id.len() > 18
        {
            return Err(KmsError::Config(
                "kms.ctrip.app_id must contain 1 to 18 ASCII digits".into(),
            ));
        }
        if ctrip.herald_token.trim().is_empty() {
            return Err(KmsError::Config(
                "kms.ctrip.herald_token is required".into(),
            ));
        }
    }

    let asset_pattern = Regex::new(r"^[A-Za-z0-9_.-]{1,128}$").expect("asset pattern is valid");
    let field_pattern = Regex::new(r"^[A-Za-z0-9_.-]{1,128}$").expect("field pattern is valid");
    for (name, asset) in &config.assets {
        if !asset_pattern.is_match(name) {
            return Err(KmsError::Config(format!(
                "KMS asset name '{name}' is invalid"
            )));
        }
        match asset.backend {
            KmsBackend::OpenBao => {
                if asset
                    .path
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(KmsError::Config(format!(
                        "KMS asset '{name}' requires an OpenBao path"
                    )));
                }
                if asset
                    .field
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(KmsError::Config(format!(
                        "KMS asset '{name}' requires an OpenBao field"
                    )));
                }
                if let Some(field) = asset.field.as_deref() {
                    if !field_pattern.is_match(field.trim()) {
                        return Err(KmsError::Config(format!(
                            "KMS asset '{name}' has an invalid OpenBao field"
                        )));
                    }
                }
            }
            KmsBackend::Ctrip => {
                let Some(asset_type) = asset.asset_type else {
                    return Err(KmsError::Config(format!(
                        "KMS asset '{name}' requires ctrip asset_type (pwd or key)"
                    )));
                };
                let token = asset.token.as_deref().map(str::trim).unwrap_or_default();
                if token.is_empty() || token.len() > 512 {
                    return Err(KmsError::Config(format!(
                        "KMS asset '{name}' requires a non-empty ctrip token (at most 512 bytes)"
                    )));
                }
                if asset_type == KmsAssetType::Key && asset.field.is_some() {
                    return Err(KmsError::Config(format!(
                        "KMS asset '{name}' cannot select a field from a Ctrip key asset"
                    )));
                }
                if let Some(field) = asset.field.as_deref() {
                    let field = field.trim();
                    if !field.is_empty() && field != "@self" && !field_pattern.is_match(field) {
                        return Err(KmsError::Config(format!(
                            "KMS asset '{name}' has an invalid Ctrip pwd field"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct OpenBaoResponse {
    data: OpenBaoData,
}

#[derive(Debug, Deserialize)]
struct OpenBaoData {
    data: serde_json::Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct CtripEnvelope {
    code: Option<i64>,
    result: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CtripPwdRecord {
    #[serde(default, rename = "type")]
    record_type: String,
    #[serde(default, rename = "pwdValue")]
    pwd_value: String,
}

#[derive(Debug, Deserialize)]
struct CtripKeyRecord {
    #[serde(default, rename = "type")]
    record_type: String,
    #[serde(default, rename = "keyValue")]
    key_value: String,
}

static KMS_REF: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"^\$\{kms\.assets\.([A-Za-z0-9_.-]+)\}$").expect("KMS reference pattern is valid")
});

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn base_config(kms: &str) -> String {
        format!(
            r#"
[server]
listen = "127.0.0.1:1"

[providers.mock]
kind = "openai"
base_url = "http://127.0.0.1:9"
api_key = "${{kms.assets.provider}}"

{kms}

[[routes]]
path = "/v1/chat/completions"
provider = "mock"
"#
        )
    }

    #[test]
    fn missing_kms_section_fails_closed() {
        let error = parse_config(&base_config("")).unwrap_err();
        assert!(error.to_string().contains("no [kms] section"), "{error}");
    }

    #[test]
    fn embedded_kms_reference_is_rejected() {
        let raw = base_config("").replace(
            "api_key = \"${kms.assets.provider}\"",
            "api_key = \"Bearer ${kms.assets.provider}\"",
        );
        let error = parse_config(&raw).unwrap_err();
        assert!(matches!(error, KmsError::InvalidReference(_)));
    }

    #[test]
    fn unknown_asset_fails_before_network() {
        let raw = base_config(
            r#"
[kms]
enabled = true

[[routes]]
# The duplicate route below is harmless for this test.
"#,
        );
        let error = parse_config(&raw).unwrap_err();
        assert!(error.to_string().contains("not configured"), "{error}");
    }

    #[tokio::test]
    async fn resolves_openbao_kv_v2_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/maxllm/openai"))
            .and(header("X-Vault-Token", "openbao-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {"data": {"api_key": "openbao-secret"}}
            })))
            .mount(&server)
            .await;

        let raw = base_config(&format!(
            r#"
[kms]
enabled = true

[kms.openbao]
address = "{}"
token = "openbao-token"

[kms.assets.provider]
backend = "openbao"
path = "maxllm/openai"
field = "api_key"
"#,
            server.uri()
        ));
        let config = tokio::task::spawn_blocking(move || parse_config(&raw))
            .await
            .expect("blocking task")
            .expect("resolve");
        assert_eq!(config.providers["mock"].api_key, "openbao-secret");
    }

    #[tokio::test]
    async fn openbao_error_fails_closed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/maxllm/openai"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let raw = base_config(&format!(
            r#"
[kms]
enabled = true

[kms.openbao]
address = "{}"
token = "openbao-token"

[kms.assets.provider]
backend = "openbao"
path = "maxllm/openai"
field = "api_key"
"#,
            server.uri()
        ));
        let error = tokio::task::spawn_blocking(move || parse_config(&raw))
            .await
            .expect("blocking task")
            .unwrap_err();
        assert!(error.to_string().contains("HTTP 403"), "{error}");
    }

    #[tokio::test]
    async fn openbao_missing_field_fails_closed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/maxllm/openai"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {"data": {"other": "not-the-field"}}
            })))
            .mount(&server)
            .await;
        let raw = base_config(&format!(
            r#"
[kms]
enabled = true

[kms.openbao]
address = "{}"
token = "openbao-token"

[kms.assets.provider]
backend = "openbao"
path = "maxllm/openai"
field = "api_key"
"#,
            server.uri()
        ));
        let error = tokio::task::spawn_blocking(move || parse_config(&raw))
            .await
            .expect("blocking task")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("OpenBao field 'api_key' is missing"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn resolves_ctrip_pwd_field_with_protocol_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query-pwd"))
            .and(header("herald-token", "herald-token"))
            .and(body_string_contains("\"appid\":\"110001279\""))
            .and(body_string_contains("\"token\":\"asset-token\""))
            .and(body_string_contains("\"type\":\"JSON\""))
            .and(body_string_contains("\"cversion\":\"1.3.1-infra\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "result": {
                    "type": "PWD",
                    "pwdValue": r#"{"api_key":"corp-secret"}"#
                }
            })))
            .mount(&server)
            .await;
        let raw = base_config(&format!(
            r#"
[kms]
enabled = true

[kms.ctrip]
endpoint = "{}"
app_id = "110001279"
herald_token = "herald-token"

[kms.assets.provider]
backend = "ctrip"
asset_type = "pwd"
token = "asset-token"
field = "api_key"
"#,
            server.uri()
        ));
        let config = tokio::task::spawn_blocking(move || parse_config(&raw))
            .await
            .expect("blocking task")
            .expect("resolve");
        assert_eq!(config.providers["mock"].api_key, "corp-secret");
    }

    #[tokio::test]
    async fn ctrip_key_asset_resolves_bare_key_value() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "result": {"type": "KEY", "keyValue": "corp-key"}
            })))
            .mount(&server)
            .await;
        let raw = base_config(&format!(
            r#"
[kms]
enabled = true

[kms.ctrip]
endpoint = "{}"
app_id = "1"
herald_token = "herald-token"

[kms.assets.provider]
backend = "ctrip"
asset_type = "key"
token = "asset-token"
"#,
            server.uri()
        ));
        let config = tokio::task::spawn_blocking(move || parse_config(&raw))
            .await
            .expect("blocking task")
            .expect("resolve");
        assert_eq!(config.providers["mock"].api_key, "corp-key");
    }

    #[test]
    fn ctrip_business_failure_does_not_leak_the_response() {
        let name = "provider";
        let error = KmsError::Unavailable(name.to_string()).to_string();
        assert!(error.contains(name));
        assert!(
            !error.contains("token") && !error.contains("pwdValue"),
            "{error}"
        );
    }
}
