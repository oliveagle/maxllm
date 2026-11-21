// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Native Rust configuration-center support.
//!
//! QConfig and Nacos both reduce to the same deployment shape: fetch a
//! complete MaxLLM TOML document, validate it with the normal config parser,
//! and atomically replace a local cache. The existing file watcher then owns
//! hot reload, so a failed center can never take away the last known-good
//! configuration.

use maxllm_config::{Config, ConfigCenterBackend, ConfigCenterConfig};
use md5::{Digest, Md5};
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use url::Url;

/// Long-poll wire timeout plus a little local headroom.
const QCONFIG_LONG_POLL_SECS: u64 = 60;

#[derive(Debug, Error)]
pub enum ConfigCenterError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream returned HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("configuration not found")]
    NotFound,
    #[error("configuration center rejected the credentials")]
    Forbidden,
    #[error("{0}")]
    Protocol(String),
    #[error("remote configuration failed validation: {0}")]
    InvalidConfig(#[from] maxllm_config::ConfigError),
    #[error("failed to write configuration cache: {0}")]
    CacheIo(#[from] std::io::Error),
    #[error("{0}")]
    Url(#[from] url::ParseError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedConfig {
    pub content: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    NotModified,
    Updated(FetchedConfig),
}

/// A backend that can fetch once and then be polled for changes.
pub trait ConfigCenterSource: Send {
    fn fetch_initial(&mut self) -> Result<FetchedConfig, ConfigCenterError>;
    fn poll_update(&mut self, current_version: u64) -> Result<PollOutcome, ConfigCenterError>;
}

fn content_version(content: &str) -> u64 {
    let mut hasher = Md5::new();
    hasher.update(content.as_bytes());
    let digest = hasher.finalize();
    // A deterministic process-local fingerprint is enough: Nacos's v1 HTTP
    // API has no numeric config version and the value is only compared with
    // values this process produced.
    u64::from_be_bytes(digest[..8].try_into().expect("md5 has 16 bytes"))
}

fn md5_hex(content: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}

fn http_client(timeout_secs: u64) -> Result<Client, ConfigCenterError> {
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs.max(1)))
        .build()
        .map_err(ConfigCenterError::from)
}

fn status_body(response: Response) -> ConfigCenterError {
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    let body = body.trim();
    if body.len() > 512 {
        ConfigCenterError::Status {
            status,
            body: body[..512].to_string(),
        }
    } else {
        ConfigCenterError::Status {
            status,
            body: body.to_string(),
        }
    }
}

fn nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn read_properties(path: &Path) -> HashMap<String, String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    raw.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

fn properties_value(properties: &HashMap<String, String>, key: &str) -> Option<String> {
    nonempty(properties.get(key).map(String::as_str))
}

fn env_value(key: &str) -> Option<String> {
    nonempty(std::env::var(key).ok().as_deref())
}

fn first_present(values: &[Option<String>]) -> Option<String> {
    values.iter().flatten().next().cloned()
}

fn detect_local_ip() -> String {
    // UDP connect only selects a route; it does not send a packet. This gives
    // Foundation's "first non-loopback IPv4 address" behaviour on the normal
    // multi-NIC corp hosts without adding a network-interface crate.
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| socket.connect("10.0.0.1:80").map(|_| socket))
    {
        if let Ok(address) = socket.local_addr() {
            if !address.ip().is_loopback() {
                return address.ip().to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

fn detect_hostname(properties: &HashMap<String, String>) -> String {
    if let Some(name) = env_value("HOSTNAME")
        .or_else(|| properties_value(properties, "host_name"))
        .or_else(|| properties_value(properties, "hostname"))
    {
        return name;
    }
    for path in ["/proc/sys/kernel/hostname", "/etc/hostname"] {
        if let Ok(value) = std::fs::read_to_string(path) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    "unknown".to_string()
}

fn find_app_id() -> Option<String> {
    if let Some(id) = env_value("PAAS_APP_APPID") {
        return Some(id);
    }
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let path = dir.join("conf/app.conf");
        let properties = read_properties(&path);
        if let Some(id) = properties_value(&properties, "app.id") {
            return Some(id);
        }
        if !dir.pop() {
            return None;
        }
    }
}

#[derive(Debug, Clone)]
struct QConfigIdentity {
    app_id: String,
    env: String,
    subenv: String,
    idc: String,
    ip: String,
    hostname: String,
    app_group: Option<String>,
    token: Option<String>,
    endpoints: Vec<String>,
}

impl QConfigIdentity {
    fn from_config(center: &ConfigCenterConfig) -> Result<Self, ConfigCenterError> {
        let properties = read_properties(Path::new("/opt/settings/server.properties"));
        let app_id = first_present(&[
            center.app_id.clone(),
            env_value("PAAS_APP_APPID"),
            find_app_id(),
        ])
        .ok_or_else(|| {
            ConfigCenterError::Protocol(
                "QConfig app_id is required (set config_center.app_id or PAAS_APP_APPID)".into(),
            )
        })?;
        let env = first_present(&[
            center.env.clone(),
            env_value("env"),
            properties_value(&properties, "env"),
        ])
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| "fat".to_string());
        let idc = first_present(&[
            center.idc.clone(),
            env_value("idc"),
            properties_value(&properties, "idc"),
        ])
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
        let subenv = first_present(&[
            center.subenv.clone(),
            env_value("subenv"),
            properties_value(&properties, "subenv"),
            env_value("PAAS_APP_GROUPID"),
            properties_value(&properties, "Paas_app_groupid"),
            properties_value(&properties, "group.id"),
        ])
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| idc.clone());
        let ip = first_present(&[
            center.ip.clone(),
            env_value("PAAS_INSTANCE_IP"),
            env_value("INSTANCE_IP"),
            properties_value(&properties, "ip"),
        ])
        .unwrap_or_else(detect_local_ip);
        let hostname = first_present(&[
            center.hostname.clone(),
            properties_value(&properties, "host_name"),
            properties_value(&properties, "hostname"),
        ])
        .unwrap_or_else(|| detect_hostname(&properties));

        Ok(Self {
            app_id,
            env,
            subenv,
            idc,
            ip,
            hostname,
            app_group: first_present(&[
                center.app_group.clone(),
                env_value("PAAS_APP_GROUPID"),
                properties_value(&properties, "Paas_app_groupid"),
                properties_value(&properties, "group.id"),
            ]),
            token: center.token.clone(),
            endpoints: center
                .endpoints
                .iter()
                .map(|endpoint| endpoint.trim().trim_end_matches('/').to_string())
                .filter(|endpoint| !endpoint.is_empty())
                .collect(),
        })
    }

    fn profile(&self) -> String {
        format!("{}:{}", self.env, self.subenv)
    }
}

fn endpoint_url(endpoint: &str, path: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        format!("{}{}", endpoint.trim_end_matches('/'), path)
    } else {
        format!("http://{}{}", endpoint.trim_end_matches('/'), path)
    }
}

fn qconfig_service_urls(env: &str) -> (String, String) {
    match env {
        "pro" | "prod" => (
            "http://qconfigserver.ctripcorp.com/api/ctrip/app/register.json".into(),
            "http://qconfigserver.ctripcorp.com/api/globalEnv.json".into(),
        ),
        "uat" => (
            "http://qconfigserver.uat.qa.nt.ctripcorp.com/api/ctrip/app/register.json".into(),
            "http://qconfigserver.uat.qa.nt.ctripcorp.com/api/globalEnv.json".into(),
        ),
        _ => (
            "http://qconfigserver.fws.qa.nt.ctripcorp.com/api/ctrip/app/register.json".into(),
            "http://qconfigserver.fws.qa.nt.ctripcorp.com/api/globalEnv.json".into(),
        ),
    }
}

fn qconfig_json_value(response: Response) -> Result<Value, ConfigCenterError> {
    let value: Value = response.json().map_err(ConfigCenterError::from)?;
    let status = value.get("status").and_then(Value::as_i64);
    if status != Some(0) {
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown QConfig error");
        return Err(ConfigCenterError::Protocol(format!(
            "QConfig status={status:?}, message={message}"
        )));
    }
    value
        .get("data")
        .cloned()
        .ok_or_else(|| ConfigCenterError::Protocol("QConfig response has no data".into()))
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn require_string(value: &Value, key: &str) -> Result<String, ConfigCenterError> {
    value_string(value, key).ok_or_else(|| {
        ConfigCenterError::Protocol(format!("QConfig response is missing string field {key}"))
    })
}

fn qconfig_execute(
    request: RequestBuilder,
    long_poll: bool,
) -> Result<Response, ConfigCenterError> {
    let response = request.send()?;
    if long_poll && response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(response);
    }
    if !response.status().is_success() {
        return Err(status_body(response));
    }
    Ok(response)
}

#[derive(Debug)]
pub struct QConfigClient {
    http: Client,
    identity: QConfigIdentity,
    data_id: String,
    endpoints: Vec<String>,
    token: Option<String>,
    endpoint_index: usize,
}

impl QConfigClient {
    /// Construction is deliberately offline. Discovery happens on the first
    /// fetch so an optional center can start from cache even while QConfig is
    /// temporarily unreachable.
    pub fn new(center: &ConfigCenterConfig) -> Result<Self, ConfigCenterError> {
        let identity = QConfigIdentity::from_config(center)?;
        let http = http_client(center.timeout_secs.max(QCONFIG_LONG_POLL_SECS + 5))?;
        let token = identity.token.clone();
        let endpoints = identity.endpoints.clone();
        Ok(Self {
            http,
            identity,
            data_id: center.data_id.clone(),
            endpoints,
            token,
            endpoint_index: 0,
        })
    }

    fn ensure_discovered(&mut self) -> Result<(), ConfigCenterError> {
        if self.token.is_some() && !self.endpoints.is_empty() {
            return Ok(());
        }
        let (token, endpoints) = Self::discover(&self.http, &self.identity.clone())?;
        if self.token.is_none() {
            self.token = Some(token);
        }
        if self.endpoints.is_empty() {
            self.endpoints = endpoints;
        }
        if self.token.is_none() || self.endpoints.is_empty() {
            return Err(ConfigCenterError::Protocol(
                "QConfig discovery did not produce a token and data-plane endpoint".into(),
            ));
        }
        Ok(())
    }

    fn discover(
        http: &Client,
        identity: &QConfigIdentity,
    ) -> Result<(String, Vec<String>), ConfigCenterError> {
        let (register_url, global_url) = qconfig_service_urls(&identity.env);
        let mut payload = serde_json::Map::new();
        payload.insert("appId".into(), identity.app_id.clone().into());
        payload.insert("server.pid".into(), std::process::id().into());
        payload.insert("server.type".into(), identity.env.clone().into());
        payload.insert("server.subEnv".into(), identity.subenv.clone().into());
        payload.insert("server.room".into(), identity.idc.clone().into());
        payload.insert("server.ip".into(), identity.ip.clone().into());
        payload.insert("server.hostname".into(), identity.hostname.clone().into());
        // Go's SDK uses `omitempty`; the service treats an explicit null as a
        // set-but-empty group rather than an omitted one.
        if let Some(group) = &identity.app_group {
            payload.insert("app.group".into(), group.clone().into());
        }
        let response = qconfig_execute(
            http.post(&register_url)
                .header("Content-Type", "application/json;charset=utf-8")
                .json(&Value::Object(payload)),
            false,
        )?;
        let data = qconfig_json_value(response)?;
        let token = require_string(&data, "server.token")?;
        let server_type = require_string(&data, "server.type")?;

        let response = qconfig_execute(
            http.get(&global_url).header("Q-Server-Token", &token),
            false,
        )?;
        let data = qconfig_json_value(response)?;
        let primary = require_string(&data, "qconfig.server.host")?;
        let backups = value_string(&data, "qconfig.default.serverlist")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();

        for host in std::iter::once(primary).chain(backups) {
            let response = match qconfig_execute(
                http.get(endpoint_url(&host, "/entrypointv2"))
                    .header("token", &token),
                false,
            ) {
                Ok(response) => response,
                Err(error) => {
                    debug!(host = %host, error = %error, "QConfig entrypoint failed, trying backup");
                    continue;
                }
            };
            let body = response.text()?;
            let endpoints = body
                .lines()
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            if !endpoints.is_empty() {
                // server.type is only logged; the request profile remains the
                // local Foundation identity (`env:subenv`) QConfig expects.
                debug!(server_type = %server_type, "QConfig registration completed");
                return Ok((token, endpoints));
            }
        }
        Err(ConfigCenterError::Protocol(
            "QConfig did not return a usable data-plane endpoint".into(),
        ))
    }

    fn fetch_url(&self, force: bool, version: u64) -> (String, [(&'static str, String); 4]) {
        let endpoint = &self.endpoints[self.endpoint_index];
        let path = if force {
            "/client/forceloadv2"
        } else {
            "/client/getconfigv2"
        };
        (
            endpoint_url(endpoint, path),
            [
                ("dataId", self.data_id.clone()),
                ("group", self.identity.app_id.clone()),
                ("version", version.to_string()),
                ("loadProfile", self.identity.profile()),
            ],
        )
    }

    fn fetch_at_version(&self, version: u64) -> Result<FetchedConfig, ConfigCenterError> {
        let (url, query) = self.fetch_url(version == 0, version);
        let mut request = self
            .http
            .get(&url)
            .header("token", self.token.as_deref().unwrap_or_default())
            .header("profile", self.identity.profile())
            .header("c", "rust");
        for (key, value) in query {
            request = request.query(&[(key, value)]);
        }
        let response = qconfig_execute(request, false)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConfigCenterError::NotFound);
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(ConfigCenterError::Forbidden);
        }
        let version = response
            .headers()
            .get("Version")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .ok_or_else(|| ConfigCenterError::Protocol("QConfig omitted Version".into()))?;
        response
            .headers()
            .get("Profile")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|profile| !profile.is_empty())
            .ok_or_else(|| ConfigCenterError::Protocol("QConfig omitted Profile".into()))?;
        let checksum = response
            .headers()
            .get("Checksum")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .ok_or_else(|| ConfigCenterError::Protocol("QConfig omitted Checksum".into()))?
            .to_string();
        let content = response.text()?;
        if content.is_empty() {
            return Err(ConfigCenterError::Protocol(
                "QConfig returned empty config".into(),
            ));
        }
        if !checksum.eq_ignore_ascii_case(&md5_hex(content.as_bytes())) {
            return Err(ConfigCenterError::Protocol(
                "QConfig checksum does not match configuration body".into(),
            ));
        }
        Ok(FetchedConfig { content, version })
    }
}

impl ConfigCenterSource for QConfigClient {
    fn fetch_initial(&mut self) -> Result<FetchedConfig, ConfigCenterError> {
        self.ensure_discovered()?;
        self.fetch_at_version(0)
    }

    fn poll_update(&mut self, current_version: u64) -> Result<PollOutcome, ConfigCenterError> {
        self.ensure_discovered()?;
        let endpoint = self.endpoints[self.endpoint_index].clone();
        let body = format!(
            "{},{},{},{}",
            self.identity.app_id,
            self.data_id,
            current_version,
            self.identity.profile()
        );
        let response = qconfig_execute(
            self.http
                .post(endpoint_url(&endpoint, "/client/longPollingCheckupdatev2"))
                .header("Content-Type", "text/plain")
                .header("token", self.token.as_deref().unwrap_or_default())
                .header("profile", &self.identity.subenv)
                .header("c", "rust")
                .body(body.clone()),
            true,
        )?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(PollOutcome::NotModified);
        }
        let response_body = response.text()?;
        let mut next_version = None;
        for line in response_body.lines() {
            let fields = line.trim().splitn(4, ',').collect::<Vec<_>>();
            if fields.len() != 4 {
                continue;
            }
            let version = fields[2].trim().parse::<u64>().ok();
            if fields[0].trim() != self.identity.app_id
                || fields[1].trim() != self.data_id
                || !matches!(version, Some(version) if version > current_version)
                || fields[3].trim() != self.identity.profile()
            {
                continue;
            }
            next_version = version;
            break;
        }
        let Some(version) = next_version else {
            warn!(
                body = %response_body,
                expected = %body,
                "Ignoring unrelated, stale or malformed QConfig long-poll result"
            );
            return Ok(PollOutcome::NotModified);
        };
        let fetched = self.fetch_at_version(version)?;
        Ok(PollOutcome::Updated(fetched))
    }
}

#[derive(Debug)]
pub struct NacosClient {
    http: Client,
    base_url: Url,
    data_id: String,
    group: String,
    namespace: Option<String>,
    username: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
}

impl NacosClient {
    pub fn new(center: &ConfigCenterConfig) -> Result<Self, ConfigCenterError> {
        let mut base_url = Url::parse(
            nonempty(center.server_url.as_deref())
                .unwrap_or_default()
                .as_str(),
        )?;
        base_url.set_fragment(None);
        Ok(Self {
            http: http_client(center.timeout_secs.max(1))?,
            base_url,
            data_id: center.data_id.clone(),
            group: center.group.clone(),
            namespace: center.namespace.clone(),
            username: center.username.clone(),
            password: center.password.clone(),
            access_token: None,
        })
    }

    fn url(&self, suffix: &str) -> Result<Url, ConfigCenterError> {
        let base_path = self.base_url.path().trim_end_matches('/');
        let mut url = self.base_url.clone();
        url.set_path(&format!("{base_path}{suffix}"));
        Ok(url)
    }

    fn login(&mut self) -> Result<(), ConfigCenterError> {
        let Some(username) = self.username.clone() else {
            return Ok(());
        };
        let password = self.password.clone().unwrap_or_default();
        let url = self.url("/v1/auth/users/login")?;
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.append_pair("username", &username);
        form.append_pair("password", &password);
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form.finish())
            .send()?;
        if !response.status().is_success() {
            return Err(status_body(response));
        }
        let value: Value = response.json().map_err(ConfigCenterError::from)?;
        self.access_token = Some(
            value
                .get("accessToken")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigCenterError::Protocol("Nacos login omitted accessToken".into())
                })?
                .to_string(),
        );
        Ok(())
    }

    fn fetch_content(&mut self) -> Result<String, ConfigCenterError> {
        if self.access_token.is_none() {
            self.login()?;
        }
        let mut url = self.url("/v1/cs/configs")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("dataId", &self.data_id);
            query.append_pair("group", &self.group);
            if let Some(namespace) = &self.namespace {
                query.append_pair("tenant", namespace);
            }
            if let Some(token) = &self.access_token {
                query.append_pair("accessToken", token);
            }
        }
        let response = self.http.get(url).send()?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConfigCenterError::NotFound);
        }
        if !response.status().is_success() {
            return Err(status_body(response));
        }
        let content = response.text()?;
        if content.is_empty() {
            return Err(ConfigCenterError::Protocol(
                "Nacos returned empty config".into(),
            ));
        }
        Ok(content)
    }
}

impl ConfigCenterSource for NacosClient {
    fn fetch_initial(&mut self) -> Result<FetchedConfig, ConfigCenterError> {
        let content = self.fetch_content()?;
        Ok(FetchedConfig {
            version: content_version(&content),
            content,
        })
    }

    fn poll_update(&mut self, current_version: u64) -> Result<PollOutcome, ConfigCenterError> {
        let content = self.fetch_content()?;
        let version = content_version(&content);
        if version == current_version {
            Ok(PollOutcome::NotModified)
        } else {
            Ok(PollOutcome::Updated(FetchedConfig { content, version }))
        }
    }
}

/// Read-only PostgREST (BaaS) client.
///
/// This mirrors the fail-closed Go runtime client: no environment-specific
/// default endpoint, bearer auth is mandatory, and responses are plain JSON.
#[derive(Debug)]
pub struct BaasClient {
    http: Client,
    base_url: Url,
    table: String,
    row_key: String,
    row_key_column: String,
    config_column: String,
    version_column: Option<String>,
    bearer_token: String,
}

impl BaasClient {
    pub fn new(center: &ConfigCenterConfig) -> Result<Self, ConfigCenterError> {
        let base_url = Url::parse(
            nonempty(center.server_url.as_deref())
                .ok_or_else(|| ConfigCenterError::Protocol("BaaS endpoint is required".into()))?
                .as_str(),
        )?;
        let row_key = center
            .row_key
            .clone()
            .unwrap_or_else(|| center.data_id.clone());
        Ok(Self {
            http: http_client(center.timeout_secs.max(1))?,
            base_url,
            table: center.table.clone().unwrap_or_default(),
            row_key,
            row_key_column: center.row_key_column.clone(),
            config_column: center.config_column.clone().unwrap_or_default(),
            version_column: center.version_column.clone(),
            bearer_token: center.bearer_token.clone().unwrap_or_default(),
        })
    }

    fn request_url(&self) -> Result<Url, ConfigCenterError> {
        let mut url = self.base_url.clone();
        url.set_fragment(None);
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                ConfigCenterError::Protocol("BaaS endpoint cannot contain a query".into())
            })?;
            segments.pop_if_empty().push(&self.table);
        }
        {
            let mut select = vec![self.config_column.clone()];
            if let Some(version) = &self.version_column {
                select.push(version.clone());
            }
            let mut query = url.query_pairs_mut();
            query.append_pair("select", &select.join(","));
            query.append_pair(&self.row_key_column, &format!("eq.{}", self.row_key));
            query.append_pair("limit", "1");
        }
        Ok(url)
    }

    fn fetch_row(&self) -> Result<FetchedConfig, ConfigCenterError> {
        let url = self.request_url()?;
        let response = self
            .http
            .get(url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .header("Accept", "application/json")
            .header("User-Agent", "maxllm-gateway/config-center")
            .send()?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConfigCenterError::NotFound);
        }
        if !response.status().is_success() {
            return Err(status_body(response));
        }
        let rows: Vec<Value> = response.json().map_err(ConfigCenterError::from)?;
        let row = rows.first().ok_or(ConfigCenterError::NotFound)?;
        let content = row
            .get(&self.config_column)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ConfigCenterError::Protocol(format!(
                    "BaaS row is missing a string field '{}'",
                    self.config_column
                ))
            })?
            .to_string();
        if content.is_empty() {
            return Err(ConfigCenterError::Protocol(
                "BaaS returned empty configuration".into(),
            ));
        }
        Ok(FetchedConfig {
            version: content_version(&content),
            content,
        })
    }
}

impl ConfigCenterSource for BaasClient {
    fn fetch_initial(&mut self) -> Result<FetchedConfig, ConfigCenterError> {
        self.fetch_row()
    }

    fn poll_update(&mut self, current_version: u64) -> Result<PollOutcome, ConfigCenterError> {
        let fetched = self.fetch_row()?;
        if fetched.version == current_version {
            Ok(PollOutcome::NotModified)
        } else {
            Ok(PollOutcome::Updated(fetched))
        }
    }
}

pub fn make_source(
    center: &ConfigCenterConfig,
) -> Result<Box<dyn ConfigCenterSource>, ConfigCenterError> {
    match center.backend {
        ConfigCenterBackend::QConfig => {
            Ok(Box::new(QConfigClient::new(center)?) as Box<dyn ConfigCenterSource>)
        }
        ConfigCenterBackend::Nacos => {
            Ok(Box::new(NacosClient::new(center)?) as Box<dyn ConfigCenterSource>)
        }
        ConfigCenterBackend::Baas => {
            Ok(Box::new(BaasClient::new(center)?) as Box<dyn ConfigCenterSource>)
        }
    }
}

/// Resolve a center cache path relative to the bootstrap file, as operators
/// expect `cache_path = "generated/maxllm.toml"` to remain beside their input.
pub fn cache_path_for(bootstrap_path: &Path, center: &ConfigCenterConfig) -> PathBuf {
    if center.cache_path.is_absolute() {
        center.cache_path.clone()
    } else {
        bootstrap_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&center.cache_path)
    }
}

/// Write through a same-directory temporary file so readers observe either
/// the old complete file or the new complete file, never a truncated one.
pub fn atomic_write_0600(path: &Path, content: &str) -> Result<(), ConfigCenterError> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.center-{}-{}",
        path.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("maxllm"),
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or_default(),
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    // The replacement is atomic on POSIX. Cache paths in real deployments are
    // Linux paths; Windows support is not part of the gateway's service model.
    std::fs::rename(&temporary, path)?;
    Ok(())
}

/// Fetch, validate and materialize a remote document.
pub fn fetch_and_materialize(
    source: &mut dyn ConfigCenterSource,
    cache_path: &Path,
) -> Result<FetchedConfig, ConfigCenterError> {
    let fetched = source.fetch_initial()?;
    Config::parse(&fetched.content)?;
    atomic_write_0600(cache_path, &fetched.content)?;
    Ok(fetched)
}

/// Apply a polled update with the same validation and atomic-cache path.
pub fn apply_poll_update(
    fetched: &FetchedConfig,
    cache_path: &Path,
) -> Result<(), ConfigCenterError> {
    Config::parse(&fetched.content)?;
    atomic_write_0600(cache_path, &fetched.content)?;
    Ok(())
}

/// Spawn the background poller. Its output is the local cache; the gateway's
/// existing file watcher remains the only component that swaps live state.
pub fn spawn_poller(
    mut source: Box<dyn ConfigCenterSource>,
    mut current_version: u64,
    cache_path: PathBuf,
    interval_secs: u64,
    backend_name: &'static str,
) {
    std::thread::Builder::new()
        .name("maxllm-config-center".to_string())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(interval_secs.max(1)));
            match source.poll_update(current_version) {
                Ok(PollOutcome::NotModified) => {
                    debug!(backend = backend_name, "Configuration center has no update");
                }
                Ok(PollOutcome::Updated(fetched)) => {
                    match apply_poll_update(&fetched, &cache_path) {
                        Ok(()) => {
                            current_version = fetched.version;
                            info!(
                                backend = backend_name,
                                version = fetched.version,
                                path = %cache_path.display(),
                                "Configuration center update materialized"
                            );
                        }
                        Err(error) => {
                            error!(
                                backend = backend_name,
                                error = %error,
                                "Rejected invalid configuration-center update; keeping cache"
                            );
                        }
                    }
                }
                Err(error) => {
                    // The cache remains in place. A transient center outage
                    // must not stop the current gateway generation.
                    warn!(
                        backend = backend_name,
                        error = %error,
                        "Configuration-center poll failed; will retry"
                    );
                }
            }
        })
        .expect("failed to spawn config-center poller");
}

/// Stable thread/log label for a backend.
pub fn backend_name(backend: ConfigCenterBackend) -> &'static str {
    match backend {
        ConfigCenterBackend::QConfig => "qconfig",
        ConfigCenterBackend::Nacos => "nacos",
        ConfigCenterBackend::Baas => "baas",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn center_for(server: &MockServer) -> ConfigCenterConfig {
        ConfigCenterConfig {
            enabled: true,
            backend: ConfigCenterBackend::Nacos,
            data_id: "maxllm.toml".into(),
            cache_path: "maxllm.center.toml".into(),
            required: true,
            poll_interval_secs: 30,
            timeout_secs: 5,
            server_url: Some(server.uri()),
            namespace: Some("test".into()),
            group: "MAXLLM".into(),
            username: Some("nacos-user".into()),
            password: Some("nacos-password".into()),
            table: Some("maxllm_configs".into()),
            row_key: Some("global".into()),
            row_key_column: "id".into(),
            config_column: Some("config_toml".into()),
            version_column: None,
            bearer_token: Some("baas-token".into()),
            app_id: None,
            env: None,
            subenv: None,
            idc: None,
            ip: None,
            hostname: None,
            app_group: None,
            token: None,
            endpoints: Vec::new(),
        }
    }

    #[test]
    fn content_fingerprint_is_stable_and_checksum_matches_wire_format() {
        let content = "alpha\nbeta\n";
        assert_eq!(content_version(content), content_version(content));
        assert_ne!(content_version(content), content_version("changed"));
        assert_eq!(
            md5_hex(content.as_bytes()),
            "852e77b490fb4e8653fbc11f4c6f89c2"
        );
    }

    #[test]
    fn atomic_cache_write_sets_private_permissions_and_replaces_atomically() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("nested/generated.toml");
        atomic_write_0600(&path, "first = 1\n").expect("first write");
        let metadata = std::fs::metadata(&path).expect("cache metadata");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        atomic_write_0600(&path, "second = 2\n").expect("replacement");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second = 2\n");
    }

    #[test]
    fn cache_keeps_a_kms_reference_instead_of_resolving_it_to_plaintext() {
        let config = r#"[server]
listen = "127.0.0.1:1"

[providers.mock]
kind = "openai"
base_url = "http://127.0.0.1:2"
api_key = "${kms.assets.provider}"
"#;
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let server = runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/auth/users/login"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "accessToken": "test-token"
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v1/cs/configs"))
                .respond_with(ResponseTemplate::new(200).set_body_string(config))
                .mount(&server)
                .await;
            server
        });
        let center = center_for(&server);

        let directory = runtime.block_on(async {
            let directory = tempfile::tempdir().expect("tempdir");
            let cache = directory.path().join("cache.toml");
            let test_center = center.clone();
            let task_cache = cache.clone();
            tokio::task::spawn_blocking(move || {
                let mut client = NacosClient::new(&test_center).expect("client");
                fetch_and_materialize(&mut client, &task_cache)
            })
            .await
            .expect("blocking task")
            .expect("fetch");
            directory
        });

        let cache = directory.path().join("cache.toml");
        let raw = std::fs::read_to_string(&cache).expect("cache");
        assert!(raw.contains("api_key = \"${kms.assets.provider}\""));
        assert!(!raw.contains("plaintext-provider-key"));
        // A cache is valid input even before it is resolved; the gateway's
        // KMS layer owns that final step.
        Config::from_file(&cache).expect("cache parses");
    }

    #[test]
    fn nacos_fetches_authenticated_config_and_materializes_cache() {
        let config = "[server]\nlisten = \"127.0.0.1:1\"\n\n[providers.mock]\nkind = \"openai\"\nbase_url = \"http://127.0.0.1:2\"\napi_key = \"remote-key\"\n";
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let server = runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/auth/users/login"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "accessToken": "test-token"
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v1/cs/configs"))
                .and(query_param("dataId", "maxllm.toml"))
                .and(query_param("group", "MAXLLM"))
                .and(query_param("tenant", "test"))
                .and(query_param("accessToken", "test-token"))
                .respond_with(ResponseTemplate::new(200).set_body_string(config))
                .mount(&server)
                .await;
            server
        });
        let center = center_for(&server);

        let (fetched, received, directory) = runtime.block_on(async {
            let directory = tempfile::tempdir().expect("tempdir");
            let cache = directory.path().join("cache.toml");
            let test_center = center.clone();
            let task_cache = cache.clone();
            let fetched = tokio::task::spawn_blocking(move || {
                let mut client = NacosClient::new(&test_center).expect("client");
                fetch_and_materialize(&mut client, &task_cache)
            })
            .await
            .expect("blocking task");
            let received = server.received_requests().await.expect("requests");
            (fetched, received, directory)
        });
        let fetched = fetched.expect("fetch");
        assert_eq!(fetched.content, config);
        assert_eq!(received.len(), 2);
        assert_eq!(received[1].url.path(), "/v1/cs/configs");
        let cache = directory.path().join("cache.toml");
        let parsed = Config::from_file(&cache).expect("cache parses");
        assert_eq!(parsed.server.listen.to_string(), "127.0.0.1:1");
    }

    #[test]
    fn qconfig_direct_mode_verifies_version_checksum_and_poll_update() {
        let initial = "[server]\nlisten = \"127.0.0.1:3\"\n\n[providers.mock]\nkind = \"openai\"\nbase_url = \"http://127.0.0.1:4\"\napi_key = \"one\"\n";
        let updated = initial.replace("one", "two");
        let updated_checksum = md5_hex(updated.as_bytes());
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let server = runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/client/forceloadv2"))
                .and(query_param("dataId", "maxllm.toml"))
                .and(query_param("group", "app"))
                .and(query_param("loadProfile", "uat:sub"))
                .and(header("token", "wire-token"))
                .and(header("profile", "uat:sub"))
                .and(header("c", "rust"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(initial)
                        .append_header("Version", "7")
                        .append_header("Profile", "uat:sub")
                        .append_header("Checksum", md5_hex(initial.as_bytes())),
                )
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/client/longPollingCheckupdatev2"))
                .and(header("token", "wire-token"))
                .and(header("profile", "sub"))
                .and(body_string_contains("app,maxllm.toml,7,uat:sub"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string("other,data.toml,9,x\napp,maxllm.toml,8,uat:sub\n"),
                )
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/client/getconfigv2"))
                .and(query_param("version", "8"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(updated.clone())
                        .append_header("Version", "8")
                        .append_header("Profile", "uat:sub")
                        .append_header("Checksum", updated_checksum),
                )
                .mount(&server)
                .await;
            server
        });

        let mut center = center_for(&server);
        center.backend = ConfigCenterBackend::QConfig;
        center.app_id = Some("app".into());
        center.env = Some("uat".into());
        center.subenv = Some("sub".into());
        center.token = Some("wire-token".into());
        center.endpoints = vec![server.address().to_string()];
        let mut client = QConfigClient::new(&center).expect("client");
        let fetched = client.fetch_initial().expect("initial fetch");
        assert_eq!(fetched.version, 7);
        let outcome = client.poll_update(7).expect("poll update");
        let PollOutcome::Updated(new_fetch) = outcome else {
            panic!("expected an update");
        };
        assert_eq!(new_fetch.version, 8);
        assert_eq!(new_fetch.content, updated);
    }

    #[test]
    fn baas_reads_authenticated_postgrest_row_and_polls_body_changes() {
        let initial = "[server]\nlisten = \"127.0.0.1:5\"\n\n[providers.mock]\nkind = \"openai\"\nbase_url = \"http://127.0.0.1:6\"\napi_key = \"one\"\n";
        let updated = initial.replace("one", "two");
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let server = runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/rest/v1/maxllm_configs"))
                .and(query_param("select", "config_toml"))
                .and(query_param("id", "eq.global"))
                .and(query_param("limit", "1"))
                .and(header("authorization", "Bearer baas-token"))
                .and(header("accept", "application/json"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"id": "global", "config_toml": initial}
                ])))
                .up_to_n_times(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/rest/v1/maxllm_configs"))
                .and(query_param("select", "config_toml"))
                .and(query_param("id", "eq.global"))
                .and(query_param("limit", "1"))
                .and(header("authorization", "Bearer baas-token"))
                .and(header("accept", "application/json"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"id": "global", "config_toml": updated.clone()}
                ])))
                .mount(&server)
                .await;
            server
        });

        let mut center = center_for(&server);
        center.server_url = Some(format!("{}/rest/v1", server.uri()));
        let (result, received) = runtime.block_on(async {
            let test_center = center.clone();
            let result = tokio::task::spawn_blocking(move || -> Result<_, ConfigCenterError> {
                let mut client = BaasClient::new(&test_center).expect("client");
                let initial_fetch = client.fetch_initial()?;
                let outcome = client.poll_update(initial_fetch.version)?;
                Ok((initial_fetch, outcome))
            })
            .await
            .expect("blocking task");
            let received = server.received_requests().await.expect("requests");
            (result, received)
        });
        let (initial_fetch, outcome) = result.expect("fetch");

        assert_eq!(received.len(), 2);
        assert_eq!(received[0].url.path(), "/rest/v1/maxllm_configs");
        assert_eq!(
            received[0]
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer baas-token")
        );
        let query: Vec<(String, String)> = received[0]
            .url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert!(query.contains(&("id".into(), "eq.global".into())));
        assert_eq!(initial_fetch.version, content_version(initial));
        assert_eq!(initial_fetch.content, initial);
        assert_eq!(
            outcome,
            PollOutcome::Updated(FetchedConfig {
                version: content_version(&updated),
                content: updated,
            })
        );
    }
}
