// Copyright 2025 MaxLLM Contributors.
// SPDX-License-Identifier: Apache-2.0

//! CLI for the layer-1 supervisor: status, hot upgrade, reload, routes.

use std::path::PathBuf;
use std::time::Duration;

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("build http client")
}

fn print_json_pretty(body: &str) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or(body.to_string())
        ),
        Err(_) => println!("{body}"),
    }
}

async fn get_or_post(method: &str, url: &str, body: Option<serde_json::Value>) -> i32 {
    let mut req = match method {
        "GET" => client().get(url),
        "POST" => client().post(url),
        "PUT" => client().put(url),
        _ => client().get(url),
    };
    if let Some(body) = body {
        req = req.json(&body);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            print_json_pretty(&text);
            if status.is_success() {
                0
            } else {
                1
            }
        }
        Err(e) => {
            eprintln!("Error: supervisor control API unreachable at {url}: {e}");
            1
        }
    }
}

pub async fn status(base: &str) -> i32 {
    get_or_post(
        "GET",
        &format!("{}/status", base.trim_end_matches('/')),
        None,
    )
    .await
}

pub async fn reload(base: &str) -> i32 {
    get_or_post(
        "POST",
        &format!("{}/reload", base.trim_end_matches('/')),
        None,
    )
    .await
}

pub async fn upgrade(
    base: &str,
    backend: &str,
    binary: Option<PathBuf>,
    build_id: Option<String>,
    env: Vec<String>,
) -> i32 {
    let mut env_map = serde_json::Map::new();
    for pair in env {
        match pair.split_once('=') {
            Some((k, v)) if !k.is_empty() => {
                env_map.insert(k.to_string(), serde_json::Value::String(v.to_string()));
            }
            _ => {
                eprintln!("Error: --env expects KEY=VALUE, got {pair:?}");
                return 1;
            }
        }
    }
    if let Some(build_id) = build_id {
        env_map.insert(
            "MAXLLM_BUILD_ID".into(),
            serde_json::Value::String(build_id),
        );
    }
    let mut body = serde_json::json!({"backend": backend});
    if let Some(binary) = binary {
        body["binary"] = serde_json::Value::String(binary.display().to_string());
    }
    if !env_map.is_empty() {
        body["env"] = serde_json::Value::Object(env_map);
    }
    get_or_post(
        "POST",
        &format!("{}/upgrade", base.trim_end_matches('/')),
        Some(body),
    )
    .await
}

pub async fn routes_list(base: &str) -> i32 {
    get_or_post(
        "GET",
        &format!("{}/routes", base.trim_end_matches('/')),
        None,
    )
    .await
}

pub async fn routes_set(base: &str, routes: Vec<String>) -> i32 {
    let mut parsed = Vec::new();
    for spec in routes {
        // `/v1/audio` (default backend) or `/v1/audio=voice`
        let (path, backend) = match spec.split_once('=') {
            Some((p, b)) => (p.to_string(), b.to_string()),
            None => (spec.clone(), "default".to_string()),
        };
        if !path.starts_with('/') {
            eprintln!("Error: route path {path:?} must start with '/'");
            return 1;
        }
        parsed.push(serde_json::json!({"path": path, "backend": backend}));
    }
    get_or_post(
        "PUT",
        &format!("{}/routes", base.trim_end_matches('/')),
        Some(serde_json::json!({"routes": parsed})),
    )
    .await
}
