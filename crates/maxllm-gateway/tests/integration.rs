// Integration tests for the MaxLLM gateway.
//
// These tests spawn the gateway as a subprocess and send HTTP requests
// against it. Mock-based tests use wiremock as a fake upstream provider.
// Real provider tests (Ollama, Gemini) are gated with #[ignore].

use reqwest::Client;
use serde_json::{json, Value};
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{body_string_contains, header, header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct TestGateway {
    child: Child,
    port: u16,
    _config_file: tempfile::NamedTempFile,
}

/// Ports already handed to a gateway in this test binary.
///
/// `portpicker` only checks that a port is free *at the moment it is picked*,
/// so two tests starting in parallel can be handed the same one. The gateway
/// that loses the bind exits, and the winner's client then talks to the *other*
/// test's gateway — which shows up as a random 200 with the wrong provider
/// header in whichever test lost the race.
static CLAIMED_PORTS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<u16>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

fn pick_gateway_port() -> u16 {
    loop {
        let port = portpicker::pick_unused_port().expect("no free port");
        if CLAIMED_PORTS.lock().expect("port set").insert(port) {
            return port;
        }
    }
}

impl TestGateway {
    /// Spawn the gateway binary with the given TOML config.
    /// The `{PORT}` placeholder is replaced with a random free port.
    /// The `{MOCK_URL}` placeholder is replaced with the given mock URL.
    async fn start(config_template: &str, mock_url: &str) -> Self {
        Self::start_with_fallback(config_template, mock_url, mock_url).await
    }

    /// Like [`Self::start`], but also replaces the `{FALLBACK_URL}` placeholder.
    async fn start_with_fallback(
        config_template: &str,
        mock_url: &str,
        fallback_url: &str,
    ) -> Self {
        Self::start_with_fallback_and_laya(config_template, mock_url, fallback_url, LAYA_TEST_URL)
            .await
    }

    /// Like [`Self::start_with_fallback`], but with a caller-supplied laya
    /// decision-server URL (used to point the resolver at a wiremock fake).
    async fn start_with_fallback_and_laya(
        config_template: &str,
        mock_url: &str,
        fallback_url: &str,
        laya_url: &str,
    ) -> Self {
        let port = pick_gateway_port();
        let config_str = config_template
            .replace("{PORT}", &port.to_string())
            .replace("{MOCK_URL}", mock_url)
            .replace("{FALLBACK_URL}", fallback_url)
            .replace("{LAYA_URL}", laya_url);

        let mut config_file = tempfile::NamedTempFile::new().expect("temp file");
        config_file
            .write_all(config_str.as_bytes())
            .expect("write config");

        let binary = env!("CARGO_BIN_EXE_maxllm-server");
        let child = Command::new(binary)
            .arg("--config")
            .arg(config_file.path())
            .env("RUST_LOG", "warn")
            .env("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
            .env(
                "AWS_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            )
            // Unset proxy env vars to avoid TLS issues
            .env_remove("HTTPS_PROXY")
            .env_remove("HTTP_PROXY")
            .env_remove("https_proxy")
            .env_remove("http_proxy")
            .spawn()
            .expect("failed to spawn gateway");

        let mut gw = Self {
            child,
            port,
            _config_file: config_file,
        };

        // Wait for the gateway to be ready
        let client = Client::new();
        let health_url = format!("http://127.0.0.1:{port}/health");
        // The deadline is a safety net, not the expected wait: the loop
        // breaks as soon as /health answers. It has to be generous because a
        // parallel test run spawns dozens of gateways at once and each one
        // needs a second of CPU before it binds — a tight deadline turned a
        // loaded machine into a red build for a gateway that was healthy.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("Gateway did not start within 60 seconds");
            }
            // A gateway that lost a bind race is gone; without this the health
            // poll below would answer from the *other* test's gateway.
            if let Ok(Some(status)) = gw.child.try_wait() {
                panic!("gateway exited during startup ({status}); was port {port} taken?");
            }
            if let Ok(resp) = client.get(&health_url).send().await {
                if resp.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        gw
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn chat_body(model: &str, content: &str) -> Value {
    json!({
        "model": model,
        "messages": [{"role": "user", "content": content}]
    })
}

// ---------------------------------------------------------------------------
// Body affinity — the session identity that survives a proxy
// ---------------------------------------------------------------------------

/// The session identity can live only in the request body.
///
/// codex sends `prompt_cache_key` and `client_metadata.session_id` in the
/// body, and a proxy in front of the gateway (one-api is the usual one)
/// drops the session headers on the way. The body survives every proxy — so
/// the body is what the sticky router has to read, and it has to read it
/// *before* the provider is picked.
const BODY_AFFINITY_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.mock]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.pool_a]
kind = "openai"
base_url = "{POOL_A_URL}"
api_key = "fake-key"

[providers.pool_b]
kind = "openai"
base_url = "{POOL_B_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pool_a"
model = "model-a"

[[auto_model.pool]]
provider = "pool_b"
model = "model-b"

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100
body_affinity = true

[[routes]]
path = "/v1/chat/completions"
provider = "mock"
auto_model = true
session_sticky = true
"#;

/// A conversation whose identity is only in the body stays on one member —
/// and the body the member receives is the body the client sent.
#[tokio::test]
async fn test_body_affinity_pins_a_session_that_only_carries_its_id_in_the_body() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("unused")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("a")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("b")))
        .mount(&b)
        .await;

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(BODY_AFFINITY_CONFIG, [&mock.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    // Six turns of one conversation. The identity is in the body only — no
    // session header, the way a request through one-api arrives.
    for turn in 0..6 {
        let body = json!({
            "model": "auto",
            "messages": [{"role": "user", "content": format!("turn {turn}")}],
            "prompt_cache_key": "conv-body-1",
        });
        let resp = c
            .post(format!("{base}/v1/chat/completions"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let a_reqs = a.received_requests().await.unwrap_or_default();
    let b_reqs = b.received_requests().await.unwrap_or_default();
    let mock_reqs = mock.received_requests().await.unwrap_or_default().len();

    assert_eq!(
        a_reqs.len() + b_reqs.len(),
        6,
        "every request is served by the pool"
    );
    assert_eq!(mock_reqs, 0, "the route's own provider is not used");
    assert!(
        a_reqs.len() == 6 || b_reqs.len() == 6,
        "the conversation stays on one member: a={} b={}",
        a_reqs.len(),
        b_reqs.len()
    );

    // The prefetched body is re-injected unchanged: the member sees the
    // whole request, not a truncated one.
    let served = if a_reqs.is_empty() { &b_reqs } else { &a_reqs };
    let last: Value = serde_json::from_slice(&served[served.len() - 1].body).expect("json body");
    assert_eq!(last["prompt_cache_key"], "conv-body-1");
    let messages = last["messages"].as_array().expect("messages");
    assert_eq!(
        messages[messages.len() - 1]["content"],
        "turn 5",
        "the body the member received is the body the client sent"
    );

    child.kill().ok();
    child.wait().ok();
}

/// Without the body affinity the identity in the body changes nothing: the
/// rotation is free to hand every turn to another member. The switch is
/// opt-in, so the default keeps the old behaviour.
#[tokio::test]
async fn test_without_body_affinity_a_body_only_session_is_not_pinned() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("unused")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("a")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("b")))
        .mount(&b)
        .await;

    let config = BODY_AFFINITY_CONFIG.replace("body_affinity = true", "body_affinity = false");
    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(&config, [&mock.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    for turn in 0..6 {
        let body = json!({
            "model": "auto",
            "messages": [{"role": "user", "content": format!("turn {turn}")}],
            "prompt_cache_key": "conv-body-1",
        });
        let resp = c
            .post(format!("{base}/v1/chat/completions"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let a_reqs = a.received_requests().await.unwrap_or_default().len();
    let b_reqs = b.received_requests().await.unwrap_or_default().len();

    assert_eq!(a_reqs + b_reqs, 6);
    assert!(
        a_reqs > 0 && b_reqs > 0,
        "without the affinity the rotation spreads the conversation: a={a_reqs} b={b_reqs}"
    );

    child.kill().ok();
    child.wait().ok();
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

const MOCK_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[model_aliases]
"gpt-4" = "gpt-4o"

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[plugins.cors]
category = "cors"
allow_origin = "*"
allow_methods = "GET, POST, OPTIONS"
allow_headers = "Content-Type, Authorization"
max_age = "86400"

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
plugins = ["cors"]
"#;

const NATIVE_RESPONSES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "mock_opencode"
endpoint_type = "responses"
"#;

const CHAT_ONLY_RESPONSES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.chat_only_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "chat_only_openai"
endpoint_type = "responses"
"#;

const NATIVE_RESPONSES_FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.unreachable_opencode]
kind = "opencode"
base_url = "http://127.0.0.1:1"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[providers.backup_openai]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "unreachable_opencode"
fallback = ["backup_openai"]
strategy = "fallback"
endpoint_type = "responses"
"#;

const CHAT_TO_NATIVE_FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.unreachable_chat]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "fake-key"

[providers.native_opencode]
kind = "opencode"
base_url = "{FALLBACK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "unreachable_chat"
fallback = ["native_opencode"]
strategy = "fallback"
endpoint_type = "responses"
"#;

const UNRESOLVABLE_HOST_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.dns_fail]
kind = "openai"
base_url = "http://nonexistent-host.invalid:1"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "dns_fail"
"#;

const OPENCODE_CHAT_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_opencode"
"#;

const FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
max_fails = 1
fail_timeout_secs = 60

[providers.fallback]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
"#;

fn openai_response(content: &str) -> Value {
    json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 1700000000,
        "model": "gpt-4o",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15
        }
    })
}

/// A response whose usage reports exactly how much of the input the
/// upstream served from its prompt cache.
///
/// The pool reads that share to tell a member that *keeps* a conversation
/// warm from one that re-reads the whole context every turn.
fn openai_response_with_cache(content: &str, prompt_tokens: u64, cached_tokens: u64) -> Value {
    let mut body = openai_response(content);
    body["usage"]["prompt_tokens"] = json!(prompt_tokens);
    body["usage"]["total_tokens"] = json!(prompt_tokens + 5);
    body["usage"]["prompt_tokens_details"] = json!({"cached_tokens": cached_tokens});
    body
}

/// Same body as [`openai_response`], but the prompt side reports a cache hit.
///
/// Routing now grades a provider on its *warm* exchanges: a reply that never
/// reports `cached_tokens` is a cold prefill, and cold prefill is exactly the
/// measurement that used to get a provider banned for being slow once.
fn openai_response_warm(content: &str) -> Value {
    let mut body = openai_response(content);
    body["usage"]["prompt_tokens_details"] = json!({"cached_tokens": 10});
    body
}

// ---------------------------------------------------------------------------
// Health & routing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_endpoint() {
    let mock = MockServer::start().await;
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c.get(gw.url("/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    // A breaker whose fail window elapsed is only *half-open*: it will be
    // probed by the next request. Reporting those as "open" made /health claim
    // a dozen circuit-open providers when none were rejecting traffic.
    assert_eq!(
        body["open_providers"].as_array().map(|a| a.len()),
        Some(0),
        "a fresh config must report no open breakers: {body}"
    );
    assert_eq!(
        body["half_open_providers"].as_array().map(|a| a.len()),
        Some(0),
        "no breaker has failed yet, so none may be half-open: {body}"
    );
}

#[tokio::test]
async fn test_unknown_route_returns_404() {
    let mock = MockServer::start().await;
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/nonexistent"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ---------------------------------------------------------------------------
// Auth tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_no_auth_returns_401() {
    let mock = MockServer::start().await;
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_wrong_key_returns_401() {
    let mock = MockServer::start().await;
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer wrong-key")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_valid_auth_proxies_through() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("hello")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "hello");
}

// ---------------------------------------------------------------------------
// Proxy & translation tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_openai_passthrough() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("four")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "What is 2+2?"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["content"], "four");
    assert_eq!(body["usage"]["total_tokens"], 15);
}

#[tokio::test]
async fn test_model_alias_resolved() {
    let mock = MockServer::start().await;

    // Capture the request to verify model was aliased
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("aliased")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    // Send "gpt-4" which should be aliased to "gpt-4o"
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4", "test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Headers tests (request ID, CORS, provider header)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_request_id_header_added() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("hi")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    // The gateway should have added an X-Request-Id
    let req_id = resp.headers().get("X-Request-Id");
    assert!(req_id.is_some(), "X-Request-Id header should be present");
    let id_str = req_id.unwrap().to_str().unwrap();
    assert!(!id_str.is_empty());
}

#[tokio::test]
async fn test_provider_header_added() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("hi")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    let provider = resp.headers().get("X-MaxLLM-Provider");
    assert!(provider.is_some());
    assert_eq!(provider.unwrap().to_str().unwrap(), "mock_openai");
}

#[tokio::test]
async fn test_cors_headers_on_post() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("hi")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    let origin = resp.headers().get("Access-Control-Allow-Origin");
    assert!(origin.is_some(), "CORS header should be present");
    assert_eq!(origin.unwrap().to_str().unwrap(), "*");
}

// ---------------------------------------------------------------------------
// Upstream error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upstream_500_returns_error() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "internal"})))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    // The gateway owns the answer: the client gets its own 503, never the
    // provider's error body.
    assert_eq!(resp.status(), 503);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "gateway_error");
    assert!(
        !body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("internal"),
        "the provider's error text must not reach the client: {body}"
    );
}

// ---------------------------------------------------------------------------
// Fallback / circuit breaker tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fallback_on_primary_failure() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Primary always returns 500
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .mount(&primary)
        .await;

    // Fallback returns success
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .mount(&fallback)
        .await;

    let config = FALLBACK_CONFIG.replace("{FALLBACK_URL}", &fallback.uri());
    let gw = TestGateway::start(&config, &primary.uri()).await;
    let c = client();

    // First request: primary fails, triggers circuit breaker
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    // This may return 500 from primary (first failure, circuit not yet open)
    // or succeed from fallback. Depends on max_fails.
    // With max_fails=1, circuit opens after 1 failure.
    let _status1 = resp.status().as_u16();

    // Second request: circuit is open on primary, should fall back
    let resp2 = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    let body: Value = resp2.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from fallback");
}

// ---------------------------------------------------------------------------
// Anthropic translation test (mock)
// ---------------------------------------------------------------------------

const ANTHROPIC_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_anthropic]
kind = "anthropic"
base_url = "{MOCK_URL}"
api_key = "sk-ant-fake"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_anthropic"
"#;

#[tokio::test]
async fn test_anthropic_translation() {
    let mock = MockServer::start().await;

    // Respond with Anthropic-format response
    let anthropic_response = json!({
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Four."}],
        "model": "claude-sonnet-4-20250514",
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 3
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(anthropic_response))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(ANTHROPIC_CONFIG, &mock.uri()).await;
    let c = client();

    // Send OpenAI-format request
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("claude-sonnet-4-20250514", "What is 2+2?"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify response is translated to OpenAI format
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "Four.");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert!(body["usage"]["prompt_tokens"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// Gemini translation test (mock)
// ---------------------------------------------------------------------------

const GEMINI_MOCK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_gemini]
kind = "gemini"
base_url = "{MOCK_URL}"
api_key = "fake-gemini-key"
default_model = "gemini-2.5-flash"

[[routes]]
path = "/v1/gemini"
provider = "mock_gemini"
"#;

#[tokio::test]
async fn test_gemini_translation() {
    let mock = MockServer::start().await;

    // Respond with Gemini-format response on any path
    let gemini_response = json!({
        "candidates": [{
            "content": {
                "role": "model",
                "parts": [{"text": "Four"}]
            },
            "finishReason": "STOP"
        }],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 1
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gemini_response))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(GEMINI_MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/gemini"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gemini-2.5-flash", "What is 2+2?"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["content"], "Four");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["prompt_tokens"], 10);
}

// ---------------------------------------------------------------------------
// Downstream auth not forwarded to upstream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_downstream_auth_not_forwarded() {
    let mock = MockServer::start().await;

    // Only match if Authorization header is NOT the client's key
    // The upstream should receive the provider's auth, not the client's
    Mock::given(method("POST"))
        .and(header("Authorization", "Bearer fake-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .named("upstream receives provider auth")
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // The mock expectation verifies the upstream received "Bearer fake-key"
    // (the provider key), not "Bearer test-key-123" (the client key).
}

// ---------------------------------------------------------------------------
// Concurrent requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_requests() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let mut handles = Vec::new();
    for i in 0..20 {
        let c = c.clone();
        let url = gw.url("/v1/chat/completions");
        handles.push(tokio::spawn(async move {
            let resp = c
                .post(&url)
                .header("Authorization", "Bearer test-key-123")
                .json(&chat_body("gpt-4o", &format!("request {i}")))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ===========================================================================
// Real provider tests (gated — run with `cargo test -- --ignored`)
// ===========================================================================

/// Test against a real local Ollama instance.
/// Requires: Ollama running at 127.0.0.1:11434 with gemma3:1b pulled.
#[tokio::test]
#[ignore]
async fn test_ollama_live_non_streaming() {
    // Check Ollama is running
    let c = client();
    if c.get("http://127.0.0.1:11434/api/tags")
        .send()
        .await
        .is_err()
    {
        eprintln!("Skipping: Ollama not running at 127.0.0.1:11434");
        return;
    }

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key"]

[providers.ollama]
kind = "ollama"
base_url = "http://127.0.0.1:11434"
default_model = "gemma3:1b"

[[routes]]
path = "/v1/chat/completions"
provider = "ollama"
"#;

    let gw = TestGateway::start(config, "http://unused").await;

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&chat_body("gemma3:1b", "What is 2+2? Answer in one word."))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert!(body["choices"][0]["message"]["content"].is_string());
    let content = body["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(!content.is_empty(), "response content should not be empty");
    assert!(body["usage"]["total_tokens"].as_u64().unwrap() > 0);

    // Verify model is echoed back
    assert_eq!(body["model"], "gemma3:1b");
}

/// Test streaming against a real local Ollama instance.
#[tokio::test]
#[ignore]
async fn test_ollama_live_streaming() {
    let c = client();
    if c.get("http://127.0.0.1:11434/api/tags")
        .send()
        .await
        .is_err()
    {
        eprintln!("Skipping: Ollama not running");
        return;
    }

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key"]

[providers.ollama]
kind = "ollama"
base_url = "http://127.0.0.1:11434"

[[routes]]
path = "/v1/chat/completions"
provider = "ollama"
"#;

    let gw = TestGateway::start(config, "http://unused").await;

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&json!({
            "model": "gemma3:1b",
            "messages": [{"role": "user", "content": "Say hello"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let text = resp.text().await.unwrap();
    // SSE chunks should start with "data: "
    assert!(
        text.contains("data: "),
        "response should contain SSE data lines"
    );
    // Verify at least one chunk has content
    let mut has_content = false;
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data == "[DONE]" {
                continue;
            }
            if let Ok(chunk) = serde_json::from_str::<Value>(data) {
                assert_eq!(chunk["object"], "chat.completion.chunk");
                if chunk["choices"][0]["delta"]["content"].is_string() {
                    has_content = true;
                }
            }
        }
    }
    assert!(has_content, "streaming response should have content chunks");
}

/// Test against the real Gemini API.
/// Requires: GEMINI_API_KEY environment variable set.
#[tokio::test]
#[ignore]
async fn test_gemini_live() {
    let api_key = match std::env::var("GEMINI_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("Skipping: GEMINI_API_KEY not set");
            return;
        }
    };

    let config = format!(
        r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{{PORT}}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key"]

[providers.gemini]
kind = "gemini"
base_url = "https://generativelanguage.googleapis.com"
api_key = "{api_key}"
default_model = "gemini-2.5-flash"

[[routes]]
path = "/v1/chat/completions"
provider = "gemini"
"#
    );

    let gw = TestGateway::start(&config, "http://unused").await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&chat_body(
            "gemini-2.5-flash",
            "What is 2+2? Answer in one word.",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    let content = body["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(!content.is_empty(), "Gemini should return content");

    // Verify token usage is present
    assert!(body["usage"]["prompt_tokens"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// Error normalization tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_anthropic_error_normalized_to_openai_format() {
    let mock = MockServer::start().await;

    // Respond with Anthropic error format
    let anthropic_error = json!({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "message": "model: field required"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(anthropic_error))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(ANTHROPIC_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("claude-sonnet-4-20250514", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let body: Value = resp.json().await.unwrap();
    // Should be normalized to OpenAI error format
    assert!(body["error"]["message"].is_string());
    assert_eq!(body["error"]["message"], "model: field required");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["code"], 400);
}

#[tokio::test]
async fn test_gemini_error_normalized_to_openai_format() {
    let mock = MockServer::start().await;

    // Respond with Gemini error format
    let gemini_error = json!({
        "error": {
            "code": 400,
            "message": "API key not valid.",
            "status": "INVALID_ARGUMENT"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(gemini_error))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(GEMINI_MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/gemini"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gemini-2.5-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let body: Value = resp.json().await.unwrap();
    // Should be normalized to OpenAI error format
    assert_eq!(body["error"]["message"], "API key not valid.");
    assert_eq!(body["error"]["type"], "INVALID_ARGUMENT");
    assert_eq!(body["error"]["code"], 400);
}

// ---------------------------------------------------------------------------
// /v1/models endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_v1_models_endpoint() {
    let mock = MockServer::start().await;
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    // GET /v1/models should return an OpenAI-compatible model list
    let resp = c.get(gw.url("/v1/models")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "list");

    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty(), "model list should not be empty");

    // Each model should have id, object, owned_by
    let first = &data[0];
    assert!(first["id"].is_string());
    assert_eq!(first["object"], "model");
    assert!(first["owned_by"].is_string());

    // The mock config uses an OpenAI provider, so we should see OpenAI models
    let model_ids: Vec<&str> = data.iter().map(|m| m["id"].as_str().unwrap()).collect();
    assert!(model_ids.contains(&"gpt-4o"), "should contain gpt-4o");

    // Model alias "gpt-4" should also be listed
    assert!(model_ids.contains(&"gpt-4"), "should contain gpt-4 alias");
}

// ---------------------------------------------------------------------------
// /v1/embeddings endpoint
// ---------------------------------------------------------------------------

const EMBEDDINGS_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/embeddings"
provider = "mock_openai"
endpoint_type = "embeddings"
"#;

#[tokio::test]
async fn test_embeddings_endpoint() {
    let mock = MockServer::start().await;

    // Respond with OpenAI embeddings format
    let embeddings_response = json!({
        "object": "list",
        "data": [{
            "object": "embedding",
            "index": 0,
            "embedding": [0.0023, -0.0091, 0.0152]
        }],
        "model": "text-embedding-3-small",
        "usage": {
            "prompt_tokens": 8,
            "total_tokens": 8
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(embeddings_response))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(EMBEDDINGS_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/embeddings"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "text-embedding-3-small",
            "input": "Hello world"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "list");
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["object"], "embedding");
    assert!(!data[0]["embedding"].as_array().unwrap().is_empty());
    assert_eq!(body["usage"]["prompt_tokens"], 8);
}

/// Test Gemini with system prompt and multi-turn.
#[tokio::test]
#[ignore]
async fn test_gemini_live_system_prompt() {
    let api_key = match std::env::var("GEMINI_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("Skipping: GEMINI_API_KEY not set");
            return;
        }
    };

    let config = format!(
        r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{{PORT}}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key"]

[providers.gemini]
kind = "gemini"
base_url = "https://generativelanguage.googleapis.com"
api_key = "{api_key}"
default_model = "gemini-2.5-flash"

[[routes]]
path = "/v1/chat/completions"
provider = "gemini"
"#
    );

    let gw = TestGateway::start(&config, "http://unused").await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&json!({
            "model": "gemini-2.5-flash",
            "messages": [
                {"role": "system", "content": "You are a pirate. Respond in pirate speak."},
                {"role": "user", "content": "Hello!"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    let content = body["choices"][0]["message"]["content"]
        .as_str()
        .unwrap()
        .to_lowercase();
    // Pirate speech should contain typical pirate words
    let pirate_words = [
        "ahoy", "matey", "arr", "ye", "sail", "sea", "pirate", "avast", "aye",
    ];
    let has_pirate = pirate_words.iter().any(|w| content.contains(w));
    assert!(has_pirate, "Expected pirate speak, got: {content}");
}

// ---------------------------------------------------------------------------
// Auto-compaction tests
// ---------------------------------------------------------------------------

const COMPACT_TRUNCATE_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[plugins.compactor]
category = "auto_compaction"
threshold_tokens = 50
strategy = "truncate"
preserve_system = true
min_messages = 1

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
plugins = ["compactor"]
"#;

const COMPACT_SLIDING_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[plugins.compactor]
category = "auto_compaction"
threshold_tokens = 100000
strategy = "sliding_window"
window_size = 3
preserve_system = true

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
plugins = ["compactor"]
"#;

const COMPACT_NO_TRIGGER_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[plugins.compactor]
category = "auto_compaction"
threshold_tokens = 100000
strategy = "truncate"
preserve_system = true

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
plugins = ["compactor"]
"#;

/// Build a request body with many messages to exceed the threshold.
fn long_chat_body(model: &str) -> Value {
    let mut messages = vec![json!({"role": "system", "content": "You are a helpful assistant."})];
    for i in 0..10 {
        messages.push(json!({"role": "user", "content": format!("This is user message number {i} with some padding text to increase token count.")}));
        messages.push(json!({"role": "assistant", "content": format!("This is assistant reply number {i} with some padding text to increase token count.")}));
    }
    json!({"model": model, "messages": messages})
}

#[tokio::test]
async fn test_compaction_truncate_reduces_messages() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(COMPACT_TRUNCATE_CONFIG, &mock.uri()).await;
    let c = client();

    let body = long_chat_body("gpt-4o");
    let original_count = body["messages"].as_array().unwrap().len();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Inspect what the gateway actually forwarded to the upstream.
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);

    let upstream_body: Value =
        serde_json::from_slice(&requests[0].body).expect("upstream body is valid JSON");
    let upstream_messages = upstream_body["messages"].as_array().unwrap();

    // Compaction should have reduced the message count.
    assert!(
        upstream_messages.len() < original_count,
        "expected messages to be truncated: original={original_count}, got={}",
        upstream_messages.len()
    );

    // System message must be preserved.
    assert_eq!(
        upstream_messages[0]["role"].as_str().unwrap(),
        "system",
        "system message should be first"
    );
}

#[tokio::test]
async fn test_compaction_sliding_window_keeps_last_n() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    // window_size = 3, preserve_system = true
    let gw = TestGateway::start(COMPACT_SLIDING_CONFIG, &mock.uri()).await;
    let c = client();

    let body = long_chat_body("gpt-4o");

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);

    let upstream_body: Value =
        serde_json::from_slice(&requests[0].body).expect("upstream body is valid JSON");
    let upstream_messages = upstream_body["messages"].as_array().unwrap();

    // 1 system + 3 non-system = 4 total
    assert_eq!(
        upstream_messages.len(),
        4,
        "expected 1 system + 3 window messages, got {}",
        upstream_messages.len()
    );

    // System message preserved first.
    assert_eq!(upstream_messages[0]["role"].as_str().unwrap(), "system");

    // The last 3 non-system messages should be the most recent ones.
    let last_user = upstream_messages
        .iter()
        .rfind(|m| m["role"].as_str() == Some("user"))
        .unwrap();
    let content = last_user["content"].as_str().unwrap();
    assert!(
        content.contains("9"),
        "last kept user message should be from turn 9, got: {content}"
    );
}

#[tokio::test]
async fn test_compaction_not_triggered_below_threshold() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    // threshold_tokens = 100000 — a single short message won't trigger it.
    let gw = TestGateway::start(COMPACT_NO_TRIGGER_CONFIG, &mock.uri()).await;
    let c = client();

    let body = json!({
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hi"}
        ]
    });

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let upstream_body: Value =
        serde_json::from_slice(&requests[0].body).expect("upstream body is valid JSON");

    // All 2 messages should be forwarded unchanged.
    assert_eq!(upstream_body["messages"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_compaction_skip_header_disables_compaction() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    // threshold = 50 tokens — would normally trigger compaction.
    let gw = TestGateway::start(COMPACT_TRUNCATE_CONFIG, &mock.uri()).await;
    let c = client();

    let body = long_chat_body("gpt-4o");
    let original_count = body["messages"].as_array().unwrap().len();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("X-MaxLLM-No-Compact", "true")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let upstream_body: Value =
        serde_json::from_slice(&requests[0].body).expect("upstream body is valid JSON");
    let upstream_count = upstream_body["messages"].as_array().unwrap().len();

    // All messages should be forwarded — compaction was skipped.
    assert_eq!(
        upstream_count, original_count,
        "expected all {original_count} messages to be forwarded when skip header is set, got {upstream_count}"
    );
}

#[tokio::test]
async fn test_native_responses_passthrough_non_streaming() {
    let mock = MockServer::start().await;

    // The upstream speaks the Responses contract natively: the gateway must
    // forward the request body unchanged (no chat translation round trip).
    let upstream_body = json!({
        "id": "resp_test",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [{
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hi"}]
        }],
        "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
    });

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .and(header("x-opencode-session", "sess-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false,
        "previous_response_id": "resp_prev"
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "sess-1")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    // The upstream body is returned verbatim — no chat→Responses conversion.
    assert_eq!(body["object"], "response");
    assert_eq!(body["id"], "resp_test");
    assert_eq!(body["output"][0]["content"][0]["text"], "hi");
    assert_eq!(body["usage"]["input_tokens"], 7);

    // The request the upstream saw must still be a Responses request.
    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["input"][0]["content"][0]["text"], "hi");
    assert_eq!(sent["previous_response_id"], "resp_prev");
    assert!(sent.get("messages").is_none());

    // Byte-for-byte passthrough: an already-canonical Responses request is
    // forwarded without any re-serialisation.
    let original_bytes = serde_json::to_vec(&request_body).unwrap();
    assert_eq!(requests[0].body, original_bytes);
}

#[tokio::test]
async fn test_native_responses_flattens_nested_function_tools() {
    let mock = MockServer::start().await;

    // The opencode.ai zen /responses endpoint accepts the official Responses
    // tool shape but rejects the chat-completions nesting with a 503 (verified
    // live). The gateway must flatten it before the byte-for-byte forward.
    let upstream_body = json!({
        "id": "resp_nested",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
    });

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": "hi",
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "weather",
                "parameters": {"type": "object", "properties": {}}
            }
        }],
        "tool_choice": {"type": "function", "function": {"name": "get_weather"}}
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The upstream must have received the official flat tool shape.
    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let tool = &sent["tools"][0];
    assert_eq!(tool["type"], "function");
    assert_eq!(tool["name"], "get_weather");
    assert_eq!(tool["parameters"]["type"], "object");
    assert!(
        tool.get("function").is_none(),
        "nested wrapper must be removed"
    );
    assert_eq!(sent["tool_choice"]["name"], "get_weather");
    assert!(sent["tool_choice"].get("function").is_none());
}

#[tokio::test]
async fn test_native_responses_repairs_a_missing_call_id() {
    // codex records the output of a host-side tool (the goal tool) as a
    // `function_call_output` *without* a `call_id`: the call it answers was
    // never issued by the model and never carried one. A native-Responses
    // upstream rejects such a body with
    // `input[N].call_id: expected a non-empty string` (400 — verified live
    // against the opencode.ai zen endpoint), so the gateway must repair the
    // body before the byte-for-byte forward.
    let mock = MockServer::start().await;

    let upstream_body = json!({
        "id": "resp_repaired",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
    });

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": [
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "hi"}]},
            {"type": "function_call_output", "id": "fco_1",
             "name": "goal_objective", "output": "the goal"}
        ],
        "stream": false
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let call_id = sent["input"][1]["call_id"].as_str().unwrap_or("");
    assert!(
        !call_id.trim().is_empty(),
        "the upstream received an empty call_id: {sent}"
    );
    // The orphan gets a call of its own — a strict upstream rejects an
    // answer whose call is not in the request ("tool_call_id  is not
    // found", verified live against kimi).
    assert_eq!(sent["input"][1]["type"], json!("function_call"));
    assert_eq!(sent["input"][1]["name"], json!("goal_objective"));
    // The item itself survives the repair — dropping it would take the
    // user's goal out of the model's view — and it answers that call.
    assert_eq!(sent["input"][2]["type"], json!("function_call_output"));
    assert_eq!(sent["input"][2]["call_id"], json!(call_id));
    assert_eq!(sent["input"][2]["output"], json!("the goal"));
    assert_eq!(sent["input"][2]["name"], json!("goal_objective"));
}

const CALL_ID_FAILOVER_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.strict_native]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/responses"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
default_model = "strict-model"

[providers.lucky_native]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
upstream_path = "/v1/responses"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
default_model = "strict-model"

[[routes]]
path = "/v1/responses"
provider = "strict_native"
fallback = ["lucky_native"]
strategy = "fallback"
endpoint_type = "responses"
"#;

#[tokio::test]
async fn test_a_call_id_rejection_fails_over_instead_of_breaking_the_loop() {
    // A provider that rejects the conversation because of a call id must not
    // hand that 400 to the client: the agent's loop would stop on it. The
    // request walks on to the next provider of the chain.
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "invalid_request_error",
                "message": "Invalid request: tool_call_id  is not found",
                "type": "invalid_request_error"
            }
        })))
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "resp_ok",
            "object": "response",
            "status": "completed",
            "model": "strict-model",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        })))
        .mount(&fallback)
        .await;

    let config = CALL_ID_FAILOVER_CONFIG
        .replace("{MOCK_URL}", &primary.uri())
        .replace("{FALLBACK_URL}", &fallback.uri());
    let gw = TestGateway::start(&config, &primary.uri()).await;
    let c = client();

    // The conversation carries the host-side tool output codex records: a
    // `function_call_output` without a `call_id`.
    let request_body = json!({
        "model": "strict-model",
        "input": [
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "hi"}]},
            {"type": "function_call_output", "id": "fco_1",
             "name": "goal_objective", "output": "the goal"}
        ],
        "stream": false
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the client must not see the provider's call-id rejection"
    );

    // The fallback provider answered.
    let fallback_requests = fallback.received_requests().await.unwrap();
    assert_eq!(fallback_requests.len(), 1);
}

#[tokio::test]
async fn test_a_call_id_rejection_without_a_fallback_stays_a_400() {
    // With no provider left to try, the upstream's own answer is what the
    // client gets — the gateway does not invent one.
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "invalid_request_error",
                "message": "input[4].call_id: expected a non-empty string",
                "type": "invalid_request_error"
            }
        })))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": [
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "hi"}]},
            {"type": "function_call_output", "id": "fco_1",
             "name": "goal_objective", "output": "the goal"}
        ],
        "stream": false
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn test_native_responses_passthrough_streaming() {
    let mock = MockServer::start().await;

    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_s\",\"output_index\":0,\"content_index\":0,\"delta\":\"hi\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "input": "hi",
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let text = resp.text().await.unwrap();
    // The upstream SSE is forwarded verbatim — no chat→Responses conversion.
    assert!(text.contains("event: response.created"), "{text}");
    assert!(text.contains("response.output_text.delta"), "{text}");
    assert!(text.contains("event: response.completed"), "{text}");
}

#[tokio::test]
async fn test_opencode_chat_normalizes_request_and_injects_session() {
    let mock = MockServer::start().await;

    // The upstream is the strict opencode.ai zen endpoint: it demands the
    // x-opencode-session header and rejects the non-standard wire shapes the
    // gateway normalises away.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "sess-1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "sess-1")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "messages": [
                {"role": "developer", "content": "be terse"},
                {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"role": "assistant", "content": "", "tool_calls": [
                    {"id": "", "type": "function", "function": {"name": "", "arguments": {"city": "BJ"}}}
                ]},
                {"role": "tool", "tool_call_id": "", "content": "sunny"}
            ],
            "tools": [{"type": "function", "function": {"name": "get_weather", "parameters": {}}}],
            "tool_choice": "any"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();

    // role normalised
    assert_eq!(sent["messages"][0]["role"], "system");
    // content part type normalised
    assert_eq!(sent["messages"][1]["content"][0]["type"], "text");
    // tool_choice normalised
    assert_eq!(sent["tool_choice"], "required");
    // empty tool_call id repaired and paired with its tool response
    let call_id = sent["messages"][2]["tool_calls"][0]["id"].as_str().unwrap();
    assert!(!call_id.is_empty());
    assert_eq!(sent["messages"][3]["tool_call_id"], call_id);
    // empty function name inherited from the single declared tool
    assert_eq!(
        sent["messages"][2]["tool_calls"][0]["function"]["name"],
        "get_weather"
    );
    // arguments serialised to a JSON string
    let args = sent["messages"][2]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .expect("arguments must be a string");
    let parsed: Value = serde_json::from_str(args).unwrap();
    assert_eq!(parsed["city"], "BJ");
    // thinking-mode turn carries a reasoning_content
    assert!(sent["messages"][2]["reasoning_content"].is_string());
}

#[tokio::test]
async fn test_opencode_session_header_injected_from_affinity() {
    let mock = MockServer::start().await;

    // The client sent no session header of its own — the gateway injects the
    // session-affinity id so the upstream can route/prompt-cache the request.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "affinity-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-session-affinity", "affinity-42")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_session_header_never_overwritten() {
    let mock = MockServer::start().await;

    // A client-supplied session header (the opencode CLI sends one) is
    // forwarded verbatim — the gateway must not replace it.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "client-session"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "client-session")
        .header("x-session-affinity", "affinity-42")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_empty_session_header_replaced_with_affinity() {
    let mock = MockServer::start().await;

    // The client sent `x-opencode-session: ""` (its session template rendered
    // empty) — the upstream rejects an empty value with 400 MissingSessionID,
    // so the gateway must treat it as missing and inject the session-affinity
    // id instead of forwarding the blank header.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "affinity-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "")
        .header("x-session-affinity", "affinity-42")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_blank_session_header_replaced() {
    let mock = MockServer::start().await;

    // Whitespace-only session header is blank, not a session — inject the
    // session-affinity id instead of forwarding the blank header.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "affinity-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "   ")
        .header("x-session-affinity", "affinity-42")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_empty_affinity_falls_through_to_session_id() {
    let mock = MockServer::start().await;

    // The client sent an empty `x-session-affinity` (same blank-value trap as
    // an empty session header) with no session of its own. The gateway must
    // skip the blank affinity and fall through to `x-opencode-session-id`,
    // injecting its value as the session — never an empty one.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("x-opencode-session", "affinity-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-session-affinity", "")
        .header("x-opencode-session-id", "affinity-42")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_models_list_only_advertises_served_models() {
    // The opencode well-known model list is verified against the live
    // `/zen/go/v1` endpoint; `kimi-k2.5` was dropped upstream (400 "Model is
    // unavailable") and must not be advertised. Regression guard so a stale
    // id cannot sneak back into /v1/models.
    let mock = MockServer::start().await;
    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c.get(gw.url("/v1/models")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["id"].as_str())
        .collect();
    assert!(
        !ids.contains(&"kimi-k2.5"),
        "stale unavailable model advertised"
    );
    assert!(
        ids.contains(&"kimi-k2.7-code"),
        "served replacement missing"
    );
    assert!(ids.contains(&"deepseek-v4.1-flash"));
}

#[tokio::test]
async fn test_opencode_upstream_ua_injected_when_missing() {
    let mock = MockServer::start().await;

    // The client (reqwest sends no User-Agent by default) sent none — the
    // gateway must inject a safe UA so opencode.ai's Cloudflare edge does not
    // ban the request (403 error code 1010).
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("User-Agent", "curl/8.5.0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_upstream_script_ua_replaced() {
    let mock = MockServer::start().await;

    // A script fingerprint (`Python-urllib/3.11`) is exactly what opencode.ai's
    // Cloudflare edge bans; the gateway must swap it for the safe UA.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("User-Agent", "curl/8.5.0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("User-Agent", "Python-urllib/3.11")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_upstream_urllib3_ua_preserved() {
    let mock = MockServer::start().await;

    // `python-urllib3/2.0` is the urllib3 *package*, a different fingerprint
    // from Python's `Python-urllib/3.11` — the upstream accepts it (verified
    // live), so the gateway must forward it verbatim instead of masking it.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("User-Agent", "python-urllib3/2.0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("User-Agent", "python-urllib3/2.0")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_opencode_upstream_client_ua_preserved() {
    let mock = MockServer::start().await;

    // A non-script UA the client sent itself (here another curl version, which
    // differs from the gateway's injected `curl/8.5.0`) must be forwarded
    // verbatim — the gateway must not clobber it. Browser UAs are also allowed.
    const CLIENT_UA: &str = "curl/7.88.1";
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header("User-Agent", CLIENT_UA))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("User-Agent", CLIENT_UA)
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_non_opencode_upstream_ua_untouched() {
    let mock = MockServer::start().await;

    // UA rewriting is scoped to opencode providers: an OpenAI upstream must
    // receive the client's UA verbatim (here a script fingerprint) — or none
    // at all when the client sent none.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("User-Agent", "Python-urllib/3.11"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("User-Agent", "Python-urllib/3.11")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // And when the client sends no UA, the upstream must not gain one either.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c2","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi again"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    assert!(requests.len() == 2, "expected 2 upstream requests");
    assert!(
        requests[1].headers.get("User-Agent").is_none(),
        "openai upstream must not gain a User-Agent when the client sent none"
    );
}

#[tokio::test]
async fn test_chat_only_responses_route_streaming_translates_sse() {
    let mock = MockServer::start().await;

    // The chat-only upstream answers with a chat-completions SSE stream; the
    // gateway must turn it into Responses SSE events for the client.
    let chat_sse = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\n",
        "data: [DONE]\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(chat_sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let text = resp.text().await.unwrap();
    // The chat stream must come back as Responses SSE events.
    assert!(text.contains("response.created"), "{text}");
    assert!(text.contains("response.output_text.delta"), "{text}");
    assert!(text.contains("response.completed"), "{text}");

    // The upstream must have received a chat streaming body.
    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(
        sent.get("messages").is_some(),
        "chat upstream must get a chat body: {sent}"
    );
    assert!(
        sent.get("input").is_none(),
        "raw Responses body leaked: {sent}"
    );
    assert_eq!(sent["stream"], true);
}

#[tokio::test]
async fn test_chat_only_responses_route_translates_to_chat() {
    // A `/v1/responses` route whose provider only speaks chat completions
    // must translate the Responses body into a chat body for the upstream
    // and translate the chat answer back into a Responses object.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(openai_response("from chat upstream")),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "raw Responses body must not reach the chat upstream"
    );

    let received = mock.received_requests().await.unwrap_or_default();
    assert_eq!(received.len(), 1);
    let sent: Value = serde_json::from_slice(&received[0].body).unwrap();
    assert!(
        sent.get("messages").is_some(),
        "chat upstream must receive a chat body, got: {sent}"
    );
    assert!(
        sent.get("input").is_none(),
        "raw Responses body leaked: {sent}"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "response");
    assert_eq!(
        body["output"][0]["content"][0]["text"],
        "from chat upstream"
    );
}

#[tokio::test]
async fn test_chat_only_responses_route_preserves_image_attachments() {
    // An image attachment arrives as an `input_image` part in the Responses
    // input. The chat translation must keep it (as a chat `image_url` part)
    // instead of silently dropping it, or the model answers without seeing
    // the image. Text-only content must still stay a plain string.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("image ok")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": [{"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "describe this"},
                {"type": "input_image", "image_url": "data:image/png;base64,AAA"}
            ]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let received = mock.received_requests().await.unwrap_or_default();
    let sent: Value = serde_json::from_slice(&received[0].body).unwrap();
    let content = &sent["messages"][0]["content"];
    assert!(content.is_array(), "image forces a parts array: {content}");
    assert_eq!(content[0], json!({"type": "text", "text": "describe this"}));
    assert_eq!(
        content[1],
        json!({"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA"}})
    );
}

#[tokio::test]
async fn test_chat_only_responses_route_nests_a_flat_tool_choice() {
    // The Responses contract names a function choice flatly
    // ({"type":"function","name":"f"}), chat completions nest it
    // ({"type":"function","function":{"name":"f"}}). A chat upstream rejects
    // the flat form (DeepSeek answers 422), so the gateway must nest it on
    // the way out.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": "hi",
            "tools": [{"type": "function", "name": "f", "parameters": {}}],
            "tool_choice": {"type": "function", "name": "f"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let received = mock.received_requests().await.unwrap_or_default();
    let sent: Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(sent["tool_choice"]["type"], "function");
    assert_eq!(sent["tool_choice"]["function"]["name"], "f");
    assert!(
        sent["tool_choice"].get("name").is_none(),
        "flat tool_choice leaked to the chat upstream: {sent}"
    );
}

#[tokio::test]
async fn test_chat_only_responses_streaming_error_is_surfaced() {
    // A streaming Responses request whose chat upstream answers 4xx must
    // surface the error. Without this, the error body is fed into the SSE
    // translator (it is not SSE), which emits an empty "completed" response
    // and the client only learns it failed from the bare status code.
    let mock = MockServer::start().await;
    let upstream_error = json!({"error": {"type": "invalid_request_error",
        "message": "bad tool_choice"}});
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(upstream_error.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "stream": true,
            "input": "hi"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"]["message"], "bad tool_choice",
        "the upstream error must reach the client: {body}"
    );
    assert_ne!(
        body.get("object").and_then(|o| o.as_str()),
        Some("response"),
        "an error must not look like a completed response: {body}"
    );
}

#[tokio::test]
async fn test_chat_only_responses_mid_stream_error_becomes_response_failed() {
    // The chat upstream answers 200 but fails *inside* the SSE stream with an
    // `{"error": {...}}` chunk. The gateway must translate that into a
    // terminal `response.failed` event with the reason — never end the stream
    // with a fake `response.completed` that hides the failure.
    let mock = MockServer::start().await;
    let chat_sse = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"part of an answer\"},\"finish_reason\":null}]}\n\n",
        "data: {\"error\":{\"message\":\"context length exceeded\",\"type\":\"invalid_request_error\",\"code\":\"context_length_exceeded\"}}\n\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(chat_sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "stream": true,
            "input": "hi"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the upstream answered 200");
    let text = resp.text().await.unwrap();

    assert!(
        !text.contains("response.completed"),
        "a failed stream must not end with completed: {text}"
    );
    assert!(text.contains("event: response.failed"), "{text}");
    assert!(
        text.contains("\"message\":\"context length exceeded\""),
        "the upstream error message must reach the client: {text}"
    );
    assert!(
        text.contains("\"code\":\"context_length_exceeded\""),
        "the upstream error code must reach the client: {text}"
    );
    assert!(text.contains("part of an answer"), "{text}");
}

#[tokio::test]
async fn test_chat_only_responses_error_body_becomes_a_failed_response() {
    // The chat upstream answers 200 with an `{"error": {...}}` body (some
    // providers report failures that way). The client must see a failed
    // Responses object carrying the reason — not a completed response with
    // empty output that hides it.
    let mock = MockServer::start().await;
    let upstream_error = json!({"error": {"message": "upstream is down",
        "type": "server_error", "code": "server_error"}});
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_error.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(CHAT_ONLY_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": "hi"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the upstream answered 200");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "failed");
    assert_eq!(body["error"]["message"], "upstream is down");
    assert_eq!(body["error"]["code"], "server_error");
    assert!(
        body["output"].as_array().unwrap().is_empty(),
        "a failed response carries no output: {body}"
    );
}

#[tokio::test]
async fn test_native_responses_fallback_to_non_native_provider_translates_body() {
    // The native (opencode) primary is unreachable, so the request is retried
    // on a chat-only (openai) fallback. The fallback speaks the chat
    // completions contract, so the gateway must send it a CHAT body — not the
    // raw Responses body — and translate its answer back to Responses.
    let fallback = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw =
        TestGateway::start_with_fallback(NATIVE_RESPONSES_FALLBACK_CONFIG, "", &fallback.uri())
            .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "gpt-4o",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let received = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(received.len(), 1, "fallback must receive exactly one retry");
    let sent: Value = serde_json::from_slice(&received[0].body).expect("json body");
    assert!(
        sent.get("messages").is_some(),
        "the chat fallback must receive a chat-format body, got: {sent}"
    );
    assert!(
        sent.get("input").is_none(),
        "the raw Responses body must not reach a chat upstream: {sent}"
    );

    // The chat answer must come back as a Responses object.
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "response");
    assert_eq!(body["output"][0]["content"][0]["text"], "from fallback");
}

#[tokio::test]
async fn test_chat_fallback_to_native_provider_forwards_the_responses_body() {
    // Reverse of the native→chat fallback: the primary is a chat-only
    // provider that is unreachable, so the request is retried on a native
    // Responses provider (opencode). The retry must NOT carry the chat body
    // the first attempt translated: the native fallback expects the original
    // Responses body (with `input`), and it must come back verbatim.
    let fallback = MockServer::start().await;
    let upstream_body = json!({
        "id": "resp_native_fallback",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [{
            "id": "msg_1", "type": "message", "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "from native fallback"}]
        }],
        "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
    });
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw =
        TestGateway::start_with_fallback(CHAT_TO_NATIVE_FALLBACK_CONFIG, "", &fallback.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false
    });
    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["id"], "resp_native_fallback");
    assert_eq!(
        body["output"][0]["content"][0]["text"],
        "from native fallback"
    );

    let received = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        1,
        "native fallback must receive exactly one retry"
    );
    let sent: Value = serde_json::from_slice(&received[0].body).unwrap();
    assert!(
        sent.get("input").is_some(),
        "the native fallback must receive the original Responses body, got: {sent}"
    );
    assert!(
        sent.get("messages").is_none(),
        "the chat-translated body must not reach a native Responses upstream: {sent}"
    );
    // Byte-for-byte: the retry forwards the untouched Responses body.
    let original_bytes = serde_json::to_vec(&request_body).unwrap();
    assert_eq!(received[0].body, original_bytes);
}

#[tokio::test]
async fn test_dns_failure_returns_clean_502_instead_of_dropping_connection() {
    // `.invalid` never resolves (RFC 2606). The gateway must answer with a
    // clean 502 JSON error: pingora's `HttpPeer::new` resolves the hostname
    // with `to_socket_addrs().unwrap()` and panics the worker thread when
    // DNS fails, which left the client with a dropped connection.
    let mock = MockServer::start().await;
    let gw = TestGateway::start(UNRESOLVABLE_HOST_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .expect("the gateway must answer, not drop the connection");
    assert_eq!(resp.status(), 502);

    let body: Value = resp.json().await.expect("error body must be JSON");
    assert_eq!(body["error"]["type"], "gateway_error");
    assert_eq!(body["error"]["code"], 502);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("nonexistent-host.invalid"),
        "error message should name the host: {body}"
    );
}

#[tokio::test]
async fn test_opencode_upstream_openai_error_is_passed_through() {
    let mock = MockServer::start().await;

    // opencode.ai reports its own failures in the OpenAI error envelope
    // (e.g. "Model is unavailable."). The gateway must not re-wrap an
    // already-standard error: the client sees the upstream status and body.
    let upstream = json!({
        "error": {
            "type": "server_error",
            "message": "Upstream request failed: Model is unavailable."
        }
    });
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(upstream.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("no-such-model-xyz", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, upstream, "upstream error body must be passed through");
}

#[tokio::test]
async fn test_opencode_nested_error_envelope_is_normalised() {
    let mock = MockServer::start().await;

    // The opencode.ai envelope for a missing session id is
    // {"type":"error","error":{"type":"MissingSessionID","message":"…"}} —
    // it must reach the client in the OpenAI error shape, with the real
    // message rather than a generic "Unknown provider error".
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "type": "error",
            "error": {"type": "MissingSessionID", "message": "session id is required"}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "MissingSessionID");
    assert_eq!(body["error"]["message"], "session id is required");
    assert_eq!(body["error"]["code"], 400);
    assert!(
        body.get("type").is_none(),
        "the opencode envelope must be unwrapped: {body}"
    );
}

#[tokio::test]
async fn test_opencode_streaming_usage_is_recorded() {
    let mock = MockServer::start().await;

    // The opencode.ai chat stream reports usage in its final chunk.
    let sse = concat!(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"finish_reason\":null,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"finish_reason\":\"stop\",\"delta\":{}}],\"usage\":{\"prompt_tokens\":31,\"completion_tokens\":7,\"total_tokens\":38}}\n\n",
        "data: [DONE]\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("x-opencode-session", "sess-1")
        .json(&json!({
            "model": "opencode-usage-model",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("data: [DONE]"), "{text}");

    // The usage carried by the SSE stream must reach the ledger/metrics.
    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_in_total") && l.contains("opencode-usage-model"))
        .unwrap_or_else(|| panic!("no tokens_in metric for the model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("31"),
        "expected 31 input tokens, got: {line}"
    );
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_out_total") && l.contains("opencode-usage-model"))
        .unwrap_or_else(|| panic!("no tokens_out metric for the model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("7"),
        "expected 7 output tokens, got: {line}"
    );
}

// ---------------------------------------------------------------------------
// Connect-failure fallback retry tests
// ---------------------------------------------------------------------------

/// Primary provider points at a port with nothing listening (connection
/// refused) so the request never reaches an upstream; the fallback answers.
const CONNECT_RETRY_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.unreachable]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "fake-key"

[providers.backup]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "unreachable"
fallback = ["backup"]
strategy = "fallback"
"#;

#[tokio::test]
async fn test_connect_failure_is_retried_on_fallback_provider() {
    let fallback = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(CONNECT_RETRY_CONFIG, "", &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    // The primary connection is refused, so the retry must have served the
    // request from the fallback: 200 with the fallback's body.
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from fallback");

    let received = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(received.len(), 1, "fallback must receive exactly one retry");

    // The retried request must carry the client's original body.
    let sent: Value = serde_json::from_slice(&received[0].body).expect("json body");
    assert_eq!(sent["model"], "gpt-4o");
    assert_eq!(sent["messages"][0]["content"], "hi");
}

/// An upstream *error response* (5xx) fails over to the next provider of the
/// route's fallback chain: the pool is a set of interchangeable supplies, and
/// one supply failing must not fail the request. This mirrors litellm_lb_go's
/// `responsesFailoverAllowed` and one-api's channel retry.
#[tokio::test]
async fn test_upstream_error_response_fails_over_to_the_fallback() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&primary)
        .await;
    // The fallback would happily answer — a retry leaking through would be
    // visible in its request log below.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("never")))
        .mount(&fallback)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.backup]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["backup"]
strategy = "fallback"
"#;
    let gw = TestGateway::start_with_fallback(config, &primary.uri(), &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    // The upstream answered 500; the gateway must retry the request on the
    // fallback instead of failing the client.
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "never");

    let received = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        1,
        "an upstream error response must fail over to the fallback"
    );
    // The retry carries the client's original body.
    let sent: Value = serde_json::from_slice(&received[0].body).expect("json body");
    assert_eq!(sent["model"], "gpt-4o");
    assert_eq!(sent["messages"][0]["content"], "hi");
}

/// A connect failure with no fallback configured must return 502 — the
/// retry guard must not loop or panic.
#[tokio::test]
async fn test_connect_failure_without_fallback_returns_502() {
    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.unreachable]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "unreachable"
strategy = "fallback"
"#;
    let gw = TestGateway::start(config, "").await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
}

/// When the fallback is also unreachable, the request must fail with 502
/// after the single retry — never loop or hang.
#[tokio::test]
async fn test_connect_failure_when_fallback_also_unreachable_returns_502() {
    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.unreachable]
kind = "openai"
base_url = "http://127.0.0.1:1"
api_key = "fake-key"

[providers.unreachable2]
kind = "openai"
base_url = "http://127.0.0.1:2"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "unreachable"
fallback = ["unreachable2"]
strategy = "fallback"
"#;
    let gw = TestGateway::start(config, "").await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
}

#[tokio::test]
async fn test_opencode_large_request_body_reaches_upstream_intact() {
    let mock = MockServer::start().await;

    // pingora reads the downstream request body in 64 KiB chunks, so a body
    // larger than that arrives at the gateway in several `request_body_filter`
    // calls. Every byte must still reach the upstream: a real opencode-cli
    // first turn carries a ~100 KiB system prompt + tool catalog, and when the
    // gateway dropped the body the upstream answered a bare HTTP 400.
    let padding = "context-padding-".repeat(12 * 1024); // ~100 KiB
    let body = json!({
        "model": "deepseek-v4.1-flash",
        "messages": [
            {"role": "system", "content": padding},
            {"role": "user", "content": "Reply with exactly: pong"},
        ],
    });

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .respond_with(move |req: &wiremock::Request| {
            // The upstream must have received the complete JSON body.
            let recv: Value =
                serde_json::from_slice(&req.body).expect("upstream body is valid JSON");
            let content = recv["messages"][0]["content"].as_str().unwrap_or_default();
            assert!(
                content == padding,
                "upstream received a truncated body: {} bytes of {} expected",
                content.len(),
                padding.len()
            );
            ResponseTemplate::new(200).set_body_json(openai_response("pong"))
        })
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPENCODE_CHAT_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_native_responses_large_body_records_usage() {
    let mock = MockServer::start().await;

    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":11,\"output_tokens\":5}}}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    // A body larger than pingora's 64 KiB read chunk: it arrives at the
    // gateway in several `request_body_filter` calls.
    let padding = "context-padding-".repeat(12 * 1024); // ~100 KiB
    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": padding}]}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("event: response.completed"), "{text}");

    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_in_total") && l.contains("deepseek-v4.1-flash"))
        .unwrap_or_else(|| panic!("no tokens_in metric for the model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("11"),
        "expected 11 input tokens, got: {line}"
    );
}

// ---------------------------------------------------------------------------
// Request-level HTTP failover tests
// ---------------------------------------------------------------------------

const HTTP_FAILOVER_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "deepseek-v4.1-flash"

[providers.fallback]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
max_failover_attempts = 4
"#;

const HTTP_FAILOVER_TO_OPENCODE_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "deepseek-v4.1-flash"

[providers.opencode_fb]
kind = "opencode"
base_url = "{FALLBACK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["opencode_fb"]
strategy = "fallback"
max_failover_attempts = 4
"#;

const HTTP_FAILOVER_400_MODEL_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "MiniMax-M3"

[providers.fallback]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
max_failover_attempts = 4
"#;

const HTTP_FAILOVER_BUDGET_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.p1]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.p2]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[providers.p3]
kind = "openai_compat"
base_url = "http://127.0.0.1:1"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "p1"
fallback = ["p2", "p3"]
strategy = "fallback"
max_failover_attempts = 1
"#;

#[tokio::test]
async fn test_http_error_fails_over_to_fallback_provider() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Primary answers 500 (a provider failure another provider may not have).
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw =
        TestGateway::start_with_fallback(HTTP_FAILOVER_CONFIG, &primary.uri(), &fallback.uri())
            .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // The client sees the fallback's answer, not the primary's 500.
    assert_eq!(resp.status(), 200, "request must fail over to the fallback");
    // The gateway tells the client which provider served it.
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from fallback");

    // The primary was tried exactly once; the fallback exactly once.
    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(primary_reqs.len(), 1);
    let fallback_reqs = fallback.received_requests().await.unwrap();
    assert_eq!(fallback_reqs.len(), 1);
}

#[tokio::test]
async fn test_http_error_fails_over_to_opencode_with_normalized_body() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Primary rate-limits (429) — an upstream failure, not a request fault.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "rate limited"})))
        .expect(1)
        .mount(&primary)
        .await;

    // The fallback is a strict opencode.ai-style upstream: it requires the
    // x-opencode-session header and rejects the non-standard wire shapes the
    // gateway normalises away. Failing over to it must NOT forward the body
    // as-is — it has to be converted to opencode's schema.
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/chat/completions"))
        .and(header_exists("x-opencode-session"))
        .respond_with(move |req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            // role normalised: developer -> system
            assert_eq!(sent["messages"][0]["role"], "system");
            // tool_choice "any" is rejected by opencode -> normalised to "required"
            assert_eq!(sent["tool_choice"], "required");
            // arguments must be a JSON string, not an object
            let args = sent["messages"][2]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .expect("arguments must be a JSON string");
            let parsed: Value = serde_json::from_str(args).unwrap();
            assert_eq!(parsed["city"], "BJ");
            ResponseTemplate::new(200).set_body_json(openai_response("from opencode"))
        })
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        HTTP_FAILOVER_TO_OPENCODE_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "messages": [
                {"role": "developer", "content": "be terse"},
                {"role": "user", "content": "what is the weather in Beijing?"},
                {"role": "assistant", "content": "", "tool_calls": [
                    {"id": "", "type": "function", "function": {"name": "get_weather", "arguments": {"city": "BJ"}}}
                ]},
                {"role": "tool", "tool_call_id": "", "content": "sunny"}
            ],
            "tools": [{"type": "function", "function": {"name": "get_weather", "parameters": {}}}],
            "tool_choice": "any"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "must fail over to opencode and answer");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Provider").unwrap(),
        "opencode_fb"
    );
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from opencode");
}

#[tokio::test]
async fn test_400_unknown_model_fails_over() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The pool routes the requested model to the first provider; the primary
    // does not carry it and answers 400 (one-api "invalid params, unknown
    // model"). Its default model differs from the request, so the 400 is a
    // capability failure and the request must walk on.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            json!({"error": {"message": "invalid params, unknown model 'deepseek-v4-flash'"}}),
        ))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("carried here")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        HTTP_FAILOVER_400_MODEL_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4-flash", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "a 400 for an uncarried model must fail over"
    );
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "carried here");
}

#[tokio::test]
async fn test_400_for_carried_model_does_not_fail_over() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The primary carries the requested model (its default matches), so a 400
    // is a genuine request fault every provider would share — it must not
    // burn the fallback budget on it.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({"error": {"message": "bad request"}})),
        )
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("never")))
        .expect(0)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        HTTP_FAILOVER_400_MODEL_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("MiniMax-M3", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400, "a 400 for a carried model stays put");
    // The fallback must never be touched.
    let fallback_reqs = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(fallback_reqs.len(), 0);
}

/// A heterogeneous fallback chain forwards one provider's model name to every
/// entry. The pooled `/v1/responses` primary serves a provider-specific name
/// (`deepseek-v4.1-flash:cloud`); when it dies, every cloud fallback answers
/// 400 "unknown model", so a plain failover walked the whole chain and the
/// route died together with its primary (docs/ops_review_20261108.md §828).
///
/// The provider that answers 400 and declares a different `default_model` must
/// get one same-provider retry with its own model substituted; when that
/// succeeds the client sees a 200 instead of the chain's last 400.
#[tokio::test]
async fn test_unknown_model_is_rewritten_to_the_fallback_default() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The primary is simply down (the production shape: `ollama_local`
    // unreachable). Its own default is the provider-specific name the client
    // sends, so nothing about the request is wrong.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&primary)
        .await;

    // The fallback only knows its own default. The first attempt carries the
    // primary's model name and is rejected; the rewritten retry must arrive
    // with `fallback-model` and succeed.
    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            if sent["model"] == "fallback-model" {
                ResponseTemplate::new(200).set_body_json(openai_response("from fallback"))
            } else {
                ResponseTemplate::new(400).set_body_json(json!({
                    "error": {"message": format!(
                        "invalid params, unknown model '{}'",
                        sent["model"].as_str().unwrap_or_default()
                    )}
                }))
            }
        })
        .mount(&fallback)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "primary-model:cloud"

[providers.fallback]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "fallback-model"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
max_failover_attempts = 4
"#;
    let gw = TestGateway::start_with_fallback(config, &primary.uri(), &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("primary-model:cloud", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "the fallback must serve the rewritten retry instead of 400ing the whole route"
    );
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");

    let fallback_reqs = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(
        fallback_reqs.len(),
        2,
        "one rejected attempt, then the rewritten retry"
    );
    let first: Value = serde_json::from_slice(&fallback_reqs[0].body).expect("json body");
    assert_eq!(
        first["model"], "primary-model:cloud",
        "the first attempt must carry the client's model name unchanged"
    );
    let second: Value = serde_json::from_slice(&fallback_reqs[1].body).expect("json body");
    assert_eq!(
        second["model"], "fallback-model",
        "the retry must carry the fallback's own default model"
    );
}

/// The rewrite is a fallback of last resort: while an untried provider still
/// declares the requested model, the request must move to that provider
/// instead of silently serving a different model from the current one.
#[tokio::test]
async fn test_unknown_model_prefers_a_provider_that_carries_the_name() {
    let primary = MockServer::start().await;
    let middle = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"message": "invalid params, unknown model 'wanted-model'"}
        })))
        .expect(1)
        .mount(&primary)
        .await;

    // The middle provider declares the requested model as its default.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from middle")))
        .expect(1)
        .mount(&middle)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "primary-model"

[providers.middle]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "wanted-model"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["middle"]
strategy = "fallback"
max_failover_attempts = 4
"#;
    let gw = TestGateway::start_with_fallback(config, &primary.uri(), &middle.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("wanted-model", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "middle");
    let middle_reqs = middle.received_requests().await.unwrap_or_default();
    assert_eq!(
        middle_reqs.len(),
        1,
        "the carrying provider serves it first"
    );
    let sent: Value = serde_json::from_slice(&middle_reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "wanted-model",
        "the requested name must reach the provider that carries it"
    );
}

/// The rewrite must reach the upstream on the **native Responses** path too.
///
/// A native Responses body normally travels verbatim, keeping the client's
/// Content-Length; that branch forwards the bytes untouched. Substituting the
/// model therefore has to switch the retry to chunked framing first, or the
/// upstream sees the original name again and answers the same 400 (the live
/// failure on 2026-11-08: `model_rewrite_retries_total` incremented, the
/// retry's `ctx.model` was correct, and minimax still rejected the original
/// name).
#[tokio::test]
async fn test_unknown_model_is_rewritten_on_the_native_responses_path() {
    let upstream = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            if sent["model"] == "native-model" {
                ResponseTemplate::new(200).set_body_json(json!({
                    "id": "resp_1",
                    "object": "response",
                    "created_at": 1,
                    "model": "native-model",
                    "status": "completed",
                    "output": [{
                        "type": "message", "id": "msg_1", "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "from native", "annotations": []}]
                    }]
                }))
            } else {
                ResponseTemplate::new(400).set_body_json(json!({
                    "error": {"message": format!(
                        "invalid params, unknown model '{}'",
                        sent["model"].as_str().unwrap_or_default()
                    )}
                }))
            }
        })
        .mount(&upstream)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

# Owns the model name the client sends; not on this route.
[providers.owner]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "primary-model:cloud"

[providers.native]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "native-model"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses/native"
provider = "native"
endpoint_type = "responses"
strategy = "fallback"
max_failover_attempts = 2
"#;
    let gw = TestGateway::start(config, &upstream.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/native"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "primary-model:cloud",
            "input": "hi",
            "max_output_tokens": 16
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "the native-Responses retry must carry the provider's own model name"
    );

    let reqs = upstream.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        2,
        "one rejected attempt, then the rewritten retry"
    );
    let first: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(first["model"], "primary-model:cloud");
    let second: Value = serde_json::from_slice(&reqs[1].body).expect("json body");
    assert_eq!(
        second["model"], "native-model",
        "the retried native body must carry the substituted model"
    );
}

/// A model name that belongs to no provider of this deployment (a typo, or a
/// model the gateway does not offer) must stay a 400: the rewrite exists for
/// names that belong to *another* provider of the same deployment, and must
/// never silently answer a bogus name with the provider's default model.
#[tokio::test]
async fn test_unknown_model_of_no_provider_stays_a_400() {
    let primary = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"message": "invalid params, unknown model 'gpt-4o'"}
        })))
        .mount(&primary)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "only-model"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
strategy = "fallback"
"#;
    let gw = TestGateway::start(config, &primary.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "an unknown model name must not be substituted with the provider default"
    );
    let reqs = primary.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        1,
        "the provider must not be retried with a different model"
    );
}

/// vLLM-style upstreams answer an unknown model with **404** ("The model `X`
/// does not exist. Valid aliases: …"), not the 400/422 one-api uses. The
/// rewrite must cover that shape too — decided from the body, since a missing
/// endpoint is a 404 as well.
#[tokio::test]
async fn test_404_unknown_model_is_rewritten_to_the_provider_default() {
    let upstream = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            if sent["model"] == "minicpmv47-awq" {
                ResponseTemplate::new(200).set_body_json(json!({
                    "id": "resp_1",
                    "object": "response",
                    "created_at": 1,
                    "model": "minicpmv47-awq",
                    "status": "completed",
                    "output": [{
                        "type": "message", "id": "msg_1", "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "from vllm", "annotations": []}]
                    }]
                }))
            } else {
                ResponseTemplate::new(404).set_body_json(json!({
                    "error": {"message": format!(
                        "The model `{}` does not exist. Valid aliases: minicpmv47-awq.",
                        sent["model"].as_str().unwrap_or_default()
                    )}
                }))
            }
        })
        .mount(&upstream)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

# Owns the model name the client sends; not on this route.
[providers.owner]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "deepseek-v4.1-flash:cloud"

[providers.vllm]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "minicpmv47-awq"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses/vllm"
provider = "vllm"
endpoint_type = "responses"
strategy = "fallback"
max_failover_attempts = 2
"#;
    let gw = TestGateway::start(config, &upstream.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/vllm"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash:cloud",
            "input": "hi",
            "max_output_tokens": 16
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "the 404 body must be read, blamed on the model, and retried with the provider's own name"
    );

    let reqs = upstream.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        2,
        "one rejected attempt, then the rewritten retry"
    );
    let first: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(first["model"], "deepseek-v4.1-flash:cloud");
    let second: Value = serde_json::from_slice(&reqs[1].body).expect("json body");
    assert_eq!(
        second["model"], "minicpmv47-awq",
        "the retried body must carry the substituted model"
    );
}

/// A 404 that is NOT a model rejection (a missing endpoint, a dead router)
/// must walk the fallback chain with the client's model name untouched: the
/// substitution exists only for "the model does not exist".
#[tokio::test]
async fn test_404_of_a_missing_endpoint_fails_over_without_rewriting() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The primary is simply missing an endpoint — 404, no model mentioned.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_string("404 page not found"))
        .expect(1)
        .mount(&primary)
        .await;

    // The fallback serves the request as the client sent it: the original
    // model name must still be there, proving nothing was substituted.
    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            ResponseTemplate::new(200)
                .set_body_json(openai_response(sent["model"].as_str().unwrap_or_default()))
        })
        .mount(&fallback)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

# Owns the model name the client sends; not on this route. Because the
# primary's own default differs, the 404 IS rewritable at header time — the
# body must then veto the rewrite and walk the chain instead.
[providers.owner]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "owner-model:cloud"

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "primary-model"

[providers.fallback]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "fallback-model"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
max_failover_attempts = 4
"#;
    let gw = TestGateway::start_with_fallback(config, &primary.uri(), &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("owner-model:cloud", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "a non-model 404 must be failed over, not rewritten"
    );
    let primary_reqs = primary.received_requests().await.unwrap_or_default();
    assert_eq!(
        primary_reqs.len(),
        1,
        "the 404 must not trigger a rewritten retry on the same provider"
    );
    let fallback_reqs = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(fallback_reqs.len(), 1);
    let sent: Value = serde_json::from_slice(&fallback_reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "owner-model:cloud",
        "the client's model name must reach the fallback untouched"
    );
}

/// At most one rewrite per provider: a second 404 that still blames the model
/// is a genuine request fault and reaches the client (this is what stops the
/// walk from ping-ponging on the same provider).
#[tokio::test]
async fn test_404_unknown_model_is_rewritten_only_once() {
    let upstream = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"message": "The model `x` does not exist. Valid aliases: none."}
        })))
        .expect(2)
        .mount(&upstream)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

# Owns the model name the client sends; not on this route.
[providers.owner]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "deepseek-v4.1-flash:cloud"

[providers.vllm]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "minicpmv47-awq"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses/vllm"
provider = "vllm"
endpoint_type = "responses"
strategy = "fallback"
max_failover_attempts = 2
"#;
    let gw = TestGateway::start(config, &upstream.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/vllm"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash:cloud",
            "input": "hi",
            "max_output_tokens": 16
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        404,
        "the second rejection is a genuine fault and must reach the client"
    );
    let reqs = upstream.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        2,
        "exactly one rewritten retry — never a loop on the same provider"
    );
    let first: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(first["model"], "deepseek-v4.1-flash:cloud");
    let second: Value = serde_json::from_slice(&reqs[1].body).expect("json body");
    assert_eq!(second["model"], "minicpmv47-awq");
}

/// A 503 must name the candidate(s) and the reason instead of claiming that
/// "all providers are unavailable" on a single-provider route.
#[tokio::test]
async fn test_503_names_the_provider_and_the_reason() {
    let primary = MockServer::start().await;

    // `max_fails = 1`: the first upstream 5xx opens the breaker, the next
    // request is refused by the gateway before any upstream call.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .mount(&primary)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.only]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "only-model"
max_fails = 1
fail_timeout_secs = 60

[[routes]]
path = "/v1/chat/completions"
provider = "only"
strategy = "fallback"
"#;
    let gw = TestGateway::start(config, &primary.uri()).await;
    let c = client();

    let first = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("only-model", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        first.status(),
        503,
        "the first attempt reaches the upstream and the gateway answers with its own 503"
    );

    let second = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("only-model", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 503, "the open breaker refuses the request");
    let body: Value = second.json().await.unwrap();
    let message = body["error"]["message"].as_str().expect("message");
    assert!(
        message.contains("only=circuit open"),
        "the 503 must name the provider and its state, got: {message}"
    );
    assert!(
        message.contains("route '/v1/chat/completions'"),
        "the 503 must name the route, got: {message}"
    );
}

#[tokio::test]
async fn test_all_providers_fail_returns_last_error() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "quota"})))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw =
        TestGateway::start_with_fallback(HTTP_FAILOVER_CONFIG, &primary.uri(), &fallback.uri())
            .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // Both providers failed and no failover is left: the client receives the
    // gateway's own error, not the last provider's quota message.
    assert_eq!(resp.status(), 503);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "gateway_error");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no provider available"),
        "the message names the situation: {body}"
    );
    assert_eq!(
        primary.received_requests().await.unwrap_or_default().len(),
        1,
        "the primary is tried once"
    );
    assert_eq!(
        fallback.received_requests().await.unwrap_or_default().len(),
        1,
        "the fallback is tried once"
    );
}

#[tokio::test]
async fn test_failover_budget_is_respected() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down too"})))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        HTTP_FAILOVER_BUDGET_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // max_failover_attempts = 1: p1 fails -> p2; p2 fails -> budget spent, the
    // gateway answers the client itself and p3 is never touched.
    assert_eq!(resp.status(), 503);
    let fallback_reqs = fallback.received_requests().await.unwrap();
    assert_eq!(fallback_reqs.len(), 1, "p2 is tried once");
}

// ---------------------------------------------------------------------------
// Layered rate-limit resolution (headers → learned patterns → body regex →
// laya decision)
// ---------------------------------------------------------------------------

/// The real local Laya decision server. The rate-limit tests exercise the
/// full stack against it; the resolver is fail-open, so these tests still
/// pass when the server is down (the deterministic layers decide alone).
const LAYA_TEST_URL: &str = "http://10.126.126.20:18405";

/// The learning resolver is enabled but laya is NOT configured: the
/// deterministic layers must decide on their own, and a missing laya must
/// never break the failover path.
const RATE_LIMIT_LEARNING_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[rate_limit_learning]
enabled = true
laya_url = "{LAYA_URL}"
default_cooldown_secs = 300
rate_limit_threshold = 0.5

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "deepseek-v4.1-flash"

[providers.fallback]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["fallback"]
strategy = "fallback"
max_failover_attempts = 4
"#;

/// Read one Prometheus sample (`name{labels} value`) from the gateway's
/// /metrics endpoint. Returns None when the series does not exist.
async fn metric_sample(gw: &TestGateway, series: &str) -> Option<f64> {
    let text = reqwest::Client::new()
        .get(gw.url("/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let (name, value) = line.rsplit_once(' ')?;
        if name == series {
            return value.parse::<f64>().ok();
        }
    }
    None
}

#[tokio::test]
async fn test_429_body_rate_limit_fails_over_with_precise_reset() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The production Volcengine shape: the quota message carries the reset
    // instant with a numeric offset and a trailing zone name — a form a
    // whitespace-tokenised scan misses completely.
    let body = json!({
        "error": {
            "code": "AccountQuotaExceeded",
            "message": "You have exceeded the monthly usage quota. \
                        It will reset at 2099-11-06 23:59:59 +0800 CST. \
                        Request id: 021793713952"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // The client sees the fallback's answer, not the primary's 429.
    assert_eq!(resp.status(), 200, "the 429 must fail over to the fallback");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );
    let answer: Value = resp.json().await.unwrap();
    assert_eq!(answer["choices"][0]["message"]["content"], "from fallback");

    // The primary was tried exactly once.
    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(primary_reqs.len(), 1);

    // The provider was force-opened because of the quota — not because of a
    // blind cooldown — and the breaker is open.
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "the resolved quota must count exactly one quota open"
    );
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0), "the breaker must be open (1)");
}

/// A quota wall can wear a 500 status instead of a 429: MiniMax's
/// /v1/responses endpoint answers an exhausted Token Plan with HTTP 500 and
/// `{"error":{"code":"server_error","message":"已达到 Token Plan 用量上限…"}}`.
/// Before the fix the gateway booked that as a generic server failure — three
/// failures opened the breaker for a flat fail timeout, then the half-open
/// probe re-opened it again, so the provider flapped in and out of the pool
/// instead of being parked on the quota cooldown ladder. A quota-phrased 500
/// must be classified exactly like a 429: force-open via the quota ladder.
#[tokio::test]
async fn test_500_with_a_quota_body_is_treated_as_a_quota_wall() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    let laya = MockServer::start().await;

    // MiniMax's real answer: 500 with the quota phrasing, no reset instant.
    let body = json!({
        "error": {
            "code": "server_error",
            "message": "已达到 Token Plan 用量上限：请升级 Token Plan 套餐或购买积分补充用量。 (2056)"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    // No reset instant in the message: laya classifies it (rate limit, no
    // chosen instant) → the provider parks on the cooldown ladder.
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.9 }
            }
        })))
        .expect(1)
        .mount(&laya)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback_and_laya(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
        &laya.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // The client sees the fallback's answer, not the primary's 500.
    assert_eq!(
        resp.status(),
        200,
        "the quota 500 must fail over to the fallback"
    );
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );
    let answer: Value = resp.json().await.unwrap();
    assert_eq!(answer["choices"][0]["message"]["content"], "from fallback");

    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(primary_reqs.len(), 1);

    // Quota classification, not ordinary failure: exactly one quota open and
    // the breaker open after a single request (an ordinary failure would
    // need max_fails=3 before the breaker moved).
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "the quota-phrased 500 must count exactly one quota open"
    );
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0), "the breaker must be open (1)");
}

/// A raw HTTP/1.1 upstream that sends the response head first and the body
/// later — the shape that makes the proxy commit the head to the client
/// before the first body chunk arrives.
///
/// The head and the body are separated by a real delay, so the two arrive in
/// different proxy batches: the head is written on its own, and the body
/// filter only runs when the head is already on the wire.
async fn spawn_split_response_server(
    head: &'static [u8],
    body: &'static [u8],
) -> (String, Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let served = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&served);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(head).await;
                let _ = socket.flush().await;
                tokio::time::sleep(Duration::from_millis(400)).await;
                // The head announces a chunked body, so the body has to be
                // chunked too — and the terminator has to close it.
                let size = format!("{:x}\r\n", body.len());
                let _ = socket.write_all(size.as_bytes()).await;
                let _ = socket.write_all(body).await;
                let _ = socket.write_all(b"\r\n0\r\n\r\n").await;
                let _ = socket.flush().await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (format!("http://{}", addr), served)
}

/// The harder shape of the same defect: the upstream sends its response head
/// first and the failing event later, so the head is already committed to the
/// client when the failure shows up. The failover cannot take the head back —
/// but the member must still be judged (quota wall → force-open) so the
/// *next* request escapes it instead of walking into the same wall forever.
#[tokio::test]
async fn test_quota_wall_after_the_head_was_committed_escapes_on_the_next_request() {
    let head = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/event-stream\r\n",
        "Transfer-Encoding: chunked\r\n",
        "\r\n",
    )
    .as_bytes();
    let quota = concat!(
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_fail\",\"object\":\"response\",\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"您已达到每周/每月使用上限，您的限额将在 2099-01-01 00:00:00 重置。\"},\"output\":[]}}\n\n",
    )
    .as_bytes();
    let (primary, primary_hits) = spawn_split_response_server(head, quota).await;
    let fallback = MockServer::start().await;

    let ok = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_ok\",\"output_index\":0,\"content_index\":0,\"delta\":\"from fallback\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(ok, "text/event-stream"),
        )
        .expect(1..)
        .mount(&fallback)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[rate_limit_learning]
enabled = true
default_cooldown_secs = 300

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"

[providers.fallback]
kind = "opencode"
base_url = "{FALLBACK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "primary"
fallback = ["fallback"]
endpoint_type = "responses"
strategy = "fallback"
max_failover_attempts = 4
"#;

    let gw = TestGateway::start_with_fallback(config, &primary, &fallback.uri()).await;
    let c = client();

    // First request: the head is already on the wire, so the failure cannot
    // be taken back — the client is told the answer failed.
    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "m", "input": "hi", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("response.failed"),
        "the client must be told the answer failed: {text}"
    );

    // The member is judged all the same: the wall is a quota wall, so it is
    // force-opened until the reset instant its own message named.
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "the quota wall must force-open the member even when the head was committed"
    );

    // Second request: the walled member is skipped — this is what breaks the
    // endless "reconnecting" loop.
    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "m", "input": "hi again", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    let text = resp.text().await.unwrap();
    assert!(text.contains("from fallback"), "{text}");
    assert_eq!(
        primary_hits.load(Ordering::SeqCst),
        1,
        "the walled member must not be asked a second time"
    );
}

/// A provider that hits a quota wall *inside* its stream answers `200 OK` and
/// then emits the failure as the first event of the stream. The HTTP status
/// says nothing about it, so the header-time failover never triggers — and a
/// session-sticky pool keeps sending the request to the same broken member,
/// which the client experiences as an endless "reconnecting".
///
/// The request must walk on to the next member of the chain (the client sees
/// only the healthy answer), and the member must be parked until the reset
/// instant its own message names — so the wall is learned instead of waiting
/// for a human to drop the member from the pool.
#[tokio::test]
async fn test_quota_wall_inside_a_200_stream_fails_over_to_the_next_member() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Zhipu's real answer: 200, and the failure as the first (only) event.
    let quota = concat!(
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_fail\",\"object\":\"response\",\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"您已达到每周/每月使用上限，您的限额将在 2099-01-01 00:00:00 重置。\"},\"output\":[]}}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(quota, "text/event-stream"),
        )
        .expect(1)
        .mount(&primary)
        .await;

    // The fallback answers a healthy Responses stream.
    let ok = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_ok\",\"output_index\":0,\"content_index\":0,\"delta\":\"from fallback\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(ok, "text/event-stream"),
        )
        // Two requests reach the fallback: the first one (after the failover)
        // and the second one (which must skip the walled member).
        .expect(2)
        .mount(&fallback)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[rate_limit_learning]
enabled = true
default_cooldown_secs = 300

[admin]
master_key = "test-master-key"
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"

[providers.fallback]
kind = "opencode"
base_url = "{FALLBACK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "primary"
fallback = ["fallback"]
endpoint_type = "responses"
strategy = "fallback"
max_failover_attempts = 4
"#;

    let gw = TestGateway::start_with_fallback(config, &primary.uri(), &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "m", "input": "hi", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    let text = resp.text().await.unwrap();

    // The client must not see the failed attempt at all.
    assert!(
        !text.contains("response.failed"),
        "the quota wall must not reach the client: {text}"
    );
    assert!(
        !text.contains("rate_limit_exceeded"),
        "the quota wall must not reach the client: {text}"
    );
    assert!(text.contains("from fallback"), "{text}");

    // The member that hit the wall is parked until the reset instant its own
    // message named — the pool stops sending requests to it on its own.
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "the quota wall must force-open the member exactly once"
    );
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0), "the breaker must be open (1)");

    // The 2026-11-11 incident: the wall named `2099-01-01`, the parser read
    // it as an instant, and the member left the pool for 73 years while its
    // own usage API reported no limit at all. A reset instant no quota
    // window can express is not a reset — the member parks on the cooldown
    // ladder instead.
    let report: Value = c
        .get(gw.url("/admin/breakers"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the breaker report answers")
        .json()
        .await
        .expect("the report is JSON");
    let until = report["breakers"]
        .as_array()
        .and_then(|b| {
            b.iter()
                .find(|e| e["provider"] == "primary")
                .and_then(|e| e["force_open_until"].as_u64())
        })
        .expect("the walled member is listed");
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    assert!(
        until > now_ms && until < now_ms + 24 * 60 * 60 * 1000,
        "the sentinel must not become the deadline (until={until})"
    );

    // And the failure is visible as what it is — not as a clean 200.
    let failures = metric_sample(&gw, "maxllm_stream_failures_total{provider=\"primary\"}").await;
    assert_eq!(failures, Some(1.0), "the stream failure must be counted");

    // The wall is learned: the next request does not touch the member at
    // all (the mock expects exactly one call — a second one would fail the
    // expectation and answer with an error).
    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "m", "input": "hi again", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    let text = resp.text().await.unwrap();
    assert!(text.contains("from fallback"), "{text}");
    assert_eq!(
        primary.received_requests().await.unwrap().len(),
        1,
        "the walled member must not be asked a second time"
    );
}

/// The flip side of the 500-quota fix: a *genuine* 500 (no quota phrasing)
/// must stay an ordinary transient failure — fail over, count a normal
/// failure, never a quota open. Without the status-aware fallback the
/// resolver's "unclassifiable" default would have parked every flaky 500 on
/// the quota cooldown ladder.
#[tokio::test]
async fn test_500_without_a_quota_body_fails_over_as_an_ordinary_failure() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    let laya = MockServer::start().await;

    let body = json!({
        "error": {
            "code": "server_error",
            "message": "internal server error"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    // No quota/billing phrase: laya classifies it as NOT a rate limit.
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.1 }
            }
        })))
        .expect(1)
        .mount(&laya)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback_and_laya(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
        &laya.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "a genuine 500 still fails over");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "primary"
    );

    // Ordinary failure: no quota open, and the breaker stays closed (one
    // failure is below max_fails=3).
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens, None,
        "a genuine 500 must not count a quota open"
    );
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(0.0), "the breaker must stay closed (0)");
}

/// A quota wall's only regular visitor is the health probe (real rotation
/// skips open circuits). A probe that reads a quota message — here a 500
/// wearing MiniMax's Token Plan body — must re-park the member on the
/// cooldown ladder. Without this, the wall decays to half-open between real
/// hits, the failover it triggers answers 200, and pool_health misreports
/// the member as served while burning one upstream round trip per round.
#[tokio::test]
async fn test_a_probe_parks_a_500_quota_wall() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    let laya = MockServer::start().await;

    // MiniMax's real answer: 500 with the quota phrasing, no reset instant.
    let body = json!({
        "error": {
            "code": "server_error",
            "message": "已达到 Token Plan 用量上限：请升级 Token Plan 套餐或购买积分补充用量。 (2056)"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    // No reset instant: laya classifies it as a rate limit without a chosen
    // instant → the provider parks on the cooldown ladder.
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.9 }
            }
        })))
        .expect(1)
        .mount(&laya)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback_and_laya(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
        &laya.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("X-MaxLLM-Probe", "1")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // The probe fails over to the fallback, but its observation parks the
    // quota wall: one quota open and the breaker open after a single probe.
    assert_eq!(
        resp.status(),
        200,
        "the probe must fail over to the fallback"
    );
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");

    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(primary_reqs.len(), 1, "the probe reached the primary once");

    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "a probe reading a quota wall must count one quota open"
    );
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(
        state,
        Some(1.0),
        "the probe must leave the breaker open (1)"
    );
}

// --- per-provider `utc_offset` (zone-less reset timestamps) ---------------

/// Render the wall-clock timestamp `hours` ahead of now, in the zone-less
/// `%Y-%m-%d %H:%M:%S` shape upstreams write in their quota messages.
fn zoneless_ts(hours: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::hours(hours))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// The learning config, with `utc_offset` added to the primary provider.
fn rate_limit_config_with_offset(offset: &str) -> String {
    RATE_LIMIT_LEARNING_CONFIG.replace(
        "default_model = \"deepseek-v4.1-flash\"",
        &format!("default_model = \"deepseek-v4.1-flash\"\nutc_offset = \"{offset}\""),
    )
}

/// A zone-less Chinese quota message: the timestamp is Beijing wall-clock
/// time with no marker (exactly Zhipu's `1310` shape).
fn zoneless_quota_body(ts: &str) -> Value {
    json!({
        "error": {
            "code": "1310",
            "message": format!("您已达到每周/每月使用上限，您的限额将在 {ts} 重置。")
        }
    })
}

/// With `utc_offset = "+08:00"` the zone-less local time is 8 hours earlier
/// in UTC. A stamp that is still "7 hours in the future" when read as UTC but
/// already past when read as +08:00 must NOT force the breaker open: the
/// upstream has already reset, so walling it off would be wrong.
#[tokio::test]
async fn test_429_zoneless_reset_honours_provider_utc_offset() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Read as +08:00 this is one hour ago; read as UTC it would be 7h ahead.
    let body = zoneless_quota_body(&zoneless_ts(7));
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        &rate_limit_config_with_offset("+08:00"),
        &primary.uri(),
        &fallback.uri(),
    )
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the 429 must fail over to the fallback");

    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(
        state,
        Some(0.0),
        "the reset instant is in the past once +08:00 is honoured: no force-open"
    );
}

/// The same message with no `utc_offset`: the zone-less stamp is read as UTC,
/// lands in the future, and the breaker is force-opened until then. This is
/// the pre-fix behaviour, kept as the contrast that proves the offset is what
/// changes the outcome.
#[tokio::test]
async fn test_429_zoneless_reset_without_offset_reads_as_utc() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    let body = zoneless_quota_body(&zoneless_ts(7));
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the 429 must fail over to the fallback");

    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(
        state,
        Some(1.0),
        "read as UTC the reset instant is in the future: the breaker is open"
    );
}

#[tokio::test]
async fn test_429_non_rate_limit_body_is_not_force_opened() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // A billing rejection that *looks* like a quota error ("usage limit
    // reached") but does not reset on a timer: it must take the ordinary
    // failure path, never a quota force-open.
    let body = json!({
        "error": {
            "code": "InsufficientBalance",
            "message": "You have reached your usage limit. Insufficient balance — \
                        please recharge your account."
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    // A 429 is still a provider failure, so the request walks on to the
    // fallback…
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");

    // …but the provider must NOT be force-opened as a quota: the decision
    // was "ordinary failure", so no quota open was counted.
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(
        quota_opens, None,
        "a non-rate-limit error must not count a quota open"
    );
    // The ordinary failure left the breaker closed (max_fails defaults to 3).
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(0.0), "the breaker must stay closed (0)");
}

#[tokio::test]
async fn test_429_retry_after_header_fails_over_immediately() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The header layer answers the reset instant without reading the body.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "120")
                .set_body_json(json!({"error": "quota exhausted"})),
        )
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");

    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(quota_opens, Some(1.0));
    // The header path must count the quota open exactly once — the generic
    // 429 accounting must not run a second time.
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0));
}

/// A `402 Payment Required` means the upstream account has no balance: the
/// provider will answer 402 to every request until it is topped up. Before
/// the fix the gateway booked such an answer as *nothing* (402 is neither a
/// 5xx nor a 429), so the breaker kept calling the provider healthy and every
/// rotation paid one extra upstream round trip before the fallback chain took
/// over. A 402 must force-open the circuit for a cooldown instead.
#[tokio::test]
async fn test_402_payment_required_force_opens_the_provider() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // Exactly one upstream call: the second request must not reach the
    // out-of-credit provider again.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(402).set_body_json(json!({
            "error": {
                "code": "insufficient_balance",
                "message": "Insufficient wallet balance. Please top up your wallet before retrying.",
                "type": "insufficient_balance"
            }
        })))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(2)
        .mount(&fallback)
        .await;

    let gw =
        TestGateway::start_with_fallback(HTTP_FAILOVER_CONFIG, &primary.uri(), &fallback.uri())
            .await;
    let c = client();

    for _ in 0..2 {
        let resp = c
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer test-key-123")
            .json(&chat_body("deepseek-v4.1-flash", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");
    }

    // The 402 is booked as a force-open, not as a plain miss.
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(quota_opens, Some(1.0));
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0));

    // ... and the provider is reported as open by /health.
    let health: Value = c
        .get(gw.url("/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let open = health["open_providers"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        open.iter().any(|v| v == "primary"),
        "the out-of-credit provider must be reported as open: {health}"
    );
}

#[tokio::test]
async fn test_429_laya_classifies_inconclusive_text() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    // A fake laya decider: it must be consulted only when the deterministic
    // layers are inconclusive, and its answer must drive the force-open.
    let laya = MockServer::start().await;

    // 2099-01-01 00:00:00 UTC.
    let reset_id = "4070908800000";

    // No quota phrase, no billing phrase, but a concrete future instant the
    // regex layer finds: the classification is delegated to laya, which says
    // "yes, rate limit" and picks the candidate instant.
    let body = json!({
        "error": {
            "code": "TemporarilyBlocked",
            "message": "Access temporarily blocked. Please wait until 2099-01-01 00:00:00."
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_json(body))
        .expect(1)
        .mount(&primary)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "answers": {
                "is_rate_limit": { "type": "noul", "noul": 0.9 },
                "reset_time": { "type": "choice", "choice": reset_id }
            }
        })))
        .expect(1)
        .mount(&laya)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .expect(1)
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback_and_laya(
        RATE_LIMIT_LEARNING_CONFIG,
        &primary.uri(),
        &fallback.uri(),
        &laya.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "laya-classified 429 must fail over");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "fallback");

    // laya was consulted exactly once for this shape.
    let laya_reqs = laya.received_requests().await.unwrap();
    assert_eq!(laya_reqs.len(), 1, "laya is consulted once");

    // The provider is force-opened until the chosen instant (a quota open).
    let quota_opens = metric_sample(
        &gw,
        "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}",
    )
    .await;
    assert_eq!(quota_opens, Some(1.0));
    let state = metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0), "the breaker must be open (1)");
}

// ---------------------------------------------------------------------------
// auto model: rotation over an explicit (provider, model) pool
// ---------------------------------------------------------------------------

/// The auto route rotates over the `[auto_model]` pool and rewrites the
/// request body's model field to the selected entry's model — the upstream
/// never sees the sentinel. Two providers, two models, two requests: the
/// pool must be walked round-robin.
#[tokio::test]
async fn test_auto_model_rotates_over_the_pool_and_rewrites_the_model() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-a")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-b")))
        .mount(&b)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.alpha]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.beta]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "alpha"
model = "model-a"

[[auto_model.pool]]
provider = "beta"
model = "model-b"

[[routes]]
path = "/v1/chat/completions"
provider = "alpha"
auto_model = true
"#;
    let gw = TestGateway::start_with_fallback(config, &a.uri(), &b.uri()).await;
    let c = client();

    // Two requests: the pool must be walked round-robin, so each upstream
    // sees exactly one request.
    for _ in 0..2 {
        let resp = c
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer test-key-123")
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let a_reqs = a.received_requests().await.unwrap_or_default();
    let b_reqs = b.received_requests().await.unwrap_or_default();
    assert_eq!(
        a_reqs.len(),
        1,
        "alpha must serve exactly one of the two requests"
    );
    assert_eq!(
        b_reqs.len(),
        1,
        "beta must serve exactly one of the two requests"
    );

    // The sentinel must never reach the upstream — each provider gets its
    // own model name.
    let a_sent: Value = serde_json::from_slice(&a_reqs[0].body).expect("json body");
    assert_eq!(a_sent["model"], "model-a");
    let b_sent: Value = serde_json::from_slice(&b_reqs[0].body).expect("json body");
    assert_eq!(b_sent["model"], "model-b");
}

// ---------------------------------------------------------------------------
// auto model: the rotation follows the cache
// ---------------------------------------------------------------------------

/// The config the two cache tests share: a route that serves the auto pool
/// directly, with two members — one that keeps the conversation warm and one
/// that re-reads the whole context every turn.
const CACHE_POOL_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

# The cache measurement only works on a *continuation*: a session that comes
# back to the member that already served its first turn. So the pool has to
# pin the session, and the route has to opt into that.
[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100

[providers.mock]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.pool_a]
kind = "openai"
base_url = "{POOL_A_URL}"
api_key = "fake-key"

[providers.pool_b]
kind = "openai"
base_url = "{POOL_B_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pool_a"
model = "model-a"

[[auto_model.pool]]
provider = "pool_b"
model = "model-b"

[[routes]]
path = "/v1/chat/completions"
provider = "mock"
auto_model = true
session_sticky = true
"#;

/// The pool must send the traffic where the conversation stays warm.
///
/// A member that re-reads the whole context on every turn is paid for twice
/// — in prefill and in latency — so it earns less of the rotation than a
/// member that serves the context from its prompt cache. It still keeps a
/// slot: the pool has to keep measuring the members it has.
///
/// The traffic is a set of conversations, each with several turns: the cache
/// ratio is measured on the turns that *continue* a conversation (the first
/// turn of a conversation is cold by nature), so the pool only learns which
/// member keeps a conversation warm once a conversation has come back.
#[tokio::test]
async fn test_auto_pool_prefers_the_member_that_keeps_the_conversation_warm() {
    let mock = MockServer::start().await;
    let warm = MockServer::start().await;
    let cold = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("unused")))
        .mount(&mock)
        .await;
    // 90% of the input came from the upstream's prompt cache.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response_with_cache("warm", 4000, 3600)),
        )
        .mount(&warm)
        .await;
    // The upstream re-read everything, every turn.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(openai_response_with_cache("cold", 4000, 0)),
        )
        .mount(&cold)
        .await;

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(CACHE_POOL_CONFIG, [&mock.uri(), &warm.uri(), &cold.uri()]).await;
    let c = client();

    // Twelve conversations of three turns each.
    for s in 0..12 {
        for _ in 0..3 {
            let resp = c
                .post(format!("{base}/v1/chat/completions"))
                .header("session-id", format!("conv-{s}"))
                .json(&chat_body("auto", "hi"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }
    }

    let warm_reqs = warm.received_requests().await.unwrap_or_default().len();
    let cold_reqs = cold.received_requests().await.unwrap_or_default().len();
    let mock_reqs = mock.received_requests().await.unwrap_or_default().len();

    assert_eq!(
        warm_reqs + cold_reqs,
        36,
        "every request is served by the pool"
    );
    assert_eq!(mock_reqs, 0, "the route's own provider is not used");
    assert!(
        warm_reqs > cold_reqs * 2,
        "the member that keeps the conversation warm must earn the traffic: \
         warm={warm_reqs} cold={cold_reqs}"
    );
    assert!(
        cold_reqs >= 1,
        "no member is starved: the pool has to keep measuring the ones it has"
    );

    child.kill().ok();
    child.wait().ok();
}

/// A session that is pinned to a member which re-reads the whole context
/// every turn has nothing to lose by moving: there is no cache to keep. The
/// session moves to a member that serves it warm.
///
/// The move is only allowed once the member has served enough *continuing*
/// turns for its ratio to be a verdict rather than a guess, and only when
/// some member of the pool is clearly warm — otherwise the pin stays.
#[tokio::test]
async fn test_a_session_pinned_to_a_cold_member_moves_to_a_warm_one() {
    let mock = MockServer::start().await;
    let warm = MockServer::start().await;
    let cold = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("unused")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response_with_cache("warm", 4000, 3600)),
        )
        .mount(&warm)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(openai_response_with_cache("cold", 4000, 0)),
        )
        .mount(&cold)
        .await;

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(CACHE_POOL_CONFIG, [&mock.uri(), &warm.uri(), &cold.uri()]).await;
    let c = client();

    // Conversations of several turns: the member that serves the first turn
    // of a conversation has not had a chance to keep a cache yet, so the
    // pool only learns what it is worth from the turns that come back.
    let mut moved = false;
    for s in 0..16 {
        let mut served_cold = false;
        for _ in 0..4 {
            let resp = c
                .post(format!("{base}/v1/chat/completions"))
                .header("session-id", format!("escape-{s}"))
                .json(&chat_body("auto", "hi"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: Value = resp.json().await.unwrap();
            let served = body["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("");
            if served == "cold" {
                served_cold = true;
            } else if served_cold {
                moved = true;
            }
        }
        if moved {
            break;
        }
    }

    assert!(
        moved,
        "a session pinned to a member that caches nothing must move to a member \
         that keeps the conversation warm"
    );

    child.kill().ok();
    child.wait().ok();
}

/// A streaming request on an auto route must keep the native Responses
/// passthrough and its usage ledger. Regression: the auto rewrite makes the
/// body non-verbatim, which used to bypass the `keep_request_content_length`
/// branch that parses `stream`. Consequently `ctx.is_streaming` stayed false,
/// no stream translator was installed, `extract_stream_usage` never ran, and
/// every `/v1/responses/auto` response booked 0 tokens while also emitting
/// SSE under `Content-Type: application/json`.
#[tokio::test]
async fn test_auto_responses_streaming_records_usage_and_content_type() {
    let mock = MockServer::start().await;

    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_auto\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_auto\",\"output_index\":0,\"content_index\":0,\"delta\":\"hi\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_auto\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":85,\"output_tokens\":17,\"total_tokens\":102}}}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "mock_opencode"
model = "deepseek-v4.1-flash"

[[routes]]
path = "/v1/responses/auto"
provider = "mock_opencode"
endpoint_type = "responses"
auto_model = true
"#;
    let gw = TestGateway::start(config, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/auto"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "auto",
            "input": "hi",
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "a streaming Responses request must be labelled as SSE, got {content_type:?}"
    );

    let text = resp.text().await.unwrap();
    assert!(text.contains("event: response.completed"), "{text}");

    // The upstream saw the entry's model, not the sentinel.
    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["model"], "deepseek-v4.1-flash");
    assert_eq!(sent["stream"], true);

    // Usage from the native `response.completed` event must reach the ledger.
    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_in_total") && l.contains("deepseek-v4.1-flash"))
        .unwrap_or_else(|| panic!("no tokens_in metric for the auto model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("85"),
        "expected 85 input tokens, got: {line}"
    );
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_out_total") && l.contains("deepseek-v4.1-flash"))
        .unwrap_or_else(|| panic!("no tokens_out metric for the auto model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("17"),
        "expected 17 output tokens, got: {line}"
    );
}

/// A pool entry naming an unknown provider is dropped at startup, so a
/// typo cannot silently shrink the pool into a single-provider route.
#[tokio::test]
async fn test_auto_model_drops_unknown_providers_from_the_pool() {
    let a = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-a")))
        .mount(&a)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.alpha]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "alpha"
model = "model-a"

[[auto_model.pool]]
provider = "does_not_exist"
model = "model-x"

[[routes]]
path = "/v1/chat/completions"
provider = "alpha"
auto_model = true
"#;
    let gw = TestGateway::start(config, &a.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("auto", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let received = a.received_requests().await.unwrap_or_default();
    assert_eq!(received.len(), 1);
    let sent: Value = serde_json::from_slice(&received[0].body).expect("json body");
    assert_eq!(sent["model"], "model-a");
}

/// Session-sticky wins over the round-robin: a session pinned to a pool
/// provider keeps hitting it, so the upstream keeps its context.
/// `[auto_models.<key>]` declares an extra pool next to the default
/// `[auto_model]`. Each pool is its own sentinel, so `auto` must stay on the
/// cloud pool and `auto_home` on the home pool — a request may never leak from
/// one rotation set into the other.
#[tokio::test]
async fn test_named_auto_pools_keep_their_rotation_sets_apart() {
    let cloud = MockServer::start().await;
    let home = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-cloud")))
        .expect(1)
        .mount(&cloud)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-home")))
        .expect(1)
        .mount(&home)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.cloud]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.home]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

# The default pool: cloud vendors only.
[auto_model]
enabled = true

[[auto_model.pool]]
provider = "cloud"
model = "model-cloud"

# The home pool: local deployments only.
[auto_models.auto_home]

[[auto_models.auto_home.pool]]
provider = "home"
model = "model-home"

[[routes]]
path = "/v1/cloud"
provider = "auto"
auto_model = true

[[routes]]
path = "/v1/home"
provider = "auto_home"
auto_model = true
"#;
    let gw = TestGateway::start_with_fallback(config, &cloud.uri(), &home.uri()).await;
    let c = client();

    for (path, model) in [("/v1/cloud", "auto"), ("/v1/home", "auto_home")] {
        let resp = c
            .post(gw.url(path))
            .json(&chat_body(model, "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{path} must be served");
    }

    let cloud_reqs = cloud.received_requests().await.unwrap_or_default();
    let home_reqs = home.received_requests().await.unwrap_or_default();
    assert_eq!(
        cloud_reqs.len(),
        1,
        "the cloud pool serves exactly one call"
    );
    assert_eq!(home_reqs.len(), 1, "the home pool serves exactly one call");

    let cloud_body: Value = serde_json::from_slice(&cloud_reqs[0].body).expect("json body");
    let home_body: Value = serde_json::from_slice(&home_reqs[0].body).expect("json body");
    assert_eq!(cloud_body["model"], "model-cloud");
    assert_eq!(home_body["model"], "model-home");
    assert!(
        home_body["model"] != "model-cloud" && cloud_body["model"] != "model-home",
        "a request must never rotate over another pool's entries"
    );
}

/// Billing tiers: a `metered` (pay-per-token) entry is ranked after every
/// `subscription` entry, regardless of where it sits in the pool. Declared
/// first and with a healthy upstream, it must still see zero traffic while
/// the prepaid entry can serve.
#[tokio::test]
async fn test_auto_pool_never_prefers_a_metered_entry() {
    let sub = MockServer::start().await;
    let paid = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-sub")))
        .expect(4)
        .mount(&sub)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-paid")))
        .mount(&paid)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.sub]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.paid]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true

# The metered entry is declared first on purpose: selection must order by
# billing tier, not by declaration order.
[[auto_model.pool]]
provider = "paid"
model = "model-paid"
billing = "metered"

[[auto_model.pool]]
provider = "sub"
model = "model-sub"

[[routes]]
path = "/v1/chat/completions"
provider = "auto"
auto_model = true
"#;
    let gw = TestGateway::start_with_fallback(config, &sub.uri(), &paid.uri()).await;
    let c = client();

    for _ in 0..4 {
        let resp = c
            .post(gw.url("/v1/chat/completions"))
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["choices"][0]["message"]["content"], "from-sub",
            "every call must be served by the subscription entry"
        );
    }

    let paid_reqs = paid.received_requests().await.unwrap_or_default();
    assert_eq!(
        paid_reqs.len(),
        0,
        "a healthy subscription entry must keep the metered entry idle"
    );
    let sub_reqs = sub.received_requests().await.unwrap_or_default();
    assert_eq!(sub_reqs.len(), 4);
    let sent: Value = serde_json::from_slice(&sub_reqs[0].body).expect("json body");
    assert_eq!(sent["model"], "model-sub");
}

/// Billing tiers: once every subscription entry is unavailable (circuit open
/// after an upstream failure), the same request walks into the metered entry
/// instead of failing. The metered selection counter records the downgrade.
#[tokio::test]
async fn test_auto_pool_falls_back_to_metered_only_after_subscriptions_fail() {
    let subs = MockServer::start().await;
    let paid = MockServer::start().await;

    // Every subscription entry answers 500.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .mount(&subs)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-paid")))
        .mount(&paid)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[providers.sub_a]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
max_fails = 1

[providers.sub_b]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
max_fails = 1

[providers.paid]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true

[[auto_model.pool]]
provider = "sub_a"
model = "model-sub-a"

[[auto_model.pool]]
provider = "sub_b"
model = "model-sub-b"

[[auto_model.pool]]
provider = "paid"
model = "model-paid"
billing = "metered"

[[routes]]
path = "/v1/chat/completions"
provider = "auto"
auto_model = true
"#;
    let gw = TestGateway::start_with_fallback(config, &subs.uri(), &paid.uri()).await;
    let c = client();

    // Both subscription entries answer 500, so the request has to walk past
    // both of them and land on the metered entry.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("auto", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from-paid");

    let sub_reqs = subs.received_requests().await.unwrap_or_default();
    assert_eq!(
        sub_reqs.len(),
        2,
        "both subscription entries must be tried before the metered one"
    );
    let paid_reqs = paid.received_requests().await.unwrap_or_default();
    assert_eq!(paid_reqs.len(), 1, "the metered entry serves the request");
    let sent: Value = serde_json::from_slice(&paid_reqs[0].body).expect("json body");
    assert_eq!(sent["model"], "model-paid");

    let metric = metric_sample(&gw, "maxllm_auto_pool_metered_selections_total").await;
    assert_eq!(
        metric,
        Some(1.0),
        "the downgrade must be observable in metrics"
    );

    // With both subscription circuits open, later requests go straight to
    // the metered entry.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("auto", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// A client that carries a named sentinel in the body only (its base URL still
/// points at a plain route) is adopted into that named pool: the provider it
/// landed on is a member, so the body is rewritten to the entry's model and
/// every later hop stays inside that pool.
#[tokio::test]
async fn test_body_sentinel_adopts_the_named_pool() {
    let member = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-member")))
        .expect(1)
        .mount(&member)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.member]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[auto_models.auto_home]

[[auto_models.auto_home.pool]]
provider = "member"
model = "model-home"

# Not an auto route: the sentinel arrives in the body, after the provider was
# picked — which happens to be a member of the auto_home pool.
[[routes]]
path = "/v1/responses"
endpoint_type = "responses"
provider = "member"
"#;
    let gw = TestGateway::start(config, &member.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .json(&json!({"model": "auto_home", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the pool member serves the request directly (body: {})",
        resp.text().await.unwrap_or_default()
    );

    let reqs = member.received_requests().await.unwrap_or_default();
    assert_eq!(reqs.len(), 1, "one call, no wasted hop");
    let sent: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "model-home",
        "the named sentinel must be rewritten before it reaches the upstream"
    );
}

#[tokio::test]
async fn test_auto_model_prefers_the_session_binding_over_round_robin() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-a")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-b")))
        .mount(&b)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100

[providers.alpha]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.beta]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "alpha"
model = "model-a"

[[auto_model.pool]]
provider = "beta"
model = "model-b"

[[routes]]
path = "/v1/chat/completions"
provider = "alpha"
auto_model = true
session_sticky = true
"#;
    let gw = TestGateway::start_with_fallback(config, &a.uri(), &b.uri()).await;
    let c = client();

    // Two requests with the same session id: the first binds the session to
    // whichever provider served it, the second must reuse that binding
    // instead of rotating to the other provider.
    for _ in 0..2 {
        let resp = c
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer test-key-123")
            .header("session-id", "sess-1")
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let a_reqs = a.received_requests().await.unwrap_or_default();
    let b_reqs = b.received_requests().await.unwrap_or_default();
    assert_eq!(
        a_reqs.len() + b_reqs.len(),
        2,
        "both requests must be served"
    );
    assert_eq!(
        a_reqs.len(),
        2,
        "the session binding must keep both requests on the same provider"
    );
    assert_eq!(b_reqs.len(), 0, "the bound provider must not rotate away");
}

/// The rotation cohort and the sticky pin are judged by *two different* rules.
///
/// Once a provider's recent whole-exchange median is above the rotation
/// ceiling it is filtered out of the round-robin, so a three-provider tier
/// with one slow entry deterministically excludes it (sorted measurements
/// [fast, fast, slow] put the median on a fast entry) and no *new* session
/// lands on it.
///
/// A session already pinned to it is a different question: at 6s it is above
/// the cohort ceiling but below the pin escape's absolute 10s floor, so it
/// stays. Moving it would throw away the upstream prompt cache the
/// conversation was built on, and one slow turn is cheaper than a cold
/// conversation. (`auto_pool_pin_escapable` — the escape itself — is pinned
/// down by the unit tests and by the 10.5s sibling test below.)
#[tokio::test]
async fn test_auto_pool_keeps_a_warm_pin_that_is_only_above_the_cohort() {
    let slow = MockServer::start().await;
    let fast = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response_warm("from-slow"))
                .set_delay(Duration::from_secs(6)),
        )
        .mount(&slow)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response_warm("from-fast")))
        .mount(&fast)
        .await;

    // Declaration order puts the slow entry first, so the very first request
    // (no measurements yet, whole tier) lands on it and pins the session to it.
    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100

[providers.slow]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.fast_a]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[providers.fast_b]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "slow"
model = "model-slow"

[[auto_model.pool]]
provider = "fast_a"
model = "model-fast-a"

[[auto_model.pool]]
provider = "fast_b"
model = "model-fast-b"

[[routes]]
path = "/v1/chat/completions"
provider = "auto"
auto_model = true
session_sticky = true
"#;
    let gw = TestGateway::start_with_fallback(config, &slow.uri(), &fast.uri()).await;
    let c = client();

    let send = |session: Option<&'static str>| {
        let mut req = c
            .post(gw.url("/v1/chat/completions"))
            .json(&chat_body("auto", "hi"));
        if let Some(s) = session {
            req = req.header("session-id", s);
        }
        async move {
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200);
            let body: Value = resp.json().await.unwrap();
            body["choices"][0]["message"]["content"]
                .as_str()
                .unwrap()
                .to_string()
        }
    };

    // 1) First request pins session "sess-slow" to the slow entry (6s).
    assert_eq!(send(Some("sess-slow")).await, "from-slow");
    // 2) and 3) Measure both fast entries. Two measurements are not enough
    //    for the median to point at a fast entry yet, so the slow one is still
    //    inside the cohort and the round-robin walks past it by index.
    assert_eq!(send(None).await, "from-fast");
    assert_eq!(send(None).await, "from-fast");

    // 4) With [fast, fast, slow] measured, the slow entry is above the
    //    rotation ceiling: this rotation skips it (and books the skip counter)
    //    instead of handing a new session a 6s wait.
    assert_eq!(send(None).await, "from-fast");

    // 5) The warm session, however, keeps its pin: 6s is under the pin
    //    escape's absolute 10s floor, so the cohort filter does not bounce it.
    assert_eq!(
        send(Some("sess-slow")).await,
        "from-slow",
        "a warm pin below the escape floor must not be bounced by the cohort filter"
    );

    let slow_reqs = slow.received_requests().await.unwrap_or_default();
    assert_eq!(
        slow_reqs.len(),
        2,
        "the pin keeps serving the session it warmed (first request + this one)"
    );
    let fast_reqs = fast.received_requests().await.unwrap_or_default();
    assert_eq!(
        fast_reqs.len(),
        3,
        "every request after the measurement lands on a fast entry"
    );

    // The filter is observable.
    let body = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.lines().any(
            |l| l.starts_with("maxllm_auto_pool_slow_skips_total{provider=\"slow\"}")
                && !l.trim_end().ends_with(" 0")
        ),
        "the skip counter must record the slow entry; metrics tail:\n{body}"
    );
}

/// The escape half of the sticky-pin rule: a pin that is *both* more than
/// 1.5x the tier median and slower than 10s in absolute terms is abandoned for
/// speed, exactly once, and the session re-pins on a fast entry. The slow mock
/// answers after 10.5s, which clears the absolute floor that keeps the 6s
/// sibling test pinned.
#[tokio::test]
async fn test_auto_pool_escapes_a_pin_that_is_slow_enough() {
    let slow = MockServer::start().await;
    let fast = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response_warm("from-slow"))
                .set_delay(Duration::from_millis(10_500)),
        )
        .mount(&slow)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response_warm("from-fast")))
        .mount(&fast)
        .await;

    // Slow declared first: with no measurements the whole tier is the cohort
    // and the first request lands on the slow entry, pinning the session to it.
    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100

[providers.slow]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.fast_a]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[providers.fast_b]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "slow"
model = "model-slow"

[[auto_model.pool]]
provider = "fast_a"
model = "model-fast-a"

[[auto_model.pool]]
provider = "fast_b"
model = "model-fast-b"

[[routes]]
path = "/v1/chat/completions"
provider = "auto"
auto_model = true
session_sticky = true
"#;
    let gw = TestGateway::start_with_fallback(config, &slow.uri(), &fast.uri()).await;
    let c = client();

    let send = |session: Option<&'static str>| {
        let mut req = c
            .post(gw.url("/v1/chat/completions"))
            .json(&chat_body("auto", "hi"));
        if let Some(s) = session {
            req = req.header("session-id", s);
        }
        async move {
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200);
            let body: Value = resp.json().await.unwrap();
            body["choices"][0]["message"]["content"]
                .as_str()
                .unwrap()
                .to_string()
        }
    };

    // 1) Pins the session to the slow entry (10.5s).
    assert_eq!(send(Some("sess-slow")).await, "from-slow");
    // 2) and 3) Measure both fast entries: idx 1 and 2 of the still-unfiltered
    //    cohort (its only measurement is the slow one, so the ceiling still
    //    covers everything).
    assert_eq!(send(None).await, "from-fast");
    assert_eq!(send(None).await, "from-fast");

    // The tier is now [fast, fast, slow] with a fast median: the pin is
    // 1.5x+ over the median *and* over the 10s floor, so it escapes.
    assert_eq!(
        send(Some("sess-slow")).await,
        "from-fast",
        "a pin that clears both escape conditions must be abandoned for speed"
    );

    let slow_reqs = slow.received_requests().await.unwrap_or_default();
    assert_eq!(
        slow_reqs.len(),
        1,
        "the slow entry serves only the request that produced its measurement"
    );
    let fast_reqs = fast.received_requests().await.unwrap_or_default();
    assert_eq!(
        fast_reqs.len(),
        3,
        "every request after the escape must land on a fast entry"
    );
}

/// A provider with its own stall budget must not hold the client for the
/// route's whole timeout: the upstream accepts the connection, says nothing,
/// and the budget cuts it — then the walk moves on instead of serving the
/// stall.
///
/// Without the budget the request waited for the route's 120s and the client
/// saw the upstream's own error text.
#[tokio::test]
async fn test_read_timeout_budget_stops_a_stuck_upstream_and_fails_over() {
    let stuck = MockServer::start().await;
    let backup = MockServer::start().await;

    // The upstream takes the request and never answers within the test.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("never"))
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&stuck)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from backup")))
        .mount(&backup)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.stuck]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
# The stall budget: one second of silence from the upstream is enough.
read_timeout_secs = 1

[providers.backup]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "stuck"
fallback = ["backup"]
strategy = "fallback"
max_failover_attempts = 1
"#;

    let gw = TestGateway::start_with_fallback(config, &stuck.uri(), &backup.uri()).await;
    let c = client();

    let started = std::time::Instant::now();
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("model-x", "hi"))
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();

    // The budget cut the stall and the walk moved on: the client got the
    // backup provider's answer, not the stuck one's.
    assert_eq!(resp.status(), 200, "the stuck provider must fail over");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "backup");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "stuck"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from backup");

    // The 5s stall was cut by the 1s budget: the answer came back long
    // before the upstream would have replied.
    assert!(
        elapsed < Duration::from_secs(4),
        "the read budget did not cut the stall: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "the budget fired earlier than the read timeout: {elapsed:?}"
    );

    // Both providers were tried exactly once.
    let stuck_reqs = stuck.received_requests().await.unwrap();
    assert_eq!(stuck_reqs.len(), 1);
    let backup_reqs = backup.received_requests().await.unwrap();
    assert_eq!(backup_reqs.len(), 1);
}

/// The **first-byte deadline**: an upstream that takes the request and does
/// not produce its first byte in time is given up on, and the request walks
/// on to the next member of the pool.
///
/// The stall budget (`read_timeout_secs`) is a *per read* budget and says
/// nothing about how long the upstream may take to start answering; when it
/// is generous, a member that accepts the request and then goes quiet holds
/// the client for the whole budget — long after the client gave up. The
/// first-byte deadline is the budget for the start only: once the upstream
/// produces its first byte the stream may take as long as it needs.
#[tokio::test]
async fn test_first_byte_deadline_cuts_a_slow_start_and_fails_over() {
    let slow = MockServer::start().await;
    let backup = MockServer::start().await;

    let chat_sse = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    // The upstream accepts the request and answers five seconds later.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(chat_sse, "text/event-stream")
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&slow)
        .await;

    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(chat_sse, "text/event-stream"),
        )
        .mount(&backup)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.slow]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
# The stall budget is generous on purpose: the read timeout must not be what
# saves the request, the first-byte deadline must.
read_timeout_secs = 60
first_byte_timeout_secs = 1

[providers.backup]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "slow"
fallback = ["backup"]
strategy = "fallback"
max_failover_attempts = 1
"#;

    let gw = TestGateway::start_with_fallback(config, &slow.uri(), &backup.uri()).await;
    let c = client();

    let started = std::time::Instant::now();
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "model-x",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(resp.status(), 200, "the slow member must fail over");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "backup");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Fallback-From").unwrap(),
        "slow"
    );

    // The deadline cut the wait for the first byte: the client got the
    // backup member's answer long before the slow one would have replied.
    assert!(
        elapsed < Duration::from_secs(4),
        "the first-byte deadline did not cut the slow start: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "the deadline fired earlier than it should have: {elapsed:?}"
    );

    let slow_reqs = slow.received_requests().await.unwrap();
    assert_eq!(slow_reqs.len(), 1);
    let backup_reqs = backup.received_requests().await.unwrap();
    assert_eq!(backup_reqs.len(), 1);
}

/// The first-byte deadline is a budget for the **start** of the answer, so
/// it only applies to a streaming request: there the first byte is the first
/// token, which a healthy member produces in a couple of seconds. A
/// non-streaming request gets its answer in one piece — its first byte is
/// the whole answer — and the route's own timeout governs it.
#[tokio::test]
async fn test_first_byte_deadline_does_not_cut_a_non_streaming_answer() {
    let slow = MockServer::start().await;
    let backup = MockServer::start().await;

    // The upstream answers after two and a half seconds — well past the
    // first-byte deadline, but it is the whole answer.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("slow but complete"))
                .set_delay(Duration::from_millis(2500)),
        )
        .mount(&slow)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from backup")))
        .mount(&backup)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.slow]
kind = "openai_compat"
base_url = "{MOCK_URL}"
api_key = "fake-key"
read_timeout_secs = 60
first_byte_timeout_secs = 1

[providers.backup]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "slow"
fallback = ["backup"]
strategy = "fallback"
max_failover_attempts = 1
"#;

    let gw = TestGateway::start_with_fallback(config, &slow.uri(), &backup.uri()).await;
    let c = client();

    let started = std::time::Instant::now();
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("model-x", "hi"))
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(resp.status(), 200);
    // The answer came from the slow member: the deadline did not fire.
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "slow");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "slow but complete"
    );
    assert!(
        elapsed >= Duration::from_millis(2000),
        "the non-streaming answer was cut short: {elapsed:?}"
    );

    let backup_reqs = backup.received_requests().await.unwrap();
    assert_eq!(backup_reqs.len(), 0, "the backup must not be asked");
}

/// A client that selects the `auto` model on a route that does **not**
/// declare `auto_model = true` (a model-picker switch without moving the base
/// URL onto the auto route) must still rotate: the sentinel is a rotation
/// request, not an upstream model name. The first attempt hits the route's own
/// provider and fails with the upstream's "unknown model" answer; the walk
/// then switches into the `[auto_model]` pool — instead of the route's
/// fallback chain, which would collect the same 404 from every provider — and
/// rewrites the body's model to the pool entry's model.
#[tokio::test]
async fn test_auto_sentinel_on_a_non_auto_route_fails_over_to_the_pool() {
    let summary = MockServer::start().await;
    let pool = MockServer::start().await;

    // The route's own provider has never heard of a model called "auto".
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {
                "code": "UnsupportedModel",
                "message": "The requested model does not support this feature."
            }
        })))
        .mount(&summary)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-pool")))
        .mount(&pool)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.summary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.pooled]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pooled"
model = "model-b"

# The route itself is NOT an auto route — it is the summary route the client
# happens to be pointing at.
[[routes]]
path = "/v1/chat/completions"
provider = "summary"
"#;
    let gw = TestGateway::start_with_fallback(config, &summary.uri(), &pool.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("session-id", "sess-auto-off-route")
        .json(&chat_body("auto", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the sentinel must be served by the auto pool, not fail with the upstream 404"
    );

    // The sentinel lives in the body, and the provider has to be picked before
    // the body is read, so the route's own provider unavoidably sees the
    // literal sentinel once and rejects it. What matters is the retry: it must
    // land in the pool carrying that entry's model, not "auto" again.
    let summary_reqs = summary.received_requests().await.unwrap_or_default();
    assert_eq!(
        summary_reqs.len(),
        1,
        "one wasted hop to the route's own provider, then the pool"
    );
    let sent: Value = serde_json::from_slice(&summary_reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "auto",
        "the first hop carries the literal sentinel"
    );

    let pool_reqs = pool.received_requests().await.unwrap_or_default();
    assert_eq!(pool_reqs.len(), 1, "the pool provider must serve the retry");
    let sent: Value = serde_json::from_slice(&pool_reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "model-b",
        "the replayed body must carry the pool entry's model"
    );
}

/// The sentinel can only be seen in the body on a route that does not declare
/// `auto_model`, and the provider is chosen before the body is read. When the
/// provider that was chosen is itself a pool member — a session pinned to one
/// from an earlier turn, or a route whose own provider is listed in the pool —
/// the body must be rewritten to that entry's model. Forwarding the literal
/// sentinel would cost a 404 and a failover to a *different* member on every
/// turn, throwing away the upstream's prompt cache for the conversation.
#[tokio::test]
async fn test_body_sentinel_is_rewritten_for_a_pool_member_on_a_plain_route() {
    let member = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-member")))
        .expect(1)
        .mount(&member)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.member]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "member"
model = "model-member"

# Not an auto route: the sentinel arrives in the body, too late to take part
# in picking the provider — which happens to be a pool member anyway.
[[routes]]
path = "/v1/responses"
endpoint_type = "responses"
provider = "member"
"#;
    let gw = TestGateway::start(config, &member.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .json(&json!({"model": "auto", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the pool member serves the request directly (body: {})",
        resp.text().await.unwrap_or_default()
    );

    let reqs = member.received_requests().await.unwrap_or_default();
    assert_eq!(reqs.len(), 1, "one call, no wasted 404 hop");
    let sent: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "model-member",
        "the sentinel must be rewritten before it reaches the upstream"
    );
}

/// A provider that speaks the Responses contract natively used to keep the
/// client's exact bytes (Content-Length framing), so the sentinel could never
/// be rewritten: every `{"model": "auto"}` on a plain route cost a 404 from the
/// first provider and a failover. On the production gateway 1,931 failed
/// upstream attempts carried `model: "auto"` for exactly this reason.
///
/// The body must be framed chunked in that case — the same framing the auto
/// route already uses for these providers — so request_body_filter can resolve
/// the sentinel to the pool entry's model.
#[tokio::test]
async fn test_body_sentinel_is_rewritten_for_a_native_responses_provider() {
    let member = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-member")))
        .expect(1)
        .mount(&member)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.member]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
# The upstream already speaks the Responses contract, so the body would be
# forwarded byte-for-byte — unless it has to be rewritten.
native_responses = true

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "member"
model = "model-member"

# Not an auto route: the sentinel arrives in the body, after the provider was
# picked. It is a pool member, so the body must be rewritten for it.
[[routes]]
path = "/v1/responses"
endpoint_type = "responses"
provider = "member"
"#;
    let gw = TestGateway::start(config, &member.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .json(&json!({"model": "auto", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the pool member serves the request directly (body: {})",
        resp.text().await.unwrap_or_default()
    );

    let reqs = member.received_requests().await.unwrap_or_default();
    assert_eq!(reqs.len(), 1, "one call, no wasted 404 hop");
    let sent: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "model-member",
        "the sentinel must never reach a native-Responses upstream"
    );
}

/// The same sentinel on a route that *does* declare `auto_model = true` keeps
/// its old behaviour: the pool is used from the first attempt, with no wasted
/// call to the route's `provider` sentinel.
#[tokio::test]
async fn test_auto_sentinel_on_an_auto_route_skips_the_route_provider() {
    let pool = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-pool")))
        .mount(&pool)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.pooled]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pooled"
model = "model-b"

[[routes]]
path = "/v1/chat/completions"
provider = "pooled"
auto_model = true
"#;
    let gw = TestGateway::start(config, &pool.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("auto", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let reqs = pool.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        1,
        "exactly one upstream call — no wasted attempt"
    );
    let sent: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(sent["model"], "model-b");
}

/// Spawn the gateway with three upstream placeholders — the route's own
/// provider plus two auto-pool members — and wait until it answers `/health`.
/// The auto-pool tests need one more upstream than [`TestGateway`] supports.
/// A gateway child that is reaped when it goes out of scope.
///
/// This helper used to hand out a bare `Child`, so a test that failed on an
/// assertion leaked the process. The leak kept the test binary's stdout open,
/// and a piped `cargo test | tail` then blocked forever instead of reporting
/// the failure. The guard kills *and* waits, so the child is gone before the
/// process exits.
struct GatewayChild(Child);

impl Deref for GatewayChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for GatewayChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for GatewayChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn spawn_gateway_with_pool(
    config: &str,
    urls: [&str; 3],
) -> (GatewayChild, String, tempfile::NamedTempFile) {
    // `pick_gateway_port` (not `portpicker` alone) keeps two tests running in
    // parallel from being handed the same port; the loser used to lose the
    // bind and then talk to the winner's gateway.
    let port = pick_gateway_port();
    let config_str = config
        .replace("{PORT}", &port.to_string())
        .replace("{MOCK_URL}", urls[0])
        .replace("{POOL_A_URL}", urls[1])
        .replace("{POOL_B_URL}", urls[2]);
    let mut config_file = tempfile::NamedTempFile::new().expect("temp file");
    config_file
        .write_all(config_str.as_bytes())
        .expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_maxllm-server"))
        .arg("--config")
        .arg(config_file.path())
        .env("RUST_LOG", "warn")
        .env_remove("HTTPS_PROXY")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("http_proxy")
        .spawn()
        .expect("spawn gateway");
    let mut child = GatewayChild(child);
    let base = format!("http://127.0.0.1:{port}");
    let c = client();
    // Wait for the gateway, watching the child as well: a gateway that died
    // during startup must be reported here, not later as "connection refused"
    // on the first request.
    // A safety net, not the expected wait — see the note on the other
    // gateway helper: the loop breaks as soon as /health answers.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("gateway did not start within 60 seconds (port {port})");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("gateway exited during startup ({status}); was port {port} taken?");
        }
        if c.get(format!("{base}/health"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (child, base, config_file)
}

/// Config shared by the two "sentinel on a non-auto route" tests: the route's
/// own provider answers 404 (no model called `auto`), the pool has two healthy
/// members that both answer 200.
const SENTINEL_POOL_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.summary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.pool_a]
kind = "openai"
base_url = "{POOL_A_URL}"
api_key = "fake-key"

[providers.pool_b]
kind = "openai"
base_url = "{POOL_B_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pool_a"
model = "model-a"

[[auto_model.pool]]
provider = "pool_b"
model = "model-b"

[[routes]]
path = "/v1/chat/completions"
provider = "summary"
session_sticky = true
"#;

/// The sentinel on a non-auto route enters the pool mid-request, so it never
/// passes through the `select_provider` rotation slot. It must still rotate:
/// walking the pool in order would pin every such request to the first working
/// entry and leave the rest idle. Four requests over a two-entry pool must use
/// both entries.
#[tokio::test]
async fn test_auto_sentinel_on_a_non_auto_route_rotates_across_the_pool() {
    let summary = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": "UnsupportedModel", "message": "unknown model"}
        })))
        .mount(&summary)
        .await;
    for mock in [&a, &b] {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
            .mount(mock)
            .await;
    }

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(SENTINEL_POOL_CONFIG, [&summary.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    for i in 0..4 {
        let resp = c
            .post(format!("{base}/v1/chat/completions"))
            .header("session-id", format!("rotate-{i}"))
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "request {i} must be served from the pool"
        );
    }

    let a_reqs = a.received_requests().await.unwrap_or_default();
    let b_reqs = b.received_requests().await.unwrap_or_default();
    assert_eq!(a_reqs.len() + b_reqs.len(), 4, "every request served once");
    assert!(
        !a_reqs.is_empty() && !b_reqs.is_empty(),
        "the sentinel must rotate over the pool, got a={} b={}",
        a_reqs.len(),
        b_reqs.len()
    );

    child.kill().ok();
    child.wait().ok();
}

/// Once a sentinel request has landed on a pool provider, the session keeps
/// using it: the upstream keeps its context instead of hopping between keys.
#[tokio::test]
async fn test_auto_sentinel_keeps_the_session_on_one_pool_provider() {
    let summary = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": "UnsupportedModel", "message": "unknown model"}
        })))
        .mount(&summary)
        .await;
    for mock in [&a, &b] {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
            .mount(mock)
            .await;
    }

    let sticky_config = format!(
        "[session_sticky]\nenabled = true\ntimeout_secs = 3600\nmax_entries = 100\n{}",
        SENTINEL_POOL_CONFIG
    );
    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(&sticky_config, [&summary.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    for _ in 0..3 {
        let resp = c
            .post(format!("{base}/v1/chat/completions"))
            .header("session-id", "sticky-session")
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let a_reqs = a.received_requests().await.unwrap_or_default();
    let b_reqs = b.received_requests().await.unwrap_or_default();
    assert_eq!(a_reqs.len() + b_reqs.len(), 3, "every request served once");
    assert!(
        a_reqs.is_empty() || b_reqs.is_empty(),
        "the session binding must keep all three on one provider, got a={} b={}",
        a_reqs.len(),
        b_reqs.len()
    );

    child.kill().ok();
    child.wait().ok();
}

// ---------------------------------------------------------------------------
// Gap-closure tests: requests_total producer, the active-requests gauge
// balance on gateway-level errors, and the latency-/cost-based strategies.
// ---------------------------------------------------------------------------

const ROUTING_METRICS_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
"#;

/// `maxllm_requests_total` was defined but never recorded — the README's
/// metrics table listed a series the gateway never emitted. A served request
/// must appear with its provider, model and upstream status labels.
#[tokio::test]
async fn test_requests_total_is_recorded_per_provider_model_status() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&upstream)
        .await;

    let gw = TestGateway::start(ROUTING_METRICS_CONFIG, &upstream.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let text = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let series = text
        .lines()
        .find(|l| {
            l.starts_with("maxllm_requests_total")
                && l.contains("provider=\"primary\"")
                && l.contains("model=\"gpt-4o\"")
                && l.contains("status=\"200\"")
        })
        .unwrap_or_else(|| panic!("maxllm_requests_total series missing:\n{text}"));
    let value: f64 = series.rsplit_once(' ').unwrap().1.parse().unwrap();
    assert!(
        value >= 1.0,
        "requests_total must count the request: {series}"
    );
}

/// The gateway-level error paths (405/404/plugin rejections/admin answers)
/// decrement the active-request gauge themselves and answer the client — the
/// logging hook must not decrement it again, or the gauge drifts negative.
#[tokio::test]
async fn test_active_requests_stays_balanced_after_gateway_level_errors() {
    let upstream = MockServer::start().await;
    let gw = TestGateway::start(ROUTING_METRICS_CONFIG, &upstream.uri()).await;
    let c = client();

    // 405: GET on an API route.
    let resp = c
        .get(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    // 404: POST to a path with no route.
    let resp = c
        .post(gw.url("/v1/unknown"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // 401: missing auth (the plugin chain answers the request itself).
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Admin API (not enabled in this config — the handler still answers).
    let resp = c
        .get(gw.url("/admin/keys"))
        .header("Authorization", "Bearer test-key-123")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let text = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let series = text
        .lines()
        .find(|l| l.starts_with("maxllm_active_requests "))
        .unwrap_or_else(|| panic!("maxllm_active_requests series missing:\n{text}"));
    let value: f64 = series.rsplit_once(' ').unwrap().1.parse().unwrap();
    assert_eq!(
        value, 0.0,
        "the gauge must be balanced after gateway-level errors, got {value}: {series}"
    );
    let _ = value;
}

const LATENCY_BASED_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.slow]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[providers.fast]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[[routes]]
path = "/v1/chat/completions"
provider = "slow"
fallback = ["fast"]
strategy = "latency_based"
"#;

/// The latency-based strategy is no longer a stub. Both providers get
/// measured without the primary ever being counted as a breaker failure:
///
/// 1. the primary answers 200 slowly — its latency estimate is recorded,
/// 2. the next request carries a body the primary does not carry, so it
///    answers 404, which fails the request over to the fallback *without*
///    counting a breaker failure — the fallback gets measured too,
/// 3. from then on the latency-based route picks the fastest healthy
///    provider, and the slow primary is no longer contacted.
#[tokio::test]
async fn test_latency_based_moves_traffic_to_the_fastest_provider() {
    let slow = MockServer::start().await;
    let fast = MockServer::start().await;

    // Mounted last → matched first: the warm-up request ("one") is served
    // slowly, everything else gets the 404 that walks the request over.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("one"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("from slow"))
                .set_delay(Duration::from_millis(400)),
        )
        .mount(&slow)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"error": {"message": "invalid params, unknown model"}})),
        )
        .mount(&slow)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("from fast"))
                .set_delay(Duration::from_millis(10)),
        )
        .mount(&fast)
        .await;

    let gw = TestGateway::start_with_fallback(LATENCY_BASED_CONFIG, &slow.uri(), &fast.uri()).await;
    let c = client();

    // 1. No measurements yet: the primary (cold-start order) serves it.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "one"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["X-MaxLLM-Provider"], "slow");

    // 2. The primary's 404 walks the request over to the fast fallback —
    //    without tripping the slow breaker, so both stay healthy.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "two"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["X-MaxLLM-Provider"], "fast");

    // 3. Both providers are measured and healthy: the fastest one wins.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "three"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()["X-MaxLLM-Provider"],
        "fast",
        "the latency-based route must prefer the fastest measured provider"
    );

    let slow_reqs = slow.received_requests().await.unwrap_or_default();
    let fast_reqs = fast.received_requests().await.unwrap_or_default();
    assert_eq!(
        slow_reqs.len(),
        2,
        "the slow primary serves exactly the warm-up and the 404 walk"
    );
    assert_eq!(fast_reqs.len(), 2, "the fast fallback serves steps 2 and 3");
}

const COST_BASED_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.expensive]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[providers.cheap]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "gpt-4o-mini"

[[routes]]
path = "/v1/chat/completions"
provider = "expensive"
fallback = ["cheap"]
strategy = "cost_based"
"#;

/// The cost-based strategy is no longer a stub: with the built-in price
/// table, gpt-4o is pricier than gpt-4o-mini, so traffic must land on the
/// cheaper fallback even though it is not the primary.
#[tokio::test]
async fn test_cost_based_routes_to_the_cheapest_provider() {
    let expensive = MockServer::start().await;
    let cheap = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("expensive")))
        .mount(&expensive)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("cheap")))
        .mount(&cheap)
        .await;

    let gw =
        TestGateway::start_with_fallback(COST_BASED_CONFIG, &expensive.uri(), &cheap.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()["X-MaxLLM-Provider"],
        "cheap",
        "cost-based routing must prefer the cheaper provider"
    );

    let expensive_reqs = expensive.received_requests().await.unwrap_or_default();
    let cheap_reqs = cheap.received_requests().await.unwrap_or_default();
    assert_eq!(
        expensive_reqs.len(),
        0,
        "the expensive primary must not be asked"
    );
    assert_eq!(cheap_reqs.len(), 1);
}

const INFLIGHT_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.a]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[providers.b]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
default_model = "gpt-4o-mini"

[[routes]]
path = "/v1/chat/completions"
provider = "a"
fallback = ["b"]
strategy = "least_connections"
"#;

/// The per-provider in-flight gauge is derived from the counter the
/// least-connections router reads, so it must return to zero once a request
/// finishes — proving the acquire/release wiring in the real request path.
#[tokio::test]
async fn test_provider_inflight_gauge_returns_to_zero() {
    let a = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&a)
        .await;

    let gw = TestGateway::start(INFLIGHT_CONFIG, &a.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let text = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let series = text
        .lines()
        .find(|l| {
            l.starts_with("maxllm_provider_inflight_requests") && l.contains("provider=\"a\"")
        })
        .unwrap_or_else(|| panic!("inflight series missing:\\n{text}"));
    let value: f64 = series.rsplit_once(' ').unwrap().1.parse().unwrap();
    assert_eq!(
        value, 0.0,
        "the in-flight count must settle back to 0: {series}"
    );
}

/// The least-connections strategy is no longer a proxy for the failure
/// count: a provider that already has a request in flight is skipped in
/// favour of the idle one.
#[tokio::test]
async fn test_least_connections_skips_the_busy_provider() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    // Provider "a" holds the matching request for 2 s; everything else
    // answers instantly. wiremock tries mocks in registration order (the
    // first mounted mock wins), so the specific matcher is mounted first.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("hold"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("held"))
                .set_delay(Duration::from_millis(2000)),
        )
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("quick")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("quick")))
        .mount(&b)
        .await;

    let gw = TestGateway::start_with_fallback(INFLIGHT_CONFIG, &a.uri(), &b.uri()).await;
    let c = client();

    // Start a request that stays in flight on "a". `send()` returns a lazy
    // future — spawn it so it actually runs while the next request selects.
    let slow = {
        let url = gw.url("/v1/chat/completions");
        tokio::spawn(async move {
            Client::new()
                .post(url)
                .header("Authorization", "Bearer test-key-123")
                .json(&chat_body("gpt-4o", "hold"))
                .send()
                .await
                .unwrap()
        })
    };
    // Wait until the gateway has actually counted the request as in flight.
    // A fixed sleep is a race: under a loaded test suite the gateway may not
    // have selected the provider yet, and the second request would then be
    // routed to "a" as well. The gauge is the condition the router reads,
    // so wait for the gauge.
    let mut counted = false;
    for _ in 0..500 {
        let text = c
            .get(gw.url("/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let busy = text
            .lines()
            .find(|l| {
                l.starts_with("maxllm_provider_inflight_requests") && l.contains("provider=\"a\"")
            })
            .and_then(|l| {
                l.rsplit_once(' ')
                    .map(|(_, v)| v.trim().parse::<f64>().unwrap_or(0.0))
            })
            .unwrap_or(0.0);
        if busy >= 1.0 {
            counted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        counted,
        "the gateway never counted the request as in flight"
    );

    // While "a" is busy, the second request must route to the idle "b".
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "now"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()["X-MaxLLM-Provider"],
        "b",
        "the least-connections route must skip the busy provider"
    );

    let slow = slow.await.unwrap();
    assert_eq!(slow.status(), 200);
    assert_eq!(slow.headers()["X-MaxLLM-Provider"], "a");
}

// ---------------------------------------------------------------------------
// Bedrock SigV4 tests
// ---------------------------------------------------------------------------

const BEDROCK_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[providers.bedrock_mock]
kind = "bedrock"
base_url = "{MOCK_URL}"
region = "us-east-1"

[[routes]]
path = "/v1/chat/completions"
provider = "bedrock_mock"
"#;

#[tokio::test]
async fn test_bedrock_sigv4_signs_request_and_routes_model_into_path() {
    let mock = MockServer::start().await;
    let model_id = "test-claude-haiku";

    // The gateway should POST to /model/{model_id}/invoke with SigV4 headers.
    Mock::given(method("POST"))
        .and(path(format!("/model/{model_id}/invoke")))
        .and(header_exists("Authorization"))
        .and(header_exists("X-Amz-Date"))
        .and(header_exists("X-Amz-Content-Sha256"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_bedrock_test",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello from Bedrock!"}],
            "model": model_id,
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 8, "output_tokens": 4}
        })))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(BEDROCK_CONFIG, &mock.uri()).await;

    let body = chat_body(model_id, "Hello!");
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();

    assert_eq!(status, 200, "body: {text}");
    let parsed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["object"], "chat.completion");
    assert_eq!(
        parsed["choices"][0]["message"]["content"],
        "Hello from Bedrock!"
    );
    assert_eq!(parsed["usage"]["prompt_tokens"], 8);
    assert_eq!(parsed["usage"]["completion_tokens"], 4);

    // Verify the Authorization header structure on the upstream call.
    let received = mock.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    let auth = received[0]
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"),
        "auth: {auth}"
    );
    assert!(
        auth.contains("/us-east-1/bedrock/aws4_request"),
        "auth: {auth}"
    );
    assert!(
        auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"),
        "auth: {auth}"
    );
    assert!(auth.contains("Signature="), "auth: {auth}");

    // Verify the Content-Sha256 header matches the body hash.
    let body_hash = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&received[0].body);
        hasher.finalize()
    };
    let expected_hash: String = body_hash.iter().map(|b| format!("{b:02x}")).collect();
    let sent_hash = received[0]
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(sent_hash, expected_hash, "payload hash mismatch");
}

#[tokio::test]
async fn test_bedrock_sigv4_error_response_forwarded() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/model/nonexistent-model/invoke"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "__type": "ResourceNotFoundException",
            "message": "Could not find model"
        })))
        .mount(&mock)
        .await;

    let gw = TestGateway::start(BEDROCK_CONFIG, &mock.uri()).await;

    let body = chat_body("nonexistent-model", "Hi");
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    let parsed: Value = resp.json().await.unwrap();
    assert_eq!(parsed["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn test_bedrock_sigv4_streaming_response_translated() {
    let mock = MockServer::start().await;

    // Bedrock streaming returns Anthropic SSE events.
    let sse_body = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"test-model\",\"stop_reason\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    Mock::given(method("POST"))
        .and(path("/model/stream-test-model/invoke-with-response-stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse_body)
                .append_header("Content-Type", "text/event-stream"),
        )
        .mount(&mock)
        .await;

    let gw = TestGateway::start(BEDROCK_CONFIG, &mock.uri()).await;

    let body = json!({
        "model": "stream-test-model",
        "messages": [{"role": "user", "content": "Hi"}],
        "stream": true
    });
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap();

    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "streaming body: {text}");

    // The translated stream should contain OpenAI-format SSE chunks.
    assert!(
        text.contains("data: "),
        "expected SSE data lines in: {text}"
    );
    assert!(
        text.contains("chat.completion.chunk"),
        "expected OpenAI chunk format in: {text}"
    );
}

// ---------------------------------------------------------------------------
// Virtual-key auth & budget enforcement tests
// ---------------------------------------------------------------------------

const VK_CONFIG: &str = r#"
global_plugins = ["request_id", "api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[admin]
master_key = "test-master-key"
enabled = true

[plugins.request_id]
category = "request_id"
header_name = "X-Request-Id"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["static-test-key"]

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;

const OPENAI_MOCK_RESPONSE: &str = r#"{"id":"cmpl-test","object":"chat.completion","created":1700000000,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"Hello!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}"#;

async fn openai_mock(mock: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(OPENAI_MOCK_RESPONSE))
        .mount(mock)
        .await;
}

/// Create a virtual key via the admin API, returning (raw_key, key_id).
async fn create_virtual_key(
    gw: &TestGateway,
    master_key: &str,
    name: &str,
    max_budget_usd: Option<f64>,
) -> (String, String) {
    let body = json!({
        "name": name,
        "max_budget_usd": max_budget_usd,
    });
    let resp = client()
        .post(gw.url("/admin/keys"))
        .header("Authorization", format!("Bearer {master_key}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create key failed: {}",
        resp.text().await.unwrap_or_default()
    );
    let parsed: Value = resp.json().await.unwrap();
    let key = parsed["key"].as_str().unwrap().to_string();
    let id = parsed["key_id"].as_str().unwrap().to_string();
    (key, id)
}

#[tokio::test]
async fn test_virtual_key_authenticates_and_records_spend() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    // Create a virtual key with a generous budget.
    let (vk, vk_id) = create_virtual_key(&gw, "test-master-key", "test-key", Some(1000.0)).await;

    // The virtual key authenticates even though it is not in the static
    // `key_auth` key set — the gateway's virtual-key path handles it.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "virtual key should authenticate: {text}");
    let parsed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["object"], "chat.completion");

    // Spend was recorded against the virtual key.
    let resp = client()
        .get(gw.url("/admin/spend/logs"))
        .header("Authorization", "Bearer test-master-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let logs: Value = resp.json().await.unwrap();
    let arr = logs.as_array().unwrap();
    assert_eq!(arr.len(), 1, "expected exactly one spend log, got {arr:?}");
    assert_eq!(arr[0]["key_id"], vk_id);
    assert_eq!(arr[0]["tokens_in"], 8);
    assert_eq!(arr[0]["tokens_out"], 4);
    assert_eq!(arr[0]["model"], "gpt-4o");
}

#[tokio::test]
async fn test_budget_exceeded_virtual_key_gets_402() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    // A key with $0 budget is immediately over-budget.
    let (vk, _id) = create_virtual_key(&gw, "test-master-key", "broke-key", Some(0.0)).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 402, "over-budget key should get 402: {text}");
    assert!(
        text.contains("budget"),
        "error should mention budget: {text}"
    );

    // No spend was recorded (the request was blocked).
    let resp = client()
        .get(gw.url("/admin/spend/logs"))
        .header("Authorization", "Bearer test-master-key")
        .send()
        .await
        .unwrap();
    let logs: Value = resp.json().await.unwrap();
    assert_eq!(logs.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_revoked_virtual_key_gets_401() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    let (vk, vk_id) = create_virtual_key(&gw, "test-master-key", "short-lived", Some(1000.0)).await;

    // Revoke the key.
    let resp = client()
        .delete(gw.url(&format!("/admin/keys/{vk_id}")))
        .header("Authorization", "Bearer test-master-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "revoked key should get 401");
}

/// Create a virtual key restricted to `allowed_models` via the admin API.
async fn create_virtual_key_with_models(
    gw: &TestGateway,
    master_key: &str,
    name: &str,
    allowed_models: &[&str],
) -> String {
    let body = json!({
        "name": name,
        "max_budget_usd": 1000.0,
        "allowed_models": allowed_models,
    });
    let resp = client()
        .post(gw.url("/admin/keys"))
        .header("Authorization", format!("Bearer {master_key}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create key failed: {}",
        resp.text().await.unwrap_or_default()
    );
    let parsed: Value = resp.json().await.unwrap();
    parsed["key"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn test_model_allowlist_blocks_other_models() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    let vk = create_virtual_key_with_models(&gw, "test-master-key", "limited", &["gpt-4o"]).await;

    // The allowed model goes through.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert_eq!(
        status,
        200,
        "allowed model should pass: {}",
        resp.text().await.unwrap_or_default()
    );

    // A model outside the allow-list is rejected with 403 before the
    // upstream is contacted.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o-mini", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 403, "blocked model should get 403: {text}");
    assert!(
        text.contains("not allowed"),
        "error should explain the denial: {text}"
    );

    // Only the allowed request ever reached the upstream.
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "only the allowed request should reach the upstream, got {}",
        requests.len()
    );
}

#[tokio::test]
async fn test_model_allowlist_glob_patterns() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    let vk = create_virtual_key_with_models(&gw, "test-master-key", "globbed", &["gpt-4o*"]).await;

    // A model matching the wildcard passes.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o-mini", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "wildcard-matching model should pass: {}",
        resp.text().await.unwrap_or_default()
    );

    // A model outside the pattern is denied.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("claude-sonnet-4-20250514", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

/// Create a virtual key with per-key rate limits via the admin API.
async fn create_virtual_key_with_limits(
    gw: &TestGateway,
    master_key: &str,
    name: &str,
    rpm_limit: Option<u32>,
    tpm_limit: Option<u64>,
) -> String {
    let body = json!({
        "name": name,
        "max_budget_usd": 1000.0,
        "rpm_limit": rpm_limit,
        "tpm_limit": tpm_limit,
    });
    let resp = client()
        .post(gw.url("/admin/keys"))
        .header("Authorization", format!("Bearer {master_key}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create key failed: {}",
        resp.text().await.unwrap_or_default()
    );
    let parsed: Value = resp.json().await.unwrap();
    parsed["key"].as_str().unwrap().to_string()
}

const SAME_PROVIDER_RETRY_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.backup]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "primary"
fallback = ["backup"]
strategy = "fallback"
num_retries = 1
retry_backoff_ms = 10
"#;

/// `num_retries` is wired: the first transient 5xx from the primary is
/// retried on the SAME provider (after `retry_backoff_ms`), and when that
/// retry succeeds the fallback chain is never touched.
#[tokio::test]
async fn test_num_retries_replays_a_transient_error_on_the_same_provider() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    // The primary answers 500 once, then 200: the same-provider retry must
    // hit the success. A closure is needed because wiremock mocks always
    // match (the request log, not `expect`, decides what the second call
    // sees).
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(500).set_body_json(json!({"error": "transient"}))
            } else {
                ResponseTemplate::new(200).set_body_json(openai_response("from primary retry"))
            }
        })
        .mount(&primary)
        .await;
    // The fallback must never be reached when the same-provider retry wins.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("never")))
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        SAME_PROVIDER_RETRY_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "the retried attempt must succeed");
    assert_eq!(
        resp.headers().get("X-MaxLLM-Provider").unwrap(),
        "primary",
        "the same-provider retry must serve the request on the primary"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "from primary retry"
    );

    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(
        primary_reqs.len(),
        2,
        "the primary must be tried twice (500 then the retry)"
    );
    let fallback_reqs = fallback.received_requests().await.unwrap_or_default();
    assert!(
        fallback_reqs.is_empty(),
        "the fallback must stay untouched when the same-provider retry succeeds"
    );
}

/// When the same-provider retry budget is exhausted (every attempt still
/// answers 5xx), the request walks on to the fallback chain as before: the
/// primary is tried `num_retries + 1` times, the fallback once.
#[tokio::test]
async fn test_num_retries_exhausted_falls_over_to_the_fallback() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "still down"})))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from fallback")))
        .mount(&fallback)
        .await;

    let gw = TestGateway::start_with_fallback(
        SAME_PROVIDER_RETRY_CONFIG,
        &primary.uri(),
        &fallback.uri(),
    )
    .await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "the fallback must serve the request");
    assert_eq!(resp.headers().get("X-MaxLLM-Provider").unwrap(), "backup");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from fallback");

    let primary_reqs = primary.received_requests().await.unwrap();
    assert_eq!(
        primary_reqs.len(),
        2,
        "num_retries = 1 must try the primary twice before failing over"
    );
    let fallback_reqs = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(
        fallback_reqs.len(),
        1,
        "the fallback must serve exactly the exhausted retry"
    );
}

const LEGACY_AUTH_CONFIG: &str = r#"
global_plugins = []

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[auth]
api_keys = ["legacy-key"]

[rate_limit]
requests_per_minute = 2

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;

#[tokio::test]
async fn test_legacy_auth_and_rate_limit_sections_are_honoured() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(LEGACY_AUTH_CONFIG, &mock.uri()).await;

    // The top-level [auth] section authenticates without any explicit
    // key_auth plugin.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer legacy-key")
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "the [auth] api_keys section must authenticate: {}",
        resp.text().await.unwrap_or_default()
    );

    // An unknown key is rejected.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer unknown-key")
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "the [auth] api_keys section must reject unknown keys"
    );

    // The top-level [rate_limit] section caps requests per minute. The two
    // requests already sent consumed the window; the next one is denied.
    for _ in 0..3 {
        let resp = client()
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer legacy-key")
            .json(&chat_body("gpt-4o", "Hi"))
            .send()
            .await
            .unwrap();
        let status = resp.status().as_u16();
        if status == 429 {
            let text = resp.text().await.unwrap();
            assert!(
                text.contains("Rate limit"),
                "the [rate_limit] section must enforce its window: {text}"
            );
            return;
        }
        assert_eq!(status, 200);
    }
    panic!("the [rate_limit] section never produced a 429");
}

#[tokio::test]
async fn test_virtual_key_rpm_limit_blocks_excess_requests() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    let vk =
        create_virtual_key_with_limits(&gw, "test-master-key", "rpm-limited", Some(2), None).await;

    // The first two requests are inside the RPM window.
    for i in 0..2 {
        let resp = client()
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", format!("Bearer {vk}"))
            .json(&chat_body("gpt-4o", "Hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            200,
            "request {i} should be inside the RPM window: {}",
            resp.text().await.unwrap_or_default()
        );
    }

    // The third request exceeds the window.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 429, "over-RPM key should get 429: {text}");
    assert!(
        text.contains("rate limit"),
        "error should mention the limit: {text}"
    );

    // The denied request never reached the upstream.
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        2,
        "only the admitted requests should reach the upstream"
    );
}

#[tokio::test]
async fn test_virtual_key_tpm_limit_blocks_excess_tokens() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    // The mock answers with 8 input + 4 output tokens.
    let vk =
        create_virtual_key_with_limits(&gw, "test-master-key", "tpm-limited", None, Some(10)).await;

    // The window is empty -> allowed; the response consumes 12 tokens.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "first request should pass: {}",
        resp.text().await.unwrap_or_default()
    );

    // The TPM window now holds 12 tokens, which is over the limit.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 429, "over-TPM key should get 429: {text}");
    assert!(
        text.contains("rate limit"),
        "error should mention the limit: {text}"
    );
}

#[tokio::test]
async fn test_virtual_key_limits_do_not_affect_static_keys() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    let vk =
        create_virtual_key_with_limits(&gw, "test-master-key", "rpm-limited", Some(1), None).await;

    // Burn the virtual key's single request.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", format!("Bearer {vk}"))
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // The static key is unaffected by the virtual key's window.
    for _ in 0..3 {
        let resp = client()
            .post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer static-test-key")
            .json(&chat_body("gpt-4o", "Hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            200,
            "static key should be unrestricted: {}",
            resp.text().await.unwrap_or_default()
        );
    }
}

#[tokio::test]
async fn test_static_key_bypasses_model_allowlist() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    // The allow-list only applies to the virtual key; the static config key
    // is unrestricted (model access control is a per-virtual-key feature).
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer static-test-key")
        .json(&chat_body("claude-sonnet-4-20250514", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "static key should be unrestricted: {}",
        resp.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn test_static_key_still_works_alongside_virtual_keys() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(VK_CONFIG, &mock.uri()).await;

    // A static key from the key_auth config still authenticates.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer static-test-key")
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // An unknown key is rejected by the plugin.
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer unknown-key")
        .json(&chat_body("gpt-4o", "Hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Virtual-key spend was NOT recorded for the static-key request.
    let resp = client()
        .get(gw.url("/admin/spend/logs"))
        .header("Authorization", "Bearer test-master-key")
        .send()
        .await
        .unwrap();
    let logs: Value = resp.json().await.unwrap();
    assert_eq!(logs.as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// Upstream accepts the connection, then hangs up before answering
// ---------------------------------------------------------------------------

/// A TCP listener that accepts every connection and immediately closes it.
///
/// This is the signature of a model server that crashed (or is being
/// restarted) while its supervisor still holds the listen socket: curl says
/// "Empty reply from server", pingora says `ConnectionClosed` while reading
/// the response headers, and — before `error_while_proxy` learned to replay —
/// the client got a hard 502 even though the rest of the pool was idle.
fn spawn_hang_up_upstream() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind hang-up listener");
    let addr = listener.local_addr().expect("local addr");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            // Dropping the stream closes it without writing a byte.
            drop(stream);
        }
    });
    format!("http://{addr}")
}

const HANG_UP_FALLBACK_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.dead]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.alive]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "dead"
fallback = ["alive"]
"#;

#[tokio::test]
async fn test_upstream_hanging_up_before_answering_fails_over() {
    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let dead_url = spawn_hang_up_upstream();
    let gw =
        TestGateway::start_with_fallback(HANG_UP_FALLBACK_CONFIG, &dead_url, &alive.uri()).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a socket that closes before the response headers must fail over, not 502"
    );
    let body = resp.text().await.unwrap_or_default();
    assert!(
        body.contains("from-alive"),
        "the fallback provider must serve the request, got: {body}"
    );

    let reqs = alive.received_requests().await.unwrap_or_default();
    assert_eq!(
        reqs.len(),
        1,
        "the fallback provider must have been called once"
    );
}

const HANG_UP_POOL_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.dead]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.alive]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "dead"
model = "model-dead"

[[auto_model.pool]]
provider = "alive"
model = "model-alive"

[[routes]]
path = "/v1/chat/completions"
provider = "alive"
auto_model = true
"#;

/// The pool rotation is only useful if a dead member costs one failed attempt
/// instead of the whole request: `auto` must stay green while a member is
/// down, whichever order the round-robin happens to pick.
#[tokio::test]
async fn test_auto_rotation_survives_a_pool_member_that_hangs_up() {
    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let dead_url = spawn_hang_up_upstream();
    let gw = TestGateway::start_with_fallback(HANG_UP_POOL_CONFIG, &dead_url, &alive.uri()).await;

    for i in 0..4 {
        let resp = client()
            .post(gw.url("/v1/chat/completions"))
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "auto request #{i} must survive the dead pool member"
        );
        let body = resp.text().await.unwrap_or_default();
        assert!(
            body.contains("from-alive"),
            "request #{i} must be served by the healthy member, got: {body}"
        );
    }

    let reqs = alive.received_requests().await.unwrap_or_default();
    assert!(
        reqs.len() >= 4,
        "every request must reach the healthy member, got {}",
        reqs.len()
    );
    let sent: Value = serde_json::from_slice(&reqs[0].body).expect("json body");
    assert_eq!(
        sent["model"], "model-alive",
        "the replayed attempt must carry the pool entry's model"
    );
}

/// A session pinned to a pool member that also sits in the route's fallback
/// chain must get the entry's model, not the raw sentinel.
///
/// This is the `/v1/responses` shape on the NAS: the route's provider does not
/// know `auto` (404), but its fallback chain lists the pool members, so the
/// session pins inside the chain. Before this fix the second request went
/// straight to the pinned member with `"model": "auto"` in the body — a strict
/// upstream answered 400 while a llama.cpp-style server accepted it and served
/// whatever model it was hosting.
#[tokio::test]
async fn test_sentinel_rewritten_when_the_session_is_pinned_to_a_pool_member() {
    let summary = MockServer::start().await;
    let member = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": "UnsupportedModel", "message": "unknown model"}
        })))
        .mount(&summary)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-member")))
        .mount(&member)
        .await;

    let config = r#"
[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.summary]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.member]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "member"
model = "model-member"

[[routes]]
path = "/v1/chat/completions"
provider = "summary"
fallback = ["member"]
"#;
    let gw = TestGateway::start_with_fallback(config, &summary.uri(), &member.uri()).await;
    let c = client();

    for i in 0..2 {
        let resp = c
            .post(gw.url("/v1/chat/completions"))
            .header("session-id", "pinned-in-chain")
            .json(&chat_body("auto", "hi"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "request {i} must succeed");
    }

    let reqs = member.received_requests().await.unwrap_or_default();
    assert_eq!(reqs.len(), 2, "one call per request");
    for (i, r) in reqs.iter().enumerate() {
        let sent: Value = serde_json::from_slice(&r.body).expect("json body");
        assert_eq!(
            sent["model"], "model-member",
            "request {i} must carry the pool entry's model, got {}",
            sent["model"]
        );
    }
}

/// A subscription tier that answers over-limit traffic with a **400**
/// `tier_limit_exceeded` (open-design's shape: "membership rpm limit
/// exceeded: 101/100 resets …") must be read as quota, not as a request
/// fault: the gateway parks the provider and walks the chain, and the
/// agent's turn survives on the next member. The client must never see the
/// lying 400.
#[tokio::test]
async fn test_tier_limit_400_fails_over_to_the_fallback() {
    let tier = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "tier_limit_exceeded",
                "message": "[code=tier_limit_exceeded] membership rpm limit exceeded: 101/100 resets 2026-11-14T10:38:00Z",
                "type": "tier_limit_exceeded"
            }
        })))
        .mount(&tier)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-fallback")))
        .mount(&fallback)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.tier]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.fallback]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "tier"
fallback = ["fallback"]
"#;
    let gw = TestGateway::start_with_fallback(config, &tier.uri(), &fallback.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("some-model", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the tier-limit 400 must walk the chain, not end the turn"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "from-fallback");

    let tier_reqs = tier.received_requests().await.unwrap_or_default();
    let fb_reqs = fallback.received_requests().await.unwrap_or_default();
    assert_eq!(tier_reqs.len(), 1, "one attempt on the walled provider");
    assert_eq!(fb_reqs.len(), 1, "the walk reached the fallback");
}

/// A 400 that is a genuine request fault must reach the client exactly as
/// before — the tier-limit deferral must not change its shape, status or
/// body.
#[tokio::test]
async fn test_ordinary_400_still_passes_through() {
    let strict = MockServer::start().await;

    let rejection = json!({
        "error": {"code": "invalid_request_error", "message": "Invalid request: missing 'messages' parameter"}
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(rejection.clone()))
        .mount(&strict)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.strict]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "strict"
"#;
    let gw = TestGateway::start(config, &strict.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .json(&chat_body("some-model", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "a request fault stays the client's answer");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"]["message"],
        rejection["error"]["message"],
        "the body must reach the client untouched"
    );
}

// ---------------------------------------------------------------------------
// Large bodies and failover (pingora's 64 KiB replay buffer)
// ---------------------------------------------------------------------------

/// Accept, read only the request head, answer 404 and close — the shape of an
/// upstream that rejects the model before reading the body it was given.
fn spawn_early_404_upstream() -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind early-404 listener");
    let addr = listener.local_addr().expect("local addr");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read only up to the end of the request head.
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
            );
            let _ = stream.flush();
            // Drop without reading the remaining body.
        }
    });
    format!("http://{addr}")
}

const LARGE_BODY_FAILOVER_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.early_404]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.alive]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "early_404"
fallback = ["alive"]
"#;

/// A real codex request is ~100 KB (instructions + tool schemas). Pingora keeps
/// only 64 KiB of the downstream body for replay, so a failover after a large
/// body must not deadlock: either the retry is skipped (fast error) or the
/// replayed body is whole. It must never leave the client waiting forever.
#[tokio::test]
async fn test_large_body_fails_over_with_the_whole_body() {
    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let dead_url = spawn_early_404_upstream();
    let gw =
        TestGateway::start_with_fallback(LARGE_BODY_FAILOVER_CONFIG, &dead_url, &alive.uri()).await;

    let filler = "x".repeat(100_000);
    let body = json!({
        "model": "gpt-4o",
        "input": [{"role": "user", "content": filler}],
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        client()
            .post(gw.url("/v1/chat/completions"))
            .json(&body)
            .send(),
    )
    .await
    .expect("a large-body failover must not hang the client")
    .expect("request completes");

    assert_eq!(
        outcome.status(),
        200,
        "the failover must replay the whole body, not a 64 KiB prefix (body: {})",
        outcome.text().await.unwrap_or_default()
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the fallback is tried once");
    let sent: Value = serde_json::from_slice(&served[0].body).expect("json body");
    let content = sent["input"][0]["content"].as_str().unwrap_or_default();
    assert_eq!(
        content.len(),
        100_000,
        "the replayed body must be complete, got {} bytes",
        content.len()
    );
}

/// Connect-refused primary, healthy fallback, 100 KB body: pingora replays the
/// request from its 64 KiB body buffer, so this must not deadlock either.
#[tokio::test]
async fn test_large_body_survives_a_connect_failure() {
    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    // 127.0.0.1:1 — nothing listens, connect is refused immediately.
    let gw = TestGateway::start_with_fallback(
        LARGE_BODY_FAILOVER_CONFIG,
        "http://127.0.0.1:1",
        &alive.uri(),
    )
    .await;

    let filler = "x".repeat(100_000);
    let body = json!({
        "model": "gpt-4o",
        "input": [{"role": "user", "content": filler}],
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        client()
            .post(gw.url("/v1/chat/completions"))
            .json(&body)
            .send(),
    )
    .await
    .expect("a large-body connect failover must not hang")
    .expect("request completes");

    assert_eq!(
        outcome.status(),
        200,
        "the fallback must serve the request (body: {})",
        outcome.text().await.unwrap_or_default()
    );
}

/// A native Responses stream whose upstream dies before `response.completed`
/// must not be served as a clean 200: the client gets an explicit error event
/// and the exchange is booked as truncated.
#[tokio::test]
async fn test_truncated_native_responses_stream_is_booked_as_truncated() {
    let mock = MockServer::start().await;

    // Deliberately stops after a delta — no `response.completed`.
    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cut\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_cut\",\"output_index\":0,\"content_index\":0,\"delta\":\"hal\"}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .mount(&mock)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses/mock_opencode"
provider = "mock_opencode"
endpoint_type = "responses"
"#;
    let gw = TestGateway::start(config, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/mock_opencode"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "mock-model", "input": "hi", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();

    assert!(
        text.contains("event: error"),
        "a truncated stream must end with an explicit error event:\n{text}"
    );
    assert!(
        text.contains("gateway_stream_truncated"),
        "the error event must name the cause:\n{text}"
    );
    assert!(
        text.contains("\"delta\":\"hal\""),
        "the partial answer the client already received must be preserved:\n{text}"
    );

    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let truncated = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_stream_truncated_total"))
        .unwrap_or_else(|| panic!("truncation was not counted:\n{metrics}"));
    assert!(
        truncated.trim_end().ends_with(" 1"),
        "expected exactly one truncation, got: {truncated}"
    );
    let before_first_byte: Vec<&str> = metrics
        .lines()
        .filter(|l| l.starts_with("maxllm_stream_truncated_before_first_byte_total"))
        .collect();
    assert!(
        before_first_byte
            .iter()
            .all(|l| l.trim_end().ends_with(" 0")),
        "a delta had already reached the client, so this must not be a \
         before-first-byte truncation: {before_first_byte:?}"
    );
}

/// The healthy counterpart: a stream that carries `response.completed` must
/// never be flagged, or the truncation alert would fire on every good request.
#[tokio::test]
async fn test_complete_native_responses_stream_is_not_booked_as_truncated() {
    let mock = MockServer::start().await;

    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":3,\"total_tokens\":15}}}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .mount(&mock)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses/mock_opencode"
provider = "mock_opencode"
endpoint_type = "responses"
"#;
    let gw = TestGateway::start(config, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/mock_opencode"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({"model": "mock-model", "input": "hi", "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("event: response.completed"), "{text}");
    assert!(
        !text.contains("gateway_stream_truncated"),
        "a completed stream must not get a synthetic error event:\n{text}"
    );

    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let truncated_lines: Vec<&str> = metrics
        .lines()
        .filter(|l| l.starts_with("maxllm_stream_truncated_total"))
        .collect();
    assert!(
        truncated_lines.iter().all(|l| l.trim_end().ends_with(" 0")),
        "no truncation may be booked for a complete stream: {truncated_lines:?}"
    );
}

/// A provider that keeps answering 200 and then dropping the body mid-stream
/// must be demoted off the route: the plain failure counter never sees it,
/// because every response status was a success.
#[tokio::test]
async fn test_a_truncating_provider_is_demoted_and_traffic_moves_to_the_fallback() {
    let flaky = MockServer::start().await;
    let good = MockServer::start().await;

    let truncated = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"cut\"}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(truncated, "text/event-stream"),
        )
        .mount(&flaky)
        .await;

    let healthy = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/zen/go/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(healthy, "text/event-stream"),
        )
        .mount(&good)
        .await;

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.flaky]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"
max_fails = 3
fail_timeout_secs = 60

[providers.good]
kind = "opencode"
base_url = "{FALLBACK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
responses_path = "/zen/go/v1/responses"
native_responses = true
api_key = "fake-key"

[[routes]]
path = "/v1/responses/flaky"
provider = "flaky"
fallback = ["good"]
endpoint_type = "responses"
strategy = "fallback"
"#;
    let gw = TestGateway::start_with_fallback(config, &flaky.uri(), &good.uri()).await;
    let c = client();

    async fn ask(c: &Client, gw: &TestGateway) -> String {
        c.post(gw.url("/v1/responses/flaky"))
            .header("Authorization", "Bearer test-key-123")
            .json(&json!({"model": "m", "input": "hi", "stream": true}))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    // Four truncations is the demotion threshold; the ratio is 100% here, so
    // the circuit must be open by the time the last one is logged.
    for _ in 0..4 {
        let body = ask(&c, &gw).await;
        assert!(
            body.contains("gateway_stream_truncated"),
            "the flaky provider is still primary, so its streams are truncated:\n{body}"
        );
    }

    // The demotion is booked asynchronously by the logging hook — wait for it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = c
            .get(gw.url("/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if metrics
            .lines()
            .any(|l| l.starts_with("maxllm_circuit_breaker_stream_opens_total"))
        {
            assert!(
                metrics
                    .lines()
                    .filter(|l| l.starts_with("maxllm_circuit_breaker_stream_opens_total"))
                    .all(|l| l.trim_end().ends_with(" 1")),
                "the demotion must be counted exactly once:\n{metrics}"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the flaky provider was never demoted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The next request must skip the open circuit and land on the fallback.
    let body = ask(&c, &gw).await;
    assert!(
        body.contains("event: response.completed"),
        "traffic must have moved to the healthy provider:\n{body}"
    );
    assert!(
        !body.contains("gateway_stream_truncated"),
        "the healthy fallback must serve a complete stream:\n{body}"
    );

    let flaky_hits = flaky.received_requests().await.unwrap_or_default().len();
    let good_hits = good.received_requests().await.unwrap_or_default().len();
    assert_eq!(flaky_hits, 4, "the flaky provider must stop at 4 attempts");
    assert!(
        good_hits >= 1,
        "the fallback must serve the demoted traffic"
    );
}

// ---------------------------------------------------------------------------
// commandcode.ai ("provider") — native Responses, vendor paths, Messages wire
// ---------------------------------------------------------------------------

const COMMANDCODE_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.cmd]
kind = "commandcode"
base_url = "{MOCK_URL}"
api_key = "cmd-secret"
default_model = "deepseek/deepseek-v4.1-flash"

[[routes]]
path = "/v1/responses"
provider = "cmd"
endpoint_type = "responses"

[[routes]]
path = "/v1/chat/completions"
provider = "cmd"
"#;

const COMMANDCODE_MESSAGES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.cmd_claude]
kind = "commandcode"
base_url = "{MOCK_URL}"
upstream_path = "/provider/v1/messages"
api_key = "cmd-secret"
default_model = "claude-sonnet-5-5"

[[routes]]
path = "/v1/messages"
provider = "cmd_claude"
endpoint_type = "native"
"#;

#[tokio::test]
async fn test_commandcode_native_responses_passthrough() {
    let mock = MockServer::start().await;

    // commandcode.ai serves the Responses contract on /provider/v1/responses.
    // The request must land there byte-for-byte, authenticated with the
    // vendor's bearer token.
    let upstream_body = json!({
        "id": "resp_cmd",
        "object": "response",
        "status": "completed",
        "model": "deepseek/deepseek-v4.1-flash",
        "output": [{
            "id": "msg_cmd",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hi"}]
        }],
        "usage": {"input_tokens": 11, "output_tokens": 5, "total_tokens": 16}
    });

    Mock::given(method("POST"))
        .and(path("/provider/v1/responses"))
        .and(header("Authorization", "Bearer cmd-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(COMMANDCODE_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek/deepseek-v4.1-flash",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false,
        "previous_response_id": "resp_prev"
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "response");
    assert_eq!(body["id"], "resp_cmd");
    assert_eq!(body["output"][0]["content"][0]["text"], "hi");

    // Byte-for-byte passthrough to the vendor's Responses route.
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body, serde_json::to_vec(&request_body).unwrap());
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(
        sent.get("messages").is_none(),
        "no chat translation happened"
    );
}

#[tokio::test]
async fn test_commandcode_chat_completions_use_the_vendor_path() {
    let mock = MockServer::start().await;

    let upstream_body = json!({
        "id": "chatcmpl_cmd",
        "object": "chat.completion",
        "model": "deepseek/deepseek-v4.1-flash",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "hi"},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6}
    });

    Mock::given(method("POST"))
        .and(path("/provider/v1/chat/completions"))
        .and(header("Authorization", "Bearer cmd-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(COMMANDCODE_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek/deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "hi");

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["model"], "deepseek/deepseek-v4.1-flash");
}

#[tokio::test]
async fn test_commandcode_native_messages_rewrites_the_path() {
    let mock = MockServer::start().await;

    // The vendor publishes the Anthropic Messages contract at
    // /provider/v1/messages. A client that speaks the plain /v1/messages
    // contract must still reach it.
    let upstream_body = json!({
        "id": "msg_cmd",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-5-5",
        "content": [{"type": "text", "text": "hi"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 3, "output_tokens": 2}
    });

    Mock::given(method("POST"))
        .and(path("/provider/v1/messages"))
        .and(header("Authorization", "Bearer cmd-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(COMMANDCODE_MESSAGES_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/messages"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "claude-sonnet-5-5",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A native route returns the provider's own format untouched.
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["text"], "hi");

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/provider/v1/messages");
}

const OPEN_DESIGN_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.open_design]
kind = "open_design"
base_url = "{MOCK_URL}"
api_key = "sk-amr-secret"
default_model = "deepseek-v4.1-flash"

[[routes]]
path = "/v1/responses"
provider = "open_design"
endpoint_type = "responses"

[[routes]]
path = "/v1/chat/completions"
provider = "open_design"
"#;

#[tokio::test]
async fn test_open_design_native_responses_passthrough() {
    let mock = MockServer::start().await;

    // The OpenDesign AMR link serves the Responses contract on
    // /v1/responses. The request must land there byte-for-byte, with the
    // link's bearer token.
    let upstream_body = json!({
        "id": "resp_od",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [{
            "id": "msg_od",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hi"}]
        }],
        "usage": {"input_tokens": 11, "output_tokens": 5, "total_tokens": 16}
    });

    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .and(header("Authorization", "Bearer sk-amr-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPEN_DESIGN_CONFIG, &mock.uri()).await;
    let c = client();

    let request_body = json!({
        "model": "deepseek-v4.1-flash",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false,
        "previous_response_id": "resp_prev"
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "response");
    assert_eq!(body["id"], "resp_od");
    assert_eq!(body["output"][0]["content"][0]["text"], "hi");

    // Byte-for-byte passthrough to the link's own Responses route.
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/v1/responses");
    assert_eq!(requests[0].body, serde_json::to_vec(&request_body).unwrap());
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(
        sent.get("messages").is_none(),
        "no chat translation happened"
    );
}

#[tokio::test]
async fn test_open_design_native_responses_rewrites_tool_choice_any() {
    let mock = MockServer::start().await;

    // The link rejects the non-standard `tool_choice: "any"` (sent by
    // codex-cli) with HTTP 400 `upstream_error` — on its Responses route
    // just like on chat. The native path must repair the body before it is
    // forwarded, or every codex-cli tool call fails.
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(|req: &wiremock::Request| {
            let sent: Value = serde_json::from_slice(&req.body).expect("json body");
            assert_eq!(
                sent["tool_choice"], "required",
                "tool_choice \"any\" must be rewritten to \"required\": {sent}"
            );
            // The rest of the body survives the repair.
            assert_eq!(sent["model"], "deepseek-v4.1-flash");
            assert_eq!(
                sent["input"],
                json!([{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}])
            );
            ResponseTemplate::new(200).set_body_json(json!({
                "id": "resp_od",
                "object": "response",
                "status": "completed",
                "model": "deepseek-v4.1-flash",
                "output": [{
                    "id": "msg_od",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "hi"}]
                }],
                "usage": {"input_tokens": 11, "output_tokens": 5, "total_tokens": 16}
            }))
        })
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPEN_DESIGN_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "tool_choice": "any"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/v1/responses");
}

#[tokio::test]
async fn test_open_design_native_responses_streams_and_accounts_usage() {
    let mock = MockServer::start().await;

    // The OpenDesign link streams the Responses SSE event family. Its
    // native route travels chunked (the gateway may repair the body, so it
    // cannot keep the client's Content-Length) — the stream must still be
    // copied verbatim and its usage accounted for.
    let sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_s\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":11,\"output_tokens\":5}}}\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPEN_DESIGN_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("event: response.completed"), "{text}");
    assert!(text.contains("response.output_text.delta"), "{text}");

    // The usage embedded in the final event must be accounted for.
    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let line = metrics
        .lines()
        .find(|l| l.starts_with("maxllm_tokens_in_total") && l.contains("deepseek-v4.1-flash"))
        .unwrap_or_else(|| panic!("no tokens_in metric for the model:\n{metrics}"));
    assert!(
        line.trim_end().ends_with("11"),
        "expected 11 input tokens, got: {line}"
    );
}

#[tokio::test]
async fn test_open_design_chat_completions_use_the_link_path() {
    let mock = MockServer::start().await;

    let upstream_body = json!({
        "id": "chatcmpl_od",
        "object": "chat.completion",
        "model": "deepseek-v4.1-flash",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "hi",
                "reasoning_details": [{
                    "index": 0,
                    "type": "reasoning.summary",
                    "summary": "thinking"
                }]
            },
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6}
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("Authorization", "Bearer sk-amr-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body.clone()))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPEN_DESIGN_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "hi");
    // The link's reasoning_details array reaches the client untouched.
    assert_eq!(
        body["choices"][0]["message"]["reasoning_details"][0]["summary"],
        "thinking"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["model"], "deepseek-v4.1-flash");
}

#[tokio::test]
async fn test_open_design_rewrites_tool_choice_any() {
    let mock = MockServer::start().await;

    // The link rejects the non-standard `tool_choice: "any"` with HTTP 400
    // `upstream_error`; the gateway rewrites it to "required".
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl_od",
            "object": "chat.completion",
            "model": "deepseek-v4.1-flash",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hi"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(OPEN_DESIGN_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&json!({
            "model": "deepseek-v4.1-flash",
            "messages": [{"role": "user", "content": "hi"}],
            "tool_choice": "any"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["tool_choice"], "required");
}

#[tokio::test]
async fn test_commandcode_upstream_script_ua_replaced() {
    let mock = MockServer::start().await;

    // commandcode.ai's Cloudflare edge bans script fingerprints
    // (`Python-urllib/3.11` -> HTTP 403 "error code: 1010"), the same
    // fingerprint opencode.ai bans. The gateway must swap it for the safe UA.
    Mock::given(method("POST"))
        .and(path("/provider/v1/chat/completions"))
        .and(header("User-Agent", "curl/8.5.0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id":"c1","object":"chat.completion","choices":[]})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(COMMANDCODE_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .header("User-Agent", "Python-urllib/3.11")
        .json(&chat_body("deepseek/deepseek-v4.1-flash", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// A client that hangs up mid-stream must be counted as a *client* abort, not
/// as an upstream truncation and not as a gateway failure.
///
/// The 2026-11-08 audit read the 816 `Downstream ConnectionClosed … Prematurely
/// before response body is complete` rows as upstream truncations and
/// concluded 11.2% of all 200s were silently cut. pingora labels those with
/// `ErrorSource::Downstream`, i.e. the remote client hung up; only one row in
/// four days was a real upstream truncation.
///
/// The upstream here is a raw socket that answers with SSE headers plus one
/// chunk and then holds the connection open, so the gateway is genuinely
/// mid-body when the client walks away — exactly the shape that produced those
/// rows.
#[tokio::test]
async fn test_client_abort_is_counted_apart_from_truncation() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut head = [0u8; 4096];
                let _ = sock.read(&mut head).await;
                let header = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
                if sock.write_all(header.as_bytes()).await.is_err() {
                    return;
                }
                let chunk = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_abort\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n";
                let framed = format!("{:x}\r\n{}\r\n", chunk.len(), chunk);
                if sock.write_all(framed.as_bytes()).await.is_err() {
                    return;
                }
                // Never finish the body: the gateway stays mid-stream, so the
                // only way this exchange ends is the client closing its side.
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
        }
    });

    let config = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_opencode]
kind = "opencode"
base_url = "{MOCK_URL}"
upstream_path = "/zen/go/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses/mock_opencode"
provider = "mock_opencode"
endpoint_type = "responses"
"#;
    let gw = TestGateway::start(config, &upstream).await;
    let c = client();

    // Raw TCP client: read the response head, then drop the socket. reqwest
    // cannot express "abandon the body" without the stream feature, and this
    // also makes the abort unconditional.
    let body = r#"{"model":"mock-model","input":"hi","stream":true}"#;
    let request = format!(
        "POST /v1/responses/mock_opencode HTTP/1.1\r\n\
         Host: 127.0.0.1\r\n\
         Authorization: Bearer test-key-123\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", gw.port))
        .await
        .unwrap();
    sock.write_all(request.as_bytes()).await.unwrap();
    let mut buf = [0u8; 1024];
    let read = sock.read(&mut buf).await.unwrap();
    assert!(read > 0, "the gateway must start answering before we abort");
    drop(sock);

    let mut metric = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics = c
            .get(gw.url("/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if let Some(line) = metrics
            .lines()
            .find(|l| l.starts_with("maxllm_client_aborted_total{provider=\"mock_opencode\"}"))
        {
            metric = line.to_string();
            break;
        }
    }
    assert!(
        metric.trim_end().ends_with(" 1"),
        "the abandoned stream must be counted once as a client abort, got: {metric:?}"
    );
    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !metrics.contains("maxllm_stream_truncated_total{provider=\"mock_opencode\"}"),
        "a client that walks away is not an upstream truncation:\n{metrics}"
    );
}

// ---------------------------------------------------------------------------
// Phantom request-log rows
// ---------------------------------------------------------------------------

const UNROUTED_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[admin]
master_key = "test-master-key"
enabled = true

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;

/// Send `path` and drop the socket with `SO_LINGER=0`, i.e. reset it while the
/// gateway is answering. That is what a monitor (or a browser) does when it
/// hangs up in the middle of a scrape, and it is how the phantom
/// `request_logs` rows were produced in production.
async fn reset_after_request(port: u16, path: &str) {
    use std::io::Write as _;
    let sock = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .expect("socket");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    sock.connect(&addr.into()).expect("connect to gateway");
    (&sock)
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
        .expect("write request");
    // Zero linger = close with RST. Dropping the socket then aborts the
    // connection instead of closing it, so the gateway's response write fails
    // the way it does when a monitor hangs up mid-scrape.
    sock.set_linger(Some(Duration::ZERO))
        .expect("set SO_LINGER");
    drop(sock);
    // Give the gateway a moment to notice the reset.
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn aborted_fast_path_probe_leaves_no_request_log() {
    let mock = MockServer::start().await;
    openai_mock(&mock).await;
    let gw = TestGateway::start(UNROUTED_CONFIG, &mock.uri()).await;
    let c = Client::new();

    let logs = |gw: &TestGateway| {
        let c = c.clone();
        let url = gw.url("/admin/logs");
        async move {
            let v: Value = c
                .get(url)
                .bearer_auth("test-master-key")
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v.as_array().cloned().unwrap_or_default()
        }
    };

    // The readiness poll in the harness hits /health, which is a fast path and
    // must not be booked either.
    assert!(
        logs(&gw).await.is_empty(),
        "a served /health must not be written to the request journal"
    );

    // A fast path whose client resets mid-write: the write fails inside
    // `request_filter`, which used to escape with `skip_logging` still false
    // and book a row with an empty provider and a status of 0.
    for path in ["/health", "/metrics", "/v1/models", "/version"] {
        reset_after_request(gw.port, path).await;
    }
    let after = logs(&gw).await;
    assert!(
        after.is_empty(),
        "aborted fast-path probes must not appear in the request journal, got {after:#?}"
    );

    // A real request still books exactly one row: the guard must not suppress
    // the journal.
    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let rows = logs(&gw).await;
    assert_eq!(rows.len(), 1, "exactly one real request: {rows:#?}");
    assert_eq!(rows[0]["provider"], "mock_openai");
    assert_eq!(rows[0]["status"], 200);

    // The unrouted sessions are counted, and the gauge they no longer
    // decrement stays at zero instead of drifting negative.
    let metrics = c
        .get(gw.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let counter = |name: &str| -> f64 {
        metrics
            .lines()
            .find(|l| l.starts_with(&format!("{name} ")))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("metric {name} missing from:\n{metrics}"))
    };
    // The counter only fires for sessions that die *before* the fast-path flag
    // is set (a reset that races the request line, a TLS/HTTP parse failure),
    // so its value here is timing-dependent. Assert it is registered and
    // parseable rather than pinning a number.
    let _ = counter("maxllm_unrouted_sessions_total");
    assert_eq!(
        counter("maxllm_active_requests"),
        0.0,
        "an aborted fast-path probe must not push the gauge negative:\n{metrics}"
    );
}

// ---------------------------------------------------------------------------
// `/v1/responses/<provider>` pins the named provider (2026-11-08)
// ---------------------------------------------------------------------------

/// A pooled `/v1/responses` route plus a provider that is *only* reachable
/// by naming it in the path.
///
/// `side_channel` deliberately has no `[[routes]]` entry of its own: it is
/// the case that used to be served by the pool, because the pooled route
/// matched the path by prefix and the name in the path was validated and then
/// ignored.
const PATH_PINNED_PROVIDER_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.pool_head]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[providers.pool_tail]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[providers.side_channel]
kind = "openai"
base_url = "{SIDE_URL}"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses"
provider = "pool_head"
fallback = ["pool_tail"]
strategy = "fallback"
endpoint_type = "responses"
"#;

fn responses_body(text: &str) -> Value {
    json!({
        "id": "resp_test",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4.1-flash",
        "output": [{
            "id": "msg_1", "type": "message", "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": text}]
        }],
        "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}
    })
}

fn responses_request(text: &str) -> Value {
    json!({
        "model": "deepseek-v4.1-flash",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": text}]}]
    })
}

#[tokio::test]
async fn test_responses_path_provider_name_pins_that_provider() {
    let head = MockServer::start().await;
    let tail = MockServer::start().await;
    let side = MockServer::start().await;

    // The pool must not be used at all for a pinned path.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from head")))
        .expect(0)
        .mount(&head)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from tail")))
        .expect(0)
        .mount(&tail)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from side")))
        .expect(1)
        .mount(&side)
        .await;

    let config = PATH_PINNED_PROVIDER_CONFIG.replace("{SIDE_URL}", &side.uri());
    let gw = TestGateway::start_with_fallback(&config, &head.uri(), &tail.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/side_channel"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["output"][0]["content"][0]["text"], "from side",
        "the provider named in the path must serve the request, not the pool"
    );
}

#[tokio::test]
async fn test_responses_path_provider_name_does_not_fall_back_to_the_pool() {
    let head = MockServer::start().await;
    let tail = MockServer::start().await;
    let side = MockServer::start().await;

    // The pinned provider fails; the pool must stay untouched.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from head")))
        .expect(0)
        .mount(&head)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from tail")))
        .expect(0)
        .mount(&tail)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "down"})))
        .expect(1)
        .mount(&side)
        .await;

    let config = PATH_PINNED_PROVIDER_CONFIG.replace("{SIDE_URL}", &side.uri());
    let gw = TestGateway::start_with_fallback(&config, &head.uri(), &tail.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/side_channel"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request("hi"))
        .send()
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "a pinned provider that fails must not be papered over by the pool: {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_responses_path_unknown_provider_is_rejected() {
    let head = MockServer::start().await;
    let side = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(responses_body("from head")))
        .expect(0)
        .mount(&head)
        .await;

    let config = PATH_PINNED_PROVIDER_CONFIG.replace("{SIDE_URL}", &side.uri());
    let gw = TestGateway::start_with_fallback(&config, &head.uri(), &head.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/no_such_provider"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("unknown provider or pool 'no_such_provider'"),
        "the 404 must name the unknown segment: {body}"
    );
}

// ---------------------------------------------------------------------------
// Quota exhaustion (429) and the failover it must trigger
// ---------------------------------------------------------------------------

/// Zhipu's weekly/monthly quota error: no rate-limit headers at all (the
/// reset instant is only in the body, in Beijing wall clock).
/// Zhipu's quota-wall body, with the reset instant an hour ahead.
///
/// The instant is computed at run time on purpose: a hard-coded one rots
/// into the past, and a reset instant in the past is not a parking decision
/// (the breaker ignores it), so the member would stay in the rotation and
/// the test would fail on a date rather than on a behaviour.
fn quota_429_body() -> String {
    quota_429_body_resetting_at(chrono::Utc::now() + chrono::Duration::hours(1))
}

/// The Zhipu quota-wall body for a given reset instant, in the provider's
/// own zone (UTC+8 — the body carries no zone marker).
fn quota_429_body_resetting_at(reset: chrono::DateTime<chrono::Utc>) -> String {
    let beijing = reset + chrono::Duration::hours(8);
    format!(
        r#"{{"error":{{"code":"1310","message":"您已达到每周/每月使用上限，您的限额将在 {} 重置。"}}}}"#,
        beijing.format("%Y-%m-%d %H:%M:%S")
    )
}

const QUOTA_429_FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.quota_dead]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[providers.alive]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses"
provider = "quota_dead"
fallback = ["alive"]
strategy = "fallback"
endpoint_type = "responses"
max_failover_attempts = 3
"#;

/// A provider whose quota is exhausted answers 429 and must not leak that
/// error to the client: the request has to fail over to the next provider.
#[tokio::test]
async fn test_quota_429_fails_over_to_the_next_provider() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let gw = TestGateway::start_with_fallback(QUOTA_429_FALLBACK_CONFIG, &dead.uri(), &alive.uri())
        .await;

    let body = json!({"model": "gpt-4o", "input": "hi"});
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "a quota 429 must fail over, not reach the client (body: {text})"
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the fallback is tried once");
}

/// The same, with the request size a long codex session actually produces
/// (~200k tokens of context): the body no longer fits pingora's 64 KiB
/// replay buffer, so the failover must still happen — the client must never
/// see the upstream's quota error.
#[tokio::test]
async fn test_quota_429_fails_over_with_a_large_body() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let gw = TestGateway::start_with_fallback(QUOTA_429_FALLBACK_CONFIG, &dead.uri(), &alive.uri())
        .await;

    let filler = "x".repeat(700_000);
    let body = json!({
        "model": "gpt-4o",
        "input": [{"role": "user", "content": filler}],
    });

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        client()
            .post(gw.url("/v1/responses"))
            .header("Authorization", "Bearer test-key-123")
            .json(&body)
            .send(),
    )
    .await
    .expect("a large-body failover must not hang the client")
    .expect("request completes");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "a quota 429 must fail over even with a large body (body: {text})"
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the fallback is tried once");
    let sent: Value = serde_json::from_slice(&served[0].body).expect("json body");
    let content = sent["input"][0]["content"].as_str().unwrap_or_default();
    assert_eq!(
        content.len(),
        700_000,
        "the replayed body must be complete, got {} bytes",
        content.len()
    );
}

/// A half-open member that answers a *deterministic* rejection must give its
/// probe slot back when the request fails over.
///
/// The slot belongs to the request, not to the provider: a failure the breaker
/// counts (5xx, connect error, 429) takes the slot back by itself, but a
/// deterministic rejection (401/403/404/425) fails over without booking any
/// outcome — so the slot stayed busy and the member was unusable. A
/// half-open provider whose probe slot is busy answers "circuit open" to
/// every later request until the staleness reaper takes the slot (two fail
/// windows), which takes a *healthy* member out of the rotation.
const PROBE_SLOT_FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.primary]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
max_fails = 1
fail_timeout_secs = 1

[providers.backup]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses"
provider = "primary"
fallback = ["backup"]
strategy = "fallback"
endpoint_type = "responses"
max_failover_attempts = 3
"#;

#[tokio::test]
async fn test_a_deterministic_rejection_returns_the_half_open_probe_slot() {
    let primary = MockServer::start().await;
    // 1) the trip: one 5xx opens the circuit (max_fails = 1).
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&primary)
        .await;
    // 2) the probe: a deterministic rejection. It fails over, but it books
    //    no breaker outcome — only the request can return the slot.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_string(r#"{"error":{"message":"invalid api key"}}"#)
                .insert_header("content-type", "application/json"),
        )
        .up_to_n_times(1)
        .mount(&primary)
        .await;
    // 3) the next probe must reach the member again.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-primary")))
        .mount(&primary)
        .await;

    let backup = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-backup")))
        .mount(&backup)
        .await;

    let gw =
        TestGateway::start_with_fallback(PROBE_SLOT_FALLBACK_CONFIG, &primary.uri(), &backup.uri())
            .await;

    let body = json!({"model": "gpt-4o", "input": "ping"});

    // 1) trip the breaker: the request is served by the backup.
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        200,
        "the backup must serve the tripped member"
    );

    // The fail window has to elapse: until then the member is open, and only
    // after it the next request probes it.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    // 2) the probe is rejected deterministically and the request fails over.
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        200,
        "the backup must serve the rejected probe"
    );

    // 3) the member is up and its slot is free again: the next request must
    //    probe it, not skip it for a slot it no longer holds.
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap_or_default();
    assert!(
        text.contains("from-primary"),
        "the half-open member must be probeable again right after a failover: {text}"
    );
    let served = primary.received_requests().await.unwrap_or_default();
    assert_eq!(
        served.len(),
        3,
        "the member must have served the probe and the request after it"
    );
}

/// The same, on a provider that does NOT speak the Responses contract
/// natively: the body is translated, so the gateway forwards it with its
/// original framing. The failover must still happen — the client's context
/// is the expensive part, and a quota wall must not cost the session.
const QUOTA_429_LARGE_BODY_TRANSLATED_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.quota_dead]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"

[providers.alive]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "quota_dead"
fallback = ["alive"]
strategy = "fallback"
endpoint_type = "responses"
max_failover_attempts = 3
"#;

/// A quota wall on a translated provider must still rotate, with the whole
/// body: the client's context is the expensive part of the request, and a
/// provider that ran out of quota must not cost the session.
#[tokio::test]
async fn test_quota_429_fails_over_with_a_large_body_on_a_translated_provider() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let gw = TestGateway::start_with_fallback(
        QUOTA_429_LARGE_BODY_TRANSLATED_CONFIG,
        &dead.uri(),
        &alive.uri(),
    )
    .await;

    let filler = "x".repeat(700_000);
    let body = json!({
        "model": "gpt-4o",
        "input": [{"role": "user", "content": filler}],
    });

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        client()
            .post(gw.url("/v1/responses"))
            .header("Authorization", "Bearer test-key-123")
            .json(&body)
            .send(),
    )
    .await
    .expect("a large-body failover must not hang the client")
    .expect("request completes");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "a quota 429 must fail over even with a large body (body: {text})"
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the fallback is tried once");
    let sent: Value = serde_json::from_slice(&served[0].body).expect("json body");
    let content = sent["messages"]
        .as_array()
        .and_then(|m| m.last())
        .and_then(|m| m["content"].as_str())
        .unwrap_or_default();
    assert_eq!(
        content.len(),
        700_000,
        "the replayed body must be complete, got {} bytes",
        content.len()
    );
}

const QUOTA_429_AUTO_POOL_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.quota_dead]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[providers.alive]
kind = "openai_compat"
base_url = "{POOL_A_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "quota_dead"
model = "model-a"

[[auto_model.pool]]
provider = "alive"
model = "model-b"

[[routes]]
path = "/v1/responses/auto"
provider = "auto"
auto_model = true
session_sticky = true
"#;

const QUOTA_429_AUTO_POOL_SINGLE_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.dead]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[auto_model.pool]]
provider = "dead"
model = "model-a"

[[routes]]
path = "/v1/responses/auto"
provider = "auto"
auto_model = true
"#;

/// The auto pool: a quota-exhausted member must not leak its 429 to the
/// client — the request has to rotate to the next member of the pool.
#[tokio::test]
async fn test_quota_429_in_the_auto_pool_rotates_to_the_next_member() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let dead_uri = dead.uri();
    let alive_uri = alive.uri();
    let (mut child, base, _cfg) = spawn_gateway_with_pool(
        QUOTA_429_AUTO_POOL_CONFIG,
        [dead_uri.as_str(), alive_uri.as_str(), alive_uri.as_str()],
    )
    .await;
    let c = client();

    let body = json!({"model": "auto", "input": "hi"});
    let resp = c
        .post(format!("{base}/v1/responses/auto"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let _ = child.kill();
    assert_eq!(
        status, 200,
        "a quota 429 in the pool must rotate, not reach the client (body: {text})"
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the next pool member is tried once");
}

/// Same, with the body size a long codex session produces.
#[tokio::test]
async fn test_quota_429_in_the_auto_pool_rotates_with_a_large_body() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let alive = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from-alive")))
        .mount(&alive)
        .await;

    let dead_uri = dead.uri();
    let alive_uri = alive.uri();
    let (mut child, base, _cfg) = spawn_gateway_with_pool(
        QUOTA_429_AUTO_POOL_CONFIG,
        [dead_uri.as_str(), alive_uri.as_str(), alive_uri.as_str()],
    )
    .await;
    let c = client();

    let filler = "x".repeat(700_000);
    let body = json!({"model": "auto", "input": [{"role": "user", "content": filler}]});
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        c.post(format!("{base}/v1/responses/auto"))
            .json(&body)
            .send(),
    )
    .await
    .expect("a large-body failover must not hang the client")
    .expect("request completes");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let _ = child.kill();
    assert_eq!(
        status, 200,
        "a quota 429 in the pool must rotate even with a large body (body: {text})"
    );
    let served = alive.received_requests().await.unwrap_or_default();
    assert_eq!(served.len(), 1, "the next pool member is tried once");
    let sent: Value = serde_json::from_slice(&served[0].body).expect("json body");
    let content = sent["input"][0]["content"].as_str().unwrap_or_default();
    assert_eq!(
        content.len(),
        700_000,
        "the replayed body must be complete, got {} bytes",
        content.len()
    );
}

/// A route with no fallback, and the layered rate-limit resolver enabled —
/// the production shape of the incident: the 429's reset instant is only in
/// the body, so the decision is deferred to the body filter.
const QUOTA_429_RESOLVER_NO_FALLBACK_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[rate_limit_learning]
enabled = true
laya_url = "{LAYA_URL}"

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.quota_dead]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
utc_offset = "+08:00"

[[routes]]
path = "/v1/responses"
provider = "quota_dead"
strategy = "fallback"
endpoint_type = "responses"
max_failover_attempts = 3
"#;

// ---------------------------------------------------------------------------
// The gateway owns the error: no upstream error reaches the client
// ---------------------------------------------------------------------------

/// A quota wall with no fallback: the client gets the gateway's own 503,
/// never the upstream's quota message.
#[tokio::test]
async fn test_quota_429_without_a_fallback_is_answered_by_the_gateway() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let gw = TestGateway::start_with_fallback(
        QUOTA_429_RESOLVER_NO_FALLBACK_CONFIG,
        &dead.uri(),
        &dead.uri(),
    )
    .await;

    let body = json!({"model": "gpt-4o", "input": "hi"});
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");

    let status = resp.status();
    let upstream_status = resp
        .headers()
        .get("x-maxllm-upstream-status")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    let text = resp.text().await.unwrap_or_default();

    assert_eq!(
        status, 503,
        "a quota wall with no fallback must become the gateway's own 503 (body: {text})"
    );
    assert!(
        !text.contains("您已达到每周/每月使用上限"),
        "the upstream's quota message must not reach the client: {text}"
    );
    let parsed: Value = serde_json::from_str(&text).expect("the answer is JSON");
    assert_eq!(parsed["error"]["type"], "gateway_error");
    assert_eq!(parsed["error"]["code"], 503);
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no provider available"),
        "the message names the situation: {text}"
    );
    assert_eq!(
        upstream_status.as_deref(),
        Some("429"),
        "the upstream's own status travels with the answer"
    );
}

/// The production shape: an auto pool whose only member is quota-dead. The
/// client must get the gateway's own error — not the upstream's quota text.
#[tokio::test]
async fn test_quota_429_in_the_auto_pool_without_a_healthy_member_is_answered_by_the_gateway() {
    let dead = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&dead)
        .await;

    let dead_uri = dead.uri();
    let (mut child, base, _cfg) = spawn_gateway_with_pool(
        QUOTA_429_AUTO_POOL_SINGLE_CONFIG,
        [dead_uri.as_str(), dead_uri.as_str(), dead_uri.as_str()],
    )
    .await;

    let body = json!({"model": "auto", "input": "hi"});
    let resp = client()
        .post(format!("{base}/v1/responses/auto"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let _ = child.kill();

    assert_eq!(
        status, 503,
        "a dead auto pool must become the gateway's own 503 (body: {text})"
    );
    assert!(
        !text.contains("您已达到每周/每月使用上限"),
        "the upstream's quota message must not reach the client: {text}"
    );
    let parsed: Value = serde_json::from_str(&text).expect("the answer is JSON");
    assert_eq!(parsed["error"]["type"], "gateway_error");
}

/// A client's own mistake keeps the upstream's answer: a 404 that names the
/// model as unknown is the client's answer to hear, not the gateway's.
#[tokio::test]
async fn test_a_client_fault_keeps_the_upstreams_answer() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_string(r#"{"error":{"message":"The model `x` does not exist","type":"invalid_request_error"}}"#)
                .insert_header("content-type", "application/json"),
        )
        .mount(&mock)
        .await;

    let gw = TestGateway::start_with_fallback(
        QUOTA_429_RESOLVER_NO_FALLBACK_CONFIG,
        &mock.uri(),
        &mock.uri(),
    )
    .await;

    let body = json!({"model": "gpt-4o", "input": "hi"});
    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .expect("request completes");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    assert_eq!(
        status, 404,
        "a request fault stays the upstream's answer (body: {text})"
    );
    assert!(
        text.contains("does not exist"),
        "the upstream's diagnosis survives: {text}"
    );
}

// ---------------------------------------------------------------------------
// Cross-node breaker sharing, operator parking, and the breaker endpoints
// ---------------------------------------------------------------------------

/// Spawn a gateway on a fixed port (the cross-node test needs to know both
/// gateways' addresses before either of them starts).
async fn spawn_gateway_on(config: &str, port: u16) -> (Child, tempfile::NamedTempFile) {
    let config_str = config.replace("{PORT}", &port.to_string());
    let mut config_file = tempfile::NamedTempFile::new().expect("temp file");
    config_file
        .write_all(config_str.as_bytes())
        .expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_maxllm-server"))
        .arg("--config")
        .arg(config_file.path())
        .env("RUST_LOG", "warn")
        .env_remove("HTTPS_PROXY")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("http_proxy")
        .spawn()
        .expect("spawn gateway");
    let base = format!("http://127.0.0.1:{port}");
    let c = client();
    for _ in 0..60 {
        if c.get(format!("{base}/health"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (child, config_file)
}

/// A quota wall learned on one node must be adopted by its peer: the peer
/// must not spend a request discovering the same wall.
#[tokio::test]
async fn test_a_quota_wall_learned_on_one_node_is_adopted_by_the_peer() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&mock)
        .await;

    let port_a = portpicker::pick_unused_port().expect("free port");
    let port_b = portpicker::pick_unused_port().expect("free port");

    let config = |port: u16, peer: u16| {
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"
threads = 1

[rate_limit_learning]
enabled = true

[admin]
master_key = "test-master-key"
enabled = true

[breaker_sync]
enabled = true
interval_secs = 1
peers = ["http://127.0.0.1:{peer}"]
auth_token = "test-master-key"

[providers.dead]
kind = "openai_compat"
base_url = "{mock}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
utc_offset = "+08:00"

[[routes]]
path = "/v1/responses"
provider = "dead"
strategy = "fallback"
endpoint_type = "responses"
"#,
            port = port,
            peer = peer,
            mock = mock.uri()
        )
    };

    let (mut child_a, _cfg_a) = spawn_gateway_on(&config(port_a, port_b), port_a).await;
    let (mut child_b, _cfg_b) = spawn_gateway_on(&config(port_b, port_a), port_b).await;

    // Node A learns the wall: the request is answered by the gateway itself
    // and the provider is parked.
    let body = json!({"model": "gpt-4o", "input": "hi"});
    let resp = client()
        .post(format!("http://127.0.0.1:{port_a}/v1/responses"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        503,
        "the quota wall becomes the gateway's 503"
    );
    let calls_after_a = mock.received_requests().await.unwrap_or_default().len();
    assert_eq!(calls_after_a, 1, "node A asked the provider once");

    // Node B adopts the wall from A (the poll runs every second).
    let mut adopted = false;
    for _ in 0..40 {
        let report: Value = client()
            .get(format!("http://127.0.0.1:{port_b}/admin/breakers"))
            .bearer_auth("test-master-key")
            .send()
            .await
            .expect("the breaker report answers")
            .json()
            .await
            .expect("the report is JSON");
        let until = report["breakers"]
            .as_array()
            .and_then(|b| {
                b.iter()
                    .find(|e| e["provider"] == "dead")
                    .and_then(|e| e["force_open_until"].as_u64())
            })
            .unwrap_or(0);
        if until > 0 {
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(adopted, "node B must adopt node A's quota force-open");

    // …and must not ask the provider again.
    let resp = client()
        .post(format!("http://127.0.0.1:{port_b}/v1/responses"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        503,
        "the peer answers with the gateway's own error"
    );
    let calls_after_b = mock.received_requests().await.unwrap_or_default().len();
    assert_eq!(
        calls_after_b, calls_after_a,
        "the peer must not spend a request on a provider it knows is quota-dead"
    );

    let _ = child_a.kill();
    let _ = child_b.kill();
}

/// A provider that keeps failing on one node is dead for the cluster, not
/// for that node alone.
///
/// The node that never sent traffic to it cannot know that: it keeps
/// picking the member and keeps paying a failed attempt per request. The
/// peer that saw the failures tells it, and the second node stops asking.
#[tokio::test]
async fn test_a_failure_learned_on_one_node_is_adopted_by_the_peer() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream is down"))
        .mount(&mock)
        .await;

    let port_a = portpicker::pick_unused_port().expect("free port");
    let port_b = portpicker::pick_unused_port().expect("free port");

    let config = |port: u16, peer: u16| {
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"
threads = 1

[admin]
master_key = "test-master-key"
enabled = true

[breaker_sync]
enabled = true
interval_secs = 1
peers = ["http://127.0.0.1:{peer}"]
auth_token = "test-master-key"

[providers.dead]
kind = "openai_compat"
base_url = "{mock}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
utc_offset = "+08:00"
max_fails = 3
fail_timeout_secs = 60

[[routes]]
path = "/v1/responses"
provider = "dead"
strategy = "fallback"
endpoint_type = "responses"
"#,
            port = port,
            peer = peer,
            mock = mock.uri()
        )
    };

    let (mut child_a, _cfg_a) = spawn_gateway_on(&config(port_a, port_b), port_a).await;
    let (mut child_b, _cfg_b) = spawn_gateway_on(&config(port_b, port_a), port_b).await;

    let body = json!({"model": "gpt-4o", "input": "hi"});

    // Node A learns the failure: three consecutive 5xx answers put the
    // provider out of rotation there.
    for _ in 0..3 {
        let resp = client()
            .post(format!("http://127.0.0.1:{port_a}/v1/responses"))
            .json(&body)
            .send()
            .await
            .expect("request completes");
        assert_eq!(
            resp.status(),
            503,
            "the upstream 5xx becomes the gateway's own 503"
        );
    }
    let calls_after_a = mock.received_requests().await.unwrap_or_default().len();
    assert_eq!(calls_after_a, 3, "node A asked the provider three times");

    // Node B adopts the failure: the peer that never sent traffic to the
    // provider stops picking a dead member.
    let mut adopted = false;
    for _ in 0..40 {
        let report: Value = client()
            .get(format!("http://127.0.0.1:{port_b}/admin/breakers"))
            .bearer_auth("test-master-key")
            .send()
            .await
            .expect("the breaker report answers")
            .json()
            .await
            .expect("the report is JSON");
        let dead = report["breakers"]
            .as_array()
            .and_then(|b| b.iter().find(|e| e["provider"] == "dead").cloned());
        if let Some(dead) = dead {
            // An open *without* a deadline: a quota wall carries one, a
            // failure-driven open does not.
            if dead["state_name"] == "open" && dead["force_open_until"].as_u64() == Some(0) {
                adopted = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(adopted, "node B must adopt node A's failure-driven open");

    // …and must not ask the provider again.
    let resp = client()
        .post(format!("http://127.0.0.1:{port_b}/v1/responses"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        503,
        "the peer answers with the gateway's own error"
    );
    let calls_after_b = mock.received_requests().await.unwrap_or_default().len();
    assert_eq!(
        calls_after_b, calls_after_a,
        "the peer must not spend a request on a provider it knows is failing"
    );

    let _ = child_a.kill();
    let _ = child_b.kill();
}

/// An operator's release reaches the peers.
///
/// A topped-up account must not stay parked on the other nodes: the release
/// is a cluster decision, and the peers drop exactly the wall it names.
#[tokio::test]
async fn test_a_release_is_relayed_to_the_peer() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(quota_429_body())
                .insert_header("content-type", "application/json"),
        )
        .mount(&mock)
        .await;

    let port_a = portpicker::pick_unused_port().expect("free port");
    let port_b = portpicker::pick_unused_port().expect("free port");

    let config = |port: u16, peer: u16| {
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"
threads = 1

[rate_limit_learning]
enabled = true

[admin]
master_key = "test-master-key"
enabled = true

[breaker_sync]
enabled = true
interval_secs = 1
peers = ["http://127.0.0.1:{peer}"]
auth_token = "test-master-key"

[providers.dead]
kind = "openai_compat"
base_url = "{mock}"
upstream_path = "/v1/chat/completions"
api_key = "fake-key"
native_responses = true
responses_path = "/v1/responses"
utc_offset = "+08:00"

[[routes]]
path = "/v1/responses"
provider = "dead"
strategy = "fallback"
endpoint_type = "responses"
"#,
            port = port,
            peer = peer,
            mock = mock.uri()
        )
    };

    let (mut child_a, _cfg_a) = spawn_gateway_on(&config(port_a, port_b), port_a).await;
    let (mut child_b, _cfg_b) = spawn_gateway_on(&config(port_b, port_a), port_b).await;

    // Node A learns the wall and parks the provider.
    let body = json!({"model": "gpt-4o", "input": "hi"});
    let resp = client()
        .post(format!("http://127.0.0.1:{port_a}/v1/responses"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    assert_eq!(
        resp.status(),
        503,
        "the quota wall becomes the gateway's 503"
    );

    // Node B adopts the park.
    let mut adopted = false;
    for _ in 0..40 {
        let report: Value = client()
            .get(format!("http://127.0.0.1:{port_b}/admin/breakers"))
            .bearer_auth("test-master-key")
            .send()
            .await
            .expect("the breaker report answers")
            .json()
            .await
            .expect("the report is JSON");
        let until = report["breakers"]
            .as_array()
            .and_then(|b| {
                b.iter()
                    .find(|e| e["provider"] == "dead")
                    .and_then(|e| e["force_open_until"].as_u64())
            })
            .unwrap_or(0);
        if until > 0 {
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(adopted, "node B must adopt node A's quota force-open");

    // The operator releases the provider on A.
    let released: Value = client()
        .post(format!("http://127.0.0.1:{port_a}/admin/breakers/close"))
        .bearer_auth("test-master-key")
        .json(&json!({"provider": "dead"}))
        .send()
        .await
        .expect("the release answers")
        .json()
        .await
        .expect("the release is JSON");
    assert_eq!(released["released"], true, "the provider is freed on A");

    // …and the release reaches B.
    let mut freed = false;
    for _ in 0..40 {
        let report: Value = client()
            .get(format!("http://127.0.0.1:{port_b}/admin/breakers"))
            .bearer_auth("test-master-key")
            .send()
            .await
            .expect("the breaker report answers")
            .json()
            .await
            .expect("the report is JSON");
        let until = report["breakers"]
            .as_array()
            .and_then(|b| {
                b.iter()
                    .find(|e| e["provider"] == "dead")
                    .and_then(|e| e["force_open_until"].as_u64())
            })
            .unwrap_or(0);
        if until == 0 {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(freed, "the release must reach the peer");

    let _ = child_a.kill();
    let _ = child_b.kill();
}

/// A provider the operator parked is never called — the wall the operator
/// knows about costs no request.
#[tokio::test]
async fn test_a_parked_provider_is_never_called() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("never")))
        .mount(&mock)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[quota_parking]
"mock_openai" = "2099-01-01T00:00:00Z"

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#
    .to_string();
    let gw = TestGateway::start_with_fallback(&config, &mock.uri(), &mock.uri()).await;

    let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .expect("request completes");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    assert_eq!(status, 503, "a parked provider cannot serve (body: {text})");
    assert!(
        text.contains("circuit open"),
        "the answer names the parked provider: {text}"
    );
    assert!(
        mock.received_requests()
            .await
            .unwrap_or_default()
            .is_empty(),
        "a parked provider must not be called at all"
    );
}

/// The node names itself: `/health` and the breaker report both carry the
/// id, so a peer can recognise itself in a peer list that is the same on
/// every node.
#[tokio::test]
async fn test_the_node_names_itself_in_health_and_in_the_breaker_report() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[admin]
master_key = "test-master-key"
enabled = true

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;
    let gw = TestGateway::start_with_fallback(config, &mock.uri(), &mock.uri()).await;
    let c = client();

    let report: Value = c
        .get(gw.url("/admin/breakers"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the report answers")
        .json()
        .await
        .expect("the report is JSON");
    // The identity is per *instance*, never taken from the config file
    // (the config file is copied from node to node) and never per machine
    // (two gateways can share a host).
    let report_id = report["instance_id"]
        .as_str()
        .expect("the report names the instance");
    assert!(!report_id.is_empty(), "the instance id must not be empty");
    let node_name = report["node_name"]
        .as_str()
        .expect("the report names the node");
    assert!(!node_name.is_empty(), "the node name must not be empty");

    let health: Value = c
        .get(gw.url("/health"))
        .send()
        .await
        .expect("health answers")
        .json()
        .await
        .expect("health is JSON");
    assert_eq!(health["node_name"].as_str(), Some(node_name));
}

/// The pool report is a live capacity view: every member with its breaker
/// state and the utilization (closed members / total) an operator watches
/// while adding accounts.
#[tokio::test]
async fn test_the_pool_report_lists_members_and_utilization() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("a")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("b")))
        .mount(&b)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[admin]
master_key = "test-master-key"
enabled = true

[providers.pool_a]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.pool_b]
kind = "openai"
base_url = "{FALLBACK_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"

[[auto_model.pool]]
provider = "pool_a"
model = "model-a"

[[auto_model.pool]]
provider = "pool_b"
model = "model-b"

[[routes]]
path = "/v1/responses/auto"
provider = "pool_a"
auto_model = true
"#;
    let gw = TestGateway::start_with_fallback(config, &a.uri(), &b.uri()).await;
    let c = client();

    let report: Value = c
        .get(gw.url("/admin/pool"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the report answers")
        .json()
        .await
        .expect("the report is JSON");
    assert_eq!(
        report["providers"]["healthy"], 2,
        "both providers start closed"
    );
    let pool = &report["pools"]["auto"];
    assert_eq!(pool["total"], 2);
    assert_eq!(pool["closed"], 2);
    assert_eq!(pool["utilization"], 1.0, "nothing is parked yet");
    let members = pool["members"].as_array().expect("members is a list");
    assert_eq!(members.len(), 2);
    let names: Vec<&str> = members
        .iter()
        .map(|m| m["provider"].as_str().expect("member names the provider"))
        .collect();
    assert!(names.contains(&"pool_a"));
    assert!(names.contains(&"pool_b"));
    for m in members {
        assert_eq!(m["state_name"], "closed");
        assert!(m["force_open_until_iso"].is_null());
    }

    // Park one member: utilization halves and the member reads as open.
    let _: Value = c
        .post(gw.url("/admin/breakers/force-open"))
        .bearer_auth("test-master-key")
        .json(&json!({"provider": "pool_a", "until": "2099-01-01T00:00:00Z"}))
        .send()
        .await
        .expect("the park answers")
        .json()
        .await
        .expect("the park is JSON");

    let report: Value = c
        .get(gw.url("/admin/pool"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the report answers")
        .json()
        .await
        .expect("the report is JSON");
    let pool = &report["pools"]["auto"];
    assert_eq!(pool["closed"], 1);
    assert_eq!(pool["open"], 1);
    assert_eq!(pool["utilization"], 0.5);
    let a_entry = pool["members"]
        .as_array()
        .and_then(|m| m.iter().find(|e| e["provider"] == "pool_a"))
        .expect("pool_a is listed");
    assert_eq!(a_entry["state_name"], "open");
    assert!(a_entry["force_open_until_iso"].is_string());

    // A wrong key is refused.
    let resp = c
        .get(gw.url("/admin/pool"))
        .bearer_auth("wrong-key")
        .send()
        .await
        .expect("the request completes");
    assert_eq!(resp.status(), 401, "the pool report needs the master key");
}

/// The operator's park / unpark, and the live breaker report.
#[tokio::test]
async fn test_the_breaker_endpoints_park_and_release_a_provider() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    let config = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[admin]
master_key = "test-master-key"
enabled = true

[providers.mock_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[[routes]]
path = "/v1/chat/completions"
provider = "mock_openai"
"#;
    let gw = TestGateway::start_with_fallback(config, &mock.uri(), &mock.uri()).await;
    let c = client();

    // The report lists the provider.
    let report: Value = c
        .get(gw.url("/admin/breakers"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the report answers")
        .json()
        .await
        .expect("the report is JSON");
    let entry = report["breakers"]
        .as_array()
        .and_then(|b| b.iter().find(|e| e["provider"] == "mock_openai"))
        .expect("the provider is listed");
    assert_eq!(entry["state"], 0, "a fresh provider is closed");

    // Park it.
    let parked: Value = c
        .post(gw.url("/admin/breakers/force-open"))
        .bearer_auth("test-master-key")
        .json(&json!({"provider": "mock_openai", "until": "2099-01-01T00:00:00Z"}))
        .send()
        .await
        .expect("the park answers")
        .json()
        .await
        .expect("the park is JSON");
    assert!(parked["force_open_until"].as_u64().unwrap_or(0) > 0);

    let report: Value = c
        .get(gw.url("/admin/breakers"))
        .bearer_auth("test-master-key")
        .send()
        .await
        .expect("the report answers")
        .json()
        .await
        .expect("the report is JSON");
    let entry = report["breakers"]
        .as_array()
        .and_then(|b| b.iter().find(|e| e["provider"] == "mock_openai"))
        .expect("the provider is listed");
    assert_eq!(entry["state"], 1, "a parked provider reads as open");

    // Release it.
    let released: Value = c
        .post(gw.url("/admin/breakers/close"))
        .bearer_auth("test-master-key")
        .json(&json!({"provider": "mock_openai"}))
        .send()
        .await
        .expect("the release answers")
        .json()
        .await
        .expect("the release is JSON");
    assert_eq!(released["force_open_until"], 0);

    // A wrong key is refused.
    let resp = c
        .get(gw.url("/admin/breakers"))
        .bearer_auth("wrong-key")
        .send()
        .await
        .expect("the request completes");
    assert_eq!(
        resp.status(),
        401,
        "the breaker report needs the master key"
    );
}

/// Native-Responses provider that declares the ark prefill repair explicitly.
/// `base_url` points at the mock, so the repair can only come from the named
/// config field — the host sniff is exercised by the no-flag control below.
const ARK_REPAIR_NATIVE_RESPONSES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_ark]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/api/coding/v3/chat/completions"
responses_path = "/api/coding/v3/responses"
native_responses = true
native_responses_repair = "ark_trailing_assistant"
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "mock_ark"
endpoint_type = "responses"
"#;

/// Same provider without the repair declaration and with a non-ark host: the
/// body must reach the upstream byte-for-byte (no trailing-turn surgery).
const PLAIN_NATIVE_RESPONSES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.mock_plain]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/api/coding/v3/chat/completions"
responses_path = "/api/coding/v3/responses"
native_responses = true
api_key = "fake-key"

[[routes]]
path = "/v1/responses"
provider = "mock_plain"
endpoint_type = "responses"
"#;

fn trailing_assistant_input() -> Value {
    json!([
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "summarise"}]},
        {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "draft"}]}
    ])
}

async fn native_responses_upstream_reply(mock: &MockServer) {
    let upstream_body = json!({
        "id": "resp_ark",
        "object": "response",
        "status": "completed",
        "model": "deepseek-v4-flash",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
    });
    Mock::given(method("POST"))
        .and(path("/api/coding/v3/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(upstream_body))
        .expect(1)
        .mount(mock)
        .await;
}

#[tokio::test]
async fn test_ark_native_responses_drops_trailing_assistant_text_turn() {
    let mock = MockServer::start().await;
    native_responses_upstream_reply(&mock).await;

    let gw = TestGateway::start(ARK_REPAIR_NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let request_body = json!({
        "model": "deepseek-v4-flash",
        "input": trailing_assistant_input(),
        "stream": false
    });

    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let input = sent["input"].as_array().expect("input must stay an array");
    assert_eq!(
        input.len(),
        1,
        "the trailing assistant text turn must be dropped, got {input:?}"
    );
    assert_eq!(input[0]["role"], "user");
}

#[tokio::test]
async fn test_plain_native_responses_keeps_trailing_assistant_text_turn() {
    let mock = MockServer::start().await;
    native_responses_upstream_reply(&mock).await;

    let gw = TestGateway::start(PLAIN_NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let request_body = json!({
        "model": "deepseek-v4-flash",
        "input": trailing_assistant_input(),
        "stream": false
    });

    let resp = client()
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&request_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let input = sent["input"].as_array().expect("input must stay an array");
    assert_eq!(
        input.len(),
        2,
        "a non-ark provider must receive the body untouched"
    );
}

// ---------------------------------------------------------------------------
// Route matching: a provider's name must not be shadowed by a shorter one
// ---------------------------------------------------------------------------

/// `/v1/responses/volc_1` is a string prefix of `/v1/responses/volc_1_spare`.
/// The first prefix hit in config order therefore answered every request for
/// the spare account with the primary's route (2026-11-09:
/// "every candidate of route '/v1/responses/volc_1' is rejecting traffic").
/// Adding accounts to the pool is the whole point, and every account whose
/// name starts with another account's name was unreachable.
const ROUTE_SHADOW_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.volc_1]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "sk-fake"
default_model = "test-model"
native_responses = true
responses_path = "/v1/responses"

[providers.volc_1_spare]
kind = "openai_compat"
base_url = "{FALLBACK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "sk-fake"
default_model = "test-model"
native_responses = true
responses_path = "/v1/responses"

[providers.open_design]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/v1/chat/completions"
api_key = "sk-fake"
default_model = "test-model"
native_responses = true
responses_path = "/v1/responses"

[[routes]]
path = "/v1/responses/volc_1"
provider = "volc_1"
endpoint_type = "responses"

[[routes]]
path = "/v1/responses/open-design"
provider = "open_design"
endpoint_type = "responses"

[[routes]]
path = "/v1/responses"
provider = "volc_1"
endpoint_type = "responses"
"#;

fn responses_request_body(model: &str) -> Value {
    json!({
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false
    })
}

async fn mount_native_responses(mock: &MockServer, content: &str) {
    // A native-Responses upstream answers with a Responses object; the
    // gateway forwards it verbatim, so the text says which mock answered.
    let body = json!({
        "id": "resp_shadow",
        "object": "response",
        "created_at": 1700000000,
        "model": "test-model",
        "status": "completed",
        "output": [{
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": content}]
        }],
        "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5}
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(mock)
        .await;
}

/// Start the shadowing gateway: `primary` serves volc_1 + open_design,
/// `spare` serves volc_1_spare.
async fn shadow_gateway() -> (TestGateway, MockServer, MockServer) {
    let primary = MockServer::start().await;
    let spare = MockServer::start().await;
    mount_native_responses(&primary, "from primary").await;
    mount_native_responses(&spare, "from spare").await;
    let gw =
        TestGateway::start_with_fallback(ROUTE_SHADOW_CONFIG, &primary.uri(), &spare.uri()).await;
    (gw, primary, spare)
}

#[tokio::test]
async fn test_a_provider_name_is_not_shadowed_by_a_shorter_one() {
    let (gw, primary, spare) = shadow_gateway().await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/volc_1_spare"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request_body("test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("volc_1_spare"),
        "the spare account's request must not be answered by volc_1's route"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"][0]["content"][0]["text"], "from spare");
    assert_eq!(spare.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        primary.received_requests().await.unwrap().len(),
        0,
        "volc_1's upstream must not have been called at all"
    );
}

#[tokio::test]
async fn test_the_shorter_provider_route_still_serves_its_own_path() {
    let (gw, primary, spare) = shadow_gateway().await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/responses/volc_1"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request_body("test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("volc_1")
    );
    assert_eq!(primary.received_requests().await.unwrap().len(), 1);
    assert_eq!(spare.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_the_hyphen_alias_route_still_wins_over_the_generic_route() {
    let (gw, _primary, _spare) = shadow_gateway().await;
    let c = client();

    // `/v1/responses/open-design` is the documented alias for the
    // `open_design` provider (hyphen vs underscore).
    let resp = c
        .post(gw.url("/v1/responses/open-design"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request_body("test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("open_design")
    );
}

#[tokio::test]
async fn test_the_provider_name_under_the_generic_route_still_pins() {
    let (gw, primary, spare) = shadow_gateway().await;
    let c = client();

    // No `[[routes]]` entry names open_design with an underscore: the
    // generic route must pin it by name.
    let resp = c
        .post(gw.url("/v1/responses/open_design"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request_body("test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("open_design")
    );
    assert_eq!(primary.received_requests().await.unwrap().len(), 1);
    assert_eq!(spare.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_a_client_that_appends_the_endpoint_reaches_the_provider_route() {
    let (gw, primary, spare) = shadow_gateway().await;
    let c = client();

    // A client whose base URL is `/v1/responses/volc_1_spare` posts to
    // `/v1/responses/volc_1_spare/responses`: the route still matches,
    // because the extra segment starts at a boundary.
    let resp = c
        .post(gw.url("/v1/responses/volc_1_spare/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&responses_request_body("test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("volc_1_spare")
    );
    assert_eq!(spare.received_requests().await.unwrap().len(), 1);
    assert_eq!(primary.received_requests().await.unwrap().len(), 0);
}

// ── the pool sentinel in the body beats the pool implied by the path ────
//
// `auto` must only ever reach a vendor and `auto_home` only ever a home
// machine. The only thing standing between a caller and a wrong-pool answer
// is which pool the gateway *enters*, and that used to depend on whether the
// client sent a session header: the body-sentinel prefetch was skipped
// whenever one was present, so codex — which sends `session-id` on every
// request — had its `auto_home` ignored and was served by the route's pool
// (vendors), with the body's model rewritten to the vendor's model name.
const SENTINEL_VS_PATH_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.mock]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.cloud_a]
kind = "openai"
base_url = "{POOL_A_URL}"
api_key = "fake-key"
# The mock lives on 127.0.0.1, so the URL-based inference would call it
# self-hosted. Real vendors are reached over a public name; say so, otherwise
# the `vendor` scope below would (correctly) refuse to start.
self_hosted = false

[providers.home_b]
kind = "openai"
base_url = "{POOL_B_URL}"
api_key = "fake-key"

[auto_model]
enabled = true
name = "auto"
scope = "vendor"

[[auto_model.pool]]
provider = "cloud_a"
model = "model-cloud"

[auto_models.auto_home]
scope = "self_hosted"

[[auto_models.auto_home.pool]]
provider = "home_b"
model = "model-home"

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100
body_affinity = true

[[routes]]
path = "/v1/chat/completions"
provider = "mock"
auto_model = true
session_sticky = true

[[routes]]
path = "/v1/home"
provider = "auto_home"
auto_model = true
session_sticky = true
"#;

fn reply_model(body: &Value) -> String {
    body["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// The body's sentinel decides the pool even when the client sends a session
/// header, and even when the path names the other pool.
#[tokio::test]
async fn test_body_sentinel_beats_the_path_and_the_session_header() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("unused")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("vendor")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("home")))
        .mount(&b)
        .await;

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(SENTINEL_VS_PATH_CONFIG, [&mock.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    // Ask first, assert afterwards. Every request is bounded by a timeout so
    // a regression fails the test instead of wedging it, and the gateway is
    // reaped before the first assertion so a failure never leaks the child.
    async fn ask(
        c: &Client,
        base: &str,
        path: &str,
        sess: &str,
        model: &str,
    ) -> (reqwest::StatusCode, Value) {
        let resp = tokio::time::timeout(
            Duration::from_secs(30),
            c.post(format!("{base}{path}"))
                .header("session-id", sess)
                .json(&chat_body(model, "please"))
                .send(),
        )
        .await
        .expect("the gateway must answer within 30s")
        .expect("the gateway must answer");
        let status = resp.status();
        let body: Value = resp.json().await.expect("a json body");
        (status, body)
    }

    // 1. The bug: `auto_home` on the `auto` route *with* a session header.
    let (s1, b1) = ask(
        &c,
        &base,
        "/v1/chat/completions",
        "sess-sentinel-1",
        "auto_home",
    )
    .await;
    // 2. `auto` on that same route still runs on the vendor pool.
    let (s2, b2) = ask(&c, &base, "/v1/chat/completions", "sess-sentinel-2", "auto").await;
    // 3. The mirror image: `auto` on the `auto_home` route runs on vendors,
    //    so a stale base URL cannot force vendor traffic onto a home GPU.
    let (s3, b3) = ask(&c, &base, "/v1/home", "sess-sentinel-3", "auto").await;
    // 4. And `auto_home` on it runs at home.
    let (s4, b4) = ask(&c, &base, "/v1/home", "sess-sentinel-4", "auto_home").await;

    let home_reqs = b.received_requests().await.unwrap_or_default();
    let cloud_reqs = a.received_requests().await.unwrap_or_default();
    let route_reqs = mock.received_requests().await.unwrap_or_default();

    let _ = child.kill();

    assert_eq!(s1, 200);
    assert_eq!(
        reply_model(&b1),
        "home",
        "auto_home on the auto route must run on the home pool: {b1}"
    );
    assert_eq!(s2, 200);
    assert_eq!(
        reply_model(&b2),
        "vendor",
        "auto must stay on vendors: {b2}"
    );
    assert_eq!(s3, 200);
    assert_eq!(reply_model(&b3), "vendor", "auto on the home route: {b3}");
    assert_eq!(s4, 200);
    assert_eq!(
        reply_model(&b4),
        "home",
        "auto_home on the home route: {b4}"
    );

    // The sentinel itself never reaches an upstream: the entry's model does.
    assert!(!home_reqs.is_empty());
    for req in &home_reqs {
        let raw = String::from_utf8_lossy(&req.body);
        assert!(
            raw.contains("model-home"),
            "home upstream must be asked for its own model: {raw}"
        );
        assert!(
            !raw.contains("auto_home"),
            "the sentinel must not leak upstream: {raw}"
        );
    }
    assert!(!cloud_reqs.is_empty());
    for req in &cloud_reqs {
        let raw = String::from_utf8_lossy(&req.body);
        assert!(raw.contains("model-cloud"), "vendor upstream model: {raw}");
    }
    // The route's own provider must stay untouched: a request that fell
    // back to it is a request that no pool claimed.
    assert!(
        route_reqs.is_empty(),
        "the route's own provider must not serve a pooled request: {route_reqs:?}"
    );
}

// ── runtime scope enforcement: a scoped pool fails closed ────────────────
//
// `scope = "self_hosted"` / `"vendor"` is checked when the config loads, but
// the boundary is only *called* at load — at request time three more ways
// existed to hand the request across it:
//   1. every member of the scoped pool is down → the selector used to fall
//      through to the route's own chain (the vendors),
//   2. the path pins a provider (`/v1/responses/cloud_a`) while the body
//      says `auto_home` → the pin used to be served on its own, without ever
//      consulting the pool,
//   3. the sentinel only appears in the body and the affinity prefetch is
//      off → the provider picked before the body was read used to forward
//      the literal sentinel upstream instead of refusing.
// Each of these now answers 503, and no vendor ever sees the request body.
const SCOPE_FAILCLOSED_CONFIG: &str = r#"
[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[providers.mock]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"

[providers.cloud_a]
kind = "openai"
base_url = "{POOL_A_URL}"
api_key = "fake-key"
self_hosted = false

# The pool member for the fail-closed tests. The tests point {POOL_B_URL} at a
# dead port, so one refused connection opens its circuit immediately.
[providers.home_b]
kind = "openai"
base_url = "{POOL_B_URL}"
api_key = "fake-key"
max_fails = 1
fail_timeout_secs = 3600

[auto_model]
enabled = true
name = "auto"
scope = "vendor"

[[auto_model.pool]]
provider = "cloud_a"
model = "model-cloud"

[auto_models.auto_home]
scope = "self_hosted"

[[auto_models.auto_home.pool]]
provider = "home_b"
model = "model-home"

[session_sticky]
enabled = true
timeout_secs = 3600
max_entries = 100
body_affinity = true

[[routes]]
path = "/v1/chat/completions"
provider = "cloud_a"
fallback = ["mock"]
auto_model = true
session_sticky = true

[[routes]]
path = "/v1/responses"
provider = "cloud_a"
fallback = ["mock"]
endpoint_type = "responses"
session_sticky = true
"#;

async fn scope_ask(
    c: &Client,
    base: &str,
    path: &str,
    sess: &str,
    body: &Value,
) -> (reqwest::StatusCode, Value) {
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        c.post(format!("{base}{path}"))
            .header("session-id", sess)
            .json(body)
            .send(),
    )
    .await
    .expect("the gateway must answer within 30s")
    .expect("the gateway must answer");
    let status = resp.status();
    let body: Value = resp.json().await.expect("a json body");
    (status, body)
}

/// No upstream may receive the literal `auto_home` sentinel — not even when
/// the gateway refuses the request. (A refusal can leave a connection open on
/// the upstream side, so the guard is on the *body*, not on the count.)
fn upstream_saw_the_sentinel(reqs: &[wiremock::Request]) -> bool {
    reqs.iter().any(|r| {
        String::from_utf8_lossy(&r.body).contains("auto_home")
            || String::from_utf8_lossy(&r.body).contains("model-home")
    })
}

/// 1. Every member of the scoped pool is down: the request must fail closed
/// with a 503 instead of falling through to the route's vendor chain.
#[tokio::test]
async fn test_a_scoped_pool_never_falls_back_to_the_route_chain() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("route")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("vendor")))
        .mount(&a)
        .await;

    // `{POOL_B_URL}` is a dead port: the first request trips home_b's breaker
    // (max_fails = 1), the second finds the pool empty at selection time.
    let (mut child, base, _cfg) = spawn_gateway_with_pool(
        SCOPE_FAILCLOSED_CONFIG,
        [&mock.uri(), &a.uri(), "http://127.0.0.1:1"],
    )
    .await;
    let c = client();

    let (s1, b1) = scope_ask(
        &c,
        &base,
        "/v1/chat/completions",
        "scope-dead-1",
        &chat_body("auto_home", "please"),
    )
    .await;
    let (s2, b2) = scope_ask(
        &c,
        &base,
        "/v1/chat/completions",
        "scope-dead-2",
        &chat_body("auto_home", "please"),
    )
    .await;

    let route_reqs = mock.received_requests().await.unwrap_or_default();
    let cloud_reqs = a.received_requests().await.unwrap_or_default();
    let _ = child.kill();

    // The first request dies at the dead member (it can go nowhere else).
    assert!(
        s1.as_u16() >= 500,
        "the first request must fail while the member is down: {s1} {b1}"
    );
    // The second one is the regression: with every member unhealthy the
    // selector used to clear the pool and continue on the route's chain.
    assert_eq!(
        s2, 503,
        "a scoped pool with no healthy member must fail closed, not fall back: {b2}"
    );
    let msg = b2["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("home_b") && msg.contains("circuit open"),
        "the 503 must name the member and why it is out: {b2}"
    );
    assert!(
        route_reqs.is_empty(),
        "the route's own provider must not serve a scoped-pool request: {route_reqs:?}"
    );
    assert!(
        cloud_reqs.is_empty(),
        "vendors must never see a request the home pool could not serve: {cloud_reqs:?}"
    );
    assert!(!upstream_saw_the_sentinel(&cloud_reqs));
}

/// 2. The path pins a vendor while the body asks for `auto_home`: the pin is
/// on the wrong side of the pool's boundary, so the request is refused.
#[tokio::test]
async fn test_path_pinning_cannot_pull_a_scoped_pool_onto_a_vendor() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("route")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("vendor")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("home")))
        .mount(&b)
        .await;

    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(SCOPE_FAILCLOSED_CONFIG, [&mock.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    let (status, body) = scope_ask(
        &c,
        &base,
        "/v1/responses/cloud_a",
        "scope-pin-1",
        &responses_request_body("auto_home"),
    )
    .await;

    let cloud_reqs = a.received_requests().await.unwrap_or_default();
    let route_reqs = mock.received_requests().await.unwrap_or_default();
    let home_reqs = b.received_requests().await.unwrap_or_default();
    let _ = child.kill();

    assert_eq!(
        status, 503,
        "a vendor pinned by the path may not serve the self-hosted pool: {body}"
    );
    let msg = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("out of scope") && msg.contains("self-hosted only"),
        "the 503 must say the pin is out of scope: {body}"
    );
    assert!(
        cloud_reqs.is_empty(),
        "the pinned vendor must get no request at all: {cloud_reqs:?}"
    );
    assert!(
        route_reqs.is_empty(),
        "the route's own provider must not take over: {route_reqs:?}"
    );
    assert!(
        home_reqs.is_empty(),
        "a path pin is served by the pinned provider alone: {home_reqs:?}"
    );
}

/// 3. The sentinel only lives in the body (no affinity prefetch), so the
/// provider is picked before the gateway knows what pool was asked for. The
/// body filter used to forward the literal sentinel to that provider; it must
/// refuse instead.
#[tokio::test]
async fn test_a_body_only_scoped_sentinel_is_refused_when_the_prefetch_is_off() {
    let mock = MockServer::start().await;
    let a = MockServer::start().await;
    let b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("route")))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("vendor")))
        .mount(&a)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("home")))
        .mount(&b)
        .await;

    let cfg = SCOPE_FAILCLOSED_CONFIG.replace("body_affinity = true", "body_affinity = false");
    let (mut child, base, _cfg) =
        spawn_gateway_with_pool(&cfg, [&mock.uri(), &a.uri(), &b.uri()]).await;
    let c = client();

    let (status, body) = scope_ask(
        &c,
        &base,
        "/v1/chat/completions",
        "scope-body-1",
        &chat_body("auto_home", "please"),
    )
    .await;

    let cloud_reqs = a.received_requests().await.unwrap_or_default();
    let route_reqs = mock.received_requests().await.unwrap_or_default();
    let home_reqs = b.received_requests().await.unwrap_or_default();
    let _ = child.kill();

    assert_eq!(
        status, 503,
        "without the prefetch the sentinel is discovered too late to re-point; refuse: {body}"
    );
    let msg = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("auto_home") && msg.contains("deployment boundary"),
        "the refusal must name the sentinel and the boundary: {body}"
    );
    assert!(
        !upstream_saw_the_sentinel(&cloud_reqs),
        "the sentinel must never reach a vendor: {cloud_reqs:?}"
    );
    assert!(
        !upstream_saw_the_sentinel(&route_reqs),
        "the sentinel must not reach the route's provider either: {route_reqs:?}"
    );
    assert!(
        home_reqs.is_empty(),
        "the home member is never entered from a body-only first hop: {home_reqs:?}"
    );
}

// ---------------------------------------------------------------------------
// Moonshot tool-schema projection — codex's zod 4 schemas reach Kimi whole
// ---------------------------------------------------------------------------

/// The provider names a Moonshot-family host in production and the dialect
/// is switched on by the host; in tests the mock stands in for the host, so
/// the config names the dialect explicitly (`tool_schema = "moonshot"`).
const MOONSHOT_TOOL_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.kimi_mock]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/coding/v1/chat/completions"
api_key = "fake-key"
tool_schema = "moonshot"

[[routes]]
path = "/v1/chat/completions"
provider = "kimi_mock"
"#;

/// codex sends zod 4 tool schemas: reused shapes live in `$defs`, refined
/// ones are written as `$ref` beside their own keywords, and recursive ones
/// never terminate — the shapes Kimi's "moonshot flavored json schema"
/// validator refuses.
fn zod_tool() -> Value {
    json!({
        "type": "function",
        "function": {
            "name": "memory_feedback",
            "description": "Record a quality signal.",
            "parameters": {
                "type": "object",
                "properties": {
                    "show": {"$ref": "#/$defs/__schema0"},
                    "score": {"type": "integer", "maximum": 9007199254740991i64, "$ref": "#/$defs/__schema1"}
                },
                "required": ["show"],
                "$defs": {
                    "__schema0": {"$ref": "#/$defs/__schema0"},
                    "__schema1": {"type": "number", "minimum": 0}
                }
            }
        }
    })
}

fn moonshot_rejection() -> Value {
    json!({
        "error": {
            "code": "invalid_request_error",
            "message": "Invalid request: tools.function.parameters is not a valid moonshot flavored json schema, details: <At path 'properties.show': detected infinite recursion without termination condition>",
            "type": "invalid_request_error"
        }
    })
}

#[tokio::test]
async fn test_moonshot_chat_tools_are_projected_before_the_upstream() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_TOOL_CONFIG, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("kimi-for-coding", "hi");
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let schema = &sent["tools"][0]["function"]["parameters"];

    // The recursive chain is cut: the def ends at a typed object, so the
    // walk Moonshot runs terminates.
    assert_eq!(
        schema["$defs"]["__schema0"],
        json!({"type": "object"}),
        "the self-reference must be cut: {schema}"
    );
    assert_eq!(
        schema["properties"]["show"],
        json!({"$ref": "#/$defs/__schema0"}),
        "the pure reference to the cut target is the honest shape: {schema}"
    );
    // The refined `type`+`$ref` site is inlined — the site's type wins.
    assert_eq!(schema["properties"]["score"]["type"], "integer", "{schema}");
    assert!(schema["properties"]["score"].get("$ref").is_none());
    assert_eq!(schema["properties"]["score"]["minimum"], 0);
    // The tool name and description survive untouched.
    assert_eq!(sent["tools"][0]["function"]["name"], "memory_feedback");
    assert_eq!(
        sent["tools"][0]["function"]["description"],
        "Record a quality signal."
    );
}

#[tokio::test]
async fn test_moonshot_rejection_is_retried_with_degraded_tools() {
    let mock = MockServer::start().await;

    // The projection cannot know every dialect rule the upstream might add;
    // a rejection that survives it is met with the degradation retry. The
    // first attempt carries the projected schemas (still with `$defs`), the
    // retry carries the degraded ones — the mocks tell them apart by body.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("\"$defs\""))
        .respond_with(ResponseTemplate::new(400).set_body_json(moonshot_rejection()))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("\"parameters\":{\"type\":\"object\"}"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("after retry")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_TOOL_CONFIG, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("kimi-for-coding", "hi");
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the degradation retry must answer, not the 400"
    );
    assert_eq!(
        resp.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "after retry"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "the request must be retried once");
    let degraded: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let schema = &degraded["tools"][0]["function"]["parameters"];
    assert_eq!(
        *schema,
        json!({"type": "object"}),
        "the argument contracts collapse to a bare object: {schema}"
    );
    // The tool list survives — the conversation keeps its function surface.
    assert_eq!(degraded["tools"][0]["function"]["name"], "memory_feedback");
    assert_eq!(degraded["messages"][0]["role"], "user");
}

#[tokio::test]
async fn test_moonshot_orphaned_tool_call_is_repaired() {
    let mock = MockServer::start().await;

    // Kimi validates tool-call pairing: a history whose assistant call was
    // never answered (a compaction that kept the call and dropped its
    // result) is answered with a request-fault 400 that names the call_id —
    // the exact shape that ends an agent's turn. The gateway must answer the
    // orphan and replay the request, not die on the upstream's 400.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("call_abc123"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "invalid_request_error",
                "message": "Invalid request: an assistant message with 'tool_calls' must be followed by tool messages responding to each 'tool_call_id'. The following tool_call_ids did not have response messages: call_abc123",
                "type": "invalid_request_error"
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    // The repaired replay answers the call with the sentinel; both mocks
    // match the history otherwise, so the sentinel wins by priority.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("call_abc123"))
        .and(body_string_contains("no result was recorded"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("after repair")))
        .with_priority(1)
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_TOOL_CONFIG, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("kimi-for-coding", "orphan probe");
    body["messages"] = json!([
        {"role": "user", "content": "fetch the thing"},
        {"role": "assistant", "content": null, "tool_calls": [{
            "id": "call_abc123",
            "type": "function",
            "function": {"name": "memory_feedback", "arguments": "{}"}
        }]},
    ]);
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the repair retry must answer, not the upstream's 400"
    );
    assert_eq!(
        resp.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "after repair"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "the request must be retried once");
    let repaired: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let messages = repaired["messages"].as_array().unwrap();
    // The assistant call is followed by the sentinel tool reply.
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[2]["role"], "tool");
    assert_eq!(messages[2]["tool_call_id"], "call_abc123");
    assert_eq!(
        messages[2]["content"],
        "[tool call interrupted: no result was recorded]"
    );
}

/// The translation kernel: a Rhai script that owns the provider-dialect
/// decision surface. Saving the script is the deploy — no release cycle.
const KERNEL_TEST_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.kimi_mock]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/coding/v1/chat/completions"
api_key = "fake-key"
tool_schema = "moonshot"

[[routes]]
path = "/v1/chat/completions"
provider = "kimi_mock"

[translation_kernel]
script = "{KERNEL_SCRIPT}"
"#;

/// Write a kernel script to a temp file and return its path.
fn kernel_script(source: &str) -> (tempfile::NamedTempFile, String) {
    let mut file = tempfile::NamedTempFile::new().expect("temp kernel script");
    file.write_all(source.as_bytes()).expect("write kernel");
    file.flush().expect("flush kernel");
    let path = file.path().to_string_lossy().to_string();
    (file, path)
}

#[tokio::test]
async fn test_the_translation_kernel_rewrites_the_outgoing_body() {
    let mock = MockServer::start().await;

    // The script's rewrite must reach the upstream — the kernel is not a
    // simulation: it owns the last word on the body.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("\"kernel_marker\":\"applied\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .mount(&mock)
        .await;

    let (_script, path) = kernel_script(
        r#"
        fn on_request_body(ctx) {
            let body = ctx["body"];
            body["kernel_marker"] = "applied";
            body
        }
        "#,
    );
    let config = KERNEL_TEST_CONFIG.replace("{KERNEL_SCRIPT}", &path);
    let gw = TestGateway::start(&config, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("kimi-for-coding", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(sent["kernel_marker"], "applied");
}

#[tokio::test]
async fn test_the_translation_kernel_classifies_an_error_the_baseline_does_not_know() {
    let mock = MockServer::start().await;

    // The baseline markers know the Moonshot dialect's phrasings; this one
    // is new. The kernel script recognises it and names the repair — the
    // request is replayed with degraded schemas instead of ending the
    // agent's turn on the upstream's 400.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("\"$defs\""))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "invalid_request_error",
                "message": "Invalid request: upstream says: custom dialect rule violated"
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .and(body_string_contains("\"parameters\":{\"type\":\"object\"}"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("after kernel")))
        .expect(1)
        .mount(&mock)
        .await;

    let (_script, path) = kernel_script(
        r#"
        fn on_upstream_error(ctx) {
            if ctx["body"].contains("custom dialect rule violated") {
                #{ "repair": "schema" }
            } else {
                #{ "repair": "none" }
            }
        }
        "#,
    );
    let config = KERNEL_TEST_CONFIG.replace("{KERNEL_SCRIPT}", &path);
    let gw = TestGateway::start(&config, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("kimi-for-coding", "hi");
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the kernel-driven repair must answer, not the upstream's 400"
    );
    assert_eq!(
        resp.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "after kernel"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "the request must be retried once");
    let degraded: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        degraded["tools"][0]["function"]["parameters"],
        json!({"type": "object"}),
        "the retry carries the degraded schemas"
    );
}

#[tokio::test]
async fn test_saving_the_kernel_script_reloads_it_without_a_restart() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .mount(&mock)
        .await;

    let script = tempfile::NamedTempFile::new().expect("temp kernel script");
    let script_path = script.path().to_path_buf();
    let write_script = |marker: &str| {
        std::fs::write(
            &script_path,
            format!(
                "fn on_request_body(ctx) {{\n    let body = ctx[\"body\"];\n    body[\"kernel_marker\"] = \"{marker}\";\n    body\n}}\n"
            ),
        )
        .expect("write kernel");
    };
    write_script("first");

    let config = KERNEL_TEST_CONFIG.replace("{KERNEL_SCRIPT}", &script_path.to_string_lossy());
    let gw = TestGateway::start(&config, &mock.uri()).await;
    let c = client();

    let post = || async {
        c.post(gw.url("/v1/chat/completions"))
            .header("Authorization", "Bearer test-key-123")
            .json(&chat_body("kimi-for-coding", "hi"))
            .send()
            .await
            .unwrap()
    };

    let resp = post().await;
    assert_eq!(resp.status(), 200);
    let sent: Value =
        serde_json::from_slice(&mock.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(sent["kernel_marker"], "first");

    // Save the script: the watcher recompiles the kernel, and the next
    // request uses the new policy — no restart, no new binary.
    write_script("second");

    // The watcher settles for 800ms before it reads; give it room.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut second_ok = false;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let resp = post().await;
        assert_eq!(resp.status(), 200);
        for request in mock.received_requests().await.unwrap() {
            let sent: Value = serde_json::from_slice(&request.body).unwrap();
            if sent["kernel_marker"] == "second" {
                second_ok = true;
                break;
            }
        }
        if second_ok {
            break;
        }
    }
    assert!(
        second_ok,
        "saving the kernel script must reload it without a restart"
    );
}

#[tokio::test]
async fn test_a_broken_kernel_script_refuses_to_start() {
    let mock = MockServer::start().await;
    let (_script, path) = kernel_script("fn on_request_body(ctx) { let = ; }");
    let config = KERNEL_TEST_CONFIG
        .replace("{KERNEL_SCRIPT}", &path)
        .replace("{MOCK_URL}", &mock.uri());

    // A kernel that does not compile is a config error, not a warning: the
    // gateway must not keep serving with a policy the operator believes is
    // live.
    let port = pick_gateway_port();
    let config_str = config.replace("{PORT}", &port.to_string());
    let mut config_file = tempfile::NamedTempFile::new().expect("temp file");
    config_file
        .write_all(config_str.as_bytes())
        .expect("write config");
    let binary = env!("CARGO_BIN_EXE_maxllm-server");
    let mut child = Command::new(binary)
        .arg("--config")
        .arg(config_file.path())
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("failed to spawn gateway");

    // The gateway exits instead of serving without the kernel.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            assert!(
                !status.success(),
                "the gateway must not start with a kernel that does not compile"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the gateway neither started nor exited"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Kimi serves the Responses contract natively (`/coding/v1/responses`), so
/// codex's tools reach it in the flat Responses shape — the projection has to
/// walk that shape too.
const MOONSHOT_NATIVE_RESPONSES_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.kimi_mock]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/coding/v1/chat/completions"
api_key = "fake-key"
native_responses = true
tool_schema = "moonshot"

[[routes]]
path = "/v1/responses"
provider = "kimi_mock"
endpoint_type = "responses"
"#;

fn flat_zod_tool() -> Value {
    let tool = zod_tool();
    let mut flat = tool["function"].clone();
    flat["type"] = json!("function");
    flat
}

#[tokio::test]
async fn test_moonshot_native_responses_tools_are_projected() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/coding/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "resp_kimi",
            "object": "response",
            "status": "completed",
            "model": "kimi-for-coding",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_NATIVE_RESPONSES_CONFIG, &mock.uri()).await;
    let c = client();

    let body = json!({
        "model": "kimi-for-coding",
        "input": "hi",
        "stream": false,
        "tools": [flat_zod_tool()]
    });

    let resp = c
        .post(gw.url("/v1/responses"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let schema = &sent["tools"][0]["parameters"];
    assert_eq!(
        schema["$defs"]["__schema0"],
        json!({"type": "object"}),
        "the recursive chain is cut in the flat shape too: {schema}"
    );
    assert_eq!(schema["properties"]["score"]["type"], "integer");
    assert!(schema["properties"]["score"].get("$ref").is_none());
    assert_eq!(sent["tools"][0]["name"], "memory_feedback");
}

#[tokio::test]
async fn test_an_unrelated_400_is_not_retried_or_rewritten() {
    let mock = MockServer::start().await;

    // A 400 that names anything else is the client's own fault — the answer
    // reaches the client as it arrived, and the tool schemas stay whole.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": "invalid_request_error",
                      "message": "Invalid request: missing 'messages' parameter",
                      "type": "invalid_request_error"}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_TOOL_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("kimi-for-coding", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let answer: Value = resp.json().await.unwrap();
    assert!(
        answer["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("missing 'messages'"),
        "the upstream's own answer must reach the client: {answer}"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "an unrelated 400 must not be retried");
}

#[tokio::test]
async fn test_a_non_moonshot_upstream_keeps_its_tool_schemas() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .mount(&mock)
        .await;

    // A generic openai provider on a non-Moonshot host: the body is
    // forwarded byte-for-byte, zod schemas and all.
    let gw = TestGateway::start(MOCK_CONFIG, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("gpt-4o", "hi");
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(
        sent["tools"][0]["function"]["parameters"]["properties"]["score"]["$ref"],
        "#/$defs/__schema1",
        "a provider without the dialect must not see a rewritten schema"
    );
}

/// A native route on a Moonshot provider still carries OpenAI-shaped tool
/// schemas — the upstream validates them against its dialect no matter which
/// route carried them, so the projection applies to the native path too.
const MOONSHOT_NATIVE_ROUTE_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.kimi_mock]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/coding/v1/chat/completions"
api_key = "fake-key"
tool_schema = "moonshot"

[[routes]]
path = "/v1/chat/completions"
provider = "kimi_mock"
endpoint_type = "native"
"#;

#[tokio::test]
async fn test_moonshot_native_route_tools_are_projected() {
    let mock = MockServer::start().await;

    // A native route forwards the client's own path upstream.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_NATIVE_ROUTE_CONFIG, &mock.uri()).await;
    let c = client();

    let mut body = chat_body("kimi-for-coding", "hi");
    body["tools"] = json!([zod_tool()]);

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let requests = mock.received_requests().await.unwrap();
    let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let schema = &sent["tools"][0]["function"]["parameters"];
    assert_eq!(
        schema["$defs"]["__schema0"],
        json!({"type": "object"}),
        "the recursive chain is cut on the native route too: {schema}"
    );
    assert!(schema["properties"]["score"].get("$ref").is_none());
    assert_eq!(sent["tools"][0]["function"]["name"], "memory_feedback");
}
/// A Moonshot provider that does not carry the requested model answers 400
/// (one-api's `invalid params, unknown model`). The route must fail over to
/// the provider that does carry it — the tool-schema deferral only preempts
/// the handling that would end the turn, never the one that retries.
const MOONSHOT_FAILOVER_CONFIG: &str = r#"
global_plugins = ["api_auth"]

[server]
listen = "127.0.0.1:{PORT}"
threads = 1

[metrics]
enabled = true

[plugins.api_auth]
category = "key_auth"
header = "Authorization"
strip_prefix = "Bearer "
keys = ["test-key-123"]

[providers.kimi_mock]
kind = "openai_compat"
base_url = "{MOCK_URL}"
upstream_path = "/coding/v1/chat/completions"
api_key = "fake-key"
default_model = "kimi-for-coding"
tool_schema = "moonshot"

[providers.backup_openai]
kind = "openai"
base_url = "{MOCK_URL}"
api_key = "fake-key"
default_model = "gpt-4o"

[[routes]]
path = "/v1/chat/completions"
provider = "kimi_mock"
fallback = ["backup_openai"]
strategy = "fallback"
"#;

#[tokio::test]
async fn test_a_moonshot_model_mismatch_still_fails_over() {
    let mock = MockServer::start().await;

    // The Moonshot provider does not carry "gpt-4o" and says so with a 400.
    Mock::given(method("POST"))
        .and(path("/coding/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": "invalid_request_error",
                      "message": "invalid params, unknown model: gpt-4o",
                      "type": "invalid_request_error"}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("from the backup")))
        .expect(1)
        .mount(&mock)
        .await;

    let gw = TestGateway::start(MOONSHOT_FAILOVER_CONFIG, &mock.uri()).await;
    let c = client();

    let resp = c
        .post(gw.url("/v1/chat/completions"))
        .header("Authorization", "Bearer test-key-123")
        .json(&chat_body("gpt-4o", "hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the route must fail over, not swallow the 400 on the tool-schema deferral"
    );
    assert_eq!(
        resp.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "from the backup"
    );

    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "the request must reach the backup");
}
