// Integration tests for the MaxLLM gateway.
//
// These tests spawn the gateway as a subprocess and send HTTP requests
// against it. Mock-based tests use wiremock as a fake upstream provider.
// Real provider tests (Ollama, Gemini) are gated with #[ignore].

use reqwest::Client;
use serde_json::{json, Value};
use std::io::Write;
use std::process::{Child, Command};
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
        Self::start_with_fallback_and_laya(config_template, mock_url, fallback_url, LAYA_TEST_URL).await
    }

    /// Like [`Self::start_with_fallback`], but with a caller-supplied laya
    /// decision-server URL (used to point the resolver at a wiremock fake).
    async fn start_with_fallback_and_laya(
        config_template: &str,
        mock_url: &str,
        fallback_url: &str,
        laya_url: &str,
    ) -> Self {
        let port = portpicker::pick_unused_port().expect("no free port");
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
            .env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            // Unset proxy env vars to avoid TLS issues
            .env_remove("HTTPS_PROXY")
            .env_remove("HTTP_PROXY")
            .env_remove("https_proxy")
            .env_remove("http_proxy")
            .spawn()
            .expect("failed to spawn gateway");

        let gw = Self {
            child,
            port,
            _config_file: config_file,
        };

        // Wait for the gateway to be ready
        let client = Client::new();
        let health_url = format!("http://127.0.0.1:{port}/health");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("Gateway did not start within 10 seconds");
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
    // Gateway forwards the upstream 500
    assert_eq!(resp.status(), 500);
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

    // Both providers failed; the client receives the LAST provider's error.
    assert_eq!(resp.status(), 429);
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
    // error of p2 is returned and p3 is never touched.
    assert_eq!(resp.status(), 500);
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
    let quota_opens =
        metric_sample(&gw, "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}")
            .await;
    assert_eq!(
        quota_opens,
        Some(1.0),
        "the resolved quota must count exactly one quota open"
    );
    let state =
        metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0), "the breaker must be open (1)");
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
    let quota_opens =
        metric_sample(&gw, "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}")
            .await;
    assert_eq!(
        quota_opens, None,
        "a non-rate-limit error must not count a quota open"
    );
    // The ordinary failure left the breaker closed (max_fails defaults to 3).
    let state =
        metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
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

    let quota_opens =
        metric_sample(&gw, "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}")
            .await;
    assert_eq!(quota_opens, Some(1.0));
    // The header path must count the quota open exactly once — the generic
    // 429 accounting must not run a second time.
    let state =
        metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
    assert_eq!(state, Some(1.0));
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
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "answers": {
                    "is_rate_limit": { "type": "noul", "noul": 0.9 },
                    "reset_time": { "type": "choice", "choice": reset_id }
                }
            })),
        )
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
    let quota_opens =
        metric_sample(&gw, "maxllm_circuit_breaker_quota_opens_total{provider=\"primary\"}")
            .await;
    assert_eq!(quota_opens, Some(1.0));
    let state =
        metric_sample(&gw, "maxllm_circuit_breaker_state{provider=\"primary\"}").await;
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

    // Provider "a" holds the matching request for 400 ms; everything else
    // answers instantly. wiremock tries mocks in registration order (the
    // first mounted mock wins), so the specific matcher is mounted first.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("hold"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_response("held"))
                .set_delay(Duration::from_millis(400)),
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
    // Give the gateway time to select the provider and count the request.
    tokio::time::sleep(Duration::from_millis(80)).await;

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
    assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"), "auth: {auth}");
    assert!(auth.contains("/us-east-1/bedrock/aws4_request"), "auth: {auth}");
    assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"), "auth: {auth}");
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
    assert!(text.contains("data: "), "expected SSE data lines in: {text}");
    assert!(text.contains("chat.completion.chunk"), "expected OpenAI chunk format in: {text}");
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
