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
use wiremock::matchers::{header, method, path};
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
        let port = portpicker::pick_unused_port().expect("no free port");
        let config_str = config_template
            .replace("{PORT}", &port.to_string())
            .replace("{MOCK_URL}", mock_url)
            .replace("{FALLBACK_URL}", fallback_url);

        let mut config_file = tempfile::NamedTempFile::new().expect("temp file");
        config_file
            .write_all(config_str.as_bytes())
            .expect("write config");

        let binary = env!("CARGO_BIN_EXE_maxllm-server");
        let child = Command::new(binary)
            .arg("--config")
            .arg(config_file.path())
            .env("RUST_LOG", "warn")
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

/// An upstream *error response* is not a connection failure — the request was
/// executed upstream (and may have been billed), so it is never re-sent.
#[tokio::test]
async fn test_upstream_error_response_is_not_retried() {
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

    // The upstream answered 500; the gateway must forward it untouched and
    // must NOT retry on the fallback.
    assert_eq!(resp.status(), 500);

    let received = fallback.received_requests().await.unwrap_or_default();
    assert!(
        received.is_empty(),
        "an upstream error response must never be retried on the fallback, got {} request(s)",
        received.len()
    );
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
