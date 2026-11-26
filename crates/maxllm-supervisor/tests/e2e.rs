// End-to-end tests for the two-layer supervisor.
//
// These spawn the real `maxllm-supervisor` and `maxllm-server` binaries against
// a wiremock upstream. They prove the four properties the layer split exists
// for:
//   1. a layer-2 upgrade never drops an in-flight request and new requests
//      immediately use the new generation;
//   2. a URL path appears in the layer-1 route table without restarting the
//      public listener;
//   3. a new provider and its route become usable through hot reload;
//   4. a brand-new backend (e.g. a future voice/image worker) can be started
//      from config and upgraded independently of the default backend.

use reqwest::Client;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

const OPENAI_BODY: &str = r#"{
  "id": "chatcmpl-mock",
  "object": "chat.completion",
  "created": 1,
  "model": "mock-model",
  "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello from mock"}, "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 3, "completion_tokens": 3, "total_tokens": 6}
}"#;

fn worker_binary() -> PathBuf {
    // target/debug/deps/e2e-<hash> -> target/debug/maxllm-server
    let exe = std::env::current_exe().expect("current exe");
    let debug_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("target/debug")
        .to_path_buf();
    let candidate = debug_dir.join("maxllm-server");
    assert!(
        candidate.exists(),
        "maxllm-server not found at {}; run `cargo build -p maxllm-gateway` first",
        candidate.display()
    );
    candidate
}

struct Harness {
    child: Child,
    public: String,
    control: String,
    dir: tempfile::TempDir,
    config: PathBuf,
}

impl Harness {
    async fn start(mock_url: &str, default_build_id: Option<&str>) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = dir.path().join("maxllm.toml");
        let sock = dir.path().join("sock");
        let public_port = portpicker::pick_unused_port().expect("public port");
        let mut control_port = portpicker::pick_unused_port().expect("control port");
        while control_port == public_port {
            control_port = portpicker::pick_unused_port().expect("control port");
        }
        let backend_env = default_build_id
            .map(|v| {
                format!("\n[supervisor.backends.default]\nenv = {{ MAXLLM_BUILD_ID = \"{v}\" }}\n")
            })
            .unwrap_or_default();
        let cfg = format!(
            r#"
[server]
listen = "127.0.0.1:{public_port}"
threads = 1
grace_period_secs = 1

[providers.mock]
kind = "openai"
base_url = "{mock_url}"
api_key = "fake"
default_model = "mock-model"

[[routes]]
path = "/v1/chat/completions"
provider = "mock"

[supervisor]
control_listen = "127.0.0.1:{control_port}"
worker_binary = "{worker}"
worker_config = "{config_path}"
socket_dir = "{sock}"
health_timeout_secs = 10
drain_timeout_secs = 8
{backend_env}
"#,
            worker = worker_binary().display(),
            config_path = config.display(),
            sock = sock.display(),
        );
        std::fs::create_dir_all(&sock).expect("sock dir");
        std::fs::write(&config, cfg).expect("write config");

        let child = Command::new(env!("CARGO_BIN_EXE_maxllm-supervisor"))
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "warn")
            .env_remove("HTTPS_PROXY")
            .env_remove("HTTP_PROXY")
            .env_remove("https_proxy")
            .env_remove("http_proxy")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn supervisor");

        let public = format!("http://127.0.0.1:{public_port}");
        let control = format!("http://127.0.0.1:{control_port}");
        let h = Self {
            child,
            public,
            control,
            dir,
            config,
        };
        h.wait_ready().await;
        h
    }

    async fn wait_ready(&self) {
        let client = Client::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("supervisor did not become ready");
            }
            if let Ok(resp) = client.get(format!("{}/healthz", self.control)).send().await {
                if resp.status().is_success() {
                    if let Ok(resp) = client.get(format!("{}/health", self.public)).send().await {
                        if resp.status().is_success() {
                            return;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn status(&self) -> Value {
        Client::new()
            .get(format!("{}/status", self.control))
            .send()
            .await
            .expect("status")
            .json()
            .await
            .expect("status json")
    }

    async fn routes(&self) -> Value {
        Client::new()
            .get(format!("{}/routes", self.control))
            .send()
            .await
            .expect("routes")
            .json()
            .await
            .expect("routes json")
    }

    async fn upgrade(&self, backend: &str, build_id: &str) -> Value {
        let resp = Client::new()
            .post(format!("{}/upgrade", self.control))
            .json(&json!({"backend": backend, "env": {"MAXLLM_BUILD_ID": build_id}}))
            .send()
            .await
            .expect("upgrade request");
        let status = resp.status();
        let body: Value = resp.json().await.expect("upgrade json");
        assert!(status.is_success(), "upgrade failed: {body}");
        body
    }

    /// Atomically replace the config file, like an editor or config
    /// management tool would.
    fn rewrite_config(&self, mutate: impl FnOnce(&str) -> String) {
        let current = std::fs::read_to_string(&self.config).expect("read config");
        let next = mutate(&current);
        let tmp = self.dir.path().join("maxllm.toml.tmp");
        std::fs::write(&tmp, next).expect("write tmp config");
        std::fs::rename(&tmp, &self.config).expect("replace config");
    }

    async fn wait_for<F, Fut>(&self, what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if check().await {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for {what}");
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
}

impl Drop for Harness {
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

fn chat_body(content: &str) -> Value {
    json!({"model": "mock-model", "messages": [{"role": "user", "content": content}]})
}

async fn mount_upstream(mock: &MockServer) {
    Mock::given(method("POST"))
        .and(body_string_contains("SLEEP"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(3))
                .set_body_raw(OPENAI_BODY, "application/json"),
        )
        .mount(mock)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(OPENAI_BODY, "application/json"))
        .mount(mock)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_downtime_upgrade_keeps_inflight_request_on_old_generation() {
    let mock = MockServer::start().await;
    mount_upstream(&mock).await;
    let h = Harness::start(&mock.uri(), Some("v1")).await;

    let before = h.status().await;
    let supervisor_pid = before["pid"].as_u64().unwrap();
    assert_eq!(before["backends"]["default"]["active_build_id"], "v1");

    // A request the upstream answers after 3s. It must finish on generation 1
    // even though generation 2 becomes active mid-flight.
    let slow_client = client();
    let slow_url = format!("{}/v1/chat/completions", h.public);
    let slow = tokio::spawn(async move {
        slow_client
            .post(slow_url)
            .json(&chat_body("SLEEP please"))
            .send()
            .await
    });

    h.wait_for("the slow request to be in flight", || async {
        let s = h.status().await;
        s["backends"]["default"]["generations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["generation"] == 1 && g["inflight"].as_u64() == Some(1))
    })
    .await;

    let report = h.upgrade("default", "v2").await;
    assert_eq!(report["generation"], 2);

    // New requests use the new generation immediately, while the old one is
    // still draining the slow request.
    for i in 0..8 {
        let resp = client()
            .post(format!("{}/v1/chat/completions", h.public))
            .json(&chat_body(&format!("fast {i}")))
            .send()
            .await
            .expect("fast request");
        assert_eq!(resp.status(), 200, "request {i} failed during upgrade");
        assert_eq!(
            resp.headers()
                .get("x-maxllm-build-id")
                .and_then(|v| v.to_str().ok()),
            Some("v2"),
            "request {i} did not use the new generation"
        );
    }

    let slow = slow.await.expect("slow task").expect("slow request");
    assert_eq!(slow.status(), 200, "in-flight request was interrupted");
    assert_eq!(
        slow.headers()
            .get("x-maxllm-build-id")
            .and_then(|v| v.to_str().ok()),
        Some("v1"),
        "the in-flight request did not stay on the generation it started on"
    );
    assert!(slow.text().await.unwrap().contains("hello from mock"));

    h.wait_for("old generation to exit", || async {
        let s = h.status().await;
        s["backends"]["default"]["generations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["generation"] == 1 && g["state"] == "exited")
    })
    .await;
    let after = h.status().await;
    assert_eq!(after["pid"].as_u64().unwrap(), supervisor_pid);
    assert_eq!(after["backends"]["default"]["active_generation"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_reload_registers_new_route_and_provider_without_restart() {
    let mock = MockServer::start().await;
    mount_upstream(&mock).await;
    let h = Harness::start(&mock.uri(), None).await;
    let before = h.status().await;
    let supervisor_pid = before["pid"].as_u64().unwrap();
    let generation = before["backends"]["default"]["active_generation"].clone();
    let config_path = h.config.clone();

    // Not registered yet by either layer: no `[[routes]]` prefix covers it,
    // so the entry layer answers 404 itself. (`/v1/chat/completions/b` would
    // NOT work here: the existing `/v1/chat/completions` prefix matches it on
    // a segment boundary in both layers.)
    let resp = client()
        .post(format!("{}/v1/extra", h.public))
        .json(&chat_body("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    assert!(resp.text().await.unwrap().contains("route_not_registered"));

    // Add a provider first, then — 300ms later, inside the same quiet window
    // — the route that uses it. Both layers watch the file and coalesce on
    // the trailing edge, so the second write must still be applied (a plain
    // "ignore everything within 1s of the last reload" debounce would drop it
    // and `/v1/extra` would never be served).
    h.rewrite_config(|s| {
        s.replace(
            "[providers.mock]\nkind = \"openai\"",
            &format!(
                "[providers.mock2]\nkind = \"openai\"\nbase_url = \"{}\"\napi_key = \"fake\"\ndefault_model = \"mock-model\"\n\n[providers.mock]\nkind = \"openai\"",
                mock.uri()
            ),
        )
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.rewrite_config(|s| {
        s.replace(
            "[[routes]]\npath = \"/v1/chat/completions\"\nprovider = \"mock\"",
            "[[routes]]\npath = \"/v1/extra\"\nprovider = \"mock2\"\n\n[[routes]]\npath = \"/v1/chat/completions\"\nprovider = \"mock\"",
        )
    });

    h.wait_for("the new route in the layer-1 table", || async {
        h.routes()
            .await
            .get("routes")
            .and_then(|r| r.as_array())
            .map(|rs| rs.iter().any(|r| r["path"] == "/v1/extra"))
            .unwrap_or(false)
    })
    .await;

    // Wait for the worker's own watcher to pick up the new provider.
    h.wait_for("the new provider to answer", || async {
        matches!(
            client()
                .post(format!("{}/v1/extra", h.public))
                .json(&chat_body("hi"))
                .send()
                .await,
            Ok(r) if r.status() == 200
        )
    })
    .await;

    let resp = client()
        .post(format!("{}/v1/extra", h.public))
        .json(&chat_body("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-provider")
            .and_then(|v| v.to_str().ok()),
        Some("mock2")
    );

    // Control-managed route registration: add a path that the worker does
    // not know. The request now reaches the worker (which answers its own
    // 404) instead of being rejected by the entry layer, and the overlay is
    // persisted for the next supervisor start.
    let resp = client()
        .put(format!("{}/routes", h.control))
        .json(&json!({"routes": [{"path": "/v1/overlay-only", "backend": "default"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = client()
        .post(format!("{}/v1/overlay-only", h.public))
        .json(&chat_body("hi"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !body.contains("route_not_registered"),
        "the request should have been forwarded to layer 2, got {body}"
    );
    assert!(
        config_path
            .with_extension("toml.supervisor-routes.json")
            .exists()
            || PathBuf::from(format!("{}.supervisor-routes.json", config_path.display())).exists()
    );

    // An overlay naming a backend that does not exist is rejected.
    let resp = client()
        .put(format!("{}/routes", h.control))
        .json(&json!({"routes": [{"path": "/v1/overlay-only", "backend": "nope"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let after = h.status().await;
    assert_eq!(after["pid"].as_u64().unwrap(), supervisor_pid);
    assert_eq!(
        after["backends"]["default"]["active_generation"], generation,
        "a provider/route change must not restart the worker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_backend_can_be_added_and_upgraded_from_config() {
    let mock = MockServer::start().await;
    mount_upstream(&mock).await;
    let h = Harness::start(&mock.uri(), None).await;
    let supervisor_pid = h.status().await["pid"].as_u64().unwrap();

    // Add a second backend (the shape a future voice/image worker takes) plus
    // a dedicated route. The backend inherits `[supervisor].worker_config`.
    let worker = worker_binary();
    h.rewrite_config(|s| {
        let next = format!(
            "{s}\n[supervisor.backends.voice]\nbinary = \"{}\"\nenv = {{ MAXLLM_BUILD_ID = \"voice-v1\" }}\n\n[[supervisor.routes]]\npath = \"/v1/voice\"\nbackend = \"voice\"\n",
            worker.display()
        );
        next.replace(
            "[[routes]]\npath = \"/v1/chat/completions\"\nprovider = \"mock\"",
            "[[routes]]\npath = \"/v1/voice\"\nprovider = \"mock\"\n\n[[routes]]\npath = \"/v1/chat/completions\"\nprovider = \"mock\"",
        )
    });

    h.wait_for("the voice backend to start", || async {
        let s = h.status().await;
        s["backends"]["voice"]["active_build_id"] == "voice-v1"
    })
    .await;

    let resp = client()
        .post(format!("{}/v1/voice", h.public))
        .json(&chat_body("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-maxllm-build-id")
            .and_then(|v| v.to_str().ok()),
        Some("voice-v1")
    );

    // Bump the backend version in config. Only that backend is upgraded; the
    // default backend keeps its generation.
    let default_generation = h.status().await["backends"]["default"]["active_generation"].clone();
    h.rewrite_config(|s| s.replace("voice-v1", "voice-v2"));
    h.wait_for("the voice backend hot upgrade", || async {
        let s = h.status().await;
        s["backends"]["voice"]["active_build_id"] == "voice-v2"
    })
    .await;

    let resp = client()
        .post(format!("{}/v1/voice", h.public))
        .json(&chat_body("hi"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("x-maxllm-build-id")
            .and_then(|v| v.to_str().ok()),
        Some("voice-v2")
    );

    let after = h.status().await;
    assert_eq!(after["pid"].as_u64().unwrap(), supervisor_pid);
    assert_eq!(
        after["backends"]["default"]["active_generation"], default_generation,
        "upgrading voice must not touch the default backend"
    );
}
