# MaxLLM Architecture Documentation

## Overview

MaxLLM is a high-performance AI gateway built on Pingora (Cloudflare's reverse proxy framework) that routes OpenAI-compatible chat/completion requests across multiple LLM providers. It provides request translation, fallback routing, plugin system for middleware, admin API for key/budget management, and comprehensive metrics.

**Key Technologies:**
- **Pingora** (0.8.0) -- Async I/O foundation with thread-per-core model
- **Rust** -- Type-safe, memory-safe language; all crates in workspace use Rust 2021 edition
- **Serde** -- Serialization/deserialization for JSON and TOML
- **AHash** -- Fast, deterministic hashing for provider lookup tables

---

## Project Structure

```
maxllm/
├── Cargo.toml                    # Workspace root (5 crates)
├── maxllm.toml                   # Gateway configuration (TOML)
├── Makefile                      # Build/test/run automation
├── crates/
│   ├── maxllm-config/            # Configuration parsing & validation
│   ├── maxllm-plugin/            # Plugin system & builtin plugins
│   ├── maxllm-translate/         # Provider format translation layer
│   ├── maxllm-gateway/           # Main gateway (Pingora integration)
│   └── maxllm-admin/             # Virtual key/budget/cost management
└── target/                       # Build artifacts
```

---

## Crate Details

### 1. maxllm-config (Configuration Layer)

**Purpose:** Parse and validate TOML configuration files with environment variable expansion.

**Key Types:**
- `Config` -- Top-level configuration struct
- `ServerConfig` -- Server binding, thread count, TCP settings
- `ProviderConfig` -- Provider endpoint definition with circuit breaker settings
- `RouteConfig` -- Request path to provider(s) mapping with fallback list
- `PluginConfig` -- Named plugin definitions
- `ProviderKind` -- Enum: OpenAI, Anthropic, Gemini, AzureOpenai, Bedrock, Groq, Together, Fireworks, DeepInfra, Mistral, XAI, DeepSeek, Ollama, Cohere, OpenaiCompat
- `RoutingStrategy` -- Enum: Fallback, Weighted, LatencyBased, LeastConnections, CostBased, RoundRobin
- `EndpointType` -- Enum: ChatCompletions, Embeddings, ImageGenerations, AudioTranscriptions, AudioSpeech, Moderations, Completions, Rerank
- `AuthConfig` -- API key list
- `RateLimitConfig` -- Global rate limit (RPM and TPM)
- `MetricsConfig` -- Prometheus metrics enablement
- `ModelCostConfig` -- Override pricing for specific models
- `AdminConfig` -- Master key and admin API enablement

**Key Functions:**
- `Config::from_file(path)` -- Load from TOML file with env var expansion (`${VAR_NAME}`)
- `Config::parse(s)` -- Parse from string
- `expand_env_vars(input)` -- Replace `${VAR}` patterns with environment values
- Validation: ensures all route providers exist, all referenced plugins exist

**Example Configuration:**
```toml
[server]
listen = "0.0.0.0:8080"
threads = 4

[providers.openai]
kind = "openai"
base_url = "https://api.openai.com"
api_key = "${OPENAI_API_KEY}"
default_model = "gpt-4o"

[[routes]]
path = "/v1/chat/completions"
provider = "openai"
fallback = ["anthropic"]
strategy = "fallback"
timeout_secs = 120
```

---

### 2. maxllm-plugin (Plugin System)

**Purpose:** Extensible middleware system for request/response filtering, authentication, rate limiting, caching, etc.

**Core Trait:**
```rust
pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    async fn on_request(&self, session, ctx) -> Result<RequestAction>;
    async fn on_upstream_request(&self, session, upstream_request, ctx) -> Result<()>;
    async fn on_response(&self, session, upstream_response, ctx) -> Result<()>;
    fn on_response_body(&self, session, body, end_of_stream, ctx) -> Result<()>;
    async fn on_logging(&self, session, error, ctx);
}
```

**RequestAction:**
- `Continue` -- Proceed to next plugin
- `Respond(HttpResponse)` -- Short-circuit with custom response

**Plugin Context (PluginCtx):**
- `route_path` -- Matched route
- `provider_name` -- Selected provider
- `model` -- Model from request
- `client_id` -- Set by auth plugins
- `request_id` -- Set by request_id plugin
- `client_ip` -- Client IP
- `extensions` -- HashMap for plugin-to-plugin data

**PluginChain:**
- Ordered list of plugins executed sequentially
- `run_request()` -- Pre-route request filtering
- `run_upstream_request()` -- Modify upstream request
- `run_response()` -- Modify upstream response
- `run_response_body()` -- Transform streaming/chunked body
- `run_logging()` -- Post-request logging

**Builtin Plugins (10 total):**

| # | Plugin | Description | Key Config |
|---|--------|-------------|------------|
| 1 | **request_id** | Generates unique request IDs (UUID v7) | `header_name` (default: "X-Request-Id") |
| 2 | **key_auth** | Bearer token authentication | `header`, `strip_prefix`, `keys`, `hide_credentials` |
| 3 | **rate_limit** | Sliding-window rate limiting | `requests_per_minute`, `key` (client_ip\|client_id\|header_name) |
| 4 | **cors** | CORS header handling | `allow_origin`, `allow_methods`, `allow_headers`, `max_age` |
| 5 | **ip_restriction** | IP allow/deny lists | `type` (allow\|deny), `ip_list` (CIDR ranges) |
| 6 | **cache** | Response caching (in-memory LRU) | `max_entries`, `default_ttl_secs` |
| 7 | **webhook** | Async webhook notifications | `target`, `include_body`, `event_types` |
| 8 | **pii_filter** | PII detection and blocking | `patterns` (email, ssn, credit_card, phone), `action` (block\|redact) |
| 9 | **keyword_block** | Keyword/prompt injection filtering | `keywords`, `case_sensitive` |
| 10 | **max_size** | Request/response size limits | `max_body_bytes`, `max_tokens` |

**Plugin Factory:**
```rust
pub fn create_plugin(name: &str, config: &toml::Table) -> Result<Arc<dyn Plugin>>
```

---

### 3. maxllm-translate (Translation Layer)

**Purpose:** Translate between OpenAI canonical format and provider-specific formats.

**Core Traits:**

```rust
pub trait ProviderTranslator: Send + Sync {
    fn name(&self) -> &str;
    fn translate_request(&self, body: &[u8], model_override: Option<&str>) -> Result<TranslatedRequest>;
    fn translate_response(&self, body: &[u8]) -> Result<Vec<u8>>;
    fn streaming_translator(&self) -> Box<dyn StreamTranslator>;
    fn upstream_path(&self) -> &str;
    fn upstream_headers(&self, api_key: &str) -> Vec<(String, String)>;
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait StreamTranslator: Send {
    fn process_chunk(&mut self, data: &[u8], end_of_stream: bool) -> Vec<u8>;
}
```

**Supported Providers (14 translators):**

**Bedrock SigV4 signing.** The Bedrock Runtime API authenticates with AWS
Signature Version 4, whose signature covers the SHA-256 of the translated
request payload. The exact bytes must therefore be known before the upstream
request headers are written — but Pingora's proxy pipeline streams body
chunks after the headers are already on the wire. The gateway handles this
in `request_filter`: it reads the full client body, translates it, signs it
(`maxllm_translate::aws_sigv4`), and forwards it over a separate HTTP
client, then translates the response back before writing it to the client.
Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; the
region from the provider's `region` key, then `AWS_REGION`, then
`us-east-1`. The model travels in the URL path (`/model/{id}/invoke` or
`/model/{id}/invoke-with-response-stream`), so the translator strips the
body's `model` field.

| Provider | Module | Key Features |
|----------|--------|--------------|
| OpenAI | `openai.rs` | Pass-through (canonical format) |
| Anthropic | `anthropic.rs`, `anthropic_stream.rs` | Maps to Messages API (`/v1/messages`) |
| Gemini | `gemini.rs`, `gemini_stream.rs` | Maps to Generative Language API, query-param auth |
| Azure OpenAI | `azure_openai.rs` | Custom deployment & API version |
| Bedrock | `bedrock.rs`, `aws_sigv4.rs` | AWS service format, model-in-URL, SigV4-signed (gateway) |
| Groq | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| Together | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| Fireworks | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| Mistral | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| XAI | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| DeepInfra | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| DeepSeek | `openai_compat.rs` | OpenAI-compatible (Bearer auth) |
| Ollama | `openai_compat.rs` | OpenAI-compatible (no auth) |
| Cohere | `cohere.rs`, `cohere_stream.rs` | Cohere-specific format |

**Canonical Format (OpenAI):**
```rust
pub struct OpenAIChatRequest {
    pub model: String,
    pub messages: Vec<OpenAIMessage>,
    pub max_tokens: Option<u64>,
    pub temperature: Option<f64>,
    pub stream: Option<bool>,
    pub tools: Option<Vec<OpenAITool>>,
    pub response_format: Option<Value>,
    pub extra: Map<String, Value>,
}
```

**Translation Flow:**
1. Request arrives in OpenAI format
2. `translate_request()` converts to provider format + sets headers
3. Gateway forwards to provider
4. `translate_response()` converts response back to OpenAI format
5. For streaming: `streaming_translator()` processes SSE chunks incrementally

---

### 4. maxllm-gateway (Main Gateway)

**Purpose:** Pingora-based HTTP proxy implementing request routing, fallback, plugins, and cost tracking.

**Main Components:**

#### AiGateway (ProxyHttp Implementation)
```rust
pub struct AiGateway {
    pub providers: AHashMap<String, Arc<ProviderState>>,
    pub routes: Vec<RouteConfig>,
    pub global_chain: PluginChain,
    pub route_chains: Vec<PluginChain>,
    pub model_aliases: HashMap<String, String>,
    pub cost_calculator: Arc<CostCalculator>,
    pub budget_enforcer: Option<Arc<BudgetEnforcer>>,
}
```

#### ProviderState
```rust
pub struct ProviderState {
    pub kind: ProviderKind,
    pub translator: Box<dyn ProviderTranslator>,
    pub circuit_breaker: CircuitBreaker,
    pub api_key: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub sni: String,
    pub weight: u32,
    pub tags: Vec<String>,   // reserved — no routing rule consumes these yet
    pub default_model: Option<String>,
}
```

#### RequestCtx (Per-Request Context)
```rust
pub struct RequestCtx {
    pub route_index: Option<usize>,
    pub provider_name: String,
    pub is_streaming: bool,
    pub stream_translator: Mutex<Option<Box<dyn StreamTranslator>>>,
    pub request_body_buf: Vec<u8>,
    pub response_body_buf: Vec<u8>,
    pub request_start: Instant,
    pub upstream_send_time: Option<Instant>,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub model: String,
    pub fallback_used: bool,
    pub cost_usd: f64,
    pub plugin_ctx: PluginCtx,
}
```

**Pingora Lifecycle Hooks:**

| Hook | Purpose |
|------|---------|
| `request_filter` | Auth, routing, plugin chains, provider selection |
| `upstream_peer` | Resolve upstream host:port + TLS |
| `upstream_request_filter` | Set upstream path, headers, strip client auth |
| `request_body_filter` | Translated: OpenAI -> provider format. Native: extract metadata, forward as-is. Passthrough: skip. |
| `response_filter` | Circuit breaker, add gateway headers, timing headers |
| `upstream_response_body_filter` | Translated: provider -> OpenAI format. Native: extract usage, forward as-is. Passthrough: skip. |
| `logging` | Metrics, cost calculation, structured logging |

**Circuit Breaker:**
```rust
pub struct CircuitBreaker {
    failures: AtomicU32,
    last_failure_at: AtomicU64,
    max_fails: u32,           // Default 3
    fail_timeout_ms: u64,     // Default 60 seconds
}
```
- **Closed:** failures < max_fails (healthy)
- **Open:** failures >= max_fails AND timeout not elapsed (skip provider)
- **Half-Open:** failures >= max_fails BUT timeout elapsed (retry on next request)

**Routing Strategies:**

| Strategy | Description |
|----------|-------------|
| Fallback | Try primary first; on circuit breaker open, use fallbacks in order |
| Weighted | Proportional distribution based on provider weight (default 100) |
| RoundRobin | Even distribution using atomic counter |
| LeastConnections | Provider with the fewest requests currently in flight (process-wide counter) |
| LatencyBased | Provider with the lowest observed upstream latency (EWMA over successful attempts) |
| CostBased | Cheapest provider by blended USD price per 1M tokens (model price table) |

**Response Headers Added by Gateway:**

| Header | Description |
|--------|-------------|
| `X-MaxLLM-Provider` | Which provider handled the request |
| `X-MaxLLM-Fallback-From` | Original provider if fallback was used |
| `X-MaxLLM-Upstream-Ms` | Time waiting for upstream provider (TTFB) |
| `X-MaxLLM-Overhead-Ms` | Gateway processing time (total - upstream) |

**Prometheus Metrics:**
```rust
pub struct GatewayMetrics {
    pub requests_total: IntCounterVec,          // [provider, model, status]
    pub tokens_in_total: IntCounterVec,         // [provider, model]
    pub tokens_out_total: IntCounterVec,        // [provider, model]
    pub request_duration_seconds: HistogramVec, // [provider]
    pub active_requests: IntGauge,
    pub fallbacks_total: IntCounterVec,         // [from_provider, to_provider]
}
```
Histogram buckets: `[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]` seconds.

---

### 5. maxllm-admin (Key & Budget Management)

**Purpose:** Manage virtual API keys, teams, spend tracking, and budget enforcement.

The gateway enforces the per-key limits it stores: a virtual key is
authenticated (active / unexpired / within budget), its model allow-list is
checked before the upstream is contacted, and its RPM / TPM / concurrency
limits are enforced with 60-second sliding windows.

**Core Models:**

#### VirtualKey
```rust
pub struct VirtualKey {
    pub id: String,
    pub key_hash: String,              // SHA-256 hex
    pub key_prefix: String,            // First 12 chars for display
    pub name: String,
    pub team_id: Option<String>,
    pub allowed_models: Vec<String>,   // Empty = all
    pub max_budget_usd: Option<f64>,
    pub budget_reset_days: Option<u32>,// Rolling budget window
    pub budget_spent_usd: f64,
    pub rpm_limit: Option<u32>,       // Requests per minute (enforced)
    pub tpm_limit: Option<u64>,       // Tokens per minute (enforced)
    pub max_parallel_requests: Option<u32>, // In-flight limit (enforced)
    pub expires_at: Option<DateTime<Utc>>,
    pub is_active: bool,
    pub total_requests: u64,
    pub total_tokens_in: u64,
    pub total_tokens_out: u64,
    pub total_spend_usd: f64,
}
```

#### Team
```rust
pub struct Team {
    pub id: String,
    pub name: String,
    pub max_budget_usd: Option<f64>,
    pub budget_spent_usd: f64,
    pub members: Vec<String>,  // Key IDs
}
```

#### Cost Calculation
```rust
pub struct ModelCost {
    pub model_pattern: String,     // Exact or glob (e.g., "gpt-4o*")
    pub input_cost_per_1m: f64,    // USD per 1M tokens
    pub output_cost_per_1m: f64,   // USD per 1M tokens
}
```

**Storage Trait:**
```rust
pub trait AdminStore: Send + Sync {
    fn create_key(&self, key: VirtualKey) -> Result<()>;
    fn get_key_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>>;
    fn list_keys(&self, offset: usize, limit: usize) -> Result<Vec<VirtualKey>>;
    fn update_key(&self, key: VirtualKey) -> Result<()>;
    fn delete_key(&self, id: &str) -> Result<bool>;
    fn create_team(&self, team: Team) -> Result<()>;
    fn record_spend(&self, record: SpendRecord) -> Result<()>;
    fn get_spend_summary(&self, key_id: Option<&str>) -> Result<SpendReport>;
    // ... more methods
}
```

`InMemoryStore` is the default implementation (RwLock + HashMap). Swap in SQLite/Postgres for production via the trait.

**Admin API Endpoints** (master key required):

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/keys` | List keys |
| POST | `/admin/keys` | Create key (returns raw key once) |
| GET | `/admin/keys/{id}` | Get key details |
| PUT | `/admin/keys/{id}` | Update key |
| DELETE | `/admin/keys/{id}` | Revoke key |
| GET | `/admin/teams` | List teams |
| POST | `/admin/teams` | Create team |
| PUT | `/admin/teams/{id}` | Update team |
| POST | `/admin/teams/{id}/members` | Add key to team |
| GET | `/admin/spend` | Spend summary |
| GET | `/admin/spend/logs` | Spend records |

---

## Request Lifecycle

```
Client Request (OpenAI format)
    |
    v
[request_filter]
    |-- Virtual-key authentication (sk-maxllm-… tokens)
    |     '-- active / unexpired / within budget -> 401 / 402 otherwise
    |-- Global PluginChain.run_request()
    |     |-- request_id: Generate UUID v7
    |     |-- key_auth: Validate Bearer token (defers to virtual-key auth)
    |     '-- Other global plugins
    |-- Route matching (by path prefix)
    |     '-- No match? -> 404
    |-- Route PluginChain.run_request()
    |     '-- rate_limit, cors, etc.
    '-- ProviderSelector.select()
          |-- Build candidates: primary + fallbacks
          |-- Filter out open circuit breakers
          '-- Apply strategy (Fallback/Weighted/RoundRobin/...)
    |
    v
[upstream_peer]
    '-- Resolve host:port + TLS from ProviderState
    |
    v
[upstream_request_filter]
    |-- Set upstream path (provider-specific)
    |-- Strip client Authorization header
    |-- Set provider auth headers (Bearer / query param)
    |-- Set Host header
    |-- Record upstream_send_time (for overhead calc)
    '-- Run plugin chains on upstream request
    |
    v
[request_body_filter] (4 modes based on endpoint_type)
    |-- Translated: buffer body, extract model, translate OpenAI -> provider
    |     '-- virtual-key model allow-list enforced here (403)
    |-- Responses: native upstream -> forward the body as-is when the provider
    |     is configured native_responses (the default whenever the upstream
    |     serves /responses); otherwise translate Responses -> chat -> provider
    |-- Native: buffer body, extract metadata from native format, forward as-is
    '-- Passthrough: raw forward (buffered only to check the model allow-list)
    |
    v
  +-----------------------+
  | Upstream Provider     |
  | (OpenAI, Anthropic,   |
  |  Gemini, Ollama, ...) |
  +-----------------------+
    |
    v
[response_filter]
    |-- Record circuit breaker state (success/failure)
    |-- Remove upstream Content-Length (body size changes)
    |-- Add X-MaxLLM-Provider header
    |-- Add X-MaxLLM-Upstream-Ms / X-MaxLLM-Overhead-Ms
    '-- Run plugin chains on response
    |
    v
[upstream_response_body_filter] (4 modes based on endpoint_type)
    |-- Translated: streaming via StreamTranslator, non-streaming via translate_response()
    |-- Responses: forward the upstream's own Responses body (native), or
    |     translate provider chat -> Responses JSON/SSE (fallback path only)
    |-- Native: extract usage from native format, forward body as-is
    |-- Passthrough: skip translation (run plugin chains only)
    '-- Run plugin chains on response body
    |
    v
[logging]
    |-- Record Prometheus metrics
    |-- Calculate cost (CostCalculator)
    |-- Run plugin logging chains
    '-- Structured log: provider, model, tokens, cost, latency
    |
    v
Client Response (translated: OpenAI format / native: provider format / passthrough: raw)
```

---

## Data Flow Diagram

```
+--------------+
|   Client     |
+------+-------+
       | POST /v1/chat/completions
       | Authorization: Bearer sk-maxllm-...
       | {"model": "gpt-4o", "messages": [...]}
       v
+----------------------------------------------+
| maxllm-gateway (Pingora ProxyHttp)           |
|----------------------------------------------|
| Plugins: auth -> rate_limit -> cors          |
| Route:   /v1/chat/completions -> "openai"    |
| Select:  openai (primary, CB closed)         |
+------+---------------------------------------+
       |
       v
+----------------------------------------------+
| maxllm-translate                             |
|----------------------------------------------|
| OpenAI -> pass-through (same format)         |
| Anthropic -> Messages API format             |
| Gemini -> generateContent format             |
| Cohere -> Cohere chat format                 |
+------+---------------------------------------+
       |
       v
+----------------------------------------------+
| Upstream Provider (e.g., api.openai.com)     |
|----------------------------------------------|
| Provider-specific request format             |
| Provider-specific response format            |
+------+---------------------------------------+
       |
       v
+----------------------------------------------+
| maxllm-translate (reverse)                   |
|----------------------------------------------|
| Provider format -> OpenAI format             |
| Extract token usage                          |
+------+---------------------------------------+
       |
       v
+----------------------------------------------+
| maxllm-admin                                 |
|----------------------------------------------|
| CostCalculator: tokens -> USD                |
| BudgetEnforcer: update spend, check limits   |
| SpendRecord: log to store                    |
+------+---------------------------------------+
       |
       v
+----------------------------------------------+
| Prometheus Metrics                           |
|----------------------------------------------|
| requests_total, tokens_total, latency, etc.  |
+------+---------------------------------------+
       |
       v
+--------------+
|   Client     | <- OpenAI format response
+--------------+    + X-MaxLLM-Provider
                    + X-MaxLLM-Upstream-Ms
                    + X-MaxLLM-Overhead-Ms
```

---

## Key Design Patterns

### 1. Trait-Based Abstraction
- `Plugin` -- Pluggable middleware (14 built-in plugins in `maxllm-plugin`)
- `ProviderTranslator` -- Format translation (10 translator implementations covering the 18 provider kinds)
- `StreamTranslator` -- Streaming response handling
- `AdminStore` -- Persistence abstraction (swap InMemory for Postgres/SQLite)

### 2. Arc + Atomics for Shared State
- `Arc<ProviderState>` -- Shared provider config across requests
- `Arc<PluginChain>` -- Shared plugin chains
- `AtomicU32/U64` -- Lock-free circuit breaker (no mutex contention)
- `Mutex<Option<Box<dyn StreamTranslator>>>` -- Per-request streaming state

### 3. Per-Request Context Pattern
- `RequestCtx` carries state through all Pingora lifecycle hooks
- Plugins read/write via mutable reference through `PluginCtx`
- Timing, buffers, and token counts accumulated across hooks

### 4. Environment Variable Expansion
- Config values support `${VAR_NAME}` syntax
- Expanded at parse time via regex replacement
- Keeps secrets out of config files

### 5. Canonical Format Translation
- All clients speak OpenAI format
- Gateway translates to/from the 18 provider kinds transparently
  (openai, anthropic, gemini, azure_openai, bedrock, groq, together,
  fireworks, deepinfra, mistral, xai, deepseek, ollama, cohere,
  openai_compat, opencode, commandcode, open_design)
- Streaming translation handles SSE chunk-by-chunk

---

## Performance Characteristics

1. **Lock-Free Circuit Breaker** -- AtomicU32/U64, no mutex contention on hot path
2. **AHashMap** -- Faster than std HashMap for provider/route lookups
3. **Streaming SSE** -- Chunks translated incrementally, not buffered
4. **TCP Reuse** -- `tcp_reuseport` for kernel-level load balancing
5. **Thread-Per-Core** -- Pingora's work-stealing scheduler
6. **Zero-Copy Where Possible** -- Bytes type avoids unnecessary copies
7. **Lazy Static Metrics** -- Prometheus collectors initialized once

---

## Security Model

| Layer | Mechanism | Description |
|-------|-----------|-------------|
| Auth | `key_auth` plugin | Bearer token validation against whitelist |
| Auth | Virtual keys | SHA-256 hashed, expiry, model allowlist |
| Auth | Master key | Admin API protected by separate master key |
| Budget | `BudgetEnforcer` | Pre-request validation prevents overspend |
| Network | `ip_restriction` | CIDR-based allow/deny lists |
| Content | `pii_filter` | Regex-based PII detection (email, SSN, etc.) |
| Content | `keyword_block` | Prompt injection / keyword filtering |
| Content | `max_size` | Request body and token limits |
| Transport | Client auth stripping | Client's auth header never forwarded to providers |

---

## Integration Tests

**Location:** `crates/maxllm-gateway/tests/integration.rs`

**Test Harness (`TestGateway`):**
- Spawns the actual gateway binary as a child process
- Picks a random port via `portpicker`
- Writes a temp config file with `{PORT}` and `{MOCK_URL}` placeholders replaced
- Polls `/health` until the gateway is ready
- Kills the child process on `Drop`

**Mock Providers:**
- Uses `wiremock` crate to stub upstream responses
- Validates request translation (headers, path, body format)
- Tests response translation back to OpenAI format

**Test Categories (127 tests: 123 mock-based, 4 live gated with `#[ignore]`):**

| Category | Count | Description |
|----------|-------|-------------|
| Health & routing | 3 | Health check, unknown route 404, `GET /v1/models` |
| Authentication (static) | 6 | Missing/wrong key 401, valid auth, auth stripping, static key alongside virtual keys, static key bypasses model allowlist |
| Virtual keys | 9 | Auth + spend, revoked 401, budget 402, RPM/TPM limits, per-key model allowlist (incl. glob), legacy `[auth]`/`[rate_limit]` |
| Proxy & translation | 7 | OpenAI passthrough, Anthropic/Gemini translation + error normalisation, model alias, embeddings |
| Responses API | 7 | chat-only translation (the fallback path: native `native_responses` forwarding is the norm), tool-choice nesting, images, SSE, error/mid-stream failure surfacing |
| Native & passthrough | 6 | Responses passthrough (streaming/non-streaming), function-tool flattening, large-body usage, chat→native fallback |
| OpenCode (zen) | 16 | session header injection/affinity, body normalisation, error envelopes, UA handling, models list, streaming usage |
| Command Code | 10 | vendor path defaults, native Responses, Anthropic Messages wire + translation, bearer auth, models list |
| OpenDesign | 3 | native Responses passthrough, link chat path, `tool_choice: "any"` rewrite |
| Headers & metrics | 6 | X-Request-Id, X-MaxLLM-Provider, CORS, request/token/active/inflight metrics |
| Bedrock SigV4 | 3 | signed request + model routing, error forwarding, streaming translation |
| Routing strategies | 6 | latency, cost, least-connections, auto-model rotation (+session affinity, pool filtering) |
| Resilience & failover | 15 | 500 passthrough, fallback on HTTP error / connect / DNS failure, 400-model rules, failover budget, same-provider retries, all-providers-fail |
| Rate-limit learning | 4 | Retry-After header, body reset instant, Laya classification, non-rate-limit body |
| Compaction | 4 | truncate, sliding window, threshold, skip header |
| Concurrency | 1 | concurrent requests |
| Live providers | 4 | Ollama non-streaming/streaming, Gemini, Gemini system prompt (gated with `#[ignore]`) |

---

## Build & Run

```bash
make build           # cargo build --release
make run             # Build + run gateway
make run-debug       # Debug build + RUST_LOG=debug
make test            # Unit tests (workspace)
make test-integration # Unit + mock integration tests
make test-live       # Real provider tests (requires Ollama + GEMINI_API_KEY)
make test-all        # Everything
make fmt             # cargo fmt
make clippy          # cargo clippy
make clean           # cargo clean
```

---

## Future Roadmap

- **Phase 9: Admin UI** -- Web dashboard for key/spend management (config
  hot-reload itself is already implemented via the config file watcher)
- **Phase 10: Advanced Protocols** -- MCP gateway, A2A, batch API
- **Postgres backend** -- PostgreSQL implementation of `AdminStore` (SQLite is implemented)
