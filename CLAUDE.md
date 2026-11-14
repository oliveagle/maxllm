# MaxLLM

A high-performance AI gateway built from scratch on Cloudflare's [Pingora](https://github.com/cloudflare/pingora) framework. MaxLLM acts as a reverse proxy that accepts OpenAI-format requests and routes them to any supported LLM provider, translating request/response formats transparently.

## Architecture

MaxLLM is a **proxy, not a client**. It uses zero HTTP client libraries (no reqwest, no hyper client). Pingora handles all TCP/TLS/HTTP connections, connection pooling, and protocol negotiation. Our code only mutates headers and bodies as they flow through Pingora's proxy pipeline.

### Request Flow

```
Client (OpenAI SDK/curl)
  |
  v
Pingora accepts connection
  |
  v
request_filter:
  - Health/metrics/admin endpoints (bypass everything)
  - Extract client IP
  - Global plugin chain (request_id, key_auth)
  - Route matching (path prefix)
  - Route plugin chain (rate_limit, cors, cache, guardrails)
  - Model alias resolution
  - Provider selection (strategy + circuit breaker + fallback)
  |
  v
upstream_peer:
  - Construct HttpPeer (host, port, TLS, SNI)
  - Set timeouts (connect: 10s, read: 120s, write: 30s)
  |
  v
upstream_request_filter:
  - Set upstream path (e.g. /v1/messages for Anthropic)
  - Set provider headers (API key, version)
  - Set Host header
  - Run plugin chains
  |
  v
request_body_filter (3 modes):
  - Translated (default): buffer body, translate OpenAI -> provider format,
    extract model, resolve aliases, detect streaming, run pre-call guardrails
  - Native: buffer body, extract metadata from native format (model, streaming),
    run guardrails on native content, forward body as-is (no translation)
  - Passthrough: skip entirely — zero buffering, parsing, or translation
  |
  v
[Pingora proxies to upstream provider over TLS]
  |
  v
response_filter:
  - Circuit breaker: record success/failure
  - Add X-MaxLLM-Provider header
  - Run plugin chains (CORS headers, etc.)
  |
  v
upstream_response_body_filter (3 modes):
  - Translated: streaming via StreamTranslator (provider SSE -> OpenAI SSE),
    non-streaming: buffer, translate (provider -> OpenAI), extract usage, run post-call guardrails
  - Native: extract usage from native format, run post-call guardrails, forward body as-is
  - Passthrough: skip translation — run plugin chains only
  |
  v
logging:
  - Prometheus metrics (latency, tokens, fallbacks)
  - Cost calculation (model cost map)
  - Plugin logging chains (webhook, structured logging)
  - Structured tracing log
  |
  v
Client receives response
```

### Crate Structure

```
maxllm/
  Cargo.toml              # Workspace root (6 crates)
  maxllm.toml             # Gateway configuration
  crates/
    maxllm-config/        # TOML config parsing, env var expansion, validation
    maxllm-plugin/        # Plugin trait, chain executor, 14 built-in plugins
    maxllm-translate/     # Provider translation (OpenAI <-> 17 providers)
    maxllm-admin/         # Virtual keys, teams, cost tracking, budget enforcement, admin API
    maxllm-gateway/       # Main binary: Pingora ProxyHttp implementation + routing
```

**Dependency flow:** `maxllm-gateway` depends on all four crates. `maxllm-plugin` and `maxllm-translate` are independent of each other. `maxllm-config` and `maxllm-admin` have no internal dependencies.

### Key Design Decisions

- **Three endpoint modes**: Translated routes accept OpenAI-format and translate to/from providers. Native routes accept provider-native format (Anthropic Messages API, Gemini, etc.) with metadata extraction but no body translation. Passthrough routes proxy raw bytes with zero processing.
- **Proxy, not client**: Body translation happens in Pingora's `request_body_filter` and `upstream_response_body_filter` hooks. We never call `read_request_body()` in `request_filter` — doing so consumes the body stream and causes Pingora to send headers with end-of-stream, preventing body forwarding.
- **Plugin system over hardcoded middleware**: Auth, rate limiting, CORS, caching, guardrails, etc. are config-driven plugins, not compiled-in logic. Plugins hook into 5 lifecycle phases.
- **Lock-free circuit breaker**: Uses AtomicU32/AtomicU64 for per-provider failure tracking. No mutexes in the hot path.
- **StreamTranslator wrapped in Mutex**: Pingora requires `CTX: Send + Sync`. The `StreamTranslator` trait object is `Send` but not `Sync`, so it's wrapped in `Mutex<Option<Box<dyn StreamTranslator>>>`.

## Supported Providers (17)

| Provider | Kind | Format | Auth |
|----------|------|--------|------|
| OpenAI | `openai` | Passthrough | Bearer token |
| Anthropic | `anthropic` | Full translation | x-api-key header |
| Google Gemini | `gemini` | Full translation | API key query param |
| Azure OpenAI | `azure_openai` | Passthrough (different URL/auth) | api-key header |
| AWS Bedrock | `bedrock` | Anthropic format (SigV4 auth stub) | AWS SigV4 |
| Groq | `groq` | OpenAI-compatible | Bearer token |
| Together AI | `together` | OpenAI-compatible | Bearer token |
| Fireworks AI | `fireworks` | OpenAI-compatible | Bearer token |
| DeepInfra | `deepinfra` | OpenAI-compatible | Bearer token |
| Mistral AI | `mistral` | OpenAI-compatible | Bearer token |
| xAI (Grok) | `xai` | OpenAI-compatible | Bearer token |
| DeepSeek | `deepseek` | OpenAI-compatible | Bearer token |
| Ollama | `ollama` | OpenAI-compatible | None (local) |
| Cohere | `cohere` | Full translation | Bearer token |
| OpenCode (zen) | `opencode` | OpenAI-compatible + strict schema | Bearer token |
| Command Code | `commandcode` | OpenAI-compatible + native Responses + Anthropic Messages | Bearer token |
| OpenDesign | `open_design` | OpenAI-compatible + native Responses | Bearer token |
| Custom | `openai_compat` | OpenAI-compatible | Configurable |

### Command Code (`kind = "commandcode"`)

`kind = "commandcode"` talks to the vendor's **Provider API**
(`/provider/v1/*`) — the sanctioned path — which needs the paid Provider plan:
on the Go plan every call answers `403 upgrade_required`.

The vendor's other surface (`/alpha/*`) is the wire protocol of their own
`cmd` client: it is **CLI-only** ("This endpoint only serves CLI"), and
proxying a subscription through it violates the vendor's terms. So there is
exactly one way in — upgrade the plan. `/v1/responses/cmd` is then forwarded
byte-for-byte to `/provider/v1/responses`; the commented-out `cmd_claude`
entry in `maxllm.toml` adds the Anthropic Messages route.

## Plugin System

Plugins are defined in TOML and referenced by name:

```toml
global_plugins = ["request_id", "api_auth"]

[plugins.api_auth]
category = "key_auth"
keys = ["sk-maxllm-dev-key"]

[[routes]]
path = "/v1/chat/completions"
provider = "openai"
plugins = ["rate_limiter", "cors"]
```

### Built-in Plugins (14)

| Plugin | Category | Purpose |
|--------|----------|---------|
| key_auth | `key_auth` | Bearer token authentication |
| rate_limit | `rate_limit` | Sliding-window rate limiter (pingora-limits) |
| request_id | `request_id` | UUIDv7 request ID generation |
| cors | `cors` | CORS preflight + response headers |
| ip_restriction | `ip_restriction` | IP allow/deny lists with CIDR |
| cache | `cache` | In-memory LRU response cache with TTL |
| webhook | `webhook` | Structured JSON logging for observability |
| pii_filter | `pii_filter` | PII detection (email, SSN, credit card, phone, IP) |
| keyword_block | `keyword_block` | Keyword/phrase blocking for guardrails |
| max_size | `max_size` | Request body size and token limits |
| prompt_guard | `prompt_guard` | Prompt injection/jailbreak detection (6 built-in rules) |
| secret_scan | `secret_scan` | Secret/credential detection (9 built-in patterns) |
| regex_guard | `regex_guard` | Custom regex-based content rules |
| auto_compaction | `auto_compaction` | Context window compaction (truncate, sliding window, LLM summarize) |

### Adding a New Plugin

1. Create `crates/maxllm-plugin/src/builtin/my_plugin.rs`
2. Implement the `Plugin` trait (only override hooks you need)
3. Add to `builtin/mod.rs` and register in `factory.rs`
4. Configure in `maxllm.toml`

## Routing Strategies

| Strategy | Config Value | Description |
|----------|-------------|-------------|
| Fallback | `fallback` | Try primary, then fallbacks in order (default) |
| Weighted | `weighted` | Distribute traffic by provider weight |
| Round Robin | `round_robin` | Even distribution across providers |
| Least Connections | `least_connections` | Route to least busy provider |
| Latency Based | `latency_based` | Route to fastest provider (lowest observed upstream latency) |
| Cost Based | `cost_based` | Route to cheapest provider (blended USD price per 1M tokens) |

```toml
[[routes]]
path = "/v1/chat/completions"
provider = "openai"
fallback = ["anthropic", "groq"]
strategy = "weighted"
num_retries = 2
```

## Virtual Keys & Multi-Tenancy (maxllm-admin)

| Feature | Description |
|---------|-------------|
| Key generation | `POST /admin/keys` — generates `sk-maxllm-{uuid}` keys |
| Per-key budgets | USD budgets with auto-reset periods |
| Per-key rate limits | RPM/TPM limits per virtual key |
| Model access control | Restrict which models a key can access |
| Teams | Group keys into teams with team-level budgets |
| Cost tracking | Automatic cost calculation using model cost map |
| Spend reporting | `/admin/spend/report` with per-key/model/provider breakdown |
| Admin API | 9 REST endpoints under `/admin/` protected by master key |

### Default Model Pricing

Built-in cost map for 10+ model families (gpt-4o, claude-sonnet, gemini-flash, etc.) with override support in config.

## Provider Translation

| Provider | Request Translation | Response Translation | Streaming |
|----------|-------------------|---------------------|-----------|
| OpenAI | Passthrough | Passthrough | Passthrough |
| Anthropic | OpenAI -> Anthropic (system extraction, tool_choice mapping) | Anthropic -> OpenAI (content blocks, stop_reason, usage) | Anthropic SSE -> OpenAI SSE chunks |
| Gemini | OpenAI -> Gemini (contents/parts format, functionDeclarations) | Gemini -> OpenAI (candidates, usageMetadata) | Gemini SSE -> OpenAI SSE chunks |
| Cohere | OpenAI -> Cohere v2 (message format, tool mapping) | Cohere -> OpenAI (content blocks, token usage) | Cohere SSE -> OpenAI SSE chunks |
| Azure OpenAI | Passthrough (deployment URL + api-key header) | Passthrough | Passthrough |
| Bedrock | OpenAI -> Anthropic (reuses Anthropic translator) | Anthropic -> OpenAI | Anthropic SSE -> OpenAI SSE |
| OpenAI-compat | Passthrough | Passthrough | Passthrough |
| Command Code | Passthrough (chat route) or OpenAI -> Anthropic (messages route) | Passthrough or Anthropic -> OpenAI | Passthrough or Anthropic SSE -> OpenAI SSE |
| OpenDesign | Passthrough (chat route, `tool_choice: "any"` -> `"required"`) | Passthrough | Passthrough |

## Tool Schema Dialects (MUST)

A provider may validate `tools[].function.parameters` against its own JSON
Schema dialect. Moonshot/Kimi runs the strictest one: a `$ref` must be the
*only* keyword of its site, its target must carry a `type`, a `type` beside an
`anyOf`/`oneOf` is an error, and a reference cycle is rejected outright. The
shapes codex-cli sends (zod 4 emits `$defs.__schemaN` with `$ref` beside
siblings) therefore earn

```
400 {"error":{"code":"invalid_request_error","message":"Invalid request:
tools.function.parameters is not a valid moonshot flavored json schema,
details: <At path 'properties.show': detected infinite recursion without
termination condition>"}}
```

### Rule: a translation fault must never end the agent loop

An agent's turn is a loop: the client sends a request, the model asks for a
tool, the client runs it, sends the result back. A translation fault that
surfaces as a `4xx` **kills that loop** — the client has no turn to continue,
and every later request in the conversation is dead. So a translation fault is
never allowed to be the client's answer:

| layer | duty |
|-------|------|
| **prevent** | project the tool schemas into the provider's dialect *before* they leave the gateway (`maxllm-translate/src/moonshot.rs`) |
| **repair** | when an upstream still refuses the schemas, replay the request **once, against the same provider**, with degraded tool schemas — the tool names survive, so the tool-call surface and the turn continue |
| **pass through** | a `4xx` that names anything else is the client's own fault and must reach the client exactly as it arrived |

The repair only preempts the handling that would end the turn: a 400 the route
would answer by model rewrite or provider failover keeps that handling — the
request is retried anyway, and a schema rejection that survives the rewrite
reaches this deferral on the next attempt (one rewrite per provider, one
degradation per request).

The repair is bounded (one retry per request, `ctx.tool_schema_degraded`), is
counted (`maxllm_tool_schema_repair_retries_total{provider}`) and only ever
fires for a body that names the tool schemas (`tools.function.parameters` /
`moonshot flavored json schema`). A provider without the dialect keeps its body
byte-for-byte — the projection returns the original bytes when nothing needs
repairing, so prompt caches stay warm.

### The projection

`maxllm_translate::moonshot::normalize_tools` rewrites each tool schema:

* a `$ref` **beside** another keyword is inlined — the site's own keywords
  win over the referenced target's;
* a `$ref` target without a `type` gains the inferred one (`properties` ->
  `object`, `items` -> `array`, `enum`/`const` -> the branch's type);
* a `type` beside an `anyOf`/`oneOf` moves into the branches;
* a reference cycle is cut at a terminating object (`{"type":"object"}`);
* a pure, typed, cycle-free `$ref` is kept — it is legal in the dialect.

The last resort is `degrade_tool_schemas`: every `parameters` becomes
`{"type":"object"}`. The tool list, the tool names and the model's ability to
call them survive; only the argument contract is lost, which the model still
fills in from the prompt.

### Configuration

```toml
[providers.kimi]
kind = "openai_compat"
base_url = "https://api.kimi.com"
tool_schema = "moonshot"   # "moonshot" | "none"; unset = detect by host
```

Unset means the gateway decides by upstream host: the Moonshot family
(`api.moonshot.ai`, `api.moonshot.cn`, `api.kimi.com`, `moonshot.ai`,
`kimi.com`) gets the projection, every other provider keeps its body
untouched. `tool_schema = "none"` forces no projection, `"moonshot"` forces
it — use the knob for a Moonshot-compatible upstream on a custom host, or to
opt a self-hosted mirror out of the projection.

## Responses Contract Policy (MUST)

`/v1/responses` traffic takes exactly one of two paths:

| path | when | mechanism |
|------|------|-----------|
| **native** — `native_responses = true` | the upstream serves the Responses contract | the request is forwarded byte-for-byte to the provider's own `/responses` route and the upstream emits the Responses SSE |
| **translated** — the default for other kinds | the upstream has no Responses route | the gateway rewrites the body into `/chat/completions` and rebuilds the Responses stream from chat deltas |

**Rule: if the upstream supports Responses, the provider MUST be configured
native. Translation is a recorded fallback for upstreams that have no
Responses route — never a default you land on because a derived path 404s.**

Translation is lossy by construction: it only knows the fields it has been
taught, so `previous_response_id`, `store`, `include`/`encrypted_content`,
`text.verbosity`, `prompt_cache_key`, `service_tier`, reasoning summaries and
vendor extensions are dropped or re-derived, and tool-call fragments are
re-assembled by the gateway instead of the provider. Native forwarding keeps
the full contract, is cheaper, and removes a whole class of translation bugs.

### Procedure for every new or changed provider

1. **Probe the route.** `curl -s -o /dev/null -w '%{http_code}' -X POST
   "{base_url}{responses_path}" -H "Authorization: Bearer $KEY" -H
   "Content-Type: application/json" -d '{"model":"<model>","input":"ping"}'`.
   `404` (with a JSON "not found" body) = no route. `400` / `401` / `403` /
   `429` / `5xx` = **route exists** (the body, credentials or quota were
   rejected — not the path). Treat only a genuine 404 as "unsupported".
2. **Configure native.** `native_responses = true`. `responses_path` is derived
   from `upstream_path` (`…/chat/completions` → `…/responses`) when omitted; set
   it explicitly when the vendor's route differs (zhipu: `/api/v1/responses`,
   while the derived `/api/coding/paas/v4/responses` is a real 404).
3. **Prove the path is in use, without spending quota.** Send `{"model": …}`
   with no `input`: the gateway's translation path answers
   `400 invalid Responses request: responses request is missing 'input'` before anything leaves the
   process, while the native path returns the *upstream's* status/body. A
   second check is to point `responses_path` at a bogus path for one call — the
   upstream's own 404 (not the gateway's validation error) proves the route is
   being used.
4. **Record the exemption** when (and only when) step 1 finds no route on any
   path the vendor documents: keep `native_responses = false`, add a config
   comment `# no Responses route: 404 <path> @YYYY-MM-DD`, and add the provider
   to `EXEMPT` in `scripts/check_responses_policy.py` with the same reason.
   `kind = "anthropic" | "gemini" | "bedrock" | "cohere"` are exempt by kind
   (their upstreams speak a different wire protocol); embedding/rerank
   providers are not chat providers at all.
5. **Gate it.** `python3 scripts/check_responses_policy.py --config <toml>`
   (`make policy`; a CI job runs it on `maxllm.toml`) fails on any
   `openai`-shaped provider (including `openai_compat`, `deepseek`, `ollama`,
   `opencode`, `commandcode`) left on translation without an exemption.

### Kinds that are native by default

`opencode`, `commandcode` and `open_design` default to
`native_responses = true` — their upstreams serve the Responses contract on
their own route (`/zen/go/v1/responses`, `/provider/v1/responses` and
`/v1/responses` respectively), so the default is native and translation is
the exception. A `commandcode` provider
entry whose `upstream_path` points at the Anthropic Messages route
(`/provider/v1/messages` — the only route that serves the vendor's Claude
models) has no Responses sibling and is therefore translated by design; the
policy script recognises that shape and reports it `BY-KIND`.
   `--live` additionally posts a minimal Responses body to every native route
   and reports `404` as a hard failure (wrong `responses_path` — fix the path,
   do not disable native), `--only <substring>` narrows the run.

Do not: leave `native_responses` unset on an OpenAI-shaped provider (the
default is `false` for every kind except `opencode`), use translation to paper
over a wrong `responses_path`, or add a provider without having run step 1.

## Configuration

Config is TOML with `${ENV_VAR}` expansion. Key sections:

- `[server]` — listen address, worker threads
- `global_plugins` — plugin names for all requests
- `[plugins.*]` — plugin definitions with `category` + params
- `[providers.*]` — LLM provider configs (kind, base_url, api_key, weight, tags)
- `[[routes]]` — path-prefix routing with provider, fallback chain, strategy, per-route plugins
- `[model_aliases]` — map requested model names to actual models
- `[admin]` — master key and admin API toggle
- `[model_costs.*]` — override built-in model pricing
- `[metrics]` — Prometheus metrics toggle

## Building and Running

```bash
# Build
cargo build --release

# Run (set API keys as env vars)
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="sk-ant-..."
./target/release/maxllm --config maxllm.toml

# Test
curl -H "Authorization: Bearer sk-maxllm-dev-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hello"}]}' \
  http://localhost:8080/v1/chat/completions
```

## Performance

Benchmarked on macOS (Apple Silicon), 4 Pingora worker threads:

| Scenario | Req/sec | p99 Latency |
|----------|---------|-------------|
| /health (no plugins) | 178,000 | 980us |
| Auth rejection (2 plugins) | 14,000 | 13ms |

## Test Coverage

798 tests across 6 crates:
- `maxllm-admin`: 51 tests (keys, teams, costs, budget, API)
- `maxllm-translate`: 239 tests (all provider translators + streaming)
- `maxllm-plugin`: 193 tests (all 14 plugins + the plugin contract)
- `maxllm-config`: 32 tests (parsing, validation, env vars)
- `maxllm-gateway`: 283 tests (159 unit + 124 mock integration; 4 live tests are gated behind `#[ignore]`) + 1 doc-test
- `maxllm-cli`: 1 test

## Development Notes

- `cargo check --offline` works if deps are cached (avoids SSL proxy issues)
- `RUST_LOG=warn` suppresses per-request logs for benchmarking
- `RUST_LOG=debug` shows full Pingora connection/TLS/header details
- Always unset `HTTPS_PROXY` before running — Pingora routes through it and it breaks TLS to upstream providers
- The `middleware.rs` file is legacy (auth moved to key_auth plugin) — can be deleted
