.PHONY: build run dev test test-all test-integration test-live clean fmt check perf perf-stream perf-health perf-mock perf-proxy

# Build release binary
build:
	cargo build --release

# Build debug binary
dev:
	cargo build

# Run the gateway (release)
run: build
	unset HTTPS_PROXY HTTP_PROXY http_proxy https_proxy; \
	./target/release/maxllm start --config maxllm.toml

# Run the gateway (debug, with verbose logging)
run-debug: dev
	unset HTTPS_PROXY HTTP_PROXY http_proxy https_proxy; \
	RUST_LOG=debug ./target/debug/maxllm start --config maxllm.toml

# Run unit tests only
test:
	cargo test --workspace

# Run unit + mock integration tests
test-integration:
	cargo test --workspace
	cargo test -p maxllm-gateway --test integration

# Run real provider tests (requires Ollama + GEMINI_API_KEY)
test-live:
	unset HTTPS_PROXY HTTP_PROXY http_proxy https_proxy; \
	cargo test -p maxllm-gateway --test integration -- --ignored

# Run everything
test-all:
	cargo test --workspace
	cargo test -p maxllm-gateway --test integration
	unset HTTPS_PROXY HTTP_PROXY http_proxy https_proxy; \
	cargo test -p maxllm-gateway --test integration -- --ignored

# Format code
fmt:
	cargo fmt --all

# Check without building
check:
	cargo check --workspace

# Lint
clippy:
	cargo clippy --workspace -- -W clippy::all

# Enforce the native-Responses-first policy on a provider config
# (CLAUDE.md § Responses Contract Policy). --live posts a minimal Responses body
# to every native route; 404 means a wrong responses_path, not "unsupported".
policy:
	python3 scripts/check_responses_policy.py --config maxllm.toml

policy-live:
	python3 scripts/check_responses_policy.py --config maxllm.toml --live

# Performance test — non-streaming (requires wrk + running gateway + Ollama)
perf:
	@echo "Running wrk against /v1/ollama (non-streaming, 10s, 10 connections)..."
	wrk -t4 -c10 -d10s -s perf/post.lua http://localhost:8080/v1/ollama

# Performance test — health endpoint (pure gateway overhead, no upstream)
perf-health:
	@echo "Running wrk against /health (30s, 100 connections)..."
	wrk -t4 -c100 -d30s --latency http://localhost:8080/health

# Performance test — mock upstream (pure proxy overhead, requires mock_upstream.py running)
perf-mock:
	@echo "Running wrk against /v1/mock (30s, 100 connections)..."
	wrk -t4 -c100 -d30s --latency -s perf/post_mock.lua http://localhost:8080/v1/mock

# Performance test — bare proxy (no plugins, no auth, just routing + upstream)
# Requires: mock_upstream running, gateway started with: ./target/release/maxllm --config perf/bare_proxy.toml
perf-proxy:
	@echo "Running wrk against /v1/proxy (bare proxy, 30s, 100 connections)..."
	wrk -t4 -c100 -d30s --latency -s perf/post_proxy.lua http://localhost:8080/v1/proxy

# Performance test — streaming
perf-stream:
	@echo "Running wrk against /v1/ollama (streaming, 10s, 10 connections)..."
	wrk -t4 -c10 -d10s -s perf/post_stream.lua http://localhost:8080/v1/ollama

# Clean build artifacts
clean:
	cargo clean
