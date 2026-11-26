# MaxLLM Gateway — Multi-stage Docker build
# Stage 1: Build the Rust binaries
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/maxllm-config/Cargo.toml crates/maxllm-config/Cargo.toml
COPY crates/maxllm-plugin/Cargo.toml crates/maxllm-plugin/Cargo.toml
COPY crates/maxllm-translate/Cargo.toml crates/maxllm-translate/Cargo.toml
COPY crates/maxllm-gateway/Cargo.toml crates/maxllm-gateway/Cargo.toml
COPY crates/maxllm-admin/Cargo.toml crates/maxllm-admin/Cargo.toml
COPY crates/maxllm-cli/Cargo.toml crates/maxllm-cli/Cargo.toml
COPY crates/maxllm-supervisor/Cargo.toml crates/maxllm-supervisor/Cargo.toml

# Create dummy source files for dependency caching
RUN mkdir -p crates/maxllm-config/src && echo "fn main() {}" > crates/maxllm-config/src/lib.rs \
    && mkdir -p crates/maxllm-plugin/src && echo "fn main() {}" > crates/maxllm-plugin/src/lib.rs \
    && mkdir -p crates/maxllm-translate/src && echo "fn main() {}" > crates/maxllm-translate/src/lib.rs \
    && mkdir -p crates/maxllm-gateway/src && echo "fn main() {}" > crates/maxllm-gateway/src/main.rs \
    && mkdir -p crates/maxllm-admin/src && echo "fn main() {}" > crates/maxllm-admin/src/lib.rs \
    && mkdir -p crates/maxllm-cli/src && echo "fn main() {}" > crates/maxllm-cli/src/main.rs \
    && mkdir -p crates/maxllm-supervisor/src && echo "fn main() {}" > crates/maxllm-supervisor/src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release 2>/dev/null || true

# Now copy the actual source code
COPY crates/ crates/

# Touch source files to invalidate the cached build of our code
RUN touch crates/*/src/*.rs crates/*/src/**/*.rs 2>/dev/null || true

# Build the real binaries
RUN cargo build --release --bin maxllm --bin maxllm-server --bin maxllm-supervisor

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd --create-home --shell /bin/bash maxllm

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/target/release/maxllm /usr/local/bin/maxllm
COPY --from=builder /build/target/release/maxllm-server /usr/local/bin/maxllm-server
COPY --from=builder /build/target/release/maxllm-supervisor /usr/local/bin/maxllm-supervisor

# Copy default config if it exists
COPY maxllm.toml /app/maxllm.toml

# Create data directory for SQLite
RUN mkdir -p /app/data && chown -R maxllm:maxllm /app

USER maxllm

EXPOSE 8080

# Health check
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD maxllm health --url http://127.0.0.1:8080 || exit 1

ENTRYPOINT ["maxllm"]
CMD ["start", "--config", "/app/maxllm.toml"]
