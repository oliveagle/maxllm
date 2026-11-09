# Vendored dependencies

Everything here is upstream code, pinned, with the smallest possible local
change. Each entry says what changed and why, so the patch can be re-applied
(or dropped) when the dependency is upgraded.

## `pingora-proxy` 0.8.0

Apache-2.0 (see `pingora-proxy/LICENSE`), copied verbatim from the crates.io
release of `pingora-proxy` 0.8.0 except for two local changes: one condition
in `src/proxy_h1.rs` and the **first-byte deadline** (a new module
`src/first_byte.rs`, a new trait method, and the H1 proxy loop). Both are kept
as a reviewable diff in `pingora-proxy-maxllm.patch`.

### The first-byte deadline

Pingora's `read_timeout` is a *per read* budget. It catches an upstream that
goes completely quiet, but it says nothing about how long the upstream may
take to **start** answering: an upstream that accepts the request and then
sends its headers (or its first token) much later holds the client for the
whole budget, and a pool that could have answered in two seconds is not asked
until then.

A deadline for the first byte cannot simply be a number the proxy is given up
front, because the caller can only decide it once it has read the request
body — that is when it knows whether the answer arrives as a stream (whose
first byte comes quickly) or as one document (whose first byte is the whole
answer). The request body is read *concurrently* with the wait for the
upstream's response, and the upstream request is written before either.

So the decision travels through a cell:

* `src/first_byte.rs` — `FirstByteBudget`: a cloneable cell that carries
  "the upstream has N seconds to produce its first byte" (or "no deadline")
  from the caller to the proxy. The first decision wins, so a failover reuses
  the decision of the first attempt.
* `ProxyHttp::first_byte_budget()` — the caller hands the cell over. The
  default is `None`: no deadline.
* `src/proxy_h1.rs` — the upstream loop waits for the decision *before* it
  reads the response (a read that is already under way must not be
  cancelled: a partially read response header would be lost), and arms the
  deadline on the first response byte. The response header is not the
  answer, so the deadline stays armed until the first body chunk arrives;
  after that the stream may take as long as it needs. A timeout is reported
  as `ReadTimedout`, which the caller reads as "this member is not
  answering" and turns into a failover to the next candidate.

The H2 upstream loop (`src/proxy_h2.rs`) is unchanged: the deadline applies to
H1 upstreams only, which is every upstream the gateway talks to
(`PeerOptions::new()` defaults to `ALPN::H1`).

### The body-replay condition

**The change.** In `proxy_handle_downstream`, the request body is handed to
`request_body_filter` only when pingora has a retry buffer *or* the body is
empty:

```rust
if buffer.is_some() || session.as_mut().is_body_empty() { ... }
```

`Session::get_retry_buffer()` returns `None` once the buffer truncated — and it
truncates at 64 KiB (`BODY_BUF_LIMIT` in `pingora-core`). So for any request
whose body is larger than that, the body is never given to the filter at all.

That matters here because the gateway buffers the whole client body anyway
(translation needs the complete JSON), and pingora replays at most 64 KiB of
it. A failover after the body was consumed — an upstream that answers 404/429/5xx
to an agent request, which carries ~100 KB of instructions and tool schemas —
therefore sent a truncated body to the next upstream while the client waited for
a response. Neither side could make progress: the request sat there until the
client gave up (observed: 391 s of silence from `codex`).

With the extra `|| body_done` term, the filter is called once with an empty
final chunk when the downstream body has already been read to the end. The
gateway's `request_body_filter` re-injects the body it kept (`ctx.replay_body`)
in one piece; a proxy that keeps no body of its own now ends the upstream body
instead of deadlocking it.

**Upgrading.** Re-copy the new release over `vendor/pingora-proxy`, then
re-apply `vendor/pingora-proxy-maxllm.patch` (or drop the vendor directory and
the `[patch.crates-io]` block in the root `Cargo.toml` once upstream lets a
request body be replayed without truncation — `pingora-proxy` 0.9.1 still has
the same `if buffer.is_some() || is_body_empty()` condition).
