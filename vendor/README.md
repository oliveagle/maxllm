# Vendored dependencies

Everything here is upstream code, pinned, with the smallest possible local
change. Each entry says what changed and why, so the patch can be re-applied
(or dropped) when the dependency is upgraded.

## `pingora-proxy` 0.8.0

Apache-2.0 (see `pingora-proxy/LICENSE`), copied verbatim from the crates.io
release of `pingora-proxy` 0.8.0 except for one condition in
`src/proxy_h1.rs`. The change is also kept as a reviewable diff in
`pingora-proxy-maxllm.patch`.

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
