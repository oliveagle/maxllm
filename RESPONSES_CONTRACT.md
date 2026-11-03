# Responses conversion contract (opencode CLI)

This document records the contract maxllm's Responses translation must satisfy
for the opencode CLI, and how each part is verified. It is derived from the
opencode source (upstream `anomalyco/opencode` v2.0.16 and the native
`open-responses` adapter in the oliveagle fork, `packages/llm/src/protocols/
openai-responses.ts`) plus live verification against the production gateway.

## Two clients, one contract

The opencode CLI can talk to a Responses server in two ways:

1. **AI SDK provider** (`"npm": "@ai-sdk/openai"`, the installed CLI's default):
   posts to `<baseURL>/responses` and consumes the OpenAI Responses SSE stream.
2. **Native adapter** (the fork's `open-responses.ts`, `PATH = "/responses"`):
   posts the same `response.create` shape and consumes the same events.

Both expect the same wire contract, so maxllm serves a single translation.

## Request fields the clients send (all handled)

`model`, `input`, `instructions` (→ system message), `tools`, `tool_choice`,
`store`, `service_tier`, `prompt_cache_key`, `include`, `reasoning`
(`effort`/`summary`), `text` (`verbosity`), `max_output_tokens` (→
`max_tokens`), `temperature`, `top_p`, `parallel_tool_calls`, `stream`.

`input` items handled: `message`, `function_call`, `function_call_output`,
`reasoning` (summary text kept, `encrypted_content` ignored), and unknown
types such as `compaction` are dropped without error (unit-tested).

## Streaming events required by the client

Terminal events (any one ends the stream):
`response.completed`, `response.incomplete`, `response.failed`.

Event order the fork documents and maxllm emits (verified live):

```
response.created
response.output_item.added          (reasoning item)
response.reasoning_summary_part.added   (summary_index 0)
response.reasoning_summary_text.delta   (…fragments…)
response.reasoning_summary_text.done
response.reasoning_summary_part.done    (summary_index 0)
response.output_item.done           (reasoning, status completed)
response.output_item.added          (message item)
response.content_part.added
response.output_text.delta          (…fragments…)
response.output_text.done
response.content_part.done
response.output_item.done           (message, status completed)
response.completed / response.incomplete / response.failed
data: [DONE]
```

A tool-call turn replaces the message block with:

```
response.output_item.added          (function_call, arguments "")
response.function_call_arguments.delta (…fragments…)
response.output_item.done           (function_call, arguments complete)
```

The reasoning item must close before the message item opens, and the message
item before a function_call item opens (the contract opens/closes output items
one after another, never interleaved). maxllm does this (unit-tested and
verified live: `output_item.added` → `reasoning_summary_part.added` →
`reasoning_summary_text.delta` → … → `output_item.done`, in that order).

## Terminal event semantics

- `response.completed`: the response object carries `status: "completed"`,
  the output items, and `usage` (`input_tokens`, `input_tokens_details`,
  `output_tokens`, `output_tokens_details.reasoning_tokens`, `total_tokens`).
- `response.incomplete`: carries `incomplete_details: {reason}` for
  `length`/`max_tokens` finish reasons (`max_output_tokens`).
- `response.failed`: carries `response.error` with `message`/`code`/`type`/
  `param` — the client reads `event.response.error.message|code` and the
  `response` object, and classifies `context_length_exceeded` / overflow
  messages as context-overflow.

## Usage in streaming (verified live)

The gateway always asks the upstream for usage
(`stream_options.include_usage`). Upstreams differ in how they report it:

- DeepSeek bundles `usage` in the finish-reason chunk.
- MiniMax (HTTP/2), Xiaomi and the one-api/ollama channels send a separate
  `choices: []` usage chunk right after the finish-reason chunk.

maxllm ends the stream on the first chunk that carries the usage of a run
whose finish reason is already known — the finish-reason chunk for DeepSeek,
the following usage chunk for the others — while `[DONE]` and the gateway's
end-of-stream signal still finalize the streams that send them. MiniMax ends
its HTTP/2 stream without a `[DONE]` frame and pingora 0.8.0 does not forward
an end-of-stream signal for such a connection, so waiting for `[DONE]` alone
would hang the client; finalizing on the usage chunk covers it and keeps the
token counts in `response.completed` (verified: MiniMax 199 in / 29 out /
228 total, previously 0/0/0).

## Non-streaming response

A `stream: false` request returns one JSON object (not SSE):
`{id, object: "response", output, status: "completed", usage, ...}` with the
same fields the streaming `response.completed` carries. Verified live:
`instructions` ("only reply with numbers") reached the upstream and the model
answered `391`.

## Verified end to end

- Real multi-turn opencode sessions (10 steps, tool calls, file writes,
  pytest) complete on deepseek, one_api_minimax, one_api_xiaomi and
  one_api_ollama_local with zero "Invalid ... stream event", zero hangs, and
  real usage.
- A fork-style request carrying every client field (`store`, `service_tier`,
  `prompt_cache_key`, `include`, `text.verbosity`, `reasoning.effort+summary`,
  a `compaction` item, a `reasoning` item with `encrypted_content`, and a
  tool history) translates with no 400 and emits the exact event order above.
- A forced tool call (`tool_choice: {type: function, name}`) streams
  `function_call_arguments.delta` fragments and completes the item with the
  full arguments `{"city": "Paris"}`.
