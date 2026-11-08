# maxllm-commandcode — an OpenAI bridge in front of the Command Code CLI

`maxllm-commandcode` turns the vendor's own client — the `cmd` binary that
commandcode.ai ships — into an OpenAI-compatible upstream, so a MaxLLM
deployment can serve the Command Code subscription over the **OpenAI
Responses contract** (`POST /v1/responses`) and the Chat Completions contract
(`POST /v1/chat/completions`).

## Why a bridge instead of a provider entry

commandcode.ai exposes two API surfaces:

| Surface | Path | Plan | Notes |
|---------|------|------|-------|
| Provider API | `/provider/v1/*` | Provider ($15/mo + $1.01) | OpenAI Chat Completions, OpenAI Responses and Anthropic Messages. On the Go plan every call answers **HTTP 403 `upgrade_required`** — the key is valid, the plan is not. |
| CLI protocol | `/alpha/*` | Go | The wire protocol the `cmd` binary speaks. The vendor answers a direct call with `Proxy use detected. This endpoint only serves CLI. Use Command Code provider API instead. Continued proxying of your subscription violates the TOS and will result in account ban.` |

MaxLLM's `kind = "commandcode"` provider entry talks to the **Provider API**.
That is the sanctioned path and it is the one to use once the account is on the
Provider plan.

This bridge covers the gap *before* the upgrade: instead of re-implementing
the vendor's `/alpha/*` protocol (which the vendor explicitly reserves for
their own client), it runs **the vendor's own client** in the mode the CLI
documents for automation — `cmd -p <query> --output-format json` — and
translates the NDJSON event stream into an OpenAI contract. The request
travels over the same subscription, through the same client, with the same
credentials as a human typing into the CLI.

> The vendor's terms call continued proxying of a subscription a violation.
> This bridge is the *documented automation mode* of their own client, not a
> re-implementation of their wire protocol — but that is the user's call to
> make, and the Provider plan is the sanctioned alternative.

## The contract it speaks

`POST /v1/responses` — the **OpenAI Responses contract**, natively:

* streaming: the `response.*` SSE event family
  (`response.created`, `response.in_progress`, `response.output_item.added`,
  `response.content_part.added`, `response.output_text.delta`,
  `response.output_text.done`, `response.content_part.done`,
  `response.output_item.done`, `response.completed`, then `data: [DONE]`),
  every event stamped with a monotonic `sequence_number`;
* non-streaming: one `response` object with the `output` message item and the
  `usage` block.

`POST /v1/chat/completions` — the Chat Completions contract (`chat.completion`
/ `chat.completion.chunk`).

Both accept the usual request fields (`model`, `instructions` / `messages`,
`stream`, `tools`). `GET /v1/models` lists the served model ids, `GET /health`
is a liveness probe.

In MaxLLM the bridge is an `openai_compat` provider with
`native_responses = true` and `responses_path = "/v1/responses"`, so the
gateway forwards a client's Responses body **byte-for-byte** — no translation
on the way in or out.

## Running it

```sh
maxllm-commandcode \
  --listen 127.0.0.1:3801 \
  --cmd-bin /home/oliveagle/.local/node/bin/cmd \
  --model commandcode-cli/deepseek-v4.1-flash \
  --cli-model deepseek/deepseek-v4.1-flash \
  --workspace /home/oliveagle/opt/maxllm/cmd-workspace \
  --timeout-secs 300
```

| Flag | Meaning |
|------|---------|
| `--listen` | listen address (default `127.0.0.1:3801`) |
| `--cmd-bin` | the `cmd` binary to drive |
| `--model` | the model id the bridge **serves** (what clients ask for, what the answer reports) |
| `--cli-model` | the model the CLI **runs** (`cmd --model`) |
| `--allow-model` | extra served ids (repeatable) |
| `--workspace` | the directory the CLI runs in — a scratch directory, never a real workspace |
| `--max-turns` | cap on the CLI's internal conversation turns (default 8) |
| `--timeout-secs` | wall-clock budget for one CLI run (default 300) |
| `--effort` | reasoning effort handed to the CLI |
| `--home` | `HOME` for the CLI process (it reads `$HOME/.commandcode/auth.json`) |

### Why the served id differs from the CLI model id

`--model` is the id the client sends and the gateway books. The CLI plan is a
flat subscription, so the served id must **not** match a per-token price entry
in MaxLLM's cost table — otherwise subscription traffic is booked as if it
were metered. `commandcode-cli/deepseek-v4.1-flash` matches no price entry and
is therefore booked at `$0` while the token counters are still recorded. The
CLI's own id (`deepseek/deepseek-v4.1-flash`) is served as well, for clients
that hardcode it; traffic on that id *is* billed at the metered rate.

## The CLI invocation

```sh
cmd -p <prompt> --output-format json --no-session --no-auto-update \
    --skip-onboarding --trust --permission-mode dont-ask --max-turns 8 \
    --model <cli-model>
```

* `-p` — non-interactive: print the answer and exit;
* `--output-format json` — the NDJSON event stream the bridge parses;
* `--no-session` — nothing is persisted to disk;
* `--no-auto-update` — the CLI never rewrites itself mid-request;
* `--skip-onboarding` — never prompt for taste onboarding;
* `--permission-mode dont-ask` — never prompt; anything that would need
  approval is **denied** instead, so a headless run can never block;
* `--trust` — skip the initial project-trust prompt.

`--tools-all` is deliberately **not** passed: in `-p` mode the CLI withholds
the interactive tools (`ask_user_question`, `enter_plan_mode`,
`exit_plan_mode`, `plan_review`, `todo_write`, `cron_*`, `schedule_wakeup`,
`taste`) unless they are explicitly enabled.

## Known limits

* **No function calling.** The CLI has no channel for the caller's tools, so a
  request that carries `tools` is answered without them (the tools are
  dropped, a warning is logged).
* **~24k input tokens per request.** The CLI's own agent system prompt is
  part of every request; the model is billed for it out of the subscription.
* **One CLI process per request.** Latency is the CLI's (~5-8 s for a short
  answer), and the bridge starts a fresh process each time.
* **The CLI agent has its own tools** (file and shell). They run inside the
  scratch workspace, and `--permission-mode dont-ask` denies whatever would
  need an approval — but the CLI still runs with the user's permissions, so
  point `--workspace` at a throwaway directory.

## Tests

```sh
cargo test -p maxllm-commandcode
```

The unit tests cover the NDJSON event parsing, the prompt construction (both
contracts), the usage mapping and the Responses event family.
