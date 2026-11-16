// The Moonshot-family dialect kernel.
//
// This file is the updatable half of the gateway. The compiled baseline
// already projects the tool schemas and answers the error phrasings it
// knows; everything here is the part an operator can change by editing a
// file — saving it hot-reloads the kernel, no release cycle.
//
// The contract:
//   * `on_request_body(ctx)` — adapt the outgoing body. `ctx` carries the
//     translated body, the provider, the model and the replay flags.
//     Return the new body, or nothing to pass it through.
//   * `on_upstream_error(ctx)` — classify an upstream error. `ctx` carries
//     the status, the error body (a string), the provider and the model.
//     Return `#{ "repair": "schema" | "orphan" | "none" }`.
//
// The kernel may only ESCALATE: when it says "none", the compiled baseline
// rules still decide. It can never switch a baseline repair off, and a
// script fault (runtime error, operation limit) is answered as "no
// decision" — the baseline keeps its say and the agent's turn survives.

// ── The request side ──────────────────────────────────────────────────────
//
// The OpenDesign link answers HTTP 400 for the non-standard
// `tool_choice: "any"` that codex-cli sends. The built-in open_design
// provider gets this rewrite in Rust; a *new* provider with the same quirk
// gets it here, by naming the provider — no release required.
fn on_request_body(ctx) {
    let body = ctx["body"];
    let provider = ctx["provider"];

    if provider.contains("open_design") {
        if body["tool_choice"] == "any" {
            body["tool_choice"] = "required";
        }
    }

    // DeepSeek (api.deepseek.com) speaks the Responses contract but refuses
    // every tool_choice but "auto"/"none":
    //   * "any" does not even deserialize — HTTP 422
    //     "tool_choice: expected one of `none`, `auto`, `required` or a tool choice object";
    //   * "required" and a named function choice are refused by thinking mode
    //     — HTTP 400 "Thinking mode does not support this tool_choice".
    // Both statuses reached the client and ended the agent's turn: the 400
    // was the last leg of the chain, so no failover was left. The choice is
    // degraded to "auto" — the model decides whether to call a tool, and
    // the turn continues.
    if provider.contains("deepseek") {
        let choice = body["tool_choice"];
        if choice == "any" || choice == "required" || type_of(choice) == "map" {
            body["tool_choice"] = "auto";
        }
    }

    body
}

// ── The error side ────────────────────────────────────────────────────────
//
// The Moonshot schema dialect: the upstream refused the tool schemas
// (`tools.function.parameters is not a valid moonshot flavored json
// schema`). The repair replays the request with every tool schema reduced
// to a bare object — the tool names survive, the turn continues.
//
// The strict pairing rule: the history carries an assistant tool call that
// nothing answered. The repair answers the orphan with a sentinel and
// replays the request.
fn on_upstream_error(ctx) {
    let body = ctx["body"];

    if body.contains("moonshot flavored json schema")
        || body.contains("tools.function.parameters") {
        return #{ "repair": "schema" };
    }

    if body.contains("did not have response messages")
        || body.contains("No tool output found for function call")
        || body.contains("tool_call_id") {
        return #{ "repair": "orphan" };
    }

    #{ "repair": "none" }
}
