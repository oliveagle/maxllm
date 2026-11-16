// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The shipped translation kernel, locked by a regression test.
//!
//! The kernel script (`examples/kernel/moonshot/main.rhai`) is the updatable
//! half of the gateway: a provider-dialect rule lives in a `.rhai` file, so
//! nothing but a test stops an edit from dropping it. These tests run the
//! shipped script through the same engine the gateway runs it with.

use maxllm_translate::kernel::{KernelRepair, RequestKernelCtx, TranslationKernel};
use serde_json::{json, Value};
use std::path::PathBuf;

/// The script the gateway loads (`[translation_kernel] script = ...`).
fn shipped_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/kernel/moonshot/main.rhai")
}

fn shipped_kernel() -> TranslationKernel {
    let path = shipped_script();
    TranslationKernel::load(&path, 2_000_000).expect("the shipped kernel compiles")
}

/// Run the shipped kernel's request hook over `body`, as the provider
/// `provider` would see it.
fn outgoing_body(kernel: &TranslationKernel, provider: &str, body: Value) -> Value {
    let ctx = RequestKernelCtx {
        body,
        provider: provider.to_string(),
        model: "deepseek-v4.1-flash".to_string(),
        tool_schema_degraded: false,
        tool_calls_repaired: false,
    };
    kernel
        .transform_request(&ctx)
        .expect("the kernel runs")
        .expect("the kernel rewrote the body")
}

/// DeepSeek (api.deepseek.com) refuses every tool_choice but "auto"/"none":
/// "any" does not even deserialize (HTTP 422) and "required" or a named
/// function choice are refused by thinking mode (HTTP 400 "Thinking mode
/// does not support this tool_choice"). Both statuses reached the client and
/// ended the agent's turn, so the choice is degraded to "auto" — the model
/// decides whether to call a tool and the turn continues.
#[test]
fn deepseek_degrades_a_tool_choice_its_upstream_refuses() {
    let kernel = shipped_kernel();
    for choice in [
        json!("any"),
        json!("required"),
        json!({ "type": "function", "name": "shell" }),
    ] {
        let out = outgoing_body(
            &kernel,
            "deepseek",
            json!({ "model": "deepseek-flash", "tool_choice": choice }),
        );
        assert_eq!(
            out["tool_choice"],
            json!("auto"),
            "a choice the upstream refuses must be degraded before it leaves"
        );
    }
}

/// A choice the upstream accepts is left alone — the rule must not rewrite
/// what already works, and it must not add a key the client never sent.
#[test]
fn deepseek_keeps_a_tool_choice_its_upstream_accepts() {
    let kernel = shipped_kernel();
    let out = outgoing_body(
        &kernel,
        "deepseek",
        json!({ "model": "deepseek-flash", "tool_choice": "auto" }),
    );
    assert_eq!(out["tool_choice"], json!("auto"));
    let out = outgoing_body(
        &kernel,
        "deepseek",
        json!({ "model": "deepseek-flash", "tool_choice": "none" }),
    );
    assert_eq!(out["tool_choice"], json!("none"));
    let out = outgoing_body(&kernel, "deepseek", json!({ "model": "deepseek-flash" }));
    assert!(
        out.get("tool_choice").is_none(),
        "a body without a tool_choice gains no key"
    );
}

/// The rule is scoped to the provider that has the quirk: an upstream that
/// speaks "required" keeps it.
#[test]
fn the_rule_is_scoped_to_the_provider_that_has_the_quirk() {
    let kernel = shipped_kernel();
    let out = outgoing_body(
        &kernel,
        "kimi",
        json!({ "model": "kimi-for-coding", "tool_choice": "required" }),
    );
    assert_eq!(out["tool_choice"], json!("required"));
    let out = outgoing_body(
        &kernel,
        "minimax",
        json!({ "model": "MiniMax-M3", "tool_choice": "any" }),
    );
    assert_eq!(out["tool_choice"], json!("any"));
    // The open_design rule ("any" -> "required") still runs.
    let out = outgoing_body(
        &kernel,
        "open_design",
        json!({ "model": "deepseek-v4.1-flash", "tool_choice": "any" }),
    );
    assert_eq!(out["tool_choice"], json!("required"));
}

/// The error side of the same script still classifies the phrasings it owns
/// — the request rule must not have displaced them.
#[test]
fn the_error_side_still_classifies_the_phrasings_it_owns() {
    let kernel = shipped_kernel();
    let schema = kernel
        .classify_error(
            400,
            "tools.function.parameters is not a valid moonshot flavored json schema",
            "kimi",
            "kimi-for-coding",
        )
        .expect("the kernel classifies");
    assert_eq!(schema, KernelRepair::SchemaDegrade);
    let orphan = kernel
        .classify_error(
            400,
            "No tool output found for function call",
            "kimi",
            "kimi-for-coding",
        )
        .expect("the kernel classifies");
    assert_eq!(orphan, KernelRepair::OrphanRepair);
    // The DeepSeek refusal is not an error-side repair: it is prevented on
    // the request side, so the error hook has no opinion about it.
    let none = kernel
        .classify_error(
            400,
            "Thinking mode does not support this tool_choice",
            "deepseek",
            "deepseek-flash",
        )
        .expect("the kernel classifies");
    assert_eq!(none, KernelRepair::None);
}
