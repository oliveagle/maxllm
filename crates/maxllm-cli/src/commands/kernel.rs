// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The kernel edit loop: compile-check and dry-run a kernel script without
//! starting the gateway.
//!
//! Editing a kernel is not the problem — knowing whether the edit worked is.
//! The engine is the only compiler, and the obvious way to run it is a live
//! gateway. These commands replace that with a sub-second gate: `check`
//! compiles the script and reports which hooks it defines; `run` executes
//! the classifier against a status/body pair straight from the terminal.

use std::io::Read;
use std::path::Path;

const DEFAULT_MAX_OPERATIONS: u64 = 2_000_000;

fn load(
    script: &Path,
    max_operations: Option<u64>,
) -> Result<maxllm_translate::kernel::TranslationKernel, i32> {
    match maxllm_translate::kernel::TranslationKernel::load(
        script,
        max_operations.unwrap_or(DEFAULT_MAX_OPERATIONS),
    ) {
        Ok(kernel) => Ok(kernel),
        Err(e) => {
            eprintln!("Error: {e}");
            Err(1)
        }
    }
}

/// `maxllm kernel check` — compile the script, report the hooks.
pub fn check(script: &Path, max_operations: Option<u64>) -> i32 {
    let kernel = match load(script, max_operations) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let hooks = kernel.hooks();
    println!("Kernel compiles: {}", kernel.path().display());
    println!("  on_request_body:   {}", yes(hooks.request));
    println!("  on_upstream_error: {}", yes(hooks.error));
    if !hooks.request && !hooks.error {
        println!();
        println!("  (the script defines neither hook — it is a no-op kernel)");
    }
    0
}

/// `maxllm kernel run` — dry-run the classifier against a status/body pair.
pub fn run(
    script: &Path,
    status: u16,
    error_body: Option<&str>,
    provider: &str,
    model: &str,
    max_operations: Option<u64>,
) -> i32 {
    let kernel = match load(script, max_operations) {
        Ok(k) => k,
        Err(code) => return code,
    };
    if !kernel.hooks().error {
        eprintln!("Error: the script defines no on_upstream_error hook");
        return 1;
    }
    let body = match error_body {
        Some("-") => {
            let mut buf = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut buf) {
                eprintln!("Error: cannot read stdin: {e}");
                return 1;
            }
            if buf.is_empty() {
                eprintln!("Error: stdin was empty");
                return 1;
            }
            buf
        }
        Some(text) => text.to_string(),
        None => {
            eprintln!("Error: --error-body is required (\"-\" reads stdin)");
            return 1;
        }
    };
    match kernel.classify_error(status, &body, provider, model) {
        Ok(repair) => {
            println!(
                "{}",
                match repair {
                    maxllm_translate::kernel::KernelRepair::None => "none",
                    maxllm_translate::kernel::KernelRepair::SchemaDegrade => "schema",
                    maxllm_translate::kernel::KernelRepair::OrphanRepair => "orphan",
                }
            );
            0
        }
        Err(e) => {
            eprintln!("Error: {e}");
            1
        }
    }
}

fn yes(v: bool) -> &'static str {
    if v {
        "defined"
    } else {
        "not defined"
    }
}
