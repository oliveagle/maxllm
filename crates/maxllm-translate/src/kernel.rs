// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! The translation kernel: the updatable half of the gateway.
//!
//! The runtime (this binary) changes rarely; the provider-dialect policy
//! changes often — upstreams add error phrasings, tighten schema dialects,
//! reject new request shapes. This module loads a Rhai script that owns
//! that decision surface, so a dialect fix is "edit the script, hot-reload"
//! instead of a release cycle:
//!
//! * `on_request_body(ctx)` — adapt the outgoing request before it leaves
//!   the gateway. Receives the translated body as a JSON map plus routing
//!   facts; returns the new body map, or `()` to pass it through.
//! * `on_upstream_error(ctx)` — classify an upstream error (status + body
//!   string). Returns `#{ "repair": "schema" | "orphan" }`, or `()` / null
//!   for "the baseline rules decide".
//!
//! The script is a *policy layer over* the Rust baseline, never a
//! replacement: a kernel fault (compile, runtime, limit) is answered with
//! "no kernel decision" and the built-in translation rules keep running.
//! A script can only escalate a repair the baseline would also make — it
//! cannot disable a baseline repair, because the whole point of the
//! baseline is that a translation fault must never end the agent's turn.
//!
//! The engine is sandboxed exactly like the laya-workflow plugin engine:
//! no `eval`, no `import`, no raw `print`, hard operation/string/array/map
//! limits. The only host surface is `log`, which lands in the gateway log.

use rhai::{Dynamic, Engine, Scope, AST};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// What the kernel decided to do about an upstream error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelRepair {
    /// No kernel-driven repair; the baseline rules decide.
    None,
    /// The upstream refused the tool schemas — replay with degraded ones.
    SchemaDegrade,
    /// The upstream refused an unanswered tool call — repair the history.
    OrphanRepair,
}

/// A kernel fault. The caller treats every variant as "no kernel decision":
/// the script can never end the agent's turn either.
#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("kernel script {path:?} does not exist")]
    Missing { path: PathBuf },
    #[error("kernel script {path:?} did not compile: {message}")]
    Compile { path: PathBuf, message: String },
    #[error("kernel script faulted: {0}")]
    Runtime(String),
    #[error("kernel script returned a value that is not a JSON object: {0}")]
    NotJsonObject(String),
}

/// The facts a request-body hook receives.
#[derive(Debug, Clone)]
pub struct RequestKernelCtx {
    /// The outgoing body, already translated for the provider.
    pub body: Value,
    /// The provider the request is being sent to.
    pub provider: String,
    /// The model the provider will see.
    pub model: String,
    /// Whether this replay already carries degraded tool schemas.
    pub tool_schema_degraded: bool,
    /// Whether this replay already carries a repaired tool history.
    pub tool_calls_repaired: bool,
}

/// A compiled translation kernel: one sandboxed engine, one compiled AST.
pub struct TranslationKernel {
    engine: Engine,
    ast: AST,
    path: PathBuf,
    has_request_hook: bool,
    has_error_hook: bool,
}

/// Build the sandboxed engine. Everything a script can reach is registered
/// here; the language itself contributes no I/O. Keep this list small and
/// auditable — it is the kernel API.
fn build_engine(max_operations: u64) -> Engine {
    let mut engine = Engine::new();
    engine.set_max_operations(max_operations.max(100_000));
    engine.set_max_call_levels(32);
    engine.set_max_expr_depths(96, 48);
    engine.set_max_string_size(8 << 20);
    engine.set_max_array_size(200_000);
    engine.set_max_map_size(200_000);
    // No escape hatches: no dynamic eval, no module import, no raw printing.
    for sym in ["eval", "import", "print", "debug", "Fn"] {
        let _ = engine.disable_symbol(sym);
    }
    engine.register_fn("log", |msg: &str| {
        tracing::info!(target: "maxllm_kernel", "{msg}");
    });
    engine
}

impl TranslationKernel {
    /// Compile the script at `path`. A compile error is a load error: the
    /// gateway refuses this config (loudly), so a broken kernel can never
    /// ship silently.
    pub fn load(path: &Path, max_operations: u64) -> Result<Self, KernelError> {
        if !path.is_file() {
            return Err(KernelError::Missing {
                path: path.to_path_buf(),
            });
        }
        let source =
            std::fs::read_to_string(path).map_err(|e| KernelError::Runtime(e.to_string()))?;
        let engine = build_engine(max_operations);
        let ast = engine.compile(&source).map_err(|e| KernelError::Compile {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
        let has_request_hook = ast.iter_functions().any(|f| f.name == "on_request_body");
        let has_error_hook = ast.iter_functions().any(|f| f.name == "on_upstream_error");
        if !has_request_hook && !has_error_hook {
            return Err(KernelError::Compile {
                path: path.to_path_buf(),
                message: "the script defines neither on_request_body nor on_upstream_error".into(),
            });
        }
        Ok(TranslationKernel {
            engine,
            ast,
            path: path.to_path_buf(),
            has_request_hook,
            has_error_hook,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Which hooks the script actually defines.
    pub fn hooks(&self) -> KernelHooks {
        KernelHooks {
            request: self.has_request_hook,
            error: self.has_error_hook,
        }
    }

    /// Run `on_request_body`. `Ok(None)` means "pass the body through":
    /// either the script said so, or (on a fault) the caller decides the
    /// same, having logged the error.
    pub fn transform_request(&self, ctx: &RequestKernelCtx) -> Result<Option<Value>, KernelError> {
        if !self.has_request_hook {
            return Ok(None);
        }
        let mut scope = Scope::new();
        let input = serde_json::json!({
            "body": ctx.body,
            "provider": ctx.provider,
            "model": ctx.model,
            "tool_schema_degraded": ctx.tool_schema_degraded,
            "tool_calls_repaired": ctx.tool_calls_repaired,
        });
        let ctx_dyn = to_dynamic(input)?;
        let result: Dynamic = self
            .engine
            .call_fn(&mut scope, &self.ast, "on_request_body", (ctx_dyn,))
            .map_err(|e| KernelError::Runtime(e.to_string()))?;
        if result.is_unit() {
            return Ok(None);
        }
        let value = rhai::serde::from_dynamic::<Value>(&result)
            .map_err(|e| KernelError::NotJsonObject(e.to_string()))?;
        if !value.is_object() {
            return Err(KernelError::NotJsonObject(format!("{value:?}")));
        }
        Ok(Some(value))
    }

    /// Run `on_upstream_error`. An unknown repair string or a non-map
    /// return means `None` — the baseline rules decide.
    pub fn classify_error(
        &self,
        status: u16,
        body: &str,
        provider: &str,
        model: &str,
    ) -> Result<KernelRepair, KernelError> {
        if !self.has_error_hook {
            return Ok(KernelRepair::None);
        }
        let mut scope = Scope::new();
        let input = serde_json::json!({
            "status": status,
            "body": body,
            "provider": provider,
            "model": model,
        });
        let ctx_dyn = to_dynamic(input)?;
        let result: Dynamic = self
            .engine
            .call_fn(&mut scope, &self.ast, "on_upstream_error", (ctx_dyn,))
            .map_err(|e| KernelError::Runtime(e.to_string()))?;
        if result.is_unit() {
            return Ok(KernelRepair::None);
        }
        let value = rhai::serde::from_dynamic::<Value>(&result)
            .map_err(|e| KernelError::NotJsonObject(e.to_string()))?;
        let repair = value
            .get("repair")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(match repair {
            "schema" => KernelRepair::SchemaDegrade,
            "orphan" => KernelRepair::OrphanRepair,
            "" | "none" => KernelRepair::None,
            other => {
                tracing::warn!(repair = %other, "kernel returned an unknown repair kind; ignoring");
                KernelRepair::None
            }
        })
    }
}

/// Which hooks a kernel script defines.
#[derive(Debug, Clone, Copy)]
pub struct KernelHooks {
    pub request: bool,
    pub error: bool,
}

fn to_dynamic(value: Value) -> Result<Dynamic, KernelError> {
    rhai::serde::to_dynamic(value).map_err(|e| KernelError::NotJsonObject(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write a kernel script to a temp file and compile it.
    fn compile_source(source: &str) -> Result<TranslationKernel, KernelError> {
        let path = std::env::temp_dir().join(format!(
            "maxllm-kernel-test-{}-{:?}.rhai",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, source).expect("write script");
        let result = TranslationKernel::load(&path, 100_000);
        std::fs::remove_file(&path).ok();
        result
    }

    fn kernel(source: &str) -> TranslationKernel {
        compile_source(source).expect("the script compiles")
    }

    fn ctx(body: Value) -> RequestKernelCtx {
        RequestKernelCtx {
            body,
            provider: "kimi".into(),
            model: "kimi-for-coding".into(),
            tool_schema_degraded: false,
            tool_calls_repaired: false,
        }
    }

    #[test]
    fn a_script_rewrites_the_body() {
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let body = ctx["body"];
                body["tools"] = [];
                body["stream"] = false;
                body
            }
            "#,
        );
        assert!(k.hooks().request);
        assert!(!k.hooks().error);
        let out = k
            .transform_request(&ctx(serde_json::json!({"model": "m", "tools": [1]})))
            .expect("the script runs")
            .expect("the script rewrote the body");
        assert_eq!(out["tools"], serde_json::json!([]));
        assert_eq!(out["stream"], serde_json::json!(false));
        assert_eq!(out["model"], serde_json::json!("m"));
    }

    #[test]
    fn a_script_that_returns_nothing_passes_the_body_through() {
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let ignored = ctx["body"];
            }
            "#,
        );
        assert!(
            k.transform_request(&ctx(serde_json::json!({"model": "m"})))
                .expect("the script runs")
                .is_none(),
            "an empty return means 'no change'"
        );
    }

    #[test]
    fn a_script_sees_the_routing_facts() {
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let body = ctx["body"];
                body["provider_seen"] = ctx["provider"];
                body["model_seen"] = ctx["model"];
                body["degraded"] = ctx["tool_schema_degraded"];
                body
            }
            "#,
        );
        let out = k
            .transform_request(&ctx(serde_json::json!({})))
            .expect("the script runs")
            .expect("the script rewrote the body");
        assert_eq!(out["provider_seen"], serde_json::json!("kimi"));
        assert_eq!(out["model_seen"], serde_json::json!("kimi-for-coding"));
        assert_eq!(out["degraded"], serde_json::json!(false));
    }

    #[test]
    fn a_broken_script_is_a_load_error() {
        let path = std::env::temp_dir().join("maxllm-kernel-broken.rhai");
        std::fs::write(&path, "fn on_request_body(ctx) { let = ; }").expect("write");
        let err = TranslationKernel::load(&path, 100_000)
            .err()
            .expect("a broken script does not load");
        assert!(matches!(err, KernelError::Compile { .. }), "{err:?}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_missing_script_is_a_load_error() {
        let err = TranslationKernel::load(Path::new("/nonexistent/maxllm/kernel.rhai"), 100_000)
            .err()
            .expect("a missing script does not load");
        assert!(matches!(err, KernelError::Missing { .. }), "{err:?}");
    }

    #[test]
    fn a_script_without_hooks_is_a_load_error() {
        let err = compile_source("fn something_else() { 1 }")
            .err()
            .expect("a script without hooks does not load");
        assert!(matches!(err, KernelError::Compile { .. }), "{err:?}");
    }

    #[test]
    fn the_kernel_classifies_an_error() {
        let k = kernel(
            r#"
            fn on_upstream_error(ctx) {
                if ctx["body"].contains("moonshot flavored") {
                    #{ "repair": "schema" }
                } else if ctx["body"].contains("tool_call_id") {
                    #{ "repair": "orphan" }
                } else {
                    #{ "repair": "none" }
                }
            }
            "#,
        );
        assert!(k.hooks().error);
        assert_eq!(
            k.classify_error(
                400,
                "not a valid moonshot flavored json schema",
                "kimi",
                "m"
            )
            .expect("the script runs"),
            KernelRepair::SchemaDegrade
        );
        assert_eq!(
            k.classify_error(400, "tool_call_id f:0 is not found", "kimi", "m")
                .expect("the script runs"),
            KernelRepair::OrphanRepair
        );
        assert_eq!(
            k.classify_error(400, "quota exceeded", "kimi", "m")
                .expect("the script runs"),
            KernelRepair::None
        );
    }

    #[test]
    fn an_unknown_repair_kind_is_ignored() {
        let k = kernel(
            r#"
            fn on_upstream_error(ctx) {
                #{ "repair": "explode" }
            }
            "#,
        );
        assert_eq!(
            k.classify_error(400, "whatever", "kimi", "m")
                .expect("the script runs"),
            KernelRepair::None,
            "an unknown repair kind must not become a decision"
        );
    }

    #[test]
    fn a_faulting_script_is_an_error_not_a_crash() {
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let body = ctx["body"];
                body["x"] = body["nope"]["deeper"];
                body
            }
            "#,
        );
        let err = k
            .transform_request(&ctx(serde_json::json!({})))
            .expect_err("the fault surfaces as an error");
        assert!(matches!(err, KernelError::Runtime(_)), "{err:?}");
    }

    #[test]
    fn a_runaway_script_hits_the_operation_limit() {
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let i = 0;
                loop {
                    i += 1;
                }
                ctx["body"]
            }
            "#,
        );
        let err = k
            .transform_request(&ctx(serde_json::json!({})))
            .expect_err("the runaway script is stopped");
        assert!(matches!(err, KernelError::Runtime(_)), "{err:?}");
    }

    #[test]
    fn the_sandbox_has_no_file_access() {
        // Rhai has no file I/O at all: a script that tries is a runtime
        // error, never a read of the gateway's disk.
        let k = kernel(
            r#"
            fn on_request_body(ctx) {
                let body = ctx["body"];
                body["stolen"] = open("/etc/passwd");
                body
            }
            "#,
        );
        let err = k
            .transform_request(&ctx(serde_json::json!({})))
            .expect_err("the script cannot read the disk");
        assert!(matches!(err, KernelError::Runtime(_)), "{err:?}");
    }
}
