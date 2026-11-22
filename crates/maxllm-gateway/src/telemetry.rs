// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Native OpenTelemetry export.
//!
//! This is the "the gateway ships its own telemetry" half of the pipeline:
//! every `tracing` event becomes an OTLP **log record** (real
//! `severity_text`/`severity_number`, unlike a file tail that only sees the
//! rendered text) and every request span becomes an OTLP **span**. Both leave
//! the process over OTLP/HTTP to the endpoint in `[otel]` — by default
//! GreptimeDB itself, so nothing has to tail a log file or run a sidecar.
//!
//! Two design points that are easy to get wrong:
//!
//! * **Logs and traces need separate exporters.** GreptimeDB's trace pipeline
//!   is selected by `X-Greptime-Pipeline-Name: greptime_trace_v1`; sending
//!   that same header to `/v1/logs` is rejected with *400 Unsupported pipeline
//!   for logs*. Hence `logs_headers` / `traces_headers` in the config, and two
//!   exporters here.
//! * **The log attributes must carry `log_source`/`machine`.** GreptimeDB only
//!   turns a log attribute into a queryable column when the writer names it in
//!   `X-Greptime-Log-Extract-Keys`; a resource attribute does not become a
//!   column. So every record gets those two attributes from the exporter, not
//!   from the call site.
//!
//! The file/stdout `fmt` layer is never replaced: the OTel layers are *added*
//! to it, so an operator's `maxllm.log` keeps working byte-for-byte and a
//! failed OTel exporter cannot cost the gateway its logs.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use maxllm_config::OtelConfig;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::trace::{SpanContext, TraceContextExt as _, TracerProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::{runtime, Resource};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

/// Settings after environment overrides have been applied.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub enabled: bool,
    pub endpoint: String,
    pub service_name: String,
    pub machine: String,
    pub username: String,
    pub password: String,
    pub logs: bool,
    pub traces: bool,
    pub sample_ratio: f64,
    pub log_source: String,
    pub headers: HashMap<String, String>,
    pub logs_headers: HashMap<String, String>,
    pub traces_headers: HashMap<String, String>,
    pub filter: String,
}

/// Environment overrides, so the password can live in the service's 0600
/// `EnvironmentFile` instead of the world-readable config.
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

pub fn resolve(cfg: &OtelConfig) -> Resolved {
    let enabled = match env("MAXLLM_OTEL_ENABLED") {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        None => cfg.enabled,
    };
    let machine = env("MAXLLM_OTEL_MACHINE")
        .or_else(|| {
            let configured = cfg.machine.trim();
            (!configured.is_empty()).then(|| configured.to_string())
        })
        .unwrap_or_else(hostname);
    Resolved {
        enabled,
        endpoint: env("MAXLLM_OTEL_ENDPOINT")
            .unwrap_or_else(|| cfg.endpoint.clone())
            .trim_end_matches('/')
            .to_string(),
        service_name: cfg.service_name.clone(),
        machine,
        username: env("MAXLLM_OTEL_USERNAME").unwrap_or_else(|| cfg.username.clone()),
        password: env("MAXLLM_OTEL_PASSWORD").unwrap_or_else(|| cfg.password.clone()),
        logs: cfg.logs,
        traces: cfg.traces,
        sample_ratio: cfg.sample_ratio,
        log_source: cfg.log_source.clone(),
        headers: cfg.headers.clone(),
        logs_headers: cfg.logs_headers.clone(),
        traces_headers: cfg.traces_headers.clone(),
        filter: cfg.filter.clone(),
    }
}

fn hostname() -> String {
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.trim().is_empty() {
            return h;
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Keeps the exporter runtime and the providers alive.
///
/// The batch processors spawn their export tasks on this runtime; dropping the
/// providers first (which flushes) and the runtime last is what makes the
/// final records survive a `SIGTERM` — `server.run_forever()` returns on the
/// drain path, then `shutdown()` runs.
pub struct Telemetry {
    logger_provider: Option<LoggerProvider>,
    tracer_provider: Option<TracerProvider>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Telemetry {
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("otel: tracer provider shutdown failed: {err}");
            }
        }
        if let Some(provider) = self.logger_provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("otel: logger provider shutdown failed: {err}");
            }
        }
        self.runtime.take();
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Install the subscriber: `fmt` (unchanged) + OTLP logs + OTLP traces.
///
/// Returns `None` when `[otel]` is absent or disabled — the caller then keeps
/// the plain `fmt` subscriber.
pub fn install(cfg: &OtelConfig) -> Result<Option<Telemetry>, String> {
    let cfg = resolve(cfg);
    if !cfg.enabled {
        return Ok(None);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("maxllm-otel")
        .build()
        .map_err(|e| format!("failed to build the OTel runtime: {e}"))?;
    // The batch processors spawn their tasks on this runtime; enter it while
    // building them, then keep the runtime alive in `Telemetry`.
    let _entered = runtime.enter();

    let resource = Resource::new_with_defaults(vec![
        KeyValue::new("service.name", cfg.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("host.name", cfg.machine.clone()),
        KeyValue::new("machine", cfg.machine.clone()),
        KeyValue::new("log_source", cfg.log_source.clone()),
    ]);

    configure_export_env(&cfg);

    let mut logger_provider = None;
    let mut log_layer: Option<OtelLogLayer> = None;
    if cfg.logs {
        let exporter = log_exporter(&cfg)?;
        let provider = LoggerProvider::builder()
            .with_batch_exporter(exporter, runtime::Tokio)
            .with_resource(resource.clone())
            .build();
        log_layer = Some(OtelLogLayer {
            logger: provider.logger("maxllm"),
            log_source: cfg.log_source.clone(),
            machine: cfg.machine.clone(),
        });
        logger_provider = Some(provider);
    }

    let mut tracer_provider = None;
    let mut trace_layer = None;
    if cfg.traces {
        let exporter = span_exporter(&cfg)?;
        let ratio = cfg.sample_ratio.clamp(0.0, 1.0);
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, runtime::Tokio)
            .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
                opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(ratio),
            )))
            .with_resource(resource.clone())
            .build();
        trace_layer = Some(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("maxllm"))
                .with_location(false),
        );
        tracer_provider = Some(provider);
    }

    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let otel_filter = EnvFilter::new(&cfg.filter);

    let mut layers: Vec<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync>> =
        vec![Box::new(
            tracing_subscriber::fmt::layer().with_filter(fmt_filter),
        )];
    if let Some(layer) = log_layer {
        layers.push(Box::new(layer.with_filter(otel_filter.clone())));
    }
    if let Some(layer) = trace_layer {
        layers.push(Box::new(layer.with_filter(otel_filter)));
    }

    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| format!("failed to install the tracing subscriber: {e}"))?;

    Ok(Some(Telemetry {
        logger_provider,
        tracer_provider,
        runtime: Some(runtime),
    }))
}

fn signal_headers(cfg: &Resolved, extra: &HashMap<String, String>) -> HashMap<String, String> {
    let mut headers = cfg.headers.clone();
    headers.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    if !cfg.username.is_empty() {
        use base64::Engine as _;
        let raw = format!("{}:{}", cfg.username, cfg.password);
        headers.insert(
            "Authorization".to_string(),
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(raw)
            ),
        );
    }
    headers
}

fn log_exporter(cfg: &Resolved) -> Result<opentelemetry_otlp::LogExporter, String> {
    opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(format!("{}/v1/logs", cfg.endpoint))
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("failed to build the OTLP log exporter: {e}"))
}

fn span_exporter(cfg: &Resolved) -> Result<opentelemetry_otlp::SpanExporter, String> {
    opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(format!("{}/v1/traces", cfg.endpoint))
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("failed to build the OTLP span exporter: {e}"))
}

/// Flatten headers into the comma-separated `k=v` form the OTLP HTTP exporter
/// parses from its `*_HEADERS` environment variables.
///
/// The builder's own `with_headers` is unusable here: it zips the *existing*
/// header map with the new one, so with more than one header only the first is
/// kept (and which one is arbitrary). The environment form is parsed in full.
fn header_env(headers: &HashMap<String, String>) -> String {
    headers
        .iter()
        .map(|(key, value)| {
            let escaped = value.replace('%', "%25").replace(',', "%2C");
            format!("{key}={escaped}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Make the exporters read *our* settings, not whatever `OTEL_*` variables
/// happen to be in the service's environment.
///
/// `opentelemetry-otlp` gives the environment priority over the builder
/// (`resolve_http_endpoint` checks `OTEL_EXPORTER_OTLP_*_ENDPOINT` first, then
/// the generic one, and only then the endpoint passed to `with_endpoint`).
/// A host that exports telemetry of its own — e.g. an `OTEL_EXPORTER_OTLP_ENDPOINT`
/// left over from a Go demo — would therefore silently redirect this gateway's
/// logs and traces. This runs before any thread is spawned, so the process
/// environment is still single-threaded here.
fn configure_export_env(cfg: &Resolved) {
    for var in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_SERVICE_NAME",
        "OTEL_RESOURCE_ATTRIBUTES",
    ] {
        std::env::remove_var(var);
    }
    if cfg.logs {
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            format!("{}/v1/logs", cfg.endpoint),
        );
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
            header_env(&signal_headers(cfg, &cfg.logs_headers)),
        );
    }
    if cfg.traces {
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            format!("{}/v1/traces", cfg.endpoint),
        );
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            header_env(&signal_headers(cfg, &cfg.traces_headers)),
        );
    }
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
}

/// `tracing` events → OTLP log records.
struct OtelLogLayer {
    logger: opentelemetry_sdk::logs::Logger,
    log_source: String,
    machine: String,
}

impl<S> Layer<S> for OtelLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let mut record = self.logger.create_log_record();
        record.set_target(event.metadata().target());
        record.set_timestamp(SystemTime::now());
        let (severity, text) = severity_for(event.metadata().level());
        record.set_severity_number(severity);
        record.set_severity_text(text);

        let body = visitor
            .message
            .clone()
            .unwrap_or_else(|| event.metadata().target().to_string());
        record.set_body(AnyValue::String(body.into()));

        // GreptimeDB only turns log attributes into columns, so both of these
        // have to be attributes on the record itself (not resource attrs).
        record.add_attribute("log_source", self.log_source.clone());
        record.add_attribute("machine", self.machine.clone());
        for (key, value) in visitor.fields {
            record.add_attribute(key, value);
        }

        if let Some(context) = current_span_context() {
            record.set_trace_context(
                context.trace_id(),
                context.span_id(),
                Some(context.trace_flags()),
            );
        }

        self.logger.emit(record);
    }
}

fn current_span_context() -> Option<SpanContext> {
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    let context = tracing::Span::current().context();
    let span_context = context.span().span_context().clone();
    span_context.is_valid().then_some(span_context)
}

fn severity_for(level: &tracing::Level) -> (Severity, &'static str) {
    match *level {
        tracing::Level::ERROR => (Severity::Error, "ERROR"),
        tracing::Level::WARN => (Severity::Warn, "WARN"),
        tracing::Level::INFO => (Severity::Info, "INFO"),
        tracing::Level::DEBUG => (Severity::Debug, "DEBUG"),
        tracing::Level::TRACE => (Severity::Trace, "TRACE"),
    }
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    fn push(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.push((field.name().to_string(), value));
        }
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(field, format!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_mapping_is_total() {
        assert_eq!(
            severity_for(&tracing::Level::ERROR),
            (Severity::Error, "ERROR")
        );
        assert_eq!(
            severity_for(&tracing::Level::WARN),
            (Severity::Warn, "WARN")
        );
        assert_eq!(
            severity_for(&tracing::Level::INFO),
            (Severity::Info, "INFO")
        );
        assert_eq!(
            severity_for(&tracing::Level::DEBUG),
            (Severity::Debug, "DEBUG")
        );
        assert_eq!(
            severity_for(&tracing::Level::TRACE),
            (Severity::Trace, "TRACE")
        );
    }

    #[test]
    fn disabled_config_yields_no_telemetry() {
        let cfg = OtelConfig::default();
        assert!(install(&cfg).expect("install").is_none());
    }

    #[test]
    fn env_override_wins_and_trims_endpoint() {
        std::env::set_var("MAXLLM_OTEL_ENDPOINT", "http://127.0.0.1:9999/v1/otlp/");
        std::env::set_var("MAXLLM_OTEL_MACHINE", "testbox");
        let cfg = OtelConfig {
            enabled: true,
            ..OtelConfig::default()
        };
        let resolved = resolve(&cfg);
        assert_eq!(resolved.endpoint, "http://127.0.0.1:9999/v1/otlp");
        assert_eq!(resolved.machine, "testbox");
        std::env::remove_var("MAXLLM_OTEL_ENDPOINT");
        std::env::remove_var("MAXLLM_OTEL_MACHINE");
    }
}
