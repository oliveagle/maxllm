// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Layer-2 process lifecycle: spawn, health-check, drain and reap.
//!
//! The worker is the existing `maxllm-server` binary, told to listen on a
//! private Unix socket with `--listen unix:<path>`. Every generation gets a
//! fresh path, so a new generation can bind while the previous one is still
//! serving its in-flight requests.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::state::{BackendSpec, Generation, Supervisor, GEN_ACTIVE, GEN_EXITED};

struct HealthInfo {
    version: String,
    build_id: Option<String>,
}

/// Spawn one worker generation and block until it answers `/health`.
///
/// A generation that never becomes healthy is killed before returning, so a
/// failed upgrade leaves the previous generation untouched and running.
pub fn spawn_generation(
    sup: &Supervisor,
    backend: &str,
    spec: &BackendSpec,
    extra_env: &BTreeMap<String, String>,
) -> Result<Arc<Generation>, String> {
    let id = sup.next_generation_id();
    let safe_backend: String = backend
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let socket_path = sup.socket_dir.join(format!("{safe_backend}-g{id}.sock"));
    let _ = std::fs::remove_file(&socket_path);

    let mut cmd = Command::new(&spec.binary);
    cmd.arg("--config")
        .arg(&spec.config)
        .arg("--listen")
        .arg(format!("unix:{}", socket_path.display()));
    for arg in &spec.args {
        cmd.arg(arg);
    }
    cmd.envs(&spec.env).envs(extra_env);
    cmd.stdin(Stdio::null());
    // Inherit stdout/stderr: worker logs stay in the supervisor's log stream
    // (and in the daemon's error log), which is the only place an operator
    // looks when a generation fails to start.
    cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

    // If the supervisor dies, the kernel sends SIGTERM to the worker. Without
    // this a crashed supervisor leaves a worker holding the private socket
    // and its upstream connections.
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            // Close the race where the parent died between fork and prctl.
            if libc::getppid() == 1 {
                libc::_exit(1);
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().map_err(|e| {
        format!(
            "failed to spawn worker '{}' for backend '{backend}': {e}",
            spec.binary.display()
        )
    })?;
    let pid = child.id();
    let started = Instant::now();
    let deadline = started + sup.health_timeout;

    loop {
        if let Some(health) = probe_health(&socket_path) {
            info!(
                backend = %backend,
                generation = id,
                pid,
                version = %health.version,
                build_id = health.build_id.as_deref().unwrap_or(""),
                elapsed_ms = started.elapsed().as_millis(),
                "layer-2 generation is healthy"
            );
            return Ok(Arc::new(Generation {
                id,
                backend: backend.to_string(),
                pid,
                binary: spec.binary.clone(),
                socket_path,
                version: health.version,
                build_id: health.build_id,
                started_at: Instant::now(),
                inflight: AtomicUsize::new(0),
                state: AtomicU8::new(GEN_ACTIVE),
                child: Mutex::new(Some(child)),
            }));
        }

        if let Ok(Some(status)) = child.try_wait() {
            let _ = std::fs::remove_file(&socket_path);
            return Err(format!(
                "worker for backend '{backend}' exited during startup ({status}); see its log above"
            ));
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_file(&socket_path);
            return Err(format!(
                "worker for backend '{backend}' did not answer /health within {}s",
                sup.health_timeout.as_secs()
            ));
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

fn probe_health(socket_path: &Path) -> Option<HealthInfo> {
    let mut stream = UnixStream::connect(socket_path).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1500)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(1500)))
        .ok()?;
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .ok()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let text = String::from_utf8_lossy(&response);
    let status_line = text.lines().next()?;
    if !status_line.contains(" 200 ") {
        return None;
    }
    let body = text.split("\r\n\r\n").nth(1)?;
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    Some(HealthInfo {
        version: value.get("version")?.as_str()?.to_string(),
        build_id: value
            .get("build_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

/// Wait for every request that entered through `gen` to finish, then ask the
/// worker to shut down. Runs on its own thread so the upgrade response does
/// not wait for long-lived streams.
pub fn drain_generation(gen: Arc<Generation>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while gen.inflight_count() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let remaining = gen.inflight_count();
    if remaining > 0 {
        warn!(
            backend = %gen.backend,
            generation = gen.id,
            pid = gen.pid,
            remaining,
            "drain timeout reached; telling the old generation to stop with requests still in flight"
        );
    } else {
        info!(
            backend = %gen.backend,
            generation = gen.id,
            pid = gen.pid,
            waited_ms = timeout.saturating_sub(deadline.saturating_duration_since(Instant::now())).as_millis(),
            "old generation drained"
        );
    }

    // SIGTERM lets pingora finish anything still in flight within its own
    // grace period; after 5s it is gone.
    unsafe {
        libc::kill(gen.pid as i32, libc::SIGTERM);
    }

    let child = gen.child.lock().ok().and_then(|mut c| c.take());
    if let Some(mut child) = child {
        wait_with_timeout(&mut child, Duration::from_secs(5));
    }
    gen.state.store(GEN_EXITED, Ordering::SeqCst);
    let _ = std::fs::remove_file(&gen.socket_path);
    info!(
        backend = %gen.backend,
        generation = gen.id,
        pid = gen.pid,
        "old generation exited"
    );
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}
