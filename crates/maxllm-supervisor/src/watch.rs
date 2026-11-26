// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Config-file watcher for layer 1.
//!
//! The worker already watches the same file for provider/route changes. The
//! supervisor watches it for the things *it* owns: which path prefixes are
//! registered and which layer-2 backend versions should be running. Both
//! reloads are atomic; neither closes the public listener.

use notify::{EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info};

use crate::state::Supervisor;

/// Coalesce a burst of editor events into one reload, but never *drop* the
/// last event: a second edit made inside the quiet window must still be
/// applied. A hard deadline bounds how long a continuous stream of events can
/// postpone a reload.
const QUIET: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(3);

pub fn start(config_path: PathBuf, sup: Arc<Supervisor>) {
    let watch_path = config_path.clone();
    std::thread::Builder::new()
        .name("supervisor-config-watch".into())
        .spawn(move || {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = match notify::recommended_watcher(tx) {
                Ok(w) => w,
                Err(e) => {
                    error!(error = %e, "failed to create supervisor config watcher");
                    return;
                }
            };
            let watch_dir = watch_path.parent().unwrap_or(&watch_path);
            if let Err(e) = watcher.watch(watch_dir, RecursiveMode::NonRecursive) {
                error!(error = %e, path = ?watch_dir, "failed to watch config directory");
                return;
            }
            info!(path = ?config_path, "layer-1 config watcher started");

            let mut dirty = false;
            let mut first_dirty = Instant::now();
            let mut deadline = Instant::now();

            loop {
                let event = if dirty {
                    let now = Instant::now();
                    if now >= deadline || now.saturating_duration_since(first_dirty) >= MAX_DELAY {
                        dirty = false;
                        info!("config file changed; refreshing layer-1 routes/backends");
                        match sup.reload_from_config() {
                            Ok(summary) => info!(summary, "layer-1 config reloaded"),
                            Err(e) => error!(
                                error = %e,
                                "layer-1 config reload failed; keeping the current routes and generations"
                            ),
                        }
                        continue;
                    }
                    match rx.recv_timeout(deadline.saturating_duration_since(now)) {
                        Ok(Ok(ev)) => ev,
                        Ok(Err(e)) => {
                            error!(error = %e, "supervisor config watcher error");
                            continue;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                } else {
                    match rx.recv() {
                        Ok(Ok(ev)) => ev,
                        Ok(Err(e)) => {
                            error!(error = %e, "supervisor config watcher error");
                            continue;
                        }
                        Err(_) => break,
                    }
                };

                let relevant = matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_))
                    && event.paths.iter().any(|p| {
                        p.canonicalize().ok().as_ref() == Some(&config_path)
                            || p.file_name() == config_path.file_name()
                    });
                if relevant {
                    if !dirty {
                        first_dirty = Instant::now();
                    }
                    dirty = true;
                    deadline = Instant::now() + QUIET;
                }
            }
        })
        .ok();
}
