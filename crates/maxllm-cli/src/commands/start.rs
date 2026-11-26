// Copyright 2025 MaxLLM Contributors.
// SPDX-License-Identifier: Apache-2.0

use nix::sys::stat::{umask, Mode};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Stdio;

/// Start the gateway server by spawning the `maxllm-server` binary.
pub fn run(config: PathBuf, daemon: bool) -> i32 {
    // Validate config exists
    if !config.exists() {
        eprintln!("Error: config file not found: {}", config.display());
        eprintln!("Run `maxllm init` to generate a starter configuration.");
        return 1;
    }

    // Validate the config can be parsed and decide which layer to launch.
    // With a [supervisor] section, the stable layer-1 entry process is the
    // entry point and it owns the layer-2 worker generations. Without one,
    // keep the original single-process behaviour.
    let supervised = match maxllm_config::Config::from_file(&config) {
        Ok(cfg) => cfg.supervisor.is_some(),
        Err(e) => {
            eprintln!("Error: invalid configuration: {e}");
            return 1;
        }
    };

    let server_bin = if supervised {
        match find_supervisor_binary() {
            Some(path) => path,
            None => {
                eprintln!("Error: [supervisor] is configured but the maxllm-supervisor binary was not found.");
                eprintln!("Build it with `cargo build --release` (it sits next to maxllm-server).");
                return 1;
            }
        }
    } else {
        find_server_binary()
    };

    let mut cmd = std::process::Command::new(&server_bin);
    cmd.arg("--config").arg(&config);

    if daemon {
        // Detach here instead of passing `--daemon` through. Pingora's
        // daemoniser forks inside `Server::run()`, i.e. after the binary has
        // already spawned its background threads (the config watcher; for the
        // supervisor, the lifecycle thread that parents every layer-2 worker
        // plus the control-API thread), and a fork there silently drops them.
        // A new process group with stdio on /dev/null detaches the same way,
        // without forking, and keeps the PID written below pointing at the
        // process that actually serves traffic.
        cmd.process_group(0);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
    }

    // Pingora's daemoniser would have applied umask(0o007) inside the forked
    // child. The detached child inherits the umask set here, so files the
    // gateway or supervisor creates (the admin store, recordings, sockets)
    // keep the same permissions as a pingora daemon.
    let old_umask = if daemon {
        Some(umask(Mode::from_bits_truncate(0o007)))
    } else {
        None
    };

    eprintln!(
        "Starting MaxLLM {}...",
        if supervised {
            "layer-1 supervisor"
        } else {
            "gateway"
        }
    );
    eprintln!("  Config: {}", config.display());
    eprintln!("  Binary: {}", server_bin.display());

    let pid_file = super::pid_file_path();

    let spawned = cmd.spawn();
    if let Some(mask) = old_umask {
        umask(mask);
    }

    match spawned {
        Ok(child) => {
            let pid = child.id();
            // Write PID file
            if let Err(e) = std::fs::write(&pid_file, pid.to_string()) {
                eprintln!("Warning: failed to write PID file: {e}");
            }
            if daemon {
                eprintln!("Gateway started in background (PID: {pid})");
                eprintln!("PID file: {}", pid_file.display());
                0
            } else {
                eprintln!("Gateway started (PID: {pid})");
                // Wait for the child process
                let mut child = child;
                match child.wait() {
                    Ok(status) => {
                        let _ = std::fs::remove_file(&pid_file);
                        status.code().unwrap_or(1)
                    }
                    Err(e) => {
                        eprintln!("Error waiting for gateway: {e}");
                        let _ = std::fs::remove_file(&pid_file);
                        1
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error: failed to start gateway: {e}");
            eprintln!("Looked for server binary at: {}", server_bin.display());
            1
        }
    }
}

fn find_supervisor_binary() -> Option<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        let dir = exe.parent().unwrap_or(std::path::Path::new("."));
        let candidate = dir.join("maxllm-supervisor");
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let path = PathBuf::from("maxllm-supervisor");
    if path.exists() {
        return Some(path);
    }
    None
}

fn find_server_binary() -> PathBuf {
    // Try same directory as current executable
    if let Ok(exe) = std::env::current_exe() {
        let dir = exe.parent().unwrap_or(std::path::Path::new("."));
        let candidate = dir.join("maxllm-server");
        if candidate.exists() {
            return candidate;
        }
    }
    // Fall back to PATH
    PathBuf::from("maxllm-server")
}
