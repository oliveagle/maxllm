// Copyright 2025 MaxLLM Contributors.
// SPDX-License-Identifier: Apache-2.0

pub mod admin;
pub mod config;
pub mod health;
pub mod init;
pub mod kernel;
pub mod keys;
pub mod start;
pub mod status;
pub mod stop;
pub mod test;

use std::path::PathBuf;

/// Return the path to the PID file: `~/.maxllm/maxllm.pid`.
/// Creates the `~/.maxllm/` directory if it does not exist.
pub fn pid_file_path() -> PathBuf {
    let dir = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".maxllm");
    let _ = std::fs::create_dir_all(&dir);
    dir.join("maxllm.pid")
}
