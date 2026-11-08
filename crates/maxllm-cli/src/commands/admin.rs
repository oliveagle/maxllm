// Copyright 2025 MaxLLM Contributors.
// SPDX-License-Identifier: Apache-2.0

//! Offline admin operations that touch the SQLite store directly.
//!
//! These run against a stopped (or idle) gateway and do not need the admin
//! HTTP API, so they still work when `[admin] enabled = false` or when the
//! gateway is down for maintenance — the usual moment a rename happens.

use std::path::{Path, PathBuf};

use maxllm_admin::{AdminStore, ProviderRenameReport, SqliteStore};
use maxllm_config::Config;

/// Resolve the SQLite path from `--db` or the config's `[admin] db_path`.
fn resolve_db_path(db: Option<&Path>, config: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(path) = db {
        return Ok(path.to_path_buf());
    }
    let config_path = config.unwrap_or_else(|| Path::new("maxllm.toml"));
    let cfg = Config::from_file(config_path)
        .map_err(|e| format!("cannot read config {}: {e}", config_path.display()))?;
    match cfg.admin.as_ref().and_then(|a| a.db_path.clone()) {
        Some(path) if !path.is_empty() => Ok(PathBuf::from(path)),
        _ => Err(format!(
            "{} has no [admin] db_path; pass --db or point --config at the gateway config",
            config_path.display()
        )),
    }
}

/// Config-level sanity check for a rename.
///
/// The order matters: rename the provider in the config **first**, then move
/// the state here, then restart the gateway. If the config still knows the old
/// name (and not the new one), the moved rows would be pruned at the next
/// startup and the whole point of the migration is lost — so that ordering
/// mistake is an error unless `--force` is given.
fn check_config_order(
    config: Option<&Path>,
    from: &str,
    to: &str,
    force: bool,
) -> Result<Vec<String>, String> {
    let Some(config_path) = config else {
        return Ok(Vec::new());
    };
    let cfg = Config::from_file(config_path)
        .map_err(|e| format!("cannot read config {}: {e}", config_path.display()))?;
    if cfg.providers.is_empty() {
        return Ok(Vec::new());
    }
    let mut warnings = Vec::new();
    if !cfg.providers.contains_key(to) {
        let msg = format!(
            "config {} has no provider '{to}': rename it there first, then migrate",
            config_path.display()
        );
        if force {
            warnings.push(format!("{msg} (forced)"));
        } else {
            return Err(msg);
        }
    }
    if cfg.providers.contains_key(from) {
        let msg = format!(
            "config {} still defines provider '{from}': the gateway would keep using it and \
             the migrated rows would be pruned at the next startup",
            config_path.display()
        );
        if force {
            warnings.push(format!("{msg} (forced)"));
        } else {
            return Err(msg);
        }
    }
    Ok(warnings)
}

fn print_report(report: &ProviderRenameReport, dry_run: bool, json: bool) {
    if json {
        let doc = serde_json::json!({
            "dry_run": dry_run,
            "from": report.from,
            "to": report.to,
            "breaker_migrated": report.breaker_migrated,
            "breaker_merged": report.breaker_merged,
            "sessions_moved": report.sessions_moved,
        });
        println!("{doc}");
        return;
    }
    let verb = if dry_run { "would move" } else { "moved" };
    if report.breaker_migrated {
        let how = if report.breaker_merged {
            "merged (target already had state, stricter fields kept)"
        } else {
            "carried over"
        };
        println!(
            "circuit breaker: {verb} {} -> {} ({how})",
            report.from, report.to
        );
    } else {
        println!(
            "circuit breaker: no persisted state for '{}', nothing to move",
            report.from
        );
    }
    println!(
        "session pins: {verb} {} binding(s) from '{}' to '{}'",
        report.sessions_moved, report.from, report.to
    );
    if !dry_run {
        println!("restart the gateway so the new name is restored from the store");
    }
}

/// `maxllm admin rename-provider` — migrate a renamed provider's resilience state.
pub fn rename_provider(
    db: Option<&Path>,
    config: Option<&Path>,
    from: &str,
    to: &str,
    dry_run: bool,
    json: bool,
    force: bool,
) -> i32 {
    let from = from.trim();
    let to = to.trim();
    if from.is_empty() || to.is_empty() {
        eprintln!("Error: --from and --to must both be non-empty");
        return 2;
    }
    if from == to {
        eprintln!("Error: --from and --to name the same provider");
        return 2;
    }

    let warnings = match check_config_order(config, from, to, force) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Error: {e}");
            eprintln!("Hint: rename the provider block in the config first, then run this command, then restart.");
            return 2;
        }
    };
    for warning in &warnings {
        eprintln!("Warning: {warning}");
    }

    let db_path = match resolve_db_path(db, config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: {e}");
            return 2;
        }
    };
    if !db_path.exists() {
        eprintln!(
            "Error: no database at {} (is the gateway configured with [admin] db_path?)",
            db_path.display()
        );
        return 2;
    }

    // A dry run must not take a write lock on a database the gateway is
    // writing request logs into, so it opens read-only.
    let opened = if dry_run {
        SqliteStore::open_read_only(&db_path)
    } else {
        SqliteStore::open(&db_path)
    };
    let store = match opened {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: cannot open {}: {e}", db_path.display());
            return 1;
        }
    };

    if dry_run {
        // Read-only preview: the same decisions the real call would take,
        // without writing anything.
        let breaker = store
            .load_circuit_breaker_states()
            .map(|rows| rows.into_iter().find(|r| r.provider == from))
            .unwrap_or(None);
        let sessions = store
            .load_session_bindings()
            .map(|rows| rows.into_iter().filter(|(_, p)| p == from).count())
            .unwrap_or(0);
        let report = ProviderRenameReport {
            from: from.to_string(),
            to: to.to_string(),
            breaker_migrated: breaker.is_some(),
            breaker_merged: false,
            sessions_moved: sessions,
        };
        print_report(&report, true, json);
        return 0;
    }

    match store.rename_provider(from, to) {
        Ok(report) => {
            print_report(&report, false, json);
            0
        }
        Err(e) => {
            eprintln!("Error: rename failed: {e}");
            1
        }
    }
}
