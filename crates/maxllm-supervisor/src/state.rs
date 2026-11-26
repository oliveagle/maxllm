// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Layer-1 state: the route table and the worker generations behind it.
//!
//! The public listener only ever consults [`Supervisor::routes`]; an upgrade
//! builds a new [`Generation`] entirely off to the side, waits for it to
//! answer `/health`, then atomically swaps the backend's `active` pointer.
//! In-flight requests keep their own `Arc<Generation>` (through an
//! [`InflightGuard`]), so the old worker is only asked to stop once every
//! request that entered through it has finished.

use arc_swap::ArcSwap;
use maxllm_config::{Config, SupervisorBackendConfig, SupervisorConfig, SupervisorRouteConfig};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::worker;

pub const GEN_ACTIVE: u8 = 0;
pub const GEN_DRAINING: u8 = 1;
pub const GEN_EXITED: u8 = 2;

/// Paths the gateway always serves, even when no `[[routes]]` entry names
/// them. Registering them here keeps `/health`, `/metrics`, the model list and
/// the admin API reachable through the entry layer.
pub const BUILTIN_PATHS: &[&str] = &[
    "/health",
    "/healthz",
    "/version",
    "/v1/models",
    "/metrics",
    "/admin",
];

/// One layer-2 worker process.
pub struct Generation {
    pub id: u64,
    pub backend: String,
    pub pid: u32,
    pub binary: PathBuf,
    pub socket_path: PathBuf,
    pub version: String,
    pub build_id: Option<String>,
    pub started_at: Instant,
    /// Requests that entered through this generation and have not finished.
    pub inflight: AtomicUsize,
    /// `GEN_ACTIVE`, `GEN_DRAINING` or `GEN_EXITED`.
    pub state: AtomicU8,
    /// The child handle is taken by the drain thread, which owns `wait()`.
    pub child: Mutex<Option<std::process::Child>>,
}

impl Generation {
    pub fn inflight_count(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    pub fn state_name(&self) -> &'static str {
        match self.state.load(Ordering::SeqCst) {
            GEN_ACTIVE => "active",
            GEN_DRAINING => "draining",
            _ => "exited",
        }
    }
}

/// RAII in-flight marker. It is dropped when the request context is dropped,
/// which pingora guarantees after the `logging` hook, so every exit path
/// (success, upstream error, locally generated 404) releases its generation.
pub struct InflightGuard(Arc<Generation>);

impl InflightGuard {
    pub fn new(gen: Arc<Generation>) -> Self {
        gen.inflight.fetch_add(1, Ordering::SeqCst);
        Self(gen)
    }

    pub fn generation(&self) -> &Arc<Generation> {
        &self.0
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Everything needed to start (or restart) one backend's worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendSpec {
    pub binary: PathBuf,
    pub config: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

/// One named layer-2 backend, e.g. `default`, `voice` or `image`.
pub struct Backend {
    pub name: String,
    pub spec: RwLock<BackendSpec>,
    /// The generation that receives new requests.
    pub active: ArcSwap<Generation>,
    /// Every generation still known to this process (active, draining, or
    /// waiting for its drain thread to reap it).
    pub generations: Mutex<Vec<Arc<Generation>>>,
    /// Serialises upgrades of this backend only; different backends can be
    /// upgraded concurrently.
    pub upgrade_lock: Mutex<()>,
}

impl Backend {
    fn new(name: String, spec: BackendSpec, gen: Arc<Generation>) -> Self {
        Self {
            name,
            spec: RwLock::new(spec),
            active: ArcSwap::from(gen.clone()),
            generations: Mutex::new(vec![gen]),
            upgrade_lock: Mutex::new(()),
        }
    }
}

/// Commands executed on the single long-lived lifecycle thread.
///
/// Every worker process is spawned from this thread. On Linux the worker's
/// `PR_SET_PDEATHSIG` is tied to the parent *thread*, so spawning from a
/// tokio blocking-pool thread (which is reaped after 10 idle seconds) would
/// SIGTERM the worker about 10s after a perfectly successful upgrade.
enum LifecycleCmd {
    Start {
        name: String,
        spec: BackendSpec,
        reply: Sender<Result<Arc<Generation>, String>>,
    },
    Upgrade {
        backend: Arc<Backend>,
        spec: BackendSpec,
        env: BTreeMap<String, String>,
        persist_spec: bool,
        reply: Sender<Result<Arc<Generation>, String>>,
    },
}

/// A path-prefix route table. Longer prefixes win, so `/v1/responses/auto`
/// can override `/v1/responses` regardless of declaration order.
#[derive(Default)]
pub struct RouteTable {
    pub routes: Vec<SupervisorRouteConfig>,
}

impl RouteTable {
    pub fn new(routes: Vec<SupervisorRouteConfig>) -> Self {
        // Later registrations win (the overlay is appended after the config
        // base). Deduplicate first, then sort longest prefix first.
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        for route in routes {
            map.insert(route.path, route.backend);
        }
        let mut routes: Vec<SupervisorRouteConfig> = map
            .into_iter()
            .map(|(path, backend)| SupervisorRouteConfig { path, backend })
            .collect();
        routes.sort_by(|a, b| {
            b.path
                .len()
                .cmp(&a.path.len())
                .then_with(|| a.path.cmp(&b.path))
        });
        Self { routes }
    }

    /// Longest registered prefix that matches `path` on a segment boundary.
    /// `/v1/responses` matches `/v1/responses/auto` but not
    /// `/v1/responsesXYZ`.
    pub fn resolve(&self, path: &str) -> Option<&SupervisorRouteConfig> {
        self.routes.iter().find(|r| prefix_matches(&r.path, path))
    }
}

fn prefix_matches(prefix: &str, path: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    if !path.starts_with(prefix) {
        return false;
    }
    matches!(path.as_bytes().get(prefix.len()), None | Some(b'/'))
}

/// Layer-1 process state, shared by the proxy, the control API and the
/// config watcher.
pub struct Supervisor {
    pub config_path: PathBuf,
    pub public_listen: String,
    pub control_listen: std::net::SocketAddr,
    pub socket_dir: PathBuf,
    pub routes_file: Option<PathBuf>,
    pub health_timeout: Duration,
    pub drain_timeout: Duration,
    pub default_worker_binary: PathBuf,
    pub default_worker_config: PathBuf,

    backends: RwLock<HashMap<String, Arc<Backend>>>,
    base_routes: RwLock<Vec<SupervisorRouteConfig>>,
    overlay_routes: RwLock<Vec<SupervisorRouteConfig>>,
    /// Effective table read by every request.
    pub routes: ArcSwap<RouteTable>,
    /// Serialises config reloads (which may add/remove backends).
    reconcile_lock: Mutex<()>,
    next_generation: AtomicU64,
    lifecycle_tx: Sender<LifecycleCmd>,
    pub started_at: Instant,
}

impl Supervisor {
    pub fn new(config_path: PathBuf, config: &Config, sup_cfg: &SupervisorConfig) -> Arc<Self> {
        let socket_dir = sup_cfg.socket_dir.clone().unwrap_or_else(|| {
            let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
            let name = config_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "maxllm.toml".into());
            parent.join(format!("{name}.supervisor.sock.d"))
        });
        let routes_file = Some(sup_cfg.routes_file.clone().unwrap_or_else(|| {
            let mut path = config_path.clone().into_os_string();
            path.push(".supervisor-routes.json");
            PathBuf::from(path)
        }));

        let default_worker_binary = sup_cfg
            .worker_binary
            .clone()
            .unwrap_or_else(default_worker_binary);
        let default_worker_config = sup_cfg
            .worker_config
            .clone()
            .unwrap_or_else(|| config_path.clone());

        let base_routes = derive_base_routes(config, sup_cfg);
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel();

        let sup = Arc::new(Self {
            config_path,
            public_listen: config.server.listen.to_string(),
            control_listen: sup_cfg.control_listen,
            socket_dir,
            routes_file,
            health_timeout: Duration::from_secs(sup_cfg.health_timeout_secs.max(1)),
            drain_timeout: Duration::from_secs(sup_cfg.drain_timeout_secs),
            default_worker_binary,
            default_worker_config,
            backends: RwLock::new(HashMap::new()),
            base_routes: RwLock::new(base_routes),
            overlay_routes: RwLock::new(Vec::new()),
            routes: ArcSwap::from_pointee(RouteTable::new(Vec::new())),
            reconcile_lock: Mutex::new(()),
            next_generation: AtomicU64::new(0),
            lifecycle_tx,
            started_at: Instant::now(),
        });

        // The lifecycle thread outlives every worker it spawns and therefore
        // keeps `PR_SET_PDEATHSIG` meaningful for the worker's whole life.
        let weak = Arc::downgrade(&sup);
        std::thread::Builder::new()
            .name("supervisor-lifecycle".into())
            .spawn(move || lifecycle_loop(weak, lifecycle_rx))
            .expect("spawn supervisor lifecycle thread");

        sup
    }

    pub fn next_generation_id(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn backend(&self, name: &str) -> Option<Arc<Backend>> {
        self.backends.read().ok()?.get(name).cloned()
    }

    pub fn backend_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .backends
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    fn insert_backend(&self, backend: Arc<Backend>) {
        if let Ok(mut map) = self.backends.write() {
            map.insert(backend.name.clone(), backend);
        }
    }

    /// Spawn and health-check the first generation of every configured
    /// backend. Called once, after daemonisation (so workers are children of
    /// the final process and `PR_SET_PDEATHSIG` is meaningful).
    pub fn start_all(&self, config: &Config, sup_cfg: &SupervisorConfig) -> Result<(), String> {
        std::fs::create_dir_all(&self.socket_dir).map_err(|e| {
            format!(
                "cannot create socket dir {}: {e}",
                self.socket_dir.display()
            )
        })?;
        // A previous supervisor run may have crashed and left sockets behind.
        // The directory is dedicated to this config file, so every `.sock` in
        // it is stale at startup.
        if let Ok(entries) = std::fs::read_dir(&self.socket_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("sock") {
                    let _ = std::fs::remove_file(path);
                }
            }
        }

        for name in desired_backend_names(sup_cfg) {
            let spec = self.resolve_spec(&name, sup_cfg);
            self.spawn_backend(&name, spec)?;
        }

        let _ = config; // kept for symmetry with reload_from_config
        Ok(())
    }

    pub fn resolve_spec(&self, name: &str, sup_cfg: &SupervisorConfig) -> BackendSpec {
        let entry: Option<&SupervisorBackendConfig> = sup_cfg.backends.get(name);
        BackendSpec {
            binary: entry
                .and_then(|b| b.binary.clone())
                .unwrap_or_else(|| self.default_worker_binary.clone()),
            config: entry
                .and_then(|b| b.config.clone())
                .unwrap_or_else(|| self.default_worker_config.clone()),
            args: entry.map(|b| b.args.clone()).unwrap_or_default(),
            env: entry
                .map(|b| b.env.clone().into_iter().collect())
                .unwrap_or_default(),
        }
    }

    /// Spawn a new backend on the lifecycle thread and register it.
    fn spawn_backend(&self, name: &str, spec: BackendSpec) -> Result<Arc<Generation>, String> {
        let (reply, rx) = mpsc::channel();
        self.lifecycle_tx
            .send(LifecycleCmd::Start {
                name: name.to_string(),
                spec,
                reply,
            })
            .map_err(|_| "lifecycle thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "lifecycle thread dropped the reply".to_string())?
    }

    /// Swap a backend to a new generation. Runs on the lifecycle thread.
    pub fn upgrade_generation(
        &self,
        backend: &Arc<Backend>,
        spec: &BackendSpec,
        extra_env: &BTreeMap<String, String>,
        persist_spec: bool,
    ) -> Result<Arc<Generation>, String> {
        let (reply, rx) = mpsc::channel();
        self.lifecycle_tx
            .send(LifecycleCmd::Upgrade {
                backend: backend.clone(),
                spec: spec.clone(),
                env: extra_env.clone(),
                persist_spec,
                reply,
            })
            .map_err(|_| "lifecycle thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "lifecycle thread dropped the reply".to_string())?
    }

    /// The actual swap. Only called on the lifecycle thread.
    fn upgrade_generation_inner(
        &self,
        backend: &Arc<Backend>,
        spec: &BackendSpec,
        extra_env: &BTreeMap<String, String>,
        persist_spec: bool,
    ) -> Result<Arc<Generation>, String> {
        let _upgrade = backend
            .upgrade_lock
            .lock()
            .map_err(|_| "backend upgrade lock poisoned".to_string())?;

        let gen = worker::spawn_generation(self, &backend.name, spec, extra_env)?;

        let old = backend.active.swap(gen.clone());
        old.state.store(GEN_DRAINING, Ordering::SeqCst);
        backend
            .generations
            .lock()
            .map_err(|_| "generation list lock poisoned".to_string())?
            .push(gen.clone());

        if persist_spec {
            if let Ok(mut current) = backend.spec.write() {
                *current = spec.clone();
            }
        }

        let timeout = self.drain_timeout;
        let old_clone = old.clone();
        std::thread::Builder::new()
            .name(format!("drain-{}", backend.name))
            .spawn(move || worker::drain_generation(old_clone, timeout))
            .map_err(|e| format!("cannot start drain thread: {e}"))?;

        info!(
            backend = %backend.name,
            old_generation = old.id,
            old_pid = old.pid,
            new_generation = gen.id,
            new_pid = gen.pid,
            version = %gen.version,
            build_id = gen.build_id.as_deref().unwrap_or(""),
            inflight_on_old = old.inflight_count(),
            "layer-2 generation swapped; old generation draining"
        );
        Ok(gen)
    }

    /// Imperative upgrade for the control API.
    pub fn upgrade(
        &self,
        backend_name: &str,
        binary: Option<PathBuf>,
        env: BTreeMap<String, String>,
    ) -> Result<Arc<Generation>, String> {
        let backend = self
            .backend(backend_name)
            .ok_or_else(|| format!("unknown backend '{backend_name}'"))?;
        let spec = {
            let current = backend
                .spec
                .read()
                .map_err(|_| "backend spec lock poisoned".to_string())?
                .clone();
            let mut spec = current;
            if let Some(binary) = binary {
                spec.binary = binary;
            }
            spec
        };
        self.upgrade_generation(&backend, &spec, &env, false)
    }

    fn stop_backend_inner(&self, backend: &Arc<Backend>) {
        let gens = backend
            .generations
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        for gen in gens {
            if gen.state.load(Ordering::SeqCst) == GEN_EXITED {
                continue;
            }
            gen.state.store(GEN_DRAINING, Ordering::SeqCst);
            let timeout = self.drain_timeout;
            let _ = std::thread::Builder::new()
                .name(format!("drain-removed-{}", backend.name))
                .spawn(move || worker::drain_generation(gen, timeout));
        }
    }

    /// Reconcile the running backends with the config: start new ones,
    /// hot-upgrade ones whose binary/config/args/env changed, stop removed
    /// ones.
    fn reconcile_backends(&self, sup_cfg: &SupervisorConfig) -> Result<Vec<String>, String> {
        let desired = desired_backend_names(sup_cfg);
        let mut actions = Vec::new();

        for name in &desired {
            let spec = self.resolve_spec(name, sup_cfg);
            match self.backend(name) {
                None => {
                    self.spawn_backend(name, spec)?;
                    actions.push(format!("started backend '{name}'"));
                }
                Some(backend) => {
                    let current = backend
                        .spec
                        .read()
                        .map_err(|_| "backend spec lock poisoned".to_string())?
                        .clone();
                    if current != spec {
                        self.upgrade_generation(&backend, &spec, &BTreeMap::new(), true)?;
                        actions.push(format!(
                            "hot-upgraded backend '{name}' to {}",
                            spec.binary.display()
                        ));
                    }
                }
            }
        }

        let desired_set: HashSet<&String> = desired.iter().collect();
        let existing = self.backend_names();
        for name in existing {
            if desired_set.contains(&name) {
                continue;
            }
            let removed = self
                .backends
                .write()
                .ok()
                .and_then(|mut map| map.remove(&name));
            if let Some(backend) = removed {
                self.stop_backend_inner(&backend);
                actions.push(format!("stopped removed backend '{name}'"));
            }
        }

        Ok(actions)
    }

    /// Re-read the shared config file. Provider changes are handled by the
    /// worker's own watcher; this function owns what layer 1 owns: the route
    /// table and the set/version of layer-2 backends.
    pub fn reload_from_config(&self) -> Result<String, String> {
        let _guard = self
            .reconcile_lock
            .lock()
            .map_err(|_| "supervisor reconcile lock poisoned".to_string())?;

        let config =
            Config::from_file(&self.config_path).map_err(|e| format!("invalid config: {e}"))?;
        let sup_cfg = config
            .supervisor
            .clone()
            .ok_or_else(|| "config no longer has a [supervisor] section".to_string())?;
        if sup_cfg.control_listen != self.control_listen {
            warn!(
                old = %self.control_listen,
                new = %sup_cfg.control_listen,
                "supervisor.control_listen changed; this needs a supervisor restart"
            );
        }

        let mut actions = self.reconcile_backends(&sup_cfg)?;

        let base = derive_base_routes(&config, &sup_cfg);
        if let Ok(mut guard) = self.base_routes.write() {
            *guard = base;
        }
        self.rebuild_routes();
        actions.push("route table refreshed".to_string());
        Ok(actions.join(", "))
    }

    pub fn rebuild_routes(&self) {
        let mut routes = self
            .base_routes
            .read()
            .map(|r| r.clone())
            .unwrap_or_default();
        if let Ok(overlay) = self.overlay_routes.read() {
            routes.extend(overlay.iter().cloned());
        }
        let table = RouteTable::new(routes);
        self.routes.store(Arc::new(table));
    }

    /// Load the route overlay written by the control API. Called once at
    /// startup, after the first backends are running.
    pub fn load_persisted_routes(&self) -> Result<(), String> {
        let Some(path) = self.routes_file.as_ref() else {
            return Ok(());
        };
        match std::fs::read(path) {
            Ok(bytes) => {
                let mut routes: Vec<SupervisorRouteConfig> = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("invalid route overlay {}: {e}", path.display()))?;
                let before = routes.len();
                routes.retain(|r| {
                    if self.backend(&r.backend).is_none() {
                        warn!(
                            path = %r.path,
                            backend = %r.backend,
                            "dropping persisted route for an unknown backend"
                        );
                        false
                    } else {
                        true
                    }
                });
                if routes.len() != before {
                    warn!(path = %path.display(), dropped = before - routes.len(), "persisted route overlay partially ignored");
                }
                if let Ok(mut guard) = self.overlay_routes.write() {
                    *guard = routes;
                }
                self.rebuild_routes();
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        }
        Ok(())
    }

    /// Replace the control-managed route overlay and persist it.
    pub fn set_overlay_routes(&self, routes: Vec<SupervisorRouteConfig>) -> Result<(), String> {
        for route in &routes {
            if !route.path.starts_with('/') {
                return Err(format!("route path '{}' must start with '/'", route.path));
            }
            if self.backend(&route.backend).is_none() {
                return Err(format!(
                    "route '{}' references unknown backend '{}'; running backends: {}",
                    route.path,
                    route.backend,
                    self.backend_names().join(", ")
                ));
            }
        }
        if let Some(path) = self.routes_file.as_ref() {
            let bytes = serde_json::to_vec_pretty(
                &routes
                    .iter()
                    .map(|r| serde_json::json!({"path": r.path, "backend": r.backend}))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| format!("cannot serialise routes: {e}"))?;
            let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
            std::fs::write(&tmp, &bytes)
                .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
            std::fs::rename(&tmp, path)
                .map_err(|e| format!("cannot replace {}: {e}", path.display()))?;
        }
        if let Ok(mut guard) = self.overlay_routes.write() {
            *guard = routes;
        }
        self.rebuild_routes();
        Ok(())
    }

    pub fn status_json(&self) -> serde_json::Value {
        let routes: Vec<serde_json::Value> = self
            .routes
            .load()
            .routes
            .iter()
            .map(|r| serde_json::json!({"path": r.path, "backend": r.backend}))
            .collect();

        let mut backends = serde_json::Map::new();
        let mut names = self.backend_names();
        names.sort();
        for name in names {
            let Some(backend) = self.backend(&name) else {
                continue;
            };
            let active = backend.active.load_full();
            let mut gens: Vec<serde_json::Value> = backend
                .generations
                .lock()
                .map(|g| {
                    g.iter()
                        .map(|gen| {
                            serde_json::json!({
                                "generation": gen.id,
                                "pid": gen.pid,
                                "binary": gen.binary.display().to_string(),
                                "state": gen.state_name(),
                                "version": gen.version,
                                "build_id": gen.build_id,
                                "inflight": gen.inflight_count(),
                                "socket": gen.socket_path.display().to_string(),
                                "uptime_secs": gen.started_at.elapsed().as_secs(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            gens.sort_by_key(|g| g["generation"].as_u64().unwrap_or(0));
            let spec = backend
                .spec
                .read()
                .map(|s| {
                    serde_json::json!({
                        "binary": s.binary.display().to_string(),
                        "config": s.config.display().to_string(),
                        "args": s.args,
                        "env_keys": s.env.keys().collect::<Vec<_>>(),
                    })
                })
                .unwrap_or(serde_json::Value::Null);
            backends.insert(
                name,
                serde_json::json!({
                    "active_generation": active.id,
                    "active_pid": active.pid,
                    "active_version": active.version,
                    "active_build_id": active.build_id,
                    "active_inflight": active.inflight_count(),
                    "spec": spec,
                    "generations": gens,
                }),
            );
        }

        serde_json::json!({
            "layer": 1,
            "role": "supervisor",
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "public_listen": self.public_listen,
            "control_listen": self.control_listen.to_string(),
            "uptime_secs": self.started_at.elapsed().as_secs(),
            "routes_file": self.routes_file.as_ref().map(|p| p.display().to_string()),
            "socket_dir": self.socket_dir.display().to_string(),
            "routes": routes,
            "backends": backends,
        })
    }
}

fn lifecycle_loop(weak: std::sync::Weak<Supervisor>, rx: Receiver<LifecycleCmd>) {
    while let Ok(cmd) = rx.recv() {
        let Some(sup) = weak.upgrade() else {
            break;
        };
        match cmd {
            LifecycleCmd::Start { name, spec, reply } => {
                let result = worker::spawn_generation(&sup, &name, &spec, &BTreeMap::new());
                if let Ok(gen) = &result {
                    sup.insert_backend(Arc::new(Backend::new(name, spec, gen.clone())));
                }
                let _ = reply.send(result);
            }
            LifecycleCmd::Upgrade {
                backend,
                spec,
                env,
                persist_spec,
                reply,
            } => {
                let result = sup.upgrade_generation_inner(&backend, &spec, &env, persist_spec);
                let _ = reply.send(result);
            }
        }
    }
}

/// The backends a config asks for. `default` always exists: builtin paths and
/// auto-derived routes target it, and a deployment can run named extra
/// backends (voice/image) without duplicating the entry layer.
pub fn desired_backend_names(sup_cfg: &SupervisorConfig) -> Vec<String> {
    if sup_cfg.backends.is_empty() {
        return vec!["default".to_string()];
    }
    let mut names: Vec<String> = sup_cfg.backends.keys().cloned().collect();
    if !names.iter().any(|n| n == "default") {
        names.push("default".to_string());
    }
    names.sort();
    names
}

/// Build the config-owned half of the route table.
pub fn derive_base_routes(
    config: &Config,
    sup_cfg: &SupervisorConfig,
) -> Vec<SupervisorRouteConfig> {
    let default = maxllm_config::default_supervisor_backend();
    let mut routes: Vec<SupervisorRouteConfig> = BUILTIN_PATHS
        .iter()
        .map(|path| SupervisorRouteConfig {
            path: (*path).to_string(),
            backend: default.clone(),
        })
        .collect();

    // Register every path the gateway config serves against the default
    // backend, then let `[supervisor].routes` override individual prefixes
    // (e.g. send `/v1/audio` to a dedicated `voice` backend). An override
    // therefore never drops the rest of the gateway's routes.
    for route in &config.routes {
        routes.push(SupervisorRouteConfig {
            path: route.path.clone(),
            backend: default.clone(),
        });
    }
    routes.extend(sup_cfg.routes.iter().cloned());
    routes
}

fn default_worker_binary() -> PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("maxllm-server");
            if candidate.exists() {
                return candidate;
            }
        }
    }
    PathBuf::from("maxllm-server")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(path: &str, backend: &str) -> SupervisorRouteConfig {
        SupervisorRouteConfig {
            path: path.into(),
            backend: backend.into(),
        }
    }

    #[test]
    fn longest_prefix_wins_on_segment_boundary() {
        let table = RouteTable::new(vec![
            r("/v1/responses", "default"),
            r("/v1/responses/auto", "auto"),
            r("/v1/audio", "voice"),
        ]);
        assert_eq!(table.resolve("/v1/responses").unwrap().backend, "default");
        assert_eq!(table.resolve("/v1/responses/auto").unwrap().backend, "auto");
        assert_eq!(
            table.resolve("/v1/responses/auto/child").unwrap().backend,
            "auto"
        );
        assert_eq!(
            table.resolve("/v1/responses/openai").unwrap().backend,
            "default"
        );
        // Segment boundary: a longer path that merely shares the string
        // prefix must not match.
        assert!(table.resolve("/v1/responsesXYZ").is_none());
        assert_eq!(table.resolve("/v1/audio/speech").unwrap().backend, "voice");
    }

    #[test]
    fn later_registration_replaces_the_same_path() {
        let table = RouteTable::new(vec![r("/v1/x", "a"), r("/v1/x", "b")]);
        assert_eq!(table.resolve("/v1/x").unwrap().backend, "b");
    }

    #[test]
    fn catch_all_matches_everything() {
        let table = RouteTable::new(vec![r("/", "default")]);
        assert_eq!(table.resolve("/anything").unwrap().backend, "default");
        assert_eq!(table.resolve("/").unwrap().backend, "default");
    }
}
