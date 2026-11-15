# MaxLLM Autofix Playbook — 30-minute patrol + repair round

You are a fresh headless session. You have NO prior context. This file is the whole procedure.

PRIMARY STORE: git-ai (graft). Work in the graft workspace `~/opt/git-ai/ws/maxllm` and commit
with `graft commit -m "..." --land main`. GitHub (`git@github.com:oliveagle/maxllm`) is a
READ-ONLY MIRROR fed by the mirror timer — NEVER run `git push` to GitHub yourself; a landed
changeset reaches it within ~10 min (or force it:
`~/opt/git-ai/bin/mirror-to-github.sh maxllm.git git@github.com:oliveagle/maxllm.git`).
upstream chaitanya14/maxllm is NEVER pushed.

OPS CHECKOUT: the plain git clone at `/mnt/data/repos/github.com/oliveagle/maxllm-src` is for
interactive ops only. Before anything that reads it (deploy, audits), sync it from the mirror
(only when its working tree is clean):
`git -C /mnt/data/repos/github.com/oliveagle/maxllm-src fetch origin && git -C ... merge --ff-only origin/main`.

The gateway runs on NAS `oliveagle@oleNas01` (also 10.126.126.6), dir `~/opt/maxllm`, log `~/opt/maxllm/logs/maxllm.log`, metrics `http://127.0.0.1:3795/metrics`, health `http://127.0.0.1:3795/health`.

NOTE: the NAS host has no rg and the textual-filter word g-r-e-p is blocked by policy — filter its
files with awk or python3 over ssh. Locally use rg.

## The invariant (highest law)
Any translation-layer bug — schema dialect, tool plumbing, orphan calls, kernel faults — must NEVER
end the agent's turn. The client must always get a 2xx from some provider in the chain, or the
refusal must walk the chain to failover. A 400 reaching the client from a translation-class fault
is the ONLY class of bug you auto-fix here. Everything else is noise to classify, not fix.

## Layer model (fix at the right layer)
- **Runtime** (rarely changes): the Rust gateway `crates/maxllm-gateway`, `crates/maxllm-translate`.
  Fixes here need: quality gate -> commit -> push -> `scripts/deploy_maxllm.sh nas` (musl build, 7-10 min).
- **Kernel** (the updatable half): Rhai script `examples/kernel/moonshot/main.rhai`, loaded per
  `[translation_kernel]` in maxllm.toml. It owns the provider-dialect decision surface and
  hot-reloads on save (the gateway watches the script file — saving IS the deploy, no restart).
  Metrics: `maxllm_kernel_rewrites_total` (policy applied), `maxllm_kernel_faults_total` (script
  faults — must stay 0; a fault is answered as "no decision", the Rust baseline keeps running).
  The kernel may ONLY escalate a repair the baseline would also make; it can never disable a
  baseline repair. Edit the kernel when the fix is dialect/phrasing policy; edit Rust when the
  replay/failover mechanism itself is wrong.

## Round procedure

### 1. Run one fresh patrol round
```
bash ~/.maxllm/patrol/patrol_once.sh        # exit 0 = clean, 1 = findings, 2 = ssh failed
```
It prints FINDINGS / CONTEXT / NEW_SHAPES lines and appends them to `~/.maxllm/patrol/patrol.log`.
If exit is 2 -> log "ssh failed" and STOP (do not touch the repo on a blind round).
If exit is 0 -> you are done. Do not commit anything. Exit.

### 2. Triage each FINDINGS line
For every `F <status> <provider> <cause> | <log-line>`:
- `CLIENT_BODY_FAULT` / `ABSORBED_BY_CHAIN` / `CLIENT_ABORTED` -> absorbed noise. Log to
  `~/.maxllm/patrol/autofix.log` as CONTEXT and skip — do NOT fix.
- `UPSTREAM_FAULT` or `UNATTRIBUTED` -> real candidate. Pull the trace (NAS has no rg):
  ```
  ssh oliveagle@oleNas01 "awk '/<request_id>/' ~/opt/maxllm/logs/maxllm.log | tail -50"
  ```
  Also check `~/.maxllm/patrol/seen_shapes.txt` — a NEW_SHAPES entry that names a client-facing
  signature (refused / translate / tool / schema) repeated across >=2 rounds is a candidate even
  without a FINDINGS line yet.

### 3. Reproduce locally before touching code
Extract the exact upstream error body from the NAS recording:
`~/opt/maxllm/recordings/<date>/` holds per-request recordings; find the one by request_id
(the patrol F line shows the request id / time window). Get the verbatim upstream response body.
Dry-run the kernel classifier against it:
```
./scripts/kernel/check.sh --run examples/kernel/moonshot/main.rhai \
    --error-body '<verbatim body>' --status 400
```
If the kernel already returns the right repair (schema/orphan), the mechanism is fine — the bug is
upstream noise or a gap in the baseline walk; investigate that instead.

### 4. Fix at the right layer, then the quality gate
**Kernel fix** (preferred for dialect/phrasing):
- Edit `examples/kernel/moonshot/main.rhai`; add the new phrasing to `on_upstream_error` or the
  request quirk to `on_request_body`.
- Re-run the dry-run in step 3 — must now return the intended repair.
- Deploy: `scp examples/kernel/moonshot/main.rhai oliveagle@oleNas01:~/opt/maxllm/kernels/moonshot/main.rhai`
  (hot reload — no restart). Confirm via
  `ssh oliveagle@oleNas01 "curl -s http://127.0.0.1:3795/metrics | awk '/maxllm_kernel_/'"`
  (rewrites climbing, faults 0) and the gateway log shows the kernel reloaded
  (`ssh oliveagle@oleNas01 "awk '/[Kk]ernel/' ~/opt/maxllm/logs/maxllm.log | tail -5"`).
- Land the change in git-ai (English, conventional: `fix(kernel): ...`):
  `graft commit -m "fix(kernel): ..." --land main` (run inside the graft workspace; conflicts:
  `graft update` then `graft commit --land main --resolve`).
  Then force the mirror so GitHub sees it:
  `~/opt/git-ai/bin/mirror-to-github.sh maxllm.git git@github.com:oliveagle/maxllm.git`.

**Runtime fix** (mechanism-level only):
- Add a regression test in `crates/maxllm-gateway/tests/` that replays the observed chain
  (mirrors the existing "surviving refusal walks the chain" test style).
- `cargo test` (workspace), `cargo clippy -p maxllm-gateway --all-targets -- -D warnings`,
  `cargo fmt -p maxllm-gateway -- --check` (zero NEW drift vs HEAD).
- Land in git-ai, then deploy: `graft commit -m "fix(gateway): ..." --land main` (run inside the
  graft workspace; a conflict after `graft update` resolves with `--resolve`), then deploy:
  `setsid nohup bash scripts/deploy_maxllm.sh nas` from the graft workspace
  (MUST be setsid+nohup+& or the exec session kill takes the build down; the deploy script's
  CARGO_TARGET_DIR defaults outside the workspace, so the build never dirties it).
- After deploy: confirm NAS `systemctl --user status maxllm` active, health endpoint build id new,
  then run one more `patrol_once.sh` round — the same signature must now be CONTEXT, not FINDINGS.

### 5. Write the report
Append to `~/.maxllm/patrol/autofix.log`:
`<ts> round findings=<n> fix=<layer:what> commit=<sha> deployed=<build-id> verify=<patrol-after>`
If you fixed nothing (all findings were absorbed/context), still log one line:
`findings=<n> fix=none reason=<attribution>`.

## Hard rules
- NEVER let a translation bug end the agent loop — if in doubt, escalate the repair chain, never
  pass the 400 to the client.
- NEVER push to GitHub directly (it is the mirror — direct pushes race the mirror timer).
  NEVER push upstream (chaitanya14/maxllm). NEVER force push. Commit English, atomic, one fix.
- NEVER deploy from a dirty workspace; `graft status` must be clean (only intentional edits)
  before a Rust deploy — land first, then deploy.
- NEVER kill the always-on detector `patrol.sh` (it runs independently; your round is separate).
- One fix per round. If the fix is big, do the smallest safe kernel-layer mitigation now, note the
  structural follow-up in autofix.log, don't half-deploy a Rust change.
- fmt drift: compare `git show HEAD:<file> | rustfmt | diff` vs the working tree — the drift sets
  must match (no new drift introduced by you).
