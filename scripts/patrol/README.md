# maxllm patrol — ops scripts

The autopilot that keeps the "no translation bug ends the agent loop" invariant
true in production.

| file | role |
|------|------|
| `patrol_once.sh` | one patrol round: ssh to the NAS gateway, attribute every client-facing failure, print FINDINGS / CONTEXT / NEW_SHAPES. Exit 0 = clean. Driven by the 30-minute `codex-cron-maxllm-autofix` systemd timer. |
| `patrol_loop.sh` | the always-on detector loop (600 s cadence), appends every round to `~/.maxllm/patrol/patrol.log`. Installed copy runs from `~/.maxllm/patrol/`. |
| `remote.py` | the remote half that runs on the gateway host (NAS has no rg — all filtering is python/awk there). |
| `AUTOFIX_PLAYBOOK.md` | the procedure a fresh headless session follows each round: patrol -> triage -> fix at the right layer (kernel Rhai script preferred, Rust runtime for mechanism bugs) -> quality gate -> commit -> deploy -> verify -> report. |

## how the autopilot is wired

1. `systemd --user` timer `codex-cron-maxllm-autofix.timer` fires every 30 min
   (installed via the ole-cron skill; trigger script at
   `~/.local/state/codex-schedule/codex-cron-maxllm-autofix.sh`, serialized by
   `flock` on `~/.maxllm/patrol/autofix.lock`).
2. It runs `cx one-api-ch exec` in the graft workspace with the playbook message.
3. A clean round exits without touching the repo. A findings round fixes,
   gates, lands the changeset in **git-ai** (`graft commit --land main`), mirrors
   it to GitHub, deploys (kernel script hot-reload via scp, or
   `scripts/deploy_maxllm.sh nas` for Rust changes), verifies, and appends a
   report line to `~/.maxllm/patrol/autofix.log`.

## the primary store is git-ai; GitHub is a mirror

The code of record lives in git-ai (`maxllm.git`, PostgreSQL `graft_refs` + the
node object store). Commits are landed with `graft commit --land main` from the
graft workspace `~/opt/git-ai/ws/maxllm`. GitHub
(`git@github.com:oliveagle/maxllm.git`) is a **read-only mirror**: the systemd
timer `git-ai-mirror-maxllm.timer` rebuilds a plain git repo from the
authoritative state (objects from the node repo dir, refs from `graft_refs`) and
pushes heads+tags with `--prune`. Force it by hand with:

```
~/opt/git-ai/bin/mirror-to-github.sh maxllm.git git@github.com:oliveagle/maxllm.git
```

## install / reinstall on a fresh operator machine

```
mkdir -p ~/.maxllm/patrol
cp scripts/patrol/patrol_loop.sh    ~/.maxllm/patrol/patrol.sh
cp scripts/patrol/patrol_once.sh    ~/.maxllm/patrol/patrol_once.sh
cp scripts/patrol/remote.py         ~/.maxllm/patrol/remote.py
cp scripts/patrol/AUTOFIX_PLAYBOOK.md ~/.maxllm/patrol/AUTOFIX_PLAYBOOK.md
chmod +x ~/.maxllm/patrol/*.sh
setsid nohup bash ~/.maxllm/patrol/patrol.sh >> ~/.maxllm/patrol/patrol.log 2>&1 &
# then create the systemd timer with the ole-cron skill
```

Logs live in `~/.maxllm/patrol/` (`patrol.log` detector rounds, `autofix.log`
fix reports, `seen_shapes.txt` absorbed-noise shape set).
