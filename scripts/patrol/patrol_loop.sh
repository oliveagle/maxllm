#!/usr/bin/env bash
# maxllm patrol — every 10 minutes. The detection logic lives in remote.py
# (next to this script); the loop here is only the schedule and the log.
set -uo pipefail

LOGFILE="${PATROL_LOG:-$HOME/.maxllm/patrol/patrol.log}"
SEEN="${PATROL_SEEN:-$HOME/.maxllm/patrol/seen_shapes.txt}"
HOST="${PATROL_HOST:-oliveagle@oleNas01}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT_CONFIG="${CLIENT_CONFIG:-$HOME/.agents/config/codex-settings/configs.d/proxy/one-api-local.toml}"

client_path_ok() {
  rg -q 'base_url = "http://192\.168\.2\.190:3795/v1/responses/coding_resps"' "$CLIENT_CONFIG"
}

log() { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOGFILE"; }

round() {
  local out seen_b64=""
  [ -f "$SEEN" ] && seen_b64=$(base64 -w0 < "$SEEN")
  if ! out=$(ssh -o BatchMode=yes "$HOST" python3 - "$seen_b64" < "$HERE/remote.py"); then
    log "ssh or python failed — skipping this round"
    return
  fi
  printf '%s\n' "$out" | while IFS= read -r l; do log "  $l"; done
  if ! client_path_ok; then
    log "CLIENT_PATH_FAULT $CLIENT_CONFIG"
  fi
  # remember the shapes this round reported (append-only, deduped here)
  printf '%s\n' "$out" | rg '^  N ' | sed 's/^  N //' >> "$SEEN"
}

if [[ "${1:-}" == "--once" ]]; then
  round
  exit 0
fi

log "patrol started (pid $$), every 600s"
round
while true; do sleep 600; round; done
