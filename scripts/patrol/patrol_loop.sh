#!/usr/bin/env bash
# maxllm patrol — every 10 minutes. The detection logic lives in remote.py
# (next to this script); the loop here is only the schedule and the log.
set -uo pipefail

LOGFILE="${PATROL_LOG:-$HOME/.maxllm/patrol/patrol.log}"
SEEN="${PATROL_SEEN:-$HOME/.maxllm/patrol/seen_shapes.txt}"
HOST="${PATROL_HOST:-oliveagle@oleNas01}"
HERE="$(cd "$(dirname "$0")" && pwd)"

log() { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOGFILE"; }

round() {
  local out seen_b64=""
  [ -f "$SEEN" ] && seen_b64=$(base64 -w0 < "$SEEN")
  if ! out=$(ssh -o BatchMode=yes "$HOST" python3 - "$seen_b64" < "$HERE/remote.py"); then
    log "ssh or python failed — skipping this round"
    return
  fi
  printf '%s\n' "$out" | while IFS= read -r l; do log "  $l"; done
  # remember the shapes this round reported (append-only, deduped here)
  printf '%s\n' "$out" | rg '^  N ' | sed 's/^  N //' >> "$SEEN"
}

log "patrol started (pid $$), every 600s"
round
while true; do sleep 600; round; done
