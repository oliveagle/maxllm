#!/usr/bin/env bash
# patrol_once.sh — run exactly one patrol round, then exit.
# Used by the 30-minute autofix timer; the always-on patrol.sh loop keeps
# running independently (its 600 s cadence is the cheap detector).
# Exit code: 0 = no findings, 1 = findings present.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SEEN="${PATROL_SEEN:-$HOME/.maxllm/patrol/seen_shapes.txt}"
HOST="${PATROL_HOST:-oliveagle@oleNas01}"

seen_b64=""
[ -f "$SEEN" ] && seen_b64=$(base64 -w0 < "$SEEN")
out=$(ssh -o BatchMode=yes -o ConnectTimeout=15 "$HOST" python3 - "$seen_b64" < "$HERE/remote.py") || {
  echo "PATROL_ERROR ssh or python failed"
  exit 2
}

printf '%s\n' "$out"
printf '%s\n' "$out" >> "$HERE/patrol.log"

# absorb the shapes this round reported (append-only, deduped here)
printf '%s\n' "$out" | rg '^  N ' | sed 's/^  N //' >> "$SEEN"

if printf '%s\n' "$out" | rg -q '^FINDINGS [1-9]'; then
  exit 1
fi
exit 0
