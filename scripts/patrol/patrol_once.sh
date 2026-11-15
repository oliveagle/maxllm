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

# Guard the client-facing contract, not only gateway telemetry: a profile
# that silently drifts back to one-api bypasses every gateway repair and
# therefore looks clean to the gateway while it keeps killing agent loops.
CLIENT_CONFIG="${CLIENT_CONFIG:-$HOME/.agents/config/codex-settings/configs.d/proxy/one-api-local.toml}"
if ! rg -q 'base_url = "http://192\.168\.2\.190:3795/v1/responses/coding_resps"' "$CLIENT_CONFIG"; then
  printf 'CLIENT_PATH_FAULT %s\n' "$CLIENT_CONFIG"
  exit 3
fi

if printf '%s\n' "$out" | rg -q '^FINDINGS [1-9]'; then
  exit 1
fi
exit 0
