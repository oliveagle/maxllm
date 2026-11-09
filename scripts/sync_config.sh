#!/usr/bin/env bash
# sync_config.sh — keep this node's pool config in step with the source of truth.
#
# Why this exists. The pool is edited in ONE place (the source-of-truth node)
# and every node has to learn the same edit — otherwise the nodes serve
# different capacity and the dashboards compare apples with oranges. Copying
# the file by hand is how they drift: on 2026-11-09 one provider carried
# `read_timeout_secs = 45` on the NAS and `60` on Fedora — the same provider,
# two behaviours, and nobody knew until the two nodes' numbers were read
# side by side.
#
# The copy is safe to run unattended:
#   * the file is fetched to a temporary path first and is only installed
#     when it parses as TOML, so a half-written source never lands;
#   * it is installed atomically (write, then rename), so the gateway's
#     watcher never reads a partial file;
#   * the previous file is kept as a timestamped backup;
#   * the gateway hot-reloads it — no restart, no dropped connections, and
#     the breaker state (including a quota force-open) survives the reload.
#
# The peer list is the same on every node, and each node skips the peer that
# reports its own id (see `breaker_sync`), so a copied config keeps the
# cross-node sharing working. The node's own identity lives in the env file
# (`MAXLLM_NODE_ID`), which is node-local and never synced — a copied id
# would make two nodes believe they are the same node.
#
# The source of truth wins: an edit made directly on this node is reverted
# by the next sync (and kept as a backup). Edit the source, not the copy.
#
# Usage:
#   scripts/sync_config.sh              # fetch + install when it differs
#   scripts/sync_config.sh --check      # report what would happen, change nothing
#
#   SOURCE=oleNas01 DEST=/path/maxllm.toml scripts/sync_config.sh
set -euo pipefail

SOURCE="${SOURCE:-oleNas01}"
DEST="${DEST:-/home/oliveagle/opt/maxllm/maxllm.toml}"
LOG="${SYNC_LOG:-/home/oliveagle/opt/maxllm/logs/config-sync.log}"
CHECK_ONLY=0
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=1

log() {
    printf '%s %s\n' "$(date -Is)" "$*" >>"$LOG" 2>/dev/null || true
    printf '%s\n' "$*"
}

SRC_HOST="$SOURCE"
SRC_PATH="$DEST"
if [[ "$SOURCE" == *:* ]]; then
    SRC_HOST="${SOURCE%%:*}"
    SRC_PATH="${SOURCE#*:}"
fi

TMP="$(mktemp "${DEST}.sync.XXXXXX")"
trap 'rm -f "$TMP" "$TMP.new"' EXIT

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$SRC_HOST" "cat '$SRC_PATH'" >"$TMP" 2>/dev/null; then
    log "config sync: cannot reach $SRC_HOST — keeping the local config"
    exit 0
fi

if [[ ! -s "$TMP" ]]; then
    log "config sync: $SRC_HOST returned an empty file — keeping the local config"
    exit 0
fi

# A config that does not parse is never installed: the gateway would keep
# the old one anyway, but the operator would see a broken file on disk.
if ! python3 -c 'import sys, tomllib; tomllib.load(open(sys.argv[1], "rb"))' "$TMP" 2>/dev/null; then
    log "config sync: the file from $SRC_HOST does not parse as TOML — keeping the local config"
    exit 0
fi

LOCAL_SUM="$(sha256sum "$DEST" 2>/dev/null | cut -d' ' -f1 || echo none)"
REMOTE_SUM="$(sha256sum "$TMP" | cut -d' ' -f1)"

if [[ "$LOCAL_SUM" == "$REMOTE_SUM" ]]; then
    log "config sync: already in step with $SRC_HOST"
    exit 0
fi

if [[ "$CHECK_ONLY" == "1" ]]; then
    log "config sync: would install the config from $SRC_HOST (local $LOCAL_SUM, source $REMOTE_SUM)"
    exit 0
fi

BACKUP="${DEST}.bak-$(date +%Y%m%d-%H%M%S)-sync"
cp -p "$DEST" "$BACKUP" 2>/dev/null || true
install -m 600 "$TMP" "${DEST}.new"
chmod --reference="$DEST" "${DEST}.new" 2>/dev/null || true
mv -f "${DEST}.new" "$DEST"
log "config sync: installed the config from $SRC_HOST (local $LOCAL_SUM -> $REMOTE_SUM), backup $BACKUP"
