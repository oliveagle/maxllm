#!/usr/bin/env bash
# deploy_maxllm.sh — build maxllm-server for musl and roll it out to one HA node.
#
# Why this script exists: on 2026-11-09 09:41 CST a `cargo build --release` artifact
# (gnu target, built on Fedora 44 / glibc 2.4x) was copied to oleNas01 (Debian 12 /
# glibc 2.36). It died instantly with "version `GLIBC_2.38' not found", systemd
# crash-looped, and the gateway was down ~90 minutes. The guard below refuses to ship
# any binary that is not a static-pie musl build.
#
# Usage: scripts/deploy_maxllm.sh <nas|fedora|vps> [--check]
#   --check  build + guard only, do not touch the node
set -euo pipefail

NODE="${1:-}"
CHECK_ONLY=0
[[ "${2:-}" == "--check" ]] && CHECK_ONLY=1

case "$NODE" in
  nas)    HOST=oliveagle@oleNas01;      DIR=/home/oliveagle/opt/maxllm; RESTART='systemctl --user restart maxllm'; HEALTH=http://127.0.0.1:3795/health ;;
  fedora) HOST=oliveagle@oleFedora01;   DIR=/home/oliveagle/opt/maxllm; RESTART='systemctl --user restart maxllm'; HEALTH=http://127.0.0.1:3795/health ;;
  vps)    HOST=root@vm_tecent_cloud_01; DIR=/opt/maxllm;                RESTART='systemctl restart maxllm';       HEALTH=http://127.0.0.1:3795/health ;;
  *) echo "usage: $0 <nas|fedora|vps> [--check]" >&2; exit 2 ;;
esac

REPO="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/mx-gate-target}"
BIN="$TARGET_DIR/x86_64-unknown-linux-musl/release/maxllm-server"

echo "==> building musl static maxllm-server ($(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo '?'))"
CARGO_TARGET_DIR="$TARGET_DIR" CC_x86_64_unknown_linux_musl=musl-gcc \
  cargo build --release --target x86_64-unknown-linux-musl \
    -p maxllm-gateway --bin maxllm-server --manifest-path "$REPO/Cargo.toml"

check_static() {   # $1 = ELF path; refuses anything but static-pie musl
  local info; info="$(file -b "$1")"
  case "$info" in
    *static-pie*) return 0 ;;
    *) echo "REFUSING to deploy: $(basename "$1") is not a static-pie musl build." >&2
       echo "  $info" >&2
       echo "  Rebuild with --target x86_64-unknown-linux-musl (see header of this script)." >&2
       return 1 ;;
  esac
}

check_static "$BIN" || exit 1
SHA="$(sha256sum "$BIN" | cut -c1-16)"
echo "==> guarded ok: $(file -b "$BIN")"
echo "==> sha256[:16]=$SHA size=$(stat -c%s "$BIN")"

if [[ $CHECK_ONLY == 1 ]]; then echo "==> --check only, nothing deployed"; exit 0; fi

TS="$(date +%Y%m%d-%H%M)"
echo "==> backup remote -> $DIR/bin/maxllm-server.bak-$TS-deploy"
ssh -o BatchMode=yes "$HOST" "cd $DIR/bin && cp -p maxllm-server maxllm-server.bak-$TS-deploy"

echo "==> uploading"
scp -q -o BatchMode=yes "$BIN" "$HOST:$DIR/bin/maxllm-server.new"

echo "==> switching over + restarting"
ssh -o BatchMode=yes "$HOST" "cd $DIR/bin && chmod 755 maxllm-server.new && mv maxllm-server.new maxllm-server && $RESTART"

echo "==> health check"
ok=0
for _ in $(seq 1 20); do
  code="$(ssh -o BatchMode=yes "$HOST" "curl -s -m 3 -o /dev/null -w '%{http_code}' $HEALTH")"
  if [[ "$code" == 200 ]]; then ok=1; break; fi
  sleep 1
done

if [[ $ok == 1 ]]; then
  echo "==> OK: $NODE runs $SHA"
  ssh -o BatchMode=yes "$HOST" "sha256sum $DIR/bin/maxllm-server | cut -c1-16"
else
  echo "==> HEALTH FAILED — rolling back" >&2
  ssh -o BatchMode=yes "$HOST" "cd $DIR/bin && cp -p maxllm-server.bak-$TS-deploy maxllm-server && $RESTART"
  ssh -o BatchMode=yes "$HOST" "tail -5 $DIR/logs/maxllm.err" >&2 || true
  exit 1
fi
