#!/usr/bin/env bash
# check.sh — the kernel edit loop.
#
# Editing a kernel script is not the problem; finding out whether the edit
# worked is. The Rhai engine is the only compiler, and the obvious way to
# run it is a live gateway (~20 minutes of build and deploy). This gate
# replaces that with a sub-second check:
#
#   scripts/kernel/check.sh                      # compile every shipped kernel
#   scripts/kernel/check.sh path/to/main.rhai    # compile one
#   scripts/kernel/check.sh --run path/to/main.rhai \
#       --error-body 'Invalid request: ...'      # dry-run the classifier
#
# The gateway hot-reloads the script on save, so this gate is the whole
# inner loop: edit → check → save.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
CLI="$REPO/target/debug/maxllm"

# The gate must be fast: build the CLI only when it is missing. A stale
# binary is fine — it compiles scripts with the same engine.
if [[ ! -x "$CLI" ]]; then
  echo "==> building the kernel gate (first run)"
  cargo build -p maxllm-cli --manifest-path "$REPO/Cargo.toml" >&2
fi

RUN=0
ERROR_BODY=""
SCRIPTS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run) RUN=1; shift ;;
    --error-body) ERROR_BODY="$2"; shift 2 ;;
    *) SCRIPTS+=("$1"); shift ;;
  esac
done

if [[ ${#SCRIPTS[@]} -eq 0 ]]; then
  while IFS= read -r f; do SCRIPTS+=("$f"); done \
    < <(find "$REPO/examples/kernel" -name 'main.rhai' -type f | sort)
fi

if [[ ${#SCRIPTS[@]} -eq 0 ]]; then
  echo "no kernel scripts found" >&2
  exit 1
fi

failed=0
for script in "${SCRIPTS[@]}"; do
  echo "==> $(basename "$(dirname "$script")")/$(basename "$script")"
  if ! "$CLI" kernel check --script "$script"; then
    failed=1
    continue
  fi
  if [[ $RUN -eq 1 ]]; then
    if [[ -z "$ERROR_BODY" ]]; then
      echo "  --run needs --error-body" >&2
      failed=1
      continue
    fi
    printf '  classify -> '
    "$CLI" kernel run --script "$script" --status 400 --error-body "$ERROR_BODY" || failed=1
  fi
done

if [[ $failed -ne 0 ]]; then
  echo "==> kernel gate FAILED" >&2
  exit 1
fi
echo "==> kernel gate OK"
