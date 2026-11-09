#!/usr/bin/env bash
# add_pool_provider.sh — one command to add an account/provider to the pool
# and prove it works, the way the operator actually does it ("往池子里加账号").
#
# Source of truth is oleNas01's maxllm.toml; every other node follows within
# <=2 minutes (maxllm-config-sync.timer on oleFedora01). The gateway hot-reloads
# the change — no restart, no dropped connections, breaker state preserved.
#
# The API key is NEVER passed on the command line (it would land in shell
# history and /proc). Give it by env-var name (--api-key-env) or dotvault name
# (--key-from-vault), e.g.:
#   MAXLLM_MY_KEY='sk-...' scripts/add_pool_provider.sh add myacct \
#       https://api.example.com --kind openai_compat --model deepseek-x \
#       --api-key-env MAXLLM_MY_KEY
#   scripts/add_pool_provider.sh add myacct https://opencode.ai \
#       --kind opencode --model deepseek-v4 --key-from-vault MY_VAULT_NAME
#   scripts/add_pool_provider.sh rm myacct
#   scripts/add_pool_provider.sh verify myacct
#
# --dry-run prints the exact TOML it would write and does not touch anything.
set -euo pipefail

NAS="${NAS:-oleNas01}"
TOML="${TOML:-/home/oliveagle/opt/maxllm/maxllm.toml}"
MASTER_KEY_VAR="${MASTER_KEY_VAR:-MAXLLM_MASTER_KEY}"
POOL="${POOL:-auto}"
BILLING="${BILLING:-subscription}"

usage() {
    sed -n '2,24p' "$0"
    echo
    echo "usage: $0 add <name> <base_url> [opts] | rm <name> | verify <name>"
    echo "  opts: --kind openai_compat|opencode (default openai_compat)"
    echo "        --upstream-path PATH (default /v1/chat/completions)"
    echo "        --responses-path PATH  only when the Responses route is NOT the chat path's sibling"
    echo "        --chat-only  upstream speaks chat only; the gateway translates instead"
    echo "        --model MODEL (required; the pool slot's model)"
    echo "        --api-key-env VAR | --key-from-vault NAME (exactly one required)"
    echo "        --pool auto|auto_home (default auto)   --billing subscription|metered (default subscription)"
    echo "        --dry-run"
    exit 2
}

[ $# -ge 2 ] || usage
ACTION="$1"; NAME="$2"; shift 2 || true
[[ "$NAME" =~ ^[a-zA-Z0-9_-]+$ ]] || { echo "ERROR: name must be [a-zA-Z0-9_-]+" >&2; exit 1; }
BASE_URL=""
if [ "$ACTION" = "add" ] && [ $# -ge 1 ]; then BASE_URL="$1"; shift; fi

KIND="openai_compat"; UPSTREAM="/v1/chat/completions"; MODEL=""; KEY=""; DRY=0
RESP_PATH=""; CHAT_ONLY=0
while [ $# -gt 0 ]; do
    case "$1" in
        --kind) KIND="$2"; shift 2;;
        --upstream-path) UPSTREAM="$2"; shift 2;;
        --model) MODEL="$2"; shift 2;;
        --api-key-env) KEY="${!2:-}"; [ -n "$KEY" ] || { echo "ERROR: env $2 is empty/unset" >&2; exit 1; }; shift 2;;
        --key-from-vault) KEY="$(cd "$HOME/.agents" && "$HOME/.local/bin/dotvault" get "$2" 2>/dev/null)" || { echo "ERROR: dotvault '$2' not found" >&2; exit 1; }; shift 2;;
        --responses-path) RESP_PATH="$2"; shift 2;;
        --chat-only) CHAT_ONLY=1; shift;;
        --pool) POOL="$2"; shift 2;;
        --billing) BILLING="$2"; shift 2;;
        --dry-run) DRY=1; shift;;
        *) echo "ERROR: unknown option $1" >&2; usage;;
    esac
done
# ---- build the provider + pool-entry TOML ----
# The gateway derives the Responses path from the chat path when the config
# does not name one (`gateway.rs derive_responses_path`:
# `…/chat/completions` → `…/responses`), and that derivation is authoritative.
# This script used to write a hardcoded `responses_path = "/v1/responses"`
# instead. On 2026-11-09 that made a new Ark Coding member 404 on every
# request: `volc_1_spare` joined the pool but its real Responses route is
# `/api/coding/v3/responses`, the pool routed it, the upstream answered 404
# and the gateway immediately failed over — the member could never serve.
# `--responses-path` is for upstreams whose Responses route is NOT the chat
# route's sibling (zhipu_1: chat `/api/coding/paas/v4/chat/completions`,
# responses `/api/v1/responses`).
# `--chat-only` omits native_responses entirely: the gateway then translates
# the client's Responses request to chat for this upstream. That is the safe
# choice for an upstream that only speaks chat (forcing native_responses on a
# chat-only upstream would POST a Responses body at it).
derive_responses_path() {
    local p="${1%/}"
    case "$p" in
        ""|"/") echo "";;
        */chat/completions) echo "${p%/chat/completions}/responses";;
        */completions)      echo "${p%/completions}/responses";;
        *)                  echo "/v1/responses";;
    esac
}
if [ "$CHAT_ONLY" = 1 ]; then
    NATIVE=""
else
    DERIVED="$(derive_responses_path "$UPSTREAM")"
    RP="${RESP_PATH:-$DERIVED}"
    [ -n "$RP" ] || { echo "ERROR: cannot derive a responses path from --upstream-path '$UPSTREAM'; pass --responses-path" >&2; exit 1; }
    NATIVE="native_responses = true
responses_path = \"$RP\""
fi
if [ "$KIND" = "opencode" ]; then
    CONN="connect_timeout_secs = 30
total_connect_timeout_secs = 60"
else
    CONN=""
fi
PROVIDER_BLOCK="# added by add_pool_provider.sh $(date -Is)
[providers.$NAME]
kind = \"$KIND\"
base_url = \"$BASE_URL\"
upstream_path = \"$UPSTREAM\"
api_key = \"$KEY\"
default_model = \"$MODEL\"
$CONN
max_fails = 3
fail_timeout_secs = 60
read_timeout_secs = 60
$NATIVE
"
# The entry table is named after the pool it belongs to: the default pool is
# `[[auto_model.pool]]`, a named pool is `[[auto_models.<name>.pool]]`.
# Writing the wrong table name silently lands the entry in the AUTO pool.
if [ "$POOL" = "auto" ]; then
    POOL_TABLE="auto_model.pool"
else
    POOL_TABLE="auto_models.$POOL.pool"
fi
POOL_ENTRY="[[$POOL_TABLE]]
provider = \"$NAME\"
model = \"$MODEL\"
billing = \"$BILLING\"
"

gen_edit() {
    # $1 = local file with provider block, $2 = local file with pool entry
    scp -q -o BatchMode=yes "$1" "$NAS:/tmp/pb.txt"
    scp -q -o BatchMode=yes "$2" "$NAS:/tmp/pe.txt"
    ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import os, shutil, time
p = "$TOML"
s = open(p, encoding="utf-8").read()
if "[providers.$NAME]" in s:
    print("ALREADY_EXISTS"); raise SystemExit(3)
pb = open("/tmp/pb.txt", encoding="utf-8").read()
pe = open("/tmp/pe.txt", encoding="utf-8").read()
routes_anchor = "# --------------------------------------------------------------------------\n# Routes\n"
assert routes_anchor in s, "Routes anchor not found"
s = s.replace(routes_anchor, pb + "\n" + routes_anchor, 1)
if "$POOL" == "auto":
    pool_anchor = "[auto_models.auto_home]"
    assert pool_anchor in s, "auto_home anchor not found"
    s = s.replace(pool_anchor, pe + pool_anchor, 1)
else:
    import re as _re
    m = _re.search(r"(?ms)^\[auto_models\.auto_home\]\n(.*?)(?=^\[\[routes\]\])", s)
    assert m, "auto_home pool block not found"
    s = s[:m.end()] + pe + s[m.end():]
# The edited file MUST parse before it replaces the live config. A config
# that does not parse fails the gateway's TOML parse, and the gateway then
# keeps the OLD config — the new member silently never joins the pool.
# That is exactly what happened on 2026-11-09 21:27: a `billing = "sub"`
# (the enum wants "subscription") made the whole file unparseable.
import tomllib
try:
    tomllib.loads(s)
except Exception as exc:
    print("REFUSING to write: the edited config does not parse:", exc)
    raise SystemExit(5)

bak = p + ".bak-" + time.strftime("%Y%m%d-%H%M%S") + "-add-" + "$NAME"
shutil.copy2(p, bak)
open(p + ".new", "w", encoding="utf-8").write(s)
os.chmod(p + ".new", os.stat(p).st_mode & 0o7777)
os.replace(p + ".new", p)
os.unlink("/tmp/pb.txt"); os.unlink("/tmp/pe.txt")
print("WROTE", bak)
PY
}

case "$ACTION" in
    add)
        [ -n "$MODEL" ] || { echo "ERROR: --model is required" >&2; exit 1; }
        case "$KIND" in openai_compat|opencode) ;; *) echo "ERROR: bad --kind" >&2; exit 1;; esac
        case "$POOL" in auto|auto_home) ;; *) echo "ERROR: bad --pool" >&2; exit 1;; esac
        # The config enum is `subscription` | `metered` (serde snake_case).
        # Anything else fails the whole TOML parse and the gateway then keeps
        # the OLD config — the pool silently loses the new member.
        case "$BILLING" in subscription|metered) ;; *) echo "ERROR: --billing must be 'subscription' or 'metered' (got '$BILLING')" >&2; exit 1;; esac
        [ -n "$BASE_URL" ] || { echo "ERROR: base_url missing" >&2; usage; }
        [ -n "$KEY" ] || { echo "ERROR: need --api-key-env or --key-from-vault" >&2; exit 1; }
        if [ "$DRY" = 1 ]; then
            echo "===== 将要写入 provider 块（密钥已打码）====="
            echo "${PROVIDER_BLOCK//$KEY/<redacted>}"
            echo "===== 将要写入的池条目 ====="
            echo "$POOL_ENTRY"
            exit 0
        fi
        echo "==> 写入 $NAS:$TOML"
        TMPDIR_LOCAL="$(mktemp -d)"
        trap 'rm -rf "$TMPDIR_LOCAL"' EXIT
        printf '%s' "$PROVIDER_BLOCK" > "$TMPDIR_LOCAL/pb.txt"
        printf '%s' "$POOL_ENTRY" > "$TMPDIR_LOCAL/pe.txt"
        gen_edit "$TMPDIR_LOCAL/pb.txt" "$TMPDIR_LOCAL/pe.txt"
        echo "==> 等网关热加载并验证"
        # A write that the gateway cannot parse is silently ignored (it keeps
        # the old config), so the pool would not grow and nothing would say
        # so. Confirm the reload actually landed before declaring success.
        if ! ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import json, time, urllib.request
key = [l.split("=",1)[1].strip().strip(chr(34)) for l in open("/home/oliveagle/opt/maxllm/maxllm.env", errors="replace") if l.startswith("$MASTER_KEY_VAR=")][0]
deadline = time.time() + 30
while True:
    req = urllib.request.Request("http://127.0.0.1:3795/health", headers={"Authorization": "Bearer " + key})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=5))
    except Exception:
        d = {}
    if "$NAME" in d.get("providers", {}):
        print("RELOADED: $NAME is in the running config")
        break
    if time.time() > deadline:
        print("NOT_RELOADED: $NAME is still missing after 30 s — the config was probably rejected")
        raise SystemExit(6)
    time.sleep(2)
PY
        then
            echo "ERROR: the gateway did not pick up $NAME — check the log for 'Failed to parse updated config'" >&2
            echo "  the config file was written; a backup is next to it. Fix or restore it, then re-run." >&2
            exit 6
        fi
        exec "$0" verify "$NAME"
        ;;
    rm)
        if [ "$DRY" = 1 ]; then echo "dry-run: would remove provider $NAME"; exit 0; fi
        ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import os, shutil, time, re
p = "$TOML"
s = open(p, encoding="utf-8").read()

# Target THIS provider's block. The old code searched for the first
# "added by add_pool_provider.sh" marker anywhere in the file, so removing
# one account deleted a different provider's block.
pat = re.compile(
    r"(?ms)^# added by add_pool_provider\.sh[^\n]*\n\[providers\.%s\]\n.*?(?=^# added by add_pool_provider\.sh|^\[providers\.|^\[\[auto_model|\Z)"
    % re.escape("$NAME")
)
s2, n = pat.subn("", s)
if n == 0:
    print("NOT_FOUND (no add_pool_provider block for $NAME)")
    raise SystemExit(4)

# The pool entry lives in the pool's own table: the default pool is
# [[auto_model.pool]], a named pool is [[auto_models.<name>.pool]].
entry = re.compile(
    r"(?ms)^\[\[auto_models?\.(?:%s)?\.pool\]\]\nprovider = \"%s\"\n.*?(?=^\[\[auto_models?\.(?:%s)?\.pool\]\]|\Z|^\[auto)"
    % (re.escape("$POOL"), re.escape("$NAME"), re.escape("$POOL"))
)
s3, m = entry.subn("", s2)

# The edited file must still parse: a bad edit would fail the gateway's
# TOML parse and the gateway would silently keep the old config.
import tomllib
try:
    tomllib.loads(s3)
except Exception as exc:
    print("REFUSING: the edited config does not parse:", exc)
    raise SystemExit(5)

bak = p + ".bak-" + time.strftime("%Y%m%d-%H%M%S") + "-rm-" + "$NAME"
shutil.copy2(p, bak)
open(p + ".new", "w", encoding="utf-8").write(s3)
os.chmod(p + ".new", os.stat(p).st_mode & 0o7777)
os.replace(p + ".new", p)
print("REMOVED provider block x" + str(n) + ", pool-entry x" + str(m), bak)
PY
        sleep 2
        ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
lines = open('/home/oliveagle/opt/maxllm/logs/maxllm.log', errors='replace').read().splitlines()
hits = [l for l in lines if 'reloaded successfully' in l]
for l in hits[-1:]: print(l[:160])
PY
        ;;
    verify)
        echo "-- NAS 熔断器里有没有它"
        ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import json, urllib.request
key = [l.split("=",1)[1].strip().strip(chr(34)) for l in open("/home/oliveagle/opt/maxllm/maxllm.env", errors="replace") if l.startswith("$MASTER_KEY_VAR=")][0]
req = urllib.request.Request("http://127.0.0.1:3795/admin/breakers", headers={"Authorization": "Bearer " + key})
d = json.load(urllib.request.urlopen(req, timeout=8))
b = [x for x in d["breakers"] if x["provider"] == "$NAME"]
print(json.dumps(b, ensure_ascii=False)[:280] if b else "not in breakers yet")
PY
        echo "-- 池子轮换实测（12 发全新会话，统计命中）"
        ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import subprocess, json, os, collections
key = [l.split("=",1)[1].strip().strip(chr(34)) for l in open("/home/oliveagle/opt/maxllm/maxllm.env", errors="replace") if l.startswith("$MASTER_KEY_VAR=")][0]
hits = collections.Counter()
for i in range(12):
    body = json.dumps({"model": "auto", "input": "reply with the single word: pong", "stream": False})
    sid = "verify-$NAME-%d-%d" % (i, os.getpid())
    cmd = ["curl","-s","-m","30","-D","-","-o","/dev/null","http://127.0.0.1:3795/v1/responses/$POOL",
           "-H","Content-Type: application/json","-H","Authorization: Bearer " + key,
           "-H","x-maxllm-session-id: " + sid, "-d", body]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=40).stdout
    prov = None; code = "?"
    for l in out.splitlines():
        ll = l.lower()
        if ll.startswith("x-maxllm-provider"): prov = l.split(":",1)[1].strip()
        if l.startswith("HTTP/"): code = l.split()[1]
    hits[(code, prov)] += 1
print("  ", {k: v for k, v in hits.items()})
PY
        echo "-- Fedora 侧传播（timer 每 2 分钟，等一轮再看）"
        ssh -o BatchMode=yes oleFedora01 "python3 -" <<PY
c = open('/home/oliveagle/opt/maxllm/maxllm.toml', errors='replace').read()
n = c.count('provider = "$NAME"')
print('fedora config 命中条目:', n)
PY
        ;;
        *) usage;;
esac
