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
BILLING="${BILLING:-sub}"

usage() {
    sed -n '2,24p' "$0"
    echo
    echo "usage: $0 add <name> <base_url> [opts] | rm <name> | verify <name>"
    echo "  opts: --kind openai_compat|opencode (default openai_compat)"
    echo "        --upstream-path PATH (default /v1/chat/completions)"
    echo "        --model MODEL (required; the pool slot's model)"
    echo "        --api-key-env VAR | --key-from-vault NAME (exactly one required)"
    echo "        --pool auto|auto_home (default auto)   --billing sub|metered (default sub)"
    echo "        --dry-run"
    exit 2
}

[ $# -ge 2 ] || usage
ACTION="$1"; NAME="$2"; shift 2 || true
[[ "$NAME" =~ ^[a-zA-Z0-9_-]+$ ]] || { echo "ERROR: name must be [a-zA-Z0-9_-]+" >&2; exit 1; }
BASE_URL=""
if [ "$ACTION" = "add" ] && [ $# -ge 1 ]; then BASE_URL="$1"; shift; fi

KIND="openai_compat"; UPSTREAM="/v1/chat/completions"; MODEL=""; KEY=""; DRY=0
while [ $# -gt 0 ]; do
    case "$1" in
        --kind) KIND="$2"; shift 2;;
        --upstream-path) UPSTREAM="$2"; shift 2;;
        --model) MODEL="$2"; shift 2;;
        --api-key-env) KEY="${!2:-}"; [ -n "$KEY" ] || { echo "ERROR: env $2 is empty/unset" >&2; exit 1; }; shift 2;;
        --key-from-vault) KEY="$(cd "$HOME/.agents" && "$HOME/.local/bin/dotvault" get "$2" 2>/dev/null)" || { echo "ERROR: dotvault '$2' not found" >&2; exit 1; }; shift 2;;
        --pool) POOL="$2"; shift 2;;
        --billing) BILLING="$2"; shift 2;;
        --dry-run) DRY=1; shift;;
        *) echo "ERROR: unknown option $1" >&2; usage;;
    esac
done
# ---- build the provider + pool-entry TOML ----
if [ "$KIND" = "opencode" ]; then
    NATIVE="native_responses = true
responses_path = \"/responses\""
    CONN="connect_timeout_secs = 30
total_connect_timeout_secs = 60"
else
    NATIVE="native_responses = true
responses_path = \"/v1/responses\""
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
POOL_ENTRY="[[auto_model.pool]]
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
        case "$BILLING" in sub|metered) ;; *) echo "ERROR: bad --billing" >&2; exit 1;; esac
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
        sleep 3
        exec "$0" verify "$NAME"
        ;;
    rm)
        if [ "$DRY" = 1 ]; then echo "dry-run: would remove provider $NAME"; exit 0; fi
        ssh -o BatchMode=yes "$NAS" "python3 -" <<PY
import os, shutil, time, re
p = "$TOML"
s = open(p, encoding="utf-8").read()
i = s.find("# added by add_pool_provider.sh")
if i < 0:
    print("NOT_FOUND (provider block with add_pool_provider marker)")
else:
    j = s.find("\n[providers.", i+1)
    if j < 0: j = len(s)
    pat = re.compile(r"(?ms)^\[\[auto_model\.pool\]\]\nprovider = \"$NAME\"\n.*?(?=^\[\[auto_model\.pool\]\]|\Z|^\[auto)")
    s2, n = pat.subn("", s)
    bak = p + ".bak-" + time.strftime("%Y%m%d-%H%M%S") + "-rm-" + "$NAME"
    shutil.copy2(p, bak)
    open(p + ".new", "w", encoding="utf-8").write(s2)
    os.chmod(p + ".new", os.stat(p).st_mode & 0o7777)
    os.replace(p + ".new", p)
    print("REMOVED provider + pool-entry x" + str(n), bak)
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
