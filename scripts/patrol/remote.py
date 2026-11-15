#!/usr/bin/env python3
"""The remote half of the maxllm patrol — runs on the gateway host.

Reads the seen-shape list on stdin, prints the patrol report on stdout:
the attributed client-facing failures, the never-seen-before WARN/ERROR
shapes (marked N), the kernel counters, the breaker state, the binary hash.
"""
import re, subprocess, os, json, time, sys, urllib.request, hashlib, base64

# The seen-shape list arrives as argv[1], base64-encoded: the remote shell
# parses the command line, so the list must not travel as raw text.
# stdin carries this script itself.
seen = set()
if len(sys.argv) > 1 and sys.argv[1]:
    try:
        seen = set(l for l in base64.b64decode(sys.argv[1]).decode().splitlines() if l)
    except Exception:
        seen = set()
ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS   = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d+Z")
now  = time.time()
cut  = now - 1800

def parse_ts(s):
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%S")) - time.timezone
    except Exception:
        return None

raw = subprocess.run(["tail", "-20000", os.path.expanduser("~/opt/maxllm/logs/maxllm.log")],
                     capture_output=True, text=True, errors="replace").stdout
lines = [ANSI.sub("", l) for l in raw.splitlines()]
window = [l for l in lines if (parse_ts(m.group(1)) if (m := TS.match(l)) else now) >= cut]

# ── attribution of every client-facing failure of the window ──────────────
STATUS = re.compile(r"http\.response\.status_code=(\d+)")
findings, context = [], []
for i, line in enumerate(window):
    if "request completed" not in line:
        continue
    m = STATUS.search(line)
    if not m:
        continue
    status = int(m.group(1))
    if status < 400:
        continue
    pm = re.search(r'maxllm\.provider="([^"]*)"', line)
    prov = pm.group(1) if pm else "?"
    cause = "UNATTRIBUTED"
    for w in reversed(window[max(0, i - 80):i]):
        if "Failed to translate request body" in w:
            cause = "CLIENT_BODY_FAULT"; break
        if ("refused the tool schemas" in w or "refused an orphaned tool call" in w
                or "does not carry the requested model" in w
                or "survives the degraded replay" in w or "survives the repaired replay" in w):
            cause = "UPSTREAM_FAULT"; break
        if "Downstream ConnectionClosed" in w:
            cause = "CLIENT_ABORTED"; break
        if "retrying the request on the fallback provider" in w:
            cause = "ABSORBED_BY_CHAIN"; break
    if cause in ("CLIENT_BODY_FAULT", "ABSORBED_BY_CHAIN"):
        context.append(cause)
    else:
        findings.append((status, prov, cause, line[:110]))

print("FINDINGS %d" % len(findings))
for f in findings[-20:]:
    print("  F %s %s %s | %s" % f)
print("CONTEXT %d %s" % (len(context), json.dumps({k: context.count(k) for k in set(context)})))

# ── the shapes of every WARN/ERROR of the window ──────────────────────────
def shape(line):
    s = TS.sub("", line)
    s = re.sub(r"\d{4}-\d{2}-\d{2}T[\d:.]+Z?", "", s)
    s = re.sub(r"\b\d{1,3}(\.\d{1,3}){3}\b", "<ip>", s)
    s = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{27,}\b", "<uuid>", s)
    s = re.sub(r"\b[0-9a-f]{8,}\b", "<id>", s)
    s = re.sub(r"\b\d+\b", "<n>", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200]

new_shapes, all_shapes = [], []
for l in window:
    if " WARN " in l or " ERROR " in l:
        sh = shape(l)
        if sh and sh not in all_shapes:
            all_shapes.append(sh)
            if sh not in seen and sh not in new_shapes:
                new_shapes.append(sh)

print("NEW_SHAPES %d" % len(new_shapes))
for sh in new_shapes:
    print("  N %s" % sh)

# ── kernel + breakers + binary, through urllib (no shell helpers) ─────────
def fetch(url, token=None):
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read().decode(errors="replace")

metrics = fetch("http://127.0.0.1:3795/metrics")
km = [l for l in metrics.splitlines() if l.startswith("maxllm_kernel_") and "_total " in l]
print("KERNEL %s" % " ".join(km[:4]))

token = ""
env = open(os.path.expanduser("~/opt/maxllm/maxllm.env"), errors="replace").read()
for line in env.splitlines():
    if line.startswith("MAXLLM_MASTER_KEY="):
        token = line.split("=", 1)[1].strip(); break
health = json.loads(fetch("http://127.0.0.1:3795/health", token))
print("GATEWAY build=%s open=%s" % (health.get("build_id"), health.get("open_providers")))

h = hashlib.sha256()
with open(os.path.expanduser("~/opt/maxllm/bin/maxllm-server"), "rb") as fh:
    for chunk in iter(lambda: fh.read(1048576), b""):
        h.update(chunk)
print("BINARY %s" % h.hexdigest()[:16])
