#!/usr/bin/env python3
"""Probe every auto-pool member (``[auto_model]`` + ``[auto_models.*]``) and
print a pass/fail table.

The auto pool is only as good as its members: a dead endpoint costs one
failed attempt per request that happens to land on it, and a member that
answers 403/404/429 costs the same plus a pointless round trip. This script
sends one minimal request per (provider, model) pair to the provider's own
``/v1/responses/<provider>`` route, so the answer is attributed to that
member alone (no rotation, no failover), and prints status, latency and the
upstream's own error message.

**The probe is not free.** Every probe is a real request against a real
subscription: 25 members every five minutes is ~250 requests an hour, ~6 000
a day, all of them eaten out of the very weekly/monthly allowance the pool
exists to protect. On 2026-11-09 that was 21% of every upstream request the
NAS made, and 270 of them went to members that were *known* to be walled
(``kimi`` answering 429, ``open_design`` answering 402).

So the probe only asks the questions it still has to ask:

* a member whose breaker is **open with a known deadline** is parked — the
  gateway already knows when it comes back, so the probe can only confirm
  what is known. It is reported as ``WALL`` and not probed;
* a member that **answered a quota error** (402 payment, 429 rate limit) is
  walled. The probe traffic is excluded from the gateway's breaker
  accounting, so the gateway never learns the wall from a probe — the probe
  remembers it itself and backs off (the back-off doubles with every failed
  round, so a walled member costs one request per hour instead of one per
  round). It is reported as ``QUOTA`` and not probed;
* a member that **served real traffic recently** is alive — the gateway's
  own ledger (``request_logs``) says so, so the probe would learn nothing.
  It is reported as ``SERVED`` and not probed. Such a member is still
  re-probed every ``--recheck-rounds`` rounds, so a member that breaks
  *after* a good turn is caught eventually.

Everything else is probed as before. The report says how many upstream
requests the round saved.

Usage::

    MAXLLM_MASTER_KEY=... python3 scripts/pool_health.py --config maxllm.toml
    python3 scripts/pool_health.py --config maxllm.toml --env-file maxllm.env
    python3 scripts/pool_health.py --config maxllm.toml --only minicpm
    python3 scripts/pool_health.py --config maxllm.toml --pool auto_home   # one named pool
    python3 scripts/pool_health.py --config maxllm.toml --pool auto,auto_home

Exit status is 1 when any member fails, so it can gate a deploy.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request

PROBE_PROMPT = "reply with the single word ok"

# What a quota wall looks like in the gateway's own error text: it names the
# status the provider answered ("provider 'kimi' answered 429 and no other
# candidate ... can serve the request").
QUOTA_HINTS = ("answered 429", "answered 402", "answered 403",
               "quota force-open", "rate limit", "payment required")


def load_key(args: argparse.Namespace) -> str:
    if args.key:
        return args.key
    if os.environ.get("MAXLLM_MASTER_KEY"):
        return os.environ["MAXLLM_MASTER_KEY"]
    if args.env_file:
        with open(args.env_file, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MAXLLM_MASTER_KEY="):
                    return line.split("=", 1)[1].strip()
    sys.exit("no master key: set MAXLLM_MASTER_KEY, pass --key or --env-file")


def load_pools(path: str) -> dict[str, list[dict]]:
    """Every enabled pool, keyed by sentinel name.

    ``[auto_model]`` is the default pool (sentinel ``auto`` unless ``name``
    says otherwise); each ``[auto_models.<key>]`` is another pool whose
    sentinel defaults to the table key. Disabled pools are skipped, mirroring
    what the gateway itself loads.
    """
    with open(path, "rb") as handle:
        config = tomllib.load(handle)
    pools: dict[str, list[dict]] = {}
    default = config.get("auto_model") or {}
    if default.get("pool") and default.get("enabled", True):
        pools[default.get("name", "auto")] = [dict(e) for e in default["pool"]]
    for key, block in (config.get("auto_models") or {}).items():
        if block.get("pool") and block.get("enabled", True):
            pools[block.get("name", key)] = [dict(e) for e in block["pool"]]
    if not pools:
        sys.exit(f"{path}: no enabled auto pool entries")
    return pools


def select_entries(pools: dict[str, list[dict]], wanted: str | None) -> list[dict]:
    """Flatten the requested pools, tagging each entry with its pool name."""
    if wanted in (None, "", "all"):
        names = list(pools)
    else:
        names = [n.strip() for n in wanted.split(",") if n.strip()]
    unknown = [n for n in names if n not in pools]
    if unknown:
        sys.exit(f"unknown pool(s) {unknown!r}; available: {list(pools)}")
    return [{**entry, "pool": name} for name in names for entry in pools[name]]


def load_walls(url: str, key: str, timeout: float) -> dict[str, dict]:
    """The members the gateway has already parked, with their deadline.

    ``GET /admin/breakers`` answers the breaker report. A member that is
    open with a deadline in the future is parked until then — probing it
    can only confirm a known state, and costs one request of the member's
    own allowance. An old gateway without the endpoint answers 404; then
    nothing is parked and every member is probed as before.
    """
    request = urllib.request.Request(
        f"{url}/admin/breakers",
        headers={"Authorization": f"Bearer {key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", "replace"))
    except Exception:
        return {}
    now_ms = int(time.time() * 1000)
    walls: dict[str, dict] = {}
    for row in payload.get("breakers", []):
        name = row.get("provider")
        if not name:
            continue
        deadline = int(row.get("force_open_until") or 0)
        if int(row.get("state") or 0) == 1 and deadline > now_ms:
            walls[name] = {"until": deadline, "until_iso": row.get("force_open_until_iso")}
    return walls


def db_path_from_config(config_path: str) -> str:
    """The gateway's ledger path (``admin.db_path`` in the config).

    A relative path is resolved against the config's own directory, the way
    the gateway itself resolves it.
    """
    try:
        with open(config_path, "rb") as handle:
            config = tomllib.load(handle)
    except Exception:
        return ""
    path = ((config.get("admin") or {}).get("db_path") or "").strip()
    if not path:
        return ""
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(config_path)), path)
    return path


def _query_served(db_path: str, window_minutes: int) -> dict[str, int]:
    """Read the served-per-provider counts out of the gateway's ledger."""
    sql = (
        "SELECT provider, COUNT(*) FROM request_logs "
        "WHERE status = 200 AND timestamp >= "
        "strftime('%Y-%m-%dT%H:%M:%S','now','-{} minutes') "
        "GROUP BY provider".format(max(1, int(window_minutes)))
    )
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = connection.execute(sql).fetchall()
    finally:
        connection.close()
    return {str(provider): int(count) for provider, count in rows if provider}


def load_served_from_db(db_path: str, window_minutes: int) -> dict[str, int]:
    """Which members served real traffic recently, per the gateway's ledger.

    The gateway books every real request in ``request_logs`` — the probe
    traffic is excluded from it.  A member that booked a 200 inside the
    window is alive by definition: probing it would spend one more request
    of that member's allowance to learn what the ledger already says.  This
    is the gateway's own record, so it survives a gateway restart (a metric
    counter does not: it restarts at zero and every member would look like
    it had never been used).

    The database is opened read-only.  A WAL database whose shared-memory
    file cannot be opened read-only is copied (db + wal + shm) first, and a
    locked database is retried a few times.
    """
    if not db_path or not os.path.exists(db_path):
        return {}
    for attempt in range(3):
        try:
            return _query_served(db_path, window_minutes)
        except sqlite3.OperationalError as err:
            message = str(err).lower()
            if "locked" in message or "busy" in message:
                time.sleep(1.0 + attempt)
                continue
            if "readonly" in message or "read-only" in message or "unable to open" in message:
                break
            raise
    # The WAL copy path: read a snapshot instead of the live file.
    with tempfile.TemporaryDirectory(prefix="pool-health-db-") as workdir:
        snapshot = os.path.join(workdir, "maxllm.db")
        shutil.copyfile(db_path, snapshot)
        for suffix in ("-wal", "-shm"):
            sidecar = db_path + suffix
            if os.path.exists(sidecar):
                shutil.copyfile(sidecar, snapshot + suffix)
        for attempt in range(3):
            try:
                return _query_served(snapshot, window_minutes)
            except sqlite3.OperationalError as err:
                if "locked" in str(err).lower() or "busy" in str(err).lower():
                    time.sleep(1.0 + attempt)
                    continue
                raise
    return {}


def load_served_counters(url: str, timeout: float) -> dict[str, int]:
    """How many *successful* requests each provider has served so far.

    ``GET /metrics`` carries ``maxllm_requests_total{provider,model,status}``.
    The gateway keeps probe traffic out of it, so a counter that grew since
    the last round means real traffic reached that provider — and a provider
    that served real traffic is alive.
    """
    request = urllib.request.Request(f"{url}/metrics")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8", "replace")
    except Exception:
        return {}
    counters: dict[str, int] = {}
    for line in text.splitlines():
        if not line.startswith("maxllm_requests_total{"):
            continue
        labels = line[line.index("{") + 1:line.index("}")]
        fields = dict(
            part.split("=", 1) for part in labels.split(",") if "=" in part
        )
        provider = fields.get("provider", "").strip('"')
        status = fields.get("status", "").strip('"')
        if not provider or status != "200":
            continue
        try:
            value = int(float(line.rsplit(" ", 1)[1]))
        except (IndexError, ValueError):
            continue
        counters[provider] = counters.get(provider, 0) + value
    return counters


def load_state(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def save_state(path: str, state: dict) -> None:
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(tmp, path)
    except Exception:
        pass


def probe(base_url: str, key: str, entry: dict, timeout: float) -> dict:
    provider, model = entry["provider"], entry["model"]
    pool_name = entry.get("pool", "auto")
    # Billing tier the gateway will rank this entry by: subscription entries
    # are preferred, metered ones are only used once every subscription entry
    # of the pool is unavailable.
    tier = entry.get("billing", "subscription")
    body = json.dumps({
        "model": model,
        "input": PROBE_PROMPT,
        "max_output_tokens": 16,
    }).encode()
    request = urllib.request.Request(
        f"{base_url}/v1/responses/{provider}",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            # The gateway excludes probe traffic from metrics, request logs
            # and breaker accounting.
            "X-MaxLLM-Probe": "1",
        },
    )
    started = time.time()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
            status = response.status
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8", "replace")
        status = err.code
    except Exception as err:  # connection refused, timeout, reset, ...
        raw = repr(err)
        status = -1
    elapsed_ms = int((time.time() - started) * 1000)

    detail = raw[:200].replace("\n", " ")
    # A quota wall reaches the probe in two shapes: the upstream's own status
    # (402 payment, 429 rate limit) or — because the gateway owns its errors —
    # a 503 whose message names the status the provider answered.  Both mean
    # the same thing: the member's allowance is gone for now.
    lowered = raw.lower()
    quota = status in (402, 429) or (
        status == 503 and any(hint in lowered for hint in QUOTA_HINTS)
    )
    try:
        payload = json.loads(raw)
        if payload.get("error"):
            detail = json.dumps(payload["error"], ensure_ascii=False)[:200]
        else:
            text = "".join(
                part.get("text", "")
                for item in payload.get("output", [])
                for part in (item.get("content") or [])
            )
            detail = f"text={text.strip()[:60]!r}"
    except ValueError:
        pass
    return {"pool": pool_name, "tier": tier, "provider": provider,
            "model": model, "status": status, "ms": elapsed_ms,
            "quota": quota, "detail": detail}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="maxllm.toml", help="gateway config (default: maxllm.toml)")
    parser.add_argument("--url", default="http://127.0.0.1:3795", help="gateway base URL")
    parser.add_argument("--key", help="master key (default: $MAXLLM_MASTER_KEY)")
    parser.add_argument("--env-file", help="file holding MAXLLM_MASTER_KEY=...")
    parser.add_argument("--pool", default="all",
                        help="pool sentinel(s) to probe: a name, a comma list, or all (default: all)")
    parser.add_argument("--only", help="substring filter on provider/model")
    parser.add_argument("--timeout", type=float, default=45.0, help="per-request timeout (s)")
    parser.add_argument("--workers", type=int, default=8, help="parallel probes")
    parser.add_argument("--state", default="logs/pool-health-state.json",
                        help="round state (served counters, last probe round)")
    parser.add_argument("--probe-walls", action="store_true",
                        help="probe members that are parked too (default: skip them)")
    parser.add_argument("--probe-served", action="store_true",
                        help="probe members that served real traffic (default: skip them)")
    parser.add_argument("--recheck-rounds", type=int, default=6,
                        help="probe a member that served traffic again every N rounds "
                             "(default: 6, i.e. every 30 minutes at 5-minute rounds)")
    parser.add_argument("--db", help="the gateway's ledger (default: admin.db_path "
                                     "from the config)")
    parser.add_argument("--served-window", type=int, default=30,
                        help="how far back the ledger is read for 'served real "
                             "traffic' (minutes, default: 30)")
    parser.add_argument("--quota-backoff-rounds", type=int, default=3,
                        help="skip a member that answered a quota error for this "
                             "many rounds (the streak doubles it, default: 3)")
    parser.add_argument("--quota-backoff-max", type=int, default=12,
                        help="the longest back-off in rounds (default: 12 = 1 h)")
    parser.add_argument("--fail-on-wall", action="store_true",
                        help="count a parked member as a failure (default: it is not)")
    args = parser.parse_args()

    key = load_key(args)
    pool = select_entries(load_pools(args.config), args.pool)
    if args.only:
        pool = [e for e in pool if args.only in e["provider"] + e["model"]]
        if not pool:
            sys.exit(f"no pool entry matches {args.only!r}")

    # What the gateway already knows: a member that is parked until a known
    # instant, and a member that has served real traffic since the last
    # round. Both are answers the probe cannot improve on.
    walls = {} if args.probe_walls else load_walls(args.url, key, args.timeout)
    state = load_state(args.state)
    round_no = int(state.get("round", 0)) + 1
    served_then = state.get("served", {})
    last_probe = state.get("probed", {})

    # "Served real traffic" comes from the gateway's own ledger first: it is
    # the gateway's record, so it survives a restart (a metric counter does
    # not — it starts at zero again and every member would look unused).
    served_now: dict[str, int] = {}
    served_source = "none"
    if not args.probe_served:
        db_path = args.db or db_path_from_config(args.config)
        served_now = load_served_from_db(db_path, args.served_window)
        served_source = "ledger"
        if not served_now:
            # No ledger (or it cannot be read): fall back to the metric
            # counters, which only count a member as served when the counter
            # grew against the stored baseline — a counter that restarted at
            # zero must not make every member look alive.
            counters = load_served_counters(args.url, args.timeout)
            served_now = {
                provider: counter
                for provider, counter in counters.items()
                if provider in served_then and counter > int(served_then[provider])
            }
            served_source = "counters" if counters else "none"

    quota_backoff = state.get("quota_backoff", {})
    to_probe: list[dict] = []
    skipped: list[dict] = []
    for entry in pool:
        provider = entry["provider"]
        wall = walls.get(provider)
        if wall:
            skipped.append({**entry, "state": "WALL",
                            "detail": f"parked until {wall['until_iso']}"})
            continue
        backoff = quota_backoff.get(provider) or {}
        until = int(backoff.get("until") or 0)
        if until > round_no:
            skipped.append({**entry, "state": "QUOTA",
                            "detail": f"quota wall (streak {backoff.get('streak', 0)}, "
                                      f"retry in {until - round_no} rounds)"})
            continue
        counter = served_now.get(provider, 0)
        rounds_since = round_no - int(last_probe.get(provider, 0))
        if counter > 0 and rounds_since < max(1, args.recheck_rounds):
            skipped.append({**entry, "state": "SERVED",
                            "detail": f"served real traffic ({counter} ok requests "
                                      f"in the last {args.served_window} min)"})
            continue
        to_probe.append(entry)

    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool_executor:
        rows = list(pool_executor.map(lambda e: probe(args.url, key, e, args.timeout), to_probe))
    elapsed = time.time() - started

    healthy = [r for r in rows if r["status"] == 200]
    failed = len(rows) - len(healthy)
    parked = [r for r in skipped if r["state"] == "WALL"]
    quota = [r for r in skipped if r["state"] == "QUOTA"]
    print(f"pool={len(pool)} probed={len(rows)} healthy={len(healthy)} failed={failed} "
          f"wall={len(parked)} quota={len(quota)} served={len(skipped) - len(parked) - len(quota)} "
          f"served_source={served_source} window={args.served_window}min "
          f"saved={len(pool) - len(rows)} upstream requests elapsed={elapsed:.1f}s")
    for row in rows:
        flag = "ok " if row["status"] == 200 else "BAD"
        print(f"{flag} {row['status']:>4} {row['ms']:>6}ms {row['pool']:<10} "
              f"{row['tier']:<12} {row['provider']:<38} {row['model']:<28} {row['detail']}")
    for row in skipped:
        print(f"{row['state']:<6} {'':>4} {'':>6}  {row['pool']:<10} "
              f"{row.get('tier', 'subscription'):<12} {row['provider']:<38} "
              f"{row['model']:<28} {row['detail']}")

    # A member that answered a quota error (402 payment, 429 rate limit) is
    # walled for now.  The probe traffic is excluded from the gateway's own
    # breaker accounting, so the gateway never learns the wall from it — the
    # probe has to remember it itself, or it keeps buying a 429 every round.
    # The back-off doubles with every failed round and resets on a 200.
    for row in rows:
        provider = row["provider"]
        if row.get("quota"):
            streak = int((quota_backoff.get(provider) or {}).get("streak", 0)) + 1
            delay = min(streak * max(1, args.quota_backoff_rounds),
                        max(1, args.quota_backoff_max))
            quota_backoff[provider] = {"until": round_no + delay, "streak": streak}
        elif row["status"] == 200:
            quota_backoff.pop(provider, None)

    save_state(args.state, {
        "round": round_no,
        "served": served_now,
        "served_source": served_source,
        "quota_backoff": quota_backoff,
        "probed": {**last_probe, **{r["provider"]: round_no for r in rows}},
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })

    if failed:
        return 1
    if args.fail_on_wall and parked:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
