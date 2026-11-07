#!/usr/bin/env python3
"""Probe every ``[auto_model]`` pool member and print a pass/fail table.

The auto pool is only as good as its members: a dead endpoint costs one
failed attempt per request that happens to land on it, and a member that
answers 403/404/429 costs the same plus a pointless round trip. This script
sends one minimal request per (provider, model) pair to the provider's own
``/v1/responses/<provider>`` route, so the answer is attributed to that
member alone (no rotation, no failover), and prints status, latency and the
upstream's own error message.

Usage::

    MAXLLM_MASTER_KEY=... python3 scripts/pool_health.py --config maxllm.toml
    python3 scripts/pool_health.py --config maxllm.toml --env-file maxllm.env
    python3 scripts/pool_health.py --config maxllm.toml --only minicpm

Exit status is 1 when any member fails, so it can gate a deploy.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
import tomllib
import urllib.error
import urllib.request

PROBE_PROMPT = "reply with the single word ok"


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


def load_pool(path: str) -> list[dict]:
    with open(path, "rb") as handle:
        config = tomllib.load(handle)
    pool = config.get("auto_model", {}).get("pool", [])
    if not pool:
        sys.exit(f"{path}: no [auto_model].pool entries")
    return pool


def probe(base_url: str, key: str, entry: dict, timeout: float) -> dict:
    provider, model = entry["provider"], entry["model"]
    body = json.dumps({
        "model": model,
        "input": PROBE_PROMPT,
        "max_output_tokens": 16,
    }).encode()
    request = urllib.request.Request(
        f"{base_url}/v1/responses/{provider}",
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
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
    return {"provider": provider, "model": model, "status": status,
            "ms": elapsed_ms, "detail": detail}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="maxllm.toml", help="gateway config (default: maxllm.toml)")
    parser.add_argument("--url", default="http://127.0.0.1:3795", help="gateway base URL")
    parser.add_argument("--key", help="master key (default: $MAXLLM_MASTER_KEY)")
    parser.add_argument("--env-file", help="file holding MAXLLM_MASTER_KEY=...")
    parser.add_argument("--only", help="substring filter on provider/model")
    parser.add_argument("--timeout", type=float, default=45.0, help="per-request timeout (s)")
    parser.add_argument("--workers", type=int, default=8, help="parallel probes")
    args = parser.parse_args()

    key = load_key(args)
    pool = load_pool(args.config)
    if args.only:
        pool = [e for e in pool if args.only in e["provider"] + e["model"]]
        if not pool:
            sys.exit(f"no pool entry matches {args.only!r}")

    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool_executor:
        rows = list(pool_executor.map(lambda e: probe(args.url, key, e, args.timeout), pool))

    healthy = [r for r in rows if r["status"] == 200]
    print(f"pool={len(rows)} healthy={len(healthy)} failed={len(rows) - len(healthy)} "
          f"elapsed={time.time() - started:.1f}s")
    for row in rows:
        flag = "ok " if row["status"] == 200 else "BAD"
        print(f"{flag} {row['status']:>4} {row['ms']:>6}ms {row['provider']:<38} "
              f"{row['model']:<28} {row['detail']}")
    return 0 if len(healthy) == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
