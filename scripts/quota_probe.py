#!/usr/bin/env python3
"""Probe one provider **directly at the upstream** until its quota comes back.

Some upstreams state the quota reset instant in the 429 body and maxllm learns
it (``force_open_until``). Others say nothing at all — Kimi answers

    429 {"error":{"message":"You've reached our weekly (7-day) usage limit. Your
         quota will reset when the current 7-day window ends. ..."}}

with no ``Retry-After``, no ``x-ratelimit-*`` headers and no timestamp, so the
recovery instant is simply not knowable in advance. What *is* knowable is the
moment it comes back, and that is worth measuring: the probe sends the
cheapest possible request (``max_tokens: 1``, a two-letter prompt) straight to
the provider's own ``base_url`` + ``upstream_path``, so the gateway's circuit
breaker (which is open — that is the whole point) cannot mask the answer.

Every probe is appended to a JSONL log, so the log answers "when did it
recover" exactly, and "how long did it stay down" after the fact.

Usage::

    python3 scripts/quota_probe.py --config maxllm.toml --provider kimi --once
    python3 scripts/quota_probe.py --config maxllm.toml --provider kimi \\
        --interval 600 --log logs/kimi-quota.jsonl

Exit status: 0 as soon as an upstream answers 200 (recovered), 1 when
``--once`` saw a failure or ``--max-checks`` ran out without a 200.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import tomllib
import urllib.error
import urllib.request

PROBE_PROMPT = "ok"
ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def load_env_file(path: str | None) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path:
        return env
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'\"")
    return env


def expand(value: str, env: dict[str, str]) -> str:
    """Expand ``${VAR}`` from the env file, then from the process environment."""

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in env:
            return env[name]
        if name in os.environ:
            return os.environ[name]
        sys.exit(f"config references ${{{name}}} but it is set nowhere "
                 f"(pass --env-file)")

    return ENV_REF.sub(repl, value)


def load_provider(path: str, name: str, env: dict[str, str]) -> dict:
    with open(path, "rb") as handle:
        config = tomllib.load(handle)
    providers = config.get("providers", {})
    if name not in providers:
        known = ", ".join(sorted(providers)) or "<none>"
        sys.exit(f"{path}: no [providers.{name}]; known: {known}")
    provider = dict(providers[name])
    provider["name"] = name
    provider["base_url"] = expand(str(provider.get("base_url", "")), env).rstrip("/")
    provider["upstream_path"] = expand(
        str(provider.get("upstream_path") or "/v1/chat/completions"), env
    )
    provider["api_key"] = expand(str(provider.get("api_key", "")), env)
    provider["model"] = expand(str(provider.get("default_model", "")), env)
    if not provider["base_url"] or not provider["api_key"] or not provider["model"]:
        sys.exit(f"provider {name}: base_url / api_key / default_model must all be set")
    return provider


def probe(provider: dict, timeout: float) -> dict:
    body = json.dumps({
        "model": provider["model"],
        # Reasoning-only models refuse an empty budget; one token of output is
        # still the cheapest request that cannot be mistaken for a real one.
        "max_tokens": 1,
        "messages": [{"role": "user", "content": PROBE_PROMPT}],
    }).encode()
    request = urllib.request.Request(
        provider["base_url"] + provider["upstream_path"],
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {provider['api_key']}",
            "Content-Type": "application/json",
        },
    )
    started = time.time()
    raw = ""
    status = -1
    error = None
    retry_after = None
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            retry_after = response.headers.get("Retry-After")
            raw = response.read(4000).decode("utf-8", "replace")
    except urllib.error.HTTPError as err:
        status = err.code
        retry_after = err.headers.get("Retry-After") if err.headers else None
        raw = err.read(4000).decode("utf-8", "replace")
        error = err.reason
    except Exception as err:  # connect refused / timeout / reset
        error = repr(err)
    elapsed_ms = int((time.time() - started) * 1000)

    message = ""
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict) and payload.get("error"):
            item = payload["error"]
            message = item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
        elif isinstance(payload, dict):
            message = "ok"
    except ValueError:
        message = (raw or error or "")[:200]
    return {
        "status": status,
        "ok": status == 200,
        "ms": elapsed_ms,
        "message": " ".join(message.split())[:300],
        "retry_after": retry_after,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", default="maxllm.toml", help="gateway config (default: maxllm.toml)")
    parser.add_argument("--env-file", help="file holding the ${VARS} used by the config")
    parser.add_argument("--provider", required=True, help="[providers.<name>] to probe")
    parser.add_argument("--interval", type=float, default=300.0,
                        help="seconds between probes while down (default: 300)")
    parser.add_argument("--max-checks", type=int, default=0,
                        help="stop after N probes (default: 0 = until it recovers)")
    parser.add_argument("--timeout", type=float, default=60.0, help="per-probe timeout (s)")
    parser.add_argument("--once", action="store_true", help="probe once and exit")
    parser.add_argument("--log", help="append one JSON object per probe to this file")
    args = parser.parse_args()

    env = load_env_file(args.env_file)
    provider = load_provider(args.config, args.provider, env)
    print(f"probing {provider['name']} -> {provider['base_url']}{provider['upstream_path']} "
          f"model={provider['model']} interval={args.interval:g}s", flush=True)

    started = time.time()
    checks = 0
    while True:
        checks += 1
        result = probe(provider, args.timeout)
        elapsed = time.time() - started
        stamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        flag = "ok " if result["ok"] else "BAD"
        print(f"{stamp} {flag} {result['status']:>4} {result['ms']:>6}ms "
              f"elapsed={elapsed:7.1f}s {result['message'][:160]}", flush=True)
        if args.log:
            with open(args.log, "a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "ts": dt.datetime.now().astimezone().isoformat(),
                    "provider": provider["name"],
                    "model": provider["model"],
                    "elapsed_s": round(elapsed, 1),
                    **result,
                }, ensure_ascii=False) + "\n")

        if result["ok"]:
            print(f"RECOVERED after {elapsed:.0f}s ({checks} probes)", flush=True)
            return 0
        if args.once:
            return 1
        if args.max_checks and checks >= args.max_checks:
            print(f"still down after {checks} probes / {elapsed:.0f}s", flush=True)
            return 1
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
