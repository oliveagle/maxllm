#!/usr/bin/env python3
"""Enforce the native-Responses-first policy on a maxllm provider config.

Policy (see CLAUDE.md "Responses Contract Policy" and RESPONSES_CONTRACT.md):
a provider whose upstream serves the Responses contract MUST be configured
`native_responses = true`, so `/v1/responses` traffic is forwarded byte-for-byte.
Down-translating to chat completions is a fallback reserved for upstreams that
have no Responses endpoint, and every such fallback must be a recorded
exemption rather than a silent default.

    python3 scripts/check_responses_policy.py                     # static check
    python3 scripts/check_responses_policy.py --config X.toml
    python3 scripts/check_responses_policy.py --live              # probe upstreams
    python3 scripts/check_responses_policy.py --live --only kimi  # one provider

Exit status: 0 = policy satisfied, 1 = at least one violation, 2 = bad usage.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tomllib
import urllib.error
import urllib.request

# Kinds whose upstream speaks a wire protocol other than OpenAI's, so there is
# no Responses route to forward a request to. Responses traffic for them is
# translated by design (anthropic/gemini/bedrock/cohere translators).
NON_RESPONSES_KINDS = {"anthropic", "gemini", "bedrock", "cohere"}

# Providers that are not chat models at all — the Responses contract does not
# apply to them.
NON_CHAT_MARKERS = ("embedding", "rerank", "moderation")

# Recorded exemptions: provider name -> why it may stay on chat translation.
# Keep this list short; an entry here is a claim that the upstream has no
# Responses endpoint (probe evidence belongs in the config comment).
EXEMPT = {
    "mock": "perf/CI test double: perf/mock_upstream.py answers any path with a "
            "canned chat completion, it is not a product upstream",
}

LIVE_EXISTS_STATUSES = {200, 201, 202, 400, 401, 403, 405, 409, 422, 429, 500, 502, 503}


ENV_REF = re.compile(r"\$\{([A-Z0-9_]+)\}")


def expand_env(value: str | None) -> str | None:
    """Resolve `${VAR}` the way maxllm does, so --live can authenticate."""
    if not value:
        return value
    return ENV_REF.sub(lambda m: os.environ.get(m.group(1), m.group(0)), value)


def load_providers(path: str) -> dict:
    with open(path, "rb") as fh:
        conf = tomllib.load(fh)
    return conf.get("providers", {})


def is_non_chat(name: str, conf: dict) -> bool:
    haystack = f"{name} {conf.get('upstream_path', '')} {conf.get('default_model', '')}"
    return any(marker in haystack.lower() for marker in NON_CHAT_MARKERS)


# The gateway falls back to `/v1/responses` when neither `responses_path` nor a
# derivable `upstream_path` is configured (crates/maxllm-gateway/src/gateway.rs).
DEFAULT_RESPONSES_PATH = "/v1/responses"

# Kinds whose native Responses route is not the OpenAI `/v1/responses` shape and
# therefore must name `responses_path` explicitly.
PATH_MUST_BE_EXPLICIT = {"azure_openai": "/openai/responses?api-version=<api_version>"}


def derive_responses_path(conf: dict) -> str:
    explicit = conf.get("responses_path")
    if explicit:
        return explicit
    upstream = conf.get("upstream_path")
    if upstream and upstream.endswith("/chat/completions"):
        return upstream[: -len("/chat/completions")] + "/responses"
    return DEFAULT_RESPONSES_PATH


def classify(conf: dict, kind: str) -> tuple[str, str]:
    """Return (verdict, detail) for one provider."""
    native = conf.get("native_responses")
    if native is None:
        native = kind == "opencode"  # documented default in maxllm-gateway
    if native:
        path = derive_responses_path(conf)
        if kind in PATH_MUST_BE_EXPLICIT and not conf.get("responses_path"):
            return "WARN", (f"native_responses = true on {kind}, whose native route is "
                            f"{PATH_MUST_BE_EXPLICIT[kind]} — set responses_path explicitly "
                            f"(the fallback {path} would 404)")
        return "NATIVE", path
    return "TRANSLATED", "no Responses endpoint configured: /v1/responses is " \
                         "rewritten into /chat/completions"


def probe(url: str, model: str | None, api_key: str | None, timeout: float) -> str:
    body = json.dumps({"model": model or "probe", "input": "ping"}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return f"{resp.status} route exists"
    except urllib.error.HTTPError as err:
        detail = ""
        try:
            detail = err.read(160).decode("utf-8", "replace").replace("\n", " ")
        except Exception:  # noqa: BLE001 - diagnostics only
            pass
        if err.code == 404:
            return f"404 route absent {detail}".strip()
        if err.code in LIVE_EXISTS_STATUSES:
            return f"{err.code} route exists {detail}".strip()
        return f"{err.code} unexpected {detail}".strip()
    except Exception as err:  # noqa: BLE001 - connection failures are reported
        return f"unreachable ({type(err).__name__}: {err})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default="maxllm.toml", help="maxllm TOML config")
    ap.add_argument("--live", action="store_true",
                    help="POST a minimal Responses body to each native endpoint")
    ap.add_argument("--only", help="substring filter on the provider name")
    ap.add_argument("--timeout", type=float, default=20.0, help="live probe timeout (s)")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    try:
        providers = load_providers(args.config)
    except FileNotFoundError:
        print(f"error: config not found: {args.config}", file=sys.stderr)
        return 2
    except tomllib.TOMLDecodeError as err:
        print(f"error: {args.config} is not valid TOML: {err}", file=sys.stderr)
        return 2

    rows, violations = [], 0
    for name in sorted(providers):
        conf = providers[name]
        if args.only and args.only not in name:
            continue
        kind = conf.get("kind", "openai_compat")
        if kind in NON_RESPONSES_KINDS:
            rows.append({"provider": name, "kind": kind, "verdict": "BY-KIND",
                         "detail": f"{kind} upstream has no Responses route; translation is the contract"})
            continue
        if is_non_chat(name, conf):
            rows.append({"provider": name, "kind": kind, "verdict": "NON-CHAT",
                         "detail": "embedding/rerank endpoint; the Responses contract does not apply"})
            continue
        verdict, detail = classify(conf, kind)
        if verdict == "TRANSLATED":
            if name in EXEMPT:
                verdict, detail = "EXEMPT", EXEMPT[name]
            else:
                violations += 1
        if verdict == "WARN":
            violations += 1
        row = {"provider": name, "kind": kind, "verdict": verdict, "detail": detail}
        if verdict in {"NATIVE", "WARN"} and args.live:
            base = conf.get("base_url", "").rstrip("/")
            row["probe"] = probe(f"{base}{detail}", conf.get("default_model"),
                                 expand_env(conf.get("api_key")), args.timeout)
            if row["probe"].startswith("404"):
                row["verdict"] = "FAIL-LIVE"
                violations += 1
        rows.append(row)

    if args.json:
        print(json.dumps({"config": args.config, "violations": violations, "providers": rows},
                         indent=2))
        return 1 if violations else 0

    width = max((len(r["provider"]) for r in rows), default=8)
    for row in rows:
        line = f"{row['verdict']:<10} {row['provider']:<{width}}  {row['kind']:<14} {row['detail']}"
        if "probe" in row:
            line += f"\n{'':<11}{'':<{width}}  {'probe:':<14} {row['probe']}"
        print(line)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    summary = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(f"\n{args.config}: {len(rows)} providers — {summary}")
    if violations:
        print(f"FAIL: {violations} provider(s) violate the native-Responses-first policy "
              f"(see CLAUDE.md § Responses Contract Policy)")
        return 1
    print("OK: every Responses-capable upstream is native; translation is used only "
          "for recorded exemptions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
