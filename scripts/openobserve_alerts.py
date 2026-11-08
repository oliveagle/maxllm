#!/usr/bin/env python3
"""Create or update the MaxLLM gateway alert set in OpenObserve.

The gateway's reliability signals only matter if somebody is paged on them.
Keeping the definitions here (rather than only in the OpenObserve UI) makes the
alert set reviewable, reviewable in a diff, and rebuildable after an instance is
replaced.

Alerts are PromQL over the OTLP metric streams that OTel exports; OpenObserve
stores one metrics stream per metric name, so the expressions reference the
metric names directly.

Credentials come from the environment (never from this file):

    OPENOBSERVE_ENDPOINT   e.g. http://openobserve.internal:5080/api/default
    OPENOBSERVE_AUTH       base64 "user:password" for the Basic header

Usage:
    python3 scripts/openobserve_alerts.py --dry-run
    python3 scripts/openobserve_alerts.py --apply
    python3 scripts/openobserve_alerts.py --apply \\
        --destination-url https://open.feishu.cn/open-apis/bot/v2/hook/XXXX

`--destination-url` rewrites the shared `maxllm-http-sink` destination, so a
real webhook replaces the placeholder without touching every alert. Without it
the alerts are still created and shown in the UI, but they notify a placeholder.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DESTINATION_NAME = "maxllm-http-sink"
TEMPLATE_NAME = "maxllm-json"

# name -> (description, promql, threshold, period_minutes, silence_minutes,
#          stream_name)
#
# `stream_name` looks redundant when the expression aggregates several metric
# streams, but OpenObserve rejects a PromQL alert without one — with the
# misleading message "Alert name is required" — and 404s when the named stream
# has never been written ("Stream X not found"). Every alert therefore anchors
# on a stream that carries data from the first request onwards, never on a
# counter that is still at zero.
#
# `period` is the look-back window the expression is evaluated over, which is
# also how the operator reads "how fast does this fire".
ALERTS: dict[str, tuple[str, str, float, int, int, str]] = {
    "maxllm-5xx-ratio": (
        "5xx share of gateway responses",
        'sum(rate(maxllm_requests_total{status=~"5.."}[5m]))'
        " / clamp_min(sum(rate(maxllm_requests_total[5m])), 0.001)",
        0.05,
        15,
        30,
        "maxllm_requests_total",
    ),
    "maxllm-breakers-open": (
        "More than two providers circuit-open",
        "sum(maxllm_circuit_breaker_state == 1)",
        2,
        15,
        30,
        "maxllm_circuit_breaker_state",
    ),
    # A stream that ends before its protocol terminal event was booked as a
    # truncated 200. 1% of requests already means clients are losing answers.
    "maxllm-stream-truncation": (
        "Streaming responses truncated before their terminal event",
        "sum(rate(maxllm_stream_truncated_total[5m]))"
        " / clamp_min(sum(rate(maxllm_streams_judged_total[5m])), 0.001)",
        0.01,
        15,
        30,
        "maxllm_streams_judged_total",
    ),
    # A 2xx that reported no tokens is booked as a free request: the ledger
    # under-counts and the usage bug is invisible without this.
    "maxllm-zero-usage": (
        "Successful responses carrying no token usage",
        "sum(rate(maxllm_usage_missing_total[5m]))"
        " / clamp_min(sum(rate(maxllm_requests_total[5m])), 0.001)",
        0.05,
        15,
        30,
        "maxllm_requests_total",
    ),
    # The unrecoverable class: the upstream died before the client saw a byte.
    "maxllm-truncation-before-first-byte": (
        "Streams truncated before the first byte reached the client",
        "sum(rate(maxllm_stream_truncated_before_first_byte_total[10m]))",
        0,
        10,
        60,
        "maxllm_streams_judged_total",
    ),
    # A provider that truncates a large share of its streams is demoted off the
    # route; that is a deliberate routing change and should be reported.
    "maxllm-stream-demotions": (
        "Provider demoted for an excessive stream-truncation ratio",
        "max(increase(maxllm_circuit_breaker_stream_opens_total[10m]))",
        0,
        10,
        60,
        "maxllm_circuit_breaker_state",
    ),
}


def _request(method: str, url: str, auth: str, body: dict | None = None) -> tuple[int, object]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Basic {auth}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode() or "{}"
            return resp.status, json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode() or "{}"
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def alert_body(name: str) -> dict:
    description, promql, threshold, period, silence, stream_name = ALERTS[name]
    return {
        "name": name,
        "description": description,
        "stream_type": "metrics",
        "stream_name": stream_name,
        "is_real_time": False,
        # The write schema calls this `query_condition`; the read API renames it
        # to `condition`. Getting it wrong fails with a misleading
        # "alert name is required".
        "query_condition": {
            "type": "promql",
            "promql": promql,
            "promql_condition": {
                "column": "value",
                "operator": ">=",
                "value": threshold,
                "ignore_case": False,
            },
        },
        "trigger_condition": {
            "period": period,
            "operator": ">=",
            "threshold": 0,
            "frequency": 5,
            "frequency_type": "minutes",
            "silence": silence,
            "timezone": "Asia/Shanghai",
            "align_time": True,
        },
        "destinations": [DESTINATION_NAME],
        "row_template": "",
        "row_template_type": "String",
        "enabled": True,
        "tz_offset": 0,
        "creates_incident": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--org", default="default")
    parser.add_argument("--endpoint", default=os.environ.get("OPENOBSERVE_ENDPOINT", ""),
                        help="OpenObserve base incl. /api/<org>; defaults to $OPENOBSERVE_ENDPOINT")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    parser.add_argument("--apply", action="store_true", help="create/update the alerts")
    parser.add_argument("--destination-url", default=None,
                        help="rewrite the %s webhook destination" % DESTINATION_NAME)
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("choose --dry-run or --apply")

    if args.dry_run:
        for name in ALERTS:
            body = alert_body(name)
            print(f"--- {name} (period={body['trigger_condition']['period']}m, threshold {ALERTS[name][2]})")
            print(f"    {ALERTS[name][0]}")
            print(f"    {ALERTS[name][1]}")
        if args.destination_url:
            print(f"--- destination {DESTINATION_NAME} -> {args.destination_url}")
        print(f"\n{len(ALERTS)} alerts, dry run — nothing changed")
        return 0

    auth = os.environ.get("OPENOBSERVE_AUTH", "")
    if not auth:
        return _fail("OPENOBSERVE_AUTH is not set")
    if not args.endpoint:
        return _fail("no endpoint: pass --endpoint or set OPENOBSERVE_ENDPOINT")

    # OPENOBSERVE_ENDPOINT is ".../api/default"; the API root is what precedes it.
    endpoint = args.endpoint.rstrip("/")
    suffix = f"/api/{args.org}"
    base = endpoint[: -len(suffix)] if endpoint.endswith(suffix) else endpoint
    api = f"{base}/api/v2/{args.org}/alerts"

    if args.destination_url:
        status, payload = _request(
            "GET", f"{base}/api/{args.org}/alerts/destinations/{DESTINATION_NAME}", auth
        )
        if status != 200:
            return _fail(f"cannot read destination {DESTINATION_NAME}: {status} {payload}")
        payload["url"] = args.destination_url
        status, payload = _request(
            "PUT", f"{base}/api/{args.org}/alerts/destinations/{DESTINATION_NAME}", auth, payload
        )
        if status not in (200, 201):
            return _fail(f"cannot update destination: {status} {payload}")
        print(f"destination {DESTINATION_NAME} -> {args.destination_url}")

    status, existing = _request("GET", api, auth)
    if status != 200:
        return _fail(f"cannot list alerts: {status} {existing}")
    by_name = {a["name"]: a["alert_id"] for a in existing.get("list", [])}

    failures = 0
    for name in ALERTS:
        body = alert_body(name)
        if name in by_name:
            status, payload = _request("PUT", f"{api}/{by_name[name]}", auth, body)
            action = "updated"
        else:
            status, payload = _request("POST", api, auth, body)
            action = "created"
        if status not in (200, 201):
            print(f"FAIL  {name}: {status} {payload}", file=sys.stderr)
            failures += 1
        else:
            print(f"ok    {name}: {action}")
    return 1 if failures else 0


def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
