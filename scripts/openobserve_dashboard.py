#!/usr/bin/env python3
"""Ensure the MaxLLM reliability dashboard has its core panels.

The dashboard itself was assembled in the OpenObserve UI, which makes it the
one piece of the observability stack that cannot be rebuilt from the repo — a
replaced instance loses the panels silently. This script makes the panel set
declarative: it fetches the live dashboard, adds any panel whose title is
missing (cloning the layout/query config of an existing PromQL panel so the
styling matches), and leaves existing panels untouched so hand edits survive.

Idempotent: a second run reports "nothing to add". Credentials come from the
environment, never from this file:

    OPENOBSERVE_ENDPOINT   e.g. http://openobserve.internal:5080/api/default
    OPENOBSERVE_AUTH       base64 "user:password" for the Basic header

Usage:
    python3 scripts/openobserve_dashboard.py --dry-run
    python3 scripts/openobserve_dashboard.py --apply
    python3 scripts/openobserve_dashboard.py --apply --title "MaxLLM Gateway"
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_TITLE = "MaxLLM Gateway"

# title -> (promql, anchor stream, unit, description)
PANELS: list[tuple[str, str, str, str, str]] = [
    (
        "Stream truncations/s by provider",
        "sum by (provider) (rate(maxllm_stream_truncated_total[5m]))",
        "maxllm_stream_truncated_total",
        "reqps",
        "Which provider is dropping bodies — the per-provider view the ratio panels aggregate away.",
    ),
    (
        "Truncation ratio by provider",
        "sum by (provider) (rate(maxllm_stream_truncated_total[5m]))"
        " / clamp_min(sum by (provider) (rate(maxllm_streams_judged_total[5m])), 0.001)",
        "maxllm_streams_judged_total",
        "percentunit",
        "Per-provider share of judged streams that ended without their terminal event.",
    ),
    (
        "Truncated before first byte/s",
        "sum(rate(maxllm_stream_truncated_before_first_byte_total[10m]))",
        "maxllm_stream_truncated_before_first_byte_total",
        "reqps",
        "The class a future first-byte failover could still rescue: the upstream died before the client saw any body byte.",
    ),
    (
        "Recording redactions / 10m",
        "sum(increase(maxllm_recording_redactions_total[10m]))",
        "maxllm_recording_redactions_total",
        "short",
        "Clients pasting credentials or personal data into prompts. Scrubbed before disk, but the key still reached a model provider.",
    ),
]


def _request(
    method: str, url: str, auth: str, body: dict | None = None
) -> tuple[int, object]:
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


def find_dashboard(endpoint: str, auth: str, title: str) -> dict:
    status, payload = _request("GET", f"{endpoint}/dashboards", auth)
    if status != 200 or not isinstance(payload, dict):
        raise SystemExit(f"dashboard list failed: {status} {payload}")
    entries = payload.get("dashboards") or payload
    matches = [e for e in entries if title in str(e.get("title", ""))]
    if not matches:
        raise SystemExit(f"no dashboard whose title contains {title!r}")
    if len(matches) > 1:
        titles = ", ".join(sorted({str(e.get("title")) for e in matches}))
        raise SystemExit(f"title {title!r} is ambiguous: {titles}")
    return matches[0]


def template_panel(dashboard: dict) -> dict:
    """An existing PromQL panel whose layout/config we clone."""
    for tab in dashboard["v8"]["tabs"]:
        for panel in tab.get("panels", []):
            if panel.get("queryType") == "promql":
                return panel
    raise SystemExit("dashboard has no PromQL panel to clone")


def add_missing(dashboard: dict) -> list[str]:
    tabs = dashboard["v8"]["tabs"]
    if not tabs:
        raise SystemExit("dashboard has no tabs")
    tab = tabs[0]
    panels = tab["panels"]
    have = {str(p.get("title")) for p in panels}
    tmpl = template_panel(dashboard)
    added: list[str] = []
    next_id = 1 + max(
        [int(str(p.get("id", "p0")).lstrip("p") or 0) for p in panels] or [0]
    )
    # First free grid row: two columns of w=6.
    y = max([int(p.get("layout", {}).get("y", 0)) + int(p.get("layout", {}).get("h", 4)) for p in panels] or [0])
    slot = 0
    for title, promql, stream, unit, description in PANELS:
        if title in have:
            continue
        panel = copy.deepcopy(tmpl)
        panel["id"] = f"p{next_id}"
        next_id += 1
        panel["title"] = title
        panel["description"] = description
        query = panel["queries"][0]
        query["query"] = promql
        query["fields"]["stream"] = stream
        panel["config"]["unit"] = unit
        panel["layout"] = {"x": (slot % 2) * 6, "y": y + (slot // 2) * 4, "w": 6, "h": 4, "i": 0}
        slot += 1
        panels.append(panel)
        added.append(title)
    return added


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default=os.environ.get("OPENOBSERVE_ENDPOINT"))
    ap.add_argument("--auth", default=os.environ.get("OPENOBSERVE_AUTH"))
    ap.add_argument("--title", default=DEFAULT_TITLE, help="dashboard title substring")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.endpoint or not args.auth:
        print("OPENOBSERVE_ENDPOINT / OPENOBSERVE_AUTH are required", file=sys.stderr)
        return 2
    if not args.dry_run and not args.apply:
        print("pass --dry-run or --apply", file=sys.stderr)
        return 2

    dashboard = find_dashboard(args.endpoint.rstrip("/"), args.auth, args.title)
    before = len(dashboard["v8"]["tabs"][0]["panels"])
    added = add_missing(dashboard)
    after = len(dashboard["v8"]["tabs"][0]["panels"])
    for title in added:
        print(f"+ {title}")
    if not added:
        print(f"nothing to add — {before} panels already present")
        return 0
    if args.dry_run:
        print(f"dry run — would go from {before} to {after} panels")
        return 0

    dash_id = dashboard["dashboard_id"]
    # The update endpoint takes the v8 dashboard *itself* (not the legacy
    # `{"v1": …, "v8": …}` envelope the list returns) and the optimistic-lock
    # hash as a query parameter. Sending the envelope is accepted with 200 but
    # silently drops `tabs` — which is why this verifies the result below
    # instead of trusting the status code.
    url = f"{args.endpoint.rstrip('/')}/dashboards/{dash_id}?hash={dashboard['hash']}"
    status, payload = _request("PUT", url, args.auth, dashboard["v8"])
    if status != 200:
        print(f"update failed: {status} {payload}", file=sys.stderr)
        return 1

    verify = find_dashboard(args.endpoint.rstrip("/"), args.auth, args.title)
    landed = len((verify["v8"].get("tabs") or [{}])[0].get("panels") or [])
    if landed != after:
        print(
            f"update reported success but the dashboard has {landed} panels, "
            f"expected {after} — refusing to call this done",
            file=sys.stderr,
        )
        return 1
    print(f"dashboard {dash_id}: {before} -> {after} panels (verified)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
