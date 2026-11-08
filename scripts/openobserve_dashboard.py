#!/usr/bin/env python3
"""Ensure the MaxLLM reliability dashboard has its core panels.

The dashboard itself was assembled in the OpenObserve UI, which makes it the
one piece of the observability stack that cannot be rebuilt from the repo — a
replaced instance loses the panels silently. This script makes the panel set
declarative: it fetches the live dashboard, adds any panel whose title is
missing (cloning the layout/config of a live PromQL panel, or a built-in
skeleton when the dashboard has none), and re-points a declared panel whose
query or unit drifted away from this file — one declared panel summed a
per-node breaker gauge and read one provider as two the moment two gateway
nodes had it open. Panels whose titles are not declared here are never
touched, so hand-made panels survive.

Every `--apply` re-reads the dashboard afterwards and checks that each
declared panel really carries the declared query, stream and unit: the update
endpoint answers 200 for writes it silently drops.

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
        "Client aborts/s by provider",
        "sum by (provider) (rate(maxllm_client_aborted_total[5m]))",
        "maxllm_client_aborted_total",
        "reqps",
        "Callers walking away mid-body (Ctrl-C, a client timeout, a model switch)."
        " Not a provider fault - read it next to the truncation panels, never as"
        " one: on 2026-11-08 the two were conflated and 816 aborts were reported as"
        " 11.2% silent truncation when only one real truncation existed.",
    ),
    (
        "Recording redactions / 10m",
        "sum(increase(maxllm_recording_redactions_total[10m]))",
        "maxllm_recording_redactions_total",
        "short",
        "Clients pasting credentials or personal data into prompts. Scrubbed before disk, but the key still reached a model provider.",
    ),
    (
        "Request rate by provider",
        "sum by (provider) (rate(maxllm_requests_total[5m]))",
        "maxllm_requests_total",
        "reqps",
        "Gateway throughput split by the provider that served it — the first panel to read when a provider starts to lag.",
    ),
    (
        "5xx ratio",
        "sum(rate(maxllm_requests_total{status=~\"5..\"}[5m]))"
        " / clamp_min(sum(rate(maxllm_requests_total[5m])), 0.001)",
        "maxllm_requests_total",
        "percentunit",
        "Share of responses the upstream failed. Backed by the maxllm-5xx-ratio alert.",
    ),
    (
        "4xx ratio",
        "sum(rate(maxllm_requests_total{status=~\"4..\"}[5m]))"
        " / clamp_min(sum(rate(maxllm_requests_total[5m])), 0.001)",
        "maxllm_requests_total",
        "percentunit",
        "Share of requests the upstream rejected (bad model name, quota, auth). A 429-heavy line is quota pressure, not breakage.",
    ),
    (
        "p95 latency by provider",
        "histogram_quantile(0.95, sum by (le, provider) (rate(maxllm_request_duration_seconds_bucket[5m])))",
        "maxllm_request_duration_seconds_bucket",
        "s",
        "Whole-response latency per provider — the measurement the auto-pool fast-cohort rotation is built on.",
    ),
    (
        "Input tokens/s",
        "sum(rate(maxllm_tokens_in_total[5m]))",
        "maxllm_tokens_in_total",
        "short",
        "Prompt throughput — the cheapest early signal for a runaway long-prompt client.",
    ),
    (
        "Output tokens/s",
        "sum(rate(maxllm_tokens_out_total[5m]))",
        "maxllm_tokens_out_total",
        "short",
        "Generated throughput.",
    ),
    (
        "Breakers open",
        "sum(max by (provider) (maxllm_circuit_breaker_state) == 1)",
        "maxllm_circuit_breaker_state",
        "short",
        "How many providers have an open circuit right now. The gauge is 0 closed / 1 open / 2 half-open, and the same provider is exported once per gateway node, so it is de-duplicated with max by (provider) before counting — summing the raw series counted one provider as two the moment two nodes had it open.",
    ),
    (
        "Half-open providers",
        "sum(max by (provider) (maxllm_circuit_breaker_state) == 2)",
        "maxllm_circuit_breaker_state",
        "short",
        "Providers whose fail window elapsed and that wait for one probe. A permanently half-open local GPU is normal (the box is off), a cloud provider stuck here is not.",
    ),
    (
        "Quota opens / 10m",
        "sum by (provider) (increase(maxllm_circuit_breaker_quota_opens_total[10m]))",
        "maxllm_circuit_breaker_quota_opens_total",
        "short",
        "How often a provider was force-opened for quota exhaustion or an empty wallet — the cost signal behind the breaker panel.",
    ),
    (
        "Zero-usage ratio",
        "sum(rate(maxllm_usage_missing_total[5m]))"
        " / clamp_min(sum(rate(maxllm_requests_total[5m])), 0.001)",
        "maxllm_usage_missing_total",
        "percentunit",
        "Share of responses carrying no token usage — the token/cost ledger going blind. Backed by the maxllm-zero-usage alert.",
    ),
    (
        "Metered pool selections / 10m",
        "sum(increase(maxllm_auto_pool_metered_selections_total[10m]))",
        "maxllm_auto_pool_metered_selections_total",
        "short",
        "Requests the auto pool had to serve from a metered entry instead of a subscription one — the daily cost lever.",
    ),
]

# A panel skeleton for an empty dashboard.
#
# The dashboard was first assembled in the UI, so the script could only clone an
# existing PromQL panel. That is unrecoverable on a dashboard with no panels at
# all — the live one had exactly zero, so the script reported "nothing to add"
# and the dashboard rendered blank. This skeleton is a copy of a panel the
# instance itself produced, stripped of its query, so the panel set can be
# rebuilt from scratch (fresh instance or emptied dashboard alike).
#
# Regenerate it with:
#   curl -s -H "Authorization: Basic $OPENOBSERVE_AUTH" \
#     "$OPENOBSERVE_ENDPOINT/dashboards/<id>" \
#     | python3 -c 'import json,sys; d=json.load(sys.stdin)["v8"]; \
#         print(json.dumps(d["tabs"][0]["panels"][0], indent=1, ensure_ascii=False))'
SKELETON_PANEL = json.loads(
    """
{
 "type": "metric",
 "config": {
  "show_legends": true,
  "legends_position": "bottom",
  "unit": "reqps",
  "decimals": 2.0,
  "line_thickness": 1.5,
  "step_value": "0",
  "top_results_others": false,
  "axis_border_show": false,
  "label_option": {
   "rotate": 0.0
  },
  "axis_label_rotate": 0.0,
  "show_symbol": false,
  "line_interpolation": "smooth",
  "legend_width": {
   "unit": "px"
  },
  "legend_height": {
   "unit": "px"
  },
  "base_map": {
   "type": "osm"
  },
  "map_type": {
   "type": "world"
  },
  "map_view": {
   "zoom": 1.0,
   "lat": 0.0,
   "lng": 0.0
  },
  "map_symbol_style": {
   "size": "by Value",
   "size_by_value": {
    "min": 1.0,
    "max": 100.0
   },
   "size_fixed": 2.0
  },
  "drilldown": [],
  "mark_line": [],
  "override_config": [],
  "connect_nulls": false,
  "no_value_replacement": "",
  "wrap_table_cells": false,
  "table_transpose": false,
  "table_dynamic_columns": false,
  "mappings": [],
  "color": {
   "mode": "palette-classic-by-series",
   "fixedColor": [
    "#53ca53"
   ],
   "seriesBy": "last",
   "colorBySeries": []
  },
  "trellis": {
   "layout": null,
   "num_of_columns": 1,
   "group_by_y_axis": false
  },
  "show_gridlines": true,
  "aggregation": "last",
  "lat_label": "latitude",
  "lon_label": "longitude",
  "weight_label": "weight",
  "name_label": "name",
  "table_aggregations": [
   "last"
  ],
  "promql_table_mode": "single",
  "visible_columns": [],
  "hidden_columns": [],
  "sticky_columns": [],
  "sticky_first_column": false,
  "column_order": [],
  "table_pagination": false,
  "table_pivot_show_row_totals": false,
  "table_pivot_show_col_totals": false,
  "table_pivot_sticky_row_totals": false,
  "table_pivot_sticky_col_totals": false,
  "panel_time_enabled": false
 },
 "queryType": "promql",
 "queries": [
  {
   "query": "",
   "vrlFunctionQuery": "",
   "customQuery": true,
   "fields": {
    "stream": "",
    "stream_type": "metrics",
    "x": [],
    "y": [],
    "z": [],
    "breakdown": [],
    "filter": {
     "filterType": "group",
     "logicalOperator": "AND",
     "conditions": []
    }
   },
   "config": {
    "promql_legend": "{}",
    "layer_type": "scatter",
    "weight_fixed": 1.0,
    "limit": 0.0,
    "min": 0.0,
    "max": 100.0,
    "time_shift": [],
    "query_label": ""
   },
   "joins": []
  }
 ]
}
"""
)


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
    """A PromQL panel whose layout/config we clone.

    Falls back to the built-in skeleton when the dashboard has no panel to
    clone: an empty dashboard used to be unrecoverable, and the live one was
    empty (the panel set could never be seeded).
    """
    for tab in dashboard["v8"].get("tabs") or []:
        for panel in tab.get("panels", []):
            if panel.get("queryType") == "promql":
                return panel
    return copy.deepcopy(SKELETON_PANEL)


def add_missing(dashboard: dict) -> tuple[list[str], list[str]]:
    """Reconcile the declarative panel set with the live dashboard.

    Returns (added, corrected): panels that were missing and panels whose
    query had drifted away from the declaration. A panel whose title is not
    declared here is never touched, so hand-made panels survive.
    """
    tabs = dashboard["v8"].setdefault("tabs", [])
    if not tabs:
        # A dashboard without a tab cannot hold panels; give it the default
        # tab the UI would have created.
        tabs.append({"name": "Tab1", "tabId": "tab1", "panels": []})
    tab = tabs[0]
    panels = tab["panels"]
    by_title = {str(p.get("title")): p for p in panels}
    tmpl = template_panel(dashboard)
    added: list[str] = []
    corrected: list[str] = []
    next_id = 1 + max(
        [int(str(p.get("id", "p0")).lstrip("p") or 0) for p in panels] or [0]
    )
    # First free grid row: two columns of w=6.
    y = max([int(p.get("layout", {}).get("y", 0)) + int(p.get("layout", {}).get("h", 4)) for p in panels] or [0])
    slot = 0
    for title, promql, stream, unit, description in PANELS:
        existing = by_title.get(title)
        if existing is not None:
            # The declared query is the source of truth for a managed panel:
            # a drifted query (e.g. one that summed a gauge across the
            # gateway nodes) is corrected instead of left to rot.
            queries = existing.get("queries") or []
            if queries:
                q = queries[0]
                drift = (
                    q.get("query") != promql
                    or (q.get("fields") or {}).get("stream") != stream
                    or (existing.get("config") or {}).get("unit") != unit
                )
                if drift:
                    q["query"] = promql
                    q.setdefault("fields", {})["stream"] = stream
                    existing.setdefault("config", {})["unit"] = unit
                    existing["description"] = description
                    corrected.append(title)
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
    return added, corrected


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
    added, corrected = add_missing(dashboard)
    after = len(dashboard["v8"]["tabs"][0]["panels"])
    for title in added:
        print(f"+ {title}")
    for title in corrected:
        print(f"~ {title} (query corrected)")
    if not added and not corrected:
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
    landed_panels = (verify["v8"].get("tabs") or [{}])[0].get("panels") or []
    landed = len(landed_panels)
    if landed != after:
        print(
            f"update reported success but the dashboard has {landed} panels, "
            f"expected {after} — refusing to call this done",
            file=sys.stderr,
        )
        return 1
    # The panel count is not enough: a panel can exist and still carry the
    # wrong query. Re-read the live dashboard and compare every declared
    # query against what is actually stored.
    live = {str(p.get("title")): p for p in landed_panels}
    for title, promql, stream, unit, _desc in PANELS:
        panel = live.get(title)
        if panel is None:
            print(f"panel {title!r} did not land", file=sys.stderr)
            return 1
        queries = panel.get("queries") or []
        if not queries or queries[0].get("query") != promql:
            print(f"panel {title!r} carries the wrong query", file=sys.stderr)
            return 1
        if (queries[0].get("fields") or {}).get("stream") != stream:
            print(f"panel {title!r} points at the wrong stream", file=sys.stderr)
            return 1
        if (panel.get("config") or {}).get("unit") != unit:
            print(f"panel {title!r} carries the wrong unit", file=sys.stderr)
            return 1
    print(
        f"dashboard {dash_id}: {before} -> {after} panels "
        f"({len(added)} added, {len(corrected)} corrected, queries verified)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
