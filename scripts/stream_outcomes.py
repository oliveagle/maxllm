#!/usr/bin/env python3
"""Split the gateway's 200s by *who* closed the stream.

The 2026-11-08 audit read every ``client_response`` with a non-empty ``error``
as a silent upstream truncation and concluded that 11.2% of all 200s were
truncated. pingora puts the error *source* first in that string, and almost all
of those rows said ``Downstream`` — the remote **client** hung up while the
gateway was still writing the body. Only ``Upstream`` means the origin server
dropped the stream:

    Downstream ConnectionClosed ... Prematurely before response body is complete
        -> ErrorSource::Downstream (pingora-error): "caused by the remote
           client"; produced by http/v1/server.rs read_body_or_idle().
    Upstream ConnectionClosed ... without the termination chunk, read N bytes
        -> ErrorSource::Upstream: the origin died mid-stream. This is the only
           shape that belongs in a truncation rate.

The ``Peer: addr: <upstream>`` fragment in either message is the HttpPeer that
``error_while_proxy()`` appends to every proxied error; it says nothing about
which side closed, which is how the two got conflated.

Usage:
    python3 scripts/stream_outcomes.py --dir recordings
    python3 scripts/stream_outcomes.py --dir recordings --json
    python3 scripts/stream_outcomes.py --dir recordings --since 2026-11-07

Read-only: opens the shards, never writes.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import Counter, defaultdict

CLIENT = "Downstream"
UPSTREAM = "Upstream"


def percentile(values: list[int], q: float) -> int:
    if not values:
        return 0
    return values[min(len(values) - 1, int(q * (len(values) - 1)))]


def duration_bucket(ms: int) -> str:
    if ms < 10_000:
        return "<10s"
    if ms < 30_000:
        return "10-30s"
    if ms < 60_000:
        return "30-60s"
    if ms < 120_000:
        return "60-120s"
    if ms < 300_000:
        return "120-300s"
    return ">=300s"


def error_source(error: str) -> str:
    """pingora prints ``<ErrorSource> <ErrorType> ...``; anything else is ours."""
    head = error.split(" ", 1)[0]
    return head if head in (CLIENT, UPSTREAM) else "Other"


def collect(directory: str, since: str | None) -> dict:
    stats = {
        "responses": 0,
        "status_200": 0,
        "client_abort": 0,
        "client_abort_before_first_byte": 0,
        "upstream_truncation": 0,
        "other_error": 0,
        "per_provider": defaultdict(
            lambda: {"rows": 0, "n": 0, "client": 0, "upstream": 0, "other": 0}
        ),
        "abort_durations": [],
        "abort_buckets": Counter(),
        "truncation_examples": [],
        "error_shapes": Counter(),
    }
    shards = sorted(glob.glob(os.path.join(directory, "*.jsonl")))
    for path in shards:
        if since and os.path.basename(path) < since:
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get("event_type") != "client_response":
                    continue
                stats["responses"] += 1
                is_200 = row.get("status_code") == 200
                if is_200:
                    stats["status_200"] += 1
                provider = row.get("provider") or "unknown"
                entry = stats["per_provider"][provider]
                entry["rows"] += 1
                if not is_200:
                    # Non-200 failures are ordinary errors: failover already
                    # reacted to them, and they never carried a partial body.
                    continue
                entry["n"] += 1
                error = (row.get("error") or "").strip()
                if not error:
                    continue
                source = error_source(error)
                # The informative bit is the innermost cause, not the
                # `<Source> <Type> context: Peer: …` prefix every error repeats.
                shape = error.rsplit("context:", 1)[-1].strip()[:80]
                stats["error_shapes"][(source, shape)] += 1
                if source == CLIENT:
                    entry["client"] += 1
                    stats["client_abort"] += 1
                    if not row.get("ttft_ms"):
                        stats["client_abort_before_first_byte"] += 1
                    duration = int(row.get("duration_ms") or 0)
                    stats["abort_durations"].append(duration)
                    stats["abort_buckets"][duration_bucket(duration)] += 1
                elif source == UPSTREAM:
                    entry["upstream"] += 1
                    stats["upstream_truncation"] += 1
                    if len(stats["truncation_examples"]) < 20:
                        stats["truncation_examples"].append(
                            {
                                "provider": provider,
                                "model": row.get("model"),
                                "duration_ms": row.get("duration_ms"),
                                "ttft_ms": row.get("ttft_ms"),
                                "error": error[:200],
                            }
                        )
                else:
                    entry["other"] += 1
                    stats["other_error"] += 1
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dir", default="recordings", help="recording shard directory")
    ap.add_argument(
        "--since",
        default=None,
        help="only shards whose file name sorts >= this (e.g. 2026-11-07)",
    )
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument(
        "--min-requests",
        type=int,
        default=20,
        help="hide providers with fewer 200s than this in the table",
    )
    args = ap.parse_args(argv)

    if not os.path.isdir(args.dir):
        print(f"no such directory: {args.dir}", file=sys.stderr)
        return 2
    stats = collect(args.dir, args.since)

    n200 = stats["status_200"]
    report = {
        "responses": stats["responses"],
        "status_200": n200,
        "upstream_truncation": stats["upstream_truncation"],
        "client_abort": stats["client_abort"],
        "client_abort_before_first_byte": stats["client_abort_before_first_byte"],
        "other_error": stats["other_error"],
        "abort_durations_ms": {
            "n": len(stats["abort_durations"]),
            "p50": percentile(sorted(stats["abort_durations"]), 0.5),
            "p90": percentile(sorted(stats["abort_durations"]), 0.9),
            "buckets": dict(stats["abort_buckets"]),
        },
        "per_provider": {
            name: entry for name, entry in sorted(stats["per_provider"].items())
        },
        "truncation_examples": stats["truncation_examples"],
    }
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0

    print(f"client_response rows : {stats['responses']}")
    print(f"  HTTP 200           : {n200}")
    print(
        f"  upstream truncation: {stats['upstream_truncation']}"
        f"  ({(100.0 * stats['upstream_truncation'] / n200) if n200 else 0:.2f}% of 200s)"
    )
    print(
        f"  client abort       : {stats['client_abort']}"
        f"  ({(100.0 * stats['client_abort'] / n200) if n200 else 0:.1f}% of 200s)"
        f"  [{stats['client_abort_before_first_byte']} before the first byte]"
    )
    print(f"  other 200 + error  : {stats['other_error']}")
    if stats["abort_durations"]:
        d = sorted(stats["abort_durations"])
        print(
            f"\nclient-abort duration: p50 {percentile(d, 0.5)}ms"
            f"  p90 {percentile(d, 0.9)}ms  max {d[-1]}ms"
        )
        for bucket in ("<10s", "10-30s", "30-60s", "60-120s", "120-300s", ">=300s"):
            if bucket in stats["abort_buckets"]:
                print(f"  {bucket:>8}: {stats['abort_buckets'][bucket]}")

    print("\nper provider (200s):")
    print(f"  {'provider':<34}{'n':>6}{'upstream':>10}{'client':>9}{'other':>7}")
    for name, entry in sorted(
        stats["per_provider"].items(), key=lambda kv: -kv[1]["n"]
    ):
        if entry["n"] < args.min_requests:
            continue
        print(
            f"  {name:<34}{entry['n']:>6}"
            f"{100.0 * entry['upstream'] / entry['n']:>9.2f}%"
            f"{100.0 * entry['client'] / entry['n']:>8.1f}%"
            f"{entry['other']:>7}"
        )

    if stats["error_shapes"]:
        print("\n200s that carried an error, by source and shape:")
        for (source, shape), count in stats["error_shapes"].most_common(8):
            print(f"  {count:>5}  {source:<10} {shape}")

    if stats["truncation_examples"]:
        print("\nupstream truncations (should be rare — these are the real ones):")
        for row in stats["truncation_examples"]:
            print(
                f"  {row['provider']:<28} {row['model'] or '':<28}"
                f" {row['duration_ms']}ms  {row['error'][:110]}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
