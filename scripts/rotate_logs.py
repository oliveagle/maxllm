#!/usr/bin/env python3
"""Rotate the gateway's append-only logs and prune old recording files.

The service unit writes stdout/stderr with systemd's ``append:`` target, which
opens the file once with ``O_APPEND`` and never reopens it. That makes plain
truncation safe: with ``O_APPEND`` every write lands at the current end of file,
so the writer does not keep writing at a stale offset into a sparse file (the
classic ``copytruncate`` hole). This script therefore copies the current log to
``<name>.1`` and truncates the original in place — no restart, no signal, no
lost lines.

Recording files (``recordings/YYYY-MM-DDTHH.jsonl``) hold full request and
response bodies, so they are also retained for a bounded number of days rather
than forever.

Dated binary backups written next to the server
(``maxllm-server.bak-YYYYMMDD-<stamp>``) are pruned to the newest
``--bin-keep``; a deploy that keeps its previous binary "just in case"
otherwise leaves one 32 MB copy per deploy on the disk. Hand-named archives
(``.bak-pre-auto``, ``.disabled-*``) are left alone.

Usage::

    python3 scripts/rotate_logs.py --dir logs --recordings-dir recordings
    python3 scripts/rotate_logs.py --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shutil
import sys
import time

DEFAULT_LOGS = ("maxllm.log", "maxllm.err", "quota-probe-kimi.jsonl")


def rotate(path: str, max_bytes: int, keep: int, dry_run: bool) -> str | None:
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return None
    if size < max_bytes:
        return None

    # Shift <name>.N -> <name>.N+1, dropping the oldest.
    for generation in range(keep, 0, -1):
        src = f"{path}.{generation}"
        dst = f"{path}.{generation + 1}"
        if not os.path.exists(src):
            continue
        if generation == keep:
            if dry_run:
                print(f"would drop {src}")
            else:
                os.unlink(src)
        elif dry_run:
            print(f"would rename {src} -> {dst}")
        else:
            os.replace(src, dst)

    if dry_run:
        print(f"would rotate {path} ({size} bytes) -> {path}.1 and truncate")
        return path
    shutil.copyfile(path, f"{path}.1")
    with open(path, "r+") as handle:
        handle.truncate(0)
    return path


def prune_recordings(directory: str, days: int, dry_run: bool) -> list[str]:
    cutoff = time.time() - days * 86400
    removed: list[str] = []
    try:
        entries = os.listdir(directory)
    except FileNotFoundError:
        return removed
    for name in entries:
        if not name.endswith(".jsonl"):
            continue
        path = os.path.join(directory, name)
        try:
            if os.path.getmtime(path) >= cutoff:
                continue
        except OSError:
            continue
        removed.append(path)
        if dry_run:
            print(f"would prune {path}")
        else:
            os.unlink(path)
    return removed


# Only dated deploy backups (``maxllm-server.bak-20261108-213916``) are
# rotated. A hand-named archive (``.bak-pre-auto``, ``.bak-20261107-2201`` is
# dated, ``.disabled-20261108``) is a deliberate keep and is never touched.
DATED_BACKUP = re.compile(r"\.bak-\d{8}-")


def prune_binary_backups(directory: str, keep: int, dry_run: bool) -> list[str]:
    """Delete stale dated ``*.bak-YYYYMMDD-*`` binaries, keeping the newest `keep`."""
    removed: list[str] = []
    if keep < 0:
        return removed
    try:
        names = [n for n in os.listdir(directory) if DATED_BACKUP.search(n)]
    except FileNotFoundError:
        return removed
    stamped = []
    for name in names:
        path = os.path.join(directory, name)
        try:
            stamped.append((os.path.getmtime(path), path))
        except OSError:
            continue
    stamped.sort(reverse=True)
    for _, path in stamped[keep:]:
        removed.append(path)
        if dry_run:
            print(f"would prune {path}")
        else:
            os.unlink(path)
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dir", default="logs", help="directory holding the service logs")
    parser.add_argument("--file", action="append", help="log file name to rotate (repeatable)")
    parser.add_argument("--max-bytes", type=int, default=20 * 1024 * 1024)
    parser.add_argument("--keep", type=int, default=2, help="rotated generations to keep")
    parser.add_argument("--recordings-dir", default=None, help="prune recordings/*.jsonl here")
    parser.add_argument("--recordings-days", type=int, default=14)
    parser.add_argument(
        "--bin-dir",
        default=None,
        help="prune *.bak-* binaries here, keeping the newest --bin-keep",
    )
    parser.add_argument("--bin-keep", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    names = args.file or list(DEFAULT_LOGS)
    rotated = []
    for name in names:
        path = name if os.path.isabs(name) else os.path.join(args.dir, name)
        if rotate(path, args.max_bytes, args.keep, args.dry_run):
            rotated.append(path)

    pruned = []
    if args.recordings_dir:
        pruned = prune_recordings(args.recordings_dir, args.recordings_days, args.dry_run)

    pruned_bins = []
    if args.bin_dir:
        pruned_bins = prune_binary_backups(args.bin_dir, args.bin_keep, args.dry_run)

    stamp = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    verb = "would rotate" if args.dry_run else "rotated"
    print(
        f"{stamp} {verb} {len(rotated)} log(s), pruned {len(pruned)} recording file(s), "
        f"pruned {len(pruned_bins)} binary backup(s)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
