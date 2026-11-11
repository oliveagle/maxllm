#!/usr/bin/env python3
"""Make one auto pool answer with exactly one model family — repeatably.

Why this exists. The `auto_home` pool is the deployment's promise that
"the model you asked for at home" is a home model and not a cloud model.
Two axes carry that promise and both are checked by the gateway at load:

    scope          whose hardware may answer   (self_hosted | vendor)
    allowed_models which model may answer      (a list of model names)

`auto_home` wants both: our own hardware, and only the groots. The
`humanizer` model also lives at home, so nothing but `allowed_models`
keeps it out of the rotation — without that field the pool's membership
is a convention, and one config edit away from answering with a model
nobody asked for.

This script applies that policy to a config file: it drops every pool
member whose model is not on the allowed list and declares the list on
the pool, so the gateway enforces it from then on. It is idempotent.

Why it exists as a script and not as a hand edit: oleNas01 is the
config source of truth and oleFedora01 pulls it every two minutes, but
the Tencent Cloud node has no sync timer and its config has drifted
structurally (its own paths, its own provider set) — a wholesale copy
from the NAS would break it. So the policy travels as a script and is
applied to that node's own file.

Usage:
    scripts/apply-auto-home-policy.py <config.toml> [--pool auto_home]
                                       [--model groot-awq] [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:                                   # pragma: no cover
    print("python 3.11+ required (tomllib)", file=sys.stderr)
    raise

HEADER = re.compile(r"^\s*\[")


def blocks_of(lines: list[str]) -> list[dict]:
    out = [{"hdr": None, "lines": []}]
    for line in lines:
        if HEADER.match(line):
            out.append({"hdr": line.strip(), "lines": [line]})
        else:
            out[-1]["lines"].append(line)
    return out


def field(block: dict, key: str) -> str | None:
    pat = re.compile(rf'^\s*{re.escape(key)}\s*=\s*"([^"]*)"')
    for line in block["lines"]:
        m = pat.match(line)
        if m:
            return m.group(1)
    return None


def set_field(block: dict, key: str, value: str) -> None:
    pat = re.compile(rf"^\s*{re.escape(key)}\s*=")
    for i, line in enumerate(block["lines"]):
        if pat.match(line):
            block["lines"][i] = f"{key} = {value}\n"
            return
    block["lines"].append(f"{key} = {value}\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("--pool", default="auto_home",
                    help="pool name, i.e. the [auto_models.<pool>] section")
    ap.add_argument("--model", action="append", dest="models",
                    help="allowed model name (repeatable)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    allowed = args.models or ["groot-awq"]
    path = Path(args.config).expanduser()
    original = path.read_text()
    blocks = blocks_of(original.splitlines(keepends=True))

    section_hdr = f"[auto_models.{args.pool}]"
    entry_hdr = f"[[auto_models.{args.pool}.pool]]"

    kept: list[dict] = []
    dropped: list[str] = []
    in_section = False
    saw_section = False

    for blk in blocks:
        hdr = blk["hdr"]
        if hdr == section_hdr:
            in_section = True
            saw_section = True
            set_field(blk, "allowed_models",
                      "[" + ", ".join('"%s"' % m for m in allowed) + "]")
            kept.append(blk)
            continue
        if hdr == entry_hdr:
            model = field(blk, "model") or ""
            if model not in allowed:
                dropped.append("%s (%s)" % (field(blk, "provider"), model))
                continue
            kept.append(blk)
            continue
        if hdr is not None and not hdr.startswith(entry_hdr):
            in_section = False
        kept.append(blk)

    if not saw_section:
        print(f"no {section_hdr} section in {path} — nothing to do", file=sys.stderr)
        return 2

    new_text = "".join("".join(b["lines"]) for b in kept)

    # Fail closed: never write a file the gateway would refuse to load.
    try:
        doc = tomllib.loads(new_text)
    except Exception as e:                              # noqa: BLE001
        print(f"REFUSING to write: the result is not valid TOML ({e})", file=sys.stderr)
        return 1
    pool = doc.get("auto_models", {}).get(args.pool, {})
    if pool.get("allowed_models") != allowed:
        print("REFUSING to write: allowed_models did not land", file=sys.stderr)
        return 1
    for entry in pool.get("pool", []):
        if entry.get("model") not in allowed:
            print("REFUSING to write: %s still serves '%s'"
                  % (entry.get("provider"), entry.get("model")), file=sys.stderr)
            return 1

    if args.dry_run:
        print(f"would drop {len(dropped)} member(s) from {args.pool}: "
              + (", ".join(dropped) or "none"))
        print(f"would set allowed_models = {allowed}")
        return 0

    shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    path.write_text(new_text)
    print(f"wrote {path}  (backup {path.name}.bak)")
    print(f"  {args.pool} allowed_models = {allowed}")
    for d in dropped:
        print(f"  removed member: {d}")
    print(f"  {args.pool} members now: "
          + ", ".join("%s(%s)" % (e.get("provider"), e.get("model"))
                      for e in pool.get("pool", [])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
