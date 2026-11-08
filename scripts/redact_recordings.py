#!/usr/bin/env python3
"""Scrub credentials and personal data out of recording shards already on disk.

The gateway redacts bodies before writing them (``[recording] redact``), but
shards written before that switch existed are plaintext. This script rewrites
every ``*.jsonl`` shard in place with the *same* rules the Rust recorder now
applies, so the historical audit trail survives while the secrets do not:

* JSON keys that conventionally carry credentials (``authorization``,
  ``api_key``, ``token``, ``password``, ``cookie``, …) are replaced whole.
  Keys are compared as normalised names, so token *counts* (``max_tokens``,
  ``prompt_tokens``) are untouched.
* High-confidence secret shapes are scrubbed inside ordinary strings — a key
  pasted into a prompt lands in ``input``/``messages``, not in a header field.
* Optional PII: e-mail addresses, mainland-China mobile numbers and 18-digit
  resident IDs (``--no-pii`` keeps them).

Entries whose body changed gain ``"redacted": true`` so a reader can tell a
scrubbed body from a partial one. Files with nothing to scrub are left
untouched (no mtime churn, no needless re-ship).

Usage::

    python3 scripts/redact_recordings.py --dir recordings            # rewrite
    python3 scripts/redact_recordings.py --dir recordings --dry-run  # report only
    python3 scripts/redact_recordings.py --dir recordings --no-pii
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile

REDACTED = "[REDACTED]"

SECRET_RULES = [
    # `Authorization: Bearer <token>` — keep the scheme, drop the secret.
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=\-]{8,}"), r"\1 [REDACTED]"),
    # OpenAI / Anthropic / generic `sk-` / `rk-` keys.
    (re.compile(r"\b[sr]k-[A-Za-z0-9_\-]{12,}"), REDACTED),
    # AWS access key id.
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), REDACTED),
    # GitHub tokens.
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}"), REDACTED),
    # Slack tokens.
    (re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{8,}"), REDACTED),
    # Google API keys.
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"), REDACTED),
    # JWTs.
    (
        re.compile(
            r"\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}"
        ),
        REDACTED,
    ),
    # Inline `api_key=…` / `token: …` inside free text.
    (
        re.compile(
            r"""(?i)\b(api[_-]?key|access[_-]?key|secret[_-]?key|auth[_-]?token|token|password)\b\s*[:=]\s*["']?[A-Za-z0-9._\-+/=]{12,}"""
        ),
        r"\1=[REDACTED]",
    ),
]

PII_RULES = [
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "[EMAIL]"),
    (re.compile(r"\b1[3-9][0-9]{9}\b"), "[PHONE]"),
    (
        re.compile(
            r"\b[1-9][0-9]{5}(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[0-9]{3}[0-9Xx]\b"
        ),
        "[ID]",
    ),
]

CREDENTIAL_KEYS = {
    "authorization",
    "proxyauthorization",
    "apikey",
    "xapikey",
    "apikeyid",
    "accesskey",
    "accesskeyid",
    "secretkey",
    "secretaccesskey",
    "awssecretaccesskey",
    "secret",
    "clientsecret",
    "password",
    "passwd",
    "pwd",
    "token",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "sessiontoken",
    "authtoken",
    "bearertoken",
    "cookie",
    "setcookie",
    "privatekey",
    "credentials",
    "subscriptionkey",
}


def _norm_key(key: str) -> str:
    return "".join(c for c in key if c.isalnum()).lower()


def redact_text(text: str, pii: bool) -> tuple[str, bool]:
    changed = False
    for pattern, replacement in SECRET_RULES:
        new, n = pattern.subn(replacement, text)
        if n:
            text, changed = new, True
    if pii:
        for pattern, replacement in PII_RULES:
            new, n = pattern.subn(replacement, text)
            if n:
                text, changed = new, True
    return text, changed


def redact_value(value, pii: bool) -> tuple[object, bool]:
    changed = False
    if isinstance(value, dict):
        out = {}
        for key, val in value.items():
            if _norm_key(str(key)) in CREDENTIAL_KEYS and val not in (None, ""):
                out[key] = REDACTED
                changed = True
            else:
                new, sub_changed = redact_value(val, pii)
                out[key] = new
                changed = changed or sub_changed
        return out, changed
    if isinstance(value, list):
        out = []
        for item in value:
            new, sub_changed = redact_value(item, pii)
            out.append(new)
            changed = changed or sub_changed
        return out, changed
    if isinstance(value, str):
        return redact_text(value, pii)
    return value, False


def scrub_entry(entry: dict, pii: bool) -> tuple[dict, bool]:
    changed = False
    if "body" in entry:
        new_body, body_changed = redact_value(entry["body"], pii)
        if body_changed:
            entry["body"] = new_body
            changed = True
    if isinstance(entry.get("error"), str):
        new_error, error_changed = redact_text(entry["error"], pii)
        if error_changed:
            entry["error"] = new_error
            changed = True
    if changed:
        entry["redacted"] = True
    return entry, changed


def scrub_line(line: str, pii: bool) -> tuple[str, bool]:
    stripped = line.strip()
    if not stripped:
        return line, False
    try:
        entry = json.loads(stripped)
    except json.JSONDecodeError:
        # A truncated/garbled line still must not leak: scrub it as text.
        new, changed = redact_text(line, pii)
        return (new if changed else line), changed
    if not isinstance(entry, dict):
        return line, False
    entry, changed = scrub_entry(entry, pii)
    if not changed:
        return line, False
    return json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n", True


def scrub_file(path: str, pii: bool, dry_run: bool) -> tuple[int, int]:
    """Return (lines_total, lines_changed) and rewrite the file when needed."""
    lines_total = 0
    lines_changed = 0
    out_lines: list[str] = []
    with open(path, "r", encoding="utf-8", errors="surrogateescape") as fh:
        for line in fh:
            lines_total += 1
            new, changed = scrub_line(line, pii)
            out_lines.append(new)
            if changed:
                lines_changed += 1
    if lines_changed == 0 or dry_run:
        return lines_total, lines_changed

    # Atomic replace inside the same directory keeps a reader (the OTel
    # collector tails these files) from ever seeing a half-written shard.
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".redact-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", errors="surrogateescape") as fh:
            fh.writelines(out_lines)
        os.chmod(tmp, os.stat(path).st_mode & 0o777)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return lines_total, lines_changed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default="recordings", help="recording shard directory")
    ap.add_argument(
        "--no-pii",
        action="store_true",
        help="keep e-mail / phone / resident-ID shapes (credentials are still scrubbed)",
    )
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    ap.add_argument(
        "--live",
        action="store_true",
        help="gateway is running: skip the newest shard, whose append-only fd a "
        "rename would orphan (run the scrub with the service stopped instead)",
    )
    args = ap.parse_args(argv)

    if not os.path.isdir(args.dir):
        print(f"not a directory: {args.dir}", file=sys.stderr)
        return 2

    pii = not args.no_pii
    shards = sorted(
        os.path.join(args.dir, name)
        for name in os.listdir(args.dir)
        if name.endswith(".jsonl")
    )
    if args.live and shards:
        skipped = shards.pop()
        print(f"live mode: skipping {os.path.basename(skipped)} (open by the gateway)")

    total_lines = total_changed = touched = 0
    for path in shards:
        lines, changed = scrub_file(path, pii, args.dry_run)
        total_lines += lines
        total_changed += changed
        if changed:
            touched += 1
            verb = "would scrub" if args.dry_run else "scrubbed"
            print(f"{verb} {changed}/{lines} lines: {os.path.basename(path)}")

    print(
        f"{'DRY RUN — ' if args.dry_run else ''}"
        f"{total_changed} of {total_lines} lines redacted across "
        f"{touched}/{len(shards)} shard(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
