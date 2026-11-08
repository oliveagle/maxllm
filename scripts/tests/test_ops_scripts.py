#!/usr/bin/env python3
"""Regression tests for the ops scripts that touch production state.

These run in CI: ``python3 -m unittest discover -s scripts/tests``.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import redact_recordings  # noqa: E402
import rotate_logs  # noqa: E402


class RotateLogsTest(unittest.TestCase):
    def test_small_log_is_left_alone(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "maxllm.log")
            Path(path).write_text("a few lines\n")
            self.assertIsNone(rotate_logs.rotate(path, max_bytes=1024, keep=2, dry_run=False))
            self.assertFalse(os.path.exists(path + ".1"))

    def test_rotation_copies_then_truncates(self) -> None:
        # The service unit holds the log open with O_APPEND, so the file must
        # be truncated in place rather than renamed away.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "maxllm.err")
            Path(path).write_text("x" * 2048)
            rotated = rotate_logs.rotate(path, max_bytes=1024, keep=2, dry_run=False)
            self.assertEqual(rotated, path)
            self.assertEqual(Path(path).read_text(), "")
            self.assertEqual(os.path.getsize(path + ".1"), 2048)

    def test_dry_run_touches_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "maxllm.log")
            Path(path).write_text("x" * 2048)
            rotate_logs.rotate(path, max_bytes=1024, keep=2, dry_run=True)
            self.assertEqual(os.path.getsize(path), 2048)
            self.assertFalse(os.path.exists(path + ".1"))

    def test_prune_recordings_by_age(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old = os.path.join(tmp, "2026-01-01T00.jsonl")
            new = os.path.join(tmp, "2026-01-02T00.jsonl")
            Path(old).write_text("{}\n")
            Path(new).write_text("{}\n")
            two_days_ago = time.time() - 2 * 86400
            os.utime(old, (two_days_ago, two_days_ago))
            removed = rotate_logs.prune_recordings(tmp, days=1, dry_run=False)
            self.assertEqual(removed, [old])
            self.assertTrue(os.path.exists(new))

    def test_prune_binary_backups_keeps_the_newest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            now = time.time()
            for i in range(6):
                path = os.path.join(tmp, f"maxllm-server.bak-20261108-{i:02d}")
                Path(path).write_text("bin")
                os.utime(path, (now - i * 60, now - i * 60))
            Path(os.path.join(tmp, "maxllm-server")).write_text("current")
            Path(os.path.join(tmp, "maxllm-commandcode.disabled-20261108")).write_text("d")
            Path(os.path.join(tmp, "maxllm-server.bak-pre-auto")).write_text("archived")

            removed = rotate_logs.prune_binary_backups(tmp, keep=2, dry_run=False)
            self.assertEqual(len(removed), 4)
            left = sorted(n for n in os.listdir(tmp) if rotate_logs.DATED_BACKUP.search(n))
            self.assertEqual(
                left, ["maxllm-server.bak-20261108-00", "maxllm-server.bak-20261108-01"]
            )
            # Never the live binary, never a deliberate archive: only dated
            # deploy backups (`.bak-YYYYMMDD-*`) are rotated.
            self.assertTrue(os.path.exists(os.path.join(tmp, "maxllm-server")))
            self.assertTrue(
                os.path.exists(os.path.join(tmp, "maxllm-commandcode.disabled-20261108"))
            )
            self.assertTrue(os.path.exists(os.path.join(tmp, "maxllm-server.bak-pre-auto")))


class RedactRecordingsTest(unittest.TestCase):
    def test_credential_keys_are_replaced_whole(self) -> None:
        body = {"authorization": "Bearer abc", "api_key": "sk-live-1234", "model": "gpt"}
        out, changed = redact_recordings.redact_value(body, pii=True)
        self.assertTrue(changed)
        self.assertEqual(out["authorization"], redact_recordings.REDACTED)
        self.assertEqual(out["api_key"], redact_recordings.REDACTED)
        self.assertEqual(out["model"], "gpt")

    def test_token_counts_are_not_credentials(self) -> None:
        body = {"max_tokens": 4096, "prompt_tokens": 12, "completion_tokens": 7}
        out, changed = redact_recordings.redact_value(body, pii=True)
        self.assertFalse(changed)
        self.assertEqual(out, body)

    def test_secret_shapes_inside_prompts(self) -> None:
        text = "use sk-abcdefghijklmnop1234 and Authorization: Bearer abcdefghijklmnop"
        scrubbed, changed = redact_recordings.redact_text(text, pii=False)
        self.assertTrue(changed)
        self.assertNotIn("sk-abcdefghijklmnop1234", scrubbed)
        self.assertNotIn("abcdefghijklmnop", scrubbed)

    def test_pii_is_optional(self) -> None:
        text = "mail a.b@example.com or call 13800138000"
        kept, changed = redact_recordings.redact_text(text, pii=False)
        self.assertFalse(changed)
        self.assertIn("a.b@example.com", kept)

        scrubbed, changed = redact_recordings.redact_text(text, pii=True)
        self.assertTrue(changed)
        self.assertNotIn("a.b@example.com", scrubbed)
        self.assertNotIn("13800138000", scrubbed)

    def test_untouched_shard_keeps_its_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "2026-01-01T00.jsonl")
            entry = {"event_type": "client_request", "body": {"model": "auto"}}
            Path(path).write_text(json.dumps(entry) + "\n")
            before = os.path.getmtime(path)
            total, changed = redact_recordings.scrub_file(path, pii=True, dry_run=False)
            self.assertEqual((total, changed), (1, 0))
            self.assertEqual(os.path.getmtime(path), before)

    def test_scrubbed_entry_is_marked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "2026-01-01T00.jsonl")
            entry = {"event_type": "client_request", "body": {"authorization": "Bearer xyz"}}
            Path(path).write_text(json.dumps(entry) + "\n")
            total, changed = redact_recordings.scrub_file(path, pii=True, dry_run=False)
            self.assertEqual((total, changed), (1, 1))
            rewritten = json.loads(Path(path).read_text())
            self.assertTrue(rewritten["redacted"])
            self.assertEqual(rewritten["body"]["authorization"], redact_recordings.REDACTED)


if __name__ == "__main__":
    unittest.main()
