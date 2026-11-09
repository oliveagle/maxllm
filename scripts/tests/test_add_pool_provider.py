#!/usr/bin/env python3
"""Regression tests for scripts/add_pool_provider.sh.

The script edits oleNas01's live maxllm.toml. Three of its bugs cost a
production outage of the pool's growth path (2026-11-09 21:27):

* `billing = "sub"` — the config enum accepts `subscription` | `metered`,
  so the whole file stopped parsing and the gateway kept the OLD config;
  the new member never joined the pool and nothing said so.
* `--pool auto_home` wrote the table `[[auto_model.pool]]` (the default
  pool's name), so the entry landed in the auto pool.
* `rm` removed the FIRST `add_pool_provider.sh` block in the file, not the
  named provider's block.

These tests drive the real script text, so they fail if a fix is reverted.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "add_pool_provider.sh"


def script_text() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def embedded_python(source: str, marker: str) -> str:
    """Extract one `<<PY ... PY` heredoc body from the script."""
    blocks = re.findall(r"<<PY\n(.*?)\nPY", source, flags=re.S)
    matches = [b for b in blocks if marker in b]
    assert len(matches) == 1, f"expected one heredoc containing {marker!r}, got {len(matches)}"
    return matches[0]


def provider_block_shell() -> str:
    """The shell that builds PROVIDER_BLOCK: init line .. closing quote.

    Runs the real argument parser and the real derivation, so the tests
    exercise the behaviour instead of matching a string.
    """
    lines = script_text().splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith('KIND="openai_compat"; UPSTREAM='))
    assign = next(i for i, l in enumerate(lines) if l.startswith("PROVIDER_BLOCK="))
    end = next(i for i in range(assign, len(lines)) if lines[i].strip() == '"')
    return "\n".join(lines[start : end + 1]) + '\nprintf "%s" "$PROVIDER_BLOCK"\n'


def provider_block(*args: str) -> str:
    script = provider_block_shell()
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "block.sh"
        f.write_text(script, encoding="utf-8")
        env = {
            "PATH": "/usr/bin:/bin",
            "NAME": "acct",
            "MODEL": "m",
            "KEY": "sk-test",
            "POOL": "auto",
            "BILLING": "subscription",
        }
        out = subprocess.run(["bash", str(f), *args], capture_output=True, text=True, env=env)
    assert out.returncode == 0, out.stderr
    tomllib.loads(out.stdout)  # a provider block that does not parse is unusable
    return out.stdout


class BillingValueTest(unittest.TestCase):
    def test_only_the_values_the_config_enum_accepts_are_offered(self) -> None:
        source = script_text()
        # The config enum is `subscription` | `metered` (serde snake_case).
        self.assertIn('case "$BILLING" in subscription|metered)', source)
        self.assertNotIn('case "$BILLING" in sub|metered)', source)

    def test_the_default_billing_is_subscription(self) -> None:
        source = script_text()
        self.assertIn('BILLING="${BILLING:-subscription}"', source)

    def test_the_written_entry_parses_and_keeps_the_subscription_tier(self) -> None:
        source = script_text()
        # Evaluate the shell that builds POOL_ENTRY, the way bash would.
        # The assignment spans lines and contains escaped quotes, so it is
        # cut by lines: from the `if [ "$POOL" ...` guard up to the line
        # that closes the assignment (a lone `"`).
        lines = source.splitlines()
        start = next(i for i, l in enumerate(lines) if l.startswith('if [ "$POOL" = "auto" ]'))
        assign = next(i for i, l in enumerate(lines) if l.startswith("POOL_ENTRY="))
        end = next(i for i in range(assign, len(lines)) if lines[i].strip() == '"')
        snippet = "\n".join(lines[start : end + 1]) + '\nprintf %s "$POOL_ENTRY"\n'
        with tempfile.TemporaryDirectory() as tmp:
            script_file = Path(tmp) / "entry.sh"
            script_file.write_text(snippet, encoding="utf-8")
            out = subprocess.run(
                ["bash", str(script_file)],
                capture_output=True,
                text=True,
                env={"PATH": "/usr/bin:/bin", "POOL": "auto", "NAME": "acct", "MODEL": "m", "BILLING": "subscription"},
            )
        self.assertEqual(out.returncode, 0, out.stderr)
        parsed = tomllib.loads(out.stdout)
        self.assertEqual(parsed["auto_model"]["pool"][0]["billing"], "subscription")
        self.assertEqual(parsed["auto_model"]["pool"][0]["provider"], "acct")


class ResponsesPathTest(unittest.TestCase):
    """The Responses path must follow the chat path, not a hardcoded default.

    `volc_1_spare` (2026-11-09) was added with `responses_path =
    "/v1/responses"` while its upstream path was
    `/api/coding/v3/chat/completions`: the pool routed to it, the upstream
    answered 404 and the gateway failed over — the member joined the pool
    but could never serve a single request.
    """

    def test_the_responses_path_follows_the_chat_path(self) -> None:
        block = provider_block("--upstream-path", "/api/coding/v3/chat/completions")
        self.assertIn('responses_path = "/api/coding/v3/responses"', block)
        self.assertIn("native_responses = true", block)

    def test_the_openai_default_path_is_derived_not_handed_over_as_a_v1_path(self) -> None:
        block = provider_block("--upstream-path", "/v1/chat/completions")
        self.assertIn('responses_path = "/v1/responses"', block)

    def test_a_non_sibling_responses_route_can_be_named(self) -> None:
        # zhipu_1: chat lives under /api/coding/paas/v4 but responses on /api/v1.
        block = provider_block(
            "--upstream-path", "/api/coding/paas/v4/chat/completions",
            "--responses-path", "/api/v1/responses",
        )
        self.assertIn('responses_path = "/api/v1/responses"', block)
        self.assertNotIn('responses_path = "/api/coding/paas/v4/responses"', block)

    def test_an_opencode_upstream_gets_its_zen_path_not_a_bare_one(self) -> None:
        block = provider_block(
            "--kind", "opencode", "--upstream-path", "/zen/go/v1/chat/completions"
        )
        self.assertIn('responses_path = "/zen/go/v1/responses"', block)
        self.assertNotIn('responses_path = "/responses"', block)

    def test_chat_only_omits_native_responses_entirely(self) -> None:
        # Forcing native_responses on a chat-only upstream would POST a
        # Responses body at it; --chat-only asks the gateway to translate.
        block = provider_block("--upstream-path", "/v1/chat/completions", "--chat-only")
        self.assertNotIn("native_responses", block)
        self.assertNotIn("responses_path", block)

    def test_the_usage_documents_both_switches(self) -> None:
        source = script_text()
        self.assertIn("--responses-path", source)
        self.assertIn("--chat-only", source)

    def test_a_hardcoded_v1_responses_default_is_not_written_for_a_namespaced_chat_path(self) -> None:
        block = provider_block("--upstream-path", "/api/coding/v3/chat/completions")
        self.assertNotIn('responses_path = "/v1/responses"', block)


class PoolTableNameTest(unittest.TestCase):
    def test_the_pool_entry_table_follows_the_target_pool(self) -> None:
        source = script_text()
        self.assertIn('POOL_TABLE="auto_model.pool"', source)
        self.assertIn('POOL_TABLE="auto_models.$POOL.pool"', source)

    def test_an_auto_home_entry_does_not_land_in_the_auto_pool(self) -> None:
        # Reproduce the real TOML shape: the entry's table name decides which
        # pool it belongs to, and the wrong name put it in the auto pool.
        source = script_text()
        table = re.search(r'if \[ "\$POOL" = "auto" \]; then\n    POOL_TABLE="([^"]+)"\nelse\n    POOL_TABLE="([^"]+)"', source)
        assert table is not None, "POOL_TABLE branch not found"
        auto_table, named_table = table.group(1), table.group(2)
        named = named_table.replace("$POOL", "auto_home")
        config = f"""
[auto_model]
name = "auto"

[[{auto_table}]]
provider = "cloud"
model = "m"

[auto_models.auto_home]

[[{named}]]
provider = "mygpu"
model = "minicpmv47-awq"
"""
        parsed = tomllib.loads(config)
        self.assertEqual([e["provider"] for e in parsed["auto_model"]["pool"]], ["cloud"])
        self.assertEqual(
            [e["provider"] for e in parsed["auto_models"]["auto_home"]["pool"]],
            ["mygpu"],
        )


class RemoveTargetsTheNamedProviderTest(unittest.TestCase):
    def test_rm_targets_the_named_provider_not_the_first_marker(self) -> None:
        body = embedded_python(script_text(), "-rm-")
        self.assertNotIn('s.find("# added by add_pool_provider.sh")', body)
        self.assertIn("re.escape(\"$NAME\")", body)

    def test_rm_removes_only_the_named_block(self) -> None:
        body = embedded_python(script_text(), "-rm-")
        # Run the embedded remover against a two-account config.
        body = body.replace('p = "$TOML"', "p = %r" % "/dev/null")
        body = body.replace('raise SystemExit(4)', "raise SystemExit(4)")
        config = """# added by add_pool_provider.sh 2026-11-09T21:27:22+08:00
[providers.volc_1_spare]
kind = "openai_compat"
base_url = "https://x"

# added by add_pool_provider.sh 2026-11-09T22:00:00+08:00
[providers.other_acct]
kind = "openai_compat"
base_url = "https://y"

[providers.unrelated]
kind = "openai_compat"
"""
        name = "other_acct"
        script = body.replace('"$NAME"', repr(name)).replace('"$POOL"', "'auto'")
        # Point the script at the fixture and make it print instead of write.
        script = script.replace('p = "/dev/null"', "p = %r" % "FIXTURE")
        script = script.replace(
            'bak = p + ".bak-" + time.strftime("%Y%m%d-%H%M%S") + "-rm-" + "other_acct"',
            "bak = 'bak'",
        )
        script = script.replace("shutil.copy2(p, bak)", "pass")
        script = script.replace('open(p + ".new", "w", encoding="utf-8").write(s3)', "pass")
        script = script.replace('os.chmod(p + ".new", os.stat(p).st_mode & 0o7777)', "pass")
        script = script.replace('os.replace(p + ".new", p)', "pass")
        script = script.replace("s = open(p, encoding=\"utf-8\").read()", "s = open('FIXTURE', encoding='utf-8').read()")
        with tempfile.TemporaryDirectory() as tmp:
            fixture = str(Path(tmp) / "maxllm.toml")
            Path(fixture).write_text(config, encoding="utf-8")
            local = script.replace("FIXTURE", fixture)
            out = subprocess.run([sys.executable, "-c", local], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("REMOVED provider block x1", out.stdout)
            self.assertIn("other_acct", out.stdout)


class ConfigValidationGateTest(unittest.TestCase):
    def test_add_refuses_to_write_a_config_that_does_not_parse(self) -> None:
        body = embedded_python(script_text(), "-add-")
        self.assertIn("import tomllib", body)
        self.assertIn("REFUSING to write", body)

    def test_rm_also_refuses_to_write_a_config_that_does_not_parse(self) -> None:
        body = embedded_python(script_text(), "-rm-")
        self.assertIn("import tomllib", body)
        self.assertIn("REFUSING", body)


class ReloadIsVerifiedTest(unittest.TestCase):
    def test_add_confirms_the_gateway_actually_reloaded(self) -> None:
        source = script_text()
        self.assertIn("NOT_RELOADED", source)
        self.assertIn("RELOADED:", source)


class ShellSyntaxTest(unittest.TestCase):
    def test_the_script_parses(self) -> None:
        result = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
