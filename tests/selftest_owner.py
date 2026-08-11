#!/usr/bin/env python3
"""Deterministic same-project orchestrator ownership checks."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import common  # noqa: E402


def main() -> None:
    scratch = tempfile.mkdtemp(prefix="rxmcp-owner-")
    previous_home = os.environ.get("REASONIX_HOME")
    previous_thread = os.environ.get("CODEX_THREAD_ID")
    previous_explicit = os.environ.get("REASONIX_MCP_ORCHESTRATOR_ID")
    old_cwd = os.getcwd()
    try:
        os.environ["REASONIX_HOME"] = os.path.join(scratch, "home")
        os.environ.pop("REASONIX_MCP_ORCHESTRATOR_ID", None)
        os.chdir(scratch)

        os.environ["CODEX_THREAD_ID"] = "thread-one"
        first = common.orchestrator_owner_id("codex")
        assert common.orchestrator_owner_id("codex") == first

        os.environ["CODEX_THREAD_ID"] = "thread-two"
        second = common.orchestrator_owner_id("codex")
        assert second != first, (first, second)

        os.environ["CODEX_THREAD_ID"] = "thread-one"
        assert common.orchestrator_owner_id("codex") == first

        os.environ["REASONIX_MCP_ORCHESTRATOR_ID"] = "explicit-owner"
        explicit = common.orchestrator_owner_id("codex")
        os.environ["CODEX_THREAD_ID"] = "thread-three"
        assert common.orchestrator_owner_id("codex") == explicit
        print("OWNER ISOLATION SELFTEST PASS")
    finally:
        os.chdir(old_cwd)
        for key, value in (
            ("REASONIX_HOME", previous_home),
            ("CODEX_THREAD_ID", previous_thread),
            ("REASONIX_MCP_ORCHESTRATOR_ID", previous_explicit),
        ):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
