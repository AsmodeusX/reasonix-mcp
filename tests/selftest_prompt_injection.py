#!/usr/bin/env python3
"""Verify the orchestrator status protocol is injected once per session."""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import agentd  # noqa: E402
import common  # noqa: E402


class FakeAgent:
    def __init__(self):
        self.prompts: list[str] = []

    def start_turn(self, text: str) -> None:
        self.prompts.append(text)


def main() -> None:
    daemon = agentd.Agentd()

    fresh = FakeAgent()
    daemon._start_agent_turn(fresh, "Initial task")
    daemon._start_agent_turn(fresh, "Follow-up task")
    assert fresh.prompts[0].count(common.STATUS_PROMPT) == 1
    assert common.STATUS_PROMPT not in fresh.prompts[1]
    assert sum(prompt.count(common.STATUS_PROMPT) for prompt in fresh.prompts) == 1

    # A user can mention the marker without suppressing the actual contract.
    marker_only = FakeAgent()
    daemon._start_agent_turn(marker_only, f"Discuss {common.STATUS_PROMPT_MARKER}")
    assert marker_only.prompts[0].count(common.STATUS_PROMPT) == 1

    scratch = tempfile.mkdtemp(prefix="rxmcp-prompt-state-")
    old_home = os.environ.get("REASONIX_HOME")
    os.environ["REASONIX_HOME"] = scratch
    try:
        session_dir = os.path.join(scratch, "sessions")
        os.makedirs(session_dir)
        session_id = "resumed-with-contract"
        with open(os.path.join(session_dir, f"{session_id}.jsonl"), "w", encoding="utf-8") as transcript:
            transcript.write(json.dumps({
                "role": "user",
                "content": f"Original task\n\n{common.STATUS_PROMPT}",
            }) + "\n")
        assert common.transcript_has_status_protocol(session_id)

        resumed = FakeAgent()
        resumed.status_protocol_injected = common.transcript_has_status_protocol(session_id)
        daemon._start_agent_turn(resumed, "Continue after resume")
        assert resumed.prompts == ["Continue after resume"]

        legacy_id = "legacy-with-marker-only"
        with open(os.path.join(session_dir, f"{legacy_id}.jsonl"), "w", encoding="utf-8") as transcript:
            transcript.write(json.dumps({"role": "user", "content": common.STATUS_PROMPT_MARKER}) + "\n")
        assert not common.transcript_has_status_protocol(legacy_id)
    finally:
        if old_home is None:
            os.environ.pop("REASONIX_HOME", None)
        else:
            os.environ["REASONIX_HOME"] = old_home
        shutil.rmtree(scratch, ignore_errors=True)

    print("PROMPT-INJECTION SELFTEST PASS")


if __name__ == "__main__":
    main()
