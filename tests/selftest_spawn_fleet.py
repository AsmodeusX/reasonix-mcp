#!/usr/bin/env python3
"""Assigned-fleet JSON validation and compact spawning checks."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import server  # noqa: E402


def main() -> None:
    scratch = tempfile.mkdtemp(prefix="rxmcp-fleet-")
    old_home = os.environ.get("REASONIX_HOME")
    original_rpc = server._rpc
    original_owner = server._owner_id
    original_owned = server._owned_sessions
    calls: list[dict] = []
    try:
        os.environ["REASONIX_HOME"] = os.path.join(scratch, "home")
        source = os.path.join(scratch, "fleet.json")
        with open(source, "w", encoding="utf-8") as fh:
            json.dump({
                "name": "acceptance fleet",
                "parallelism": 2,
                "defaults": {
                    "cwd": ROOT,
                    "model": "provider/default",
                    "effort": "high",
                    "tool_approval": "ask",
                },
                "agents": {
                    "compile": "Repair compile failures",
                    "traps": {
                        "task": "Analyze trap mismatches",
                        "model": "provider/special",
                        "effort": "max",
                        "keep_alive": True,
                    },
                    "runtime-failure": "Fail during spawn",
                },
            }, fh)

        loaded = server.load_fleet_file(source)
        assert loaded["name"] == "acceptance fleet"
        assert [item["name"] for item in loaded["agents"]] == [
            "compile", "traps", "runtime-failure",
        ]
        assert loaded["agents"][1]["model"] == "provider/special"
        assert loaded["agents"][0]["effort"] == "high"
        assert loaded["agents"][1]["effort"] == "max"
        assert loaded["agents"][1]["tool_approval"] == "ask"

        async def fake_rpc(method: str, params: dict, **_kwargs) -> dict:
            assert method == "spawn"
            calls.append(dict(params))
            if params["task"] == "Fail during spawn":
                raise ValueError("provider unavailable")
            index = len([call for call in calls if call["task"] != "Fail during spawn"])
            return {
                "session_id": f"session-{index}",
                "status": "running",
                "cwd": params["cwd"],
                "config": {
                    key: params[key]
                    for key in ("model", "effort", "work_mode", "tool_approval")
                },
            }

        server._rpc = fake_rpc
        server._owner_id = "fleet-owner"
        server._owned_sessions = set()
        result = asyncio.run(server.reasonix_spawn_fleet(source))
        assert result["requested"] == 3
        assert result["spawned"] == 2
        assert result["failed"] == 1
        assert result["session_ids"] == ["session-1", "session-2"]
        assert result["agents"][2]["error"] == "provider unavailable"
        assert server._owned_sessions == {"session-1", "session-2"}
        by_task = {call["task"]: call for call in calls}
        assert by_task["Repair compile failures"]["model"] == "provider/default"
        assert by_task["Repair compile failures"]["effort"] == "high"
        assert by_task["Analyze trap mismatches"]["model"] == "provider/special"
        assert by_task["Analyze trap mismatches"]["effort"] == "max"

        invalid = os.path.join(scratch, "invalid.json")
        with open(invalid, "w", encoding="utf-8") as fh:
            json.dump({
                "defaults": {"cwd": ROOT},
                "agents": [
                    {"name": "same", "task": "One"},
                    {"name": "same", "task": "Two"},
                ],
            }, fh)
        before = len(calls)
        try:
            asyncio.run(server.reasonix_spawn_fleet(invalid))
            raise AssertionError("duplicate names were accepted")
        except ValueError as exc:
            assert "duplicate agent name" in str(exc)
        assert len(calls) == before, "validation failure spawned a partial fleet"
        print("SPAWN FLEET SELFTEST PASS")
    finally:
        server._rpc = original_rpc
        server._owner_id = original_owner
        server._owned_sessions = original_owned
        if old_home is None:
            os.environ.pop("REASONIX_HOME", None)
        else:
            os.environ["REASONIX_HOME"] = old_home
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
