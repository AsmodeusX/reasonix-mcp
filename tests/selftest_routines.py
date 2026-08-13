#!/usr/bin/env python3
"""Deterministic persistence, overlap, recovery, and scheduler checks."""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGE = os.path.join(ROOT, "src", "reasonix_mcp")
sys.path.insert(0, PACKAGE)


def main() -> None:
    scratch = tempfile.mkdtemp(prefix="rxmcp-routines-")
    scheduler = None
    try:
        os.environ["REASONIX_HOME"] = os.path.join(scratch, "home")
        os.environ["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(scratch, "agentd.sock")
        os.environ["REASONIX_MCP_ALLOW_ANY_CWD"] = "1"
        import routines
        import routined

        owner = "owner-test"
        catalog = routines.templates()
        assert len(catalog) >= 10
        assert len({item["template_id"] for item in catalog}) == len(catalog)
        assert {"github-issue-maintainer", "crash-fuzzer", "flaky-test-fixer"} <= {
            item["template_id"] for item in catalog
        }
        templated_data = routines.apply_template("crash-fuzzer", {
            "cwd": scratch, "name": "Custom crash sweep", "timezone": "Europe/Athens",
        })
        templated_data["template_id"] = "crash-fuzzer"
        templated = routines.create(owner, templated_data)
        assert templated["template_id"] == "crash-fuzzer"
        assert templated["name"] == "Custom crash sweep"
        assert templated["delegation"] == "encouraged"
        assert routines.template("crash-fuzzer")["name"] == "Crash fuzzer"
        try:
            routines.template("not-a-template")
            raise AssertionError("unknown template was accepted")
        except ValueError:
            pass

        daily = routines.create(owner, {
            "name": "Issue maintainer", "prompt": "Review current issues safely",
            "cwd": scratch, "schedule_kind": "daily", "daily_at": "09:30",
            "timezone": "UTC", "delegation": "encouraged",
        })
        assert daily["tool_approval"] == "auto"
        assert os.stat(routines.path_for(daily["routine_id"])).st_mode & 0o777 == 0o600
        next_dt = datetime.fromtimestamp(daily["next_run_at"], routines.ZoneInfo("UTC"))
        assert (next_dt.hour, next_dt.minute) == (9, 30)
        unchanged_next = daily["next_run_at"]
        daily = routines.update(daily["routine_id"], owner, {"prompt": "Updated"})
        assert daily["next_run_at"] == unchanged_next
        try:
            routines.update(daily["routine_id"], "owner-other", {"prompt": "steal"})
            raise AssertionError("cross-owner update succeeded")
        except ValueError as exc:
            assert "another orchestrator" in str(exc)

        immediate = routines.create(owner, {
            "name": "PR checker", "prompt": "Check PRs", "cwd": scratch,
            "schedule_kind": "manual", "run_immediately": True,
            "overlap_policy": "skip", "delegation": "allowed",
        })
        claims = routined.claim_due(time.time())
        claim = next(pair for pair in claims if pair[0]["routine_id"] == immediate["routine_id"])
        assert claim[1]["status"] == "starting"

        captured: list[tuple[str, str, dict]] = []

        async def fake_spawn(owner_id: str, method: str, params: dict) -> dict:
            captured.append((owner_id, method, params))
            return {"session_id": f"session-{len(captured)}", "config": {"model": params["model"]}}

        original_rpc = routined.rpc
        routined.rpc = fake_spawn
        try:
            asyncio.run(routined.launch(*claim))
        finally:
            routined.rpc = original_rpc
        launched = routines.get(immediate["routine_id"])
        run = launched["runs"][-1]
        assert run["status"] == "running" and run["session_id"] == "session-1"
        assert captured[0][0] == routined.routine_owner(immediate["routine_id"])
        assert "This is a fresh context" in captured[0][2]["task"]
        assert "task/fleet" in captured[0][2]["task"]

        skipped = routines.trigger(immediate["routine_id"], owner)
        assert skipped == {"queued": False, "skipped": True, "reason": "already_running"}
        immediate = routines.update(immediate["routine_id"], owner, {"overlap_policy": "queue"})
        queued = routines.trigger(immediate["routine_id"], owner)
        assert queued["queued"] is True

        async def fake_terminal(_owner: str, method: str, params: dict) -> dict:
            if method == "list":
                return {"sessions": [{
                    "session_id": "session-1", "status": "idle", "stop_reason": "end_turn",
                }]}
            if method == "poll":
                return {"session_id": params["session_id"], "status": "idle",
                        "stop_reason": "end_turn", "message": "Opened PR #12"}
            raise AssertionError(method)

        routined.rpc = fake_terminal
        try:
            asyncio.run(routined.monitor(routines.get(immediate["routine_id"])))
        finally:
            routined.rpc = original_rpc
        finished = routines.get(immediate["routine_id"])["runs"][-1]
        assert finished["status"] == "completed" and finished["message"] == "Opened PR #12"
        # Queued overlap starts only after the prior run reaches terminal state.
        followup = [pair for pair in routined.claim_due(time.time())
                    if pair[0]["routine_id"] == immediate["routine_id"]]
        assert len(followup) == 1 and followup[0][1]["run_id"] != run["run_id"]

        # Recover the spawn-accepted/persist-not-written crash window by the
        # unique run marker rather than duplicating the run.
        recovery_value, recovery_run = followup[0]
        marker = f"Run ID: {recovery_run['run_id']}"

        async def fake_recovery(_owner: str, method: str, _params: dict) -> dict:
            assert method == "list"
            return {"sessions": [{"session_id": "recovered-session", "status": "running",
                                   "stop_reason": None, "task": marker}]}

        routined.rpc = fake_recovery
        try:
            asyncio.run(routined.monitor(recovery_value))
        finally:
            routined.rpc = original_rpc
        recovered = routines.get(immediate["routine_id"])["runs"][-1]
        assert recovered["session_id"] == "recovered-session" and recovered["recovered"] is True
        routined.update_run(immediate["routine_id"], recovered["run_id"], {
            "status": "canceled", "finished_at": time.time(),
        })

        # Only one detached scheduler can hold the fleet lock.
        env = dict(os.environ)
        scheduler = subprocess.Popen(
            [sys.executable, routined.__file__], env=env,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(100):
            if os.path.exists(routined.STATE_PATH):
                break
            time.sleep(0.03)
        with open(routined.STATE_PATH, encoding="utf-8") as fh:
            state = json.load(fh)
        assert state["pid"] == scheduler.pid and state["source_signature"] == routined.code_signature()
        duplicate = subprocess.run(
            [sys.executable, routined.__file__], env=env, timeout=2,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        assert duplicate.returncode == 0
        print("routine self-test passed")
    finally:
        if scheduler and scheduler.poll() is None:
            scheduler.send_signal(signal.SIGTERM)
            try:
                scheduler.wait(timeout=2)
            except subprocess.TimeoutExpired:
                scheduler.kill()
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
