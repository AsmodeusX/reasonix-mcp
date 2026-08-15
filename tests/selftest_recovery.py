#!/usr/bin/env python3
"""Regression checks for dead-child detection and durable fleet recovery."""

from __future__ import annotations

import asyncio
import itertools
import json
import os
import queue
import sys
import tempfile
import threading
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import acp_bridge  # noqa: E402
import agentd  # noqa: E402
import common  # noqa: E402


class DeadProcess:
    pid = 424242

    @staticmethod
    def poll() -> int:
        return 17


def dead_agent() -> acp_bridge.ReasonixAgent:
    value = object.__new__(acp_bridge.ReasonixAgent)
    value.proc = DeadProcess()
    value._pgid = DeadProcess.pid
    value.status = "running"
    value.active_turn = True
    value._orphaned = False
    value._exit_code = None
    value._exit_lock = threading.Lock()
    value._exit_reported = False
    value._closed = False
    value._stderr_tail = []
    value.stop_reason = None
    value.current_work = {"summary": "stale work"}
    value._events = queue.Queue()
    value._qlock = threading.Lock()
    value._pending = {}
    value._terminal_observed = True
    value.last_error = None
    value.last_error_text = ""
    value.event_seq = 0
    value.on_activity = None
    value.on_event = None
    value._kill_orphaned_process_group = lambda: None
    return value


def test_dead_child() -> None:
    value = dead_agent()
    waiter = queue.Queue(maxsize=1)
    value._pending[1] = waiter
    assert common.agent_status(value) == "orphaned"
    assert value.status == "exited" and not value.active_turn
    assert value.current_work is None and value.stop_reason == "error"
    assert value.poll()[0][0] == acp_bridge.EV_PROCESS_EXIT
    assert waiter.get_nowait()["error"]["message"] == "agent process exited"


def test_fast_response_registration() -> None:
    value = object.__new__(acp_bridge.ReasonixAgent)
    value._ids = itertools.count(1)
    value._pending = {}
    value._wlock = threading.Lock()

    class ImmediateStdin:
        closed = False

        def write(self, _frame: str) -> None:
            assert 1 in value._pending, "response waiter was registered after write"

        def flush(self) -> None:
            value._pending[1].put({"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})

    value.proc = SimpleNamespace(stdin=ImmediateStdin())
    assert value._request("probe", {}, timeout=0.1) == {"ok": True}


def test_durable_discovery() -> None:
    old_home = os.environ.get("REASONIX_HOME")
    with tempfile.TemporaryDirectory(prefix="reasonix-recovery-") as home:
        os.environ["REASONIX_HOME"] = home
        try:
            owner = "owner-recovery-test"
            sid = "dead-session"
            session_dir = os.path.join(home, "sessions")
            os.makedirs(session_dir)
            with open(os.path.join(session_dir, f"{sid}.jsonl"), "w", encoding="utf-8") as fh:
                fh.write("{}\n")
            with open(common.session_acp_path(sid), "w", encoding="utf-8") as fh:
                json.dump({"cwd": "/tmp/project", "title": "Recover me", "model": "test/model"}, fh)
            common.save_owner_sessions(owner, {sid})
            common.write_session_owner(sid, owner)
            common.write_session_lifecycle(sid, {
                "state": "running", "owner_id": owner,
                "cwd": "/tmp/project", "task": "original task",
            })
            listed = agentd.Agentd().list({"owner_id": owner, "pending_only": True})
            assert listed["sessions"] == [{
                "session_id": sid,
                "status": "orphaned",
                "cwd": "/tmp/project",
                "task": "original task",
                "stop_reason": "agentd_lost_process",
                "transcript_path": os.path.join(session_dir, f"{sid}.jsonl"),
                "permission_request": False,
                "plan": [],
                "current_work": None,
                "config": {"model": "test/model"},
                "pending_config": {},
                "pending_config_error": None,
                "process_alive": False,
                "resumable": True,
                "recovered_from_disk": True,
                "error_text": "agent process is not owned by the current daemon; resume it from the persisted transcript",
            }]
        finally:
            if old_home is None:
                os.environ.pop("REASONIX_HOME", None)
            else:
                os.environ["REASONIX_HOME"] = old_home


async def test_resume_fleet() -> None:
    daemon = agentd.Agentd()
    continued = []

    def resume(params: dict) -> dict:
        sid = params["session_id"]
        if sid == "bad":
            raise agentd.AgentdError("broken transcript")
        return {"session_id": sid, "status": "idle", "resumed": True}

    daemon.resume = resume
    daemon._get = lambda sid, _owner: SimpleNamespace(session_id=sid)
    daemon._start_agent_turn = lambda value, message: continued.append((value.session_id, message))
    original_lifecycle = common.read_session_lifecycle
    common.read_session_lifecycle = lambda sid: {
        "state": "running" if sid == "one" else "idle"
    }
    try:
        result = await daemon.resume_fleet({
            "session_ids": ["one", "bad", "one", "two"], "parallelism": 2,
        })
    finally:
        common.read_session_lifecycle = original_lifecycle
    assert result["requested"] == 3
    assert result["resumed"] == ["one", "two"]
    assert result["failed"] == ["bad"]
    assert result["results"]["one"]["status"] == "running"
    assert result["results"]["one"]["continued_interrupted_turn"] is True
    assert [sid for sid, _ in continued] == ["one"]


def main() -> None:
    test_dead_child()
    test_fast_response_registration()
    test_durable_discovery()
    asyncio.run(test_resume_fleet())
    print("RECOVERY SELFTEST PASS")


if __name__ == "__main__":
    main()
