#!/usr/bin/env python3
"""Deterministic workspace contention and process-progress checks."""

from __future__ import annotations

import os
import sys
import fcntl
import tempfile
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import agentd  # noqa: E402
import common  # noqa: E402


class FakeAgent:
    def __init__(self, session_id: str, pid: int, cwd: str) -> None:
        self.session_id = session_id
        self.proc = SimpleNamespace(pid=pid)
        self.cwd = cwd
        self.status = "running"
        self.active_turn = True
        self.stop_reason = None
        self.runtime_status = "running"
        self.runtime_health = {}
        self._runtime_signature = None
        self._last_protocol_activity_at = 80.0
        self._last_runtime_progress_at = 80.0
        self._runtime_alert_observed = False


def main() -> None:
    daemon = agentd.Agentd()
    holder = FakeAgent("holder", 101, "/tmp/shared-project")
    waiter = FakeAgent("waiter", 102, "/tmp/shared-project")
    snapshots = {
        101: {"signature": (((101, "S"),), 10, 20, 30), "processes": 1,
              "pids": [101], "children": 0, "cpu_ticks": 10, "read_chars": 20, "write_chars": 30},
        102: {"signature": (((102, "S"),), 4, 5, 6), "processes": 1,
              "pids": [102], "children": 0, "cpu_ticks": 4, "read_chars": 5, "write_chars": 6},
    }
    original_snapshot = agentd.process_tree_snapshot
    original_holder = agentd.lease_holder_pid
    original_blocked = agentd.WORKSPACE_BLOCKED_AFTER
    original_stalled = agentd.RUNTIME_STALLED_AFTER
    agentd.process_tree_snapshot = lambda pid: dict(snapshots[pid])
    agentd.lease_holder_pid = lambda _path: 101
    agentd.WORKSPACE_BLOCKED_AFTER = 5.0
    agentd.RUNTIME_STALLED_AFTER = 60.0
    try:
        daemon._update_runtime_health([holder, waiter], 100.0)
        assert common.agent_status(holder) == "running", holder.runtime_health
        assert common.agent_status(waiter) == "blocked", waiter.runtime_health
        assert waiter.runtime_health["workspace_lease_holder"] == "holder"

        # No protocol, CPU, I/O, child, or state change: the lock holder is
        # suspect while the queued peer remains accurately workspace-blocked.
        daemon._update_runtime_health([holder, waiter], 170.0)
        assert common.agent_status(holder) == "stalled", holder.runtime_health
        assert common.agent_status(waiter) == "blocked", waiter.runtime_health
        assert holder.runtime_health["reason"] == "no_protocol_or_process_progress"

        # Process-tree progress is sufficient proof of life even with a quiet
        # ACP pipe (long compiler/emulator commands are expected).
        snapshots[101]["signature"] = (((101, "S"), (201, "R")), 11, 20, 30)
        snapshots[101]["processes"] = 2
        snapshots[101]["pids"] = [101, 201]
        snapshots[101]["children"] = 1
        daemon._update_runtime_health([holder, waiter], 171.0)
        assert common.agent_status(holder) == "running", holder.runtime_health
        assert holder.runtime_health["child_processes"] == 1

        real = original_snapshot(os.getpid())
        assert real["processes"] >= 1 and real["cpu_ticks"] >= 0, real
        with tempfile.NamedTemporaryFile() as lease:
            fcntl.flock(lease.fileno(), fcntl.LOCK_EX)
            assert original_holder(lease.name) == os.getpid()
    finally:
        agentd.process_tree_snapshot = original_snapshot
        agentd.lease_holder_pid = original_holder
        agentd.WORKSPACE_BLOCKED_AFTER = original_blocked
        agentd.RUNTIME_STALLED_AFTER = original_stalled
    print("runtime health selftest passed")


if __name__ == "__main__":
    main()
