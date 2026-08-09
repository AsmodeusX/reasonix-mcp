#!/usr/bin/env python3
"""Deterministic watch concurrency and cancellation checks (no model)."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

from agentd import Agentd, AgentdError  # noqa: E402


async def main() -> None:
    daemon = Agentd()
    agent = SimpleNamespace(
        session_id="agent-watch",
        owner_id="owner-watch",
        status="running",
        active_turn=True,
        stop_reason=None,
        _pending_permission=None,
        _terminal_observed=False,
    )
    daemon._registry[agent.session_id] = agent

    first = asyncio.create_task(daemon.watch({
        "session_ids": [agent.session_id],
        "owner_id": agent.owner_id,
        "timeout": 10,
    }))
    await asyncio.sleep(0)  # let the first coroutine register its in-flight watch
    try:
        try:
            await daemon.watch({
                "session_ids": [agent.session_id],
                "owner_id": agent.owner_id,
                "timeout": 0,
            })
            raise AssertionError("overlapping watch should be rejected")
        except AgentdError as exc:
            assert "already have an in-flight watch" in str(exc), exc
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)

    assert not daemon._watch_waiters, daemon._watch_waiters
    assert not daemon._active_watch_sessions, daemon._active_watch_sessions
    print("WATCH CONCURRENCY SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
