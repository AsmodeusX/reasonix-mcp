#!/usr/bin/env python3
"""Deterministic recovery checks for failed native background bash jobs."""

from __future__ import annotations

import asyncio
import os
import queue
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import acp_bridge  # noqa: E402
import common  # noqa: E402
from agentd import (  # noqa: E402
    Agentd,
    BACKGROUND_RECOVERY_MAX_ATTEMPTS,
)


FAILURE_TEXT = "[warning] background bash failed: needs attention"


def fake_agent(session_id: str, *, start_error: Exception | None = None):
    prompts: list[str] = []

    def start_turn(message: str) -> None:
        if start_error is not None:
            raise start_error
        prompts.append(message)
        agent.active_turn = True
        agent.status = "running"
        agent.stop_reason = None
        agent.last_error = None
        agent.last_error_text = ""

    agent = SimpleNamespace(
        session_id=session_id,
        owner_id="owner-recovery",
        cwd="/tmp",
        task="test task",
        keep_alive=True,
        idle_timeout=-1,
        status="idle",
        active_turn=False,
        stop_reason="error",
        last_error={"message": FAILURE_TEXT},
        last_error_text=FAILURE_TEXT,
        turns=[{
            "text": FAILURE_TEXT,
            "stop_reason": "error",
            "error_text": FAILURE_TEXT,
        }],
        pending_config={},
        current_work=None,
        runtime_status="idle",
        runtime_health={},
        status_protocol_injected=True,
        event_seq=1,
        _orphaned=False,
        _terminal_observed=False,
        _pending_permission=None,
        on_event=None,
        on_activity=None,
        start_turn=start_turn,
    )
    return agent, prompts


async def registered_daemon(agent):
    daemon = Agentd()
    daemon._loop = asyncio.get_running_loop()
    daemon._persist_lifecycle = lambda *_args, **_kwargs: None
    emitted: list[dict] = []

    async def broadcast(event: dict, _owner_id: str) -> None:
        emitted.append(event)

    daemon._broadcast = broadcast
    daemon._register(agent)
    return daemon, emitted


async def check_recovery_continues_without_terminal_wake() -> None:
    agent, prompts = fake_agent("background-recovery")
    daemon, emitted = await registered_daemon(agent)
    waiter = asyncio.get_running_loop().create_future()
    daemon._add_watch_waiter(waiter, [agent.session_id])
    payload = {"stopReason": "error", "error": {"message": FAILURE_TEXT}}

    agent.on_event(acp_bridge.EV_TURN_END, payload)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert prompts and "do not poll or wait on that job id again" in prompts[0]
    assert agent.active_turn and common.agent_status(agent) == "running"
    assert not waiter.done(), "recoverable boundary incorrectly woke terminal watch"
    assert payload["recovery_pending"] is True
    assert agent.turns[-1]["auto_recovered"] is True
    assert emitted and emitted[0]["event"] == "background_failure_recovery"


async def check_recovery_start_failure_falls_back_to_terminal() -> None:
    agent, _prompts = fake_agent(
        "background-recovery-start-failure", start_error=RuntimeError("write failed")
    )
    daemon, emitted = await registered_daemon(agent)
    waiter = asyncio.get_running_loop().create_future()
    daemon._add_watch_waiter(waiter, [agent.session_id])
    payload = {"stopReason": "error", "error": {"message": FAILURE_TEXT}}

    agent.on_event(acp_bridge.EV_TURN_END, payload)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    await asyncio.wait_for(asyncio.shield(waiter), timeout=1)
    assert payload["recovery_pending"] is False
    assert "write failed" in payload["recovery_start_error"]
    assert any(event["event"] == acp_bridge.EV_TURN_END for event in emitted), emitted


def check_recovery_is_bounded() -> None:
    agent, _prompts = fake_agent("background-recovery-cap")
    daemon = Agentd()
    for attempt in range(1, BACKGROUND_RECOVERY_MAX_ATTEMPTS + 1):
        payload = {"stopReason": "error", "error": {"message": FAILURE_TEXT}}
        assert daemon._prepare_background_recovery(agent, payload)
        assert payload["recovery_attempt"] == attempt
    payload = {"stopReason": "error", "error": {"message": FAILURE_TEXT}}
    assert not daemon._prepare_background_recovery(agent, payload)


def check_poll_keeps_live_recovery_nonterminal() -> None:
    agent, _prompts = fake_agent("background-recovery-poll")
    payload = {
        "stopReason": "error",
        "error": {"message": FAILURE_TEXT},
        "recovery_pending": True,
        "recovery_attempt": 1,
        "recovery_max_attempts": 3,
    }
    events: queue.Queue = queue.Queue()
    events.put((acp_bridge.EV_TURN_END, payload))
    agent.poll = lambda: [events.get_nowait()]
    agent.snapshot_turns = lambda: [dict(agent.turns[-1])]
    agent.full_text = FAILURE_TEXT
    agent.full_thought = ""
    agent._background_recovery_pending = True

    result = common.shape_poll(agent)
    assert result["status"] == "running", result
    assert "stop_reason" not in result and "error" not in result, result
    assert result["auto_recovery"]["reason"] == "background_job_failed"


async def main() -> None:
    await check_recovery_continues_without_terminal_wake()
    await check_recovery_start_failure_falls_back_to_terminal()
    check_recovery_is_bounded()
    check_poll_keeps_live_recovery_nonterminal()
    print("BACKGROUND RECOVERY SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
