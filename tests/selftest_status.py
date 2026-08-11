#!/usr/bin/env python3
"""Deterministic coverage for plan/current-work state shaping."""

from __future__ import annotations

import queue
import sys
import threading

sys.path.insert(0, "src/reasonix_mcp")
import acp_bridge


def make_agent() -> acp_bridge.ReasonixAgent:
    agent = acp_bridge.ReasonixAgent.__new__(acp_bridge.ReasonixAgent)
    agent._events = queue.Queue()
    agent._qlock = threading.Lock()
    agent._tool_calls = {}
    agent._last_tool_call = None
    agent.plan_entries = []
    agent.current_work = None
    agent.full_text_parts = []
    agent.full_thought_parts = []
    agent._current_parts = []
    agent.event_seq = 0
    agent.on_event = None
    agent.on_activity = None
    agent.turns = []
    agent._turns_served = 0
    agent.stop_reason = None
    agent.last_error = None
    agent.last_error_text = ""
    agent.transcript_path = None
    agent._terminal_observed = False
    return agent


def main() -> None:
    agent = make_agent()
    agent._enqueue(acp_bridge.EV_UPDATE, {
        "sessionUpdate": "plan",
        "entries": [{"content": "Implement status reporting", "status": "in_progress"}],
    })
    assert agent.current_work["source"] == "plan"
    assert agent.current_work["summary"] == "Implement status reporting"

    agent._enqueue(acp_bridge.EV_UPDATE, {
        "sessionUpdate": "tool_call",
        "toolCallId": "tool-1",
        "title": "run focused tests",
        "kind": "execute",
        "status": "in_progress",
    })
    assert agent.current_work["source"] == "tool_call"
    assert agent.current_work["summary"] == "run focused tests"

    agent._enqueue(acp_bridge.EV_UPDATE, {
        "sessionUpdate": "tool_call_update",
        "toolCallId": "tool-1",
        "status": "completed",
    })
    assert agent.current_work["source"] == "plan"

    # Expected teardown after an observed turn must not manufacture a second
    # completion; abnormal exits remain actionable.
    agent.stop_reason = "end_turn"
    agent._terminal_observed = True
    agent._enqueue(acp_bridge.EV_PROCESS_EXIT, {"code": 0, "stderr": ""})
    assert agent._terminal_observed
    agent._enqueue(acp_bridge.EV_PROCESS_EXIT, {
        "code": 1,
        "error": {"code": 1, "message": "crashed"},
        "error_text": "crashed",
    })
    assert not agent._terminal_observed

    configurable = acp_bridge.ReasonixAgent.__new__(acp_bridge.ReasonixAgent)
    configurable.status = "idle"
    configurable.active_turn = False
    configurable.session_id = "configurable"
    configurable._config_lock = threading.Lock()
    configurable.config_options = [
        {"id": "model", "currentValue": "old/model"},
        {"id": "effort", "currentValue": "high"},
    ]
    configurable.config_values = {"model": "old/model", "effort": "high"}
    calls = []

    def request(_method: str, params: dict, timeout: float) -> dict:
        calls.append((params["configId"], params["value"], timeout))
        current = params["value"]
        if params["configId"] == "model":
            return {"configOptions": [
                {"id": "model", "currentValue": current},
                {"id": "effort", "currentValue": "auto"},
            ]}
        return {"configOptions": [
            {"id": "model", "currentValue": "new/model"},
            {"id": "effort", "currentValue": current},
        ]}

    configurable._request = request
    configured = configurable.configure({"effort": "max", "model": "new/model"})
    assert [call[:2] for call in calls] == [
        ("model", "new/model"), ("effort", "max")
    ], calls
    assert configured["config"] == {"model": "new/model", "effort": "max"}
    print("STATUS SELFTEST PASS")


if __name__ == "__main__":
    main()
