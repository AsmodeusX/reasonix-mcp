#!/usr/bin/env python3
"""Verify ACP decisions bridge to standard MCP elicitation."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import acp_bridge  # noqa: E402
import server  # noqa: E402
from common import shape_poll  # noqa: E402


class FakeSession:
    def __init__(self, action: str = "accept", option_id: str = "allow_once"):
        self.client_params = SimpleNamespace(
            capabilities=SimpleNamespace(elicitation=SimpleNamespace())
        )
        self.requests: list[tuple[str, dict]] = []
        self.action = action
        self.option_id = option_id

    async def elicit_form(self, message: str, schema: dict):
        self.requests.append((message, schema))
        return SimpleNamespace(action=self.action, content={"option_id": self.option_id})


class FakeAgent:
    def __init__(self, pending: bool):
        self.session_id = "agent-poll"
        self.status = "running"
        self.active_turn = True
        self.stop_reason = None
        self.plan_entries = []
        self.current_work = None
        self.full_text = ""
        self.full_thought = ""
        self._pending_permission = (7, {}) if pending else None
        self._events = [(
            acp_bridge.EV_PERMISSION,
            {
                "id": 7,
                "params": {
                    "toolCall": {"title": "Run it?"},
                    "options": [{"optionId": "allow_once", "name": "Allow"}],
                },
            },
        )]

    def poll(self):
        return self._events

    def snapshot_turns(self):
        return []


async def main() -> None:
    calls: list[tuple[str, dict]] = []

    async def fake_rpc(method: str, params: dict, timeout: float = 0) -> dict:
        calls.append((method, params))
        return {"answered": params.get("option_id")}

    session = FakeSession()
    original_session = server._mcp_session
    original_rpc = server._rpc
    server._mcp_session = session
    server._rpc = fake_rpc
    try:
        await server._elicit_agent_decision({
            "session_id": "agent-1",
            "event": "permission_request",
            "permission_request": {
                "request_id": 9,
                "tool_call": {"title": "Run a gated command?", "kind": "execute"},
                "options": [
                    {"optionId": "allow_once", "name": "Allow once"},
                    {"optionId": "reject_once", "name": "Reject"},
                ],
            },
        })
    finally:
        server._mcp_session = original_session
        server._rpc = original_rpc

    assert len(session.requests) == 1
    message, schema = session.requests[0]
    assert "Run a gated command?" in message
    assert schema["properties"]["option_id"]["enum"] == ["allow_once", "reject_once"]
    assert calls == [(
        "respond_permission",
        {"session_id": "agent-1", "option_id": "allow_once"},
    )]

    for action in ("cancel", "decline"):
        calls.clear()
        server._mcp_session = FakeSession(action=action, option_id="")
        server._rpc = fake_rpc
        try:
            await server._elicit_agent_decision({
                "session_id": f"agent-{action}",
                "permission_request": {
                    "tool_call": {"title": "Question"},
                    "options": [{"optionId": "answer", "name": "Answer"}],
                },
            })
        finally:
            server._mcp_session = original_session
            server._rpc = original_rpc
        assert calls == [], f"elicitation {action} must preserve the poll fallback"

    pending_poll = shape_poll(FakeAgent(pending=True))
    assert pending_poll["permission_request"]["request_id"] == 7
    answered_poll = shape_poll(FakeAgent(pending=False))
    assert answered_poll["permission_request"] is None
    assert answered_poll["events"][0]["type"] == "permission_request"
    print("ELICITATION SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
