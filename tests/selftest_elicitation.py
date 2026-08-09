#!/usr/bin/env python3
"""Verify ACP decisions bridge to standard MCP elicitation."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import server  # noqa: E402


class FakeSession:
    def __init__(self):
        self.client_params = SimpleNamespace(
            capabilities=SimpleNamespace(elicitation=SimpleNamespace())
        )
        self.requests: list[tuple[str, dict]] = []

    async def elicit_form(self, message: str, schema: dict):
        self.requests.append((message, schema))
        return SimpleNamespace(action="accept", content={"option_id": "allow_once"})


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
    print("ELICITATION SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
