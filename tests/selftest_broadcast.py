#!/usr/bin/env python3
"""Selected-agent broadcast validation and partial-delivery checks."""

from __future__ import annotations

import asyncio
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import server  # noqa: E402


async def exercise() -> None:
    original_send = server.reasonix_send
    original_owned = server._owned_sessions
    calls: list[tuple[str, str, str]] = []

    async def fake_send(
        session_id: str, message: str, expect: str = "any", ctx=None
    ) -> dict:
        calls.append((session_id, message, expect))
        if session_id == "broken":
            raise ValueError("agent is orphaned")
        return {
            "session_id": session_id,
            "delivered": "steered" if session_id == "active" else "new_turn",
        }

    try:
        server.reasonix_send = fake_send
        server._owned_sessions = {"active", "idle", "broken"}
        result = await server.reasonix_broadcast(
            ["active", "idle", "active", "broken"],
            "Use the corrected interface.",
            expect="any",
            parallelism=2,
        )
        assert result["requested"] == 3
        assert result["delivered"] == ["active", "idle"]
        assert result["failed"] == ["broken"]
        assert result["delivery_modes"] == {"steered": 1, "new_turn": 1}
        assert result["results"]["broken"]["error"] == "agent is orphaned"
        assert [session_id for session_id, _, _ in calls] == [
            "active", "idle", "broken",
        ]

        before = len(calls)
        try:
            await server.reasonix_broadcast(
                ["active", "foreign"], "Must not partially send"
            )
            raise AssertionError("foreign session selection was accepted")
        except ValueError as exc:
            assert "not owned" in str(exc)
        assert len(calls) == before, "ownership failure caused a partial broadcast"

        for invalid in ([], [""], [f"session-{index}" for index in range(65)]):
            try:
                await server.reasonix_broadcast(invalid, "message")
                raise AssertionError(f"invalid selection was accepted: {invalid!r}")
            except ValueError:
                pass
        print("BROADCAST SELFTEST PASS")
    finally:
        server.reasonix_send = original_send
        server._owned_sessions = original_owned


if __name__ == "__main__":
    asyncio.run(exercise())
