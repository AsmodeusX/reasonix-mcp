#!/usr/bin/env python3
"""Deterministic watch concurrency and cancellation checks (no model)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

from agentd import Agentd  # noqa: E402
import common  # noqa: E402
import server as mcp_server  # noqa: E402


class RecordingWriter:
    def __init__(self) -> None:
        self.frames: list[dict] = []
        self.changed = asyncio.Event()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.frames.extend(json.loads(line) for line in data.decode().splitlines())
        self.changed.set()

    async def drain(self) -> None:
        pass

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True

    async def wait_for_responses(self, *request_ids: int) -> None:
        expected = set(request_ids)
        while not expected.issubset({frame.get("id") for frame in self.frames}):
            self.changed.clear()
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class BlockingDrainWriter(RecordingWriter):
    async def drain(self) -> None:
        await asyncio.Event().wait()


async def check_server_cancellation() -> None:
    original_ensure = mcp_server._ensure_agentd
    original_writer = mcp_server._agentd_writer
    original_cancel_supported = mcp_server._agentd_cancel_supported
    try:
        for writer in (RecordingWriter(), BlockingDrainWriter()):
            async def connected(writer: RecordingWriter = writer) -> None:
                mcp_server._agentd_writer = writer

            mcp_server._ensure_agentd = connected
            mcp_server._agentd_cancel_supported = True
            call = asyncio.create_task(
                mcp_server._rpc("watch", {"session_ids": ["cancel-me"]}, timeout=None)
            )
            await asyncio.sleep(0)
            call.cancel()
            await asyncio.gather(call, return_exceptions=True)

            assert writer.frames[0]["method"] == "watch", writer.frames
            assert writer.frames[1]["method"] == "cancel_request", writer.frames
            assert writer.frames[1]["params"]["request_id"] == writer.frames[0]["id"], writer.frames
            assert writer.frames[0]["id"] not in mcp_server._pending, mcp_server._pending

        legacy_writer = RecordingWriter()
        mcp_server._agentd_cancel_supported = False
        mcp_server._cancel_agentd_request(legacy_writer, 99)
        assert legacy_writer.frames[0]["method"] == "cancel_request", legacy_writer.frames
        assert legacy_writer.closed, "legacy daemon fallback must close the client connection"
    finally:
        mcp_server._ensure_agentd = original_ensure
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_cancel_supported


async def check_agentd_wire_cancellation(agent: SimpleNamespace) -> None:
    daemon = Agentd()
    daemon._registry[agent.session_id] = agent
    reader = asyncio.StreamReader()
    writer = RecordingWriter()
    connection = asyncio.create_task(daemon.client_loop(reader, writer))

    def send(frame: dict) -> None:
        reader.feed_data((json.dumps(frame) + "\n").encode())

    watch_params = {
        "session_ids": [agent.session_id],
        "owner_id": agent.owner_id,
        "timeout": 10,
    }
    send({"jsonrpc": "2.0", "id": 1, "method": "watch", "params": watch_params})
    await asyncio.sleep(0)
    send({
        "jsonrpc": "2.0", "id": 2, "method": "watch",
        "params": {**watch_params, "timeout": 0},
    })
    await writer.wait_for_responses(1, 2)
    responses = {frame["id"]: frame for frame in writer.frames if "id" in frame}
    assert responses[1]["result"]["superseded"] is True, responses
    assert responses[2]["result"]["timed_out"] is True, responses

    send({"jsonrpc": "2.0", "id": 3, "method": "watch", "params": watch_params})
    await asyncio.sleep(0)
    send({
        "jsonrpc": "2.0", "method": "cancel_request",
        "params": {"request_id": 3, "owner_id": agent.owner_id},
    })
    await writer.wait_for_responses(3)
    response = next(frame for frame in writer.frames if frame.get("id") == 3)
    assert response["error"]["code"] == -32800, response

    reader.feed_eof()
    await connection
    assert not daemon._watch_waiters, daemon._watch_waiters
    assert not daemon._active_watch_sessions, daemon._active_watch_sessions


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
    replacement = await daemon.watch({
        "session_ids": [agent.session_id],
        "owner_id": agent.owner_id,
        "timeout": 0,
    })
    assert replacement == {"woke": [], "timed_out": True, "results": {}}, replacement
    displaced = await asyncio.gather(first, return_exceptions=True)
    assert isinstance(displaced[0], asyncio.CancelledError), displaced

    assert not daemon._watch_waiters, daemon._watch_waiters
    assert not daemon._active_watch_sessions, daemon._active_watch_sessions
    await check_server_cancellation()
    await check_agentd_wire_cancellation(agent)

    long_text = "A" * (max(0, common.MAX_WATCH_MESSAGE) + 100)
    compact = daemon._compact_watch_result({
        "session_id": "compact",
        "status": "idle",
        "stop_reason": "end_turn",
        "text": long_text,
        "turns": [{"text": long_text, "stop_reason": "end_turn"}],
        "permission_request": None,
        "plan": [],
        "current_work": None,
        "transcript_path": "/tmp/compact.jsonl",
    })
    assert compact["message_truncated"] is True
    assert len(compact["message"]) <= max(0, common.MAX_WATCH_MESSAGE)
    assert not ({"events", "turns", "text", "thought", "full_text"} & compact.keys())
    print("WATCH CONCURRENCY SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
