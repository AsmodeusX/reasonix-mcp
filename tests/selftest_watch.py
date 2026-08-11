#!/usr/bin/env python3
"""Deterministic watch concurrency and cancellation checks (no model)."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

from agentd import Agentd  # noqa: E402
import acp_bridge  # noqa: E402
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


class EOFReader:
    async def readline(self) -> bytes:
        return b""


class ResetReader:
    async def readline(self) -> bytes:
        raise ConnectionResetError("test reset")


class RetryWriter(RecordingWriter):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self.tokens: list[str] = []

    def write(self, data: bytes) -> None:
        super().write(data)
        frame = self.frames[-1]
        if frame.get("method") != "poll" or "id" not in frame:
            return
        self.calls += 1
        self.tokens.append(str((frame.get("params") or {}).get("_request_token")))
        rid = frame["id"]

        def respond() -> None:
            future = mcp_server._pending.pop(rid)
            mcp_server._pending_writers.pop(rid, None)
            if self.calls == 1:
                future.set_result({
                    "error": {"code": -32001, "message": "connection replaced"}
                })
            else:
                future.set_result({"result": {"status": "running", "retried": True}})

        asyncio.get_running_loop().call_soon(respond)


async def check_connection_probe_serialization() -> None:
    original_open = mcp_server.asyncio.open_unix_connection
    original_rpc = mcp_server._rpc
    original_read = mcp_server._read_agentd
    original_reader = mcp_server._agentd_reader
    original_writer = mcp_server._agentd_writer
    original_supported = mcp_server._agentd_cancel_supported
    original_lock = mcp_server._agentd_connect_lock
    original_probe = mcp_server._agentd_probe_task
    probe_started = asyncio.Event()
    release_probe = asyncio.Event()
    writer = RecordingWriter()
    opens = 0

    async def fake_open(_sock: str):
        nonlocal opens
        opens += 1
        return EOFReader(), writer

    async def fake_rpc(method: str, _params: dict, **_kwargs) -> dict:
        assert method == "ping", method
        probe_started.set()
        await release_probe.wait()
        return {"cancel_request": True}

    async def fake_read(_reader, _writer) -> None:
        return

    mcp_server.asyncio.open_unix_connection = fake_open
    mcp_server._rpc = fake_rpc
    mcp_server._read_agentd = fake_read
    mcp_server._agentd_reader = None
    mcp_server._agentd_writer = None
    mcp_server._agentd_cancel_supported = None
    mcp_server._agentd_connect_lock = None
    mcp_server._agentd_probe_task = None
    try:
        first = asyncio.create_task(mcp_server._ensure_agentd())
        await probe_started.wait()
        second = asyncio.create_task(mcp_server._ensure_agentd())
        await asyncio.sleep(0)
        assert not second.done(), "concurrent RPC escaped before capability probe"
        release_probe.set()
        await asyncio.gather(first, second)
        assert opens == 1, opens
        assert mcp_server._agentd_cancel_supported is True
    finally:
        mcp_server.asyncio.open_unix_connection = original_open
        mcp_server._rpc = original_rpc
        mcp_server._read_agentd = original_read
        mcp_server._agentd_reader = original_reader
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_supported
        mcp_server._agentd_connect_lock = original_lock
        mcp_server._agentd_probe_task = original_probe


async def check_old_reader_isolation() -> None:
    original_reader = mcp_server._agentd_reader
    original_writer = mcp_server._agentd_writer
    original_supported = mcp_server._agentd_cancel_supported
    old_writer = RecordingWriter()
    new_writer = RecordingWriter()
    new_reader = object()
    old_future = asyncio.get_running_loop().create_future()
    new_future = asyncio.get_running_loop().create_future()
    mcp_server._agentd_reader = new_reader
    mcp_server._agentd_writer = new_writer
    mcp_server._agentd_cancel_supported = True
    mcp_server._pending[9001] = old_future
    mcp_server._pending_writers[9001] = old_writer
    mcp_server._pending[9002] = new_future
    mcp_server._pending_writers[9002] = new_writer
    try:
        await mcp_server._read_agentd(EOFReader(), old_writer)
        assert (await old_future)["error"]["code"] == -32001
        assert not new_future.done(), "old reader failed a replacement connection's RPC"
        assert mcp_server._pending.get(9002) is new_future
    finally:
        mcp_server._pending.pop(9001, None)
        mcp_server._pending_writers.pop(9001, None)
        mcp_server._pending.pop(9002, None)
        mcp_server._pending_writers.pop(9002, None)
        new_future.cancel()
        mcp_server._agentd_reader = original_reader
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_supported


async def check_read_retry() -> None:
    original_ensure = mcp_server._ensure_agentd
    original_writer = mcp_server._agentd_writer
    original_supported = mcp_server._agentd_cancel_supported
    writer = RetryWriter()

    async def connected() -> None:
        mcp_server._agentd_writer = writer

    mcp_server._ensure_agentd = connected
    mcp_server._agentd_cancel_supported = True
    try:
        result = await mcp_server._rpc("poll", {"session_id": "retry-me"})
        assert result == {"status": "running", "retried": True}, result
        assert writer.calls == 2, writer.calls
        assert writer.tokens[0] and writer.tokens[0] == writer.tokens[1], writer.tokens
    finally:
        mcp_server._ensure_agentd = original_ensure
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_supported


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

    reset_writer = RecordingWriter()
    await daemon.client_loop(ResetReader(), reset_writer)
    assert reset_writer.closed


def watch_agent(session_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        session_id=session_id,
        owner_id="owner-watch",
        status="running",
        active_turn=True,
        stop_reason=None,
        _pending_permission=None,
        _terminal_observed=False,
        turns=[],
        event_seq=0,
        _events=queue.Queue(),
        plan_entries=[],
        current_work=None,
        last_error=None,
    )


async def check_targeted_watch_wakeups() -> None:
    daemon = Agentd()
    first_agent = watch_agent("targeted-first")
    second_agent = watch_agent("targeted-second")
    daemon._registry = {
        first_agent.session_id: first_agent,
        second_agent.session_id: second_agent,
    }

    def fake_poll(params: dict) -> dict:
        agent = daemon._registry[params["session_id"]]
        if params.get("_watch_delivery"):
            agent._terminal_observed = True
        return {
            "session_id": agent.session_id,
            "status": common.agent_status(agent),
            "stop_reason": agent.stop_reason,
            "turns": [],
            "permission_request": None,
            "plan": [],
            "current_work": None,
            "transcript_path": None,
        }

    daemon.poll = fake_poll
    first_params = {
        "session_ids": [first_agent.session_id],
        "owner_id": first_agent.owner_id,
        "_request_token": "targeted-first-delivery",
    }
    first = asyncio.create_task(daemon.watch(first_params))
    second = asyncio.create_task(daemon.watch({
        "session_ids": [second_agent.session_id],
        "owner_id": second_agent.owner_id,
    }))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert set(daemon._watch_waiters) == {
        first_agent.session_id, second_agent.session_id,
    }, daemon._watch_waiters

    # Progress is snapshot state, not an actionable completion/decision.
    daemon._wake_watchers(first_agent.session_id, acp_bridge.EV_STATUS)
    await asyncio.sleep(0)
    assert not first.done() and not second.done()

    first_agent.active_turn = False
    first_agent.status = "idle"
    first_agent.stop_reason = "end_turn"
    first_agent.turns = [{"text": "FIRST", "stop_reason": "end_turn"}]
    daemon._wake_watchers(first_agent.session_id, acp_bridge.EV_TURN_END)
    first_result = await asyncio.wait_for(first, timeout=1)
    assert first_result["results"][first_agent.session_id]["message"] == "FIRST"
    assert not second.done(), "an unrelated agent woke another fleet watch"
    replayed = await daemon.watch(first_params)
    assert replayed == first_result, (replayed, first_result)

    second_agent.active_turn = False
    second_agent.status = "idle"
    second_agent.stop_reason = "end_turn"
    second_agent.turns = [{"text": "SECOND", "stop_reason": "end_turn"}]
    daemon._wake_watchers(second_agent.session_id, acp_bridge.EV_TURN_END)
    second_result = await asyncio.wait_for(second, timeout=1)
    assert second_result["results"][second_agent.session_id]["message"] == "SECOND"
    assert not daemon._watch_waiters, daemon._watch_waiters


async def check_poll_does_not_steal_watch_terminal() -> None:
    daemon = Agentd()
    agent = watch_agent("poll-race")
    agent.active_turn = False
    agent.status = "idle"
    agent.stop_reason = "end_turn"
    daemon._registry[agent.session_id] = agent
    original_shape_poll = common.shape_poll

    def terminal_poll(_agent, **_kwargs) -> dict:
        return {
            "session_id": agent.session_id,
            "status": "idle",
            "stop_reason": "end_turn",
        }

    common.shape_poll = terminal_poll
    daemon._active_watch_sessions[agent.session_id] = asyncio.current_task()
    try:
        daemon.poll({"session_id": agent.session_id, "owner_id": agent.owner_id})
        assert not agent._terminal_observed, "diagnostic poll stole watch completion"
        daemon.poll({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "_watch_delivery": True,
        })
        assert agent._terminal_observed, "watch delivery did not consume completion"
    finally:
        common.shape_poll = original_shape_poll
        daemon._active_watch_sessions.clear()


async def check_event_driven_wait() -> None:
    daemon = Agentd()
    first_agent = watch_agent("wait-first")
    second_agent = watch_agent("wait-second")
    daemon._registry = {
        first_agent.session_id: first_agent,
        second_agent.session_id: second_agent,
    }
    waiting = asyncio.create_task(daemon.wait({
        "session_ids": [first_agent.session_id, second_agent.session_id],
        "owner_id": first_agent.owner_id,
        "timeout": 10,
    }))
    await asyncio.sleep(0)
    assert set(daemon._wait_waiters) == {
        first_agent.session_id, second_agent.session_id,
    }, daemon._wait_waiters
    first_agent.event_seq += 1
    daemon._wake_waiters(first_agent.session_id)
    result = await asyncio.wait_for(waiting, timeout=1)
    assert result["woke"] == [first_agent.session_id], result
    assert not daemon._wait_waiters, daemon._wait_waiters


def check_pending_permission_is_durable() -> None:
    agent = watch_agent("permission-race")
    agent._pending_permission = (17, {
        "toolCall": {"title": "Proceed?"},
        "options": [{"optionId": "yes", "name": "Yes"}],
    })
    agent.poll = lambda: []
    agent.snapshot_turns = lambda: []
    agent.plan_entries = []
    agent.current_work = None
    agent.transcript_path = "/tmp/permission-race.jsonl"
    agent.last_error = None
    result = common.shape_poll(agent)
    assert result["permission_request"]["request_id"] == 17, result
    assert result["transcript_path"] == agent.transcript_path


async def main() -> None:
    await check_connection_probe_serialization()
    await check_old_reader_isolation()
    await check_read_retry()
    await check_targeted_watch_wakeups()
    await check_poll_does_not_steal_watch_terminal()
    await check_event_driven_wait()
    check_pending_permission_is_durable()
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
