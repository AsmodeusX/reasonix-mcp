#!/usr/bin/env python3
"""Deterministic watch concurrency and cancellation checks (no model)."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import sys
import tempfile
import threading
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
    def __init__(self, *, disconnect: bool = False, write_failure: bool = False) -> None:
        super().__init__()
        self.calls = 0
        self.tokens: list[str] = []
        self.disconnect = disconnect
        self.write_failure = write_failure

    def write(self, data: bytes) -> None:
        if self.write_failure:
            raise BrokenPipeError("test transport closed before write")
        super().write(data)
        frame = self.frames[-1]
        if frame.get("method") not in ("list", "poll") or "id" not in frame:
            return
        self.calls += 1
        self.tokens.append(str((frame.get("params") or {}).get("_request_token")))
        rid = frame["id"]

        def respond() -> None:
            future = mcp_server._pending.pop(rid)
            mcp_server._pending_writers.pop(rid, None)
            if self.disconnect:
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
    failed_writer = RetryWriter(disconnect=True)
    replacement_writer = RetryWriter()
    writers = iter((failed_writer, replacement_writer))

    async def connected() -> None:
        mcp_server._agentd_writer = next(writers)

    mcp_server._ensure_agentd = connected
    mcp_server._agentd_cancel_supported = True
    try:
        result = await mcp_server._rpc("poll", {"session_id": "retry-me"})
        assert result == {"status": "running", "retried": True}, result
        assert failed_writer.closed, "failed read transport was not discarded"
        assert failed_writer.calls == replacement_writer.calls == 1
        assert failed_writer.tokens[0], failed_writer.tokens
        assert failed_writer.tokens == replacement_writer.tokens
    finally:
        mcp_server._ensure_agentd = original_ensure
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_supported


async def check_write_retry() -> None:
    original_ensure = mcp_server._ensure_agentd
    original_writer = mcp_server._agentd_writer
    original_supported = mcp_server._agentd_cancel_supported
    failed_writer = RetryWriter(write_failure=True)
    replacement_writer = RetryWriter()
    writers = iter((failed_writer, replacement_writer))

    async def connected() -> None:
        mcp_server._agentd_writer = next(writers)

    mcp_server._ensure_agentd = connected
    mcp_server._agentd_cancel_supported = True
    try:
        result = await mcp_server._rpc("list", {})
        assert result == {"status": "running", "retried": True}, result
        assert failed_writer.closed, "failed write transport was not discarded"
        assert replacement_writer.calls == 1
    finally:
        mcp_server._ensure_agentd = original_ensure
        mcp_server._agentd_writer = original_writer
        mcp_server._agentd_cancel_supported = original_supported


async def check_probe_disconnect_retry() -> None:
    original_ensure = mcp_server._ensure_agentd
    original_writer = mcp_server._agentd_writer
    original_supported = mcp_server._agentd_cancel_supported
    replacement_writer = RetryWriter()
    attempts = 0

    async def connected() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("agentd connection closed; agents may still be running")
        mcp_server._agentd_writer = replacement_writer

    mcp_server._ensure_agentd = connected
    mcp_server._agentd_cancel_supported = True
    try:
        result = await mcp_server._rpc("list", {})
        assert result == {"status": "running", "retried": True}, result
        assert attempts == 2
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
        config_values={},
        pending_config={},
        pending_config_error=None,
        runtime_status="running",
        runtime_health={},
        _runtime_alert_observed=False,
    )


async def check_runtime_stall_is_actionable() -> None:
    daemon = Agentd()
    agent = watch_agent("runtime-stall")
    daemon._registry[agent.session_id] = agent

    # Workspace queueing is informational and must not flood a fleet watch.
    agent.runtime_status = "blocked"
    agent.runtime_health = {"reason": "workspace_lease"}
    blocked = await daemon.watch({
        "session_ids": [agent.session_id],
        "owner_id": agent.owner_id,
        "timeout": 0,
    })
    assert blocked["timed_out"] and not blocked["woke"], blocked

    agent.runtime_status = "stalled"
    agent.runtime_health = {"reason": "no_protocol_or_process_progress"}

    def fake_poll(params: dict) -> dict:
        if params.get("_watch_delivery"):
            agent._runtime_alert_observed = True
        return {
            "session_id": agent.session_id,
            "status": "stalled",
            "health": dict(agent.runtime_health),
            "turns": [],
            "permission_request": None,
            "plan": [],
            "current_work": None,
            "transcript_path": None,
        }

    daemon.poll = fake_poll
    stalled = await daemon.watch({
        "session_ids": [agent.session_id],
        "owner_id": agent.owner_id,
    })
    assert stalled["woke"] == [agent.session_id], stalled
    assert stalled["results"][agent.session_id]["status"] == "stalled", stalled
    assert stalled["results"][agent.session_id]["health"]["reason"] == (
        "no_protocol_or_process_progress"
    ), stalled


async def check_mcp_list_compacts_old_daemon_results() -> None:
    original_rpc = mcp_server._rpc
    original_owned = mcp_server._owned_sessions
    original_checkpoint = mcp_server._checkpoint_agent_state
    session_id = "legacy-list-session"
    large_session = {
        "session_id": session_id,
        "status": "running",
        "cwd": "/tmp/project",
        "task": "short task",
        "permission_request": False,
        "process_alive": True,
        "resumable": False,
        "plan": [{"status": "pending", "content": "P" * 5000} for _ in range(12)],
        "current_work": {"plan_step": {"content": "P" * 5000}},
        "config": {"model": "provider/model"},
        "transcript_path": "/tmp/large.jsonl",
    }

    async def legacy_list(_method: str, _params: dict, **_kwargs) -> dict:
        return {"sessions": [dict(large_session)]}

    mcp_server._rpc = legacy_list
    mcp_server._owned_sessions = {session_id}
    mcp_server._checkpoint_agent_state = lambda *_args, **_kwargs: None
    try:
        compact = await mcp_server.reasonix_list()
        assert compact["count"] == 1 and compact["counts"] == {"running": 1}
        assert len(json.dumps(compact)) < 2000, len(json.dumps(compact))
        assert not ({"plan", "current_work", "config", "transcript_path"}
                    & compact["sessions"][0].keys())
        detailed = await mcp_server.reasonix_list(detail=True)
        assert detailed["sessions"][0]["plan"] == large_session["plan"]
    finally:
        mcp_server._rpc = original_rpc
        mcp_server._owned_sessions = original_owned
        mcp_server._checkpoint_agent_state = original_checkpoint


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


def check_resume_is_idempotent_for_live_agent() -> None:
    daemon = Agentd()
    agent = watch_agent("already-live")
    agent.cwd = "/tmp/already-live"
    daemon._registry[agent.session_id] = agent
    result = daemon.resume({
        "session_id": agent.session_id,
        "owner_id": agent.owner_id,
        "cwd": "/tmp/ignored-new-cwd",
    })
    assert result == {
        "session_id": agent.session_id,
        "status": "running",
        "cwd": agent.cwd,
        "already_live": True,
        "resumed": False,
        "note": (
            "session already has a live agent process; use watch/poll/send "
            "instead of starting another ACP process"
        ),
    }, result
    assert daemon._registry[agent.session_id] is agent


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


async def check_configure_queues_active_turn() -> None:
    daemon = Agentd()
    agent = watch_agent("configure-active")
    daemon._registry[agent.session_id] = agent
    original_write = common.write_session_config
    writes: list[dict] = []
    applied: list[dict] = []

    def write_config(_session_id: str, updates: dict, **_kwargs) -> dict:
        writes.append(dict(updates))
        return dict(updates)

    def configure(updates: dict) -> dict:
        applied.append(dict(updates))
        agent.config_values.update(updates)
        return {
            "applied_options": dict(updates),
            "skipped_options": [],
            "config": dict(agent.config_values),
        }

    common.write_session_config = write_config
    agent.configure = configure
    try:
        queued = await daemon.configure({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "model": "codex/gpt-next",
            "effort": "max",
        })
        assert queued["queued"] and queued["applies"] == "after_current_turn"
        assert not applied
        agent.active_turn = False
        agent.status = "idle"
        await daemon._apply_pending_config(agent)
        assert applied == [{"model": "codex/gpt-next", "effort": "max"}]
        assert not agent.pending_config

        agent.status = "exited"
        stopped = await daemon.configure({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "model": "codex/stopped-model",
        })
        assert stopped["applies"] == "on_resume" and stopped["queued"]
        assert writes[-1] == {
            "model": "codex/stopped-model",
            "effort": "",
        }
    finally:
        common.write_session_config = original_write


async def check_configure_blocks_new_turn_until_applied() -> None:
    daemon = Agentd()
    agent = watch_agent("configure-send-race")
    agent.active_turn = False
    agent.status = "idle"
    daemon._registry[agent.session_id] = agent
    started = threading.Event()
    release = threading.Event()
    original_write = common.write_session_config

    def write_config(_session_id: str, updates: dict, **_kwargs) -> dict:
        return dict(updates)

    def configure(updates: dict) -> dict:
        started.set()
        assert release.wait(1), "test did not release the model rebuild"
        agent.config_values.update(updates)
        return {"config": dict(agent.config_values)}

    def start_turn(_message: str) -> None:
        agent.active_turn = True
        agent.status = "running"

    common.write_session_config = write_config
    agent.configure = configure
    agent.start_turn = start_turn
    try:
        changing = asyncio.create_task(daemon.configure({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "model": "codex/new-model",
        }))
        assert await asyncio.to_thread(started.wait, 1)
        sending = asyncio.create_task(daemon.send({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "message": "continue",
            "expect": "new_turn",
        }))
        await asyncio.sleep(0)
        assert not sending.done(), "new turn raced ahead of model rebuild"
        release.set()
        changed, sent = await asyncio.gather(changing, sending)
        assert changed["config"]["model"] == "codex/new-model"
        assert sent["delivered"] == "new_turn"
    finally:
        release.set()
        common.write_session_config = original_write


async def check_permission_mode_changes_resolve_only_tool_gates() -> None:
    daemon = Agentd()
    agent = watch_agent("permission-mode-change")
    daemon._registry[agent.session_id] = agent
    original_write = common.write_session_config
    answers: list[str] = []

    def write_config(_session_id: str, updates: dict, **_kwargs) -> dict:
        return dict(updates)

    def answer_permission(option_id: str) -> bool:
        answers.append(option_id)
        agent._pending_permission = None
        return True

    common.write_session_config = write_config
    agent.answer_permission = answer_permission
    try:
        agent._pending_permission = (41, {
            "toolCall": {"kind": "execute", "title": "Run command"},
            "options": [
                {"optionId": "reject_once", "name": "Reject"},
                {"optionId": "allow_always", "name": "Always allow"},
                {"optionId": "allow_once", "name": "Allow once"},
            ],
        })
        changed = await daemon.configure({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "tool_approval": "yolo",
        })
        assert changed["permission_resolution"] == "allow_once", changed
        assert not changed["permission_still_pending"]
        assert answers == ["allow_once"]
        assert agent.pending_config["tool_approval"] == "yolo"

        # A later gate in the same still-active turn follows queued YOLO even
        # though ACP cannot durably rebuild the option until turn end.
        agent._pending_permission = (42, {
            "toolCall": {"kind": "execute", "title": "Run another command"},
            "options": [{"optionId": "allow_once", "name": "Allow once"}],
        })
        assert daemon._auto_answer_pending_permission(agent) == "allow_once"
        assert answers == ["allow_once", "allow_once"]

        # Explicit agent questions never become approvals, even in YOLO.
        agent._pending_permission = (43, {
            "toolCall": {"kind": "other", "title": "Which design?"},
            "options": [{"optionId": "choice_a", "name": "A"}],
        })
        assert daemon._auto_answer_pending_permission(agent) is None
        assert agent._pending_permission is not None

        # AUTO cannot safely infer how policy would classify an already-open
        # request, so it remains pending.
        agent.pending_config.clear()
        auto = await daemon.configure({
            "session_id": agent.session_id,
            "owner_id": agent.owner_id,
            "tool_approval": "auto",
        })
        assert auto["permission_resolution"] is None
        assert auto["permission_still_pending"] is True
    finally:
        common.write_session_config = original_write


async def check_configure_persisted_session_without_tombstone() -> None:
    daemon = Agentd()
    old_home = os.environ.get("REASONIX_HOME")
    with tempfile.TemporaryDirectory(prefix="reasonix-configure-") as scratch:
        os.environ["REASONIX_HOME"] = scratch
        session_id = "persisted-without-tombstone"
        owner_id = "owner-persisted"
        sessions = os.path.join(scratch, "sessions")
        os.makedirs(sessions)
        with open(os.path.join(sessions, f"{session_id}.jsonl"), "w"):
            pass
        common.write_session_owner(session_id, owner_id)
        common.write_session_config(session_id, {
            "model": "codex/old-model",
            "effort": "old-model-only-effort",
        })
        try:
            result = await daemon.configure({
                "session_id": session_id,
                "owner_id": owner_id,
                "model": "codex/resurrected-model",
            })
            assert result["applies"] == "on_resume", result
            assert common.read_session_config(session_id)["model"] == (
                "codex/resurrected-model"
            )
            assert "effort" not in common.read_session_config(session_id)
        finally:
            if old_home is None:
                os.environ.pop("REASONIX_HOME", None)
            else:
                os.environ["REASONIX_HOME"] = old_home


async def check_server_compacts_legacy_daemon_poll() -> None:
    session_id = "legacy-daemon-poll"
    original_rpc = mcp_server._rpc
    original_owned = set(mcp_server._owned_sessions)

    async def legacy_rpc(_method: str, _params: dict, **_kwargs) -> dict:
        return {
            "session_id": session_id,
            "status": "idle",
            "stop_reason": "end_turn",
            "text": "done",
            "text_truncated": False,
            "turns": [{"text": "done", "stop_reason": "end_turn"}],
            "plan": [],
            "current_work": None,
            "events": [{"type": "large_legacy_event"}],
            "events_dropped": 0,
            "events_filtered": 0,
            "permission_request": None,
            "config": {"model": "legacy/model"},
            "pending_config": {},
            "pending_config_error": None,
            "transcript_path": "/tmp/legacy.jsonl",
        }

    mcp_server._rpc = legacy_rpc
    mcp_server._owned_sessions.add(session_id)
    try:
        compact = await mcp_server.reasonix_poll(session_id)
        assert compact["message"] == "done"
        assert not ({"events", "turns", "text", "config"} & compact.keys())
        detailed = await mcp_server.reasonix_poll(session_id, detail=True)
        assert detailed["events"] and detailed["turns"]
    finally:
        mcp_server._rpc = original_rpc
        mcp_server._owned_sessions = original_owned


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


def check_list_permission_detail_does_not_hide_fleet() -> None:
    daemon = Agentd()
    agent = watch_agent("permission-list")
    agent.task = "Needs a decision"
    agent.cwd = ROOT
    agent.transcript_path = "/tmp/permission-list.jsonl"
    agent.plan_entries = [
        {"status": "pending", "content": "P" * 4000} for _ in range(12)
    ]
    agent.current_work = {
        "summary": "working",
        "plan_step": {"content": "P" * 4000, "status": "in_progress"},
    }
    agent.config_values = {"model": "provider/model", "effort": "high"}
    agent._pending_permission = (23, {
        "toolCall": {"kind": "execute", "title": "Run command"},
        "options": [{"optionId": "allow_once", "name": "Allow once"}],
    })
    daemon._registry[agent.session_id] = agent
    result = daemon.list({
        "owner_id": agent.owner_id,
        "include_permission_detail": True,
    })
    assert len(result["sessions"]) == 1, result
    permission = result["sessions"][0]["permission_request"]
    assert permission == {
        "request_id": 23,
        "tool_call": {"kind": "execute", "title": "Run command"},
        "options": [{"optionId": "allow_once", "name": "Allow once"}],
    }, permission
    compact_session = result["sessions"][0]
    assert result["count"] == 1 and result["counts"] == {"running": 1}, result
    assert not ({"plan", "current_work", "config", "transcript_path"} & compact_session.keys())
    assert len(json.dumps(result)) < 2000, len(json.dumps(result))

    detailed = daemon.list({
        "owner_id": agent.owner_id,
        "detail": True,
    })
    detailed_session = detailed["sessions"][0]
    assert detailed_session["plan"] == agent.plan_entries
    assert detailed_session["current_work"] == agent.current_work
    assert detailed_session["config"] == agent.config_values
    assert detailed_session["transcript_path"] == agent.transcript_path
    assert len(json.dumps(detailed)) > 40_000


async def main() -> None:
    await check_runtime_stall_is_actionable()
    await check_mcp_list_compacts_old_daemon_results()
    await check_connection_probe_serialization()
    await check_old_reader_isolation()
    await check_read_retry()
    await check_write_retry()
    await check_probe_disconnect_retry()
    await check_targeted_watch_wakeups()
    await check_poll_does_not_steal_watch_terminal()
    check_resume_is_idempotent_for_live_agent()
    await check_event_driven_wait()
    await check_configure_queues_active_turn()
    await check_configure_blocks_new_turn_until_applied()
    await check_permission_mode_changes_resolve_only_tool_gates()
    await check_configure_persisted_session_without_tombstone()
    await check_server_compacts_legacy_daemon_poll()
    check_pending_permission_is_durable()
    check_list_permission_detail_does_not_hide_fleet()
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

    poll_compact = daemon._compact_poll_result({
        "session_id": "compact",
        "status": "running",
        "text": long_text,
        "text_truncated": False,
        "turns": [],
        "plan": [{"content": "Implement", "status": "in_progress"}],
        "current_work": {
            "source": "plan",
            "summary": "Implement",
            "plan_step": {"content": "Implement", "status": "in_progress"},
        },
        "permission_request": None,
        "events": [{"type": "tool_call", "rawInput": "large"}],
        "events_dropped": 12,
        "events_filtered": 30,
        "transcript_path": "/tmp/compact.jsonl",
        "config": {"model": "large/catalogue"},
        "pending_config": {},
        "pending_config_error": None,
    })
    assert poll_compact["message_truncated"] is True
    assert poll_compact["plan"] and poll_compact["current_work"]
    assert "plan_step" not in poll_compact["current_work"]
    assert not ({
        "events", "events_dropped", "events_filtered", "turns", "text",
        "transcript_path", "config", "permission_request",
    } & poll_compact.keys()), poll_compact
    print("WATCH CONCURRENCY SELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
