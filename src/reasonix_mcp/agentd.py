#!/usr/bin/env python3
"""agentd — detached Reasonix agent daemon.

Owns the `reasonix acp` subprocesses so agents **survive MCP host restarts**:
the host (Claude Code, Codex, or any other MCP client) and its stdio server can
die and come back; the daemon keeps the agents running, and a new server
reconnects and sees them via reasonix_list.

Wire protocol over a Unix socket: NDJSON JSON-RPC 2.0, one frame per line.
  server -> daemon:  {"jsonrpc":"2.0","id":N,"method":"spawn","params":{...}}
  server -> daemon:  {"jsonrpc":"2.0","method":"cancel_request","params":{"request_id":N}}
  daemon -> server:  {"jsonrpc":"2.0","id":N,"result":{...}}  (or "error")
  daemon -> server:  {"method":"agent_event","params":{...}}   (push; best-effort)

Methods mirror the MCP tools (spawn/send/poll/wait/list/models/transcript/
watch/respond_permission/stop/resume). The daemon holds the authoritative session
registry; agents carry PDEATHSIG so they die with the daemon, never orphan.

Started detached by server.py (or manually: `python agentd.py --sock PATH`).
"""

from __future__ import annotations

import argparse
import asyncio
from collections import OrderedDict
from collections.abc import Iterable
import json
import os
import signal
import subprocess
import sys
import threading
import time

import acp_bridge
import common


SOURCE_WATCH_INTERVAL = 0.5
AUTO_RESTART_IDLE_GRACE = 3.0


class AgentdError(Exception):
    def __init__(self, message: str, code: int = -32000):
        super().__init__(message)
        self.code = code


class Agentd:
    def __init__(self):
        self._registry: dict[str, acp_bridge.ReasonixAgent] = {}
        self._lock = threading.Lock()
        self._clients: set[asyncio.StreamWriter] = set()
        self._client_owners: dict[asyncio.StreamWriter, str] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        # Actionable watches are indexed by session. A plan/tool update for one
        # busy child must not wake every fleet watch in the daemon.
        self._watch_waiters: dict[str, set[asyncio.Future]] = {}
        self._wait_waiters: dict[str, set[asyncio.Future]] = {}
        self._active_watch_sessions: dict[str, asyncio.Task] = {}
        self._idle_cleanup_tasks: dict[str, asyncio.Task] = {}
        # Consuming calls can finish just as their Unix socket drops. Cache a
        # bounded set of tokenized results so the MCP front-end can reconnect
        # and retry without losing poll deltas or a delivered watch terminal.
        self._delivery_cache: OrderedDict[tuple[str, str, str], dict] = OrderedDict()
        self._code_signature = common.agentd_code_signature()
        self._auto_restart_requested = False

    # ------------------------------------------------------------- registry

    def _get(self, session_id: str, owner_id: str = "") -> acp_bridge.ReasonixAgent:
        with self._lock:
            agent = self._registry.get(session_id)
        if agent is None or getattr(agent, "owner_id", "") != owner_id:
            raise AgentdError(f"unknown session_id {session_id!r} — did you call reasonix_spawn first?")
        return agent

    def _register(self, agent: acp_bridge.ReasonixAgent) -> None:
        if self._loop is not None:
            def _broadcast(kind, payload, agent=agent):
                try:
                    event = common.build_agent_event(agent, kind, payload)
                    self._loop.call_soon_threadsafe(
                        self._wake_watchers, agent.session_id, kind
                    )
                    if kind == acp_bridge.EV_TURN_END:
                        self._loop.call_soon_threadsafe(self._schedule_idle_cleanup, agent)
                    elif kind == acp_bridge.EV_PROCESS_EXIT:
                        self._loop.call_soon_threadsafe(self._cancel_idle_cleanup, agent.session_id)
                    print(
                        f"reasonix agent_event emit: event={event.get('event')} "
                        f"session={agent.session_id} owner={getattr(agent, 'owner_id', '')}",
                        file=sys.stderr, flush=True,
                    )
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast(event, getattr(agent, "owner_id", "")), self._loop
                    )
                except Exception:
                    pass
            agent.on_event = _broadcast
            def _activity(_kind, _payload, agent=agent):
                # The reader thread checks first so ordinary streaming has no
                # event-loop scheduling cost unless a legacy wait is active.
                if agent.session_id in self._wait_waiters:
                    self._loop.call_soon_threadsafe(
                        self._wake_waiters, agent.session_id
                    )
            agent.on_activity = _activity
        with self._lock:
            self._registry[agent.session_id] = agent
        # The ACP reader can observe an immediate process death during the
        # constructor, before the daemon has attached its callback. Replay one
        # terminal notification so a fast crash is not silent.
        if agent.status == "exited" and agent.on_event is not None:
            payload: dict = {
                "code": getattr(agent, "_exit_code", None),
                "stderr": "\n".join(getattr(agent, "_stderr_tail", [])[-50:])[-8000:],
            }
            if getattr(agent, "last_error", None) is not None:
                payload["error"] = agent.last_error
                payload["error_text"] = agent.last_error_text
            agent.on_event(acp_bridge.EV_PROCESS_EXIT, payload)

    def _cancel_idle_cleanup(self, session_id: str) -> None:
        task = self._idle_cleanup_tasks.pop(session_id, None)
        if task is not None and not task.done():
            task.cancel()

    def _wake_watchers(self, session_id: str, kind: str) -> None:
        """Wake only watches affected by an actionable ACP callback."""
        if kind not in (
            acp_bridge.EV_TURN_END,
            acp_bridge.EV_PERMISSION,
            acp_bridge.EV_PROCESS_EXIT,
        ):
            return
        waiters = list(self._watch_waiters.pop(session_id, ()))
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _add_watch_waiter(
        self, waiter: asyncio.Future, session_ids: Iterable[str]
    ) -> None:
        for session_id in session_ids:
            self._watch_waiters.setdefault(str(session_id), set()).add(waiter)

    def _discard_watch_waiter(
        self, waiter: asyncio.Future, session_ids: Iterable[str]
    ) -> None:
        for session_id in session_ids:
            sid = str(session_id)
            waiters = self._watch_waiters.get(sid)
            if waiters is None:
                continue
            waiters.discard(waiter)
            if not waiters:
                self._watch_waiters.pop(sid, None)

    def _wake_waiters(self, session_id: str) -> None:
        waiters = list(self._wait_waiters.pop(session_id, ()))
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _add_wait_waiter(
        self, waiter: asyncio.Future, session_ids: Iterable[str]
    ) -> None:
        for session_id in session_ids:
            self._wait_waiters.setdefault(str(session_id), set()).add(waiter)

    def _discard_wait_waiter(
        self, waiter: asyncio.Future, session_ids: Iterable[str]
    ) -> None:
        for session_id in session_ids:
            sid = str(session_id)
            waiters = self._wait_waiters.get(sid)
            if waiters is None:
                continue
            waiters.discard(waiter)
            if not waiters:
                self._wait_waiters.pop(sid, None)

    def _schedule_idle_cleanup(self, agent: acp_bridge.ReasonixAgent) -> None:
        if getattr(agent, "keep_alive", False):
            return
        self._cancel_idle_cleanup(agent.session_id)
        timeout = float(getattr(agent, "idle_timeout", common.DEFAULT_IDLE_TIMEOUT))
        if timeout < 0:
            return
        self._idle_cleanup_tasks[agent.session_id] = asyncio.create_task(
            self._auto_stop_idle(agent, agent.event_seq, timeout)
        )

    def _cached_delivery(self, method: str, params: dict) -> dict | None:
        token = str(params.get("_request_token") or "")
        if not token:
            return None
        key = (str(params.get("owner_id") or ""), method, token)
        result = self._delivery_cache.get(key)
        if result is not None:
            self._delivery_cache.move_to_end(key)
        return result

    def _cache_delivery(self, method: str, params: dict, result: dict) -> dict:
        token = str(params.get("_request_token") or "")
        if not token:
            return result
        key = (str(params.get("owner_id") or ""), method, token)
        self._delivery_cache[key] = result
        self._delivery_cache.move_to_end(key)
        while len(self._delivery_cache) > 512:
            self._delivery_cache.popitem(last=False)
        return result

    async def _auto_stop_idle(
        self, agent: acp_bridge.ReasonixAgent, event_seq: int, timeout: float
    ) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(timeout)
            if (
                agent.status == "idle"
                and not agent.active_turn
                and agent.event_seq == event_seq
            ):
                # Mark before moving the blocking close operation to a worker;
                # a concurrent send then gets a clear lifecycle error instead
                # of racing the process teardown.
                agent._auto_closing = True
                await asyncio.to_thread(agent.close)
        except asyncio.CancelledError:
            pass
        finally:
            if self._idle_cleanup_tasks.get(agent.session_id) is task:
                self._idle_cleanup_tasks.pop(agent.session_id, None)

    async def _broadcast(self, event: dict, owner_id: str) -> None:
        line = json.dumps({"method": "agent_event", "params": event}) + "\n"
        dead = []
        for w in self._clients:
            if self._client_owners.get(w, "") != owner_id:
                continue
            try:
                w.write(line.encode())
                await w.drain()
            except Exception:
                dead.append(w)
        for w in dead:
            self._clients.discard(w)

    # ---------------------------------------------------------------- methods

    @staticmethod
    def _start_agent_turn(agent: acp_bridge.ReasonixAgent, message: str) -> None:
        """Start a turn, injecting the progress contract once per ACP session."""
        inject = not bool(getattr(agent, "status_protocol_injected", False))
        agent.start_turn(common.task_with_status_protocol(message) if inject else message)
        if inject:
            agent.status_protocol_injected = True

    def spawn(self, params: dict) -> dict:
        task = params.get("task", "")
        cwd = params.get("cwd") or os.getcwd()
        if not task.strip():
            raise AgentdError("task must not be empty")
        owner_id = str(params.get("owner_id") or "")
        if not os.path.isdir(cwd):
            raise AgentdError(f"cwd is not a directory: {cwd!r}")
        if not common.cwd_allowed(cwd):
            raise AgentdError(
                f"cwd {cwd!r} is outside the allowed spawn roots (project dir + "
                "[sandbox] allow_write); set REASONIX_MCP_ALLOW_ANY_CWD=1 to allow any path"
            )
        tool_approval = params.get("tool_approval", common.DEFAULT_TOOL_APPROVAL)
        if tool_approval not in ("ask", "auto", "yolo"):
            raise AgentdError("tool_approval must be one of: ask, auto, yolo")
        work_mode = params.get("work_mode", common.DEFAULT_WORK_MODE)
        if work_mode and work_mode not in ("economy", "balanced", "delivery"):
            raise AgentdError("work_mode must be one of: economy, balanced, delivery")
        keep_alive = params.get("keep_alive", False)
        if not isinstance(keep_alive, bool):
            raise AgentdError("keep_alive must be a boolean")
        try:
            idle_timeout = float(params.get("idle_timeout", common.DEFAULT_IDLE_TIMEOUT))
        except (TypeError, ValueError):
            raise AgentdError("idle_timeout must be -1 (disabled) or between 0 and 86400 seconds") from None
        if idle_timeout < -1 or idle_timeout > 86400:
            raise AgentdError("idle_timeout must be -1 (disabled) or between 0 and 86400 seconds")

        agent = acp_bridge.ReasonixAgent(
            cwd=cwd,
            model=params.get("model", common.DEFAULT_MODEL),
            work_mode=work_mode,
            tool_approval=tool_approval,
            effort=params.get("effort", common.DEFAULT_EFFORT),
        )
        agent.task = task
        agent.cwd = cwd
        agent.owner_id = owner_id
        common.write_session_owner(agent.session_id, owner_id)
        agent.keep_alive = keep_alive
        agent.idle_timeout = idle_timeout
        self._register(agent)
        self._start_agent_turn(agent, task)
        posture = common.sandbox_posture()
        for warning in posture.get("warnings", []):
            print(f"warning: {warning}", file=sys.stderr, flush=True)
        result: dict = {
            "session_id": agent.session_id,
            "status": "running",
            "cwd": cwd,
            "sandbox": posture,
            "note": ("Agent runs detached in agentd and survives MCP server restarts. "
                     "Keep reasonix_watch in flight for completion, permission, "
                     "and process-exit delivery. Wire notifications are diagnostic. "
                     "Use reasonix_poll for an explicit snapshot, "
                     "reasonix_send to steer mid-turn, reasonix_resume to revive "
                     "a stopped session."),
            "keep_alive": keep_alive,
            "idle_timeout": idle_timeout,
        }
        notes = getattr(agent, "spawn_notes", None) or {}
        if notes.get("skipped_options"):
            result["skipped_options"] = notes["skipped_options"]
            if "effort" in notes["skipped_options"]:
                result["skipped_options_note"] = (
                    "effort was not advertised by the selected model and was not "
                    "applied. For an OpenAI-compatible proxy, configure "
                    "reasoning_protocol='openai', supported_efforts, and "
                    "default_effort on the provider/model override. Use a model-name "
                    "effort variant only when that gateway actually encodes effort "
                    "in model IDs"
                )
            else:
                result["skipped_options_note"] = (
                    "these config options were not advertised by the selected model "
                    "and were not applied"
                )
        return result

    def resume(self, params: dict) -> dict:
        session_id = params.get("session_id", "")
        owner_id = str(params.get("owner_id") or "")
        cwd = params.get("cwd") or ""
        with self._lock:
            existing = self._registry.get(session_id)
        if existing is not None and getattr(existing, "owner_id", "") != owner_id:
            raise AgentdError(f"unknown session_id {session_id!r} — it belongs to another orchestrator")
        if existing is not None and common.agent_status(existing) != "exited":
            # Resume is a recovery operation, but callers can race daemon
            # reconnect/list recovery and invoke it for a process that never
            # died. Treat that as success instead of opening a second ACP
            # process, which Reasonix correctly rejects as "session in use".
            return {
                "session_id": session_id,
                "status": common.agent_status(existing),
                "cwd": getattr(existing, "cwd", None) or cwd or os.getcwd(),
                "already_live": True,
                "resumed": False,
                "note": (
                    "session already has a live agent process; use watch/poll/send "
                    "instead of starting another ACP process"
                ),
            }
        if existing is None:
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{session_id}.jsonl")
            if not os.path.isfile(persisted):
                raise AgentdError(f"no live or persisted session {session_id!r} to resume")
            stored_owner = common.read_session_owner(session_id)
            if stored_owner != owner_id:
                raise AgentdError(
                    f"session {session_id!r} is owned by another orchestrator or predates ownership tracking"
                )
        if not cwd:
            cwd = getattr(existing, "cwd", None) or os.getcwd()
        agent = acp_bridge.ReasonixAgent(cwd=cwd, resume_session_id=session_id)
        agent.status_protocol_injected = common.transcript_has_status_protocol(session_id)
        agent.task = getattr(existing, "task", None) or f"resumed {session_id}"
        agent.cwd = cwd
        agent.owner_id = owner_id
        common.write_session_owner(session_id, owner_id)
        keep_alive = params.get("keep_alive", False)
        if not isinstance(keep_alive, bool):
            raise AgentdError("keep_alive must be a boolean")
        try:
            idle_timeout = float(params.get("idle_timeout", common.DEFAULT_IDLE_TIMEOUT))
        except (TypeError, ValueError):
            raise AgentdError("idle_timeout must be -1 (disabled) or between 0 and 86400 seconds") from None
        if idle_timeout < -1 or idle_timeout > 86400:
            raise AgentdError("idle_timeout must be -1 (disabled) or between 0 and 86400 seconds")
        agent.keep_alive = keep_alive
        agent.idle_timeout = idle_timeout
        self._register(agent)
        return {
            "session_id": session_id,
            "status": common.agent_status(agent),
            "cwd": cwd,
            "already_live": False,
            "resumed": True,
            "note": "session resumed from its persisted transcript; send/poll work as before",
        }

    def send(self, params: dict) -> dict:
        session_id = params["session_id"]
        message = params.get("message", "")
        expect = params.get("expect", "any")
        if not message.strip():
            raise AgentdError("message must not be empty")
        if expect not in ("any", "steer", "new_turn"):
            raise AgentdError("expect must be one of: any, steer, new_turn")
        agent = self._get(session_id, str(params.get("owner_id") or ""))
        self._cancel_idle_cleanup(session_id)
        if getattr(agent, "_auto_closing", False):
            raise AgentdError(f"agent {session_id} is being cleaned up")
        if agent.status == "exited":
            raise AgentdError(f"agent {session_id} has exited — it can no longer accept messages")
        if expect == "steer" and not agent.active_turn:
            raise AgentdError(f"agent {session_id} has no active turn — cannot steer (expect='steer')")
        if expect == "new_turn" and agent.active_turn:
            raise AgentdError(f"agent {session_id} has an active turn — would steer, but expect='new_turn'")

        delivered = "steered"
        note = "queued as mid-turn guidance"
        for _ in range(3):
            try:
                agent.steer(message)
                break
            except acp_bridge.ACPError as e:
                if e.code != acp_bridge.ERR_INVALID_REQUEST:
                    raise AgentdError(e.message, e.code) from e
            try:
                self._start_agent_turn(agent, message)
                delivered = "new_turn"
                note = "no active turn; started a new turn with the message"
                break
            except acp_bridge.ACPError as e2:
                if "already active" not in str(e2):
                    raise AgentdError(e2.message, e2.code) from e2
        else:
            raise AgentdError("could not deliver message: turn state raced")
        if expect != "any":
            expected = "steered" if expect == "steer" else expect
            if delivered != expected:
                raise AgentdError(f"send delivered {delivered!r} but expect={expect!r} — nothing further was queued")
        return {"session_id": session_id, "delivered": delivered, "note": note}

    def poll(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        cached = self._cached_delivery("poll", params)
        if cached is not None:
            return cached
        result = common.shape_poll(
            agent,
            include_events=params.get("include_events"),
            exclude_events=params.get("exclude_events"),
            include_thought=params.get("include_thought", common.DEFAULT_INCLUDE_THOUGHT),
            include_full=params.get("include_full", common.DEFAULT_INCLUDE_FULL),
            max_events=params.get("max_events"),
        )
        if (
            result.get("status") != "running"
            and result.get("stop_reason")
            and (
                params.get("_watch_delivery")
                or agent.session_id not in self._active_watch_sessions
            )
        ):
            # Automatic code reload may close a completed idle process, but
            # only after its orchestrator has collected the terminal result.
            # A diagnostic poll must not steal that result from an in-flight
            # fleet watch.
            agent._terminal_observed = True
        return self._cache_delivery("poll", params, result)

    async def wait(self, params: dict) -> dict:
        session_ids = params.get("session_ids", [])
        timeout = float(params.get("timeout", 30.0))
        if not session_ids:
            raise AgentdError("session_ids must not be empty")
        if len(session_ids) > common.MAX_WATCH_SESSIONS:
            raise AgentdError(f"too many sessions (max {common.MAX_WATCH_SESSIONS})")
        timeout = max(0.0, min(timeout, 3600.0))
        owner_id = str(params.get("owner_id") or "")
        agents = {sid: self._get(sid, owner_id) for sid in dict.fromkeys(session_ids)}
        baselines = {sid: a.event_seq for sid, a in agents.items()}
        deadline = time.monotonic() + timeout

        def snapshot() -> tuple[list[str], dict[str, dict]]:
            woke: list[str] = []
            states: dict[str, dict] = {}
            for sid, a in agents.items():
                has_new = a.event_seq > baselines[sid] or not a._events.empty()
                pending = a._pending_permission is not None
                status = common.agent_status(a)
                states[sid] = {
                    "status": status,
                    "has_new": has_new,
                    "permission_request": pending,
                    "stop_reason": a.stop_reason if status != "running" else None,
                    "plan": list(getattr(a, "plan_entries", []) or []),
                    "current_work": (
                        dict(getattr(a, "current_work", None))
                        if getattr(a, "current_work", None) else None
                    ),
                }
                if getattr(a, "last_error", None) is not None:
                    states[sid]["error"] = a.last_error
                    states[sid]["error_text"] = a.last_error_text
                if has_new or pending or status != "running":
                    woke.append(sid)
            return woke, states

        while True:
            woke, states = snapshot()
            if woke or time.monotonic() >= deadline:
                return {"woke": woke, "timed_out": not bool(woke), "sessions": states}
            waiter = asyncio.get_running_loop().create_future()
            self._add_wait_waiter(waiter, agents)
            # Close the enqueue/register race using the durable event sequence
            # and queue state checked by snapshot().
            woke, states = snapshot()
            if woke:
                self._discard_wait_waiter(waiter, agents)
                waiter.cancel()
                return {"woke": woke, "timed_out": False, "sessions": states}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._discard_wait_waiter(waiter, agents)
                waiter.cancel()
                return {"woke": [], "timed_out": True, "sessions": states}
            try:
                await asyncio.wait_for(waiter, timeout=remaining)
            except asyncio.TimeoutError:
                woke, states = snapshot()
                return {
                    "woke": woke,
                    "timed_out": not bool(woke),
                    "sessions": states,
                }
            finally:
                self._discard_wait_waiter(waiter, agents)

    async def watch(self, params: dict) -> dict:
        """Block for an actionable event and return a compact result by default.

        Unlike wait, this does not wake for text chunks or plan updates and it
        drains the bounded event queue while omitting its detail unless
        detail=true. With no timeout it is a durable in-flight MCP callback.
        """
        session_ids = params.get("session_ids", [])
        if not session_ids:
            raise AgentdError("session_ids must not be empty")
        if len(session_ids) > common.MAX_WATCH_SESSIONS:
            raise AgentdError(f"too many sessions (max {common.MAX_WATCH_SESSIONS})")
        timeout_raw = params.get("timeout")
        if timeout_raw is None:
            deadline = None
        else:
            try:
                timeout = max(0.0, min(float(timeout_raw), 86400.0))
            except (TypeError, ValueError):
                raise AgentdError("timeout must be null or between 0 and 86400 seconds") from None
            deadline = time.monotonic() + timeout
        owner_id = str(params.get("owner_id") or "")
        agents = {sid: self._get(sid, owner_id) for sid in dict.fromkeys(session_ids)}
        cached = self._cached_delivery("watch", params)
        if cached is not None:
            return cached
        watch_task = asyncio.current_task()
        # MCP hosts may cancel a tool call without closing their stdio server.
        # A newer watch is therefore authoritative and replaces any older
        # overlapping call instead of requiring an MCP/agentd restart. Loop
        # because two replacements can race while canceled tasks unwind.
        while True:
            overlap_tasks = {
                task for sid in agents
                if (task := self._active_watch_sessions.get(sid)) is not None
                and task is not watch_task
            }
            if not overlap_tasks:
                break
            for task in overlap_tasks:
                task.cancel("superseded by a newer watch")
            await asyncio.gather(*overlap_tasks, return_exceptions=True)
        if watch_task is not None:
            for sid in agents:
                self._active_watch_sessions[sid] = watch_task
        try:
            result = await self._watch_actionable(params, agents, owner_id, deadline)
            return self._cache_delivery("watch", params, result)
        finally:
            for sid in agents:
                if self._active_watch_sessions.get(sid) is watch_task:
                    self._active_watch_sessions.pop(sid, None)

    async def _watch_actionable(
        self,
        params: dict,
        agents: dict[str, acp_bridge.ReasonixAgent],
        owner_id: str,
        deadline: float | None,
    ) -> dict:
        def actionable(agent: acp_bridge.ReasonixAgent) -> bool:
            if agent._pending_permission is not None:
                return True
            status = common.agent_status(agent)
            terminal = status == "exited" or (status != "running" and bool(agent.stop_reason))
            return terminal and not bool(getattr(agent, "_terminal_observed", False))

        while True:
            woke = [sid for sid, agent in agents.items() if actionable(agent)]
            if woke:
                detailed = {
                    sid: self.poll({
                        "session_id": sid,
                        "owner_id": owner_id,
                        "_watch_delivery": True,
                        "include_events": params.get("include_events"),
                        "exclude_events": params.get("exclude_events"),
                        "include_thought": params.get("include_thought", common.DEFAULT_INCLUDE_THOUGHT),
                        "include_full": params.get("include_full", common.DEFAULT_INCLUDE_FULL),
                        "max_events": params.get("max_events"),
                    })
                    for sid in woke
                }
                # Poll deltas are intentionally single-consumer. If a
                # diagnostic poll raced this watch, retain the durable latest
                # turn in the callback so completion never arrives empty.
                for sid, result in detailed.items():
                    if not result.get("turns") and agents[sid].turns:
                        result["turns"] = [dict(agents[sid].turns[-1])]
                results = (
                    detailed if params.get("detail")
                    else {sid: self._compact_watch_result(result) for sid, result in detailed.items()}
                )
                return {"woke": woke, "timed_out": False, "results": results}
            waiter = asyncio.get_running_loop().create_future()
            self._add_watch_waiter(waiter, agents)
            # Close the check/register race: an event before registration left
            # durable state; one after registration resolves this waiter.
            if any(actionable(agent) for agent in agents.values()):
                self._discard_watch_waiter(waiter, agents)
                waiter.cancel()
                continue
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._discard_watch_waiter(waiter, agents)
                    waiter.cancel()
                    return {"woke": [], "timed_out": True, "results": {}}
                try:
                    await asyncio.wait_for(waiter, timeout=remaining)
                except asyncio.TimeoutError:
                    # The deadline and terminal callback can race. Durable
                    # state wins over a timeout at the same instant.
                    if any(actionable(agent) for agent in agents.values()):
                        continue
                    return {"woke": [], "timed_out": True, "results": {}}
                finally:
                    self._discard_watch_waiter(waiter, agents)
            else:
                try:
                    await waiter
                finally:
                    self._discard_watch_waiter(waiter, agents)

    @staticmethod
    def _compact_watch_result(detailed: dict) -> dict:
        """Reduce a consumed poll result to one context-lean outcome."""
        turns = detailed.get("turns") or []
        message = str(turns[-1].get("text") or "") if turns else str(detailed.get("text") or "")
        limit = max(0, common.MAX_WATCH_MESSAGE)
        message_truncated = len(message) > limit
        if message_truncated:
            if limit == 0:
                message = ""
            else:
                marker = "\n…[watch message truncated]…\n"
                available = max(0, limit - len(marker))
                head = available // 2
                tail = available - head
                message = (
                    marker[:limit] if not available
                    else message[:head] + marker + message[-tail:]
                )
        result = {
            "session_id": detailed.get("session_id"),
            "status": detailed.get("status"),
            "stop_reason": detailed.get("stop_reason"),
            "message": message,
            "message_truncated": message_truncated,
            "permission_request": detailed.get("permission_request"),
            "plan": detailed.get("plan") or [],
            "current_work": detailed.get("current_work"),
            "transcript_path": detailed.get("transcript_path"),
            "detail_available": True,
        }
        if detailed.get("error") is not None:
            result["error"] = detailed["error"]
            result["error_text"] = detailed.get("error_text")
        return result

    def list(self, params: dict | None = None) -> dict:
        params = params or {}
        include_task = bool(params.get("include_task", False))
        pending_only = bool(params.get("pending_only", False))
        owner_id = str(params.get("owner_id") or "")
        with self._lock:
            items = [
                item for item in self._registry.items()
                if getattr(item[1], "owner_id", "") == owner_id
            ]
        sessions = []
        for sid, a in items:
            status = common.agent_status(a)
            unobserved_terminal = (
                status != "running"
                and bool(a.stop_reason)
                and not bool(getattr(a, "_terminal_observed", False))
            )
            if pending_only and not (
                status == "running"
                or a._pending_permission is not None
                or unobserved_terminal
            ):
                continue
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{sid}.jsonl")
            session = {
                "session_id": sid,
                "status": status,
                "cwd": getattr(a, "cwd", None),
                "task": (
                    getattr(a, "task", None)
                    if include_task else common.task_preview(getattr(a, "task", None))
                ),
                "stop_reason": a.stop_reason,
                "transcript_path": getattr(a, "transcript_path", None),
                "permission_request": a._pending_permission is not None,
                "plan": list(getattr(a, "plan_entries", []) or []),
                "current_work": (
                    dict(getattr(a, "current_work", None))
                    if getattr(a, "current_work", None) else None
                ),
                "resumable": status in ("idle", "exited") and os.path.isfile(persisted),
            }
            if getattr(a, "last_error", None) is not None:
                session["error"] = a.last_error
                session["error_text"] = a.last_error_text
            sessions.append(session)
        return {"sessions": sessions}

    def models(self, params: dict | None = None) -> dict:
        return common.available_models()

    def transcript(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        max_tool_calls = int(params.get("max_tool_calls", 200))
        path = getattr(agent, "transcript_path", None)
        if not path:
            candidate = os.path.join(common.reasonix_home(), "sessions", f"{agent.session_id}.jsonl")
            if os.path.isfile(candidate):
                path = candidate
        if not path or not os.path.isfile(path):
            return {"session_id": agent.session_id, "exists": False, "transcript_path": path}
        try:
            summary = common.transcript_summary(path, max_tool_calls)
        except Exception as e:  # defensive: corrupt/partial transcript must not break the tool
            return {"session_id": agent.session_id, "exists": False, "transcript_path": path, "error": str(e)}
        return {"session_id": agent.session_id, "exists": True, "transcript_path": path, **summary}

    def respond_permission(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        option = None if params.get("option_id") == "cancel" else params.get("option_id")
        answered = agent.answer_permission(option)
        if not answered:
            raise AgentdError(f"agent {params['session_id']} has no pending permission request")
        return {"session_id": params["session_id"], "answered": params.get("option_id")}

    def stop(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        self._cancel_idle_cleanup(params["session_id"])
        agent.cancel_turn()
        agent.close()
        return {"session_id": params["session_id"], "stopped": True}

    def ping(self, params: dict | None = None) -> dict:
        owner_id = str((params or {}).get("owner_id") or "")
        with self._lock:
            sessions = sum(
                1 for agent in self._registry.values()
                if getattr(agent, "owner_id", "") == owner_id
            )
            owners = len({getattr(agent, "owner_id", "") for agent in self._registry.values()})
        return {
            "sessions": sessions,
            "owners": owners,
            "owner_scoping": True,
            "cancel_request": True,
            "code_signature": self._code_signature,
            "code_update_available": common.agentd_code_signature() != self._code_signature,
        }

    def _can_auto_restart(self) -> bool:
        """Never interrupt active, decision-blocked, or keep-alive agents."""
        with self._lock:
            agents = list(self._registry.values())
        return all(
            common.agent_status(agent) == "exited"
            or (
                not agent.active_turn
                and getattr(agent, "_pending_permission", None) is None
                and not getattr(agent, "keep_alive", False)
                and bool(agent.stop_reason)
                and bool(getattr(agent, "_terminal_observed", False))
            )
            for agent in agents
        )

    async def _watch_source(self) -> None:
        announced = False
        safe_since: float | None = None
        while True:
            await asyncio.sleep(SOURCE_WATCH_INTERVAL)
            current = common.agentd_code_signature()
            if current == self._code_signature:
                announced = False
                safe_since = None
                continue
            if not announced:
                print(
                    "reasonix agentd: updated source detected; restart queued until agents are safe",
                    file=sys.stderr,
                    flush=True,
                )
                announced = True
            if not self._can_auto_restart():
                safe_since = None
                continue
            now = time.monotonic()
            if safe_since is None:
                safe_since = now
                continue
            if now - safe_since < AUTO_RESTART_IDLE_GRACE:
                continue
            print("reasonix agentd: restarting to load updated source", file=sys.stderr, flush=True)
            self._auto_restart_requested = True
            self._shutdown_agents()
            return

    def shutdown(self, params: dict | None = None) -> dict:
        """Close every agent and stop the daemon (used by tests; the real
        deployment just leaves the daemon running)."""
        return self._shutdown_agents()

    def restart(self, params: dict | None = None) -> dict:
        """Stop this daemon so the MCP server can start a fresh copy.

        Refuse by default when live agents exist: restarting the daemon kills
        them because they are deliberately tied to its lifetime. Terminal
        idle sessions and exited tombstones are safe to discard. A caller must
        explicitly opt into disrupting active or keep-alive agents with
        force=true.
        """
        force = bool((params or {}).get("force", False))
        owner_id = str((params or {}).get("owner_id") or "")
        with self._lock:
            agents = list(self._registry.items())
        foreign_live = [
            agent for _, agent in agents
            if getattr(agent, "owner_id", "") != owner_id
            and common.agent_status(agent) != "exited"
            and (agent.active_turn or getattr(agent, "keep_alive", False) or not agent.stop_reason)
        ]
        if foreign_live and not force:
            raise AgentdError(
                "cannot restart shared agentd while another orchestrator has live agents; "
                "retry with force=true only if a global restart is intended"
            )
        live = [
            sid for sid, agent in agents
            if getattr(agent, "owner_id", "") == owner_id
            if common.agent_status(agent) != "exited"
            and (
                agent.active_turn
                or getattr(agent, "keep_alive", False)
                or not agent.stop_reason
            )
        ]
        if live and not force:
            raise AgentdError(
                "cannot restart agentd while live agents exist; stop them first "
                f"or call reasonix_restart_agentd(force=true): {live}"
            )
        return self._shutdown_agents()

    def _shutdown_agents(self) -> dict:
        for session_id in list(self._idle_cleanup_tasks):
            self._cancel_idle_cleanup(session_id)
        with self._lock:
            agents = list(self._registry.values())
            self._registry.clear()
        for a in agents:
            try:
                a.close()
            except Exception:
                pass
        if self._serve_stop is not None:
            self._loop.call_soon_threadsafe(self._serve_stop.set)
        return {"shutdown": True, "agents_stopped": len(agents)}

    # ------------------------------------------------------------- dispatch

    async def dispatch(self, msg: dict, writer: asyncio.StreamWriter) -> None:
        method = msg.get("method", "")
        params = msg.get("params") or {}
        try:
            if method in ("wait", "watch"):
                result = await getattr(self, method)(params)
            else:
                handler = getattr(self, method, None)
                if handler is None or not callable(handler):
                    raise AgentdError(f"unknown method {method!r}", -32601)
                result = handler(params)
                if asyncio.iscoroutine(result):
                    result = await result
            self._send(writer, {"jsonrpc": "2.0", "id": msg.get("id"), "result": result})
        except asyncio.CancelledError as e:
            if e.args and e.args[0] == "superseded by a newer watch" and method == "watch":
                self._send(writer, {
                    "jsonrpc": "2.0", "id": msg.get("id"),
                    "result": {
                        "woke": [], "timed_out": False, "superseded": True, "results": {},
                    },
                })
            else:
                self._send(writer, {
                    "jsonrpc": "2.0", "id": msg.get("id"),
                    "error": {"code": -32800, "message": "request cancelled"},
                })
        except AgentdError as e:
            self._send(writer, {"jsonrpc": "2.0", "id": msg.get("id"), "error": {"code": e.code, "message": str(e)}})
        except acp_bridge.ACPError as e:
            self._send(writer, {"jsonrpc": "2.0", "id": msg.get("id"), "error": {"code": e.code, "message": str(e)}})
        except Exception as e:  # never let a bad request kill the daemon
            self._send(writer, {"jsonrpc": "2.0", "id": msg.get("id"), "error": {"code": -32000, "message": f"{type(e).__name__}: {e}"}})

    @staticmethod
    def _send(writer: asyncio.StreamWriter, obj: dict) -> None:
        try:
            writer.write((json.dumps(obj) + "\n").encode())
        except Exception:
            pass

    async def client_loop(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._clients.add(writer)
        self._client_owners[writer] = ""
        requests: dict[object, asyncio.Task] = {}
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if msg.get("method") == "cancel_request" and "id" not in msg:
                    params = msg.get("params") or {}
                    owner_id = str(params.get("owner_id") or "")
                    known_owner = self._client_owners.get(writer, "")
                    if not known_owner or owner_id == known_owner:
                        task = requests.get(params.get("request_id"))
                        if task is not None and not task.done():
                            task.cancel("client cancelled request")
                    continue
                if msg.get("method") and "id" in msg:
                    owner_id = str((msg.get("params") or {}).get("owner_id") or "")
                    known_owner = self._client_owners.get(writer, "")
                    if known_owner and owner_id != known_owner:
                        self._send(writer, {
                            "jsonrpc": "2.0", "id": msg.get("id"),
                            "error": {"code": -32000, "message": "connection owner cannot change"},
                        })
                        continue
                    self._client_owners[writer] = owner_id
                    request_id = msg.get("id")
                    if request_id in requests:
                        self._send(writer, {
                            "jsonrpc": "2.0", "id": request_id,
                            "error": {"code": -32600, "message": "duplicate in-flight request id"},
                        })
                        continue
                    task = asyncio.create_task(self.dispatch(msg, writer))
                    requests[request_id] = task

                    def discard(done: asyncio.Task, rid: object = request_id) -> None:
                        if requests.get(rid) is done:
                            requests.pop(rid, None)

                    task.add_done_callback(discard)
        except (ConnectionError, asyncio.IncompleteReadError):
            # MCP front-ends can close their daemon socket while replacing a
            # canceled request or restarting. Normal connection teardown is
            # not a daemon failure and should not produce an unhandled trace.
            pass
        finally:
            pending_requests = list(requests.values())
            for task in pending_requests:
                task.cancel()
            if pending_requests:
                await asyncio.gather(*pending_requests, return_exceptions=True)
            self._clients.discard(writer)
            self._client_owners.pop(writer, None)
            try:
                writer.close()
            except Exception:
                pass

    async def serve(self, sock_path: str) -> None:
        self._loop = asyncio.get_running_loop()
        self._serve_stop = asyncio.Event()
        os.makedirs(os.path.dirname(sock_path) or ".", exist_ok=True)
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass
        server = await asyncio.start_unix_server(self.client_loop, path=sock_path)
        source_watcher = asyncio.create_task(self._watch_source())
        async with server:
            print(f"agentd listening on {sock_path}", flush=True)
            await self._serve_stop.wait()
            server.close()
            await server.wait_closed()
        source_watcher.cancel()
        try:
            await source_watcher
        except asyncio.CancelledError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="reasonix-mcp agent daemon")
    ap.add_argument("--sock", default=common.agentd_sock_path())
    # One `reasonix acp` process per agent is deliberate: a crashed session's
    # process can be killed without taking the fleet down (blast-radius).
    args = ap.parse_args()
    agentd = Agentd()
    try:
        asyncio.run(agentd.serve(args.sock))
    except KeyboardInterrupt:
        pass
    if agentd._auto_restart_requested:
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--sock", args.sock],
            stdin=subprocess.DEVNULL,
            stdout=None,
            stderr=None,
            start_new_session=True,
            env=dict(os.environ),
        )


if __name__ == "__main__":
    main()
