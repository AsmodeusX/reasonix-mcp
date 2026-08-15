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

Methods mirror the MCP tools (spawn/send/configure/poll/wait/list/models/transcript/
watch/respond_permission/stop/resume). The daemon holds the authoritative session
registry; agents carry PDEATHSIG so they die with the daemon, never orphan.

Started detached by server.py (or manually: `python agentd.py --sock PATH`).
"""

from __future__ import annotations

import argparse
import asyncio
from collections import OrderedDict
from collections.abc import Iterable
import fcntl
import hashlib
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
PROCESS_WATCH_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 60.0
WORKSPACE_BLOCKED_AFTER = float(os.environ.get(
    "REASONIX_MCP_WORKSPACE_BLOCKED_AFTER", "5"
))
RUNTIME_STALLED_AFTER = float(os.environ.get(
    "REASONIX_MCP_RUNTIME_STALLED_AFTER", "600"
))
AUTO_RESTART_IDLE_GRACE = 3.0
WORKER_METHODS = frozenset({"spawn", "resume"})


def workspace_lease_path(cwd: str) -> str:
    """Return Reasonix's per-real-workspace lease path."""
    key = hashlib.sha256(os.path.realpath(cwd).encode()).hexdigest()
    cache = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache"
    )
    return os.path.join(cache, "reasonix", "workspace-leases", f"{key}.lock")


def process_tree_snapshot(root_pid: int) -> dict:
    """Cheap Linux process-tree progress receipt used only for diagnostics."""
    pending = [int(root_pid)]
    seen: set[int] = set()
    processes: list[tuple[int, str]] = []
    cpu_ticks = read_chars = write_chars = 0
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
                stat = fh.read()
            fields = stat[stat.rfind(")") + 2:].split()
            state = fields[0]
            cpu_ticks += int(fields[11]) + int(fields[12])
            processes.append((pid, state))
        except (OSError, ValueError, IndexError):
            continue
        try:
            with open(f"/proc/{pid}/task/{pid}/children", encoding="utf-8") as fh:
                pending.extend(int(value) for value in fh.read().split())
        except (OSError, ValueError):
            pass
        try:
            with open(f"/proc/{pid}/io", encoding="utf-8") as fh:
                io_values = {
                    key: int(value.strip())
                    for key, value in (line.split(":", 1) for line in fh)
                }
            # rchar/wchar include pipes and sockets as well as disk. That is
            # the useful signal for compilers, emulators, and provider streams.
            read_chars += io_values.get("rchar", 0)
            write_chars += io_values.get("wchar", 0)
        except (OSError, ValueError):
            pass
    processes.sort()
    return {
        "signature": (tuple(processes), cpu_ticks, read_chars, write_chars),
        "pids": [pid for pid, _state in processes],
        "processes": len(processes),
        "children": max(0, len(processes) - 1),
        "cpu_ticks": cpu_ticks,
        "read_chars": read_chars,
        "write_chars": write_chars,
    }


def lease_holder_pid(path: str) -> int | None:
    """Return the kernel-reported advisory lock owner for a lease file."""
    try:
        value = os.stat(path)
        lock_key = (
            f"{os.major(value.st_dev):02x}:{os.minor(value.st_dev):02x}:"
            f"{value.st_ino}"
        )
        with open("/proc/locks", encoding="utf-8") as locks:
            for line in locks:
                fields = line.split()
                if len(fields) >= 6 and fields[5] == lock_key:
                    return int(fields[4])
    except OSError:
        pass
    return None


def acquire_instance_lock(sock_path: str) -> int | None:
    """Hold one daemon per socket path for the daemon's entire lifetime."""
    lock_path = f"{sock_path}.lock"
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock_fd)
        return None
    return lock_fd


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
        self._pending_config_tasks: dict[str, asyncio.Task] = {}
        # Consuming calls can finish just as their Unix socket drops. Cache a
        # bounded set of tokenized results so the MCP front-end can reconnect
        # and retry without losing poll deltas or a delivered watch terminal.
        self._delivery_cache: OrderedDict[tuple[str, str, str], dict] = OrderedDict()
        self._code_signature = common.agentd_code_signature()
        self._auto_restart_requested = False
        self._shutting_down = False

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
                    if kind == acp_bridge.EV_PERMISSION:
                        resolution = self._auto_answer_pending_permission(agent)
                        if resolution is not None:
                            print(
                                "reasonix permission auto-resolved after mode change: "
                                f"session={agent.session_id} option={resolution}",
                                file=sys.stderr,
                                flush=True,
                            )
                            # Already answered: do not wake a watch or open a
                            # stale elicitation form for this request.
                            return
                    event = common.build_agent_event(agent, kind, payload)
                    self._loop.call_soon_threadsafe(
                        self._wake_watchers, agent.session_id, kind
                    )
                    if kind == acp_bridge.EV_TURN_END:
                        self._persist_lifecycle(agent, "idle")
                        self._loop.call_soon_threadsafe(self._schedule_idle_cleanup, agent)
                        self._loop.call_soon_threadsafe(
                            self._schedule_pending_config, agent
                        )
                    elif kind == acp_bridge.EV_STATUS and agent.pending_config:
                        self._loop.call_soon_threadsafe(
                            self._schedule_pending_config, agent
                        )
                    elif kind == acp_bridge.EV_PROCESS_EXIT:
                        self._persist_lifecycle(
                            agent,
                            "orphaned" if (
                                getattr(agent, "_orphaned", False)
                                or payload.get("error") is not None
                            ) else "exited",
                            error_text=str(payload.get("error_text") or ""),
                        )
                        self._loop.call_soon_threadsafe(self._cancel_idle_cleanup, agent.session_id)
                    if not self._shutting_down:
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
        self._persist_lifecycle(agent, common.agent_status(agent))
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

    @staticmethod
    def _persist_lifecycle(
        agent: acp_bridge.ReasonixAgent, state: str, **extra: object
    ) -> None:
        common.write_session_lifecycle(str(agent.session_id), {
            "state": state,
            "owner_id": getattr(agent, "owner_id", ""),
            "cwd": getattr(agent, "cwd", ""),
            "task": getattr(agent, "task", ""),
            "keep_alive": bool(getattr(agent, "keep_alive", False)),
            "idle_timeout": getattr(agent, "idle_timeout", common.DEFAULT_IDLE_TIMEOUT),
            **extra,
        })

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
            "runtime_stalled",
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

    def _schedule_pending_config(self, agent: acp_bridge.ReasonixAgent) -> None:
        if not getattr(agent, "pending_config", None):
            return
        current = self._pending_config_tasks.get(agent.session_id)
        if current is not None and not current.done():
            return
        self._pending_config_tasks[agent.session_id] = asyncio.create_task(
            self._apply_pending_config(agent)
        )

    @staticmethod
    def _permission_allow_once_option(params: dict) -> str | None:
        """Find a non-persistent affirmative option without guessing."""
        ranked: list[tuple[int, str]] = []
        for option in params.get("options", []):
            if not isinstance(option, dict) or not option.get("optionId"):
                continue
            option_id = str(option["optionId"])
            values = {
                str(value).lower().replace("-", "_").replace(" ", "_")
                for value in (option_id, option.get("name") or "")
            }
            if "allow_once" in values or "approve_once" in values:
                rank = 0
            elif any(
                ("allow" in value or "approve" in value) and "once" in value
                for value in values
            ):
                rank = 1
            elif values & {"allow", "approve", "yes"}:
                rank = 2
            else:
                continue
            ranked.append((rank, option_id))
        return min(ranked)[1] if ranked else None

    @classmethod
    def _auto_answer_pending_permission(
        cls, agent: acp_bridge.ReasonixAgent
    ) -> str | None:
        """Emulate queued YOLO for tool gates until the active turn ends.

        Explicit agent questions (kind=other) always remain user decisions.
        """
        if (getattr(agent, "pending_config", {}) or {}).get("tool_approval") != "yolo":
            return None
        pending = getattr(agent, "_pending_permission", None)
        if pending is None:
            return None
        _request_id, params = pending
        tool_call = params.get("toolCall") or {}
        if str(tool_call.get("kind") or "").lower() == "other":
            return None
        option_id = cls._permission_allow_once_option(params)
        if option_id is None:
            return None
        return option_id if agent.answer_permission(option_id) else None

    async def _apply_pending_config(self, agent: acp_bridge.ReasonixAgent) -> None:
        task = asyncio.current_task()
        try:
            while agent.pending_config and common.agent_status(agent) not in ("exited", "orphaned"):
                if agent.active_turn:
                    return
                requested = dict(agent.pending_config)
                try:
                    result = await asyncio.to_thread(agent.configure, requested)
                except Exception as exc:
                    if (
                        isinstance(exc, acp_bridge.ACPError)
                        and "active work or background jobs" in exc.message
                    ):
                        agent.pending_config_error = None
                        return
                    agent.pending_config_error = str(exc)
                    print(
                        f"reasonix agent config apply failed: session={agent.session_id} "
                        f"error={exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return
                for key, value in requested.items():
                    if agent.pending_config.get(key) == value:
                        agent.pending_config.pop(key, None)
                agent.pending_config_error = None
                # A newer configure call can arrive while this blocking ACP
                # rebuild is in flight. Persist its desired values over the
                # just-applied snapshot so a crash cannot resurrect the older
                # choice.
                persisted = dict(result.get("config") or requested)
                persisted.update(agent.pending_config)
                common.write_session_config(
                    agent.session_id, persisted, replace=True
                )
                print(
                    f"reasonix agent config applied: session={agent.session_id} "
                    f"options={sorted(requested)}",
                    file=sys.stderr,
                    flush=True,
                )
        finally:
            if self._pending_config_tasks.get(agent.session_id) is task:
                self._pending_config_tasks.pop(agent.session_id, None)

    async def _flush_pending_config(self, agent: acp_bridge.ReasonixAgent) -> None:
        if not agent.pending_config:
            return
        self._schedule_pending_config(agent)
        task = self._pending_config_tasks.get(agent.session_id)
        if task is not None:
            await asyncio.shield(task)
        if agent.pending_config:
            detail = agent.pending_config_error or "Reasonix still has active background work"
            raise AgentdError(
                f"pending configuration was not applied before the next turn: {detail}"
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

    def _start_agent_turn(self, agent: acp_bridge.ReasonixAgent, message: str) -> None:
        """Start a turn, injecting the progress contract once per ACP session."""
        inject = not bool(getattr(agent, "status_protocol_injected", False))
        agent.start_turn(common.task_with_status_protocol(message) if inject else message)
        if inject:
            agent.status_protocol_injected = True
        self._persist_lifecycle(agent, "running")

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
        try:
            language = common.normalize_language(common.DEFAULT_LANGUAGE)
        except ValueError as exc:
            raise AgentdError(str(exc)) from exc

        agent = acp_bridge.ReasonixAgent(
            cwd=cwd,
            model=params.get("model", common.DEFAULT_MODEL),
            work_mode=work_mode,
            tool_approval=tool_approval,
            effort=params.get("effort", common.DEFAULT_EFFORT),
            extra_env={"REASONIX_LANG": language},
        )
        agent.task = task
        agent.cwd = cwd
        agent.owner_id = owner_id
        common.write_session_owner(agent.session_id, owner_id)
        agent.keep_alive = keep_alive
        agent.idle_timeout = idle_timeout
        self._register(agent)
        common.write_session_config(
            agent.session_id, agent.config_values, replace=True
        )
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
            "config": dict(agent.config_values),
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
        requested_config = self._config_updates(params, require_any=False)
        if existing is not None and common.agent_status(existing) not in ("exited", "orphaned"):
            # Resume is a recovery operation, but callers can race daemon
            # reconnect/list recovery and invoke it for a process that never
            # died. Treat that as success instead of opening a second ACP
            # process, which Reasonix correctly rejects as "session in use".
            result = {
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
            if requested_config:
                existing.pending_config.update(requested_config)
                self._persist_config_request(session_id, requested_config)
                self._schedule_pending_config(existing)
                result["config_change"] = {
                    "queued": bool(existing.active_turn),
                    "requested": requested_config,
                    "note": "configuration will apply after the active turn" if existing.active_turn else "configuration apply scheduled",
                }
            return result
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
            metadata = common.read_acp_session_metadata(session_id)
            lifecycle = common.read_session_lifecycle(session_id)
            cwd = (
                getattr(existing, "cwd", None)
                or lifecycle.get("cwd")
                or metadata.get("cwd")
                or os.getcwd()
            )
        saved_config = common.read_session_config(session_id)
        if "model" in requested_config and "effort" not in requested_config:
            saved_config.pop("effort", None)
        saved_config.update(requested_config)
        try:
            language = common.normalize_language(common.DEFAULT_LANGUAGE)
        except ValueError as exc:
            raise AgentdError(str(exc)) from exc
        agent = acp_bridge.ReasonixAgent(
            cwd=cwd,
            resume_session_id=session_id,
            model=saved_config.get("model", ""),
            effort=saved_config.get("effort", ""),
            work_mode=saved_config.get("work_mode", ""),
            tool_approval=saved_config.get("tool_approval", ""),
            extra_env={"REASONIX_LANG": language},
        )
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
        common.write_session_config(session_id, agent.config_values, replace=True)
        result = {
            "session_id": session_id,
            "status": common.agent_status(agent),
            "cwd": cwd,
            "already_live": False,
            "resumed": True,
            "config": dict(agent.config_values),
            "note": "session resumed from its persisted transcript; send/poll work as before",
        }
        notes = getattr(agent, "spawn_notes", None) or {}
        if notes.get("skipped_options"):
            result["skipped_options"] = notes["skipped_options"]
        return result

    async def resume_fleet(self, params: dict) -> dict:
        """Resume persisted sessions with bounded startup concurrency."""
        session_ids = list(dict.fromkeys(str(sid) for sid in params.get("session_ids", [])))
        if not session_ids:
            raise AgentdError("session_ids must not be empty")
        if len(session_ids) > common.MAX_WATCH_SESSIONS:
            raise AgentdError(f"too many sessions (max {common.MAX_WATCH_SESSIONS})")
        try:
            parallelism = int(params.get("parallelism", 4))
        except (TypeError, ValueError):
            raise AgentdError("parallelism must be an integer") from None
        parallelism = max(1, min(parallelism, 8, len(session_ids)))
        continue_interrupted = params.get("continue_interrupted", True)
        if not isinstance(continue_interrupted, bool):
            raise AgentdError("continue_interrupted must be a boolean")
        semaphore = asyncio.Semaphore(parallelism)

        async def one(session_id: str) -> tuple[str, dict]:
            prior_state = common.read_session_lifecycle(session_id).get("state")
            child_params = dict(params)
            child_params.pop("session_ids", None)
            child_params.pop("parallelism", None)
            child_params.pop("continue_interrupted", None)
            child_params["session_id"] = session_id
            async with semaphore:
                try:
                    result = await asyncio.to_thread(self.resume, child_params)
                    if (
                        continue_interrupted
                        and result.get("resumed")
                        and prior_state in ("running", "interrupted", "orphaned")
                    ):
                        resumed_agent = self._get(
                            session_id, str(params.get("owner_id") or "")
                        )
                        self._start_agent_turn(
                            resumed_agent,
                            "Continue the interrupted task from the persisted session history. "
                            "Verify the current repository state before repeating or changing work.",
                        )
                        result["continued_interrupted_turn"] = True
                        result["status"] = "running"
                except Exception as exc:
                    result = {
                        "session_id": session_id,
                        "status": "failed",
                        "error": str(exc),
                    }
                return session_id, result

        pairs = await asyncio.gather(*(one(session_id) for session_id in session_ids))
        results = {session_id: result for session_id, result in pairs}
        return {
            "requested": len(session_ids),
            "resumed": [sid for sid, result in pairs if result.get("resumed")],
            "already_live": [sid for sid, result in pairs if result.get("already_live")],
            "failed": [sid for sid, result in pairs if result.get("status") == "failed"],
            "results": results,
            "watch_note": (
                "Interrupted sessions were continued automatically; call "
                "reasonix_watch once for the running resumed fleet. Idle or "
                "explicitly stopped sessions wait for reasonix_send."
            ),
        }

    @staticmethod
    def _config_updates(params: dict, *, require_any: bool = True) -> dict[str, str]:
        updates = {
            key: str(params.get(key) or "").strip()
            for key in ("model", "effort", "work_mode", "tool_approval")
            if str(params.get(key) or "").strip()
        }
        if require_any and not updates:
            raise AgentdError("provide at least one of: model, effort, work_mode, tool_approval")
        if updates.get("work_mode") not in (None, "economy", "balanced", "delivery"):
            raise AgentdError("work_mode must be one of: economy, balanced, delivery")
        if updates.get("tool_approval") not in (None, "ask", "auto", "yolo"):
            raise AgentdError("tool_approval must be one of: ask, auto, yolo")
        return updates

    @staticmethod
    def _persist_config_request(
        session_id: str, updates: dict[str, str]
    ) -> dict[str, str]:
        durable = dict(updates)
        if "model" in durable and "effort" not in durable:
            # A model-only ACP switch adopts the new model's default effort.
            # Clear the prior model's effort so crash/resume behaves the same.
            durable["effort"] = ""
        return common.write_session_config(session_id, durable)

    async def configure(self, params: dict) -> dict:
        session_id = params.get("session_id", "")
        owner_id = str(params.get("owner_id") or "")
        updates = self._config_updates(params)
        with self._lock:
            agent = self._registry.get(session_id)
        if agent is None:
            persisted = os.path.join(
                common.reasonix_home(), "sessions", f"{session_id}.jsonl"
            )
            if not os.path.isfile(persisted) or common.read_session_owner(session_id) != owner_id:
                raise AgentdError(
                    f"unknown session_id {session_id!r} — did you call reasonix_spawn first?"
                )
            self._persist_config_request(session_id, updates)
            return {
                "session_id": session_id,
                "status": "exited",
                "queued": True,
                "applies": "on_resume",
                "requested": updates,
                "note": "configuration saved; call reasonix_resume to start the stopped agent",
            }
        if getattr(agent, "owner_id", "") != owner_id:
            raise AgentdError(
                f"unknown session_id {session_id!r} — did you call reasonix_spawn first?"
            )
        status = common.agent_status(agent)
        self._persist_config_request(session_id, updates)
        if status in ("exited", "orphaned"):
            agent.pending_config.update(updates)
            return {
                "session_id": session_id,
                "status": status,
                "queued": True,
                "applies": "on_resume",
                "requested": updates,
                "note": "configuration saved; call reasonix_resume to start the stopped agent",
            }
        if agent.active_turn:
            agent.pending_config.update(updates)
            agent.pending_config_error = None
            permission_resolution = self._auto_answer_pending_permission(agent)
            permission_still_pending = (
                getattr(agent, "_pending_permission", None) is not None
            )
            note = (
                "the current turn keeps its configuration; changes apply "
                "before the next turn"
            )
            if permission_resolution is not None:
                note = (
                    "pending tool permission allowed once; YOLO is emulated for "
                    "further tool gates in this turn and applies durably before "
                    "the next turn"
                )
            elif permission_still_pending and updates.get("tool_approval") in (
                "auto", "yolo"
            ):
                note = (
                    "configuration queued, but the pending request still needs "
                    "an explicit answer (agent questions and unknown/auto-policy "
                    "decisions are never guessed)"
                )
            result = {
                "session_id": session_id,
                "status": status,
                "queued": True,
                "applies": "after_current_turn",
                "requested": updates,
                "note": note,
            }
            if "tool_approval" in updates:
                result["permission_resolution"] = permission_resolution
                result["permission_still_pending"] = permission_still_pending
            return result
        # Mark the update pending before yielding to a worker thread. A
        # simultaneous reasonix_send will then wait instead of starting a turn
        # on the old model while the ACP session is rebuilding.
        agent.pending_config.update(updates)
        agent.pending_config_error = None
        self._schedule_pending_config(agent)
        task = self._pending_config_tasks.get(session_id)
        if task is not None:
            await asyncio.shield(task)
        if agent.pending_config:
            if agent.pending_config_error:
                raise AgentdError(
                    f"configuration was not applied: {agent.pending_config_error}"
                )
            return {
                "session_id": session_id,
                "status": common.agent_status(agent),
                "queued": True,
                "applies": "after_background_work",
                "requested": updates,
                "note": "configuration queued until Reasonix background work is idle",
            }
        return {
            "session_id": session_id,
            "status": common.agent_status(agent),
            "queued": False,
            "applied": {
                key: value
                for key, value in updates.items()
                if agent.config_values.get(key) == value
            },
            "config": dict(agent.config_values),
        }

    async def send(self, params: dict) -> dict:
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
        if not agent.active_turn:
            # A completion watch can return before the queued model rebuild
            # finishes. Never let the next turn race ahead on the old model.
            await self._flush_pending_config(agent)
        if expect == "steer" and not agent.active_turn:
            raise AgentdError(f"agent {session_id} has no active turn — cannot steer (expect='steer')")
        if expect == "new_turn" and agent.active_turn:
            raise AgentdError(f"agent {session_id} has an active turn — would steer, but expect='new_turn'")

        if expect == "new_turn":
            try:
                self._start_agent_turn(agent, message)
            except acp_bridge.ACPError as exc:
                raise AgentdError(
                    f"agent {session_id} became active — cannot start a new turn",
                    exc.code,
                ) from exc
            return {
                "session_id": session_id,
                "delivered": "new_turn",
                "note": "started a new turn with the message",
            }

        delivered = "steered"
        note = "queued as mid-turn guidance"
        for _ in range(3):
            try:
                await asyncio.to_thread(agent.steer, message)
                break
            except acp_bridge.ACPError as e:
                if e.code != acp_bridge.ERR_INVALID_REQUEST:
                    raise AgentdError(e.message, e.code) from e
                if expect == "steer":
                    raise AgentdError(
                        f"agent {session_id} turn ended before steering; "
                        "expect='steer' forbids starting a follow-up turn"
                    ) from e
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
        return {"session_id": session_id, "delivered": delivered, "note": note}

    def poll(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        cached = self._cached_delivery("poll", params)
        if cached is not None:
            return cached
        detailed = common.shape_poll(
            agent,
            include_events=params.get("include_events"),
            exclude_events=params.get("exclude_events"),
            include_thought=params.get("include_thought", common.DEFAULT_INCLUDE_THOUGHT),
            include_full=params.get("include_full", common.DEFAULT_INCLUDE_FULL),
            max_events=params.get("max_events"),
        )
        verbose = bool(
            params.get("detail", False)
            or params.get("include_events") is not None
            or params.get("exclude_events") is not None
            or params.get("include_thought", common.DEFAULT_INCLUDE_THOUGHT)
            or params.get("include_full", common.DEFAULT_INCLUDE_FULL)
            or params.get("max_events") is not None
        )
        result = detailed if verbose else self._compact_poll_result(detailed)
        if (
            result.get("status") not in ("running", "blocked", "stalled")
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
        if result.get("status") == "stalled" and (
            params.get("_watch_delivery")
            or agent.session_id not in self._active_watch_sessions
        ):
            agent._runtime_alert_observed = True
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
                    "stop_reason": (
                        a.stop_reason
                        if status not in ("running", "blocked", "stalled") else None
                    ),
                    "health": dict(getattr(a, "runtime_health", {}) or {}),
                    "plan": list(getattr(a, "plan_entries", []) or []),
                    "current_work": (
                        dict(getattr(a, "current_work", None))
                        if getattr(a, "current_work", None) else None
                    ),
                }
                if getattr(a, "last_error", None) is not None:
                    states[sid]["error"] = a.last_error
                    states[sid]["error_text"] = a.last_error_text
                if has_new or pending or status == "stalled" or status not in (
                    "running", "blocked"
                ):
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
            if status == "stalled" and not bool(
                getattr(agent, "_runtime_alert_observed", False)
            ):
                return True
            terminal = status == "exited" or (
                status not in ("running", "blocked", "stalled")
                and bool(agent.stop_reason)
            )
            return terminal and not bool(getattr(agent, "_terminal_observed", False))

        while True:
            woke = [sid for sid, agent in agents.items() if actionable(agent)]
            if woke:
                detailed = {
                    sid: self.poll({
                        "session_id": sid,
                        "owner_id": owner_id,
                        "_watch_delivery": True,
                        # Watch consumes detail internally before applying its
                        # own terminal-result compaction below.
                        "detail": True,
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
    def _bounded_message(message: str, label: str) -> tuple[str, bool]:
        return common.bounded_orchestrator_message(message, label)

    @classmethod
    def _compact_poll_result(cls, detailed: dict) -> dict:
        """Reduce an ordinary poll to state needed for orchestration."""
        return common.compact_poll_result(detailed)

    @classmethod
    def _compact_watch_result(cls, detailed: dict) -> dict:
        """Reduce a consumed poll result to one context-lean outcome."""
        turns = detailed.get("turns") or []
        message = str(turns[-1].get("text") or "") if turns else str(detailed.get("text") or "")
        message, message_truncated = cls._bounded_message(message, "watch message")
        result = {
            "session_id": detailed.get("session_id"),
            "status": detailed.get("status"),
            "stop_reason": detailed.get("stop_reason"),
            "message": message,
            "message_truncated": message_truncated,
            "permission_request": detailed.get("permission_request"),
            "plan": detailed.get("plan") or [],
            "current_work": detailed.get("current_work"),
            "config": detailed.get("config") or {},
            "pending_config": detailed.get("pending_config") or {},
            "pending_config_error": detailed.get("pending_config_error"),
            "health": detailed.get("health") or {},
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
        detail = bool(params.get("detail", False))
        include_permission_detail = bool(params.get("include_permission_detail", False))
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
                status not in ("running", "blocked", "stalled")
                and bool(a.stop_reason)
                and not bool(getattr(a, "_terminal_observed", False))
            )
            if pending_only and not (
                status in ("running", "blocked", "stalled")
                or a._pending_permission is not None
                or unobserved_terminal
            ):
                continue
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{sid}.jsonl")
            pending_permission = getattr(a, "_pending_permission", None)
            if include_permission_detail and pending_permission is not None:
                request_id, request_params = pending_permission
                permission_request: bool | dict = {
                    "request_id": request_id,
                    "tool_call": request_params.get("toolCall", {}),
                    "options": request_params.get("options", []),
                }
            else:
                permission_request = pending_permission is not None
            session: dict = {
                "session_id": sid,
                "status": status,
                "cwd": getattr(a, "cwd", None),
                "task": (
                    getattr(a, "task", None)
                    if include_task else common.task_preview(getattr(a, "task", None))
                ),
                "permission_request": permission_request,
                "process_alive": status not in ("exited", "orphaned"),
                "resumable": status in ("idle", "exited", "orphaned") and os.path.isfile(persisted),
            }
            if a.stop_reason:
                session["stop_reason"] = a.stop_reason
            if status in ("blocked", "stalled"):
                session["health"] = dict(getattr(a, "runtime_health", {}) or {})
            pending_config = dict(getattr(a, "pending_config", {}) or {})
            pending_config_error = getattr(a, "pending_config_error", None)
            if pending_config:
                session["pending_config"] = pending_config
            if pending_config_error:
                session["pending_config_error"] = pending_config_error
            if detail:
                session.update({
                    "transcript_path": getattr(a, "transcript_path", None),
                    "plan": list(getattr(a, "plan_entries", []) or []),
                    "current_work": (
                        dict(getattr(a, "current_work", None))
                        if getattr(a, "current_work", None) else None
                    ),
                    "config": dict(getattr(a, "config_values", {}) or {}),
                    "health": dict(getattr(a, "runtime_health", {}) or {}),
                })
            if getattr(a, "last_error", None) is not None:
                if detail:
                    session["error"] = a.last_error
                session["error_text"] = common.task_preview(a.last_error_text, 240)
            sessions.append(session)
        live_ids = {sid for sid, _ in items}
        for sid in sorted(common.load_owner_sessions(owner_id) - live_ids):
            lifecycle = common.read_session_lifecycle(sid)
            if lifecycle.get("owner_id") != owner_id:
                continue
            if lifecycle.get("state") not in ("running", "interrupted", "orphaned"):
                continue
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{sid}.jsonl")
            if not os.path.isfile(persisted):
                continue
            metadata = common.read_acp_session_metadata(sid)
            task = lifecycle.get("task") or metadata.get("title") or f"Session {sid[:8]}"
            session = {
                "session_id": sid,
                "status": "orphaned",
                "cwd": lifecycle.get("cwd") or metadata.get("cwd"),
                "task": task if include_task else common.task_preview(task),
                "stop_reason": "agentd_lost_process",
                "permission_request": False,
                "process_alive": False,
                "resumable": True,
                "recovered_from_disk": True,
                "error_text": lifecycle.get("error_text") or (
                    "agent process is not owned by the current daemon; "
                    "resume it from the persisted transcript"
                ),
            }
            if detail:
                session.update({
                    "transcript_path": persisted,
                    "plan": [],
                    "current_work": None,
                    "config": (
                        common.read_session_config(sid)
                        or common.read_acp_session_config(sid)
                    ),
                    "pending_config": {},
                    "pending_config_error": None,
                    "health": {},
                })
            sessions.append(session)
        counts: dict[str, int] = {}
        for session in sessions:
            value = str(session.get("status") or "unknown")
            counts[value] = counts.get(value, 0) + 1
        result = {
            "count": len(sessions),
            "counts": counts,
            "sessions": sessions,
        }
        if not detail:
            result["detail_available"] = True
            result["detail_note"] = (
                "Set detail=true only for fleet-wide diagnostics; use poll for "
                "one agent's plan/current work."
            )
        return result

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

    async def stop(self, params: dict) -> dict:
        agent = self._get(params["session_id"], str(params.get("owner_id") or ""))
        self._cancel_idle_cleanup(params["session_id"])
        config_task = self._pending_config_tasks.pop(params["session_id"], None)
        if config_task is not None and not config_task.done():
            config_task.cancel()
        agent.cancel_turn()
        await asyncio.to_thread(agent.close)
        self._persist_lifecycle(agent, "stopped")
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
            common.agent_status(agent) in ("exited", "orphaned")
            or (
                not agent.active_turn
                and not getattr(agent, "pending_config", None)
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

    def _update_runtime_health(
        self, agents: list[acp_bridge.ReasonixAgent], now: float
    ) -> None:
        """Classify live turns from protocol, process-tree, and lease progress.

        A quiet ACP pipe is normal while a child tool or model request runs, so
        it is never sufficient evidence by itself. Conversely, a live ACP PID
        is not proof that its turn can advance. Reasonix serializes turns by
        real workspace; expose that queue explicitly and reserve ``stalled``
        for a holder with neither protocol nor process-tree progress.
        """
        active = [
            agent for agent in agents
            if agent.active_turn and agent.status != "exited"
        ]
        snapshots: dict[object, dict] = {}
        lease_paths: dict[object, str] = {}
        for agent in active:
            snapshot = process_tree_snapshot(agent.proc.pid)
            snapshots[agent] = snapshot
            lease_path = workspace_lease_path(getattr(agent, "cwd", ""))
            lease_paths[agent] = lease_path
        holder_pids = {
            path: lease_holder_pid(path) for path in set(lease_paths.values())
        }
        holders: dict[str, acp_bridge.ReasonixAgent] = {}
        for path, holder_pid in holder_pids.items():
            if holder_pid is None:
                continue
            holder = next((
                agent for agent, snapshot in snapshots.items()
                if holder_pid in snapshot["pids"]
            ), None)
            if holder is not None:
                holders[path] = holder

        for agent in agents:
            previous = getattr(agent, "runtime_status", "running")
            if agent not in snapshots:
                agent.runtime_status = "idle" if agent.status != "exited" else "exited"
                agent.runtime_health = {}
                agent._runtime_alert_observed = False
                continue

            snapshot = snapshots[agent]
            signature = snapshot["signature"]
            protocol_at = float(getattr(agent, "_last_protocol_activity_at", now))
            progress_at = float(getattr(agent, "_last_runtime_progress_at", now))
            previous_signature = getattr(agent, "_runtime_signature", None)
            previous_had_children = bool(
                previous_signature and len(previous_signature[0]) > 1
            )
            # ACP leader CPU alone can be Go GC/keepalive bookkeeping and did
            # exactly mask the reported wedge. Only a changing tool subtree,
            # or bytes received on ACP stdout above, proves turn progress.
            if signature != previous_signature and (
                snapshot["children"] > 0 or previous_had_children
            ):
                progress_at = now
            agent._runtime_signature = signature
            progress_at = max(progress_at, protocol_at)
            agent._last_runtime_progress_at = progress_at
            protocol_idle = max(0.0, now - protocol_at)
            runtime_idle = max(0.0, now - progress_at)
            lease_path = lease_paths[agent]
            holder = holders.get(lease_path)
            holder_pid = holder_pids.get(lease_path)

            status = "running"
            reason = None
            if (
                holder_pid is not None
                and holder is not agent
                and protocol_idle >= WORKSPACE_BLOCKED_AFTER
            ):
                status = "blocked"
                reason = "workspace_lease"
            elif (
                protocol_idle >= RUNTIME_STALLED_AFTER
                and runtime_idle >= RUNTIME_STALLED_AFTER
            ):
                status = "stalled"
                reason = "no_protocol_or_process_progress"

            health = {
                "reason": reason,
                "protocol_idle_seconds": round(protocol_idle, 1),
                "runtime_idle_seconds": round(runtime_idle, 1),
                "process_alive": bool(snapshot["processes"]),
                "child_processes": snapshot["children"],
            }
            if holder is not None:
                health["workspace_lease_holder"] = holder.session_id
            elif holder_pid is not None:
                health["workspace_lease_holder_pid"] = holder_pid
            if status == "blocked":
                health["note"] = (
                    "queued behind another Reasonix turn in the same real "
                    "workspace; use separate worktrees/cwd values for parallel tools"
                )
            elif status == "stalled":
                health["note"] = (
                    "ACP process is alive but neither its protocol nor process tree "
                    "has advanced; inspect or stop/resume this session"
                )
            agent.runtime_status = status
            agent.runtime_health = health

            if status != previous:
                print(
                    "reasonix agentd runtime: "
                    f"session={agent.session_id} status={status} "
                    f"reason={reason or '-'} holder={health.get('workspace_lease_holder', '-')}",
                    file=sys.stderr,
                    flush=True,
                )
                if self._loop is not None and not self._shutting_down:
                    asyncio.create_task(self._broadcast({
                        "session_id": agent.session_id,
                        "event": "runtime_status",
                        "status": status,
                        "health": dict(health),
                        "note": (
                            "agent runtime health changed; reasonix_watch wakes "
                            "only for stalled, not ordinary workspace queueing"
                        ),
                    }, getattr(agent, "owner_id", "")))
            if status == "stalled" and previous != "stalled":
                agent._runtime_alert_observed = False
                self._wake_watchers(str(agent.session_id), "runtime_stalled")
                self._wake_waiters(str(agent.session_id))
            elif status != "stalled":
                agent._runtime_alert_observed = False

    async def _watch_processes(self) -> None:
        """Detect dead ACP leaders and report whether live turns can advance."""
        last_heartbeat = time.monotonic()
        while True:
            await asyncio.sleep(PROCESS_WATCH_INTERVAL)
            with self._lock:
                agents = list(self._registry.values())
            for agent in agents:
                try:
                    previous = agent.status
                    alive = agent.reconcile_process_liveness()
                    if not alive and previous != "exited":
                        print(
                            "reasonix agentd: reaped dead ACP process; "
                            f"session={agent.session_id} pid={agent.proc.pid} resumable=true",
                            file=sys.stderr,
                            flush=True,
                        )
                except Exception as exc:
                    print(
                        f"reasonix agentd: process watchdog error: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
            now = time.monotonic()
            try:
                self._update_runtime_health(agents, now)
            except Exception as exc:
                print(
                    f"reasonix agentd: runtime health error: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                statuses = [common.agent_status(agent) for agent in agents]
                print(
                    "reasonix agentd heartbeat: "
                    f"sessions={len(agents)} running={statuses.count('running')} "
                    f"blocked={statuses.count('blocked')} "
                    f"stalled={statuses.count('stalled')} "
                    f"idle={statuses.count('idle')} orphaned={statuses.count('orphaned')}",
                    file=sys.stderr,
                    flush=True,
                )
                last_heartbeat = now

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
            and common.agent_status(agent) not in ("exited", "orphaned")
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
            if common.agent_status(agent) not in ("exited", "orphaned")
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
        self._shutting_down = True
        for session_id in list(self._idle_cleanup_tasks):
            self._cancel_idle_cleanup(session_id)
        for task in self._pending_config_tasks.values():
            if not task.done():
                task.cancel()
        self._pending_config_tasks.clear()
        with self._lock:
            agents = list(self._registry.values())
            self._registry.clear()
        for a in agents:
            lifecycle_state = "interrupted" if a.active_turn else "stopped"
            # Checkpoint before close because an already degraded daemon may
            # itself be killed while waiting for a child to tear down. Write
            # it again afterward because the process-exit callback records an
            # intermediate exited state during a clean close.
            self._persist_lifecycle(a, lifecycle_state)
            try:
                a.close()
            except Exception:
                pass
            self._persist_lifecycle(a, lifecycle_state)
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
                if method in WORKER_METHODS:
                    result = await asyncio.to_thread(handler, params)
                else:
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
        process_watcher = asyncio.create_task(self._watch_processes())
        async with server:
            print(f"agentd listening on {sock_path}", flush=True)
            await self._serve_stop.wait()
            server.close()
            await server.wait_closed()
        source_watcher.cancel()
        process_watcher.cancel()
        try:
            await asyncio.gather(source_watcher, process_watcher)
        except asyncio.CancelledError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="reasonix-mcp agent daemon")
    ap.add_argument("--sock", default=common.agentd_sock_path())
    # One `reasonix acp` process per agent is deliberate: a crashed session's
    # process can be killed without taking the fleet down (blast-radius).
    args = ap.parse_args()
    instance_lock = acquire_instance_lock(args.sock)
    if instance_lock is None:
        print(
            f"reasonix agentd: another daemon already owns {args.sock}; exiting",
            file=sys.stderr,
            flush=True,
        )
        return
    agentd = Agentd()
    try:
        asyncio.run(agentd.serve(args.sock))
    except KeyboardInterrupt:
        pass
    should_restart = agentd._auto_restart_requested
    os.close(instance_lock)
    if should_restart:
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
