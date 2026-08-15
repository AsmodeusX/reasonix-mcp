"""ACP v1 client that drives a `reasonix acp` subprocess.

Reasonix implements the Agent Client Protocol (https://agentclientprotocol.com)
as NDJSON JSON-RPC 2.0 over stdio. This module owns one subprocess per agent:
it performs the initialize handshake, opens a session, streams turns, relays
mid-turn steering (`_reasonix.io/session/steer`), and answers permission
requests.

Design notes (all verified against reasonix v1.17.20):
- stdout is pure JSON-RPC; diagnostics go to stderr and are never merged.
- Responses are routed by JSON-RPC id, never by line order
  (session/set_config_option rebuilds asynchronously and emits
  session/update notifications before its response arrives).
- session/prompt stays open until the turn ends; its response resolves with
  stopReason end_turn|cancelled|error.
- session/request_permission is an inbound *request* (has an id); the client
  replies with result {"outcome": {"outcome": "selected", "optionId": ...}}
  or {"outcome": {"outcome": "cancelled"}}.
"""

from __future__ import annotations

import itertools
import json
import os
import platform
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Callable

# PR_SET_PDEATHSIG (Linux): arm SIGKILL on parent death so a killed MCP server
# can never orphan agents (observed leaks when the server was SIGKILLed and
# stdin-EOF did not terminate `reasonix acp`). Loaded once at import (before
# any threads) so the child-side preexec makes no allocation.
try:
    import ctypes
    _LIBC_PRCTL = ctypes.CDLL("libc.so.6", use_errno=True).prctl
except Exception:
    _LIBC_PRCTL = None


def _preexec_pdeathsig() -> None:
    """Child-only: die with the MCP server."""
    if _LIBC_PRCTL is not None:
        try:
            _LIBC_PRCTL(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG
        except Exception:
            pass
    if os.getppid() == 1:  # parent died in the fork->prctl window
        os._exit(1)

STEER_METHOD = "_reasonix.io/session/steer"
ERR_INVALID_REQUEST = -32600  # steer called with no active prompt
ERR_INVALID_PARAMS = -32602  # unknown session / empty prompt

# Event kinds pushed to the per-agent queue.
EV_UPDATE = "update"  # payload: session/update notification dict (params.update)
EV_PERMISSION = "permission_request"  # payload: {"id": ..., "params": {...}}
EV_TURN_END = "turn_end"  # payload: {"stopReason": ..., "transcriptPath": ...}
EV_PROCESS_EXIT = "process_exited"  # payload: {"stderr": "...", "code": ...}
EV_STATUS = "agent_status"  # payload: {"plan": ..., "current_work": ...}

# Hours-long turns stream a lot: bound the unpolled event queue (dropping only
# droppable update events — critical ones are never dropped) so an agent that
# runs for a long time without being polled cannot grow memory without bound.
MAX_QUEUED_EVENTS = 4000


def _error_text(error: object) -> str:
    """Extract a useful message without importing the MCP-side helpers."""
    if isinstance(error, str):
        return error
    if isinstance(error, dict):
        if error.get("message"):
            return str(error["message"])
        if error.get("data") is not None:
            return str(error["data"])
    return str(error) if error is not None else ""


class ACPError(Exception):
    """A JSON-RPC error reply from the agent."""

    def __init__(self, code: int, message: str):
        super().__init__(f"reasonix acp error {code}: {message}")
        self.code = code
        self.message = message


def _platform_arch() -> str:
    machine = platform.machine().lower()
    return {"x86_64": "x64", "amd64": "x64", "aarch64": "arm64", "arm64": "arm64"}.get(machine, machine)


def resolve_reasonix_binary(bin_name: str = "reasonix") -> str:
    """Resolve the real Reasonix Go binary, skipping the npm node shim.

    `reasonix` on PATH is usually a node wrapper (bin/reasonix.js) that
    spawnSyncs the prebuilt Go binary from an optionalDependency package
    (@reasonix/cli-<platform>-<arch>). Killing the wrapper leaves the Go
    binary running as an orphan, so we must spawn the Go binary directly.
    An explicit env override (REASONIX_BIN) wins; a path that is already the
    native binary is used as-is.
    """
    override = os.environ.get("REASONIX_BIN")
    candidates: list[str] = []
    if override:
        candidates.append(override)

    resolved = None
    if bin_name and shutil.which(bin_name):
        resolved = os.path.realpath(shutil.which(bin_name))
        candidates.append(resolved)
        # npm layout: .../node_modules/reasonix/bin/reasonix.js  →  sibling
        # optionalDependency package holding the real binary.
        pkg_dir = os.path.dirname(os.path.dirname(resolved))
        if os.path.basename(pkg_dir) == "reasonix" and os.path.basename(os.path.dirname(pkg_dir)) == "node_modules":
            native = os.path.join(
                pkg_dir, "node_modules",
                f"@reasonix/cli-{sys.platform}-{_platform_arch()}", "bin", bin_name,
            )
            candidates.append(native)

    for path in candidates:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            if path.endswith(".js") or path.endswith(".cjs"):
                continue  # node shim, not the native binary
            with open(path, "rb") as fh:
                head = fh.read(64)
            if head.startswith(b"#!"):  # script, not a native binary
                continue
            return path
    raise ACPError(-1, f"could not resolve a native reasonix binary (tried: {candidates}) — set REASONIX_BIN to the Go binary path")


class ReasonixAgent:
    """One live Reasonix agent, exposed as a chat-like session."""

    def __init__(
        self,
        *,
        cwd: str,
        model: str = "",
        work_mode: str = "",
        tool_approval: str = "auto",
        effort: str = "",
        reasonix_bin: str = "reasonix",
        spawn_timeout: float = 60.0,
        extra_env: dict | None = None,
        on_event: Callable[[str, dict], None] | None = None,
        resume_session_id: str | None = None,
    ):
        """
        on_event: optional callback invoked (from the reader thread) when the
        agent emits an orchestrator-relevant event: EV_TURN_END (done/stopped),
        EV_PERMISSION (needs approval), EV_PROCESS_EXIT (process died), or
        EV_STATUS (the plan changed).
        """
        self.session_id: str | None = None
        self.status = "starting"  # starting | idle | running | exited
        self.active_turn = False
        self.stop_reason: str | None = None
        # Last terminal error, retained independently of the event queue so a
        # wait/list response can explain an errored turn after poll consumes it.
        self.last_error: object | None = None
        self.last_error_text = ""

        self._events: queue.Queue = queue.Queue()
        self._qlock = threading.Lock()  # guards _events + full-text snapshots
        self._pending: dict[int, queue.Queue] = {}
        self._ids = itertools.count(1)
        self._wlock = threading.Lock()
        self._config_lock = threading.Lock()
        self._permission_lock = threading.Lock()
        self._stderr_tail: list[str] = []
        self._exit_code: int | None = None
        self._exit_lock = threading.Lock()
        self._exit_reported = False
        self._orphaned = False
        now = time.monotonic()
        self._last_protocol_activity_at = now
        self._last_stdout_at = now
        self._last_stderr_at = now
        self._turn_started_at: float | None = None
        self._last_runtime_progress_at = now
        self._runtime_signature: tuple | None = None
        self.runtime_status = "starting"
        self.runtime_health: dict = {}
        self._runtime_alert_observed = False
        self._closed = False
        self._pending_permission: tuple[int, dict] | None = None
        self.full_text_parts: list[str] = []
        self.full_thought_parts: list[str] = []
        self._full_text_snapshot = ""
        self._full_text_snap_len = 0
        self._full_thought_snapshot = ""
        self._full_thought_snap_len = 0
        self.on_event = on_event
        # Daemon-local wake-up hook for the legacy output-sensitive wait.
        # Unlike on_event, this sees every queued update and is never relayed
        # as an MCP notification.
        self.on_activity: Callable[[str, dict], None] | None = None
        # Orchestrator support: event_seq increments on every queued event (lets
        # reasonix_wait/watch detect "something new" without consuming); turns tracks
        # completed turns with their full text + stop_reason (turn boundaries).
        self.event_seq = 0
        # Set by agentd after a terminal result has been delivered through
        # poll/watch. A later turn or process exit makes it actionable again.
        self._terminal_observed = False
        self.turns: list[dict] = []
        self._turns_served = 0
        self._current_parts: list[str] = []
        # Where the session transcript is on disk (set on first turn end;
        # falls back to <home>/sessions/<id>.jsonl for mid-run queries).
        self.transcript_path: str | None = None
        # The agent's current todo list (last `plan` sessionUpdate), so the
        # orchestrator can see what the agent is doing right now.
        self.plan_entries: list[dict] = []
        self._tool_calls: dict[str, dict] = {}
        self._last_tool_call: dict | None = None
        self.current_work: dict | None = None
        # Owned by agentd: the orchestration progress contract belongs in the
        # logical session history once, not at the start of every turn.
        self.status_protocol_injected = False
        self.config_options: list[dict] = []
        self.config_values: dict[str, str] = {}
        self.pending_config: dict[str, str] = {}
        self.pending_config_error: str | None = None

        binary = resolve_reasonix_binary(reasonix_bin)
        env = dict(os.environ)
        if extra_env:
            env.update(extra_env)
        # start_new_session=True: the agent becomes its own session/process
        # group leader, so we can kill the entire tree via killpg without any
        # risk of touching unrelated reasonix processes. preexec arms
        # PDEATHSIG so the agent also dies if THIS server dies uncleanly.
        self.proc = subprocess.Popen(
            [binary, "acp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
            preexec_fn=_preexec_pdeathsig,
            env=env,
        )
        self._pgid = self.proc.pid  # session leader ⇒ pgid == pid
        self._turn_rid: int | None = None
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        self._stderr_reader = threading.Thread(target=self._read_stderr, daemon=True)
        self._stderr_reader.start()

        try:
            self._request("initialize", {
                "protocolVersion": 1,
                "clientInfo": {"name": "reasonix-mcp", "version": "1.0.0"},
                "clientCapabilities": {"fs": {}, "terminal": False},
            }, timeout=spawn_timeout)
            if resume_session_id:
                res = self._request(
                    "session/resume",
                    {"sessionId": resume_session_id, "cwd": cwd},
                    timeout=spawn_timeout,
                )
                self.session_id = resume_session_id
            else:
                res = self._request("session/new", {"cwd": cwd}, timeout=spawn_timeout)
                self.session_id = res["sessionId"]
            self._set_config_state((res or {}).get("configOptions") or [])
            self.spawn_notes = self.configure(
                {
                    "model": model,
                    "effort": effort,
                    "work_mode": work_mode,
                    "tool_approval": tool_approval,
                },
                timeout=spawn_timeout,
                skip_unadvertised=True,
            )
        except Exception:
            self._terminate()
            raise
        # A process may die during the handshake; do not resurrect that
        # session as idle or suppress the daemon's terminal notification.
        if self.status != "exited":
            self.status = "idle"

    def _set_config_state(self, options: list[dict]) -> None:
        self.config_options = [dict(option) for option in options if isinstance(option, dict)]
        self.config_values = {
            str(option["id"]): str(option["currentValue"])
            for option in self.config_options
            if option.get("id") and option.get("currentValue") is not None
        }

    def configure(
        self,
        updates: dict[str, str],
        *,
        timeout: float = 60.0,
        skip_unadvertised: bool = False,
    ) -> dict:
        """Apply advertised ACP session options in dependency-safe order.

        Model goes first because rebuilding it refreshes the valid effort
        choices. Callers must wait until no turn is active.
        """
        if self.status == "exited":
            raise ACPError(-1, "agent process has exited")
        if self.active_turn:
            raise ACPError(-32600, "cannot switch configuration while a turn is active")
        requested = {
            key: str(value)
            for key, value in updates.items()
            if key in ("model", "effort", "work_mode", "tool_approval") and value
        }
        skipped: list[str] = []
        applied: dict[str, str] = {}
        with self._config_lock:
            known_options = {option.get("id") for option in self.config_options}
            for config_id in ("model", "effort", "work_mode", "tool_approval"):
                value = requested.get(config_id)
                if not value:
                    continue
                if config_id not in known_options:
                    if skip_unadvertised:
                        skipped.append(config_id)
                        continue
                    raise ACPError(
                        -32602,
                        f"session does not advertise config option {config_id!r}",
                    )
                if self.config_values.get(config_id) == value:
                    applied[config_id] = value
                    continue
                result = self._request(
                    "session/set_config_option",
                    {
                        "sessionId": self.session_id,
                        "configId": config_id,
                        "value": value,
                    },
                    timeout=timeout,
                )
                if isinstance(result, dict) and "configOptions" in result:
                    self._set_config_state(result.get("configOptions") or [])
                    known_options = {option.get("id") for option in self.config_options}
                else:
                    self.config_values[config_id] = value
                applied[config_id] = value
        return {
            "applied_options": applied,
            "skipped_options": skipped,
            "config": dict(self.config_values),
        }

    # ------------------------------------------------------------------ wire

    def _read_loop(self) -> None:
        """Parse NDJSON lines; route responses by id, queue updates/requests."""
        try:
            while True:
                line = self.proc.stdout.readline()
                if not line:
                    break
                self._last_stdout_at = time.monotonic()
                self._last_protocol_activity_at = self._last_stdout_at
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rid = msg.get("id")
                if rid is not None and ("result" in msg or "error" in msg):
                    if rid == self._turn_rid:
                        # session/prompt response: the turn ended. Route it to
                        # the event stream so poll() reports the stop reason.
                        self.active_turn = False
                        result = msg.get("result", {})
                        err = msg.get("error")
                        if err is not None:
                            result = {"stopReason": "error", "error": err}
                        self._enqueue(EV_TURN_END, result)
                        continue
                    waiter = self._pending.pop(rid, None)
                    if waiter is not None:
                        waiter.put(msg)
                    continue
                if "method" in msg:
                    method = msg["method"]
                    if rid is not None:  # inbound request (e.g. permission)
                        if method == "session/request_permission":
                            params = msg.get("params", {})
                            with self._permission_lock:
                                self._pending_permission = (rid, params)
                            self._enqueue(EV_PERMISSION, {"id": rid, "params": params})
                        else:
                            # Unknown inbound request: answer method-not-found so
                            # the agent never hangs waiting on us.
                            self._write({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"Method not found: {method}"}})
                    elif method == "session/update":
                        params = msg.get("params") or {}
                        self._enqueue(EV_UPDATE, params.get("update", {}))
        finally:
            self._mark_process_exit(self.proc.poll())

    def _read_stderr(self) -> None:
        for line in self.proc.stderr:
            self._last_stderr_at = time.monotonic()
            self._stderr_tail.append(line.rstrip())
            if len(self._stderr_tail) > 200:
                self._stderr_tail.pop(0)
        self._exit_code = self.proc.poll()

    def _write(self, obj: dict) -> None:
        with self._wlock:
            if self.proc.stdin is None or self.proc.stdin.closed:
                raise ACPError(-1, "agent process stdin is closed")
            self.proc.stdin.write(json.dumps(obj) + "\n")
            self.proc.stdin.flush()

    def _request(self, method: str, params: dict, timeout: float = 30.0) -> dict:
        """Send a request and wait for its response (returns result dict)."""
        rid = next(self._ids)
        waiter: queue.Queue = queue.Queue(maxsize=1)
        # Register before writing. Fast local ACP responses can otherwise land
        # between the write and registration and be discarded, pinning the
        # caller until its timeout.
        with self._wlock:
            self._pending[rid] = waiter
            try:
                if self.proc.stdin is None or self.proc.stdin.closed:
                    raise ACPError(-1, "agent process stdin is closed")
                frame = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
                self.proc.stdin.write(json.dumps(frame) + "\n")
                self.proc.stdin.flush()
            except Exception:
                self._pending.pop(rid, None)
                raise
        try:
            msg = waiter.get(timeout=timeout)
        except queue.Empty:
            raise ACPError(-1, f"timed out waiting for response to {method}") from None
        if "error" in msg and msg["error"] is not None:
            raise ACPError(msg["error"].get("code", -1), msg["error"].get("message", "unknown error"))
        return msg.get("result", {})

    def reconcile_process_liveness(self) -> bool:
        """Return process liveness and publish a missed death immediately.

        A child can exit while one of its descendants still owns the ACP pipe.
        In that case stdout never reaches EOF, so the normal reader-finally
        path cannot update status. Polling Popen independently breaks that
        dependency and killing the now-orphaned process group releases pipes.
        """
        code = self.proc.poll()
        if code is None:
            return True
        self._exit_code = code
        if self.status != "exited":
            self._orphaned = True
            self._kill_orphaned_process_group()
            self._mark_process_exit(code)
        return False

    def _kill_orphaned_process_group(self) -> None:
        try:
            os.killpg(self._pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    def _mark_process_exit(self, code: int | None) -> bool:
        """Record process death once, even if watchdog and reader race."""
        with self._exit_lock:
            if self._exit_reported:
                return False
            self._exit_reported = True
            self._exit_code = code if code is not None else self._exit_code
            was_active = self.active_turn
            stderr = "\n".join(self._stderr_tail[-50:])[-8000:]
            unexpected = was_active or not self._closed
            exit_text = stderr or (
                f"agent process exited with code {self._exit_code}"
                if self._exit_code is not None else "agent process exited unexpectedly"
            )
            payload: dict = {"code": self._exit_code, "stderr": stderr}
            if unexpected:
                payload["error_text"] = exit_text
                self.stop_reason = "error"
                payload["error"] = {
                    "code": self._exit_code if self._exit_code is not None else -1,
                    "message": exit_text,
                }
            self.status = "exited"
            self.active_turn = False
            self.current_work = None
            self._enqueue(EV_PROCESS_EXIT, payload)
            for waiter in list(self._pending.values()):
                waiter.put({"jsonrpc": "2.0", "id": None, "error": {
                    "code": -32603, "message": "agent process exited"
                }})
            self._pending.clear()
            return True

    # ------------------------------------------------------------------ api

    def _enqueue(self, kind: str, payload: dict) -> None:
        """Queue an event, accumulating text and bounding the queue.

        Runs only from the reader thread. Text/thought chunks are accumulated
        into full_text_parts/full_thought_parts (independent of polling, so
        dropping queued update events never loses conversation text). When the
        queue is full, oldest droppable update events are discarded; critical
        events (permission, turn_end, process_exit) are kept.
        """
        status_payload: dict | None = None
        with self._qlock:
            if kind == EV_UPDATE:
                su = payload.get("sessionUpdate")
                content = payload.get("content")
                # Some variants (e.g. tool_call_update) carry content as a LIST.
                if isinstance(content, dict) and content.get("type") == "text":
                    text = content.get("text", "")
                    if su == "agent_message_chunk":
                        self.full_text_parts.append(text)
                        self._current_parts.append(text)
                    elif su == "agent_thought_chunk":
                        self.full_thought_parts.append(text)
                if su == "plan":
                    self.plan_entries = payload.get("entries") or []
                    self._refresh_current_work()
                    status_payload = {
                        "plan": list(self.plan_entries),
                        "current_work": dict(self.current_work) if self.current_work else None,
                    }
                elif su in ("tool_call", "tool_call_update"):
                    tool_call_id = payload.get("toolCallId") or payload.get("tool_call_id")
                    if tool_call_id:
                        key = str(tool_call_id)
                        call = self._tool_calls.setdefault(key, {})
                        call.update(payload)
                        if str(call.get("status", "")).lower() in {
                            "completed", "failed", "cancelled", "declined"
                        }:
                            self._tool_calls.pop(key, None)
                    else:
                        self._last_tool_call = dict(payload)
                    self._refresh_current_work()
                if self._events.qsize() >= MAX_QUEUED_EVENTS:
                    dropped = 0
                    while self._events.qsize() >= MAX_QUEUED_EVENTS and dropped < MAX_QUEUED_EVENTS:
                        try:
                            k, p = self._events.get_nowait()
                        except queue.Empty:
                            break
                        if k != EV_UPDATE:
                            self._events.put((k, p))  # re-queue critical at the back
                        else:
                            dropped += 1
            elif kind == EV_TURN_END:
                # A turn finished: record it with its complete text so poll can
                # report turn boundaries (full_text alone concatenates turns).
                turn = {
                    "text": "".join(self._current_parts),
                    "stop_reason": payload.get("stopReason"),
                }
                if payload.get("error") is not None:
                    turn["error"] = payload["error"]
                    turn["error_text"] = _error_text(payload["error"])
                    self.last_error = payload["error"]
                    self.last_error_text = turn["error_text"]
                self.turns.append(turn)
                self._current_parts = []
                self.stop_reason = payload.get("stopReason") or self.stop_reason
                self._terminal_observed = False
                self.current_work = None
                if payload.get("transcriptPath"):
                    self.transcript_path = payload["transcriptPath"]
            elif kind == EV_PROCESS_EXIT:
                # Expected teardown after an already-delivered terminal turn
                # is a lifecycle transition, not a second completion. Keep an
                # unobserved or abnormal exit actionable.
                if payload.get("error") is not None or not self.stop_reason:
                    self._terminal_observed = False
                if payload.get("error") is not None:
                    self.last_error = payload["error"]
                if payload.get("error_text"):
                    self.last_error_text = payload["error_text"]
                if payload.get("error") is not None and not self.stop_reason:
                    self.stop_reason = "error"
            self._events.put((kind, payload))
            self.event_seq += 1
            if self.on_activity is not None:
                try:
                    self.on_activity(kind, payload)
                except Exception:
                    pass
            # Orchestrator callback (best-effort, from the reader thread):
            # push when the agent finishes/stops, needs approval, or dies.
            callback_kind = EV_STATUS if status_payload is not None else kind
            callback_payload = status_payload if status_payload is not None else payload
            if self.on_event is not None and (
                callback_kind in (EV_TURN_END, EV_PERMISSION, EV_PROCESS_EXIT, EV_STATUS)
            ):
                try:
                    self.on_event(callback_kind, callback_payload)
                except Exception:
                    pass

    def _refresh_current_work(self) -> None:
        """Derive a compact, JSON-safe current-work snapshot.

        Called while `_qlock` is held. Native tool-call updates are more
        concrete than a plan step, so they take precedence when active.
        """
        calls = list(self._tool_calls.values())
        if self._last_tool_call is not None:
            calls.append(self._last_tool_call)
        active = [
            call for call in calls
            if str(call.get("status", "in_progress")).lower()
            not in {"completed", "failed", "cancelled", "declined"}
        ]
        plan_step = next(
            (
                entry for entry in reversed(self.plan_entries)
                if str(entry.get("status", "")).lower() in {"in_progress", "working"}
            ),
            None,
        )
        if active:
            call = active[-1]
            title = call.get("title") or call.get("name") or call.get("kind") or "tool call"
            work = {
                "source": "tool_call",
                "status": call.get("status") or "in_progress",
                "summary": str(title),
            }
            if call.get("toolCallId") or call.get("tool_call_id"):
                work["tool_call_id"] = call.get("toolCallId") or call.get("tool_call_id")
            if call.get("kind"):
                work["kind"] = call["kind"]
            if plan_step is not None:
                work["plan_step"] = dict(plan_step)
            self.current_work = work
        elif plan_step is not None:
            content = plan_step.get("content") or plan_step.get("title") or "current plan step"
            self.current_work = {
                "source": "plan",
                "status": plan_step.get("status") or "in_progress",
                "summary": str(content),
                "plan_step": dict(plan_step),
            }
        else:
            self.current_work = None

    def snapshot_turns(self) -> list[dict]:
        """Completed turns since the last call: [{text, stop_reason}, ...]."""
        with self._qlock:
            new = [dict(t) for t in self.turns[self._turns_served:]]
            self._turns_served = len(self.turns)
        return new

    @property
    def full_text(self) -> str:
        """Incremental join of all agent message text so far (amortized O(1)/chunk)."""
        with self._qlock:
            if self._full_text_snap_len < len(self.full_text_parts):
                self._full_text_snapshot += "".join(self.full_text_parts[self._full_text_snap_len:])
                self._full_text_snap_len = len(self.full_text_parts)
            return self._full_text_snapshot

    @property
    def full_thought(self) -> str:
        with self._qlock:
            if self._full_thought_snap_len < len(self.full_thought_parts):
                self._full_thought_snapshot += "".join(self.full_thought_parts[self._full_thought_snap_len:])
                self._full_thought_snap_len = len(self.full_thought_parts)
            return self._full_thought_snapshot

    def start_turn(self, text: str) -> None:
        """Begin a session/prompt turn; returns immediately (non-blocking)."""
        if self.active_turn:
            raise ACPError(-1, "a turn is already active")
        if self.status == "exited":
            raise ACPError(-1, "agent process has exited")
        with self._qlock:
            self._current_parts = []  # a fresh turn's text starts now
            self._tool_calls.clear()
            self._last_tool_call = None
            self.current_work = {"source": "turn", "status": "starting", "summary": "starting turn"}
        rid = next(self._ids)
        self._turn_rid = rid  # set before writing so the reader never races
        self.active_turn = True
        self._turn_started_at = time.monotonic()
        self._last_protocol_activity_at = self._turn_started_at
        self._last_runtime_progress_at = self._turn_started_at
        self._runtime_signature = None
        self.runtime_status = "running"
        self.runtime_health = {}
        self._runtime_alert_observed = False
        self.stop_reason = None
        # Errors belong to the completed turn that produced them. Retaining
        # them on a successful follow-up makes poll/list falsely report that
        # the live continuation is still errored.
        self.last_error = None
        self.last_error_text = ""
        self._terminal_observed = False
        # The turn's response resolves when the turn ends; the reader routes it
        # to the event stream as EV_TURN_END so poll() sees the stop reason.
        self._write({"jsonrpc": "2.0", "id": rid, "method": "session/prompt", "params": {
            "sessionId": self.session_id,
            "prompt": [{"type": "text", "text": text}],
        }})

    def steer(self, text: str) -> dict:
        """Send mid-turn guidance. Returns {} if accepted.

        Raises ACPError(-32600) when no prompt is active — the caller may then
        fall back to starting a new turn with the same text.
        """
        if self.status == "exited":
            raise ACPError(-1, "agent process has exited")
        return self._request(STEER_METHOD, {
            "sessionId": self.session_id,
            "prompt": [{"type": "text", "text": text}],
        }, timeout=15.0)

    def respond_permission(self, request_id: int, option_id: str | None) -> None:
        """Answer a session/request_permission request from the agent."""
        with self._permission_lock:
            if self._pending_permission and self._pending_permission[0] == request_id:
                self._pending_permission = None
        outcome = {"outcome": "cancelled"} if option_id is None else {"outcome": "selected", "optionId": option_id}
        self._write({"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}})

    def answer_permission(self, option_id: str | None) -> bool:
        """Answer the latest pending permission request; False if none pending."""
        with self._permission_lock:
            if self._pending_permission is None:
                return False
            rid, params = self._pending_permission
            if option_id is not None:
                known = {o.get("optionId") for o in params.get("options", [])}
                if option_id not in known:
                    raise ACPError(-32602, f"unknown permission option {option_id!r}; choose from {sorted(known)}")
            # Claim before writing so concurrent elicitation/configuration
            # responders cannot both answer this request.
            self._pending_permission = None
        outcome = {"outcome": "cancelled"} if option_id is None else {"outcome": "selected", "optionId": option_id}
        self._write({"jsonrpc": "2.0", "id": rid, "result": {"outcome": outcome}})
        return True

    def cancel_turn(self) -> None:
        """Cancel the active turn (session/cancel is a notification)."""
        if self.active_turn:
            self._write({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": self.session_id}})
            self.active_turn = False

    def close(self, timeout: float = 10.0) -> None:
        """Close the ACP session and terminate the subprocess."""
        if self._closed:
            return
        self._closed = True
        try:
            if self.status != "exited" and self.session_id:
                self._request("session/close", {"sessionId": self.session_id}, timeout=timeout)
        except Exception:
            pass
        self._terminate()
        # Let the reader see EOF so status flips to "exited" (and the
        # EV_PROCESS_EXIT event is queued) before close() returns.
        self._reader.join(timeout=3.0)

    def _terminate(self) -> None:
        """Kill the agent's whole process group and verify it is gone."""
        try:
            if self.proc.poll() is None:
                os.killpg(self._pgid, signal.SIGTERM)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self._pgid, signal.SIGKILL)
                self.proc.wait(timeout=5)
            else:
                # The ACP leader may be gone while descendants keep its pipe
                # descriptors open. Kill the still-existing process group.
                self._kill_orphaned_process_group()
        except (ProcessLookupError, PermissionError):
            pass  # already dead
        except Exception:
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except Exception:
                pass

    # ------------------------------------------------------------------ poll

    def poll(self) -> list[tuple[str, dict]]:
        """Drain queued events since the last poll (non-blocking)."""
        out: list[tuple[str, dict]] = []
        while True:
            try:
                out.append(self._events.get_nowait())
            except queue.Empty:
                break
        return out
