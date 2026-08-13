#!/usr/bin/env python3
"""MCP server (stdio): front-end for the reasonix-mcp agent daemon.

Any MCP host (Claude Code, Codex, …) launches this script. It proxies every tool
call to `agentd.py` — the detached daemon that actually owns the `reasonix acp`
subprocesses — over a Unix socket, and relays the daemon's agent_event pushes
as MCP notifications.

Because the agents live in the daemon, not in this process, they **survive MCP
server restarts**: close the MCP host, come back, and reasonix_list still shows
the fleet running; reasonix_resume revives sessions whose process died.

Tools:
  reasonix_spawn / reasonix_resume / reasonix_send / reasonix_poll
  reasonix_watch / reasonix_wait / reasonix_list / reasonix_models / reasonix_transcript
  reasonix_respond_permission / reasonix_stop / reasonix_restart_agentd /
  reasonix_restart_mcp_server / reasonix_configure / reasonix_dashboard
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import itertools
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver import Context

import common

MCP_INSTRUCTIONS = """Reasonix is an agent-orchestration MCP server backed by a
detached agent daemon. Use reasonix_spawn once per child task and retain each
session_id. Agents keep running even if this MCP server (or the MCP host —
Claude Code, Codex, …) restarts — reasonix_list shows the fleet, reasonix_resume revives a session
whose process died. Resume is idempotent if recovery races a live process;
optional model/session changes still follow reasonix_configure semantics. For
multiple children keep one reasonix_watch call in
flight with all ids; it returns compact results for completion, permission,
or process death without a follow-up poll. Keep one watch per orchestrator
fleet; a newer overlapping watch safely supersedes an older abandoned call, so
an MCP restart is never required to clear stale watches. Remove terminal ids
and watch the remaining ids again. If
a result contains
permission_request, inspect the tool_call and answer with
reasonix_respond_permission before watching again. tool_approval=ask gates only
commands Reasonix classifies as
permission-requiring; it does not pause every shell command. Explicit agent
questions always require a response. Use reasonix_send with
expect=steer to redirect an active turn, expect=new_turn to start work only
when idle. reasonix_transcript summarizes what an agent actually did (tool
calls, files touched) for rebasing decisions. reasonix_watch is the primary
model-visible wake-up mechanism. Custom reasonix/agent_event notifications are
diagnostic decoration;
clients may silently drop unknown notifications, and MCP notifications are not
injected into an orchestrator model's conversation. Clients that advertise MCP
elicitation receive a standard elicitation/create request for permission gates
and agent questions; the selected option is returned to the child
automatically. Watch remains authoritative when elicitation is unavailable,
declined, or canceled. reasonix_poll provides explicit snapshots and is compact
by default; use detail=true only for diagnostics. reasonix_wait is the legacy
event-driven, output-sensitive long poll.
Use reasonix_configure to switch model/effort/work mode/approval. An active
turn keeps its current model and applies the queued change before the next
turn; idle agents apply immediately; exited agents apply it on resume. An
ask→yolo change immediately allows a pending tool gate once, but never answers
an explicit agent question. ask→auto preserves pending decisions.
Omit watch timeout for normal orchestration; its default is indefinite. Never
copy explicit test safety timeouts (such as 240 seconds) into production watch
calls unless the user specifically requests a bounded wait.
A client-side `timed out awaiting tools/call` is the MCP host's transport
limit, not an agent or daemon failure. Never restart MCP or agentd for that
error: reissue one watch for the complete remaining fleet. Codex installations
should set `mcp_servers.reasonix.tool_timeout_sec` above the longest expected
agent run (86400 is recommended) so the host does not cut off an indefinite
watch.
Set watch/poll detail=true or read the transcript only when deeper output is
needed. Terminal errors include error_text; watch exposes plan and current_work. The
daemon and MCP server log event emission/relay results to stderr.
Lifecycle controls:
- reasonix_restart_mcp_server() reloads only this orchestrator's MCP stdio
  front-end through launcher.py; it preserves agentd and all agent sessions and
  does not interrupt other orchestrators.
- reasonix_restart_agentd(force=false) reloads the shared agent daemon after
  its live agents are stopped; force=true terminates live agents and affects
  every orchestrator connected to that daemon. Use it after agentd or
  acp_bridge changes. Use reasonix_restart_mcp_server after server.py changes.
- reasonix_dashboard() opens a loopback-only authenticated fleet UI. It sees
  current and previous sessions across local orchestrators without consuming
  their watch queues; actions are owner-scoped by persisted session metadata.
When launcher.py is configured, Python source changes restart this MCP
front-end automatically and replay its MCP handshake. An in-flight watch/wait
is completed with server_restarted=true so the orchestrator can immediately
rewatch the surviving fleet; other requests finish before reload. agentd
watches its own runtime sources and restarts after active, keep-alive, and
decision-blocked agents are clear and terminal output has been delivered by
watch or poll. Manual restart tools remain available for explicit control.
The orchestrator status protocol is injected once per persisted child session;
follow-up and resumed turns do not duplicate it.
Keep orchestration state in the parent agent. Completed agents can be
cleaned up after a configured idle grace period (disabled by default) unless
spawned with keep_alive=true;
session ownership is scoped to this orchestrator, so never expect to see
another MCP client's sessions;
do not claim a child is complete
until its poll reports idle/exited status and a stop reason."""

mcp = MCPServer("reasonix", instructions=MCP_INSTRUCTIONS)

AGENT_EVENT_METHOD = "reasonix/agent_event"
AGENTD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agentd.py")
DASHBOARD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py")
DASHBOARD_STATE = os.path.join(os.path.dirname(common.agentd_sock_path()), "dashboard.json")
DASHBOARD_LOCK = os.path.join(os.path.dirname(common.agentd_sock_path()), "dashboard.lock")
MCP_RESTART_EXIT_CODE = 75

_agentd_reader: asyncio.StreamReader | None = None
_agentd_writer: asyncio.StreamWriter | None = None
_agentd_cancel_supported: bool | None = None
_agentd_connect_lock: asyncio.Lock | None = None
_agentd_probe_task: asyncio.Task | None = None
_pending: dict[int, asyncio.Future] = {}
_pending_writers: dict[int, asyncio.StreamWriter] = {}
_next_id = itertools.count(1)
_mcp_session = None  # captured on first tool call, used to relay daemon pushes
_elicitation_tasks: dict[str, asyncio.Task] = {}
_rpc_default_timeout = 600.0
_owner_id = common.orchestrator_owner_id()
_owner_explicit = bool(os.environ.get("REASONIX_MCP_ORCHESTRATOR_ID", "").strip())
_owned_sessions = common.load_owner_sessions(_owner_id)


async def _start_agentd(sock: str) -> None:
    sdir = os.path.dirname(sock)
    os.makedirs(sdir, exist_ok=True)
    log = open(os.path.join(sdir, "agentd.log"), "ab")
    # Detached (its own session): it must outlive this MCP server process.
    subprocess.Popen(
        [sys.executable, AGENTD_SCRIPT, "--sock", sock],
        start_new_session=True,
        stdin=subprocess.DEVNULL, stdout=log, stderr=log,
        env=dict(os.environ),
    )


async def _ensure_agentd() -> None:
    global _agentd_reader, _agentd_writer, _agentd_cancel_supported
    global _agentd_connect_lock, _agentd_probe_task
    current_task = asyncio.current_task()
    if (
        _agentd_writer is not None
        and not _agentd_writer.is_closing()
        and (_agentd_cancel_supported is not None or current_task is _agentd_probe_task)
    ):
        return
    if _agentd_connect_lock is None:
        _agentd_connect_lock = asyncio.Lock()
    async with _agentd_connect_lock:
        if (
            _agentd_writer is not None
            and not _agentd_writer.is_closing()
            and _agentd_cancel_supported is not None
        ):
            return
        if _agentd_writer is None or _agentd_writer.is_closing():
            sock = common.agentd_sock_path()
            try:
                reader, writer = await asyncio.open_unix_connection(sock)
            except OSError:
                await _start_agentd(sock)
                for _ in range(100):  # up to ~10s for the daemon to bind
                    try:
                        reader, writer = await asyncio.open_unix_connection(sock)
                        break
                    except OSError:
                        await asyncio.sleep(0.1)
                else:
                    raise RuntimeError(f"agentd did not start on {sock} — see agentd.log")
            _agentd_reader, _agentd_writer = reader, writer
            _agentd_cancel_supported = None
            asyncio.create_task(_read_agentd(reader, writer))
        # Detect daemons from before request cancellation existed. Do not let
        # concurrent RPCs use the connection until this probe has completed:
        # an unknown capability used to make cancellation close a new socket.
        _agentd_probe_task = current_task
        try:
            state = await _rpc("ping", {}, timeout=5.0, _retry_disconnect=False)
            _agentd_cancel_supported = bool(state.get("cancel_request"))
        finally:
            _agentd_probe_task = None


async def _read_agentd(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global _agentd_reader, _agentd_writer, _agentd_cancel_supported
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "id" in msg:
                fut = _pending.pop(msg["id"], None)
                _pending_writers.pop(msg["id"], None)
                if fut is not None and not fut.done():
                    fut.set_result(msg)
            elif msg.get("method") == "agent_event":
                await _relay_agent_event(msg.get("params") or {})
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        # Fail only requests sent through this connection. An old reader can
        # finish after a replacement is already carrying new RPCs.
        for rid, fut in list(_pending.items()):
            if _pending_writers.get(rid) is not writer:
                continue
            _pending.pop(rid, None)
            _pending_writers.pop(rid, None)
            if not fut.done():
                fut.set_result({
                    "error": {
                        "code": -32001,
                        "message": "agentd connection closed; agents may still be running",
                    }
                })
        # An old reader can finish after a replacement connection is already
        # installed. Do not let it clear the replacement writer.
        if _agentd_reader is reader:
            _agentd_reader = None
        if _agentd_writer is writer:
            _agentd_writer = None
            _agentd_cancel_supported = None


_DISCONNECT_RETRY_METHODS = {
    "configure", "list", "models", "ping", "poll", "transcript", "wait", "watch",
}
_CONSUMING_RETRY_METHODS = {"poll", "watch"}
_next_request_token = itertools.count(1)


async def _rpc(
    method: str,
    params: dict,
    timeout: float | None = _rpc_default_timeout,
    *,
    _retry_disconnect: bool = True,
) -> dict:
    params = dict(params)
    params.setdefault("owner_id", _owner_id)
    if method in _CONSUMING_RETRY_METHODS and "_request_token" not in params:
        params["_request_token"] = f"{os.getpid()}-{next(_next_request_token)}"
    await _ensure_agentd()
    rid = next(_next_id)
    fut = asyncio.get_running_loop().create_future()
    _pending[rid] = fut
    writer = _agentd_writer
    if writer is None:
        _pending.pop(rid, None)
        raise RuntimeError("agentd connection unavailable after startup")
    _pending_writers[rid] = writer
    frame = json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n"
    try:
        writer.write(frame.encode())
        await writer.drain()
    except asyncio.CancelledError:
        _pending.pop(rid, None)
        _pending_writers.pop(rid, None)
        _cancel_agentd_request(writer, rid)
        raise
    except Exception as e:
        _pending.pop(rid, None)
        _pending_writers.pop(rid, None)
        raise RuntimeError(f"agentd connection failed: {e}") from e
    try:
        msg = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.CancelledError:
        _pending.pop(rid, None)
        _pending_writers.pop(rid, None)
        _cancel_agentd_request(writer, rid)
        raise
    except asyncio.TimeoutError:
        _pending.pop(rid, None)
        _pending_writers.pop(rid, None)
        _cancel_agentd_request(writer, rid)
        raise RuntimeError(f"agentd timed out on {method} after {timeout}s") from None
    if msg.get("error"):
        err = msg["error"]
        if (
            err.get("code") == -32001
            and _retry_disconnect
            and method in _DISCONNECT_RETRY_METHODS
        ):
            return await _rpc(method, params, timeout=timeout, _retry_disconnect=False)
        raise ValueError(err.get("message", "agentd error"))
    return msg.get("result", {})


def _cancel_agentd_request(writer: asyncio.StreamWriter, request_id: int) -> None:
    """Best-effort cancellation for an RPC the MCP client stopped awaiting.

    The write is deliberately synchronous: cancellation cleanup must not add a
    second await point where the task can be canceled again before agentd sees
    the notification. A live transport flushes the buffered frame; a closed
    transport makes agentd's client loop cancel all requests for the socket.
    """
    if writer is None or writer.is_closing():
        return
    frame = json.dumps({
        "jsonrpc": "2.0",
        "method": "cancel_request",
        "params": {"request_id": request_id, "owner_id": _owner_id},
    }) + "\n"
    try:
        writer.write(frame.encode())
    except Exception:
        pass
    if _agentd_cancel_supported is False:
        # Rolling-upgrade fallback: an older daemon ignores cancel_request.
        # Closing only this orchestrator's daemon connection makes its existing
        # client-loop cleanup cancel stale calls while all agents keep running.
        try:
            writer.close()
        except Exception:
            pass


async def _relay_agent_event(event: dict) -> None:
    event_owner = event.get("_owner_id")
    if event_owner is not None:
        if event_owner != _owner_id:
            return
        event = {key: value for key, value in event.items() if key != "_owner_id"}
    elif event.get("session_id") not in _owned_sessions:
        # Compatibility with daemons started before owner metadata existed.
        print(
            f"reasonix-mcp notification drop: unowned session={event.get('session_id')}",
            file=sys.stderr, flush=True,
        )
        return
    if event.get("event") == "permission_request" and await _resolve_legacy_yolo_event(event):
        return
    print(
        f"reasonix-mcp notification relay: event={event.get('event')} "
        f"session={event.get('session_id')}",
        file=sys.stderr, flush=True,
    )
    if event.get("event") == "permission_request":
        _schedule_elicitation(event)
    if _mcp_session is None:
        print("reasonix-mcp notification drop: MCP session unavailable", file=sys.stderr, flush=True)
        return
    try:
        outbound = getattr(getattr(_mcp_session, "_connection", None), "outbound", None)
        if outbound is None:
            print("reasonix-mcp notification drop: MCP outbound unavailable", file=sys.stderr, flush=True)
            return
        delivered: list[str] = []
        for method, params in (
            (AGENT_EVENT_METHOD, event),
            ("notifications/message", {"level": "info", "logger": "reasonix-mcp", "data": event}),
        ):
            try:
                await outbound.notify(method, params)
                delivered.append(method)
            except Exception:
                print(
                    f"reasonix-mcp notification send failed: method={method} "
                    f"session={event.get('session_id')}",
                    file=sys.stderr, flush=True,
                )
                continue
        print(
            f"reasonix-mcp notification delivered: methods={','.join(delivered) or 'none'} "
            f"session={event.get('session_id')}",
            file=sys.stderr, flush=True,
        )
    except Exception:
        print(
            f"reasonix-mcp notification relay failed: session={event.get('session_id')}",
            file=sys.stderr, flush=True,
        )
        pass  # best-effort push; wait/poll remain authoritative


def _allow_once_option(permission: dict) -> str | None:
    tool_call = permission.get("tool_call") or {}
    if str(tool_call.get("kind") or "").lower() == "other":
        return None
    ranked: list[tuple[int, str]] = []
    for option in permission.get("options") or []:
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


async def _resolve_legacy_yolo_event(event: dict) -> bool:
    """Auto-answer a tool gate while a stale daemon cannot configure ACP."""
    session_id = str(event.get("session_id") or "")
    override = common.read_session_runtime_override(session_id)
    if override.get("tool_approval") != "yolo":
        return False
    try:
        state = await _rpc("ping", {})
    except Exception:
        return False
    if state.get("code_signature") != override.get("daemon_code_signature"):
        common.clear_session_runtime_override(session_id)
        return False
    option_id = _allow_once_option(event.get("permission_request") or {})
    if option_id is None:
        return False
    try:
        await _rpc(
            "respond_permission",
            {"session_id": session_id, "option_id": option_id},
        )
    except Exception:
        return False
    print(
        "reasonix-mcp legacy YOLO auto-resolved tool gate: "
        f"session={session_id} option={option_id}",
        file=sys.stderr,
        flush=True,
    )
    return True


def _schedule_elicitation(event: dict) -> None:
    session_id = str(event.get("session_id") or "")
    if not session_id or _mcp_session is None:
        return
    current = _elicitation_tasks.get(session_id)
    if current is not None and not current.done():
        return
    task = asyncio.create_task(_elicit_agent_decision(event))
    _elicitation_tasks[session_id] = task
    task.add_done_callback(lambda _done, sid=session_id: _elicitation_tasks.pop(sid, None))


def _cancel_resolved_elicitation(session_id: str, result: dict) -> None:
    if not result.get("permission_resolution"):
        return
    elicitation = _elicitation_tasks.get(session_id)
    if elicitation is not None and not elicitation.done():
        elicitation.cancel()


async def _configure_stale_agentd(
    session_id: str, updates: dict[str, str]
) -> dict:
    """Persist configuration and emulate YOLO when agentd predates configure."""
    if updates.get("work_mode") not in (None, "economy", "balanced", "delivery"):
        raise ValueError("work_mode must be one of: economy, balanced, delivery")
    if updates.get("tool_approval") not in (None, "ask", "auto", "yolo"):
        raise ValueError("tool_approval must be one of: ask, auto, yolo")
    durable = dict(updates)
    if "model" in durable and "effort" not in durable:
        durable["effort"] = ""
    common.write_session_config(session_id, durable)

    listing, state = await asyncio.gather(_rpc("list", {}), _rpc("ping", {}))
    existing = next(
        (
            item for item in listing.get("sessions", [])
            if item.get("session_id") == session_id
        ),
        None,
    )
    if existing is None:
        raise ValueError(f"unknown session_id {session_id!r}")
    status = str(existing.get("status") or "exited")
    if status == "exited":
        common.clear_session_runtime_override(session_id)
        return {
            "session_id": session_id,
            "status": status,
            "queued": True,
            "applies": "on_resume",
            "requested": updates,
            "compatibility_fallback": "stale_agentd",
            "note": "configuration saved; it applies when this session is resumed on the updated daemon",
        }

    permission_resolution = None
    runtime_emulation = updates.get("tool_approval") == "yolo"
    if runtime_emulation:
        common.write_session_runtime_override(session_id, {
            "tool_approval": "yolo",
            "daemon_code_signature": state.get("code_signature"),
            "created_at": time.time(),
        })
        # The permission event may have arrived before this MCP server loaded
        # or before configure was called. Poll's permission state is durable.
        detailed = await _rpc("poll", {"session_id": session_id, "detail": True})
        option_id = _allow_once_option(detailed.get("permission_request") or {})
        if option_id is not None:
            try:
                await _rpc(
                    "respond_permission",
                    {"session_id": session_id, "option_id": option_id},
                )
            except ValueError as exc:
                if "no pending permission request" not in str(exc):
                    raise
            else:
                permission_resolution = option_id
    elif "tool_approval" in updates:
        common.clear_session_runtime_override(session_id)

    waiting = [key for key in updates if key != "tool_approval" or not runtime_emulation]
    note = (
        "running agentd predates live configuration; YOLO is being emulated "
        "for tool gates and the durable option will apply after its safe reload"
        if runtime_emulation
        else "configuration saved but cannot affect this live ACP process until the shared agentd safely reloads"
    )
    result = {
        "session_id": session_id,
        "status": status,
        "queued": True,
        "applies": "after_agentd_reload",
        "requested": updates,
        "waiting_options": waiting,
        "runtime_emulation": "yolo" if runtime_emulation else None,
        "permission_resolution": permission_resolution,
        "permission_still_pending": bool(
            detailed.get("permission_request") if runtime_emulation else existing.get("permission_request")
        ) and permission_resolution is None,
        "compatibility_fallback": "stale_agentd",
        "daemon_code_update_available": bool(state.get("code_update_available")),
        "note": note,
    }
    return result


async def _configure_with_compat(
    session_id: str, updates: dict[str, str]
) -> dict:
    try:
        result = await _rpc("configure", {"session_id": session_id, **updates})
    except ValueError as exc:
        if "unknown method 'configure'" not in str(exc):
            raise
        result = await _configure_stale_agentd(session_id, updates)
    else:
        common.clear_session_runtime_override(session_id)
    _cancel_resolved_elicitation(session_id, result)
    return result


async def _elicit_agent_decision(event: dict) -> None:
    """Bridge an ACP permission/question request to standard MCP elicitation."""
    session = _mcp_session
    if session is None:
        return
    params = getattr(session, "client_params", None)
    capabilities = getattr(params, "capabilities", None)
    if getattr(capabilities, "elicitation", None) is None:
        print(
            f"reasonix-mcp elicitation unavailable: session={event.get('session_id')}",
            file=sys.stderr,
            flush=True,
        )
        return
    permission = event.get("permission_request") or {}
    options = [option for option in permission.get("options", []) if option.get("optionId")]
    option_ids = [str(option["optionId"]) for option in options]
    if not option_ids:
        return
    tool_call = permission.get("tool_call") or {}
    title = str(tool_call.get("title") or event.get("question") or "Agent decision required")
    choices = ", ".join(
        f"{option['optionId']} ({option.get('name') or option['optionId']})"
        for option in options
    )
    message = f"Reasonix agent {event.get('session_id')} asks: {title}\nChoices: {choices}"
    schema = {
        "type": "object",
        "properties": {
            "option_id": {
                "type": "string",
                "title": "Decision",
                "description": "Choose one of the agent-provided option IDs.",
                "enum": option_ids,
            }
        },
        "required": ["option_id"],
    }
    try:
        result = await session.elicit_form(message, schema)
        action = getattr(result, "action", "cancel")
        content = getattr(result, "content", None) or {}
        if action != "accept":
            # Clients may advertise elicitation yet dismiss unsupported or
            # non-interactive requests automatically. Declining the form is
            # not the same as selecting the child request's reject option.
            # Preserve ACP so reasonix_watch/poll + respond still work.
            print(
                f"reasonix-mcp elicitation {action}; poll fallback retained: "
                f"session={event.get('session_id')}",
                file=sys.stderr,
                flush=True,
            )
            return
        option_id = str(content.get("option_id") or "")
        if option_id not in option_ids and option_id != "cancel":
            raise ValueError(f"client returned unknown elicitation option {option_id!r}")
        await _rpc(
            "respond_permission",
            {"session_id": str(event.get("session_id")), "option_id": option_id or "cancel"},
        )
        print(
            f"reasonix-mcp elicitation answered: session={event.get('session_id')} "
            f"option={option_id or 'cancel'}",
            file=sys.stderr,
            flush=True,
        )
    except Exception as exc:
        print(
            f"reasonix-mcp elicitation failed: session={event.get('session_id')} error={exc}",
            file=sys.stderr,
            flush=True,
        )


def _capture_relay_ctx(ctx: Context | None) -> None:
    global _mcp_session, _owner_id, _owned_sessions
    if ctx is not None:
        try:
            if _mcp_session is None:
                _mcp_session = ctx.session
            if not _owner_explicit:
                params = getattr(ctx.session, "client_params", None)
                info = getattr(params, "client_info", None)
                client_name = str(getattr(info, "name", "") or "")
                owner_id = common.orchestrator_owner_id(client_name)
                if owner_id != _owner_id:
                    _owner_id = owner_id
                    _owned_sessions = common.load_owner_sessions(_owner_id)
        except Exception:
            pass


def _remember_session(session_id: str) -> None:
    global _owned_sessions
    if session_id:
        _owned_sessions = common.load_owner_sessions(_owner_id) | _owned_sessions | {session_id}
        try:
            common.write_session_owner(session_id, _owner_id)
            common.save_owner_sessions(_owner_id, _owned_sessions)
        except OSError:
            # A persistence failure must not turn a successful spawn/resume
            # into a tool error; the daemon remains authoritative for the
            # current connection and will report the issue on reconnect.
            pass


def _require_owned(session_id: str) -> None:
    if session_id not in _owned_sessions:
        raise ValueError(
            f"session {session_id!r} is not owned by this orchestrator; "
            "use the MCP client that spawned it"
        )


@mcp.tool()
async def reasonix_spawn(
    task: str,
    cwd: str = "",
    model: str = common.DEFAULT_MODEL,
    work_mode: str = common.DEFAULT_WORK_MODE,
    tool_approval: str = common.DEFAULT_TOOL_APPROVAL,
    effort: str = common.DEFAULT_EFFORT,
    keep_alive: bool = False,
    idle_timeout: float = common.DEFAULT_IDLE_TIMEOUT,
    ctx: Context | None = None,
) -> dict:
    """Spawn a Reasonix coding agent and immediately start it on `task`.

    Returns a session_id immediately (non-blocking). The agent runs in the
    detached daemon and SURVIVES this MCP server / MCP-host restarts —
    reasonix_list shows it afterwards. Call reasonix_poll for output,
    reasonix_send to steer mid-turn, reasonix_stop when done.

    Defaults: model=opencode-go/deepseek-v4-flash, effort=max,
    tool_approval=yolo (env-overridable REASONIX_MCP_DEFAULT_*). Completed
    agents are cleaned up after an idle grace period; set keep_alive=true for
    an interactive session that needs follow-up turns. work_mode:
    economy|balanced|delivery; effort values are model-advertised and can
    include auto|disabled|low|medium|high|xhigh|max; tool_approval:
    ask|auto|yolo. `cwd` is confined to the project dir + [sandbox] allow_write
    (escape: REASONIX_MCP_ALLOW_ANY_CWD=1). Returns the effective sandbox
    posture, and `skipped_options` when the selected model does not advertise
    e.g. an effort option.
    """
    _capture_relay_ctx(ctx)
    result = await _rpc("spawn", {
        "task": task, "cwd": cwd, "model": model,
        "work_mode": work_mode, "tool_approval": tool_approval, "effort": effort,
        "keep_alive": keep_alive, "idle_timeout": idle_timeout,
    })
    _remember_session(result.get("session_id", ""))
    return result


@mcp.tool()
async def reasonix_resume(
    session_id: str,
    cwd: str = "",
    model: str = "",
    effort: str = "",
    work_mode: str = "",
    tool_approval: str = "",
    keep_alive: bool = False,
    idle_timeout: float = common.DEFAULT_IDLE_TIMEOUT,
    ctx: Context | None = None,
) -> dict:
    """Revive a session whose process died (after a daemon restart, a crash,
    or reasonix_stop — history is persisted on disk). This is idempotent: a
    running/idle session returns already_live=true without opening a second
    ACP process. Optional model/effort/work_mode/tool_approval values apply to
    the revived process; on a live session they follow configure semantics.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    # This front-end guard also protects sessions held by an older daemon that
    # predates idempotent resume. The daemon repeats the check to close the
    # list/resume race once it reloads.
    listing = await _rpc("list", {})
    existing = next(
        (
            session for session in listing.get("sessions", [])
            if session.get("session_id") == session_id
        ),
        None,
    )
    if existing is not None and existing.get("status") != "exited":
        result = {
            "session_id": session_id,
            "status": existing.get("status"),
            "cwd": existing.get("cwd") or cwd,
            "already_live": True,
            "resumed": False,
            "note": (
                "session already has a live agent process; use watch/poll/send "
                "instead of starting another ACP process"
            ),
        }
        updates = {
            key: value for key, value in {
                "model": model,
                "effort": effort,
                "work_mode": work_mode,
                "tool_approval": tool_approval,
            }.items() if value
        }
        if updates:
            config_change = await _configure_with_compat(session_id, updates)
            result["config_change"] = config_change
        return result
    result = await _rpc("resume", {
        "session_id": session_id, "cwd": cwd,
        "model": model, "effort": effort,
        "work_mode": work_mode, "tool_approval": tool_approval,
        "keep_alive": keep_alive, "idle_timeout": idle_timeout,
    })
    _remember_session(session_id)
    return result


@mcp.tool()
async def reasonix_configure(
    session_id: str,
    model: str = "",
    effort: str = "",
    work_mode: str = "",
    tool_approval: str = "",
    ctx: Context | None = None,
) -> dict:
    """Change a live or stopped agent's model/session options.

    Idle agents apply immediately. Active turns keep the old configuration and
    queue the change before their next turn. Exited agents persist the change;
    call reasonix_resume to launch them with it. At least one option is
    required. Model changes preserve the session transcript. During an active
    turn, tool_approval=yolo immediately allows known tool gates once and keeps
    doing so until the durable switch applies; agent questions are untouched.
    tool_approval=auto never guesses an already-pending decision.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    updates = {
        key: value for key, value in {
            "model": model,
            "effort": effort,
            "work_mode": work_mode,
            "tool_approval": tool_approval,
        }.items() if value
    }
    if not updates:
        raise ValueError("provide at least one of: model, effort, work_mode, tool_approval")
    return await _configure_with_compat(session_id, updates)


@mcp.tool()
async def reasonix_send(
    session_id: str, message: str, expect: str = "any", ctx: Context | None = None
) -> dict:
    """Send a message to a running agent — always delivered (forced steer).

    Steer into the active turn (`_reasonix.io/session/steer`), or start a new
    turn if idle. expect: any|steer|new_turn; a refused call has no side effect.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    return await _rpc("send", {"session_id": session_id, "message": message, "expect": expect})


@mcp.tool()
async def reasonix_poll(
    session_id: str,
    detail: bool = False,
    include_events: list[str] | None = None,
    exclude_events: list[str] | None = None,
    include_thought: bool = common.DEFAULT_INCLUDE_THOUGHT,
    include_full: bool = common.DEFAULT_INCLUDE_FULL,
    max_events: int | None = None,
    ctx: Context | None = None,
) -> dict:
    """Read a context-lean snapshot of recent agent state and output.

    By default returns status, a capped `message` when new output exists,
    stop_reason, plan/current_work, pending permission, and errors when present.
    Set detail=true for the legacy diagnostic shape with text, turns, events,
    counters, transcript path, and configuration. Existing explicit selectors
    (include_events/exclude_events/max_events/include_thought/include_full)
    also opt into that detailed shape. Static setup events remain filtered
    unless explicitly selected.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    result = await _rpc("poll", {
        "session_id": session_id,
        "detail": detail,
        "include_events": include_events, "exclude_events": exclude_events,
        "include_thought": include_thought, "include_full": include_full,
        "max_events": max_events,
    })
    verbose = bool(
        detail
        or include_events is not None
        or exclude_events is not None
        or include_thought
        or include_full
        or max_events is not None
    )
    # Compatibility with an older shared daemon that is waiting for its
    # agents to become restart-safe: compact its legacy response here so MCP
    # hot reload delivers the lean contract immediately.
    if not verbose and ({"events", "turns", "text"} & result.keys()):
        return common.compact_poll_result(result)
    return result


@mcp.tool()
async def reasonix_wait(
    session_ids: list[str], timeout: float = 30.0, ctx: Context | None = None
) -> dict:
    """Block until any watched session produces output, finishes a turn, or
    raises a permission request (or `timeout` seconds pass). Returns woke ids,
    per-session status + stop_reason. This legacy path is event-driven but
    output-sensitive and requires a follow-up poll; prefer reasonix_watch.
    """
    _capture_relay_ctx(ctx)
    for session_id in session_ids:
        _require_owned(session_id)
    return await _rpc("wait", {"session_ids": session_ids, "timeout": timeout}, timeout=timeout + 60)


@mcp.tool()
async def reasonix_watch(
    session_ids: list[str],
    timeout: float | None = None,
    detail: bool = False,
    include_events: list[str] | None = None,
    exclude_events: list[str] | None = None,
    include_thought: bool = common.DEFAULT_INCLUDE_THOUGHT,
    include_full: bool = common.DEFAULT_INCLUDE_FULL,
    max_events: int | None = None,
    ctx: Context | None = None,
) -> dict:
    """Wait for completion, permission/question, or process death and return
    compact results for every woken agent. The default omits event history,
    thought, duplicate turn text, and full history; set detail=true for the
    poll-shaped result. Omit timeout for normal orchestration: the default is
    indefinite. Explicit timeouts are only for deliberately bounded waits.
    Keep one call for the complete remaining fleet. A newer overlapping call
    safely supersedes an older abandoned watch; no server restart is needed.
    Wire notifications are diagnostic only.
    """
    _capture_relay_ctx(ctx)
    for session_id in session_ids:
        _require_owned(session_id)
    rpc_timeout = None if timeout is None else max(0.0, float(timeout)) + 60.0
    return await _rpc("watch", {
        "session_ids": session_ids,
        "timeout": timeout,
        "detail": detail,
        "include_events": include_events,
        "exclude_events": exclude_events,
        "include_thought": include_thought,
        "include_full": include_full,
        "max_events": max_events,
    }, timeout=rpc_timeout)


@mcp.tool()
async def reasonix_list(
    include_task: bool = False,
    pending_only: bool = False,
    ctx: Context | None = None,
) -> dict:
    """List sessions with a compact task preview by default.

    Set include_task=true only when the full original orchestration prompt is
    needed; it can be thousands of words. Set pending_only=true to recover
    only running, decision-blocked, or not-yet-collected terminal sessions.
    """
    _capture_relay_ctx(ctx)
    result = await _rpc(
        "list", {"include_task": include_task, "pending_only": pending_only}
    )
    result["sessions"] = [
        session for session in result.get("sessions", [])
        if session.get("session_id") in _owned_sessions
    ]
    return result


@mcp.tool()
async def reasonix_models(ctx: Context | None = None) -> dict:
    """List selectable models: provider/model refs, default, per-model
    supported_efforts, and price hints where the config declares them."""
    _capture_relay_ctx(ctx)
    return await _rpc("models", {})


@mcp.tool()
async def reasonix_transcript(
    session_id: str, max_tool_calls: int = 200, ctx: Context | None = None
) -> dict:
    """What the agent actually did: tool calls with args, files touched,
    roles, work duration, last text. NB: Reasonix does not persist token/cost
    usage — these are activity metrics."""
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    return await _rpc("transcript", {"session_id": session_id, "max_tool_calls": max_tool_calls})


@mcp.tool()
async def reasonix_respond_permission(
    session_id: str, option_id: str, ctx: Context | None = None
) -> dict:
    """Answer the agent's pending tool-approval or question request. option_id
    is one of watch/poll's permission_request.options[].optionId, or "cancel"."""
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    return await _rpc("respond_permission", {"session_id": session_id, "option_id": option_id})


@mcp.tool()
async def reasonix_stop(session_id: str, ctx: Context | None = None) -> dict:
    """Cancel the active turn, close the ACP session, kill the agent process.
    The session stays listed as a tombstone and is resumable via
    reasonix_resume (its transcript is persisted)."""
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    return await _rpc("stop", {"session_id": session_id})


def _dashboard_source_signature() -> str:
    digest = hashlib.sha256()
    package_dir = os.path.dirname(DASHBOARD_SCRIPT)
    for path in [
        DASHBOARD_SCRIPT,
        os.path.join(package_dir, "static", "dashboard.html"),
        os.path.join(package_dir, "static", "dashboard.css"),
        os.path.join(package_dir, "static", "dashboard.js"),
    ]:
        digest.update(os.path.basename(path).encode())
        try:
            with open(path, "rb") as fh:
                digest.update(fh.read())
        except OSError:
            digest.update(b"missing")
    return digest.hexdigest()


def _read_dashboard_state() -> dict:
    try:
        with open(DASHBOARD_STATE, encoding="utf-8") as fh:
            state = json.load(fh)
        return state if isinstance(state, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _dashboard_health(state: dict) -> dict | None:
    token = str(state.get("token") or "")
    host = str(state.get("host") or "127.0.0.1")
    port = int(state.get("port") or 0)
    if not token or not port:
        return None
    host_url = f"[{host}]" if ":" in host else host
    request = urllib.request.Request(
        f"http://{host_url}:{port}/api/health",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=1.0) as response:
            value = json.load(response)
        return value if isinstance(value, dict) and value.get("ok") else None
    except (OSError, ValueError, TypeError, urllib.error.URLError):
        return None


def _is_dashboard_process(pid: int) -> bool:
    if pid <= 1:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            command = fh.read().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return False
    return os.path.abspath(DASHBOARD_SCRIPT) in command


async def _reasonix_dashboard_locked(open_browser: bool) -> dict:
    expected_signature = _dashboard_source_signature()
    state = _read_dashboard_state()
    health = await asyncio.to_thread(_dashboard_health, state)
    if (
        health
        and state.get("source_signature") == expected_signature
        and health.get("source_signature") == expected_signature
    ):
        if open_browser:
            await asyncio.to_thread(webbrowser.open, str(state["url"]))
        return {
            "running": True,
            "started": False,
            "url": state["url"],
            "pid": health.get("pid") or state.get("pid"),
            "non_consuming_observer": True,
        }

    old_pid = int(state.get("pid") or 0)
    if _is_dashboard_process(old_pid):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.kill(old_pid, 15)
        for _ in range(30):
            if not _is_dashboard_process(old_pid):
                break
            await asyncio.sleep(0.1)
        if _is_dashboard_process(old_pid):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(old_pid, 9)
    with contextlib.suppress(OSError):
        if _read_dashboard_state().get("pid") == state.get("pid"):
            os.unlink(DASHBOARD_STATE)

    runtime_dir = os.path.dirname(DASHBOARD_STATE)
    os.makedirs(runtime_dir, exist_ok=True)
    log = open(os.path.join(runtime_dir, "dashboard.log"), "ab")
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                DASHBOARD_SCRIPT,
                "--port", "0",
                "--state-file", DASHBOARD_STATE,
            ],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            env=dict(os.environ),
        )
    finally:
        log.close()
    for _ in range(100):
        state = _read_dashboard_state()
        if state.get("pid") == process.pid:
            health = await asyncio.to_thread(_dashboard_health, state)
            if health and health.get("source_signature") == expected_signature:
                if open_browser:
                    await asyncio.to_thread(webbrowser.open, str(state["url"]))
                return {
                    "running": True,
                    "started": True,
                    "url": state["url"],
                    "pid": process.pid,
                    "non_consuming_observer": True,
                }
        if process.poll() is not None:
            break
        await asyncio.sleep(0.1)
    raise RuntimeError(
        "Reasonix dashboard failed to start; inspect ~/.reasonix-mcp/dashboard.log"
    )


@mcp.tool()
async def reasonix_dashboard(open_browser: bool = True) -> dict:
    """Open or return the local authenticated Reasonix fleet dashboard.

    The dashboard shows current and previous agents grouped by orchestrator,
    streams non-consuming live state updates, and can steer, stop, resume,
    reconfigure, or answer a pending permission request. It binds only to
    loopback and derives session ownership server-side. It never takes over an
    orchestrator's reasonix_watch call or consumes its terminal delivery.
    Concurrent calls from multiple orchestrators safely reuse one process.
    """
    os.makedirs(os.path.dirname(DASHBOARD_LOCK), exist_ok=True)
    lock_fd = os.open(DASHBOARD_LOCK, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        await asyncio.to_thread(fcntl.flock, lock_fd, fcntl.LOCK_EX)
        return await _reasonix_dashboard_locked(open_browser)
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


@mcp.tool()
async def reasonix_restart_agentd(force: bool = False, ctx: Context | None = None) -> dict:
    """Restart the detached agent daemon so it loads updated code.

    By default this refuses while live agents exist. Set force=true only when
    terminating those agents is acceptable; their transcripts remain
    resumable. The next Reasonix tool call automatically starts the fresh
    daemon.
    """
    global _agentd_reader, _agentd_writer
    _capture_relay_ctx(ctx)
    try:
        result = await _rpc("restart", {"force": force})
    except ValueError as exc:
        # A daemon started before this tool existed only knows `shutdown`.
        # Use its existing ping method to preserve the safe refusal behavior
        # before falling back to shutdown.
        if "unknown method 'restart'" not in str(exc):
            raise
        state = await _rpc("ping", {})
        sessions = int(state.get("sessions", 0))
        if sessions and not force:
            # Older daemons expose list but not restart. Terminal idle
            # sessions are safe to discard; running/unfinished ones are not.
            try:
                listing = await _rpc("list", {})
                live = [
                    item for item in listing.get("sessions", [])
                    if item.get("status") == "running"
                    or (item.get("status") == "idle" and not item.get("stop_reason"))
                ]
            except Exception:
                live = listing = None
            if live is not None:
                sessions = len(live)
        if sessions and not force:
            raise ValueError(
                "the running agentd predates restart support and has live "
                "sessions; stop them first or retry with force=true"
            )
        result = await _rpc("shutdown", {})
        result["compatibility_fallback"] = "shutdown"
    result["restarted"] = True
    result["note"] = (
        "agentd has stopped; the next Reasonix tool call starts a fresh daemon "
        "from the current code. Other MCP clients sharing this daemon must "
        "reconnect on their next call."
    )
    writer = _agentd_writer
    _agentd_reader = None
    _agentd_writer = None
    if writer is not None:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    return result


@mcp.tool()
async def reasonix_restart_mcp_server() -> dict:
    """Restart this MCP server through its per-orchestrator launcher.

    The launcher starts a fresh server after this response is delivered. The
    detached agentd and all agent sessions remain running. This tool requires
    the configured MCP command to use ``launcher.py``; a direct server.py
    command will simply disconnect after returning the response.
    """
    asyncio.create_task(_exit_after_mcp_restart())
    return {
        "restart_requested": True,
        "preserves_agents": True,
        "note": (
            "this MCP server will restart through launcher.py; agentd and its "
            "sessions are preserved"
        ),
    }


async def _exit_after_mcp_restart() -> None:
    # Let the MCP framework flush the tool result before replacing this
    # process. The launcher catches this private code and starts server.py.
    await asyncio.sleep(0.25)
    os._exit(MCP_RESTART_EXIT_CODE)


def main() -> None:
    asyncio.run(mcp.run_stdio_async())


if __name__ == "__main__":
    main()
