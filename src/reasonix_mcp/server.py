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
  reasonix_wait / reasonix_list / reasonix_models / reasonix_transcript
  reasonix_respond_permission / reasonix_stop / reasonix_restart_agentd /
  reasonix_restart_mcp_server
"""

from __future__ import annotations

import asyncio
import itertools
import json
import os
import subprocess
import sys

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver import Context

import common

MCP_INSTRUCTIONS = """Reasonix is an agent-orchestration MCP server backed by a
detached agent daemon. Use reasonix_spawn once per child task and retain each
session_id. Agents keep running even if this MCP server (or the MCP host —
Claude Code, Codex, …) restarts — reasonix_list shows the fleet, reasonix_resume revives a session
whose process died. For multiple children call reasonix_wait with all ids, then
reasonix_poll only for the ids in `woke`; poll is the authoritative way to
collect text, events, completion, and permission requests. If a poll contains
permission_request, inspect the tool_call and answer with
reasonix_respond_permission before waiting again. Use reasonix_send with
expect=steer to redirect an active turn, expect=new_turn to start work only
when idle. reasonix_transcript summarizes what an agent actually did (tool
calls, files touched) for rebasing decisions. Each child pushes a
reasonix/agent_event notification (best-effort) when it finishes a turn, needs
a permission decision, reports a plan change, or exits; terminal errors
include error_text. Progress events include the current plan and current_work.
The custom notification may be dropped by the client, so reasonix_wait/poll are the
guaranteed control path. Use reasonix_restart_agentd after updating this
server; it reloads the daemon once no live agents remain. Keep
orchestration state in the parent agent. Completed agents can be cleaned up
after a configured idle grace period (disabled by default) unless spawned with
keep_alive=true;
use reasonix_restart_mcp_server after updating this MCP server; it replaces
only this orchestrator's stdio server through launcher.py and preserves agentd;
session ownership is scoped to this orchestrator, so never expect to see
another MCP client's sessions;
do not claim a child is complete
until its poll reports idle/exited status and a stop reason."""

mcp = MCPServer("reasonix", instructions=MCP_INSTRUCTIONS)

NOTIFY_ENABLED = os.environ.get("REASONIX_MCP_NOTIFY", "1").strip().lower() not in ("0", "false", "no", "off")
AGENT_EVENT_METHOD = "reasonix/agent_event"
AGENTD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agentd.py")
MCP_RESTART_EXIT_CODE = 75

_agentd_reader: asyncio.StreamReader | None = None
_agentd_writer: asyncio.StreamWriter | None = None
_pending: dict[int, asyncio.Future] = {}
_next_id = itertools.count(1)
_mcp_session = None  # captured on first tool call, used to relay daemon pushes
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
    global _agentd_reader, _agentd_writer
    if _agentd_writer is not None and not _agentd_writer.is_closing():
        return
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
    asyncio.create_task(_read_agentd(reader, writer))


async def _read_agentd(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global _agentd_reader, _agentd_writer
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
                if fut is not None and not fut.done():
                    fut.set_result(msg)
            elif msg.get("method") == "agent_event":
                await _relay_agent_event(msg.get("params") or {})
    finally:
        # The daemon died (its agents died with it): fail in-flight requests.
        for fut in _pending.values():
            if not fut.done():
                fut.set_result({"error": {"code": -32000, "message": "agentd disconnected (its agents exited)"}})
        _pending.clear()
        # An old reader can finish after a replacement connection is already
        # installed. Do not let it clear the replacement writer.
        if _agentd_reader is reader:
            _agentd_reader = None
        if _agentd_writer is writer:
            _agentd_writer = None


async def _rpc(method: str, params: dict, timeout: float = _rpc_default_timeout) -> dict:
    params = dict(params)
    params.setdefault("owner_id", _owner_id)
    await _ensure_agentd()
    rid = next(_next_id)
    fut = asyncio.get_running_loop().create_future()
    _pending[rid] = fut
    frame = json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n"
    try:
        _agentd_writer.write(frame.encode())
        await _agentd_writer.drain()
    except Exception as e:
        _pending.pop(rid, None)
        raise RuntimeError(f"agentd connection failed: {e}") from e
    try:
        msg = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _pending.pop(rid, None)
        raise RuntimeError(f"agentd timed out on {method} after {timeout}s") from None
    if msg.get("error"):
        err = msg["error"]
        raise ValueError(err.get("message", "agentd error"))
    return msg.get("result", {})


async def _relay_agent_event(event: dict) -> None:
    event_owner = event.get("_owner_id")
    if event_owner is not None:
        if event_owner != _owner_id:
            return
        event = {key: value for key, value in event.items() if key != "_owner_id"}
    elif event.get("session_id") not in _owned_sessions:
        # Compatibility with daemons started before owner metadata existed.
        return
    if not NOTIFY_ENABLED or _mcp_session is None:
        return
    try:
        outbound = getattr(getattr(_mcp_session, "_connection", None), "outbound", None)
        if outbound is None:
            return
        for method, params in (
            (AGENT_EVENT_METHOD, event),
            ("notifications/message", {"level": "info", "logger": "reasonix-mcp", "data": event}),
        ):
            try:
                await outbound.notify(method, params)
            except Exception:
                continue
    except Exception:
        pass  # best-effort push; wait/poll remain authoritative


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
    economy|balanced|delivery; effort: auto|disabled|high|max; tool_approval:
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
    keep_alive: bool = False,
    idle_timeout: float = common.DEFAULT_IDLE_TIMEOUT,
    ctx: Context | None = None,
) -> dict:
    """Revive a session whose process died (after a daemon restart, a crash,
    or reasonix_stop — history is persisted on disk). Opens a fresh ACP
    process and session/resume's the stored session; poll/send work as before.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    result = await _rpc("resume", {
        "session_id": session_id, "cwd": cwd,
        "keep_alive": keep_alive, "idle_timeout": idle_timeout,
    })
    _remember_session(session_id)
    return result


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
    include_events: list[str] | None = None,
    exclude_events: list[str] | None = None,
    include_thought: bool = common.DEFAULT_INCLUDE_THOUGHT,
    include_full: bool = common.DEFAULT_INCLUDE_FULL,
    max_events: int | None = None,
    ctx: Context | None = None,
) -> dict:
    """Read recent output the agent produced since the last poll.

    Always returns text (delta), turns (completed, with stop_reason), plan,
    current_work, events, status, permission_request. Ordinary polls return a small recent
    event tail; include_events opts event types back in (and gets the larger
    event cap). Static setup events are filtered by default. thought/full_* are
    opt-in (include_thought / include_full). Errors include error_text.
    """
    _capture_relay_ctx(ctx)
    _require_owned(session_id)
    return await _rpc("poll", {
        "session_id": session_id,
        "include_events": include_events, "exclude_events": exclude_events,
        "include_thought": include_thought, "include_full": include_full,
        "max_events": max_events,
    })


@mcp.tool()
async def reasonix_wait(
    session_ids: list[str], timeout: float = 30.0, ctx: Context | None = None
) -> dict:
    """Block until any watched session produces output, finishes a turn, or
    raises a permission request (or `timeout` seconds pass). Returns woke ids,
    per-session status + stop_reason. The authoritative callback — use it as
    the loop's source of truth; the push notification is best-effort.
    """
    _capture_relay_ctx(ctx)
    for session_id in session_ids:
        _require_owned(session_id)
    return await _rpc("wait", {"session_ids": session_ids, "timeout": timeout}, timeout=timeout + 60)


@mcp.tool()
async def reasonix_list(include_task: bool = False, ctx: Context | None = None) -> dict:
    """List sessions with a compact task preview by default.

    Set include_task=true only when the full original orchestration prompt is
    needed; it can be thousands of words.
    """
    _capture_relay_ctx(ctx)
    result = await _rpc("list", {"include_task": include_task})
    result["sessions"] = [
        session for session in result.get("sessions", [])
        if session.get("session_id") in _owned_sessions
    ]
    return result


@mcp.tool()
async def reasonix_models() -> dict:
    """List selectable models: provider/model refs, default, per-model
    supported_efforts, and price hints where the config declares them."""
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
    is one of poll's permission_request.options[].optionId, or "cancel"."""
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
