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
  reasonix_respond_permission / reasonix_stop / reasonix_restart_agentd
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
a permission decision, or exits; terminal errors include error_text. The
custom notification may be dropped by the client, so reasonix_wait/poll are the
guaranteed control path. Use reasonix_restart_agentd after updating this
server; it reloads the daemon once no live agents remain. Keep
orchestration state in the parent agent. Completed agents are automatically
cleaned up after their idle grace period unless spawned with keep_alive=true;
do not claim a child is complete
until its poll reports idle/exited status and a stop reason."""

mcp = MCPServer("reasonix", instructions=MCP_INSTRUCTIONS)

NOTIFY_ENABLED = os.environ.get("REASONIX_MCP_NOTIFY", "1").strip().lower() not in ("0", "false", "no", "off")
AGENT_EVENT_METHOD = "reasonix/agent_event"
AGENTD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agentd.py")

_agentd_reader: asyncio.StreamReader | None = None
_agentd_writer: asyncio.StreamWriter | None = None
_pending: dict[int, asyncio.Future] = {}
_next_id = itertools.count(1)
_mcp_session = None  # captured on first tool call, used to relay daemon pushes
_rpc_default_timeout = 600.0


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
    global _mcp_session
    if ctx is not None and _mcp_session is None:
        try:
            _mcp_session = ctx.session
        except Exception:
            pass


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
    return await _rpc("spawn", {
        "task": task, "cwd": cwd, "model": model,
        "work_mode": work_mode, "tool_approval": tool_approval, "effort": effort,
        "keep_alive": keep_alive, "idle_timeout": idle_timeout,
    })


@mcp.tool()
async def reasonix_resume(
    session_id: str,
    cwd: str = "",
    keep_alive: bool = False,
    idle_timeout: float = common.DEFAULT_IDLE_TIMEOUT,
) -> dict:
    """Revive a session whose process died (after a daemon restart, a crash,
    or reasonix_stop — history is persisted on disk). Opens a fresh ACP
    process and session/resume's the stored session; poll/send work as before.
    """
    return await _rpc("resume", {
        "session_id": session_id, "cwd": cwd,
        "keep_alive": keep_alive, "idle_timeout": idle_timeout,
    })


@mcp.tool()
async def reasonix_send(session_id: str, message: str, expect: str = "any") -> dict:
    """Send a message to a running agent — always delivered (forced steer).

    Steer into the active turn (`_reasonix.io/session/steer`), or start a new
    turn if idle. expect: any|steer|new_turn; a refused call has no side effect.
    """
    return await _rpc("send", {"session_id": session_id, "message": message, "expect": expect})


@mcp.tool()
async def reasonix_poll(
    session_id: str,
    include_events: list[str] | None = None,
    exclude_events: list[str] | None = None,
    include_thought: bool = common.DEFAULT_INCLUDE_THOUGHT,
    include_full: bool = common.DEFAULT_INCLUDE_FULL,
    max_events: int | None = None,
) -> dict:
    """Read recent output the agent produced since the last poll.

    Always returns text (delta), turns (completed, with stop_reason), plan,
    events, status, permission_request. Ordinary polls return a small recent
    event tail; include_events opts event types back in (and gets the larger
    event cap). Static setup events are filtered by default. thought/full_* are
    opt-in (include_thought / include_full). Errors include error_text.
    """
    return await _rpc("poll", {
        "session_id": session_id,
        "include_events": include_events, "exclude_events": exclude_events,
        "include_thought": include_thought, "include_full": include_full,
        "max_events": max_events,
    })


@mcp.tool()
async def reasonix_wait(session_ids: list[str], timeout: float = 30.0) -> dict:
    """Block until any watched session produces output, finishes a turn, or
    raises a permission request (or `timeout` seconds pass). Returns woke ids,
    per-session status + stop_reason. The authoritative callback — use it as
    the loop's source of truth; the push notification is best-effort.
    """
    return await _rpc("wait", {"session_ids": session_ids, "timeout": timeout}, timeout=timeout + 60)


@mcp.tool()
async def reasonix_list(include_task: bool = False) -> dict:
    """List sessions with a compact task preview by default.

    Set include_task=true only when the full original orchestration prompt is
    needed; it can be thousands of words.
    """
    return await _rpc("list", {"include_task": include_task})


@mcp.tool()
async def reasonix_models() -> dict:
    """List selectable models: provider/model refs, default, per-model
    supported_efforts, and price hints where the config declares them."""
    return await _rpc("models", {})


@mcp.tool()
async def reasonix_transcript(session_id: str, max_tool_calls: int = 200) -> dict:
    """What the agent actually did: tool calls with args, files touched,
    roles, work duration, last text. NB: Reasonix does not persist token/cost
    usage — these are activity metrics."""
    return await _rpc("transcript", {"session_id": session_id, "max_tool_calls": max_tool_calls})


@mcp.tool()
async def reasonix_respond_permission(session_id: str, option_id: str) -> dict:
    """Answer the agent's pending tool-approval or question request. option_id
    is one of poll's permission_request.options[].optionId, or "cancel"."""
    return await _rpc("respond_permission", {"session_id": session_id, "option_id": option_id})


@mcp.tool()
async def reasonix_stop(session_id: str) -> dict:
    """Cancel the active turn, close the ACP session, kill the agent process.
    The session stays listed as a tombstone and is resumable via
    reasonix_resume (its transcript is persisted)."""
    return await _rpc("stop", {"session_id": session_id})


@mcp.tool()
async def reasonix_restart_agentd(force: bool = False) -> dict:
    """Restart the detached agent daemon so it loads updated code.

    By default this refuses while live agents exist. Set force=true only when
    terminating those agents is acceptable; their transcripts remain
    resumable. The next Reasonix tool call automatically starts the fresh
    daemon.
    """
    global _agentd_reader, _agentd_writer
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
                "the running agentd predates restart support and has "
                f"{sessions} session(s); stop them first or retry with force=true"
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


def main() -> None:
    asyncio.run(mcp.run_stdio_async())


if __name__ == "__main__":
    main()
