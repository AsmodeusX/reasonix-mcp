#!/usr/bin/env python3
"""agentd — detached Reasonix agent daemon.

Owns the `reasonix acp` subprocesses so agents **survive MCP server restarts**:
Claude Code (and its stdio MCP server) can die and come back; the daemon keeps
the agents running, and a new server reconnects and sees them via reasonix_list.

Wire protocol over a Unix socket: NDJSON JSON-RPC 2.0, one frame per line.
  server -> daemon:  {"jsonrpc":"2.0","id":N,"method":"spawn","params":{...}}
  daemon -> server:  {"jsonrpc":"2.0","id":N,"result":{...}}  (or "error")
  daemon -> server:  {"method":"agent_event","params":{...}}   (push; best-effort)

Methods mirror the MCP tools (spawn/send/poll/wait/list/models/transcript/
respond_permission/stop/resume). The daemon holds the authoritative session
registry; agents carry PDEATHSIG so they die with the daemon, never orphan.

Started detached by server.py (or manually: `python agentd.py --sock PATH`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import threading
import time

import acp_bridge
import common


class AgentdError(Exception):
    def __init__(self, message: str, code: int = -32000):
        super().__init__(message)
        self.code = code


class Agentd:
    def __init__(self):
        self._registry: dict[str, acp_bridge.ReasonixAgent] = {}
        self._lock = threading.Lock()
        self._clients: set[asyncio.StreamWriter] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------- registry

    def _get(self, session_id: str) -> acp_bridge.ReasonixAgent:
        with self._lock:
            agent = self._registry.get(session_id)
        if agent is None:
            raise AgentdError(f"unknown session_id {session_id!r} — did you call reasonix_spawn first?")
        return agent

    def _register(self, agent: acp_bridge.ReasonixAgent) -> None:
        if self._loop is not None:
            def _broadcast(kind, payload, agent=agent):
                try:
                    event = common.build_agent_event(agent, kind, payload)
                    asyncio.run_coroutine_threadsafe(self._broadcast(event), self._loop)
                except Exception:
                    pass
            agent.on_event = _broadcast
        with self._lock:
            self._registry[agent.session_id] = agent

    async def _broadcast(self, event: dict) -> None:
        line = json.dumps({"method": "agent_event", "params": event}) + "\n"
        dead = []
        for w in self._clients:
            try:
                w.write(line.encode())
                await w.drain()
            except Exception:
                dead.append(w)
        for w in dead:
            self._clients.discard(w)

    # ---------------------------------------------------------------- methods

    def spawn(self, params: dict) -> dict:
        task = params.get("task", "")
        cwd = params.get("cwd") or os.getcwd()
        if not task.strip():
            raise AgentdError("task must not be empty")
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

        agent = acp_bridge.ReasonixAgent(
            cwd=cwd,
            model=params.get("model", common.DEFAULT_MODEL),
            work_mode=work_mode,
            tool_approval=tool_approval,
            effort=params.get("effort", common.DEFAULT_EFFORT),
        )
        agent.task = task
        agent.cwd = cwd
        self._register(agent)
        agent.start_turn(task)
        result: dict = {
            "session_id": agent.session_id,
            "status": "running",
            "cwd": cwd,
            "sandbox": common.sandbox_posture(),
            "note": ("Agent runs detached in agentd and survives MCP server restarts. "
                     "It will push a reasonix/agent_event notification when done "
                     "(turn end / permission needed / exit); reasonix_wait is the "
                     "guaranteed callback. reasonix_poll for output, "
                     "reasonix_send to steer mid-turn, reasonix_resume to revive "
                     "a stopped session."),
        }
        notes = getattr(agent, "spawn_notes", None) or {}
        if notes.get("skipped_options"):
            result["skipped_options"] = notes["skipped_options"]
            result["skipped_options_note"] = (
                "these config options are not advertised by the selected model and "
                "were not applied (e.g. effort is often baked into the model id — "
                "pick a -low/-high/-max variant instead)"
            )
        return result

    def resume(self, params: dict) -> dict:
        session_id = params.get("session_id", "")
        cwd = params.get("cwd") or ""
        with self._lock:
            existing = self._registry.get(session_id)
        if existing is None:
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{session_id}.jsonl")
            if not os.path.isfile(persisted):
                raise AgentdError(f"no live or persisted session {session_id!r} to resume")
        if not cwd:
            cwd = getattr(existing, "cwd", None) or os.getcwd()
        agent = acp_bridge.ReasonixAgent(cwd=cwd, resume_session_id=session_id)
        agent.task = getattr(existing, "task", None) or f"resumed {session_id}"
        agent.cwd = cwd
        self._register(agent)
        return {
            "session_id": session_id,
            "status": common.agent_status(agent),
            "cwd": cwd,
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
        agent = self._get(session_id)
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
                agent.start_turn(message)
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
        agent = self._get(params["session_id"])
        return common.shape_poll(
            agent,
            include_events=params.get("include_events"),
            exclude_events=params.get("exclude_events"),
            include_thought=params.get("include_thought", common.DEFAULT_INCLUDE_THOUGHT),
            include_full=params.get("include_full", common.DEFAULT_INCLUDE_FULL),
        )

    async def wait(self, params: dict) -> dict:
        session_ids = params.get("session_ids", [])
        timeout = float(params.get("timeout", 30.0))
        if not session_ids:
            raise AgentdError("session_ids must not be empty")
        if len(session_ids) > common.MAX_WATCH_SESSIONS:
            raise AgentdError(f"too many sessions (max {common.MAX_WATCH_SESSIONS})")
        timeout = max(0.0, min(timeout, 3600.0))
        agents = {sid: self._get(sid) for sid in dict.fromkeys(session_ids)}
        baselines = {sid: a.event_seq for sid, a in agents.items()}
        deadline = time.monotonic() + timeout

        while True:
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
                }
                if has_new or pending or status != "running":
                    woke.append(sid)
            if woke or time.monotonic() >= deadline:
                return {"woke": woke, "timed_out": not bool(woke), "sessions": states}
            await asyncio.sleep(0.2)

    def list(self, params: dict | None = None) -> dict:
        with self._lock:
            items = list(self._registry.items())
        sessions = []
        for sid, a in items:
            status = common.agent_status(a)
            persisted = os.path.join(common.reasonix_home(), "sessions", f"{sid}.jsonl")
            sessions.append({
                "session_id": sid,
                "status": status,
                "cwd": getattr(a, "cwd", None),
                "task": getattr(a, "task", None),
                "stop_reason": a.stop_reason,
                "transcript_path": getattr(a, "transcript_path", None),
                "permission_request": a._pending_permission is not None,
                "resumable": status in ("idle", "exited") and os.path.isfile(persisted),
            })
        return {"sessions": sessions}

    def models(self, params: dict | None = None) -> dict:
        return common.available_models()

    def transcript(self, params: dict) -> dict:
        agent = self._get(params["session_id"])
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
        agent = self._get(params["session_id"])
        option = None if params.get("option_id") == "cancel" else params.get("option_id")
        answered = agent.answer_permission(option)
        if not answered:
            raise AgentdError(f"agent {params['session_id']} has no pending permission request")
        return {"session_id": params["session_id"], "answered": params.get("option_id")}

    def stop(self, params: dict) -> dict:
        agent = self._get(params["session_id"])
        agent.cancel_turn()
        agent.close()
        return {"session_id": params["session_id"], "stopped": True}

    def ping(self, params: dict | None = None) -> dict:
        with self._lock:
            return {"sessions": len(self._registry)}

    def shutdown(self, params: dict | None = None) -> dict:
        """Close every agent and stop the daemon (used by tests; the real
        deployment just leaves the daemon running)."""
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
        return {"shutdown": True}

    # ------------------------------------------------------------- dispatch

    async def dispatch(self, msg: dict, writer: asyncio.StreamWriter) -> None:
        method = msg.get("method", "")
        params = msg.get("params") or {}
        try:
            if method == "wait":
                result = await self.wait(params)
            else:
                handler = getattr(self, method, None)
                if handler is None or not callable(handler):
                    raise AgentdError(f"unknown method {method!r}", -32601)
                result = handler(params)
                if asyncio.iscoroutine(result):
                    result = await result
            self._send(writer, {"jsonrpc": "2.0", "id": msg.get("id"), "result": result})
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
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if msg.get("method") and "id" in msg:
                    asyncio.create_task(self.dispatch(msg, writer))
        finally:
            self._clients.discard(writer)
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
        async with server:
            print(f"agentd listening on {sock_path}", flush=True)
            await self._serve_stop.wait()
            server.close()
            await server.wait_closed()


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


if __name__ == "__main__":
    main()
