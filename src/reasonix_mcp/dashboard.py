#!/usr/bin/env python3
"""Local authenticated Reasonix fleet dashboard.

The browser never talks to agentd directly. This service resolves ownership
from persisted sidecars, opens one non-consuming observer socket per owner,
and exposes a small loopback-only HTTP/SSE API.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict, deque
import contextlib
import glob
import hashlib
import json
import os
import secrets
import socket
import subprocess
import sys
import time
from typing import AsyncIterator
import webbrowser

from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route
import uvicorn

import common


PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(PACKAGE_DIR, "static")
AGENTD_SCRIPT = os.path.join(PACKAGE_DIR, "agentd.py")
DEFAULT_STATE_FILE = os.path.join(
    os.path.dirname(common.agentd_sock_path()), "dashboard.json"
)
MAX_BODY = 256_000
AGENTD_LINE_LIMIT = 16 * 1024 * 1024
MAX_TRANSCRIPT_MESSAGES = 120
MAX_TRANSCRIPT_TEXT = 12_000
PREVIEW_BYTES = 256_000
_preview_cache: dict[str, tuple[int, int, dict]] = {}


def dashboard_signature() -> str:
    digest = hashlib.sha256()
    for path in [
        __file__,
        os.path.join(STATIC_DIR, "dashboard.html"),
        os.path.join(STATIC_DIR, "dashboard.css"),
        os.path.join(STATIC_DIR, "dashboard.js"),
    ]:
        digest.update(os.path.basename(path).encode())
        try:
            with open(path, "rb") as fh:
                digest.update(fh.read())
        except OSError:
            digest.update(b"missing")
    return digest.hexdigest()


class DashboardError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


async def ensure_agentd() -> None:
    path = common.agentd_sock_path()
    try:
        reader, writer = await asyncio.open_unix_connection(
            path, limit=AGENTD_LINE_LIMIT
        )
    except OSError:
        runtime_dir = os.path.dirname(path)
        os.makedirs(runtime_dir, exist_ok=True)
        log = open(os.path.join(runtime_dir, "agentd.log"), "ab")
        subprocess.Popen(
            [sys.executable, AGENTD_SCRIPT, "--sock", path],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            env=dict(os.environ),
        )
        for _ in range(100):
            try:
                reader, writer = await asyncio.open_unix_connection(
                    path, limit=AGENTD_LINE_LIMIT
                )
                break
            except OSError:
                await asyncio.sleep(0.1)
        else:
            raise DashboardError("agentd did not start; inspect agentd.log", 503)
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()


async def agentd_rpc(owner_id: str, method: str, params: dict | None = None) -> dict:
    try:
        reader, writer = await asyncio.open_unix_connection(
            common.agentd_sock_path(), limit=AGENTD_LINE_LIMIT
        )
    except OSError:
        await ensure_agentd()
        try:
            reader, writer = await asyncio.open_unix_connection(
                common.agentd_sock_path(), limit=AGENTD_LINE_LIMIT
            )
        except OSError as exc:
            raise DashboardError(f"agentd unavailable: {exc}", 503) from exc
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {**(params or {}), "owner_id": owner_id},
    }
    try:
        writer.write((json.dumps(request) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=600)
        if not line:
            raise DashboardError("agentd disconnected", 503)
        message = json.loads(line)
        if message.get("error"):
            error = message["error"]
            raise DashboardError(str(error.get("message") or "agentd error"), 409)
        return message.get("result") or {}
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def known_owners() -> set[str]:
    owners: set[str] = set()
    home = common.reasonix_home()
    for path in glob.glob(os.path.join(home, "orchestrators", "*.json")):
        try:
            with open(path, encoding="utf-8") as fh:
                owner = str(json.load(fh).get("owner_id") or "")
            if owner:
                owners.add(owner)
        except (OSError, ValueError, TypeError):
            pass
    for path in glob.glob(os.path.join(home, "sessions", "*.owner.json")):
        try:
            with open(path, encoding="utf-8") as fh:
                owner = str(json.load(fh).get("owner_id") or "")
            if owner:
                owners.add(owner)
        except (OSError, ValueError, TypeError):
            pass
    return owners


def owner_for_session(session_id: str) -> str:
    if not session_id or "/" in session_id or ".." in session_id:
        raise DashboardError("invalid session id")
    owner = common.read_session_owner(session_id)
    if not owner:
        raise DashboardError("session has no recorded orchestrator owner", 404)
    return owner


def _clean_task(text: str) -> str:
    marker = common.STATUS_PROMPT_MARKER
    if marker in text:
        text = text.split(marker, 1)[0]
    return " ".join(text.strip().split())[:500]


def transcript_preview(session_id: str) -> dict:
    path = os.path.join(common.reasonix_home(), "sessions", f"{session_id}.jsonl")
    try:
        stat = os.stat(path)
    except OSError:
        stat = None
    result = {
        "transcript_path": path if stat else None,
        "updated_at": stat.st_mtime if stat else None,
        "task": "",
        "cwd": "",
    }
    if stat is None:
        return result
    cache_key = (stat.st_mtime_ns, stat.st_size)
    cached = _preview_cache.get(path)
    if cached and cached[:2] == cache_key:
        return dict(cached[2])
    try:
        with open(path, "rb") as fh:
            head = fh.read(PREVIEW_BYTES)
        for line in head.decode("utf-8", errors="replace").splitlines():
            try:
                row = json.loads(line)
            except (ValueError, TypeError):
                continue
            role = row.get("role")
            content = row.get("content")
            if role == "system" and isinstance(content, str) and not result["cwd"]:
                marker = 'Current workspace: "'
                if marker in content:
                    result["cwd"] = content.split(marker, 1)[1].split('"', 1)[0]
            elif role == "user" and isinstance(content, str) and not result["task"]:
                result["task"] = _clean_task(content)
    except OSError:
        pass
    if len(_preview_cache) > 4096:
        _preview_cache.clear()
    _preview_cache[path] = (stat.st_mtime_ns, stat.st_size, dict(result))
    return result


def transcript_messages(session_id: str) -> tuple[list[dict], dict]:
    path = os.path.join(common.reasonix_home(), "sessions", f"{session_id}.jsonl")
    messages: deque[dict] = deque(maxlen=MAX_TRANSCRIPT_MESSAGES)
    tool_calls: deque[dict] = deque(maxlen=50)
    messages_total = 0
    tool_calls_total = 0
    if not os.path.isfile(path):
        return messages, {"exists": False, "transcript_path": path}
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except (ValueError, TypeError):
                    continue
                role = str(row.get("role") or "")
                if role in ("user", "assistant") and isinstance(row.get("content"), str):
                    text = row["content"].strip()
                    if role == "user":
                        text = _clean_task(text)
                    if text:
                        messages_total += 1
                        messages.append({
                            "role": role,
                            "text": text[-MAX_TRANSCRIPT_TEXT:],
                            "truncated": len(text) > MAX_TRANSCRIPT_TEXT,
                            "created_at": row.get("createdAt"),
                            "work_duration_ms": row.get("workDurationMs"),
                        })
                for call in row.get("tool_calls") or []:
                    if not isinstance(call, dict):
                        continue
                    tool_calls_total += 1
                    tool_calls.append({
                        "name": str(call.get("name") or "tool"),
                        "arguments": str(call.get("arguments") or "")[:4000],
                    })
    except OSError as exc:
        raise DashboardError(f"cannot read transcript: {exc}", 500) from exc
    return list(messages), {
        "exists": True,
        "transcript_path": path,
        "messages_total": messages_total,
        "messages_truncated": messages_total > MAX_TRANSCRIPT_MESSAGES,
        "tool_calls": list(tool_calls),
        "tool_calls_total": tool_calls_total,
        "updated_at": os.path.getmtime(path),
    }


class DashboardState:
    def __init__(self, token: str):
        self.token = token
        self.subscribers: set[asyncio.Queue] = set()
        self.observers: dict[str, asyncio.Task] = {}
        self.permission_requests: dict[str, dict] = {}
        self.closed = False

    async def publish(self, event: dict) -> None:
        for queue in list(self.subscribers):
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    async def sync_observers(self) -> None:
        while not self.closed:
            owners = known_owners()
            for owner in owners:
                task = self.observers.get(owner)
                if task is None or task.done():
                    self.observers[owner] = asyncio.create_task(self.observe(owner))
            for owner in set(self.observers) - owners:
                self.observers.pop(owner).cancel()
            await asyncio.sleep(3)

    async def observe(self, owner_id: str) -> None:
        while not self.closed:
            writer = None
            try:
                try:
                    reader, writer = await asyncio.open_unix_connection(
                        common.agentd_sock_path(), limit=AGENTD_LINE_LIMIT
                    )
                except OSError:
                    await ensure_agentd()
                    reader, writer = await asyncio.open_unix_connection(
                        common.agentd_sock_path(), limit=AGENTD_LINE_LIMIT
                    )
                writer.write((json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "ping",
                    "params": {"owner_id": owner_id},
                }) + "\n").encode())
                await writer.drain()
                while not self.closed:
                    line = await reader.readline()
                    if not line:
                        break
                    message = json.loads(line)
                    if message.get("method") != "agent_event":
                        continue
                    event = dict(message.get("params") or {})
                    event.pop("_owner_id", None)
                    event["owner_id"] = owner_id
                    session_id = str(event.get("session_id") or "")
                    if await self.resolve_yolo(owner_id, event):
                        self.permission_requests.pop(session_id, None)
                        continue
                    if event.get("event") == "permission_request" and isinstance(
                        event.get("permission_request"), dict
                    ):
                        self.permission_requests[session_id] = event["permission_request"]
                    elif event.get("event") in ("turn_end", "process_exited"):
                        self.permission_requests.pop(session_id, None)
                    await self.publish({"type": "agent_event", "event": event})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.publish({
                    "type": "observer_status", "owner_id": owner_id,
                    "connected": False, "error": str(exc),
                })
            finally:
                if writer is not None:
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()
            await asyncio.sleep(1)

    async def resolve_yolo(self, owner_id: str, event: dict) -> bool:
        if event.get("event") != "permission_request":
            return False
        session_id = str(event.get("session_id") or "")
        override = common.read_session_runtime_override(session_id)
        if override.get("tool_approval") != "yolo":
            return False
        permission = event.get("permission_request") or {}
        tool_call = permission.get("tool_call") or {}
        if str(tool_call.get("kind") or "").lower() == "other":
            return False
        option = allow_once_option(permission)
        if option is None:
            return False
        try:
            await agentd_rpc(owner_id, "respond_permission", {
                "session_id": session_id, "option_id": option,
            })
        except DashboardError:
            return False
        await self.publish({
            "type": "permission_auto_resolved", "session_id": session_id,
            "owner_id": owner_id, "option_id": option,
        })
        return True


def allow_once_option(permission: dict) -> str | None:
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
        elif any(("allow" in value or "approve" in value) and "once" in value for value in values):
            rank = 1
        elif values & {"allow", "approve", "yes"}:
            rank = 2
        else:
            continue
        ranked.append((rank, option_id))
    return min(ranked)[1] if ranked else None


def authenticated(request: Request) -> bool:
    header = request.headers.get("authorization", "")
    return header.startswith("Bearer ") and secrets.compare_digest(
        header[7:], request.app.state.dashboard.token
    )


async def require_auth(request: Request) -> None:
    if not authenticated(request):
        raise DashboardError("unauthorized", 401)


async def body_json(request: Request) -> dict:
    raw = await request.body()
    if len(raw) > MAX_BODY:
        raise DashboardError("request body too large", 413)
    try:
        data = json.loads(raw or b"{}")
    except ValueError as exc:
        raise DashboardError("invalid JSON") from exc
    if not isinstance(data, dict):
        raise DashboardError("JSON body must be an object")
    return data


async def index(_request: Request) -> Response:
    return FileResponse(os.path.join(STATIC_DIR, "dashboard.html"))


async def static_file(request: Request) -> Response:
    name = request.path_params["name"]
    if name not in {"dashboard.css", "dashboard.js"}:
        return Response(status_code=404)
    return FileResponse(os.path.join(STATIC_DIR, name))


async def api_health(request: Request) -> Response:
    await require_auth(request)
    return JSONResponse({
        "ok": True,
        "pid": os.getpid(),
        "source_signature": dashboard_signature(),
        "agentd_socket": common.agentd_sock_path(),
    })


async def fleet_snapshot() -> dict:
    owners = sorted(known_owners())
    live_by_owner: dict[str, dict[str, dict]] = defaultdict(dict)
    sidecars_by_owner: dict[str, set[str]] = defaultdict(set)
    for path in glob.glob(os.path.join(common.reasonix_home(), "sessions", "*.owner.json")):
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            if data.get("owner_id") and data.get("session_id"):
                sidecars_by_owner[str(data["owner_id"])].add(str(data["session_id"]))
        except (OSError, ValueError, TypeError):
            pass

    async def load(owner: str) -> None:
        try:
            listing = await agentd_rpc(owner, "list", {
                "include_task": False,
                "include_permission_detail": True,
            })
            live_by_owner[owner] = {
                str(item["session_id"]): item for item in listing.get("sessions", [])
            }
        except DashboardError:
            live_by_owner[owner] = {}

    await asyncio.gather(*(load(owner) for owner in owners))
    groups = []
    for owner in owners:
        ids = (
            set(common.load_owner_sessions(owner))
            | set(live_by_owner[owner])
            | sidecars_by_owner[owner]
        )
        sessions = []
        for session_id in ids:
            preview = transcript_preview(session_id)
            live = live_by_owner[owner].get(session_id)
            item = {
                key: live.get(key)
                for key in (
                    "session_id", "status", "cwd", "task", "stop_reason",
                    "resumable", "error", "error_text",
                )
                if live is not None and live.get(key) is not None
            }
            if live is not None:
                item["permission_request"] = bool(live.get("permission_request"))
            item.update({
                "session_id": session_id,
                "owner_id": owner,
                "live": live is not None and item.get("status") != "exited",
                "historical": live is None or item.get("status") == "exited",
                "task": item.get("task") or preview["task"] or f"Session {session_id[:8]}",
                "cwd": item.get("cwd") or preview["cwd"],
                "updated_at": preview["updated_at"],
            })
            if live is None:
                item.setdefault("status", "previous")
                item["resumable"] = bool(preview["transcript_path"])
            sessions.append(item)
        sessions.sort(key=lambda item: (item.get("updated_at") or 0), reverse=True)
        projects = sorted({item.get("cwd") for item in sessions if item.get("cwd")})
        groups.append({
            "owner_id": owner,
            "label": f"Orchestrator {owner.removeprefix('owner-')[:8]}",
            "projects": projects,
            "sessions": sessions,
            "counts": {
                "running": sum(item.get("status") == "running" for item in sessions),
                "current": sum(bool(item.get("live")) for item in sessions),
                "previous": sum(bool(item.get("historical")) for item in sessions),
            },
        })
    return {"orchestrators": groups, "models": common.available_models()}


async def api_fleet(request: Request) -> Response:
    await require_auth(request)
    return JSONResponse(await fleet_snapshot())


async def api_session(request: Request) -> Response:
    await require_auth(request)
    session_id = request.path_params["session_id"]
    owner = owner_for_session(session_id)
    messages, transcript = transcript_messages(session_id)
    live = None
    try:
        listing = await agentd_rpc(owner, "list", {
            "include_task": False,
            "include_permission_detail": True,
        })
        live = next(
            (item for item in listing.get("sessions", []) if item.get("session_id") == session_id),
            None,
        )
    except DashboardError:
        pass
    if live and live.get("permission_request") is True:
        cached = request.app.state.dashboard.permission_requests.get(session_id)
        if cached:
            live["permission_request"] = cached
    return JSONResponse({
        "session_id": session_id,
        "owner_id": owner,
        "session": live or {
            "session_id": session_id,
            "status": "previous",
            "resumable": transcript["exists"],
            "config": common.read_session_config(session_id),
        },
        "messages": messages,
        "transcript": transcript,
    })


async def configure_compat(owner: str, session_id: str, updates: dict) -> dict:
    try:
        result = await agentd_rpc(owner, "configure", {"session_id": session_id, **updates})
        common.clear_session_runtime_override(session_id)
        return result
    except DashboardError as exc:
        if "unknown method 'configure'" not in str(exc):
            raise
    common.write_session_config(session_id, updates)
    if updates.get("tool_approval") == "yolo":
        state = await agentd_rpc(owner, "ping", {})
        common.write_session_runtime_override(session_id, {
            "tool_approval": "yolo",
            "daemon_code_signature": state.get("code_signature"),
            "created_at": time.time(),
        })
        detail = await agentd_rpc(owner, "poll", {"session_id": session_id, "detail": True})
        option = allow_once_option(detail.get("permission_request") or {})
        if option:
            await agentd_rpc(owner, "respond_permission", {
                "session_id": session_id, "option_id": option,
            })
        return {
            "session_id": session_id,
            "queued": True,
            "applies": "after_agentd_reload",
            "runtime_emulation": "yolo",
            "permission_resolution": option,
            "compatibility_fallback": "stale_agentd",
        }
    common.clear_session_runtime_override(session_id)
    return {
        "session_id": session_id,
        "queued": True,
        "applies": "after_agentd_reload",
        "waiting_options": sorted(updates),
        "compatibility_fallback": "stale_agentd",
    }


async def api_action(request: Request) -> Response:
    await require_auth(request)
    session_id = request.path_params["session_id"]
    action = request.path_params["action"]
    owner = owner_for_session(session_id)
    data = await body_json(request)
    if action == "send":
        message = str(data.get("message") or "").strip()
        if not message:
            raise DashboardError("message is required")
        result = await agentd_rpc(owner, "send", {
            "session_id": session_id,
            "message": message,
            "expect": str(data.get("expect") or "any"),
        })
    elif action == "configure":
        updates = {
            key: str(data.get(key) or "").strip()
            for key in ("model", "effort", "work_mode", "tool_approval")
            if str(data.get(key) or "").strip()
        }
        if not updates:
            raise DashboardError("one configuration option is required")
        result = await configure_compat(owner, session_id, updates)
    elif action == "permission":
        option = str(data.get("option_id") or "")
        if not option:
            raise DashboardError("option_id is required")
        result = await agentd_rpc(owner, "respond_permission", {
            "session_id": session_id, "option_id": option,
        })
    elif action == "stop":
        result = await agentd_rpc(owner, "stop", {"session_id": session_id})
    elif action == "resume":
        result = await agentd_rpc(owner, "resume", {
            "session_id": session_id,
            "cwd": str(data.get("cwd") or ""),
            "model": str(data.get("model") or ""),
            "effort": str(data.get("effort") or ""),
            "work_mode": str(data.get("work_mode") or ""),
            "tool_approval": str(data.get("tool_approval") or ""),
            "keep_alive": bool(data.get("keep_alive", False)),
            "idle_timeout": float(data.get("idle_timeout", common.DEFAULT_IDLE_TIMEOUT)),
        })
    else:
        raise DashboardError("unknown action", 404)
    await request.app.state.dashboard.publish({
        "type": "action", "session_id": session_id, "action": action,
    })
    return JSONResponse(result)


async def api_events(request: Request) -> Response:
    await require_auth(request)
    state: DashboardState = request.app.state.dashboard
    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    state.subscribers.add(queue)

    async def stream() -> AsyncIterator[bytes]:
        try:
            yield b"event: ready\ndata: {}\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield b": heartbeat\n\n"
                    continue
                yield ("event: update\ndata: " + json.dumps(event) + "\n\n").encode()
        finally:
            state.subscribers.discard(queue)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-store",
        "X-Accel-Buffering": "no",
    })


async def error_handler(_request: Request, exc: Exception) -> Response:
    if isinstance(exc, DashboardError):
        return JSONResponse({"error": str(exc)}, status_code=exc.status)
    return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=500)


def create_app(token: str) -> Starlette:
    dashboard = DashboardState(token)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette):
        dashboard.sync_task = asyncio.create_task(dashboard.sync_observers())
        try:
            yield
        finally:
            dashboard.closed = True
            dashboard.sync_task.cancel()
            for task in dashboard.observers.values():
                task.cancel()
            await asyncio.gather(
                dashboard.sync_task, *dashboard.observers.values(), return_exceptions=True
            )

    app = Starlette(
        routes=[
            Route("/", index),
            Route("/static/{name}", static_file),
            Route("/api/health", api_health),
            Route("/api/fleet", api_fleet),
            Route("/api/events", api_events),
            Route("/api/sessions/{session_id}", api_session),
            Route("/api/sessions/{session_id}/{action}", api_action, methods=["POST"]),
        ],
        lifespan=lifespan,
        exception_handlers={DashboardError: error_handler, Exception: error_handler},
    )
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
            "style-src 'self'; script-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        if request.url.path == "/" or request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response
    app.add_middleware(BaseHTTPMiddleware, dispatch=security_headers)
    app.state.dashboard = dashboard
    return app


def write_state(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reasonix local fleet dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8746)
    parser.add_argument("--token", default="")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--open", action="store_true", dest="open_browser")
    args = parser.parse_args()
    if args.host not in ("127.0.0.1", "::1", "localhost"):
        parser.error("non-loopback binding is disabled; use a local reverse proxy with authentication")
    token = args.token or secrets.token_urlsafe(32)
    if args.port == 0:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        args.port = probe.getsockname()[1]
        probe.close()
    host = "127.0.0.1" if args.host == "localhost" else args.host
    url = f"http://{host}:{args.port}/#token={token}"
    write_state(args.state_file, {
        "pid": os.getpid(), "host": host, "port": args.port,
        "token": token, "url": url, "started_at": time.time(),
        "source_signature": dashboard_signature(),
    })
    print(f"Reasonix dashboard: {url}", flush=True)
    if args.open_browser:
        webbrowser.open(url)
    try:
        uvicorn.run(create_app(token), host=host, port=args.port, log_level="warning")
    finally:
        with contextlib.suppress(OSError, ValueError, TypeError):
            with open(args.state_file, encoding="utf-8") as fh:
                state = json.load(fh)
            if state.get("pid") == os.getpid():
                os.unlink(args.state_file)


if __name__ == "__main__":
    main()
