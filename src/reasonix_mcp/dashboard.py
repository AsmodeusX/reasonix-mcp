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
import re
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
import routined
import routines


PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(PACKAGE_DIR, "static")
AGENTD_SCRIPT = os.path.join(PACKAGE_DIR, "agentd.py")
DEFAULT_STATE_FILE = os.path.join(
    os.path.dirname(common.agentd_sock_path()), "dashboard.json"
)
MAX_BODY = 256_000
AGENTD_LINE_LIMIT = 16 * 1024 * 1024
MAX_TRANSCRIPT_MESSAGES = 400
MAX_TRANSCRIPT_TEXT = 12_000
MAX_TOOL_RESULT_TEXT = 8_000
PREVIEW_BYTES = 256_000
_preview_cache: dict[str, tuple[int, int, dict]] = {}
DEFAULT_PASSWORD = os.environ.get("REASONIX_MCP_DASHBOARD_PASSWORD", "CHANGEME")


def dashboard_signature() -> str:
    digest = hashlib.sha256()
    for path in [
        __file__,
        common.__file__,
        routines.__file__,
        routined.__file__,
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


def ensure_routined() -> None:
    try:
        with open(routined.STATE_PATH, encoding="utf-8") as fh:
            pid = int(json.load(fh).get("pid") or 0)
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            command = fh.read().replace(b"\0", b" ").decode(errors="replace")
        if os.path.abspath(routined.__file__) in command:
            return
    except (OSError, ValueError, TypeError):
        pass
    runtime = os.path.dirname(routined.STATE_PATH)
    os.makedirs(runtime, exist_ok=True)
    log = open(os.path.join(runtime, "routined.log"), "ab")
    try:
        subprocess.Popen(
            [sys.executable, os.path.abspath(routined.__file__)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            start_new_session=True, env=dict(os.environ),
        )
    finally:
        log.close()


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


def _clean_user_text(text: str) -> str:
    text = re.sub(
        r"\s*<capability-route\b[^>]*>.*?</capability-route>\s*",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    marker = common.STATUS_PROMPT_MARKER
    if marker in text:
        text = text.split(marker, 1)[0]
    return text.strip()


def _clean_task(text: str) -> str:
    return " ".join(_clean_user_text(text).split())[:500]


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
    runs: list[dict] = []
    current_run: dict | None = None
    owner_id = common.read_session_owner(session_id) or ""
    external_events = common.read_orchestrator_timeline(session_id, owner_id)
    transcript_exists = os.path.isfile(path)
    if not transcript_exists and not external_events:
        return [], {
            "exists": False,
            "transcript_path": path,
            "messages_total": 0,
            "messages_truncated": False,
            "tool_calls": [],
            "tool_calls_total": 0,
            "runs": [],
            "runs_total": 0,
            "timeline_entries": 0,
            "updated_at": None,
        }

    def append_external(event: dict) -> None:
        nonlocal current_run
        if current_run is None:
            current_run = {
                "started_at": event.get("created_at"),
                "entries": [],
            }
            runs.append(current_run)
        current_run["entries"].append({
            "kind": "orchestrator_message",
            "role": "orchestrator",
            "text": str(event.get("message") or "")[-MAX_TRANSCRIPT_TEXT:],
            "truncated": len(str(event.get("message") or "")) > MAX_TRANSCRIPT_TEXT,
            "created_at": event.get("created_at"),
            "delivered": str(event.get("delivered") or "steered"),
            "event_id": str(event.get("event_id") or ""),
        })

    try:
        event_index = 0
        byte_offset = 0
        if transcript_exists:
            transcript_fh = open(path, "rb")
        else:
            transcript_fh = contextlib.nullcontext([])
        with transcript_fh as fh:
            for raw_line in fh:
                while (
                    event_index < len(external_events)
                    and int(external_events[event_index].get("transcript_offset") or 0)
                    <= byte_offset
                ):
                    append_external(external_events[event_index])
                    event_index += 1
                byte_offset += len(raw_line)
                try:
                    row = json.loads(raw_line.decode("utf-8", errors="replace"))
                except (ValueError, TypeError):
                    continue
                role = str(row.get("role") or "")
                if role == "user" and isinstance(row.get("content"), str):
                    text = _clean_user_text(row["content"])
                    if not text:
                        continue
                    current_run = {
                        "started_at": row.get("createdAt"),
                        "entries": [],
                    }
                    runs.append(current_run)
                if current_run is None and role != "system":
                    current_run = {"started_at": None, "entries": []}
                    runs.append(current_run)
                if role in ("user", "assistant") and isinstance(row.get("content"), str):
                    text = row["content"].strip()
                    if role == "user":
                        text = _clean_user_text(text)
                    if text:
                        messages_total += 1
                        entry = {
                            "kind": "message",
                            "role": role,
                            "text": text[-MAX_TRANSCRIPT_TEXT:],
                            "truncated": len(text) > MAX_TRANSCRIPT_TEXT,
                            "created_at": row.get("createdAt"),
                            "work_duration_ms": row.get("workDurationMs"),
                        }
                        messages.append(entry)
                        current_run["entries"].append(entry)
                reasoning = row.get("reasoning_content")
                if role == "assistant" and isinstance(reasoning, str) and reasoning.strip():
                    entry = {
                        "kind": "reasoning",
                        "role": "assistant",
                        "text": reasoning.strip()[-MAX_TRANSCRIPT_TEXT:],
                        "truncated": len(reasoning.strip()) > MAX_TRANSCRIPT_TEXT,
                        "work_duration_ms": row.get("workDurationMs"),
                    }
                    messages.append(entry)
                    current_run["entries"].append(entry)
                for call in row.get("tool_calls") or []:
                    if not isinstance(call, dict):
                        continue
                    tool_calls_total += 1
                    entry = {
                        "kind": "tool_call",
                        "name": str(call.get("name") or "tool"),
                        "arguments": str(call.get("arguments") or "")[:4000],
                        "tool_call_id": str(call.get("id") or ""),
                    }
                    tool_calls.append(entry)
                    current_run["entries"].append(entry)
                if role == "tool" and isinstance(row.get("content"), str):
                    content = row["content"]
                    current_run["entries"].append({
                        "kind": "tool_result",
                        "name": str(row.get("name") or "tool"),
                        "text": content[-MAX_TOOL_RESULT_TEXT:],
                        "truncated": len(content) > MAX_TOOL_RESULT_TEXT,
                        "tool_call_id": str(row.get("tool_call_id") or ""),
                    })
        while event_index < len(external_events):
            append_external(external_events[event_index])
            event_index += 1
    except OSError as exc:
        raise DashboardError(f"cannot read transcript: {exc}", 500) from exc

    # Some Reasonix versions also persist a synthetic user row for a steer.
    # Prefer that native row and suppress the owner-side audit duplicate.
    native_user_texts = [
        entry.get("text", "")
        for run in runs
        for entry in run["entries"]
        if entry.get("kind") == "message" and entry.get("role") == "user"
    ]
    for run in runs:
        run["entries"] = [
            entry
            for entry in run["entries"]
            if not (
                entry.get("kind") == "orchestrator_message"
                and any(
                    entry.get("text") == text
                    or (
                        entry.get("text") in text
                        and "steer" in text.lower()
                    )
                    for text in native_user_texts
                )
            )
        ]
    orchestrator_messages_total = sum(
        entry.get("kind") == "orchestrator_message"
        for run in runs
        for entry in run["entries"]
    )
    kept_runs = runs[-30:]
    kept_entries = sum(len(run["entries"]) for run in kept_runs)
    for index, run in enumerate(kept_runs):
        run["number"] = len(runs) - len(kept_runs) + index + 1
        run["active"] = index == len(kept_runs) - 1
    return list(messages), {
        "exists": transcript_exists,
        "transcript_path": path,
        "messages_total": messages_total + orchestrator_messages_total,
        "messages_truncated": (
            messages_total + orchestrator_messages_total > MAX_TRANSCRIPT_MESSAGES
        ),
        "tool_calls": list(tool_calls),
        "tool_calls_total": tool_calls_total,
        "runs": list(reversed(kept_runs)),
        "runs_total": len(runs),
        "timeline_entries": kept_entries,
        "updated_at": max(
            os.path.getmtime(path) if transcript_exists else 0,
            os.path.getmtime(common.session_timeline_path(session_id))
            if external_events else 0,
        ) or None,
    }


class DashboardState:
    def __init__(self, password: str):
        self.password = password
        self.subscribers: set[asyncio.Queue] = set()
        self.observers: dict[str, asyncio.Task] = {}
        self.permission_requests: dict[str, dict] = {}
        self.timeline_files: dict[str, tuple[int, int]] = {}
        self.routine_files: dict[str, tuple[int, int]] = {}
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
            owners = {
                owner for owner in known_owners() if not owner.startswith("routine-")
            } | {
                routined.routine_owner(value["routine_id"])
                for value in routines.list_all()
            }
            for owner in owners:
                task = self.observers.get(owner)
                if task is None or task.done():
                    self.observers[owner] = asyncio.create_task(self.observe(owner))
            for owner in set(self.observers) - owners:
                self.observers.pop(owner).cancel()
            await self.sync_timeline_files()
            await self.sync_routine_files()
            await asyncio.sleep(3)

    async def sync_routine_files(self) -> None:
        current: dict[str, tuple[int, int]] = {}
        for value in routines.list_all():
            routine_id = str(value.get("routine_id") or "")
            try:
                stat = os.stat(routines.path_for(routine_id))
            except (OSError, ValueError):
                continue
            fingerprint = (stat.st_mtime_ns, stat.st_size)
            current[routine_id] = fingerprint
            previous = self.routine_files.get(routine_id)
            if previous is not None and previous != fingerprint:
                await self.publish({
                    "type": "routine_changed", "routine_id": routine_id,
                })
        for routine_id in set(self.routine_files) - set(current):
            await self.publish({"type": "routine_deleted", "routine_id": routine_id})
        self.routine_files = current

    async def sync_timeline_files(self) -> None:
        current: dict[str, tuple[int, int]] = {}
        suffix = ".orchestrator-timeline.jsonl"
        for path in glob.glob(os.path.join(
            common.reasonix_home(), "sessions", f"*{suffix}"
        )):
            try:
                stat = os.stat(path)
            except OSError:
                continue
            session_id = os.path.basename(path)[:-len(suffix)]
            fingerprint = (stat.st_mtime_ns, stat.st_size)
            current[session_id] = fingerprint
            previous = self.timeline_files.get(session_id)
            if previous is not None and previous != fingerprint:
                await self.publish({
                    "type": "timeline_changed",
                    "session_id": session_id,
                })
        self.timeline_files = current

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
        header[7:], request.app.state.dashboard.password
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
    owners = sorted(owner for owner in known_owners() if not owner.startswith("routine-"))
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
        sessions.sort(
            key=lambda item: (bool(item.get("live")), item.get("updated_at") or 0),
            reverse=True,
        )
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
            "latest_at": max((item.get("updated_at") or 0 for item in sessions), default=0),
            "latest_active_at": max(
                (item.get("updated_at") or 0 for item in sessions if item.get("live")),
                default=0,
            ),
        })
    groups.sort(
        key=lambda group: (
            bool(group["counts"]["current"]),
            group["latest_active_at"],
            group["latest_at"],
        ),
        reverse=True,
    )
    return {"orchestrators": groups, "models": common.available_models()}


async def api_fleet(request: Request) -> Response:
    await require_auth(request)
    return JSONResponse(await fleet_snapshot())


def dashboard_routine(value: dict) -> dict:
    result = routines.public(value, include_prompt=True)
    result["runs"] = result.get("runs", [])[-100:]
    for run in result["runs"]:
        for key in ("message", "error"):
            if isinstance(run.get(key), str) and len(run[key]) > MAX_TRANSCRIPT_TEXT:
                run[key] = run[key][-MAX_TRANSCRIPT_TEXT:]
                run[f"{key}_truncated"] = True
    return result


def resolved_session_config(
    session_id: str, reported_config: object
) -> tuple[dict, bool, str]:
    """Resolve verified config while making legacy estimates explicit."""
    persisted = common.read_session_config(session_id)
    if isinstance(reported_config, dict) and reported_config:
        config = common.write_session_config(session_id, reported_config, replace=True)
        return config, True, "agent"
    required = {"model", "effort", "work_mode", "tool_approval"}
    if required <= persisted.keys():
        return persisted, True, "persisted"
    metadata = common.read_acp_session_config(session_id)
    recovered = {**metadata, **persisted}
    if required <= recovered.keys():
        config = common.write_session_config(session_id, recovered, replace=True)
        return config, True, "session_metadata"
    if persisted:
        return persisted, False, "persisted_partial"
    if metadata:
        return metadata, False, "session_metadata_partial"
    return {}, False, "unavailable"


async def api_routines(request: Request) -> Response:
    await require_auth(request)
    if request.method == "GET":
        return JSONResponse({"routines": [dashboard_routine(v) for v in routines.list_all()]})
    data = await body_json(request)
    template_id = str(data.get("template_id") or "")
    if template_id:
        try:
            data = routines.apply_template(template_id, data)
            data["template_id"] = template_id
        except ValueError as exc:
            raise DashboardError(str(exc)) from exc
    owner = str(data.get("owner_id") or "")
    ordinary_owners = {item for item in known_owners() if not item.startswith("routine-")}
    if owner not in ordinary_owners:
        raise DashboardError("select a known orchestrator owner")
    try:
        value = routines.create(owner, data)
    except ValueError as exc:
        raise DashboardError(str(exc)) from exc
    ensure_routined()
    return JSONResponse(dashboard_routine(value), status_code=201)


async def api_routine_templates(request: Request) -> Response:
    await require_auth(request)
    return JSONResponse({"templates": routines.templates()})


async def api_routine(request: Request) -> Response:
    await require_auth(request)
    routine_id = request.path_params["routine_id"]
    try:
        return JSONResponse(dashboard_routine(routines.get(routine_id)))
    except ValueError as exc:
        raise DashboardError(str(exc), 404) from exc


async def api_routine_action(request: Request) -> Response:
    await require_auth(request)
    routine_id = request.path_params["routine_id"]
    action = request.path_params["action"]
    data = await body_json(request)
    try:
        value = routines.get(routine_id)
        if action == "configure":
            allowed = {
                "name", "prompt", "cwd", "schedule_kind", "interval_minutes",
                "daily_at", "timezone", "overlap_policy", "delegation", "model",
                "effort", "work_mode", "tool_approval", "enabled",
            }
            changes = {key: data[key] for key in allowed if key in data}
            if not changes:
                raise ValueError("provide a setting to change")
            result = dashboard_routine(routines.update(routine_id, None, changes))
        elif action == "run":
            result = routines.trigger(routine_id, None)
            result["routine_id"] = routine_id
        elif action == "stop":
            run = next((item for item in reversed(value.get("runs", []))
                        if item.get("status") in ("starting", "running")), None)
            if not run or not run.get("session_id"):
                raise ValueError("routine has no stoppable active agent")
            result = await agentd_rpc(routined.routine_owner(routine_id), "stop", {
                "session_id": run["session_id"],
            })
            routined.update_run(routine_id, run["run_id"], {
                "status": "canceled", "finished_at": time.time(),
                "stop_reason": "canceled",
            })
        elif action == "delete":
            routines.delete(routine_id, None)
            result = {"deleted": True, "routine_id": routine_id}
        else:
            raise DashboardError("unknown routine action", 404)
    except ValueError as exc:
        raise DashboardError(str(exc), 409) from exc
    ensure_routined()
    await request.app.state.dashboard.publish({
        "type": "routine_changed", "routine_id": routine_id, "action": action,
    })
    return JSONResponse(result)


async def api_session(request: Request) -> Response:
    await require_auth(request)
    session_id = request.path_params["session_id"]
    owner = owner_for_session(session_id)
    preview = transcript_preview(session_id)
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
    historical_config = resolved_session_config(session_id, None)
    if live is not None:
        live["task"] = live.get("task") or preview.get("task") or f"Session {session_id[:8]}"
        live["cwd"] = live.get("cwd") or preview.get("cwd") or ""
        reported_config = live.get("config")
        config, known, source = resolved_session_config(session_id, reported_config)
        live["config"], live["config_known"], live["config_source"] = config, known, source
    return JSONResponse({
        "session_id": session_id,
        "owner_id": owner,
        "session": live or {
            "session_id": session_id,
            "status": "previous",
            "task": preview.get("task") or f"Session {session_id[:8]}",
            "cwd": preview.get("cwd") or "",
            "resumable": transcript["exists"],
            "config": historical_config[0],
            "config_known": historical_config[1],
            "config_source": historical_config[2],
        },
        "messages": messages,
        "transcript": transcript,
    })


async def configure_compat(owner: str, session_id: str, updates: dict) -> dict:
    try:
        result = await agentd_rpc(owner, "configure", {"session_id": session_id, **updates})
        common.clear_session_runtime_override(session_id)
        config = result.get("config") if isinstance(result.get("config"), dict) else {}
        common.write_session_config(session_id, config or updates)
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
        transcript_offset = common.session_transcript_size(session_id)
        result = await agentd_rpc(owner, "send", {
            "session_id": session_id,
            "message": message,
            "expect": str(data.get("expect") or "any"),
        })
        if result.get("delivered") == "steered":
            try:
                common.record_orchestrator_message(
                    session_id, owner, message, result["delivered"], transcript_offset
                )
            except OSError as exc:
                result["timeline_recorded"] = False
                result["timeline_record_error"] = str(exc)
            else:
                result["timeline_recorded"] = True
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


def create_app(password: str) -> Starlette:
    dashboard = DashboardState(password)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette):
        ensure_routined()
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
            Route("/api/routine-templates", api_routine_templates),
            Route("/api/routines", api_routines, methods=["GET", "POST"]),
            Route("/api/routines/{routine_id}", api_routine),
            Route(
                "/api/routines/{routine_id}/{action}",
                api_routine_action, methods=["POST"],
            ),
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
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--open", action="store_true", dest="open_browser")
    args = parser.parse_args()
    if args.host not in ("127.0.0.1", "::1", "localhost"):
        parser.error("non-loopback binding is disabled; use a local reverse proxy with authentication")
    password = args.password
    if not password:
        parser.error("dashboard password cannot be empty")
    if args.port == 0:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        args.port = probe.getsockname()[1]
        probe.close()
    host = "127.0.0.1" if args.host == "localhost" else args.host
    url = f"http://{host}:{args.port}/"
    write_state(args.state_file, {
        "pid": os.getpid(), "host": host, "port": args.port,
        "password": password, "url": url, "started_at": time.time(),
        "source_signature": dashboard_signature(),
    })
    print(f"Reasonix dashboard: {url}", flush=True)
    if args.open_browser:
        webbrowser.open(url)
    try:
        uvicorn.run(create_app(password), host=host, port=args.port, log_level="warning")
    finally:
        with contextlib.suppress(OSError, ValueError, TypeError):
            with open(args.state_file, encoding="utf-8") as fh:
                state = json.load(fh)
            if state.get("pid") == os.getpid():
                os.unlink(args.state_file)


if __name__ == "__main__":
    main()
