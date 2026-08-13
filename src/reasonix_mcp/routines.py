"""Persistent routine definitions and run history shared by all frontends."""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta
import fcntl
import json
import os
import re
import time
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import common

MAX_RUN_HISTORY = 100
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,79}$")


def routine_dir() -> str:
    return os.path.join(os.path.dirname(common.agentd_sock_path()), "routines")


def lock_path() -> str:
    return os.path.join(routine_dir(), ".lock")


@contextlib.contextmanager
def locked():
    os.makedirs(routine_dir(), mode=0o700, exist_ok=True)
    fd = os.open(lock_path(), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def path_for(routine_id: str) -> str:
    if not re.fullmatch(r"[0-9a-f-]{36}", routine_id):
        raise ValueError("invalid routine id")
    return os.path.join(routine_dir(), f"{routine_id}.json")


def _read(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as fh:
            value = json.load(fh)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, TypeError):
        return None


def _write(value: dict) -> None:
    path = path_for(value["routine_id"])
    tmp = f"{path}.tmp-{os.getpid()}-{time.time_ns()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(value, fh, sort_keys=True)
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def list_all() -> list[dict]:
    os.makedirs(routine_dir(), mode=0o700, exist_ok=True)
    values = []
    for name in os.listdir(routine_dir()):
        if not name.endswith(".json"):
            continue
        value = _read(os.path.join(routine_dir(), name))
        if value:
            values.append(value)
    return sorted(values, key=lambda item: item.get("updated_at", 0), reverse=True)


def get(routine_id: str) -> dict:
    value = _read(path_for(routine_id))
    if not value:
        raise ValueError("unknown routine")
    return value


def _timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone {name!r}") from exc


def next_scheduled(value: dict, after: float | None = None) -> float | None:
    after = time.time() if after is None else after
    kind = value["schedule_kind"]
    if kind == "manual":
        return None
    if kind == "interval":
        return after + int(value["interval_minutes"]) * 60
    tz = _timezone(value["timezone"])
    local = datetime.fromtimestamp(after, tz)
    hour, minute = map(int, value["daily_at"].split(":"))
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate.timestamp() <= after:
        candidate += timedelta(days=1)
    return candidate.timestamp()


def validate(data: dict) -> dict:
    name = str(data.get("name") or "").strip()
    prompt = str(data.get("prompt") or "").strip()
    cwd = os.path.realpath(str(data.get("cwd") or os.getcwd()))
    kind = str(data.get("schedule_kind") or "daily")
    if not NAME_RE.fullmatch(name):
        raise ValueError("name must be 1-80 simple characters")
    if not prompt or len(prompt) > 100_000:
        raise ValueError("prompt must be 1-100000 characters")
    if not os.path.isdir(cwd):
        raise ValueError(f"working directory does not exist: {cwd}")
    if not common.cwd_allowed(cwd):
        raise ValueError(
            f"working directory is outside the allowed spawn roots: {cwd}"
        )
    if kind not in ("manual", "interval", "daily"):
        raise ValueError("schedule_kind must be manual, interval, or daily")
    interval = int(data.get("interval_minutes") or 1440)
    if not 1 <= interval <= 525_600:
        raise ValueError("interval_minutes must be between 1 and 525600")
    daily_at = str(data.get("daily_at") or "09:00")
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", daily_at):
        raise ValueError("daily_at must be HH:MM")
    timezone = str(data.get("timezone") or os.environ.get("TZ") or "UTC")
    _timezone(timezone)
    overlap = str(data.get("overlap_policy") or "skip")
    if overlap not in ("skip", "queue"):
        raise ValueError("overlap_policy must be skip or queue")
    delegation = str(data.get("delegation") or "allowed")
    if delegation not in ("disabled", "allowed", "encouraged"):
        raise ValueError("delegation must be disabled, allowed, or encouraged")
    work_mode = str(data.get("work_mode") or common.DEFAULT_WORK_MODE or "balanced")
    if work_mode not in ("economy", "balanced", "delivery"):
        raise ValueError("work_mode must be economy, balanced, or delivery")
    tool_approval = str(data.get("tool_approval") or "auto")
    if tool_approval not in ("ask", "auto", "yolo"):
        raise ValueError("tool_approval must be ask, auto, or yolo")
    return {
        "name": name, "prompt": prompt, "cwd": cwd,
        "schedule_kind": kind, "interval_minutes": interval,
        "daily_at": daily_at, "timezone": timezone,
        "overlap_policy": overlap, "delegation": delegation,
        "model": str(data.get("model") or common.DEFAULT_MODEL),
        "effort": str(data.get("effort") or common.DEFAULT_EFFORT),
        "work_mode": work_mode,
        # Scheduled work is unattended. Start at the policy-driven posture;
        # users can explicitly opt a routine into ask or yolo.
        "tool_approval": tool_approval,
        "enabled": bool(data.get("enabled", True)),
    }


def create(owner_id: str, data: dict) -> dict:
    clean = validate(data)
    now = time.time()
    value = {
        **clean, "routine_id": str(uuid.uuid4()), "owner_id": owner_id,
        "created_at": now, "updated_at": now, "runs": [],
        "pending_triggers": 1 if data.get("run_immediately") else 0,
    }
    value["next_run_at"] = next_scheduled(value, now) if value["enabled"] else None
    with locked():
        _write(value)
    return value


def update(routine_id: str, owner_id: str | None, changes: dict) -> dict:
    with locked():
        value = get(routine_id)
        if owner_id is not None and value["owner_id"] != owner_id:
            raise ValueError("routine is owned by another orchestrator")
        merged = {**value, **changes}
        clean = validate(merged)
        value.update(clean)
        value["updated_at"] = time.time()
        schedule_keys = {
            "schedule_kind", "interval_minutes", "daily_at", "timezone", "enabled"
        }
        if schedule_keys & changes.keys():
            value["next_run_at"] = next_scheduled(value) if value["enabled"] else None
        _write(value)
        return value


def trigger(routine_id: str, owner_id: str | None = None) -> dict:
    with locked():
        value = get(routine_id)
        if owner_id is not None and value["owner_id"] != owner_id:
            raise ValueError("routine is owned by another orchestrator")
        active = any(run["status"] in ("starting", "running") for run in value["runs"])
        if active and value["overlap_policy"] == "skip":
            return {"queued": False, "skipped": True, "reason": "already_running"}
        value["pending_triggers"] = min(10, int(value.get("pending_triggers", 0)) + 1)
        value["updated_at"] = time.time()
        _write(value)
        return {"queued": True, "pending_triggers": value["pending_triggers"]}


def delete(routine_id: str, owner_id: str | None) -> None:
    with locked():
        value = get(routine_id)
        if owner_id is not None and value["owner_id"] != owner_id:
            raise ValueError("routine is owned by another orchestrator")
        if any(run["status"] in ("starting", "running") for run in value["runs"]):
            raise ValueError("routine has a running agent; pause it and wait or stop the run")
        os.unlink(path_for(routine_id))


def public(value: dict, include_prompt: bool = False) -> dict:
    result = dict(value)
    if not include_prompt:
        result["prompt"] = common.task_preview(result.get("prompt"))
    return result
