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

_TEMPLATE_DEFAULTS = {
    "schedule_kind": "daily", "daily_at": "09:00", "interval_minutes": 1440,
    "timezone": "UTC", "overlap_policy": "skip", "delegation": "encouraged",
    "tool_approval": "auto", "work_mode": "balanced", "enabled": True,
}

ROUTINE_TEMPLATES = [
    {
        "template_id": "github-issue-maintainer", "name": "GitHub issue maintainer",
        "category": "triage", "description": "Triages issues and prepares one reviewed fix PR.",
        "prompt": """Inspect currently open GitHub issues and pull requests plus repository state. Do not duplicate an existing issue, branch, PR, or active effort. Select at most one well-scoped, reproducible issue whose fix is appropriate for this repository. Delegate independent reproduction or code investigation when useful, review all findings, implement the smallest robust fix, run focused tests, and open a draft PR with evidence and links. Never merge, close a report, or make unrelated changes.""",
        "work_mode": "delivery",
    },
    {
        "template_id": "pr-review-maintainer", "name": "PR review maintainer",
        "category": "triage", "description": "Reviews active PRs and prepares bounded follow-up fixes.",
        "prompt": """Inspect active pull requests, their review feedback, checks, and current branch state. Pick at most one PR that needs a concrete bounded action. Do not repeat feedback already addressed or duplicate another contributor's work. Reproduce failures, review the changed code for correctness and security, and either report precise actionable findings or prepare a focused follow-up commit on an authorized branch. Run relevant tests. Never approve or merge a PR on behalf of a human.""",
        "work_mode": "delivery",
    },
    {
        "template_id": "crash-fuzzer", "name": "Crash fuzzer",
        "category": "reliability", "description": "Exercises the app and root-causes one real crash.",
        "prompt": """Run the application in its supported local test or simulator environment and explore realistic inputs and interaction sequences for crashes. Preserve reproducibility artifacts and avoid production systems or destructive external actions. If a genuine new crash is found, reduce it to the smallest deterministic reproduction, identify the root cause, add a regression test, implement a focused fix, and prepare a draft PR. If no reproducible crash is found, report coverage and stop without speculative changes.""",
        "work_mode": "delivery",
    },
    {
        "template_id": "duplicate-unifier", "name": "Duplicate unifier",
        "category": "quality", "description": "Consolidates one proven duplicate implementation.",
        "prompt": """Scan for similar implementations that have measurably diverged while representing the same concept. Select at most one high-confidence duplication. Compare semantics, callers, edge cases, and tests before changing anything. Consolidate only when one shared abstraction is clearly simpler and preserves behavior; otherwise report the candidates and stop. Add or update tests and prepare a focused draft PR.""",
    },
    {
        "template_id": "dead-code-remover", "name": "Dead-code remover",
        "category": "cleanup", "description": "Removes one provably unreachable code path.",
        "prompt": """Use static references, build configuration, tests, and runtime evidence available in the repository to identify dead code. Remove at most one bounded target only when it is provably unreachable. Treat reflection, plugins, generated code, public APIs, migrations, and platform-specific paths as live unless proven otherwise. If evidence is insufficient, add narrowly scoped observability when appropriate or report the candidate without deleting it. Run focused tests and prepare a draft PR.""",
    },
    {
        "template_id": "useless-test-pruner", "name": "Useless-test pruner",
        "category": "tests", "description": "Removes or repairs one test that cannot detect failure.",
        "prompt": """Inspect tests for assertions that cannot fail, mocks that bypass the behavior under test, permanently skipped cases, or exact duplicates. Select at most one high-confidence target. Prefer repairing a weak test when it covers meaningful behavior; delete it only when it adds no independent signal. Demonstrate the diagnosis, preserve coverage, run the affected test suite, and prepare a focused draft PR.""",
    },
    {
        "template_id": "flaky-test-fixer", "name": "Flaky-test fixer",
        "category": "tests", "description": "Reproduces and fixes one flaky CI test.",
        "prompt": """Inspect recent CI failures and repository test history for a likely flaky test. Work on at most one candidate. Reproduce the failure repeatedly or establish strong causal evidence; distinguish product races from test-only timing assumptions. Fix the root cause rather than increasing sleeps, retries, or timeouts. Stress the focused test after the change and prepare a draft PR with reproduction and verification evidence. If flakiness cannot be established, report findings without changing code.""",
        "work_mode": "delivery",
    },
    {
        "template_id": "logic-simplifier", "name": "Logic simplifier",
        "category": "quality", "description": "Simplifies one convoluted logic path without changing behavior.",
        "prompt": """Identify one bounded area of unnecessarily convoluted business logic. Establish current behavior from tests, callers, invariants, and edge cases before editing. Simplify control flow or data transformations only when the result is materially easier to reason about and does not broaden scope. Add characterization tests where needed, run focused verification, and prepare a draft PR. Do not combine unrelated cleanup.""",
    },
    {
        "template_id": "logic-bugfinder", "name": "Logic bugfinder",
        "category": "reliability", "description": "Models a tricky invariant and fixes one demonstrated bug.",
        "prompt": """Select one complex, high-risk logic path and model its inputs, state transitions, invariants, and boundary conditions. Use focused tests or a small model to search for a concrete counterexample. Change production code only when a bug is reproducible or strongly demonstrated. Add a regression test, implement the smallest root-cause fix, run relevant verification, and prepare a draft PR. Report a clean result without speculative edits when no bug is established.""",
        "work_mode": "delivery",
    },
    {
        "template_id": "shipped-feature-cleaner", "name": "Shipped-feature cleaner",
        "category": "cleanup", "description": "Removes one fully shipped feature flag safely.",
        "prompt": """Inspect feature flags and rollout configuration for a flag whose shipped state is conclusively permanent across supported environments. Select at most one flag. Verify ownership, rollback expectations, tests, configuration, telemetry references, and both code branches. Remove the obsolete path and flag plumbing only with sufficient evidence, preserve the shipped behavior, run focused tests, and prepare a draft PR. Never alter a live rollout or uncertain flag.""",
    },
    {
        "template_id": "internal-feature-cleaner", "name": "Internal-feature cleaner",
        "category": "cleanup", "description": "Ships or removes one forgotten internal-only feature.",
        "prompt": """Find one bounded internal-only feature or experiment that appears forgotten. Establish ownership, usage, intended outcome, and dependencies from repository evidence and available telemetry. Do not make a product decision without evidence: prepare a safe completion when intent is clear, remove it only when unused and authorized, or report the decision needed. Run focused tests and open a draft PR only for a well-supported mechanical change.""",
    },
    {
        "template_id": "abstraction-reviewer", "name": "Abstraction reviewer",
        "category": "architecture", "description": "Fixes one leaky or over-engineered abstraction.",
        "prompt": """Inspect dependency direction, ownership boundaries, repeated escape hatches, and abstractions that expose implementation details. Select at most one concrete, high-impact layering violation or needless abstraction. Trace callers and compatibility constraints. Make the smallest change that restores a clear boundary or removes indirection, without a broad rewrite. Add or update tests, run focused verification, and prepare a draft PR.""",
    },
]


def templates() -> list[dict]:
    return [{**_TEMPLATE_DEFAULTS, **item} for item in ROUTINE_TEMPLATES]


def template(template_id: str) -> dict:
    value = next((item for item in templates() if item["template_id"] == template_id), None)
    if value is None:
        raise ValueError(f"unknown routine template {template_id!r}")
    return value


def apply_template(template_id: str, overrides: dict) -> dict:
    value = template(template_id)
    value.pop("category", None)
    value.pop("description", None)
    value.pop("template_id", None)
    return {**value, **{key: item for key, item in overrides.items() if item not in (None, "")}}


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
    if data.get("template_id"):
        template(str(data["template_id"]))
        value["template_id"] = str(data["template_id"])
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
