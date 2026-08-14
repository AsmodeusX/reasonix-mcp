#!/usr/bin/env python3
"""Durable scheduler that launches every routine iteration in a fresh session."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid

import common
import routines

AGENTD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agentd.py")
STATE_PATH = os.path.join(os.path.dirname(common.agentd_sock_path()), "routined.json")
PROCESS_LOCK = os.path.join(os.path.dirname(common.agentd_sock_path()), "routined.lock")
MAX_CONCURRENT_RUNS = max(1, int(os.environ.get("REASONIX_MCP_ROUTINE_CONCURRENCY", "4")))


def code_signature() -> str:
    digest = hashlib.sha256()
    for path in (__file__, routines.__file__, common.__file__):
        digest.update(os.path.basename(path).encode())
        try:
            with open(path, "rb") as fh:
                digest.update(fh.read())
        except OSError:
            digest.update(b"missing")
    return digest.hexdigest()


async def ensure_agentd() -> None:
    sock = common.agentd_sock_path()
    try:
        reader, writer = await asyncio.open_unix_connection(sock)
    except OSError:
        runtime = os.path.dirname(sock)
        os.makedirs(runtime, exist_ok=True)
        log = open(os.path.join(runtime, "agentd.log"), "ab")
        try:
            subprocess.Popen(
                [sys.executable, AGENTD_SCRIPT, "--sock", sock],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                start_new_session=True, env=dict(os.environ),
            )
        finally:
            log.close()
        for _ in range(100):
            try:
                reader, writer = await asyncio.open_unix_connection(sock)
                break
            except OSError:
                await asyncio.sleep(0.1)
        else:
            raise RuntimeError("agentd did not start")
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()


async def rpc(owner_id: str, method: str, params: dict) -> dict:
    await ensure_agentd()
    reader, writer = await asyncio.open_unix_connection(
        common.agentd_sock_path(), limit=16 * 1024 * 1024
    )
    request = {
        "jsonrpc": "2.0", "id": 1, "method": method,
        "params": {**params, "owner_id": owner_id},
    }
    try:
        writer.write((json.dumps(request) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=120)
        if not line:
            raise RuntimeError("agentd disconnected")
        response = json.loads(line)
        if response.get("error"):
            raise RuntimeError(response["error"].get("message") or "agentd error")
        return response.get("result") or {}
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def routine_owner(routine_id: str) -> str:
    return f"routine-{routine_id}"


def routine_prompt(value: dict, run_id: str) -> str:
    delegation = value["delegation"]
    if delegation == "disabled":
        delegation_text = (
            "Complete the routine yourself. Do not use task, fleet, parallel_tasks, "
            "or subagent skills for this run."
        )
    elif delegation == "encouraged":
        delegation_text = (
            "Use Reasonix's native task/fleet or subagent skills when independent "
            "work can be delegated safely. Review every child result before acting."
        )
    else:
        delegation_text = (
            "You may use Reasonix's native task/fleet or subagent skills when useful. "
            "Review child results before acting."
        )
    return f"""[reasonix-mcp routine execution]
Routine: {value['name']}
Run ID: {run_id}
This is a fresh context. Do not assume memory of earlier routine runs. Inspect
the repository and external systems for current state before acting. Make work
idempotent: do not duplicate an existing issue, branch, PR, or completed fix.
{delegation_text}

Routine instructions:
{value['prompt']}"""


def claim_due(now: float) -> list[tuple[dict, dict]]:
    claims: list[tuple[dict, dict]] = []
    with routines.locked():
        values = routines.list_all()
        active_total = sum(
            run.get("status") in ("starting", "running")
            for value in values for run in value.get("runs", [])
        )
        slots = max(0, MAX_CONCURRENT_RUNS - active_total)
        for value in values:
            active = any(
                run["status"] in ("starting", "running") for run in value["runs"]
            )
            pending = int(value.get("pending_triggers", 0))
            scheduled = bool(
                value.get("enabled") and value.get("next_run_at") is not None
                and float(value["next_run_at"]) <= now
            )
            if active and scheduled:
                value["next_run_at"] = routines.next_scheduled(value, now)
                if value["overlap_policy"] == "queue":
                    value["pending_triggers"] = min(10, pending + 1)
                else:
                    value["skipped_overlaps"] = int(value.get("skipped_overlaps", 0)) + 1
                value["updated_at"] = now
                routines._write(value)
                continue
            if active or not (pending or scheduled):
                continue
            if slots <= 0:
                # Leave the due timestamp / manual trigger untouched. It will
                # be claimed as soon as another routine finishes.
                continue
            if pending:
                value["pending_triggers"] = pending - 1
                trigger = "manual"
            else:
                value["next_run_at"] = routines.next_scheduled(value, now)
                trigger = "schedule"
            run = {
                "run_id": str(uuid.uuid4()), "status": "starting",
                "trigger": trigger, "created_at": now, "started_at": None,
                "finished_at": None, "session_id": None, "stop_reason": None,
                "message": "", "error": None,
            }
            value["runs"] = (value.get("runs") or [])[-(routines.MAX_RUN_HISTORY - 1):] + [run]
            value["last_run_at"] = now
            value["updated_at"] = now
            routines._write(value)
            claims.append((value, run))
            slots -= 1
    return claims


def update_run(routine_id: str, run_id: str, changes: dict) -> None:
    with routines.locked():
        value = routines.get(routine_id)
        run = next((item for item in value["runs"] if item["run_id"] == run_id), None)
        if run is None:
            return
        run.update(changes)
        value["updated_at"] = time.time()
        routines._write(value)


async def launch(value: dict, run: dict) -> None:
    try:
        result = await rpc(routine_owner(value["routine_id"]), "spawn", {
            "task": routine_prompt(value, run["run_id"]),
            "cwd": value["cwd"], "model": value["model"],
            "effort": value["effort"], "work_mode": value["work_mode"],
            "tool_approval": value["tool_approval"],
            "keep_alive": False, "idle_timeout": 30,
        })
        common.write_session_config(
            result["session_id"], result.get("config") or {
                "model": value["model"], "effort": value["effort"],
                "work_mode": value["work_mode"],
                "tool_approval": value["tool_approval"],
            }, replace=True,
        )
        update_run(value["routine_id"], run["run_id"], {
            "status": "running", "started_at": time.time(),
            "session_id": result["session_id"], "config": result.get("config") or {},
        })
    except Exception as exc:
        update_run(value["routine_id"], run["run_id"], {
            "status": "failed", "finished_at": time.time(), "error": str(exc),
        })


async def monitor(value: dict) -> None:
    active = [
        run for run in value.get("runs", [])
        if run["status"] in ("starting", "running")
    ]
    if not active:
        return
    owner = routine_owner(value["routine_id"])
    try:
        listing = await rpc(
            owner, "list", {"include_task": any(r["status"] == "starting" for r in active)}
        )
    except Exception:
        return
    by_id = {item["session_id"]: item for item in listing.get("sessions", [])}
    for run in active:
        session_id = run.get("session_id")
        if run["status"] == "starting":
            # A scheduler can die after agentd accepted spawn but before the
            # session id reached disk. Recover by the unique run marker in the
            # injected task instead of launching duplicate work.
            marker = f"Run ID: {run['run_id']}"
            recovered = next(
                (item for item in listing.get("sessions", []) if marker in item.get("task", "")),
                None,
            )
            if recovered:
                session_id = recovered["session_id"]
                update_run(value["routine_id"], run["run_id"], {
                    "status": "running", "started_at": run.get("started_at") or time.time(),
                    "session_id": session_id, "recovered": True,
                })
            elif time.time() - float(run.get("created_at") or 0) > 60:
                update_run(value["routine_id"], run["run_id"], {
                    "status": "failed", "finished_at": time.time(),
                    "error": "scheduler restarted before the agent could be launched",
                })
                continue
            else:
                continue
        item = by_id.get(session_id)
        if item is None:
            if time.time() - float(run.get("started_at") or 0) > 60:
                update_run(value["routine_id"], run["run_id"], {
                    "status": "failed", "finished_at": time.time(),
                    "error": "agent session disappeared before completion",
                })
            continue
        if item.get("status") == "running" or not item.get("stop_reason"):
            continue
        try:
            final = await rpc(owner, "poll", {"session_id": session_id})
        except Exception:
            final = item
        reason = final.get("stop_reason") or item.get("stop_reason")
        update_run(value["routine_id"], run["run_id"], {
            "status": "completed" if reason == "end_turn" else "failed",
            "finished_at": time.time(), "stop_reason": reason,
            "message": final.get("message") or "",
            "error": final.get("error_text"),
        })


async def scheduler() -> None:
    loaded_signature = code_signature()
    while True:
        for value in routines.list_all():
            await monitor(value)
        claims = claim_due(time.time())
        if claims:
            await asyncio.gather(*(launch(value, run) for value, run in claims))
        await asyncio.sleep(2)
        if code_signature() != loaded_signature:
            # Keep the singleton flock across exec. Agent processes live in
            # agentd, so replacing the scheduler cannot interrupt a run.
            os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)])


def write_state() -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = f"{STATE_PATH}.tmp-{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump({
            "pid": os.getpid(), "started_at": time.time(),
            "source_signature": code_signature(),
        }, fh)
    os.replace(tmp, STATE_PATH)


def main() -> None:
    os.makedirs(os.path.dirname(PROCESS_LOCK), exist_ok=True)
    inherited = os.environ.get("REASONIX_ROUTINED_LOCK_FD", "")
    if inherited:
        lock_fd = int(inherited)
    else:
        lock_fd = os.open(PROCESS_LOCK, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        os.set_inheritable(lock_fd, True)
        os.environ["REASONIX_ROUTINED_LOCK_FD"] = str(lock_fd)
    write_state()
    try:
        asyncio.run(scheduler())
    finally:
        with contextlib.suppress(OSError):
            with open(STATE_PATH, encoding="utf-8") as fh:
                state = json.load(fh)
            if state.get("pid") == os.getpid():
                os.unlink(STATE_PATH)
        os.close(lock_fd)


if __name__ == "__main__":
    main()
