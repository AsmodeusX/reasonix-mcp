#!/usr/bin/env python3
"""End-to-end selftest: drive server.py through the official MCP stdio client.

Flow: list tools -> reasonix_spawn (tiny task) -> poll until idle ->
reasonix_send (steer/new turn) -> poll until idle -> reasonix_stop.
Uses a real `reasonix acp` subprocess and the user's configured provider,
so model latency applies. Run in the background if you're impatient.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from util import shutdown_agentd
import sys
import tempfile
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import SERVER, PY, shutdown_agentd, scratch_env

POLL_INTERVAL = 2.0
TURN_TIMEOUT = 240.0

# Isolated Reasonix home: this test must never touch the user's live
# ~/.reasonix (no session pollution, no shared state with a running session).
# The real config.toml + .env (provider credentials) are copied into the
# scratch home so model calls work; the scratch dir is removed on exit.
REAL_HOME = os.path.expanduser("~/.reasonix")
SCRATCH_HOME = tempfile.mkdtemp(prefix="reasonix-mcp-selftest-")


def setup_scratch_home() -> None:
    for name in ("config.toml", ".env"):
        src = os.path.join(REAL_HOME, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(SCRATCH_HOME, name))


def text_of(result) -> str:
    parts = []
    for block in result.content:
        if getattr(block, "type", "") == "text":
            parts.append(block.text)
    return "\n".join(parts)


async def call(session, tool: str, args: dict) -> dict:
    res = await session.call_tool(tool, args)
    if res.is_error:
        raise AssertionError(f"{tool} error: {text_of(res)}")
    return json.loads(text_of(res))


async def wait_idle(session, sid: str, what: str, timeout: float = TURN_TIMEOUT) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        r = await call(session, "reasonix_poll", {"session_id": sid})
        if r.get("text") or r.get("thought"):
            print(f"  [{what}] text={r.get('text', '')[:120]!r} status={r.get('status')}")
        if r.get("status") in ("idle", "exited"):
            return r
        await asyncio.sleep(POLL_INTERVAL)
    raise AssertionError(f"timed out waiting for {what}; last={last}")


async def main() -> None:
    setup_scratch_home()
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH_HOME
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH_HOME, "agentd.sock")
    try:
        await _run(env)
    finally:
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)


async def _run(env: dict) -> None:
    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = sorted(t.name for t in tools.tools)
            expected = {"reasonix_spawn", "reasonix_send", "reasonix_poll",
                        "reasonix_respond_permission", "reasonix_stop"}
            assert expected.issubset(names), f"missing tools: {expected - set(names)}"
            print("tools OK:", names)

            r = await call(session, "reasonix_spawn", {
                "task": "Reply with exactly: PONG. Do not use any tools.",
                "tool_approval": "auto",
            })
            sid = r["session_id"]
            print("spawned:", r)

            r = await wait_idle(session, sid, "first turn")
            turns = r.get("turns", [])
            assert turns and "PONG" in turns[-1]["text"], f"expected PONG, got {turns!r}"
            assert r.get("stop_reason") == "end_turn", r
            # thought/full_* are opt-in now: absent by default
            for key in ("thought", "full_thought", "full_text"):
                assert key not in r, f"{key} should be opt-in, got {list(r.keys())}"
            print("first turn OK: stop_reason =", r.get("stop_reason"), "| thought/full_* opt-in verified")

            r = await call(session, "reasonix_send", {
                "session_id": sid,
                "message": "Now reply with exactly: STEERED. Do not use any tools.",
            })
            print("send:", r)

            r = await wait_idle(session, sid, "second turn")
            turns = r.get("turns", [])
            assert turns and "STEERED" in turns[-1]["text"], f"expected STEERED, got {turns!r}"
            print("second turn OK")

            # opt-in: include_thought / include_full surface the accumulators
            r = await call(session, "reasonix_poll", {
                "session_id": sid, "include_thought": True, "include_full": True,
            })
            assert "thought" in r and "full_thought" in r and "full_text" in r, list(r.keys())
            assert "PONG" in r["full_text"] and "STEERED" in r["full_text"], r["full_text"]
            print("include_thought/include_full opt-in OK")

            r = await call(session, "reasonix_stop", {"session_id": sid})
            print("stop:", r)

            # poll after stop should report exited
            r = await call(session, "reasonix_poll", {"session_id": sid})
            print("post-stop poll status:", r.get("status"))
            assert r.get("status") == "exited"

    print("\nSELFTEST PASS")


if __name__ == "__main__":
    asyncio.run(main())
