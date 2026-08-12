#!/usr/bin/env python3
"""Permission-flow selftest (real provider, isolated home): the path the
integration feedback could not exercise.

Spawns an agent with tool_approval="ask", asks it to run a bash command, and
verifies the session/request_permission round-trip: one blocking watch
surfaces the request, then an on-the-fly switch to yolo resolves that pending
tool gate and lets the turn complete. No polling.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from util import shutdown_agentd
import sys
import tempfile

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import SERVER, PY, shutdown_agentd, scratch_env

REAL_HOME = os.path.expanduser("~/.reasonix")
SCRATCH_HOME = tempfile.mkdtemp(prefix="rxmcp-perm-")
PROBE = "/tmp/rxmcp_perm_probe.txt"


async def call(session, tool: str, args: dict) -> dict:
    res = await session.call_tool(tool, args)
    text = "\n".join(b.text for b in res.content if getattr(b, "type", "") == "text")
    if res.is_error:
        raise AssertionError(f"{tool} error: {text}")
    return json.loads(text)


async def main() -> None:
    for name in ("config.toml", ".env"):
        src = os.path.join(REAL_HOME, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(SCRATCH_HOME, name))
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH_HOME
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH_HOME, "agentd.sock")
    try:
        params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                r = await call(session, "reasonix_spawn", {
                    "task": (
                        "Use the bash tool to run exactly this command: "
                        f'printf "PERMISSION_OK" > {PROBE} '
                        "Then reply with: DONE. Do not use any other tool."
                    ),
                    "tool_approval": "ask",
                })
                sid = r["session_id"]
                print("spawned (ask mode):", sid)

                watched = await call(session, "reasonix_watch", {
                    "session_ids": [sid], "timeout": 180,
                })
                assert not watched["timed_out"] and watched["woke"] == [sid], watched
                decision = watched["results"][sid]
                req = decision.get("permission_request")
                assert req, f"watch returned without a permission request: {decision}"
                tc = req["tool_call"]
                print("permission_request tool_call:", {k: tc.get(k) for k in ("title", "kind", "status", "toolCallId")}, "| rawInput:", str(tc.get("rawInput"))[:120])
                # For gated bash the command/args live in `title` (rawInput is
                # the structured-args field and is null for bash gates).
                assert tc.get("title") and "PERMISSION_OK" in tc.get("title", ""), "tool name/args missing from permission payload"
                opts = [o.get("optionId") for o in req["options"]]
                print("options:", opts)
                assert "allow_once" in opts
                r = await call(session, "reasonix_configure", {
                    "session_id": sid,
                    "tool_approval": "yolo",
                })
                assert r.get("permission_resolution") == "allow_once", r
                assert not r.get("permission_still_pending"), r
                print("switched ask -> yolo; pending gate resolved:", r)

                watched = await call(session, "reasonix_watch", {
                    "session_ids": [sid], "timeout": 180,
                })
                assert not watched["timed_out"] and watched["woke"] == [sid], watched
                final = watched["results"][sid]
                assert final["status"] in ("idle", "exited"), final
                print("stop_reason:", final.get("stop_reason"), "| text:", final.get("message", "")[-200:])
                wrote = os.path.isfile(PROBE) and open(PROBE).read() == "PERMISSION_OK"
                print("probe file written after live mode change:", wrote)
                assert wrote, "ask -> yolo did not release the pending bash command"
                await call(session, "reasonix_stop", {"session_id": sid})
        print("PERMISSION WATCH SELFTEST PASS")
    finally:
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)
        if os.path.isfile(PROBE):
            os.remove(PROBE)


if __name__ == "__main__":
    asyncio.run(main())
