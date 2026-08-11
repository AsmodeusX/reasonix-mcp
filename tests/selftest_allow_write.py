#!/usr/bin/env python3
"""Probe: can a spawned agent write into a dir outside its cwd via allow_write?

Uses the real config.toml + .env (copied into a scratch REASONIX_HOME, so the
live ~/.reasonix is untouched) and the real provider. The agent must use
write_file (not bash) to create the probe file, proving the [sandbox]
allow_write path works.
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

REAL_HOME = os.path.expanduser("~/.reasonix")
SCRATCH_HOME = tempfile.mkdtemp(prefix="rxmcp-allowwrite-")
PROBE = "/home/asmodeus/qemu-tricore/allow_write_probe.txt"
TURN_TIMEOUT = 240.0


def setup_scratch_home() -> None:
    for name in ("config.toml", ".env"):
        src = os.path.join(REAL_HOME, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(SCRATCH_HOME, name))


async def call(session, tool: str, args: dict) -> dict:
    res = await session.call_tool(tool, args)
    text = "\n".join(b.text for b in res.content if getattr(b, "type", "") == "text")
    if res.is_error:
        raise AssertionError(f"{tool} error: {text}")
    return json.loads(text)


async def main() -> None:
    setup_scratch_home()
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
                        f'Use the write_file tool to create {PROBE} containing '
                        'exactly the text "probe-ok" on the first line. '
                        "Do not use bash. Then reply with: WRITTEN."
                    ),
                })
                sid = r["session_id"]
                print("spawned:", sid)
                deadline = time.monotonic() + TURN_TIMEOUT
                final = None
                while time.monotonic() < deadline:
                    d = await call(session, "reasonix_poll", {"session_id": sid})
                    if d.get("message"):
                        print("  agent:", d["message"][-160:].replace("\n", " | "))
                    if d["status"] in ("idle", "exited"):
                        final = d
                        break
                    await asyncio.sleep(2)
                assert final is not None, "turn never ended"
                print("stop_reason:", final.get("stop_reason"))
                print("agent said:", final.get("message", "")[-200:] or "(no message)")
                await call(session, "reasonix_stop", {"session_id": sid})

        exists = os.path.isfile(PROBE)
        content = open(PROBE).read().strip() if exists else ""
        print("probe file exists:", exists, "| content:", content)
        assert exists and content == "probe-ok", "allow_write probe FAILED"
        print("ALLOW_WRITE OK — agent wrote outside its cwd via write_file")
    finally:
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)
        if os.path.isfile(PROBE):
            os.remove(PROBE)


if __name__ == "__main__":
    asyncio.run(main())
