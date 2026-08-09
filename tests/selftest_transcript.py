#!/usr/bin/env python3
"""Transcript + plan verification (real provider, isolated home).

Spawns an agent that creates a file, then checks reasonix_transcript reports
the write (file touched, tool call) and poll carries a `plan` field.
"""
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
SCRATCH = tempfile.mkdtemp(prefix="rxmcp-tx2-")
PROBE = "/home/asmodeus/qemu-tricore/rxmcp_tx_probe.txt"


async def call(session, tool, args):
    res = await session.call_tool(tool, args)
    text = "\n".join(b.text for b in res.content if getattr(b, "type", "") == "text")
    if res.is_error:
        raise AssertionError(f"{tool}: {text}")
    return json.loads(text)


async def main() -> None:
    for name in ("config.toml", ".env"):
        src = os.path.join(REAL_HOME, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(SCRATCH, name))
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH, "agentd.sock")
    try:
        params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                r = await call(session, "reasonix_spawn", {
                    "task": (
                        f'Use write_file to create {PROBE} containing "tx-ok". '
                        "Then reply with: TXDONE."
                    ),
                })
                sid = r["session_id"]
                deadline = time.monotonic() + 180
                final = None
                while time.monotonic() < deadline:
                    d = await call(session, "reasonix_poll", {"session_id": sid})
                    if d["status"] in ("idle", "exited"):
                        final = d
                        break
                    await asyncio.sleep(2)
                assert final, "no result"
                print("plan field present:", "plan" in final, "| plan:", final.get("plan"))
                assert "plan" in final

                tx = await call(session, "reasonix_transcript", {"session_id": sid})
                print("transcript exists:", tx.get("exists"), "| path:", tx.get("transcript_path"))
                print("roles:", tx.get("roles"), "| tool_calls:", [(t["name"], t.get("arguments")) for t in tx.get("tool_calls", [])][:4])
                print("files_touched:", tx.get("files_touched"))
                assert tx.get("exists") and tx.get("lines", 0) > 0
                names = [t["name"] for t in tx.get("tool_calls", [])]
                assert "write_file" in names, names
                touched = [f["path"] for f in tx.get("files_touched", [])]
                assert any(PROBE in p for p in touched), touched
                assert "TXDONE" in tx.get("last_text", "") or "TXDONE" in "".join(
                    t.get("text", "") for t in final.get("turns", []))
                await call(session, "reasonix_stop", {"session_id": sid})
        print("TRANSCRIPT+PLAN VERIFY PASS")
    finally:
        shutdown_agentd(os.path.join(SCRATCH, "agentd.sock"))
        shutil.rmtree(SCRATCH, ignore_errors=True)
        if os.path.isfile(PROBE):
            os.remove(PROBE)


if __name__ == "__main__":
    asyncio.run(main())
