#!/usr/bin/env python3
"""Orchestrator-path selftest — NO model calls (dummy 401 provider).

Exercises the orchestration surface added for the integration feedback:
poll event filtering (static boilerplate dropped), events_filtered,
reasonix_list, reasonix_wait (wake on turn end / exited), spawn sandbox
posture, turns with stop_reason, and send expect="steer" semantics.
"""

from __future__ import annotations

import asyncio
import http.server
import json
import os
import shutil
from util import shutdown_agentd
import sys
import tempfile
import threading
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import SERVER, PY, shutdown_agentd, scratch_env

SCRATCH_HOME = tempfile.mkdtemp(prefix="rxmcp-orch-")


class Reject(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(401); self.end_headers()

    def log_message(self, *a):
        pass


async def call(session, tool: str, args: dict) -> dict:
    res = await session.call_tool(tool, args)
    text = "\n".join(b.text for b in res.content if getattr(b, "type", "") == "text")
    if res.is_error:
        raise AssertionError(f"{tool} error: {text}")
    return json.loads(text)


async def wait_status(session, sid, statuses, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        d = await call(session, "reasonix_poll", {"session_id": sid})
        if d["status"] in statuses:
            return d
        await asyncio.sleep(1)
    raise AssertionError(f"session {sid} never reached {statuses}")


async def main() -> None:
    srv = http.server.HTTPServer(("127.0.0.1", 0), Reject)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with open(os.path.join(SCRATCH_HOME, "config.toml"), "w") as f:
        f.write(f'default_model = "opencode-go/deepseek-v4-flash"\n[[providers]]\nname = "opencode-go"\nkind = "openai"\nbase_url = "http://127.0.0.1:{port}"\napi_key_env = "SCRATCH_KEY"\nmodel = "deepseek-v4-flash"\n')
    env = dict(os.environ); env["REASONIX_HOME"] = SCRATCH_HOME
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH_HOME, "agentd.sock"); env["SCRATCH_KEY"] = "dummy"
    try:
        params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Every daemon-backed tool must capture the same owner before
                # opening the shared socket, including concurrent startup calls.
                models, empty = await asyncio.gather(
                    call(session, "reasonix_models", {}),
                    call(session, "reasonix_list", {}),
                )
                assert models.get("models") and empty.get("sessions") == []

                # spawn 2 agents; posture must be present
                r = await call(session, "reasonix_spawn", {"task": "One."})
                sid1, sandbox = r["session_id"], r.get("sandbox")
                assert sandbox and sandbox.get("bash") == "enforce" and sandbox.get("allow_write") == [], sandbox
                r = await call(session, "reasonix_spawn", {"task": "Two."})
                sid2 = r["session_id"]
                print("spawned:", sid1[:8], sid2[:8], "| sandbox:", sandbox)

                # list shows both, with task; status may already be idle (the
                # 401 provider fails the turn in ~1s, before list() runs)
                lst = await call(session, "reasonix_list", {})
                by_id = {s["session_id"]: s for s in lst["sessions"]}
                assert sid1 in by_id and sid2 in by_id
                assert by_id[sid1]["task"] == "One." and by_id[sid1]["status"] in ("running", "idle"), by_id[sid1]
                print("reasonix_list OK:", [(s["session_id"][:8], s["status"], s["task"]) for s in lst["sessions"]])

                # first poll: static boilerplate must be filtered out; if the
                # 401 turn already ended, turns carries it (consumed here);
                # thought/full_* are opt-in -> absent by default
                d = await call(session, "reasonix_poll", {"session_id": sid1})
                types = [e["type"] for e in d["events"]]
                assert "available_commands_update" not in types and "config_option_update" not in types, types
                assert d["events_filtered"] >= 1, d
                for key in ("thought", "full_thought", "full_text"):
                    assert key not in d, f"{key} should be opt-in, got {list(d.keys())}"
                size = len(json.dumps(d))
                print(f"first poll: {size} bytes, events={len(d['events'])}, filtered={d['events_filtered']} (was ~31KB before fix; no thought/full_*)")
                if d["status"] == "idle":
                    assert d["turns"] and d["turns"][-1]["stop_reason"] == "error", d
                    print("turns OK at first poll:", d["turns"])

                # opt-in flags surface the accumulators
                d2 = await call(session, "reasonix_poll", {
                    "session_id": sid1, "include_thought": True, "include_full": True,
                })
                assert "thought" in d2 and "full_thought" in d2 and "full_text" in d2, list(d2.keys())
                print("include_thought/include_full opt-in OK")

                # wait wakes when the 401 turn ends (both idle with stop_reason=error)
                w = await call(session, "reasonix_wait", {"session_ids": [sid1, sid2], "timeout": 60})
                assert not w["timed_out"], w
                assert set(w["woke"]) == {sid1, sid2}, w
                print("wait woke:", w["woke"], "| states:", {k: v["status"] for k, v in w["sessions"].items()})

                # post-wait: wait wakes on ANY new output, not only on turn
                # end — so collect each woke session until it finishes
                for sid in w["woke"]:
                    d = await wait_status(session, sid, ("idle", "exited"))
                    assert d["stop_reason"] == "error", d
                    if d.get("turns"):
                        assert d["turns"][-1]["stop_reason"] == "error"
                print("post-wait results OK (idle + stop_reason=error)")

                # expect="steer" on an idle session must refuse
                try:
                    await call(session, "reasonix_send", {"session_id": sid1, "message": "x", "expect": "steer"})
                    raise AssertionError("expect=steer should have raised on idle session")
                except AssertionError as e:
                    if "should have raised" in str(e):
                        raise
                    print("send expect=steer on idle -> refused:", str(e)[:90])
                r = await call(session, "reasonix_send", {"session_id": sid1, "message": "x", "expect": "new_turn"})
                assert r["delivered"] == "new_turn", r
                print("send expect=new_turn OK:", r["delivered"])

                # wait on a stopped (tombstoned) session wakes with status exited
                await call(session, "reasonix_stop", {"session_id": sid2})
                w = await call(session, "reasonix_wait", {"session_ids": [sid2], "timeout": 30})
                assert not w["timed_out"] and w["sessions"][sid2]["status"] == "exited", w
                d = await call(session, "reasonix_poll", {"session_id": sid2})
                assert d["status"] == "exited", d
                print("wait on stopped session -> status exited OK")

                await call(session, "reasonix_stop", {"session_id": sid1})
        print("ORCHESTRATOR SELFTEST PASS")
    finally:
        srv.shutdown()
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
