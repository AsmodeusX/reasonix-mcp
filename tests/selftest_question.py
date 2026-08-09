#!/usr/bin/env python3
"""Agent-questions selftest (real provider, isolated home).

The agent's `ask` tool lets the model put a structured multiple-choice question
to the user mid-task. Over ACP this rides the same session/request_permission
round-trip as tool approvals (kind "other"), so it surfaces through
reasonix_poll's permission_request and the reasonix/agent_event notification.
This test spawns an agent that must ask, answers via
reasonix_respond_permission, and confirms the agent proceeds with the choice.

Raw NDJSON JSON-RPC client so the agent_event notification is observable too.
"""

from __future__ import annotations

import json
import os
import shutil
from util import shutdown_agentd
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import SERVER, PY, shutdown_agentd, scratch_env

REAL_HOME = os.path.expanduser("~/.reasonix")
SCRATCH_HOME = tempfile.mkdtemp(prefix="rxmcp-question-")

TASK = (
    "Before doing anything else, use the ask tool to ask me one question: "
    "'Which approach should we take?' with exactly two options: 'A' and 'B'. "
    "Wait for my answer, then reply with exactly the label I chose. "
    "Do not use any other tool."
)


def main() -> None:
    for name in ("config.toml", ".env"):
        src = os.path.join(REAL_HOME, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(SCRATCH_HOME, name))
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH_HOME
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH_HOME, "agentd.sock")

    proc = subprocess.Popen(
        [PY, SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, bufsize=1, start_new_session=True, env=env,
    )
    question_event = {}

    def send(obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    def read_until(rid, timeout=120.0):
        """Read lines until the response for rid arrives; capture notifications."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if not line:
                raise AssertionError("server exited: " + proc.stderr.read()[-1500:])
            msg = json.loads(line)
            if msg.get("id") == rid:
                if "error" in msg:
                    raise AssertionError(f"rpc error: {msg['error']}")
                return msg.get("result", {})
            if msg.get("method") == "reasonix/agent_event" and "id" not in msg:
                question_event.update(msg.get("params", {}))
        raise AssertionError(f"no response for id {rid}")

    def call_tool(tool, args, rid):
        send({"jsonrpc": "2.0", "id": rid, "method": "tools/call",
              "params": {"name": tool, "arguments": args}})
        res = read_until(rid)
        return json.loads(res["content"][0]["text"])

    try:
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "question-test", "version": "0.0.1"},
        }})
        read_until(1)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        spawn = call_tool("reasonix_spawn", {"task": TASK}, 2)
        sid = spawn["session_id"]
        print("spawned:", sid)

        # Poll until the agent asks (permission_request with kind "other"),
        # or finishes without asking.
        req = None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            d = call_tool("reasonix_poll", {"session_id": sid}, 3)
            if d.get("permission_request"):
                req = d["permission_request"]
                break
            if d["status"] in ("idle", "exited"):
                break
            time.sleep(2)
        assert req, "agent never surfaced a question (did it call ask?)"
        tc = req["tool_call"]
        print("question tool_call:", {k: tc.get(k) for k in ("title", "kind", "status", "toolCallId")})
        print("question rawInput:", str(tc.get("rawInput"))[:200])
        assert tc.get("kind") == "other" and tc.get("title"), "not a question-shaped request"
        opts = [(o["optionId"], o["name"]) for o in req["options"]]
        print("options:", opts)
        answer = next(oid for oid, _ in opts if not oid.endswith(":cancel"))
        print("agent_event captured:", json.dumps(question_event)[:220])

        r = call_tool("reasonix_respond_permission", {"session_id": sid, "option_id": answer}, 4)
        print("answered:", r)

        final = None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            d = call_tool("reasonix_poll", {"session_id": sid}, 5)
            if d["status"] in ("idle", "exited"):
                final = d
                break
            time.sleep(2)
        assert final, "agent never finished after the answer"
        text = "".join(t.get("text", "") for t in final.get("turns", []))
        print("stop_reason:", final.get("stop_reason"), "| agent reply:", text[:120])
        assert "A" in text, f"agent did not acknowledge the chosen label: {text!r}"
        call_tool("reasonix_stop", {"session_id": sid}, 6)
        print("AGENT-QUESTION SELFTEST PASS — question surfaced, answered, agent continued")
    finally:
        try:
            proc.stdin.close()
            proc.wait(timeout=10)
        except Exception:
            os.killpg(proc.pid, signal.SIGKILL)
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)


if __name__ == "__main__":
    main()
