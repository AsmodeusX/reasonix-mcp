#!/usr/bin/env python3
"""Agent-done callback selftest — NO model calls (dummy 401 provider).

Raw NDJSON JSON-RPC client over the server's stdio, so it can observe the
server-initiated `reasonix/agent_event` notification (the standard SDK client
validates server notifications against known types and may drop custom ones,
so we read the wire directly).

Flow: initialize -> spawn (turn fails fast on the 401 provider) -> expect an
agent_event turn_end callback -> stop -> expect a further agent_event
(turn_end cancelled / process_exit).
"""

from __future__ import annotations

import http.server
import json
import os
import shutil
from selftest_util import shutdown_agentd
import signal
import subprocess
import sys
import tempfile
import threading
import time

SERVER = "/home/asmodeus/reasonix-mcp/server.py"
PY = "/home/asmodeus/reasonix-mcp/.venv/bin/python"
SCRATCH_HOME = tempfile.mkdtemp(prefix="rxmcp-notif-")


class Reject(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(401)
        self.end_headers()

    def log_message(self, *a):
        pass


def main() -> None:
    srv = http.server.HTTPServer(("127.0.0.1", 0), Reject)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with open(os.path.join(SCRATCH_HOME, "config.toml"), "w") as f:
        f.write(
            'default_model = "opencode-go/deepseek-v4-flash"\n'
            '[[providers]]\nname = "opencode-go"\nkind = "openai"\n'
            f'base_url = "http://127.0.0.1:{port}"\n'
            'api_key_env = "SCRATCH_KEY"\nmodel = "deepseek-v4-flash"\n'
        )
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH_HOME
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH_HOME, "agentd.sock")
    env["SCRATCH_KEY"] = "dummy"

    proc = subprocess.Popen(
        [PY, SERVER],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1, start_new_session=True, env=env,
    )
    try:
        def send(obj):
            proc.stdin.write(json.dumps(obj) + "\n")
            proc.stdin.flush()

        def read_line():
            line = proc.stdout.readline()
            if not line:
                raise AssertionError("server exited: " + proc.stderr.read()[-1500:])
            return json.loads(line)

        def wait_response(rid, timeout=90.0):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                msg = read_line()
                if msg.get("id") == rid:
                    if "error" in msg:
                        raise AssertionError(f"rpc error: {msg['error']}")
                    return msg.get("result", {})
            raise AssertionError(f"no response for id {rid}")

        def wait_notification(method, timeout=90.0):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                msg = read_line()
                if msg.get("method") == method and "id" not in msg:
                    return msg.get("params", {})
            raise AssertionError(f"no notification {method} within {timeout}s")

        # handshake
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "notif-test", "version": "0.0.1"},
        }})
        wait_response(1)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # spawn: the 401 turn fails fast -> turn_end callback
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "reasonix_spawn", "arguments": {"task": "One."},
        }})
        result = wait_response(2)
        sid = result["content"][0]["text"]
        sid = json.loads(sid)["session_id"]
        print("spawned:", sid)

        ev = wait_notification("reasonix/agent_event", timeout=90)
        print("callback #1:", json.dumps(ev)[:300])
        assert ev.get("session_id") == sid and ev.get("event") == "turn_end", ev
        assert ev.get("stop_reason") == "error", ev

        # stop: expect a further agent_event (turn_end cancelled / process_exit)
        send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "reasonix_stop", "arguments": {"session_id": sid},
        }})
        wait_response(3)
        ev2 = wait_notification("reasonix/agent_event", timeout=30)
        print("callback #2:", json.dumps(ev2)[:300])
        assert ev2.get("session_id") == sid and ev2.get("event") in ("turn_end", "process_exited"), ev2

        print("AGENT-EVENT CALLBACK SELFTEST PASS")
    finally:
        try:
            proc.stdin.close()  # EOF -> server exits cleanly -> atexit kills agents
            proc.wait(timeout=10)
        except Exception:
            os.killpg(proc.pid, signal.SIGKILL)
        srv.shutdown()
        shutdown_agentd(os.path.join(SCRATCH_HOME, "agentd.sock"))
        shutil.rmtree(SCRATCH_HOME, ignore_errors=True)


if __name__ == "__main__":
    main()
