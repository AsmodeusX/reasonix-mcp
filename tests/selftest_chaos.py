#!/usr/bin/env python3
"""Verification for: cwd allowlist, dual notifications, PDEATHSIG.

Raw NDJSON JSON-RPC client. Dummy 401 provider (no model calls).
Scratch config declares allow_write = [qemu-tricore] so the allowlist logic is
exercised: project root + subdirs + allow_write dirs pass, others rejected.
"""
import http.server
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import shutdown_agentd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import SERVER, PY, shutdown_agentd, scratch_env

SCRATCH = tempfile.mkdtemp(prefix="rxmcp-verify-")
PROJECT = "/home/asmodeus/reasonix-mcp"


class Reject(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(401); self.end_headers()
    def log_message(self, *a):
        pass


def main() -> None:
    srv = http.server.HTTPServer(("127.0.0.1", 0), Reject)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with open(os.path.join(SCRATCH, "config.toml"), "w") as f:
        f.write(
            'default_model = "opencode-go/deepseek-v4-flash"\n'
            '[[providers]]\nname = "opencode-go"\nkind = "openai"\n'
            f'base_url = "http://127.0.0.1:{port}"\napi_key_env = "SCRATCH_KEY"\nmodel = "deepseek-v4-flash"\n'
            '[sandbox]\nallow_write = ["/home/asmodeus/qemu-tricore"]\n'
        )
    env = dict(os.environ)
    env["REASONIX_HOME"] = SCRATCH
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(SCRATCH, "agentd.sock")
    env["SCRATCH_KEY"] = "dummy"
    proc = subprocess.Popen([PY, SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, bufsize=1,
                            start_new_session=True, env=env)
    try:
        def send(o):
            proc.stdin.write(json.dumps(o) + "\n"); proc.stdin.flush()
        def read_line():
            line = proc.stdout.readline()
            if not line:
                raise AssertionError("server exited: " + proc.stderr.read()[-1200:])
            return json.loads(line)
        def wait_response(rid, timeout=90.0):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                msg = read_line()
                if msg.get("id") == rid:
                    if "error" in msg:
                        return ("error", msg["error"])
                    return ("ok", msg.get("result", {}))
            raise AssertionError(f"no response for id {rid}")
        def call_tool(tool, args, rid):
            send({"jsonrpc": "2.0", "id": rid, "method": "tools/call",
                  "params": {"name": tool, "arguments": args}})
            status, res = wait_response(rid)
            if status == "error":
                return {"__error__": res["message"]}
            text = res["content"][0]["text"]
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"__raw__": text[:200]}

        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "verify", "version": "0.0.1"}}})
        wait_response(1)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # --- cwd allowlist ---
        def err_text(r):
            return r.get("__error__", "") or r.get("__raw__", "")
        r = call_tool("reasonix_spawn", {"task": "x", "cwd": "/home/asmodeus/ghidra"}, 2)
        assert "outside the allowed spawn roots" in err_text(r), r
        print("allowlist: ghidra rejected OK")
        r = call_tool("reasonix_spawn", {"task": "x", "cwd": "/home/asmodeus/qemu-tricore"}, 3)
        assert "__error__" not in r, r
        sid = r["session_id"]
        print("allowlist: allow_write dir (qemu-tricore) accepted OK")
        r = call_tool("reasonix_spawn", {"task": "x", "cwd": PROJECT}, 4)
        assert "__error__" not in r, r
        print("allowlist: project root accepted OK")

        # --- dual notifications on turn end (401 fails fast) ---
        seen = {"agent_event": False, "message": False}
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            msg = read_line()
            if msg.get("method") == "reasonix/agent_event" and "id" not in msg:
                seen["agent_event"] = True
                ev = msg.get("params", {})
                print("agent_event:", ev.get("event"), ev.get("stop_reason"))
            elif msg.get("method") == "notifications/message" and "id" not in msg:
                seen["message"] = True
                data = msg.get("params", {}).get("data", {})
                print("notifications/message:", data.get("event"), data.get("stop_reason"))
            if all(seen.values()):
                break
        assert seen["agent_event"] and seen["message"], seen
        print("dual notification OK: agent_event + notifications/message both on the wire")

        # --- PDEATHSIG: SIGKILL the daemon, its agents must die ---
        daemon_pid = None
        for p in os.listdir("/proc"):
            if p.isdigit():
                try:
                    cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ")
                except OSError:
                    continue
                if b"agentd.py" in cmd and SCRATCH.encode() in cmd:
                    daemon_pid = int(p)
                    break
        assert daemon_pid, "agentd daemon not found under scratch"
        agent_pid = None
        for p in os.listdir("/proc"):
            if p.isdigit():
                try:
                    with open(f"/proc/{p}/stat") as fh:
                        st = fh.read().split()
                except OSError:
                    continue
                if st[3] == str(daemon_pid) and "reasonix" in st[1]:
                    agent_pid = int(p)
                    break
        assert agent_pid, "agent not found under the daemon"
        print("agent", agent_pid, "under daemon", daemon_pid, "| SIGKILLing daemon")
        os.kill(daemon_pid, signal.SIGKILL)
        time.sleep(2.5)
        gone = not os.path.isdir(f"/proc/{agent_pid}")
        print("agent dead after daemon SIGKILL:", gone)
        assert gone, "PDEATHSIG FAILED — agent survived daemon death"
        print("PDEATHSIG OK — agents die with the daemon, never orphaned")
        print("VERIFY1 PASS")
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass
        srv.shutdown()
        import shutil
        shutdown_agentd(os.path.join(SCRATCH, "agentd.sock"))
        shutil.rmtree(SCRATCH, ignore_errors=True)


if __name__ == "__main__":
    main()
