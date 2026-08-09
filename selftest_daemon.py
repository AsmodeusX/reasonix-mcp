#!/usr/bin/env python3
"""Daemon survival / resume / concurrency selftest (real provider, isolated).

Proves the deferred feature: agents live in the detached agentd daemon and
survive MCP-server death. Flow:
  1. server A spawns an agent -> SIGKILL server A -> agent keeps running.
  2. server B (same socket) reconnects: reasonix_list shows the agent,
     poll returns its output (queued in the daemon), send steers it.
  3. reasonix_resume revives a stopped (tombstoned) session.
  4. 6 agents in parallel: reasonix_wait wakes on all, poll collects, stop all.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from selftest_util import scratch_env, shutdown_agentd  # noqa: E402

SERVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py")
PY = "/home/asmodeus/reasonix-mcp/.venv/bin/python"
SCRATCH = tempfile.mkdtemp(prefix="rxmcp-daemon-")


class RawMCP:
    """Minimal stdio JSON-RPC MCP client over the server's pipes."""

    def __init__(self, env):
        self.proc = subprocess.Popen(
            [PY, SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, start_new_session=True, env=env,
        )
        self._rid = 0
        self.notifications = []

    def send(self, obj):
        self.proc.stdin.write(json.dumps(obj) + "\n")
        self.proc.stdin.flush()

    def _pump(self, want_id, timeout=180.0):
        dl = time.monotonic() + timeout
        while time.monotonic() < dl:
            line = self.proc.stdout.readline()
            if not line:
                raise AssertionError("server exited: " + self.proc.stderr.read()[-1200:])
            msg = json.loads(line)
            if msg.get("method") in ("reasonix/agent_event", "notifications/message") and "id" not in msg:
                self.notifications.append(msg)
            elif msg.get("id") == want_id:
                if "error" in msg:
                    raise AssertionError(f"rpc error: {msg['error']}")
                return msg.get("result", {})
        raise AssertionError(f"no response for id {want_id}")

    def start(self):
        self.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "daemon-test", "version": "1"}}})
        self._pump(1)
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def tool(self, name, args):
        self._rid += 1
        rid = 100 + self._rid
        self.send({"jsonrpc": "2.0", "id": rid, "method": "tools/call",
                   "params": {"name": name, "arguments": args}})
        res = self._pump(rid)
        if res.get("isError"):
            text = "".join(c.get("text", "") for c in res.get("content", []))
            raise AssertionError(f"{name} error: {text[:300]}")
        return json.loads(res["content"][0]["text"])

    def wait_idle(self, sid, timeout=180.0):
        dl = time.monotonic() + timeout
        while time.monotonic() < dl:
            d = self.tool("reasonix_poll", {"session_id": sid})
            if d["status"] in ("idle", "exited"):
                return d
            time.sleep(2)
        raise AssertionError(f"session {sid} never finished")

    def kill(self):
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except Exception:
            pass


def main() -> None:
    env = scratch_env(SCRATCH)
    sock = env["REASONIX_MCP_AGENTD_SOCK"]
    a = b = None
    try:
        # ---- part 1: survival across server death ----
        a = RawMCP(env)
        a.start()
        r = a.tool("reasonix_spawn", {"task": "Reply with exactly: PONG1. Do not use tools."})
        sid = r["session_id"]
        print("spawned:", sid[:8], "| daemon note present:", "survives" in r.get("note", ""))
        time.sleep(3)  # let the turn start / finish while server A lives
        a.kill()
        print("server A SIGKILLed")
        time.sleep(1.5)

        # the agent must still be running under the daemon (cmdline uses \0
        # separators, so normalize before matching)
        agent_pids = []
        for p in os.listdir("/proc"):
            if not p.isdigit():
                continue
            try:
                cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ")
                envb = open(f"/proc/{p}/environ", "rb").read()
            except OSError:
                continue
            if b"reasonix acp" in cmd and f"REASONIX_HOME={SCRATCH}".encode() in envb:
                agent_pids.append(int(p))
        assert agent_pids, "agent died with server A — daemon did not take it over"
        print("agent alive under daemon after server A died:", len(agent_pids), "pid(s)")

        # ---- part 2: reconnect with server B on the same socket ----
        b = RawMCP(env)
        b.start()
        lst = b.tool("reasonix_list", {})
        by_id = {s["session_id"]: s for s in lst["sessions"]}
        assert sid in by_id, f"lost session after reconnect: {list(by_id)}"
        print("reconnect: reasonix_list sees the agent:", by_id[sid]["status"])

        d = b.tool("reasonix_poll", {"session_id": sid})
        assert d["status"] in ("running", "idle"), d
        if d["status"] != "idle":
            d = b.wait_idle(sid)
        text = "".join(t.get("text", "") for t in d.get("turns", []))
        assert "PONG1" in text, text
        print("turn completed while server A was dead; poll recovered it:", text[:40])

        b.tool("reasonix_send", {"session_id": sid, "message": "Now reply STEERED."})
        d = b.wait_idle(sid)
        text = "".join(t.get("text", "") for t in d.get("turns", []))
        assert "STEERED" in text, text
        print("post-restart steer OK:", text[:40])

        # ---- part 3: resume a stopped session ----
        b.tool("reasonix_stop", {"session_id": sid})
        lst = b.tool("reasonix_list", {})
        info = next(s for s in lst["sessions"] if s["session_id"] == sid)
        assert info["status"] == "exited" and info.get("resumable"), info
        print("tombstone resumable:", info["resumable"])
        r = b.tool("reasonix_resume", {"session_id": sid})
        assert r.get("status") in ("idle", "running"), r
        b.tool("reasonix_send", {"session_id": sid, "message": "Reply RESUMED."})
        d = b.wait_idle(sid)
        text = "".join(t.get("text", "") for t in d.get("turns", []))
        assert "RESUMED" in text, text
        print("resume OK — resumed session replied:", text[:40])
        b.tool("reasonix_stop", {"session_id": sid})

        # ---- part 4: 6-way concurrency ----
        ids = []
        for i in range(6):
            r = b.tool("reasonix_spawn", {"task": f"Reply with exactly: C{i}. Do not use tools."})
            ids.append(r["session_id"])
        print("spawned 6 agents:", [i[:8] for i in ids])
        w = b.tool("reasonix_wait", {"session_ids": ids, "timeout": 240})
        assert not w["timed_out"], w
        print("wait woke:", len(w["woke"]), "of 6")
        got = []
        for sid in ids:
            d = b.wait_idle(sid, timeout=120)
            text = "".join(t.get("text", "") for t in d.get("turns", []))
            got.append(text[:20])
        print("agents replied:", got)
        assert all("C" in t for t in got), got
        for sid in ids:
            b.tool("reasonix_stop", {"session_id": sid})
        print("DAEMON SELFTEST PASS")
    finally:
        if a:
            a.kill()
        if b:
            b.kill()
        shutdown_agentd(sock)
        import shutil
        shutil.rmtree(SCRATCH, ignore_errors=True)


if __name__ == "__main__":
    main()
