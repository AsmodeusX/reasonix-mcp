#!/usr/bin/env python3
"""Verify an in-band MCP restart replaces only this orchestrator's server."""

from __future__ import annotations

import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import LAUNCHER, PY, shutdown_agentd, scratch_env  # noqa: E402


class RawMCP:
    def __init__(self, env: dict, launcher: str = LAUNCHER):
        self.proc = subprocess.Popen(
            [PY, launcher], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, start_new_session=True,
            env=env,
        )
        self.request_id = 0

    def send(self, message: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def read_response(self, request_id: int, timeout: float = 10.0) -> dict:
        assert self.proc.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                stderr = self.proc.stderr.read() if self.proc.stderr else ""
                raise AssertionError(f"MCP launcher exited: {stderr[-1200:]}")
            message = json.loads(line)
            if message.get("id") == request_id:
                if "error" in message:
                    raise AssertionError(message["error"])
                return message.get("result", {})
        raise AssertionError(f"no MCP response for request {request_id}")

    def initialize(self, request_id: int) -> None:
        self.send({
            "jsonrpc": "2.0", "id": request_id, "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "restart-test", "version": "1"},
            },
        })
        self.read_response(request_id)
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def tool(self, name: str, arguments: dict) -> dict:
        self.request_id += 1
        request_id = 100 + self.request_id
        self.send({
            "jsonrpc": "2.0", "id": request_id, "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })
        result = self.read_response(request_id)
        content = result.get("content", [])
        if result.get("isError"):
            raise AssertionError(content)
        return json.loads(content[0]["text"])

    def close(self) -> None:
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=10)


def main() -> None:
    scratch = tempfile.mkdtemp(prefix="rxmcp-restart-")
    package_copy = os.path.join(scratch, "reasonix_mcp")
    shutil.copytree(os.path.dirname(LAUNCHER), package_copy)
    copied_launcher = os.path.join(package_copy, "launcher.py")
    env = scratch_env(scratch)
    sock = env["REASONIX_MCP_AGENTD_SOCK"]
    env_a = dict(env, REASONIX_MCP_ORCHESTRATOR_ID="restart-test-a")
    env_b = dict(env, REASONIX_MCP_ORCHESTRATOR_ID="restart-test-b")
    client = RawMCP(env_a, copied_launcher)
    other = RawMCP(env_b)
    try:
        client.initialize(1)
        other.initialize(1)
        # Start agentd and prove the ordinary server is responsive.
        listing = client.tool("reasonix_list", {})
        assert listing["sessions"] == []
        assert other.tool("reasonix_list", {})["sessions"] == []

        result = client.tool("reasonix_restart_mcp_server", {})
        assert result["restart_requested"] and result["preserves_agents"]
        time.sleep(1.0)
        assert client.proc.poll() is None, "launcher did not keep the MCP service alive"

        # The launcher replays the MCP handshake into the replacement; the
        # already-running host does not need to initialize again.
        listing = client.tool("reasonix_list", {})
        assert listing["sessions"] == []
        # Restarting one stdio server must not interrupt another orchestrator.
        assert other.proc.poll() is None
        assert other.tool("reasonix_list", {})["sessions"] == []

        # A source edit automatically replaces the copied server. The launcher
        # replays initialization internally, so the host sends no new handshake.
        copied_server = os.path.join(package_copy, "server.py")
        with open(copied_server, "a", encoding="utf-8") as source:
            source.write("\n# selftest server reload probe\n")
        time.sleep(2.0)
        assert client.proc.poll() is None
        assert client.tool("reasonix_list", {})["sessions"] == []

        # The supervisor can replace its own code while preserving the same
        # host pipes and cached handshake too.
        with open(copied_launcher, "a", encoding="utf-8") as source:
            source.write("\n# selftest launcher reload probe\n")
        time.sleep(2.0)
        assert client.proc.poll() is None
        assert client.tool("reasonix_list", {})["sessions"] == []

        # An agentd restart is not successful until the old peer is gone and
        # a distinct current-code daemon answers ping. The other MCP client
        # must reconnect transparently afterward.
        restarted = client.tool("reasonix_restart_agentd", {})
        assert restarted["restarted"] is True and restarted["verified"] is True
        assert restarted["old_pid"] != restarted["new_pid"]
        assert restarted["escalation"] in ("cooperative", "term", "kill")
        assert restarted["recoverable_sessions"] == []
        assert client.tool("reasonix_list", {})["sessions"] == []
        assert other.tool("reasonix_list", {})["sessions"] == []
        print("MCP RESTART SELFTEST PASS")
    finally:
        client.close()
        other.close()
        shutdown_agentd(sock)


if __name__ == "__main__":
    main()
