#!/usr/bin/env python3
"""Verify agentd replaces itself after its runtime source changes."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import PY, ROOT, scratch_env, shutdown_agentd  # noqa: E402
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))
import agentd  # noqa: E402


def rpc(sock_path: str, method: str) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(sock_path)
        request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": {"owner_id": "reload-test"}}
        client.sendall((json.dumps(request) + "\n").encode())
        data = b""
        while b"\n" not in data:
            data += client.recv(65536)
    response = json.loads(data.split(b"\n", 1)[0])
    if response.get("error"):
        raise AssertionError(response["error"])
    return response["result"]


def main() -> None:
    daemon = agentd.Agentd()
    idle = SimpleNamespace(
        status="idle", active_turn=False, _pending_permission=None,
        keep_alive=False, stop_reason="end_turn", _terminal_observed=True,
    )
    daemon._registry = {"idle": idle}
    assert daemon._can_auto_restart()
    idle.active_turn = True
    assert not daemon._can_auto_restart()
    idle.active_turn = False
    idle._pending_permission = (1, {})
    assert not daemon._can_auto_restart()
    idle._pending_permission = None
    idle.keep_alive = True
    assert not daemon._can_auto_restart()
    idle.keep_alive = False
    idle._terminal_observed = False
    assert not daemon._can_auto_restart()

    scratch = tempfile.mkdtemp(prefix="rxmcp-agentd-reload-")
    package = os.path.join(scratch, "reasonix_mcp")
    shutil.copytree(os.path.join(ROOT, "src", "reasonix_mcp"), package)
    env = scratch_env(scratch)
    sock_path = env["REASONIX_MCP_AGENTD_SOCK"]
    log_path = os.path.join(scratch, "agentd-test.log")
    with open(log_path, "wb") as log:
        original = subprocess.Popen(
            [PY, os.path.join(package, "agentd.py"), "--sock", sock_path],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            env=env,
        )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                before = rpc(sock_path, "ping")
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise AssertionError("agentd did not start")

        with open(os.path.join(package, "common.py"), "a", encoding="utf-8") as source:
            source.write("\n# selftest agentd reload probe\n")
        deadline = time.monotonic() + 12
        after = None
        while time.monotonic() < deadline:
            try:
                candidate = rpc(sock_path, "ping")
                if candidate["code_signature"] != before["code_signature"]:
                    after = candidate
                    break
            except OSError:
                pass
            time.sleep(0.2)
        assert after is not None, "agentd did not replace itself after source changed"
        assert not after["code_update_available"]
        original.wait(timeout=5)
        print("AGENTD AUTO-RELOAD SELFTEST PASS")
    finally:
        shutdown_agentd(sock_path)
        if original.poll() is None:
            original.terminate()
            original.wait(timeout=5)
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
