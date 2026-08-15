#!/usr/bin/env python3
"""A lying shutdown acknowledgement must be escalated and verified."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util import PY, shutdown_agentd  # noqa: E402


def fake_daemon(sock_path: str) -> None:
    # Exercise the complete escalation ladder, not just cooperative -> TERM.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.makedirs(os.path.dirname(sock_path), exist_ok=True)
    lock_fd = os.open(f"{sock_path}.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    try:
        os.unlink(sock_path)
    except FileNotFoundError:
        pass
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(sock_path)
    listener.listen()
    while True:
        connection, _ = listener.accept()
        with connection:
            stream = connection.makefile("r", encoding="utf-8")
            for line in stream:
                message = json.loads(line)
                if "id" not in message:
                    continue
                method = message.get("method")
                if method == "ping":
                    result = {
                        "sessions": 0,
                        "owners": 0,
                        "owner_scoping": True,
                        "cancel_request": True,
                        "code_signature": "lying-daemon",
                    }
                elif method == "list":
                    result = {"sessions": []}
                elif method == "restart":
                    # Reproduce the bug: acknowledge shutdown but retain both
                    # the process and lock forever.
                    result = {"shutdown": True, "agents_stopped": 49}
                else:
                    result = {}
                response = {"jsonrpc": "2.0", "id": message["id"], "result": result}
                connection.sendall((json.dumps(response) + "\n").encode())


async def exercise(home: str, sock_path: str, fake: subprocess.Popen) -> None:
    os.environ["REASONIX_HOME"] = home
    os.environ["REASONIX_MCP_AGENTD_SOCK"] = sock_path
    os.environ["REASONIX_MCP_ORCHESTRATOR_ID"] = "restart-escalation"
    sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))
    import server

    result = await server.reasonix_restart_agentd(force=True)
    assert result["restarted"] is True and result["verified"] is True
    assert result["old_pid"] == fake.pid
    assert result["new_pid"] != fake.pid
    assert result["escalation"] == "kill"
    assert result["agents_stopped"] == 49
    fake.wait(timeout=3)


def main() -> None:
    if len(sys.argv) == 3 and sys.argv[1] == "--fake-daemon":
        fake_daemon(sys.argv[2])
        return
    with tempfile.TemporaryDirectory(prefix="reasonix-restart-escalation-") as home:
        sock_path = os.path.join(home, "runtime", "agentd.sock")
        fake = subprocess.Popen([PY, __file__, "--fake-daemon", sock_path])
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not os.path.exists(sock_path):
                time.sleep(0.05)
            assert os.path.exists(sock_path), "fake daemon did not bind"
            asyncio.run(exercise(home, sock_path, fake))
            print("AGENTD RESTART ESCALATION SELFTEST PASS")
        finally:
            if fake.poll() is None:
                fake.terminate()
                fake.wait(timeout=3)
            shutdown_agentd(sock_path)


if __name__ == "__main__":
    main()
