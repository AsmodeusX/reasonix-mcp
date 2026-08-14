#!/usr/bin/env python3
"""Shared helpers for the reasonix-mcp selftests (tests/).

Every test spawns the MCP server against a scratch REASONIX_HOME; with the
detached-daemon architecture the server auto-starts an agentd, so tests must
point REASONIX_MCP_AGENTD_SOCK at the scratch home (isolation) and shut the
daemon down when done (no lingering processes).
"""
import json
import os
import socket

# Project layout: tests/ -> root -> src/reasonix_mcp
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "src", "reasonix_mcp", "server.py")
LAUNCHER = os.path.join(ROOT, "src", "reasonix_mcp", "launcher.py")
PY = os.path.join(ROOT, ".venv", "bin", "python")


def scratch_env(scratch: str, real_home: str | None = None) -> dict:
    """Env for a scratch-isolated server run: scratch Reasonix home + socket."""
    if real_home is None:
        real_home = os.path.expanduser("~/.reasonix")
    for name in ("config.toml", ".env"):
        src = os.path.join(real_home, name)
        if os.path.isfile(src):
            import shutil
            shutil.copy2(src, os.path.join(scratch, name))
    env = dict(os.environ)
    env["REASONIX_HOME"] = scratch
    env["REASONIX_MCP_AGENTD_SOCK"] = os.path.join(scratch, "agentd.sock")
    # Most isolated protocol tests do not exercise the machine-wide browser UI
    # and may run concurrently on one host/port.
    env["REASONIX_MCP_DASHBOARD_AUTOSTART"] = "0"
    return env


def shutdown_agentd(sock: str) -> None:
    """Tell the daemon (if any) to close its agents and exit."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect(sock)
            s.sendall((json.dumps({"jsonrpc": "2.0", "id": 1, "method": "shutdown", "params": {}}) + "\n").encode())
            s.recv(1024)
    except OSError:
        pass
