#!/usr/bin/env python3
"""Keep one MCP stdio server current without replacing its stdio connection.

The MCP server cannot replace itself in place: doing so would lose the
already-negotiated MCP initialization handshake.  server.py exits with the
private restart code after returning the restart tool's result, and this
per-orchestrator parent starts a fresh server on the same stdio pipes.  It also
watches the package's Python sources and replaces a stale server automatically.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import selectors
import subprocess
import sys
import time


RESTART_EXIT_CODE = 75
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_SCRIPT = os.path.join(PACKAGE_DIR, "server.py")
WATCH_INTERVAL = 0.5
RELOAD_DEBOUNCE = 0.75
STOP_TIMEOUT = 5.0
REPLAY_ENV = "REASONIX_MCP_LAUNCHER_REPLAY"


def _source_snapshot() -> tuple[tuple[str, str], ...]:
    """Content fingerprint of runtime Python files, stable across walk order."""
    files: list[tuple[str, str]] = []
    for root, dirs, names in os.walk(PACKAGE_DIR):
        dirs[:] = [name for name in dirs if name != "__pycache__"]
        for name in names:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            try:
                with open(path, "rb") as source:
                    digest = hashlib.sha256(source.read()).hexdigest()
            except OSError:
                digest = "missing"
            files.append((os.path.relpath(path, PACKAGE_DIR), digest))
    return tuple(sorted(files))


def _stop_server(server: subprocess.Popen[bytes]) -> None:
    if server.poll() is not None:
        return
    server.terminate()
    try:
        server.wait(timeout=STOP_TIMEOUT)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()


def _message(line: bytes) -> dict:
    try:
        value = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write(pipe, data: bytes) -> None:
    pipe.write(data)
    pipe.flush()


def _load_replay_state() -> tuple[list[bytes], str | int | None, bool]:
    raw = os.environ.pop(REPLAY_ENV, "")
    if not raw:
        return [], None, False
    try:
        state = json.loads(raw)
        frames = [base64.b64decode(frame) for frame in state.get("handshake", [])]
        return frames, state.get("initialize_id"), True
    except Exception:
        print("reasonix-mcp launcher: invalid replay state ignored", file=sys.stderr, flush=True)
        return [], None, False


def _exec_updated_launcher(handshake: list[bytes], initialize_id: str | int | None) -> None:
    state = {
        "handshake": [base64.b64encode(frame).decode("ascii") for frame in handshake],
        "initialize_id": initialize_id,
    }
    env = dict(os.environ)
    env[REPLAY_ENV] = json.dumps(state, separators=(",", ":"))
    script = os.path.abspath(__file__)
    os.execve(sys.executable, [sys.executable, script], env)


def main() -> int:
    handshake, cached_initialize_id, restarted = _load_replay_state()
    while True:
        snapshot = _source_snapshot()
        server = subprocess.Popen(
            [sys.executable, SERVER_SCRIPT],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=dict(os.environ),
        )
        assert server.stdin is not None and server.stdout is not None
        suppress_initialize_id = cached_initialize_id if restarted else None
        if restarted and handshake:
            for frame in handshake:
                _write(server.stdin, frame)

        selector = selectors.DefaultSelector()
        selector.register(sys.stdin.buffer, selectors.EVENT_READ, "host")
        selector.register(server.stdout, selectors.EVENT_READ, "server")
        host_buffer = b""
        server_buffer = b""
        host_requests: set[str | int] = set()
        server_requests: set[str | int] = set()
        source_changed = False
        reload_announced = False
        changed_snapshot: tuple[tuple[str, str], ...] | None = None
        changed_since: float | None = None
        while server.poll() is None:
            for key, _ in selector.select(WATCH_INTERVAL):
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    if key.data == "host":
                        _stop_server(server)
                        return 0
                    continue
                if key.data == "host":
                    host_buffer += chunk
                    while b"\n" in host_buffer:
                        line, host_buffer = host_buffer.split(b"\n", 1)
                        frame = line + b"\n"
                        message = _message(line)
                        if message.get("method") == "initialize" and "id" in message:
                            handshake = [frame]
                            cached_initialize_id = message["id"]
                        elif message.get("method") == "notifications/initialized":
                            if handshake:
                                handshake = handshake[:1] + [frame]
                        elif message.get("method") and "id" in message:
                            host_requests.add(message["id"])
                        elif "id" in message:
                            server_requests.discard(message["id"])
                        _write(server.stdin, frame)
                else:
                    server_buffer += chunk
                    while b"\n" in server_buffer:
                        line, server_buffer = server_buffer.split(b"\n", 1)
                        message = _message(line)
                        # The host already received initialization before a hot
                        # restart; consume only the response to our replay.
                        if suppress_initialize_id is not None and message.get("id") == suppress_initialize_id:
                            suppress_initialize_id = None
                            continue
                        if message.get("method") and "id" in message:
                            server_requests.add(message["id"])
                        if "id" in message and not message.get("method"):
                            host_requests.discard(message["id"])
                        _write(sys.stdout.buffer, line + b"\n")
            current = _source_snapshot()
            if current == snapshot:
                changed_snapshot = None
                changed_since = None
                reload_announced = False
                continue
            now = time.monotonic()
            if current != changed_snapshot:
                changed_snapshot = current
                changed_since = now
            if changed_since is None or now - changed_since < RELOAD_DEBOUNCE:
                continue
            if host_requests or server_requests or host_buffer:
                if not reload_announced:
                    print(
                        "reasonix-mcp launcher: source changed; restart queued until "
                        f"{len(host_requests) + len(server_requests)} in-flight request(s) finish",
                        file=sys.stderr,
                        flush=True,
                    )
                    reload_announced = True
                continue
            launcher_rel = os.path.relpath(os.path.abspath(__file__), PACKAGE_DIR)
            old_launcher = next((item for item in snapshot if item[0] == launcher_rel), None)
            new_launcher = next((item for item in current if item[0] == launcher_rel), None)
            if old_launcher != new_launcher:
                print(
                    "reasonix-mcp launcher: launcher source changed; replacing supervisor",
                    file=sys.stderr,
                    flush=True,
                )
                _stop_server(server)
                _exec_updated_launcher(handshake, cached_initialize_id)
            source_changed = True
            print("reasonix-mcp launcher: source changed; restarting MCP server", file=sys.stderr, flush=True)
            _stop_server(server)
            break
        selector.close()
        return_code = server.wait()
        if not source_changed and return_code != RESTART_EXIT_CODE:
            return return_code
        restarted = True


if __name__ == "__main__":
    raise SystemExit(main())
