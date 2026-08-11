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


def _host_request_kind(message: dict) -> str:
    """Classify an in-flight host request for safe hot-reload decisions."""
    method = str(message.get("method") or "")
    if method != "tools/call":
        return method
    params = message.get("params") or {}
    return str(params.get("name") or method)


def _interrupted_wait_response(request_id: str | int, tool_name: str) -> bytes:
    """Finish a durable long poll before replacing its MCP subprocess."""
    payload = {
        "woke": [],
        "timed_out": False,
        "server_restarted": True,
        "note": (
            "Reasonix MCP reloaded updated code. Agents kept running; reissue "
            "one reasonix_watch for the complete remaining fleet."
        ),
    }
    if tool_name == "reasonix_watch":
        payload["results"] = {}
    else:
        payload["sessions"] = {}
    response = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [{"type": "text", "text": json.dumps(payload)}],
            "isError": False,
        },
    }
    return (json.dumps(response) + "\n").encode()


def _tools_changed_notification() -> bytes:
    """Ask an already-open MCP host to refresh tools after hot reload."""
    return b'{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}\n'


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
        host_requests: dict[str | int, str] = {}
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
                            host_requests[message["id"]] = _host_request_kind(message)
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
                            _write(sys.stdout.buffer, _tools_changed_notification())
                            continue
                        if message.get("method") and "id" in message:
                            server_requests.add(message["id"])
                        if "id" in message and not message.get("method"):
                            host_requests.pop(message["id"], None)
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
            interruptible = {
                request_id: kind for request_id, kind in host_requests.items()
                if kind in ("reasonix_watch", "reasonix_wait")
            }
            unsafe_host_requests = set(host_requests) - set(interruptible)
            if unsafe_host_requests or server_requests or host_buffer:
                if not reload_announced:
                    print(
                        "reasonix-mcp launcher: source changed; restart queued until "
                        f"{len(unsafe_host_requests) + len(server_requests)} "
                        "non-interruptible request(s) finish",
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
                for request_id, tool_name in interruptible.items():
                    _write(
                        sys.stdout.buffer,
                        _interrupted_wait_response(request_id, tool_name),
                    )
                _exec_updated_launcher(handshake, cached_initialize_id)
            source_changed = True
            print("reasonix-mcp launcher: source changed; restarting MCP server", file=sys.stderr, flush=True)
            _stop_server(server)
            # Watch/wait state is durable in agentd. Complete these host calls
            # explicitly so a hot reload never becomes a transport error and
            # the orchestrator can immediately watch the surviving fleet again.
            for request_id, tool_name in interruptible.items():
                _write(
                    sys.stdout.buffer,
                    _interrupted_wait_response(request_id, tool_name),
                )
                host_requests.pop(request_id, None)
            break
        selector.close()
        return_code = server.wait()
        if not source_changed and return_code != RESTART_EXIT_CODE:
            return return_code
        restarted = True


if __name__ == "__main__":
    raise SystemExit(main())
