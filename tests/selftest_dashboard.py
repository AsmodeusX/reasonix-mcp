#!/usr/bin/env python3
"""Deterministic dashboard auth, ownership, and observer checks."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGE = os.path.join(ROOT, "src", "reasonix_mcp")
sys.path.insert(0, PACKAGE)

import common  # noqa: E402
import dashboard  # noqa: E402


def request(url: str, token: str = "") -> tuple[int, dict, dict]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=2)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        body = json.loads(response.read())
        return response.status, body, dict(response.headers)


async def observer_check(sock_path: str, owner: str) -> None:
    methods: list[str] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            message = json.loads(line)
            methods.append(str(message.get("method")))
            writer.write(b'{"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n')
            writer.write((json.dumps({
                "jsonrpc": "2.0",
                "method": "agent_event",
                "params": {
                    "_owner_id": owner,
                    "session_id": "session-one",
                    "event": "turn_end",
                    "status": "idle",
                },
            }) + "\n").encode())
            await writer.drain()
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(handle, sock_path)
    state = dashboard.DashboardState("token")
    queue: asyncio.Queue = asyncio.Queue()
    state.subscribers.add(queue)
    task = asyncio.create_task(state.observe(owner))
    try:
        event = await asyncio.wait_for(queue.get(), timeout=2)
        assert event["type"] == "agent_event", event
        assert event["event"]["session_id"] == "session-one", event
        assert event["event"]["owner_id"] == owner, event
        assert "_owner_id" not in event["event"], event
        assert methods == ["ping"], methods
    finally:
        state.closed = True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        server.close()
        await server.wait_closed()


async def concurrent_start(mcp_server) -> tuple[dict, dict]:
    first, second = await asyncio.gather(
        mcp_server.reasonix_dashboard(open_browser=False),
        mcp_server.reasonix_dashboard(open_browser=False),
    )
    return first, second


def main() -> None:
    scratch = tempfile.mkdtemp(prefix="rxmcp-dashboard-")
    old_home = os.environ.get("REASONIX_HOME")
    old_sock = os.environ.get("REASONIX_MCP_AGENTD_SOCK")
    dashboard_pid = 0
    try:
        reasonix_home = os.path.join(scratch, "home")
        sock_path = os.path.join(scratch, "agentd.sock")
        os.environ["REASONIX_HOME"] = reasonix_home
        os.environ["REASONIX_MCP_AGENTD_SOCK"] = sock_path

        common.write_session_owner("session-one", "owner-one")
        common.save_owner_sessions("owner-one", {"session-one"})
        assert dashboard.owner_for_session("session-one") == "owner-one"
        try:
            dashboard.owner_for_session("session-two")
            raise AssertionError("unknown session was accepted")
        except dashboard.DashboardError as exc:
            assert exc.status == 404

        asyncio.run(observer_check(sock_path, "owner-one"))
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock_path)
        shutil.rmtree(os.path.join(reasonix_home, "orchestrators"), ignore_errors=True)
        shutil.rmtree(os.path.join(reasonix_home, "sessions"), ignore_errors=True)

        import server as mcp_server  # noqa: E402

        first, second = asyncio.run(concurrent_start(mcp_server))
        assert sorted((first["started"], second["started"])) == [False, True]
        assert first["pid"] == second["pid"], (first, second)
        dashboard_pid = int(first["pid"])
        state_file = mcp_server.DASHBOARD_STATE
        with open(state_file, encoding="utf-8") as fh:
            runtime = json.load(fh)
        runtime["source_signature"] = "stale-test-signature"
        dashboard.write_state(state_file, runtime)
        replaced = asyncio.run(mcp_server.reasonix_dashboard(open_browser=False))
        assert replaced["started"] is True, replaced
        assert int(replaced["pid"]) != dashboard_pid, (replaced, dashboard_pid)
        dashboard_pid = int(replaced["pid"])
        with open(state_file, encoding="utf-8") as fh:
            runtime = json.load(fh)
        token = runtime["token"]
        base = runtime["url"].split("/#", 1)[0]
        status, body, headers = request(base + "/api/health", token)
        assert body["ok"] is True
        assert body["source_signature"] == dashboard.dashboard_signature()
        headers = {key.lower(): value for key, value in headers.items()}
        assert headers["cache-control"] == "no-store"
        assert "frame-ancestors 'none'" in headers["content-security-policy"]
        status, body, _ = request(base + "/api/health")
        assert status == 401 and body["error"] == "unauthorized", (status, body)
        stream_request = urllib.request.Request(
            base + "/api/events",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(stream_request, timeout=2) as stream:
            assert stream.readline() == b"event: ready\n"
            assert stream.readline() == b"data: {}\n"
        mode = os.stat(state_file).st_mode & 0o777
        assert mode == 0o600, oct(mode)
        print("DASHBOARD SELFTEST PASS")
    finally:
        if dashboard_pid:
            with contextlib.suppress(ProcessLookupError):
                os.kill(dashboard_pid, 15)
            for _ in range(30):
                try:
                    os.kill(dashboard_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.1)
        if old_home is None:
            os.environ.pop("REASONIX_HOME", None)
        else:
            os.environ["REASONIX_HOME"] = old_home
        if old_sock is None:
            os.environ.pop("REASONIX_MCP_AGENTD_SOCK", None)
        else:
            os.environ["REASONIX_MCP_AGENTD_SOCK"] = old_sock
        shutil.rmtree(scratch, ignore_errors=True)
if __name__ == "__main__":
    main()
