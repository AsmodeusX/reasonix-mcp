#!/usr/bin/env python3
"""Deterministic dashboard auth, ownership, and observer checks."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import socket
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


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request(url: str, password: str = "") -> tuple[int, dict, dict]:
    headers = {"Authorization": f"Bearer {password}"} if password else {}
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
    state = dashboard.DashboardState("test-password")
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
    old_dashboard_port = os.environ.get("REASONIX_MCP_DASHBOARD_PORT")
    old_dashboard_password = os.environ.get("REASONIX_MCP_DASHBOARD_PASSWORD")
    dashboard_pid = 0
    routined_pid = 0
    try:
        reasonix_home = os.path.join(scratch, "home")
        sock_path = os.path.join(scratch, "agentd.sock")
        os.environ["REASONIX_HOME"] = reasonix_home
        os.environ["REASONIX_MCP_AGENTD_SOCK"] = sock_path
        os.environ["REASONIX_MCP_DASHBOARD_PORT"] = str(free_port())
        os.environ["REASONIX_MCP_DASHBOARD_PASSWORD"] = "CHANGEME"
        # dashboard is imported above so pin its scheduler constants to this
        # test's runtime before the app lifespan starts it.
        dashboard.routined.STATE_PATH = os.path.join(scratch, "routined.json")
        dashboard.routined.PROCESS_LOCK = os.path.join(scratch, "routined.lock")
        import server as mcp_server  # noqa: E402

        common.write_session_owner("session-one", "owner-one")
        common.save_owner_sessions("owner-one", {"session-one"})
        transcript_path = os.path.join(reasonix_home, "sessions", "session-one.jsonl")
        with open(transcript_path, "w", encoding="utf-8") as fh:
            for row in [
                {"role": "system", "content": 'Current workspace: "/tmp/project"'},
                {"role": "user", "createdAt": 1_786_000_000_000, "content": (
                    '<capability-route version="1">internal routing</capability-route>\n'
                    "Build the requested feature"
                )},
                {"role": "assistant", "reasoning_content": "Inspect the inputs", "tool_calls": [
                    {"id": "call-1", "name": "read_file", "arguments": '{"path":"a"}'}
                ]},
                {"role": "tool", "name": "read_file", "tool_call_id": "call-1", "content": "contents"},
            ]:
                fh.write(json.dumps(row) + "\n")
        event = common.record_orchestrator_message(
            "session-one", "owner-one", "Focus on the parser", "steered"
        )
        assert os.stat(common.session_timeline_path("session-one")).st_mode & 0o777 == 0o600
        assert event["transcript_offset"] == os.path.getsize(transcript_path)
        with open(transcript_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "role": "assistant", "content": "Implemented **successfully**."
            }) + "\n")
        assert dashboard.owner_for_session("session-one") == "owner-one"
        assert dashboard.transcript_preview("session-one")["task"] == "Build the requested feature"
        messages, transcript = dashboard.transcript_messages("session-one")
        entries = transcript["runs"][0]["entries"]
        assert [entry["kind"] for entry in entries] == [
            "message", "reasoning", "tool_call", "tool_result",
            "orchestrator_message", "message",
        ], entries
        assert entries[-2]["text"] == "Focus on the parser"
        assert messages[-1]["text"] == "Implemented **successfully**."
        original_rpc = mcp_server._rpc
        original_owner = mcp_server._owner_id
        original_owned = mcp_server._owned_sessions

        async def accepted_steer(_method: str, _params: dict, **_kwargs) -> dict:
            return {"session_id": "session-one", "delivered": "steered"}

        mcp_server._rpc = accepted_steer
        mcp_server._owner_id = "owner-one"
        mcp_server._owned_sessions = {"session-one"}
        try:
            send_result = asyncio.run(mcp_server.reasonix_send(
                "session-one", "Second steer", expect="steer"
            ))
        finally:
            mcp_server._rpc = original_rpc
            mcp_server._owner_id = original_owner
            mcp_server._owned_sessions = original_owned
        assert send_result["timeline_recorded"] is True
        timeline = common.read_orchestrator_timeline("session-one", "owner-one")
        assert [event["message"] for event in timeline] == [
            "Focus on the parser", "Second steer"
        ]

        async def timeline_change_check() -> None:
            state = dashboard.DashboardState("test-password")
            queue: asyncio.Queue = asyncio.Queue()
            state.subscribers.add(queue)
            await state.sync_timeline_files()
            common.record_orchestrator_message(
                "session-one", "owner-one", "Third steer", "steered"
            )
            await state.sync_timeline_files()
            changed = queue.get_nowait()
            assert changed == {
                "type": "timeline_changed", "session_id": "session-one"
            }

        asyncio.run(timeline_change_check())
        missing_messages, missing_transcript = dashboard.transcript_messages("missing")
        assert missing_messages == []
        assert missing_transcript["runs"] == []
        assert missing_transcript["tool_calls"] == []
        json.dumps({"messages": missing_messages, "transcript": missing_transcript})
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
        assert replaced["url"] == first["url"], (replaced, first)
        dashboard_pid = int(replaced["pid"])
        with open(state_file, encoding="utf-8") as fh:
            runtime = json.load(fh)
        password = runtime["password"]
        assert password == "CHANGEME"
        assert "#" not in runtime["url"] and "CHANGEME" not in runtime["url"]
        base = runtime["url"].rstrip("/")
        status, body, headers = request(base + "/api/health", password)
        assert body["ok"] is True
        assert body["source_signature"] == dashboard.dashboard_signature()
        headers = {key.lower(): value for key, value in headers.items()}
        assert headers["cache-control"] == "no-store"
        assert "frame-ancestors 'none'" in headers["content-security-policy"]
        status, templates, _ = request(base + "/api/routine-templates", password)
        assert status == 200
        assert len(templates["templates"]) >= 10
        assert any(item["template_id"] == "github-issue-maintainer"
                   for item in templates["templates"])
        status, body, _ = request(base + "/api/health")
        assert status == 401 and body["error"] == "unauthorized", (status, body)
        stream_request = urllib.request.Request(
            base + "/api/events",
            headers={"Authorization": f"Bearer {password}"},
        )
        with urllib.request.urlopen(stream_request, timeout=2) as stream:
            assert stream.readline() == b"event: ready\n"
            assert stream.readline() == b"data: {}\n"
        mode = os.stat(state_file).st_mode & 0o777
        assert mode == 0o600, oct(mode)
        for _ in range(50):
            if os.path.exists(dashboard.routined.STATE_PATH):
                with open(dashboard.routined.STATE_PATH, encoding="utf-8") as fh:
                    routined_pid = int(json.load(fh).get("pid") or 0)
                break
            time.sleep(0.02)
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
        if routined_pid:
            with contextlib.suppress(ProcessLookupError):
                os.kill(routined_pid, 15)
        if old_home is None:
            os.environ.pop("REASONIX_HOME", None)
        else:
            os.environ["REASONIX_HOME"] = old_home
        if old_sock is None:
            os.environ.pop("REASONIX_MCP_AGENTD_SOCK", None)
        else:
            os.environ["REASONIX_MCP_AGENTD_SOCK"] = old_sock
        if old_dashboard_port is None:
            os.environ.pop("REASONIX_MCP_DASHBOARD_PORT", None)
        else:
            os.environ["REASONIX_MCP_DASHBOARD_PORT"] = old_dashboard_port
        if old_dashboard_password is None:
            os.environ.pop("REASONIX_MCP_DASHBOARD_PASSWORD", None)
        else:
            os.environ["REASONIX_MCP_DASHBOARD_PASSWORD"] = old_dashboard_password
        shutil.rmtree(scratch, ignore_errors=True)
if __name__ == "__main__":
    main()
