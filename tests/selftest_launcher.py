#!/usr/bin/env python3
"""Deterministic launcher request classification/reload-result checks."""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "reasonix_mcp"))

import launcher  # noqa: E402


def main() -> None:
    watch = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {"name": "reasonix_watch", "arguments": {"session_ids": ["a"]}},
    }
    poll = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {"name": "reasonix_poll", "arguments": {"session_id": "a"}},
    }
    assert launcher._host_request_kind(watch) == "reasonix_watch"
    assert launcher._host_request_kind(poll) == "reasonix_poll"

    response = json.loads(
        launcher._interrupted_wait_response(4, "reasonix_watch").decode()
    )
    assert response["id"] == 4
    assert response["result"]["isError"] is False
    payload = json.loads(response["result"]["content"][0]["text"])
    assert payload["server_restarted"] is True
    assert payload["results"] == {}
    print("LAUNCHER SELFTEST PASS")


if __name__ == "__main__":
    main()
