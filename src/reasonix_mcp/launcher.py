#!/usr/bin/env python3
"""Keep one MCP stdio server alive across an in-band restart request.

The MCP server cannot replace itself in place: doing so would lose the
already-negotiated MCP initialization handshake.  server.py exits with the
private restart code after returning the restart tool's result, and this
per-orchestrator parent starts a fresh server on the same stdio pipes.
"""

from __future__ import annotations

import os
import subprocess
import sys


RESTART_EXIT_CODE = 75
SERVER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py")


def main() -> int:
    while True:
        result = subprocess.run([sys.executable, SERVER_SCRIPT], env=dict(os.environ))
        if result.returncode != RESTART_EXIT_CODE:
            return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
