# Design: reasonix-mcp — Claude Code ↔ Reasonix agent bridge

Date: 2026-08-09
Status: validated (wire protocol verified empirically against `reasonix v1.17.20`)

## Goal

Let Claude Code spawn live, steerable Reasonix coding agents and communicate
with them back and forth. Claude Code is the MCP client; this project is the
MCP server that bridges to Reasonix.

## Architecture

```
Claude Code (MCP client)
   │  stdio JSON-RPC (MCP)
   ▼
reasonix-mcp/server.py   (FastMCP server, stdio transport)
   │  spawns one `reasonix acp` subprocess per agent
   ▼
reasonix acp  (ACP v1: NDJSON JSON-RPC 2.0 over stdio)
   │  per-session: session/new, session/prompt, _reasonix.io/session/steer, ...
   ▼
Reasonix agent (Go engine, workspace-rooted, provider from user config)
```

- **MCP transport**: stdio. Claude Code launches `server.py` via `.mcp.json` or
  `claude mcp add` (same pattern as the existing `codegraph` stdio server in
  `~/.claude.json`).
- **ACP transport**: one `reasonix acp` subprocess per spawned agent. Each ACP
  session owns an independent controller/cwd/model/history (no state leaks).
  stderr is diagnostics only — never merged into the JSON-RPC stdout channel.
- **No client capability advertisement**: the bridge does not advertise
  `fs/*` or `terminal/*` in `initialize`, so Reasonix uses its own local
  workspace tools (file writes sandboxed to cwd, bash jailed where available).

## MCP tool surface

| Tool | Behavior |
| --- | --- |
| `reasonix_spawn(task, cwd?, model?, work_mode?, tool_approval?, effort?)` | Starts `reasonix acp`, `initialize`, `session/new`, applies config overrides (`model` via `session/set_config_option`), immediately starts the first `session/prompt` turn. Returns `{session_id, status, sandbox}` (sandbox = effective `[sandbox]` posture). Non-blocking. |
| `reasonix_models()` | Lists selectable models from the resolved config: `provider/model` refs, default, per-model `supported_efforts`/`default_effort` (provider-level `model_overrides` included). |
| `reasonix_send(session_id, message, expect?)` | Forced delivery: `_reasonix.io/session/steer` if a turn is active, else a new `session/prompt` turn — never dropped. `expect` = `any` \| `steer` \| `new_turn`; refused calls have no side effect. |
| `reasonix_poll(session_id, include_events?, exclude_events?, include_thought?, include_full?)` | Drains the event queue: delta text, `turns` (completed turns with stop_reason), current `plan`, structured events, permission requests, status. Static setup events filtered by default (`events_filtered`). `thought`/`full_thought`/`full_text` are opt-in (`include_thought`, `include_full`) — reasoning is the token bulk; it stays accumulated server-side for diagnosis polls. |
| `reasonix_transcript(session_id, max_tool_calls?)` | Summarizes the on-disk transcript: roles, tool calls (name + arguments), files touched (write/read/bash), work duration, last text — the input for rebasing a round when one agent's discovery invalidates others' work. |
| `reasonix_wait(session_ids, timeout?)` | Blocks until any watched session produces output, finishes a turn, or raises a permission request; replaces N polling round-trips. |
| `reasonix_list()` | All live sessions: id, status, cwd, opening task — regain control after losing ids. |
| `reasonix_respond_permission(session_id, option_id)` | Answers a pending `session/request_permission` (`selected` with the advertised optionId, or `cancelled`). |
| `reasonix_stop(session_id)` | `session/cancel` (notification) + `session/close` + kill subprocess; tombstone keeps poll reporting `exited`. |

## Key protocol facts (verified against the binary)

- `initialize` → `agentCapabilities._meta["reasonix.io"].sessionSteer.method` =
  `_reasonix.io/session/steer`. Steer with no active turn → `-32600 InvalidRequest`;
  accepted → `{}`. (Probe `acp_probe3.py`.)
- `session/prompt` stays open until the turn ends; streams `session/update`
  notifications (`agent_message_chunk`, `agent_thought_chunk`, `tool_call`,
  `tool_call_update`, `plan`, `config_option_update`, `available_commands_update`);
  resolves with `{stopReason: end_turn|cancelled|error}`. (Probe `acp_probe2.py`.)
- `session/request_permission` is an inbound JSON-RPC *request* (has an id);
  the client answers with `result: {outcome: {outcome: "selected", optionId} |
  {outcome: "cancelled"}}` (protocol.go).
- `session/set_config_option` (configId `model`|`effort`|`work_mode`|
  `tool_approval`) rebuilds asynchronously and may emit `session/update`
  notifications before its id-routed response arrives — the bridge routes
  responses by JSON-RPC id, never by line order.
- `session/cancel` is a notification (no response).
- Default `tool_approval` is `ask`; the bridge defaults spawned agents to
  `auto` (allow within Reasonix's sandbox) and surfaces any permission request
  through `reasonix_poll` / `reasonix_respond_permission`.

## Concurrency model

- Per agent: a reader thread parses NDJSON lines, routing responses to
  id-keyed waiters, `session/update` notifications and permission requests to a
  thread-safe event queue. Tool handlers (FastMCP async) drain the queue;
  writes are serialized by a per-agent lock.
- The MCP server keeps a `session_id → agent` registry; all subprocesses are
  terminated on server shutdown (atexit) and die anyway when stdin closes.
- **Long-running turns** (hours of thinking + tool calls) are first-class:
  spawn/send/poll never block on the agent; there is no default timeout
  (`reasonix_stop` is the manual kill). The unpolled event queue is bounded
  (`MAX_QUEUED_EVENTS`, dropping only droppable update events — permission /
  turn-end / process-exit are never dropped), text is accumulated
  incrementally (amortized O(1)/chunk, not a per-poll join), and poll results
  cap `text`/`thought`/`full_text`/`events` (tail kept, `*_truncated` /
  `events_dropped` flags) so a long gap cannot blow up Claude Code's context.
  Agents live as long as the MCP server process (the Claude Code session).

## Safety

- Agents run under the user's own Reasonix config/permissions with Reasonix's
  workspace sandbox; `tool_approval` is explicit per spawn (`yolo` default,
  `ask` relays permission requests, `auto` follows configured rules).
- `reasonix_send` is forced-steer: messages are always delivered — steered into
  the running turn, or submitted as a new turn when idle (never dropped).
- `cwd` defaults to the MCP server's cwd (= Claude Code project root).

## Testing

`selftest.py` drives the server through the official `mcp` client SDK over
stdio against the real `reasonix acp`: spawn (tiny task) → poll until output →
send steer → poll → stop. Run in background (model latency).
