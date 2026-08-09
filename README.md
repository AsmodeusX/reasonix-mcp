# reasonix-mcp — spawn and steer Reasonix agents from any MCP client

An MCP server that bridges any MCP host (Claude Code, Codex, …) to live,
interactive **Reasonix** agents. Your MCP client is the front-end; this project
adds the server it talks to, plus a detached daemon that owns the agents.
Each spawned agent runs `reasonix acp` (Agent Client Protocol v1, NDJSON
JSON-RPC over stdio) in a subprocess, rooted in your project, under your own
Reasonix config and provider credentials.

## How it works

```
MCP host (Claude Code, Codex, …) ──(MCP stdio)──▶ server.py ──(Unix socket JSON-RPC)──▶ agentd ──(ACP stdio)──▶ reasonix acp ──▶ agents
                                   │  spawn/send/poll/wait/list/…        ▲ owns the subprocesses
                                   └─ relays agent_event pushes ─────────┘
```

`server.py` is a thin MCP front-end; **`agentd.py` is a detached daemon that
owns the agents**. This split is what makes agents survive: close the MCP host
(or kill the MCP server) and the fleet keeps running in the daemon — a new
server reconnects to the same socket and `reasonix_list` shows everything
still there. `agentd` is auto-started by the server on first use
(socket: `~/.reasonix-mcp/agentd.sock`, log beside it) and stops only when
shut down (killing the daemon kills its agents — they carry PDEATHSIG).

- **Spawn** returns a `session_id` immediately; the agent works in the daemon.
- **Send** steers a running agent *mid-turn* via `_reasonix.io/session/steer`;
  if idle it starts a new turn.
- **Poll** returns what changed since last poll: text, turns, plan, events,
  permission requests, stop reason.
- **Resume** revives a stopped/crashed session from its persisted transcript.
- **Stop** cancels + closes + kills; the session stays listed and resumable.
- **Callbacks**: the daemon pushes `reasonix/agent_event` (best-effort);
  `reasonix_wait` is the guaranteed callback.

## Setup

```sh
cd ~/reasonix-mcp
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Requires `reasonix` on PATH (verified: v1.17.20) and a configured provider
(`reasonix setup`).

## Register with your MCP client

Project scope (this directory): the included `.mcp.json` is picked up
automatically when the MCP host runs in `~/reasonix-mcp`.

User scope (available in every project) — example with Claude Code:

```sh
claude mcp add reasonix --scope user -- \
  /home/asmodeus/reasonix-mcp/.venv/bin/python \
  /home/asmodeus/reasonix-mcp/src/reasonix_mcp/server.py
```

Other MCP hosts (Codex, etc.) register stdio servers through their own
`mcp add` equivalents — same command + args, different client. Then restart
your MCP client and verify the server is listed.

## Tools

| Tool | Purpose |
| --- | --- |
| `reasonix_spawn(task, cwd?, model?, work_mode?, tool_approval?, effort?)` | Start an agent in the daemon on `task`; returns `session_id` + sandbox posture. `model` selects the agent's model (see `reasonix_models`). |
| `reasonix_resume(session_id, cwd?)` | Revive a stopped/crashed session from its persisted transcript. |
| `reasonix_models()` | List selectable models: `provider/model` refs, default, per-model `supported_efforts`, and `price` hints where configured. |
| `reasonix_send(session_id, message, expect?)` | Forced steer: queue as mid-turn guidance, or start a new turn if idle. Never dropped. `expect="steer"` refuses to start a new turn. |
| `reasonix_poll(session_id, include_events?, exclude_events?, include_thought?, include_full?)` | New output / status / completed turns / current `plan` / pending permission request. Static boilerplate events filtered by default. |
| `reasonix_transcript(session_id, max_tool_calls?)` | What the agent actually did: tool calls with args, files touched (write/read/bash), roles, work duration, last text — powers rebase decisions. (Reasonix does not persist token/cost usage; these are activity metrics.) |
| `reasonix_wait(session_ids, timeout?)` | Block until any watched session produces output, finishes a turn, or raises a permission request. |
| `reasonix_list()` | All live sessions: id, status, cwd, task, transcript_path — regain control after losing ids. |
| `reasonix_respond_permission(session_id, option_id)` | Answer a tool-approval request (`option_id` from poll's `permission_request.options`, or `"cancel"`). |
| `reasonix_stop(session_id)` | Cancel + close + kill the agent (tombstone: poll keeps reporting `exited`). |

### Agent-done callbacks (push)

When a spawned agent **finishes its turn** (done, stopped, or errored), **needs
a tool-approval decision**, or **its process exits**, the server pushes a
custom MCP notification on the wire — the orchestrator is called back instead
of having to poll:

```json
{"method": "reasonix/agent_event", "params": {
  "session_id": "…", "event": "turn_end", "status": "idle",
  "stop_reason": "end_turn", "transcript_path": "…", "note": "…"}}
```

- Events: `turn_end` (done/stopped/errored, with `stop_reason`),
  `permission_request` (a tool-approval **or an agent question** — see
  below), `process_exited`.
- Best-effort: MCP custom notifications are never fatal, and a client that
does not know the method simply ignores it — **`reasonix_wait` /
`reasonix_poll` remain the guaranteed channel**. The server also emits a
standard `notifications/message` (info level, same payload) on the legacy
handshake era, so clients with standard log handling see it too.
Whether/how the MCP host surfaces either to the model is client-version
dependent.

> **Orchestrator loop**: treat the push as an accelerator, never the source of
> truth. The authoritative callback is `reasonix_wait(session_ids, timeout)`:
> it wakes on any terminal state or new output and returns each session's
> `status` **and `stop_reason`** — so an errored or interrupted agent is
> visible immediately without a follow-up poll. Pattern: `wait` → for each
> woke session, `poll` → act on `stop_reason` (e.g. `"error"` → stop/retry,
> `"end_turn"` → collect result).
- Disable with `REASONIX_MCP_NOTIFY=0`.
- Verified at the wire level by `selftest_notifications.py` (raw JSON-RPC
client — the SDK client validates server notifications against known types
and may drop custom ones).

### Agent questions

The agent can put a structured multiple-choice question to the orchestrator
mid-task via its built-in `ask` tool — and **YOLO does not bypass it** (a
question is a genuine user decision, not a tool approval). Questions ride the
same `session/request_permission` channel (kind `"other"`), so they surface
identically:

```json
{"permission_request": {"request_id": 7,
  "tool_call": {"title": "Which approach should we take?", "kind": "other",
                 "toolCallId": "ask-1-q1", "rawInput": {"id":"q1","options":[…],"question":…}},
  "options": [{"optionId": "q1:1", "name": "A"}, {"optionId": "q1:2", "name": "B"}, …]}}
```

Answer with `reasonix_respond_permission(session_id, "q1:1")` — the chosen
label is returned to the agent as the `ask` tool result and it continues.
The `agent_event` push also fires (`event: "permission_request"`, with the
question in `title`). Verified live by `selftest_question.py`.

### Spawn defaults

Spawned agents run at **`effort = max`** on **`opencode-go/deepseek-v4-flash`**
with **`tool_approval = yolo`** by default (the user's requested defaults). All
spawn options are per-call overridable, and the defaults themselves are
env-overridable (`REASONIX_MCP_DEFAULT_MODEL`, `REASONIX_MCP_DEFAULT_EFFORT`,
`REASONIX_MCP_DEFAULT_WORK_MODE`, `REASONIX_MCP_DEFAULT_TOOL_APPROVAL`).

**Model selection**: pass `reasonix_spawn(model="<provider>/<model>", ...)` to
pick the agent's model per call — `reasonix_models()` lists the valid refs and
each model's `supported_efforts` (effort is per-model: e.g. `kimi-k3` accepts
only `high`/`max`; an unsupported value fails at spawn). Some gateway models
bake effort into the id (`omniroute/codex/gpt-5.6-luna-{low,medium,high,xhigh,
max}`): they advertise no `effort` config option, so spawn skips it and
reports `skipped_options` in the result — pick the variant id instead.

`reasonix_send` is **forced steer**: a message is always delivered — queued as
mid-turn guidance while a turn is running, or submitted as a new turn if the
agent is idle (or the turn ended mid-race). Messages are never dropped.
`expect` (`any` default) narrows that: `expect="steer"` raises instead of
accidentally starting a new turn; `expect="new_turn"` raises if the message
was steered into a running turn.

| Option | Values |
| --- | --- |
| `model` | any configured `provider/model`, e.g. `opencode-go/deepseek-v4-flash` |
| `effort` | `auto` · `disabled` · `high` · `max` |
| `work_mode` | `economy` (lean tool surface) · `balanced` (complete default) · `delivery` (requires acceptance criteria + review/verification evidence) |
| `tool_approval` | `ask` · `auto` · `yolo` (default) |

### Poll is lean (orchestrator-friendly)

A spawned session emits a burst of static setup events
(`available_commands_update` — 24 slash commands with descriptions — and
`config_option_update` — the full model catalogue). Unfiltered, that is
**~31 KB / ~8k tokens per poll** for a 4-byte reply (measured), which kills
parallel orchestration. By default `reasonix_poll` **omits those two types**
from `events` (they are implied by spawn and available via `transcript_path`);
`events_filtered` counts what was omitted. To change the filter:

- `include_events=["tool_call","tool_call_update","plan"]` — only these
  sessionUpdate types (permission requests are always included);
- `exclude_events=[...]` — drop additional types;
- the orchestrator-relevant set is `tool_call`, `tool_call_update`, `plan`,
  `permission_request`.

`turns` in poll results gives completed turns as `[{text, stop_reason}]` —
clean turn boundaries (full_text alone concatenates turns).

**Thought and `full_*` are opt-in.** Reasoning is the bulk of what effort=max
models emit, so `reasonix_poll` does **not** return `thought` / `full_thought` /
`full_text` by default — it returns only what changed (`text` delta, `turns`,
events, status). They stay accumulated server-side and are available on demand:

- `include_thought=True` → `thought` (delta) + `full_thought`
- `include_full=True` → `full_text` (whole conversation; `turns` usually
  suffices — per-turn text + stop_reason)

Use `include_thought` when diagnosing a derailed agent. Defaults are
env-overridable: `REASONIX_MCP_INCLUDE_THOUGHT=1`, `REASONIX_MCP_INCLUDE_FULL=1`.

### Parallel orchestration

```
spawn 6–8 agents (note session_ids, each spawn reports its sandbox posture)
loop:
  reasonix_wait(all ids, timeout=30)   # one call, wakes on any output/idle/permission
  for sid in woke: reasonix_poll(sid)  # lean; only the sid that woke
  reasonix_send(sid, msg, expect="steer") when a discovery invalidates a round
  drop finished ids from the watch list; reasonix_stop() the rest when done
reasonix_list() whenever you lose track of session_ids
```

### Caps & truncation

Poll output is capped so long gaps can't blow up the MCP host's context;
the cuts are reported, never silent:

| Field | Limit (env override) |
| --- | --- |
| `events` | last `MAX_EVENTS_PER_POLL` (200) — `events_dropped` counts the cut |
| `text` | last `MAX_DELTA_TEXT` (100k) chars — `text_truncated` |
| `thought` / `full_thought` (only with `include_thought`) | last `MAX_FULL_TEXT` (200k) chars — `*_truncated` |
| `full_text` (only with `include_full`) | last `MAX_FULL_TEXT` (200k) chars — `full_text_truncated` |

`events_dropped` (queue/size cap) is distinct from `events_filtered`
(static-type omission). The unpolled in-server event buffer is bounded at 4000
chunk events; critical events (permission, turn end, process exit) are never
dropped.

### Sandbox posture

`reasonix_spawn` returns the effective `[sandbox]` posture
(`bash`, `allow_write`, `network`, `workspace_root`, `config_file`) so an
orchestrator knows up front whether agents can execute commands and write
outside cwd. Note Reasonix semantics: `bash = "off"` means **unconfined**
(execution allowed), `bash = "enforce"` jails commands in bubblewrap when
available. Under `tool_approval = "ask"`, gated commands raise a
`permission_request` in poll — answer with `reasonix_respond_permission`;
approving blind is not required: the request's `tool_call` carries the tool
name (`title`/`kind`) and `rawInput` (the JSON arguments).

## Safety

- Agents run under **your** Reasonix permissions and workspace sandbox
  (writes confined to `cwd` + `allow_write`; bash jailed where the OS sandbox
  is enabled and available).
- **Spawn cwd is confined**: `reasonix_spawn(cwd=…)` is rejected unless the
  target is the MCP host's project dir (or a subdir) or one of the
  `[sandbox] allow_write` dirs — a prompt-injected orchestrator can't spawn
  agents that write anywhere (the server runs with your full permissions and
  `yolo` default). Escape hatch: `REASONIX_MCP_ALLOW_ANY_CWD=1`.
- **Agents die with the server**: PDEATHSIG guarantees a killed MCP server
  can never orphan `reasonix acp` processes.
- `tool_approval` per spawn: `yolo` (default; approve except protected
  decisions), `auto` (follow configured permission rules), or `ask` (relay
  every approval through `reasonix_respond_permission`).
- `reasonix_spawn` returns the effective sandbox posture — check it before
  assigning tasks that require running commands.
- `cwd` defaults to the MCP host's project root; pass an explicit `cwd` to
  scope an agent elsewhere.

## Long-running agents

A spawned agent may legitimately run **for hours** — long thinking, tool
loops, implementing across many files. The bridge is built for that:

- **Nothing blocks.** `reasonix_spawn` / `reasonix_send` / `reasonix_poll`
  return immediately; the agent works in its own subprocess. The MCP host stays
  fully responsive while the agent grinds.
- **No default timeout.** A turn runs until it finishes or you call
  `reasonix_stop` (cancels the turn, closes the session, kills the process).
- **You can come back anytime.** `reasonix_poll` reports `status: "running"`
  with everything new since your last poll — leave it for an hour, then check
  again. `reasonix_send` steers even mid-turn.
- **Memory is bounded.** Unpolled chunk events are capped in the server; poll
  results cap `text`/`thought`/`full_text` and the structured `events` list
  (tails kept, `*_truncated` / `events_dropped` flags report the cut) so a
  long gap can't blow up the MCP host's context.

One constraint, now mostly lifted: agents live in the **daemon**, not the MCP
server — the MCP host can close and come back and the fleet is still running
(`reasonix_list`). The daemon itself is the survival boundary: kill it and its
agents die (PDEATHSIG); a crashed session's work survives on disk and can be
revived with `reasonix_resume`.

## Testing

```sh
.venv/bin/python tests/selftest.py              # spawn → poll → steer → stop (real provider)
.venv/bin/python tests/selftest_daemon.py       # survival across server kill + resume + 6-way concurrency (real provider)
.venv/bin/python tests/selftest_permission.py   # ask-mode permission round-trip (real provider)
.venv/bin/python tests/selftest_question.py     # agent asks a question via `ask` (real provider)
.venv/bin/python tests/selftest_transcript.py   # transcript + plan fields (real provider)
.venv/bin/python tests/selftest_orchestrator.py # list/wait/filtering/posture (no model calls)
.venv/bin/python tests/selftest_notifications.py # agent-done push callback (no model calls)
.venv/bin/python tests/selftest_chaos.py       # cwd allowlist + dual notify + PDEATHSIG (no model calls)
.venv/bin/python tests/selftest_allow_write.py  # cross-cwd write via allow_write (real provider)
```

The selftest runs fully isolated: it copies `config.toml` + `.env` into a
scratch `REASONIX_HOME` under `/tmp` (removed on exit), so it never touches
your live `~/.reasonix` sessions and spawns the native Reasonix Go binary
directly (never the npm node shim) in its own process group that it kills on
cleanup.
