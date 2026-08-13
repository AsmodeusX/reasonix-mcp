# reasonix-mcp — spawn and steer Reasonix agents from any MCP client

An MCP server that bridges any MCP host (Claude Code, Codex, …) to live,
interactive **Reasonix** agents. Your MCP client is the front-end; this project
adds the server it talks to, plus a detached daemon that owns the agents.
Each spawned agent runs [`reasonix acp`](https://github.com/esengine/DeepSeek-Reasonix/blob/main-v2/docs/ACP.md)
(Agent Client Protocol v1, NDJSON
JSON-RPC over stdio) in a subprocess, rooted in your project, under your own
Reasonix config and provider credentials.

## How it works

```
MCP host (Claude Code, Codex, …) ──(MCP stdio)──▶ launcher.py ──▶ server.py ──(Unix socket JSON-RPC)──▶ agentd ──(ACP stdio)──▶ reasonix acp ──▶ agents
                                   │  spawn/send/watch/poll/list/…       ▲ owns the subprocesses
                                   └─ blocking watch + elicitation ──────┘
```

`launcher.py` is the per-orchestrator MCP supervisor; `server.py` is a thin
MCP front-end; **`agentd.py` is a detached daemon that
owns the agents**. This split is what makes agents survive: close the MCP host
(or kill the MCP server) and the fleet keeps running in the daemon — a new
server reconnects to the same socket and `reasonix_list` shows everything
still there. `agentd` is auto-started by the server on first use
(socket: `~/.reasonix-mcp/agentd.sock`, log beside it) and stops only when
shut down (killing the daemon kills its agents — they carry PDEATHSIG).

- **Spawn** returns a `session_id` immediately; the agent works in the daemon.
- **Send** steers a running agent *mid-turn* via `_reasonix.io/session/steer`;
  if idle it starts a new turn.
- **Poll** returns a compact status, capped message, plan/current work,
  permission request, stop reason, and terminal error text when applicable.
  Diagnostic fields are opt-in with `detail=true`.
- **Cleanup** can stop completed agents after an idle grace period; it is
  disabled by default. Use `keep_alive=true` for interactive follow-up turns.
- **Resume** revives a stopped/crashed session from its persisted transcript.
- **Configure** switches model/effort/session options immediately when idle,
  after the current turn when active, or on the next resume when stopped.
- **Stop** cancels + closes + kills; the session stays listed and resumable.
- **Wake-up**: keep `reasonix_watch` in flight. It returns compact terminal or
  permission results directly, with no timeout and no follow-up poll by default.
  Wire notifications are diagnostics only.

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
  /home/asmodeus/reasonix-mcp/src/reasonix_mcp/launcher.py
```

For Codex, register the same launcher and raise the per-tool transport timeout
because `reasonix_watch` is intentionally long-lived:

```toml
[mcp_servers.reasonix]
command = "/home/asmodeus/reasonix-mcp/.venv/bin/python"
args = ["/home/asmodeus/reasonix-mcp/src/reasonix_mcp/launcher.py"]
tool_timeout_sec = 86400
```

Codex defaults to a finite per-tool timeout (and existing installations may
use a shorter explicit value such as 300 seconds). A
`timed out awaiting tools/call` error at that exact interval is the Codex
transport ending the call, not Reasonix timing out or an agent failing. Reload
Codex after changing `config.toml`; restarting only the Reasonix MCP server
does not reload the host's timeout setting. Never restart MCP or agentd for a
host-side tool timeout—reissue one watch for the complete remaining fleet.

Other MCP hosts register the stdio server through their own `mcp add`
equivalents—same command and arguments, different client. Then restart the MCP
client and verify the server is listed.

## Tools

| Tool | Purpose |
| --- | --- |
| `reasonix_spawn(task, cwd?, model?, work_mode?, tool_approval?, effort?, keep_alive?, idle_timeout?)` | Start an agent in the daemon on `task`; returns `session_id` + sandbox posture. Completed agents can be cleaned up after the idle grace period when cleanup is enabled, unless `keep_alive=true`. |
| `reasonix_resume(session_id, cwd?, model?, effort?, work_mode?, tool_approval?, keep_alive?, idle_timeout?)` | Revive a stopped/crashed session from its persisted transcript, optionally changing its model/options. Idempotent for live processes. |
| `reasonix_models()` | List selectable models: `provider/model` refs, default, per-model `supported_efforts`, and `price` hints where configured. |
| `reasonix_send(session_id, message, expect?)` | Forced steer: queue as mid-turn guidance, or start a new turn if idle. Never dropped. `expect="steer"` refuses to start a new turn. |
| `reasonix_configure(session_id, model?, effort?, work_mode?, tool_approval?)` | Switch session configuration while preserving history. Idle: immediate. Active: queued before next turn. Exited: persisted for `reasonix_resume`. |
| `reasonix_poll(session_id, detail?, include_events?, exclude_events?, include_thought?, include_full?, max_events?)` | Context-lean status / capped message / current plan-work / pending permission. Set `detail=true` for diagnostic events, turns, counters, transcript path, and configuration. |
| `reasonix_transcript(session_id, max_tool_calls?)` | What the agent actually did: tool calls with args, files touched (write/read/bash), roles, work duration, last text — powers rebase decisions. (Reasonix does not persist token/cost usage; these are activity metrics.) |
| `reasonix_watch(session_ids, timeout?, detail?, …poll options)` | Primary callback: block indefinitely by default until completion, permission/question, or process death. Returns a compact result; `detail=true` opts into the poll-shaped result. |
| `reasonix_wait(session_ids, timeout?)` | Legacy event-driven wait until any watched session produces output, finishes a turn, or raises a permission request. |
| `reasonix_list(include_task?, pending_only?)` | Sessions with id, status, cwd, compact task preview, plan/current work, and transcript path. `pending_only=true` returns only running, decision-blocked, or uncollected terminal sessions. |
| `reasonix_respond_permission(session_id, option_id)` | Answer a tool-approval request (`option_id` from watch/poll's `permission_request.options`, or `"cancel"`). |
| `reasonix_stop(session_id)` | Cancel + close + kill the agent (tombstone: poll keeps reporting `exited`). |
| `reasonix_restart_agentd(force?)` | Explicitly reload the detached daemon. Source changes already queue a safe automatic reload; `force=true` may terminate live agents. |
| `reasonix_restart_mcp_server()` | Explicitly restart this orchestrator's MCP server through `launcher.py`; source changes are watched automatically. |
| `reasonix_dashboard(open_browser?)` | Open or reuse the authenticated local fleet UI for current and previous agents across orchestrators. Supports live steering, stop/resume, configuration, and permission responses. |

## Local fleet dashboard

Call `reasonix_dashboard()` from any connected MCP client. It opens a dark
three-pane fleet UI at `http://127.0.0.1:8746` and returns the authenticated
URL for manual opening when the environment has no desktop browser. Override
the fixed port with `REASONIX_MCP_DASHBOARD_PORT` when 8746 is reserved by
another local service. The URL carries a
random 256-bit token in its fragment; runtime state is stored mode `0600` in
`~/.reasonix-mcp/dashboard.json`. The service refuses non-loopback binds and
authenticates every API, event stream, and action.

The sidebar groups current and previous sessions by orchestrator. Selecting an
agent shows its persisted runs, agent messages, reasoning blocks, tool calls
and results, plan, and current work. Orchestrators are collapsible preview
buttons; active runs are the default view and fleets/runs are ordered newest
first. The selected run timeline opens at its latest activity. Accepted
mid-turn steering messages are persisted in an owner-scoped orchestration
timeline and merged at their transcript byte boundary, so the guidance appears
between the agent events that happened before and after it even when Reasonix
does not write the steer into its own JSONL.
For a live agent, the user can steer or follow up, stop it, switch model,
effort, work mode, or approval policy, and answer pending permission or agent
questions. The control panel shows the values reported by the agent separately
from queued changes; an old daemon that cannot report them says `Unknown`
instead of presenting a placeholder as current state. Stopped and historical
sessions can be resumed from the UI.

Dashboard observation is deliberately separate from orchestration delivery:
it opens one read-only event socket per owner and never calls `reasonix_watch`,
`reasonix_wait`, or `reasonix_poll` to refresh state. Watching the UI therefore
does not supersede a CLI watch, consume terminal output, or add transcript data
to an orchestrator's context. Session ownership is resolved from server-side
owner sidecars; the browser cannot nominate an owner for an action. Multiple
CLI orchestrators can keep running while one dashboard observes all of their
local fleets.

Only one healthy dashboard is reused. Calling `reasonix_dashboard()` after its
HTML, CSS, JavaScript, or backend source changes replaces that verified
dashboard process and starts the current code; agentd and every agent remain
untouched. The replacement preserves port 8746 and its authentication token,
so already-open browser tabs reconnect to the upgraded dashboard.

### Completion and decision delivery

Keep `reasonix_watch(session_ids)` in flight while agents work. It returns when
a child finishes, exits, or needs a decision, with each compact result in
`results[session_id]`. By default each result contains only status,
stop reason, one capped message, permission details, plan/current work, errors,
and transcript path. Its timeout is disabled by default and it does
not wake for ordinary text chunks, so no timer loop or follow-up poll is needed.
Omit `timeout` for normal orchestration. Values such as `timeout=240` in the
self-tests are test-runner safety guards, not production defaults.
Use one watch per orchestrator fleet. If another child finishes
while a result is being handled, its terminal state remains pending and the
next watch returns it immediately. A newer overlapping watch supersedes the
older call safely; a stale canceled watch never requires an MCP-server restart.
The displaced call returns `superseded=true`, and the newest call owns the
fleet. Always include the complete remaining fleet when replacing a watch. If
the MCP host or server restarts during a watch, reconnect, recover the owned
fleet with `reasonix_list(pending_only=true)`, and watch it again; undelivered
terminal state remains pending in agentd.

Python hot reloads safely complete an in-flight watch/wait with
`server_restarted=true` before replacing the MCP front-end. Agents remain in
agentd; reissue one watch for the complete remaining fleet. Mutating calls and
server-to-client permission forms are allowed to finish before reload.

Compact watch consumes queued events so long-running sessions stay bounded.
Use `reasonix_watch(..., detail=true)` when raw event/turn detail is wanted
immediately. Full accumulated text remains available afterward through
`reasonix_poll(include_full=true)`, and tool history through
`reasonix_transcript`.

For diagnostics, the server also emits this custom notification on the wire:

```json
{"method": "reasonix/agent_event", "params": {
  "session_id": "…", "event": "turn_end", "status": "idle",
  "stop_reason": "end_turn", "transcript_path": "…", "note": "…"}}
```

- Events: `status` (plan/current-work update), `turn_end` (done/stopped/errored, with `stop_reason`),
  `permission_request` (a tool-approval **or an agent question** — see
  below), `process_exited`.
- Diagnostic only: JSON-RPC clients silently ignore unknown notifications,
  and even a standard MCP notification is delivered to the client application,
  not injected into the model's conversation. Do not build orchestration loops
  around either `reasonix/agent_event` or `notifications/message`.

> **Orchestrator loop**: `reasonix_watch(session_ids)` → handle each result →
> answer a permission or remove a terminal id → watch the remaining ids again.
> `reasonix_wait` remains available as a legacy event-driven,
> output-sensitive long poll;
> unlike watch, it requires a follow-up `reasonix_poll`.
- Diagnostic notifications are always emitted. The daemon and MCP server log
  each emit/relay result so host-side dropping is distinguishable from a
  server-side omission.

The Reasonix `[notifications].enabled` setting controls Reasonix CLI/desktop
system notifications; it is separate from this MCP transport.

Errored turns and unexpected process exits include `error_text` in the push
payload. `reasonix_watch` returns the same detail directly, so an
orchestrator can decide whether to resume or retry without inspecting the
session JSONL by hand.
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

Clients that advertise MCP elicitation receive a standard
`elicitation/create` form and the selected option is returned to Reasonix
automatically. If the client declines or cancels that form, Reasonix leaves
the ACP request pending; dismissal is not treated as rejection. Watch and
answer with `reasonix_respond_permission(session_id, "q1:1")`; the chosen label becomes
the `ask` tool result and the agent continues. A diagnostic `agent_event` is
also emitted. Verified live by `selftest_question.py`.

### Spawn defaults

Spawned agents run at **`effort = max`** on **`opencode-go/deepseek-v4-flash`**
with **`tool_approval = yolo`** by default (the user's requested defaults). All
spawn options are per-call overridable, and the defaults themselves are
env-overridable (`REASONIX_MCP_DEFAULT_MODEL`, `REASONIX_MCP_DEFAULT_EFFORT`,
`REASONIX_MCP_DEFAULT_WORK_MODE`, `REASONIX_MCP_DEFAULT_TOOL_APPROVAL`).

**Model selection**: pass `reasonix_spawn(model="<provider>/<model>", ...)` to
pick the agent's model per call — `reasonix_models()` lists the valid refs and
each model's `supported_efforts` (effort is per-model: e.g. `kimi-k3` accepts
only `high`/`max`; an unsupported value fails at spawn). OpenAI-compatible
proxy providers must opt into effort with `reasoning_protocol = "openai"`,
`supported_efforts = [...]`, and `default_effort = "..."` at provider or
model-override scope. Some gateways instead bake effort into variant model IDs;
only those gateways should be selected by suffix. If neither mechanism is
configured, spawn reports `effort` in `skipped_options`.

`reasonix_send` is **forced steer**: a message is always delivered — queued as
mid-turn guidance while a turn is running, or submitted as a new turn if the
agent is idle (or the turn ended mid-race). Messages are never dropped.
`expect` (`any` default) narrows that: `expect="steer"` raises instead of
accidentally starting a new turn; `expect="new_turn"` raises if the message
was steered into a running turn.

`reasonix_configure` changes the model without replacing the logical session
or losing its transcript. Reasonix cannot rebuild a model while a turn or
background job is active, so an active agent reports `queued=true` and keeps
the old model for that turn; the new configuration is applied before the next
turn. For a stopped/exited agent, configure first and then call
`reasonix_resume`, or pass the model/options directly to resume. Desired
configuration is persisted in a per-session sidecar so daemon/MCP restarts do
not lose it.

The same tool changes `tool_approval` (`ask`, `auto`, or `yolo`) at any point.
Idle agents change immediately; active agents use the new mode from their next
turn. To avoid deadlock, changing `ask` to `yolo` while a tool permission is
pending selects its non-persistent `allow_once` option and auto-allows further
tool gates for the remainder of that turn. It never answers an agent's explicit
question (`kind="other"`). Changing to `auto` leaves an existing decision
pending because automatic policy is not equivalent to unconditional approval.
During a rolling update, an already-running older shared agentd may not yet
have the `configure` RPC because other orchestrators still have active agents.
The hot-reloaded MCP front-end transparently persists all requested options;
it emulates `yolo` tool approvals immediately and reports other options as
waiting for the safe daemon reload instead of returning `unknown method`.

| Option | Values |
| --- | --- |
| `model` | any configured `provider/model`, e.g. `opencode-go/deepseek-v4-flash` |
| `effort` | Model-advertised values: `auto` · `disabled` · `low` · `medium` · `high` · `xhigh` · `max` |
| `work_mode` | `economy` (lean tool surface) · `balanced` (complete default) · `delivery` (requires acceptance criteria + review/verification evidence) |
| `tool_approval` | `ask` · `auto` · `yolo` (default) |

### Poll is lean (orchestrator-friendly)

A spawned session emits a burst of static setup events
(`available_commands_update` — 24 slash commands with descriptions — and
`config_option_update` — the full model catalogue). Unfiltered, that is
**~31 KB / ~8k tokens per poll** for a 4-byte reply (measured), which kills
parallel orchestration. By default `reasonix_poll` omits the entire diagnostic
event stream and returns only orchestration state: `status`, a capped `message`
when output changed, `stop_reason`, `plan`, `current_work`, pending permission,
and errors. Empty and unchanged metadata is omitted. Use `detail=true` to
retrieve the prior detailed shape. In that shape, the two static setup types
are still filtered and `events_filtered` counts what was omitted. To change
the filter:

- `include_events=["tool_call","tool_call_update","plan"]` — only these
  sessionUpdate types (permission requests are always included);
- Detailed polls return a small recent event tail (50 by default; set
  `max_events` to choose another value). `include_events` opts named types
  back in, including static setup types when explicitly named.
- `exclude_events=[...]` — drop additional types;
- the orchestrator-relevant set is `tool_call`, `tool_call_update`, `plan`,
  `permission_request`.

Poll includes `current_work` when there is active work: either the active
native tool call or the plan step marked `in_progress`. Agents spawned through
this server receive a small status contract asking them to keep that plan current.
It is injected exactly once per persisted session: follow-up turns do not
repeat it, and resume detects it in session history. Explicit task restrictions
on tools take precedence.

`turns` in detailed poll results gives completed turns as `[{text, stop_reason}]` —
clean turn boundaries (full_text alone concatenates turns).

**Thought and `full_*` are opt-in.** Reasoning is the bulk of what effort=max
models emit, so ordinary `reasonix_poll` does **not** return `thought`,
`full_thought`, `full_text`, `text`, `turns`, or events. Accumulated output
stays server-side and is available on demand:

- `include_thought=True` → `thought` (delta) + `full_thought`
- `include_full=True` → `full_text` (whole conversation; `turns` usually
  suffices — per-turn text + stop_reason)

Use `include_thought` when diagnosing a derailed agent. Defaults are
env-overridable: `REASONIX_MCP_INCLUDE_THOUGHT=1`, `REASONIX_MCP_INCLUDE_FULL=1`.

### Parallel orchestration

```
spawn 6–8 agents (note session_ids, each spawn reports its sandbox posture)
loop:
  event = reasonix_watch(all ids)      # no timeout; compact terminal/permission results
  handle event.results                 # no follow-up poll
  reasonix_send(sid, msg, expect="steer") when a discovery invalidates a round
  drop finished ids from the watch list; reasonix_stop() the rest when done
reasonix_list(pending_only=true) whenever you lose track of session_ids
```

### Caps & truncation

Poll output is capped so long gaps can't blow up the MCP host's context;
the cuts are reported, never silent:

| Field | Limit (env override) |
| --- | --- |
| `events` | recent tail (50 by default; explicit cap 200) — `events_dropped` counts the cut |
| compact poll `message` | first/last `MAX_WATCH_MESSAGE` (4k) chars — `message_truncated` |
| detailed `text` | last `MAX_DELTA_TEXT` (100k) chars — `text_truncated` |
| compact watch `message` | first/last `MAX_WATCH_MESSAGE` (4k) chars — `message_truncated` |
| `thought` / `full_thought` (only with `include_thought`) | last `MAX_FULL_TEXT` (200k) chars — `*_truncated` |
| `full_text` (only with `include_full`) | last `MAX_FULL_TEXT` (200k) chars — `full_text_truncated` |

`events_dropped` (queue/size cap) is distinct from `events_filtered`
(static-type omission). The unpolled in-server event buffer is bounded at 4000
chunk events; critical events (permission, turn end, process exit) are never
dropped.

### Sandbox posture

`reasonix_spawn` reads and returns the effective `[sandbox]` posture at spawn
time (`sandbox`, legacy `bash`, `allow_write`, `network`, `workspace_root`,
`config_file`) so an orchestrator knows up front whether agents can execute
commands and write outside cwd. Note Reasonix semantics: `bash = "off"` means
**unconfined** (execution allowed), while `bash = "enforce"` jails commands in
bubblewrap when available. The clearer posture names are `sandbox = "bwrap"`
or `sandbox = "none"`; with `none`, `allow_write` cannot be enforced for bash
and a warning is returned/logged. Sandbox posture itself is fixed for the ACP
process; session options requested during a turn apply before its next turn.
Inspect the spawn response for the effective sandbox posture. Under
`tool_approval = "ask"`, gated commands raise a
`permission_request` in watch/poll — answer with `reasonix_respond_permission`;
approving blind is not required: the request's `tool_call` carries the tool
name (`title`/`kind`) and `rawInput` (the JSON arguments).
Ask mode does not pause every shell command: Reasonix requests permission only
for commands its policy classifies as gated. Its explicit `ask` tool always
creates a user-decision request, including in YOLO mode.

### Updating the daemon

`agentd` watches `agentd.py`, `acp_bridge.py`, and `common.py`. A source change
queues an automatic restart; active turns, pending decisions, and
`keep_alive=true` sessions are never interrupted. Once agents are safely idle
and their terminal output has been polled, the daemon waits a short grace
period, closes resumable idle processes, and starts a fresh daemon from current
code. No MCP reinstallation is needed.

For explicit control, call `reasonix_restart_agentd()`. It safely reloads the
shared daemon when no live agents remain. If live agents can be discarded, use
`reasonix_restart_agentd(force=true)`; their persisted transcripts can be
resumed after the fresh daemon starts. The next tool call automatically starts
the new daemon, so no shell command or socket cleanup is needed. The daemon is
shared by MCP clients, so a restart disconnects other clients too; they
reconnect automatically on their next tool call.

### Restarting the MCP server

`launcher.py` watches the package's Python sources and replaces its MCP server
automatically after a change. It proxies stdio, replays the negotiated MCP
handshake into the new child, and leaves shared `agentd` sessions untouched,
so an already-running orchestrator can continue without being restarted.
Every orchestrator has its own launcher and refreshes independently.

`reasonix_restart_mcp_server()` remains available for an explicit reload.
Registrations that point directly to `server.py` must be changed to
`launcher.py` once; after that, source updates need no MCP reinstall.

### Orchestrator isolation

Sessions are scoped to the MCP orchestrator conversation that created them.
Codex thread IDs (and equivalent host session IDs when provided) keep two
conversations in the same project isolated while allowing that conversation
to reconnect after an IDE restart. `reasonix_list`
and all session operations only expose that orchestrator's sessions. The scope
is stable across MCP server restarts and is derived from the MCP client's name
and workspace. If multiple instances of the same CLI run in the same workspace,
set a distinct stable value in each environment:

```sh
REASONIX_MCP_ORCHESTRATOR_ID=project-a-cli-1
```

The daemon is shared, but ownership is enforced by the daemon and persisted
with each session. `reasonix_restart_agentd` remains a global operation because
restarting the shared daemon affects every orchestrator; it refuses while
another orchestrator has live agents unless `force=true`.

### Agent cleanup

After a terminal turn (including an errored turn), an agent remains available
for the idle grace period (`REASONIX_MCP_IDLE_TIMEOUT`, disabled by default, or
`idle_timeout` per spawn) so the orchestrator can poll its final output or send
a quick follow-up. It is then stopped and remains as an exited, resumable
tombstone. Set `keep_alive=true` on `reasonix_spawn` for an agent that needs
ongoing interactive turns; call `reasonix_stop` when it is no longer needed.

Use `idle_timeout=-1` to disable cleanup, `0` for immediate cleanup after a
terminal turn, or a positive number of seconds for a grace period.

## Safety

- Agents run under **your** Reasonix permissions and workspace sandbox
  (writes confined to `cwd` + `allow_write`; bash jailed where the OS sandbox
  is enabled and available). With `sandbox = "none"`/`bash = "off"`, bash is
  unjailed and `allow_write` is not enforceable.
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
  with a capped new message and current plan/work — leave it for an hour, then
  check again. `reasonix_send` steers even mid-turn.
- **Memory is bounded.** Unpolled chunk events are capped in the server; poll
  caps compact messages, while opt-in detailed results cap
  `text`/`thought`/`full_text` and the structured `events` list (tails kept,
  `*_truncated` / `events_dropped` report the cut).

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
.venv/bin/python tests/selftest_notifications.py # diagnostic event frames (no model calls)
.venv/bin/python tests/selftest_elicitation.py    # ACP decision → MCP elicitation bridge (no model calls)
.venv/bin/python tests/selftest_watch.py          # watch overlap/cancellation safety (no model calls)
.venv/bin/python tests/selftest_mcp_restart.py   # manual + source-change hot reload (no model calls)
.venv/bin/python tests/selftest_auto_reload.py   # agentd source watcher + self-replace (no model calls)
.venv/bin/python tests/selftest_prompt_injection.py # one status contract per session (no model calls)
.venv/bin/python tests/selftest_chaos.py       # cwd allowlist + dual notify + PDEATHSIG (no model calls)
.venv/bin/python tests/selftest_allow_write.py  # cross-cwd write via allow_write (real provider)
```

The selftest runs fully isolated: it copies `config.toml` + `.env` into a
scratch `REASONIX_HOME` under `/tmp` (removed on exit), so it never touches
your live `~/.reasonix` sessions and spawns the native Reasonix Go binary
directly (never the npm node shim) in its own process group that it kills on
cleanup.
