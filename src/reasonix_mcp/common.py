"""Shared logic for the reasonix-mcp server and its agent daemon.

Pure helpers only — no MCP imports, so both `server.py` (the MCP front-end)
and `agentd.py` (the detached daemon that actually owns the agents) use the
same semantics for caps, posture, model discovery, cwd confinement, transcript
summaries, and poll shaping.
"""

from __future__ import annotations

import json
import hashlib
import os
import time
import tomllib

import acp_bridge

MAX_WATCH_SESSIONS = 64

# Long-running turns: caps for what reasonix_poll hands back to the caller,
# so hours-long agents cannot blow up its context. Env-overridable.
MAX_EVENTS_PER_POLL = int(os.environ.get("REASONIX_MCP_MAX_EVENTS_PER_POLL", "200"))  # explicit event polls (tail kept)
DEFAULT_MAX_EVENTS_PER_POLL = int(os.environ.get("REASONIX_MCP_DEFAULT_MAX_EVENTS_PER_POLL", "50"))  # ordinary poll tail
MAX_DELTA_TEXT = int(os.environ.get("REASONIX_MCP_MAX_DELTA_TEXT", "100000"))  # chars of new text/thought per poll (tail kept)
MAX_FULL_TEXT = int(os.environ.get("REASONIX_MCP_MAX_FULL_TEXT", "200000"))  # chars of full_text/full_thought (tail kept)

# Poll defaults: thought and the full_* accumulators are OPT-IN — reasoning is
# the bulk of what effort=max models emit; orchestrators act on answer + tool
# calls + status. Env defaults: REASONIX_MCP_INCLUDE_THOUGHT / _FULL.
_TRUE = ("1", "true", "yes", "on")
DEFAULT_INCLUDE_THOUGHT = os.environ.get("REASONIX_MCP_INCLUDE_THOUGHT", "").strip().lower() in _TRUE
DEFAULT_INCLUDE_FULL = os.environ.get("REASONIX_MCP_INCLUDE_FULL", "").strip().lower() in _TRUE

# Static session-setup boilerplate excluded from poll events by default (~8k
# tokens per poll for a 4-byte reply, measured).
STATIC_UPDATE_TYPES = ("available_commands_update", "config_option_update")

# Defaults for spawned agents. Env-overridable.
DEFAULT_MODEL = os.environ.get("REASONIX_MCP_DEFAULT_MODEL", "opencode-go/deepseek-v4-flash")
DEFAULT_EFFORT = os.environ.get("REASONIX_MCP_DEFAULT_EFFORT", "max")
DEFAULT_WORK_MODE = os.environ.get("REASONIX_MCP_DEFAULT_WORK_MODE", "")  # "" = follow config (balanced)
DEFAULT_TOOL_APPROVAL = os.environ.get("REASONIX_MCP_DEFAULT_TOOL_APPROVAL", "yolo")
# Completed agents can be retained briefly so an orchestrator can poll the
# final turn or send a quick follow-up. -1 disables cleanup; keep_alive=true
# also disables it per spawn for interactive sessions.
DEFAULT_IDLE_TIMEOUT = float(os.environ.get("REASONIX_MCP_IDLE_TIMEOUT", "-1"))

# The native ACP `plan` update is the authoritative todo channel. This small
# contract makes agents use it consistently for coding tasks while preserving
# explicit task-level restrictions such as "use exactly this one tool".
STATUS_PROMPT_MARKER = "[reasonix-mcp orchestrator status protocol]"
STATUS_PROMPT = f"""
{STATUS_PROMPT_MARKER}
The orchestrator can read your structured todo and current activity. Unless
the task explicitly restricts which tools you may use or asks for an exact
tool sequence, use the native plan tool for non-trivial work:
- create a concise todo of concrete steps before making changes;
- keep exactly the current step marked in_progress;
- update the plan when the current focus changes and mark finished steps
  completed;
- do not mark a step completed until it is actually verified.
Keep the plan factual and concise. This is machine-readable progress state;
you do not need to narrate it in every response.
""".strip()

# Spawn cwd confinement: agents may only root at the caller's project dir (and
# subdirs) plus the [sandbox] allow_write dirs. Escapes with
# REASONIX_MCP_ALLOW_ANY_CWD=1.
ALLOW_ANY_CWD = os.environ.get("REASONIX_MCP_ALLOW_ANY_CWD", "").strip().lower() in _TRUE

_TRANSCRIPT_WRITE_TOOLS = {"write_file", "edit_file", "multi_edit", "move_file"}
_TRANSCRIPT_READ_TOOLS = {"read_file"}

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTD_RUNTIME_FILES = ("agentd.py", "acp_bridge.py", "common.py")


def agentd_code_signature() -> str:
    """Fingerprint the source files loaded by the detached daemon.

    Content hashing also catches deployments that preserve timestamps or
    replace a file with same-sized code.
    """
    parts: list[str] = []
    for name in _AGENTD_RUNTIME_FILES:
        path = os.path.join(_PACKAGE_DIR, name)
        try:
            with open(path, "rb") as source:
                digest = hashlib.sha256(source.read()).hexdigest()
        except OSError:
            parts.append(f"{name}:missing")
        else:
            parts.append(f"{name}:{digest}")
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:24]


def reasonix_home() -> str:
    return os.environ.get("REASONIX_HOME") or os.path.expanduser("~/.reasonix")


def agentd_sock_path() -> str:
    """Unix socket the daemon listens on. Tests must isolate via
    REASONIX_MCP_AGENTD_SOCK (default: <home parent>/.reasonix-mcp/agentd.sock)."""
    env = os.environ.get("REASONIX_MCP_AGENTD_SOCK")
    if env:
        return env
    parent = os.path.dirname(reasonix_home().rstrip("/")) or "/"
    return os.path.join(parent, ".reasonix-mcp", "agentd.sock")


def orchestrator_owner_id(client_name: str = "") -> str:
    """Stable, non-secret owner key used to isolate MCP orchestrators."""
    explicit = os.environ.get("REASONIX_MCP_ORCHESTRATOR_ID", "").strip()
    scope = os.path.realpath(os.getcwd())
    seed = explicit or f"{client_name.strip()}\0{scope}"
    return "owner-" + hashlib.sha256(seed.encode()).hexdigest()[:24]


def owner_state_path(owner_id: str) -> str:
    return os.path.join(reasonix_home(), "orchestrators", f"{owner_id}.json")


def load_owner_sessions(owner_id: str) -> set[str]:
    path = owner_state_path(owner_id)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return {str(sid) for sid in data.get("session_ids", [])}
    except (OSError, ValueError, TypeError):
        return set()


def save_owner_sessions(owner_id: str, session_ids: set[str]) -> None:
    path = owner_state_path(owner_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"owner_id": owner_id, "session_ids": sorted(session_ids)}, fh)
    os.replace(tmp, path)


def session_owner_path(session_id: str) -> str:
    return os.path.join(reasonix_home(), "sessions", f"{session_id}.owner.json")


def write_session_owner(session_id: str, owner_id: str) -> None:
    if not session_id or not owner_id:
        return
    path = session_owner_path(session_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"session_id": session_id, "owner_id": owner_id}, fh)
    os.replace(tmp, path)


def read_session_owner(session_id: str) -> str | None:
    try:
        with open(session_owner_path(session_id), encoding="utf-8") as fh:
            data = json.load(fh)
        owner_id = data.get("owner_id")
        return str(owner_id) if owner_id else None
    except (OSError, ValueError, TypeError):
        return None


def _cap(s: str, limit: int) -> tuple[str, bool]:
    """Return (text, truncated) — keep the tail (most recent) past the limit."""
    if len(s) <= limit:
        return s, False
    return s[-limit:], True


def task_preview(task: object, limit: int = 120) -> str:
    """Keep fleet listings useful without replaying orchestration prompts."""
    value = str(task or "").replace("\r", " ").replace("\n", " ").strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


def task_with_status_protocol(task: str) -> str:
    """Add the progress contract without overriding explicit task constraints."""
    if STATUS_PROMPT in task:
        return task
    return f"{task.rstrip()}\n\n{STATUS_PROMPT}"


def transcript_has_status_protocol(session_id: str) -> bool:
    """Whether a persisted session already received the progress contract."""

    def contains(value: object) -> bool:
        if isinstance(value, str):
            return STATUS_PROMPT in value
        if isinstance(value, dict):
            return any(contains(item) for item in value.values())
        if isinstance(value, list):
            return any(contains(item) for item in value)
        return False

    session_dir = os.path.join(reasonix_home(), "sessions")
    for suffix in (".jsonl", ".events.jsonl"):
        path = os.path.join(session_dir, f"{session_id}{suffix}")
        try:
            with open(path, encoding="utf-8") as transcript:
                for line in transcript:
                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if contains(record):
                        return True
        except OSError:
            continue
    return False


def agent_status(agent: acp_bridge.ReasonixAgent) -> str:
    if agent.status == "exited":
        return "exited"
    return "running" if agent.active_turn else "idle"


def build_agent_event(agent: acp_bridge.ReasonixAgent, kind: str, payload: dict) -> dict:
    """The agent_event payload pushed on progress / turn end / permission /
    process exit.

    Built by the daemon (which owns the agents), relayed verbatim by the MCP
    server as both the custom `reasonix/agent_event` notification and a
    standard `notifications/message`.
    """
    event: dict = {
        "session_id": agent.session_id,
        "event": kind,
        "status": agent_status(agent),
        # Internal routing metadata. The MCP server consumes and removes this
        # before forwarding notifications to the orchestrator.
        "_owner_id": getattr(agent, "owner_id", ""),
    }
    if kind == acp_bridge.EV_TURN_END:
        event["stop_reason"] = payload.get("stopReason")
        event["transcript_path"] = payload.get("transcriptPath")
        if payload.get("error") is not None:
            event["error"] = payload["error"]
            event["error_text"] = error_text(payload["error"])
        elif payload.get("stopReason") == "error" and getattr(agent, "last_error", None) is not None:
            event["error"] = agent.last_error
            event["error_text"] = getattr(agent, "last_error_text", "") or error_text(agent.last_error)
        event["note"] = "agent finished its turn (done, stopped, or errored)"
    elif kind == acp_bridge.EV_PERMISSION:
        event["note"] = "agent is waiting on a decision (tool approval or a question)"
        tool_call = (payload.get("params") or {}).get("toolCall") or {}
        event["permission_request"] = {
            "request_id": payload.get("id"),
            "tool_call": tool_call,
            "options": list((payload.get("params") or {}).get("options") or []),
        }
        if tool_call.get("title"):
            event["title"] = tool_call["title"]
            if tool_call.get("kind") == "other":
                event["question"] = tool_call["title"]
    elif kind == acp_bridge.EV_PROCESS_EXIT:
        event["note"] = "agent process exited"
        if payload.get("error") is not None:
            event["error"] = payload["error"]
        if payload.get("error_text"):
            event["error_text"] = payload["error_text"]
    elif kind == acp_bridge.EV_STATUS:
        event["event"] = "status"
        event["plan"] = list(payload.get("plan") or [])
        event["current_work"] = payload.get("current_work")
        event["note"] = "agent progress plan changed"
    return event


def error_text(error: object) -> str:
    """Extract useful human-readable text from an ACP/JSON-RPC error."""
    if isinstance(error, str):
        return error
    if isinstance(error, dict):
        message = error.get("message")
        if message:
            return str(message)
        data = error.get("data")
        if isinstance(data, str):
            return data
        if data is not None:
            return str(data)
    return str(error) if error is not None else ""


def sandbox_posture() -> dict:
    """Effective [sandbox] posture from the config the agent will load.

    Reads <Reasonix home>/config.toml (REASONIX_HOME, else ~/.reasonix) when a
    spawn is made. Project-level reasonix.toml may further override in the
    Reasonix process. `bash` is retained for compatibility; `sandbox` exposes
    the unambiguous posture (`bwrap` or `none`).
    """
    path = os.path.join(reasonix_home(), "config.toml")
    posture: dict = {
        "workspace_root": "",
        "allow_write": [],
        "forbid_read": [],
        "bash": "enforce",
        "sandbox": None,
        "network": False,
        "config_file": path if os.path.isfile(path) else None,
    }
    if posture["config_file"]:
        try:
            with open(path, "rb") as fh:
                cfg = tomllib.load(fh)
            for key in ("workspace_root", "allow_write", "forbid_read", "bash", "sandbox", "network"):
                if key in cfg.get("sandbox", {}):
                    posture[key] = cfg["sandbox"][key]
        except Exception:
            pass
    # Reasonix's historical `bash` spelling is still accepted.  Expose the
    # clearer posture name as well: `off` means no bwrap and therefore no
    # enforceable allow_write boundary.
    if posture.get("sandbox") not in ("bwrap", "none"):
        posture["sandbox"] = "bwrap" if posture.get("bash") == "enforce" else "none"
    warnings: list[str] = []
    if posture.get("allow_write") and posture.get("sandbox") == "none":
        warnings.append(
            "allow_write is configured but bash/sandbox is unconfined; "
            "the allow_write restriction cannot be enforced for bash commands"
        )
    posture["warnings"] = warnings
    return posture


def cwd_allowed(cwd: str) -> bool:
    if ALLOW_ANY_CWD:
        return True
    target = os.path.realpath(cwd)
    roots = [os.path.realpath(os.getcwd())]
    roots += [os.path.realpath(p) for p in (sandbox_posture().get("allow_write") or [])]
    for root in roots:
        if target == root or target.startswith(root + os.sep):
            return True
    return False


def reasonix_config_files() -> list[str]:
    """Resolved config.toml files in resolution order (user, then project)."""
    files = [os.path.join(reasonix_home(), "config.toml")]
    project = os.path.join(os.getcwd(), "reasonix.toml")
    if os.path.isfile(project):
        files.append(project)
    return [p for p in files if os.path.isfile(p)]


def available_models() -> dict:
    """Configured models a spawned agent can select, derived from config.

    Includes per-model price hints where the config declares them (provider
    `price` fallback, `prices` per-model override; currency included). The
    authoritative validation is the session's own configOptions.
    """
    overrides: dict[str, dict] = {}
    providers: dict[str, dict] = {}
    default_model: str | None = None
    for path in reasonix_config_files():
        try:
            with open(path, "rb") as fh:
                cfg = tomllib.load(fh)
        except Exception:
            continue
        if isinstance(cfg.get("default_model"), str):
            default_model = cfg["default_model"]
        for prov in cfg.get("providers", []):
            if not isinstance(prov, dict) or not prov.get("name"):
                continue
            name = prov["name"]
            models = prov.get("models") or ([prov["model"]] if prov.get("model") else [])
            providers[name] = {
                "models": [str(m) for m in models],
                "default": str(prov.get("default") or (models[0] if models else "")),
                "base_url": prov.get("base_url", ""),
                "model_overrides": dict(prov.get("model_overrides") or {}),
                "price": dict(prov.get("price") or {}),
                "prices": dict(prov.get("prices") or {}),
                "context_window": prov.get("context_window"),
            }
        for model_ref, meta in (cfg.get("model_overrides") or {}).items():
            if isinstance(meta, dict):
                overrides[str(model_ref)] = dict(meta)

    def price_for(prov: dict, model: str) -> dict:
        p = prov.get("prices", {}).get(model) or prov.get("price", {})
        return {k: v for k, v in (p or {}).items() if k in ("cache_hit", "input", "output", "currency")}

    models: list[dict] = []
    for name, prov in providers.items():
        prov_overrides = prov.get("model_overrides") or {}
        for m in prov["models"]:
            ref = f"{name}/{m}"
            meta = (
                prov_overrides.get(m)
                or prov_overrides.get(ref)
                or overrides.get(m)
                or overrides.get(ref)
                or {}
            )
            entry: dict = {"ref": ref, "provider": name, "model": m, "default": ref == f"{name}/{prov['default']}"}
            efforts = meta.get("supported_efforts")
            if efforts:
                entry["supported_efforts"] = list(efforts)
            if meta.get("default_effort"):
                entry["default_effort"] = str(meta["default_effort"])
            if meta.get("context_window"):
                entry["context_window"] = int(meta["context_window"])
            elif prov.get("context_window"):
                entry["context_window"] = int(prov["context_window"])
            price = price_for(prov, m)
            if price:
                entry["price"] = price
            models.append(entry)
    models.sort(key=lambda e: (e["provider"].lower(), e["model"].lower()))
    return {
        "default_model": default_model,
        "models": models,
        "effort_options": ["auto", "disabled", "high", "max"],
        "config_files": reasonix_config_files(),
        "note": "Pass a model 'ref' (provider/model) to reasonix_spawn(model=...).",
    }


def transcript_summary(path: str, max_tool_calls: int) -> dict:
    """Summarize one agent transcript jsonl: roles, tool calls, files touched,
    work duration, last text. Format observed: OpenAI-style chat lines with
    `tool_calls [{name, arguments}]`, `reasoning_content`, `workDurationMs`.
    NB: Reasonix does not persist token/cost usage in the session files, so
    activity metrics are the available telemetry.
    """
    roles: dict[str, int] = {}
    tool_calls: list[dict] = []
    files: dict[str, str] = {}  # path -> op (write > bash > read wins)
    total_ms = 0
    last_text = ""
    lines = 0
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            lines += 1
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            role = msg.get("role")
            if role:
                roles[role] = roles.get(role, 0) + 1
            wd = msg.get("workDurationMs")
            if isinstance(wd, (int, float)):
                total_ms += wd
            if isinstance(msg.get("content"), str) and not msg.get("tool_calls"):
                last_text = msg["content"]
            for tc in msg.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                name = tc.get("name") or "?"
                args = tc.get("arguments") or ""
                arg_obj: dict = {}
                if isinstance(args, str):
                    try:
                        arg_obj = json.loads(args)
                    except json.JSONDecodeError:
                        arg_obj = {"command": args}
                elif isinstance(args, dict):
                    arg_obj = args
                entry: dict = {"name": name, "arguments": arg_obj}
                if tc.get("id"):
                    entry["tool_call_id"] = tc["id"]
                tool_calls.append(entry)
                if name in _TRANSCRIPT_WRITE_TOOLS:
                    for key in ("path", "file_path", "target", "source"):
                        if arg_obj.get(key):
                            files.setdefault(str(arg_obj[key]), "write")
                    if name == "multi_edit":
                        for edit in arg_obj.get("edits") or []:
                            if isinstance(edit, dict) and edit.get("path"):
                                files.setdefault(str(edit["path"]), "write")
                elif name in _TRANSCRIPT_READ_TOOLS and arg_obj.get("path"):
                    files.setdefault(str(arg_obj["path"]), "read")
                elif name == "bash" and arg_obj.get("command"):
                    files.setdefault("bash: " + str(arg_obj["command"])[:80], "bash")
    return {
        "lines": lines,
        "roles": roles,
        "tool_calls": tool_calls[-max_tool_calls:],
        "total_tool_calls": len(tool_calls) if len(tool_calls) <= max_tool_calls else "more_than_" + str(max_tool_calls),
        "files_touched": [{"path": p, "op": op} for p, op in list(files.items())[-200:]],
        "total_work_duration_ms": int(total_ms),
        "last_text": last_text[:2000],
        "note": "Reasonix does not persist token/cost usage in session files; these are activity metrics.",
    }


def shape_poll(
    agent: acp_bridge.ReasonixAgent,
    include_events: list[str] | None = None,
    exclude_events: list[str] | None = None,
    include_thought: bool = DEFAULT_INCLUDE_THOUGHT,
    include_full: bool = DEFAULT_INCLUDE_FULL,
    max_events: int | None = None,
) -> dict:
    """Drain one agent's event queue into the poll result shape. Shared by the
    daemon (authoritative) — identical semantics to the original server tool."""
    # Explicit include_events opts a type back in, including the static setup
    # events that are omitted from ordinary polls.
    include = set(include_events or [])
    exclude = (set(STATIC_UPDATE_TYPES) - include) | set(exclude_events or [])
    text_parts: list[str] = []
    thought_parts: list[str] = []
    events: list[dict] = []
    filtered = 0
    permission = None
    turn_end = None
    process_exit = None

    for kind, payload in agent.poll():
        if kind == acp_bridge.EV_UPDATE:
            su = payload.get("sessionUpdate")
            content = payload.get("content") or {}
            # NB: some variants (e.g. tool_call_update) carry content as a LIST.
            if isinstance(content, dict) and content.get("type") == "text" and su in (
                "agent_message_chunk", "agent_thought_chunk",
            ):
                if su == "agent_message_chunk":
                    text_parts.append(content.get("text", ""))
                else:
                    thought_parts.append(content.get("text", ""))
                continue
            if (include and su not in include) or su in exclude:
                filtered += 1
                continue
            events.append({"type": su, **payload})
        elif kind == acp_bridge.EV_PERMISSION:
            permission = {
                "request_id": payload["id"],
                "tool_call": payload["params"].get("toolCall", {}),
                "options": payload["params"].get("options", []),
            }
            events.append({"type": "permission_request", **permission})
        elif kind == acp_bridge.EV_TURN_END:
            turn_end = payload
        elif kind == acp_bridge.EV_PROCESS_EXIT:
            process_exit = payload

    event_limit = max_events
    if event_limit is None:
        # A caller that explicitly selects event types is asking to inspect
        # the event stream; ordinary polls only need a small recent tail.
        event_limit = MAX_EVENTS_PER_POLL if include else DEFAULT_MAX_EVENTS_PER_POLL
    event_limit = max(0, min(int(event_limit), MAX_EVENTS_PER_POLL))
    dropped_events = 0
    if len(events) > event_limit:
        dropped_events = len(events) - event_limit
        events = events[-event_limit:] if event_limit else []
    text, text_truncated = _cap("".join(text_parts), MAX_DELTA_TEXT)

    status = agent_status(agent)
    result: dict = {
        "session_id": agent.session_id,
        "status": status,
        "text": text,
        "text_truncated": text_truncated,
        "turns": agent.snapshot_turns(),
        "plan": list(getattr(agent, "plan_entries", []) or []),
        "current_work": (
            dict(getattr(agent, "current_work", None))
            if getattr(agent, "current_work", None) else None
        ),
        "events": events,
        "events_dropped": dropped_events,
        "events_filtered": filtered,
        "permission_request": permission,
    }
    if include_thought:
        thought, thought_truncated = _cap("".join(thought_parts), MAX_DELTA_TEXT)
        full_thought, full_thought_truncated = _cap(agent.full_thought, MAX_FULL_TEXT)
        result["thought"] = thought
        result["thought_truncated"] = thought_truncated
        result["full_thought"] = full_thought
        result["full_thought_truncated"] = full_thought_truncated
    if include_full:
        full_text, full_text_truncated = _cap(agent.full_text, MAX_FULL_TEXT)
        result["full_text"] = full_text
        result["full_text_truncated"] = full_text_truncated
    if turn_end is not None:
        agent.stop_reason = turn_end.get("stopReason")
        result["stop_reason"] = turn_end.get("stopReason")
        result["transcript_path"] = turn_end.get("transcriptPath")
    if agent.stop_reason and "stop_reason" not in result:
        result["stop_reason"] = agent.stop_reason
    if process_exit is not None:
        result["process_exit"] = process_exit
    if turn_end is not None and turn_end.get("error") is not None:
        result["error"] = turn_end["error"]
        result["error_text"] = error_text(turn_end["error"])
    elif process_exit is not None and process_exit.get("error_text"):
        result["error"] = process_exit.get("error") or process_exit["error_text"]
        result["error_text"] = process_exit["error_text"]
    elif getattr(agent, "last_error", None) is not None:
        result["error"] = agent.last_error
        result["error_text"] = getattr(agent, "last_error_text", "") or error_text(agent.last_error)
    return result
