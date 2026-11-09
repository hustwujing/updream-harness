from __future__ import annotations

import json
import os
import shlex
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HarnessCommand:
    argv: list[str]
    environment: dict[str, str]


def command(harness: str, model: str, native_thread_id: str = "") -> HarnessCommand:
    gateway = os.environ["HARNESS_GATEWAY_URL"].rstrip("/")
    token_file = "/run/harness/capability-token"
    capability_token = Path(token_file).read_text().strip()
    common = {"HARNESS_CAPABILITY_TOKEN_FILE": token_file, "HARNESS_MODEL": model}
    override = os.environ.get("HARNESS_COMMAND_JSON")
    if override:
        value = json.loads(override)
        return HarnessCommand(list(value[harness]), common)
    if harness == "codex":
        codex_home = Path("/workspace/state/codex")
        codex_home.mkdir(parents=True, exist_ok=True)
        config = "\n".join([
            "model = " + json.dumps(model),
            'model_provider = "sandbox_gateway"',
            'approval_policy = "never"',
            'sandbox_mode = "danger-full-access"',
            "model_reasoning_effort = " + json.dumps(os.environ.get("HARNESS_REASONING_EFFORT", "high")),
            '[model_providers.sandbox_gateway]',
            'name = "Sandbox Harness capability gateway"',
            "base_url = " + json.dumps(gateway + "/v1/models/codex"),
            'wire_api = "responses"',
            'env_key = "OPENAI_API_KEY"',
            'requires_openai_auth = false',
            'request_max_retries = 1',
            'stream_max_retries = 1',
            '[features]',
            'multi_agent = false',
            '[shell_environment_policy]',
            'inherit = "all"',
            '',
        ])
        (codex_home / "config.toml").write_text(config)
        argv = ["codex", "exec", "--json", "--skip-git-repo-check", "--model", model, "-"]
        if native_thread_id:
            argv = ["codex", "exec", "resume", native_thread_id, "--json", "-"]
        return HarnessCommand(argv, {**common, "CODEX_HOME": str(codex_home), "OPENAI_API_KEY": capability_token})
    if harness == "claude":
        argv = ["claude", "-p", "--output-format", "stream-json", "--verbose", "--include-partial-messages",
                "--permission-mode", "bypassPermissions", "--model", model]
        if native_thread_id:
            argv += ["--resume", native_thread_id]
        return HarnessCommand(argv, {**common, "ANTHROPIC_BASE_URL": gateway + "/v1/models/claude", "ANTHROPIC_API_KEY": capability_token})
    if harness == "deepseek":
        argv = ["python", "-m", "sandbox_runtime.deepseek_runner", "--model", model]
        if native_thread_id:
            argv += ["--session-id", native_thread_id]
        return HarnessCommand(argv, {
            **common,
            "DSH_HOME": "/workspace/state/dsh",
            "DSH_PERMISSION_MODE": "danger-full-access",
            "DEEPSEEK_BASE_URL": gateway + "/v1/models/deepseek",
            "DEEPSEEK_API_KEY": capability_token,
            "DEEPSEEK_REASONING_EFFORT": os.environ.get("HARNESS_REASONING_EFFORT", "high"),
        })
    raise ValueError("Unsupported Harness")


def normalize(harness: str, line: str) -> dict:
    try:
        value = json.loads(line)
    except (ValueError, TypeError):
        return {"kind": "text_delta", "payload": {"text": line + "\n"}}
    kind = value.get("type") or value.get("event") or "tool_output"
    if harness == "codex" and kind == "thread.started":
        return {"kind": "step_finished", "payload": {"name": "Codex session", "thread_id": value.get("thread_id", "")}}
    if harness == "codex" and kind == "item.completed":
        item = value.get("item") or {}
        if item.get("type") == "agent_message":
            return {"kind": "text_delta", "payload": {"text": item.get("text", ""), "snapshot": True}}
        if item.get("type") == "error" and "fallback metadata" in item.get("message", ""):
            return {"kind": "warning", "payload": {"text": item.get("message", ""), "code": "model_metadata_fallback"}}
        return {"kind": "tool_finished", "payload": {"name": item.get("type", "tool"), "result": item}}
    if harness == "codex" and kind == "item.started":
        item = value.get("item") or {}
        return {"kind": "tool_started", "payload": {"name": item.get("type", "tool"), "detail": item}}
    if harness == "codex" and kind == "turn.completed":
        return {"kind": "step_finished", "payload": {"name": "Codex response", "usage": value.get("usage") or {}}}
    if harness == "codex" and kind == "turn.started":
        return None
    if harness == "deepseek" and kind == "thread.started":
        return {"kind": "step_finished", "payload": {"name": "DeepSeek session", "thread_id": value.get("thread_id", "")}}
    if harness == "deepseek" and kind == "deepseek.notification":
        notification = value.get("event") or {}
        event = (notification.get("payload") or {}).get("event") or {}
        event_type = event.get("type", "")
        data = event.get("data") or {}
        if event_type in {"tool/start", "tool/call", "tool_use"}:
            return {"kind": "tool_started", "payload": {"name": data.get("name", "tool"), "detail": data}}
        if event_type in {"tool/end", "tool/result", "tool_result"}:
            return {"kind": "tool_finished", "payload": {"name": data.get("name", "tool"), "result": data}}
        return None
    if harness == "claude" and kind == "stream_event":
        native = value.get("event") or {}
        delta = native.get("delta") or {}
        if native.get("type") == "content_block_delta" and delta.get("type") == "text_delta":
            return {"kind": "text_delta", "payload": {"text": delta.get("text", "")}}
        if native.get("type") == "content_block_start" and (native.get("content_block") or {}).get("type") == "tool_use":
            block = native["content_block"]
            return {"kind": "tool_started", "payload": {"name": block.get("name", "tool"), "tool_use_id": block.get("id", ""), "detail": block.get("input") or {}}}
        return None
    if harness == "claude" and kind == "system" and value.get("subtype") == "init":
        return {"kind": "step_finished", "payload": {"name": "Claude session", "thread_id": value.get("session_id", "")}}
    if harness == "claude" and kind == "assistant":
        texts = [item.get("text", "") for item in (value.get("message") or {}).get("content", []) if item.get("type") == "text"]
        if texts:
            return {"kind": "text_delta", "payload": {"text": "".join(texts), "snapshot": True, "thread_id": value.get("session_id", "")}}
        return None
    if harness == "claude" and kind == "user":
        results = [item for item in (value.get("message") or {}).get("content", []) if item.get("type") == "tool_result"]
        if results:
            item = results[0]
            return {"kind": "tool_finished", "payload": {
                "name": "tool",
                "tool_use_id": item.get("tool_use_id", ""),
                "result": value.get("tool_use_result") or item.get("content", ""),
                "is_error": bool(item.get("is_error")),
            }}
        return None
    if harness == "claude" and kind == "result":
        return {"kind": "step_finished", "payload": {"name": "Claude response", "usage": value.get("usage") or {}, "thread_id": value.get("session_id", "")}}
    if kind in {"text_delta", "response.output_text.delta", "assistant_delta"}:
        return {"kind": "text_delta", "payload": {"text": value.get("delta") or value.get("text") or ""}}
    if kind in {"tool_started", "tool_use", "item.started"}:
        return {"kind": "tool_started", "payload": {"name": value.get("name") or value.get("tool") or "tool", "detail": value.get("input") or {}}}
    if kind in {"tool_finished", "tool_result", "item.completed"}:
        return {"kind": "tool_finished", "payload": {"name": value.get("name") or value.get("tool") or "tool", "result": value.get("result") or value.get("output") or {}}}
    if kind in {"step_started", "step_progress", "step_finished", "artifact_created", "warning", "error", "waiting_user"}:
        return {"kind": kind, "payload": value.get("payload") or value}
    return {"kind": "tool_output", "payload": {"event": value}}
