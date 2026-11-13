from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import uuid

from .instructions import runtime_instructions

from deepseek_harness import DeepSeekHarness


def emit(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False, default=str), flush=True)


def public_notification(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "__dict__"):
        return vars(value)
    return {"type": type(value).__name__}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--session-id", default="")
    args = parser.parse_args()
    session_id = args.session_id or "session-" + uuid.uuid4().hex
    harness = DeepSeekHarness(
        dsh_home=os.environ["DSH_HOME"],
        cwd="/workspace/project",
        provider="deepseek-official",
        model=args.model,
        reasoning_effort=os.environ.get("DEEPSEEK_REASONING_EFFORT", "high"),
        max_tokens=32000,
        base_url=os.environ["DEEPSEEK_BASE_URL"],
        api_key=os.environ["DEEPSEEK_API_KEY"],
        env={"DSH_PERMISSION_MODE": "danger-full-access"},
        request_timeout_seconds=120,
        shutdown_timeout_seconds=10,
    )
    try:
        harness.start()
        if args.session_id:
            session = harness.resume_session(session_id)
        else:
            session = harness.start_session(session_id)
        emit({"type": "thread.started", "thread_id": session_id})
        for line in sys.stdin:
            try:
                command = json.loads(line)
            except ValueError:
                continue
            if not isinstance(command, dict) or command.get("type") != "input":
                continue
            message = runtime_instructions() + "\nUser request:\n" + command.get("message", "")
            result = session.run(message,
                on_notification=lambda value: emit({"type": "deepseek.notification", "event": public_notification(value)}))
            emit({"type": "assistant_delta", "text": result.final_response, "snapshot": True})
            if result.finish_reason != "completed":
                raise RuntimeError("DeepSeek Harness finished with " + str(result.finish_reason))
            emit({"type": "harness.input.completed", "input_ids": [command.get("id", "")], "thread_id": session_id})
    finally:
        harness.close()


if __name__ == "__main__":
    main()
