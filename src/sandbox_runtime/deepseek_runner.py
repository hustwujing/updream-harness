from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

from deepseek_harness import DeepSeekHarness


def emit(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False, default=str), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--session-id", default="")
    args = parser.parse_args()
    prompt = sys.stdin.read()
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
    emit({"type": "thread.started", "thread_id": session_id})
    try:
        harness.start()
        if args.session_id:
            harness.resume_session(session_id)
        result = harness.start_session(session_id).run(
            prompt,
            on_notification=lambda value: emit({"type": "deepseek.notification", "event": value}),
        )
        emit({"type": "assistant_delta", "text": result.final_response})
        if result.finish_reason != "completed":
            raise RuntimeError("DeepSeek Harness finished with " + str(result.finish_reason))
    finally:
        harness.close()


if __name__ == "__main__":
    main()
