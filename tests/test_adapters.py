import json

from sandbox_runtime.adapters import normalize


def test_claude_partial_text_and_final_snapshot_normalization():
    partial = normalize("claude", json.dumps({
        "type": "stream_event",
        "event": {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "hello"}},
    }))
    snapshot = normalize("claude", json.dumps({
        "type": "assistant",
        "session_id": "native-session",
        "message": {"content": [{"type": "text", "text": "hello world"}]},
    }))

    assert partial == {"kind": "text_delta", "payload": {"text": "hello"}}
    assert snapshot["payload"] == {"text": "hello world", "snapshot": True, "thread_id": "native-session"}


def test_claude_non_public_protocol_frames_are_not_transcript_steps():
    assert normalize("claude", json.dumps({
        "type": "stream_event",
        "event": {"type": "message_start", "message": {}},
    })) is None


def test_codex_agent_message_and_thread_are_normalized():
    thread = normalize("codex", json.dumps({"type": "thread.started", "thread_id": "thread-1"}))
    message = normalize("codex", json.dumps({
        "type": "item.completed",
        "item": {"type": "agent_message", "text": "done"},
    }))

    assert thread["payload"]["thread_id"] == "thread-1"
    assert message == {"kind": "text_delta", "payload": {"text": "done", "snapshot": True}}
