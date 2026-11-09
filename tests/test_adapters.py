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
