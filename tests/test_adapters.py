import json

from sandbox_runtime.adapters import control_user_message, normalize, normalize_error_code


def test_runtime_error_codes_normalize_structured_rate_limits_and_unknown_values():
    assert normalize_error_code({"responseTooManyFailedAttempts": {"httpStatusCode": 429}}) == "model_rate_limited"
    assert normalize_error_code("provider_service_unavailable") == "provider_service_unavailable"
    assert normalize_error_code({"unexpected": {"detail": "private upstream body"}}) == "harness_failed"


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


def test_claude_streamed_tool_input_is_emitted_once_with_complete_arguments():
    state = {}
    frames = [
        {"type":"stream_event","event":{"type":"content_block_start","index":1,
            "content_block":{"type":"tool_use","id":"tool-1","name":"Bash","input":{}}}},
        {"type":"stream_event","event":{"type":"content_block_delta","index":1,
            "delta":{"type":"input_json_delta","partial_json":"{\"command\":\"pwd\","}}},
        {"type":"stream_event","event":{"type":"content_block_delta","index":1,
            "delta":{"type":"input_json_delta","partial_json":"\"description\":\"Inspect workspace\"}"}}},
        {"type":"stream_event","event":{"type":"content_block_stop","index":1}},
    ]
    events = [normalize("claude", json.dumps(frame), state) for frame in frames]
    assert events[:3] == [None, None, None]
    assert events[3] == {"kind":"tool_started","payload":{"name":"Bash","tool_use_id":"tool-1",
        "detail":{"command":"pwd","description":"Inspect workspace"}}}


def test_codex_agent_message_and_thread_are_normalized():
    thread = normalize("codex", json.dumps({"type": "thread.started", "thread_id": "thread-1"}))
    message = normalize("codex", json.dumps({
        "type": "item.completed",
        "item": {"type": "agent_message", "text": "done"},
    }))

    assert thread["payload"]["thread_id"] == "thread-1"
    assert message == {"kind": "text_delta", "payload": {"text": "done", "snapshot": True}}


def test_codex_and_deepseek_tool_ids_are_stable():
    codex = normalize("codex", json.dumps({
        "type": "item.completed",
        "item": {"id": "item-1", "type": "command_execution", "exit_code": 0},
    }))
    deepseek = normalize("deepseek", json.dumps({
        "type": "deepseek.notification",
        "event": {"method": "session.event", "payload": {"event": {
            "type": "tool/result",
            "data": {"message": {"toolCallId": "call-1", "isError": False}},
        }}},
    }))

    assert codex["payload"]["tool_use_id"] == "item-1"
    assert deepseek["payload"]["tool_use_id"] == "call-1"


def test_deepseek_assistant_messages_stream_and_repeated_snapshots_are_deduplicated():
    state = {}
    frame = {"type": "deepseek.notification", "event": {
        "method": "session.event", "payload": {"event": {
            "type": "assistant/message", "data": {"message": {
                "id": "message-1", "content": [{"type": "text", "text": "正在分析"}],
            }},
        }},
    }}

    assert normalize("deepseek", json.dumps(frame), state) == {
        "kind": "text_delta", "payload": {"text": "正在分析"},
    }
    assert normalize("deepseek", json.dumps(frame), state) is None
    frame["event"]["payload"]["event"]["data"]["message"]["content"][0]["text"] = "正在分析，继续处理"
    assert normalize("deepseek", json.dumps(frame), state) == {
        "kind": "text_delta", "payload": {"text": "，继续处理"},
    }


def test_deepseek_final_response_is_a_snapshot_fallback():
    assert normalize("deepseek", json.dumps({
        "type": "assistant_delta", "text": "最终回答", "snapshot": True,
    })) == {"kind": "text_delta", "payload": {"text": "最终回答", "snapshot": True}}


def test_codex_app_server_events_and_control_inputs_are_normalized():
    control = json.loads(control_user_message("追加要求"))
    assert control["type"] == "input" and control["message"] == "追加要求" and len(control["id"]) == 32
    delta = normalize("codex", json.dumps({"method":"item/agentMessage/delta","params":{"itemId":"m","delta":"hello"}}))
    started = normalize("codex", json.dumps({"method":"item/started","params":{"item":{"id":"t","type":"commandExecution","command":"pwd"}}}))
    finished = normalize("codex", json.dumps({"method":"item/completed","params":{"item":{"id":"t","type":"commandExecution","status":"completed"}}}))
    assert delta == {"kind":"text_delta","payload":{"text":"hello"}}
    assert started["kind"] == "tool_started" and started["payload"]["tool_use_id"] == "t"
    assert finished["kind"] == "tool_finished" and finished["payload"]["tool_use_id"] == "t"


def test_terminal_protocol_events_preserve_failure_instead_of_claiming_success():
    for harness, frame in [
        ('codex', {'method':'turn/completed','params':{'turn':{'status':'failed','error':{'message':'503'}}}}),
        ('codex', {'type':'harness.input.completed','status':'failed','input_ids':['a']}),
        ('claude', {'type':'result','subtype':'error_during_execution','is_error':True}),
    ]:
        assert normalize(harness, json.dumps(frame))['payload']['is_error'] is True
