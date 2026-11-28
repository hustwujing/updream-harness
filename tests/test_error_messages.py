from sandbox_harness.common.contracts.errors import (public_failure)


def test_provider_overload_and_transport_failures_have_actionable_public_reasons():
    code, message = public_failure('other', 'unexpected status 503 Service Unavailable: Our servers are currently overloaded. url: http://private')
    assert code == 'model_service_overloaded' and '503' in message and 'http://' not in message
    code, message = public_failure('RuntimeError', 'Runtime stream ended without a result')
    assert code == 'RuntimeStreamInterrupted' and '重连' in message


def test_sandbox_capacity_limit_is_not_reported_as_model_rate_limit():
    code,message=public_failure('infrastructure_capacity_wait',
        "Client error '429 Too Many Requests' for url '/v1/sandboxes'")
    assert code=='infrastructure_capacity_wait'
    assert '沙箱容量' in message and '不是模型限流' in message


def test_local_model_budget_exhaustion_is_not_reported_as_provider_rate_limit():
    code,message=public_failure('model_token_budget_exhausted',
        'API Error: 429 {"detail":"model_token_budget_exhausted"}')
    assert code=='model_token_budget_exhausted'
    assert '上下文' in message and '模型服务限流' not in message


def test_local_model_request_limit_has_actionable_payload_free_message():
    code,message=public_failure('model_request_too_large','private body and http://private')
    assert code=='model_request_too_large'
    assert '图片' in message and '上限' in message and 'private' not in message


def test_workspace_restore_failures_distinguish_unavailable_from_invalid_checkpoint():
    code,message=public_failure('sandbox_workspace_restore_unavailable','private runtime url')
    assert code=='sandbox_workspace_restore_unavailable'
    assert '连接' in message and '工作区快照仍安全保存' in message and 'private' not in message
    code,message=public_failure('checkpoint_restore_failed','private archive detail')
    assert code=='checkpoint_restore_failed'
    assert '校验或解包失败' in message and 'private' not in message


def test_missing_native_session_has_specific_non_replay_guidance():
    code,message=public_failure('native_session_unavailable','PRIVATE RPC detail')
    assert code=='native_session_unavailable'
    assert 'Codex 原生会话' in message and '新建会话' in message
    assert '不会自动重放' in message and 'PRIVATE' not in message
