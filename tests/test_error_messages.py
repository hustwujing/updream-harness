from sandbox_harness.errors import public_failure


def test_provider_overload_and_transport_failures_have_actionable_public_reasons():
    code, message = public_failure('other', 'unexpected status 503 Service Unavailable: Our servers are currently overloaded. url: http://private')
    assert code == 'model_service_overloaded' and '503' in message and 'http://' not in message
    code, message = public_failure('RuntimeError', 'Runtime stream ended without a result')
    assert code == 'RuntimeStreamInterrupted' and '重连' in message
