from dataclasses import replace

import pytest

from sandbox_harness.common.contracts.errors import (
    ErrorEnvelope,
    PublicError,
    error_envelope,
    public_error,
)


def test_internal_error_envelope_is_bounded_and_public_projection_hides_diagnostics():
    internal = ErrorEnvelope(
        code="provider_service_unavailable",
        origin="media-worker",
        phase="status",
        retryable=True,
        retry_class="bounded_backoff",
        side_effect_state="accepted",
        exception_type="ReadError",
        internal_ref="a" * 32,
        safe_detail="provider_poll_transport_failed",
    )

    public = public_error(internal)

    assert public == PublicError(
        code="provider_service_unavailable",
        message="外部服务暂时不可用，系统将继续处理原任务。",
        action="无需重复提交；稍后刷新任务状态。",
        troubleshooting_id="a" * 32,
    )
    assert "ReadError" not in public.model_dump_json()
    assert "provider_poll_transport_failed" not in public.model_dump_json()


def test_exception_factory_records_only_type_and_explicit_safe_detail():
    value = error_envelope(
        RuntimeError("credential=secret https://private.invalid/path"),
        code="worker_internal_error",
        origin="job-worker",
        phase="internal",
        safe_detail="adapter_failed",
    )

    assert value.exception_type == "RuntimeError"
    assert value.safe_detail == "adapter_failed"
    assert value.internal_ref
    assert "secret" not in value.model_dump_json()
    assert "private.invalid" not in value.model_dump_json()


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"code": "UPPER"}, "error_code_invalid"),
        ({"origin": "bad origin"}, "error_origin_invalid"),
        ({"phase": "unknown"}, "error_phase_invalid"),
        ({"retry_class": "bad class"}, "error_retry_class_invalid"),
        ({"side_effect_state": "maybe"}, "error_side_effect_state_invalid"),
        ({"exception_type": "bad.type"}, "error_exception_type_invalid"),
        ({"internal_ref": "short"}, "error_internal_ref_invalid"),
        ({"safe_detail": "https://private.invalid"}, "error_safe_detail_invalid"),
    ],
)
def test_error_envelope_rejects_unbounded_or_unsafe_fields(change, error):
    valid = ErrorEnvelope(
        code="worker_internal_error",
        origin="job-worker",
        phase="internal",
        retryable=False,
        retry_class="none",
        side_effect_state="unknown",
        exception_type="RuntimeError",
        internal_ref="b" * 32,
        safe_detail="adapter_failed",
    )

    with pytest.raises(ValueError, match=error):
        replace(valid, **change)


def test_public_error_uses_stable_fallback_without_internal_text():
    public = public_error(ErrorEnvelope(
        code="new_internal_failure",
        origin="gateway",
        phase="internal",
        retryable=False,
        retry_class="none",
        side_effect_state="possible",
        exception_type="UnexpectedFailure",
        internal_ref="c" * 32,
        safe_detail="bounded_internal_marker",
    ))

    assert public.code == "execution_failed"
    assert public.message == "本次执行未能完成。"
    assert public.action == "请保留查询编号并稍后重试；若问题持续，请联系管理员。"
    assert public.troubleshooting_id == "c" * 32
