import pytest

from sandbox_harness.services.media_worker.media_errors import (classify_provider_error, media_error_guidance)


@pytest.mark.parametrize(("status", "provider_code", "expected"), [
    (400, "InvalidParameter", "provider_input_invalid"),
    (400, "InputVideoSensitiveContentDetected.PolicyViolation", "provider_content_policy_rejected"),
    (401, "", "provider_authentication_failed"),
    (200, 2038, "provider_permission_denied"),
    (429, "", "provider_rate_limited"),
    (200, 1039, "provider_rate_limited"),
    (503, "", "provider_service_unavailable"),
    (200, 1013, "provider_service_unavailable"),
    (200, "OtherProviderFailure", "provider_processing_failed"),
])
def test_provider_error_classification(status, provider_code, expected):
    assert classify_provider_error(status, provider_code) == expected
    assert media_error_guidance(expected)


def test_policy_detection_precedes_generic_http_auth_classification():
    assert classify_provider_error(403, "Content_Filter_PolicyViolation") == (
        "provider_content_policy_rejected")
