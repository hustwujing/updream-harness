from __future__ import annotations

from sandbox_harness.common.contracts.media_errors import (MEDIA_ERROR_CODES, MEDIA_ERROR_GUIDANCE, media_error_guidance)


def classify_provider_error(http_status: int, provider_code: object,
                            default: str = "provider_processing_failed") -> str:
    value = str(provider_code or "").strip()
    lowered = value.lower()
    if any(marker in lowered for marker in (
            "sensitive", "policyviolation", "policy_violation", "content_filter",
            "moderation", "safety", "risk_control")):
        return "provider_content_policy_rejected"
    if http_status in {401, 403} or value in {"1004", "Unauthorized", "AccessDenied"}:
        return "provider_authentication_failed"
    if value in {"2038", "PermissionDenied", "NoPermission"}:
        return "provider_permission_denied"
    if http_status == 429 or value in {"1002", "1039", "RateLimitExceeded", "TooManyRequests"}:
        return "provider_rate_limited"
    if value == "InvalidEndpoint.ClosedEndpoint":
        return "provider_endpoint_unavailable"
    if http_status in {400, 404, 409, 415, 422} or value in {
            "1042", "1043", "2013", "InvalidParameter", "InvalidArgument"}:
        return "provider_input_invalid"
    if http_status >= 500 or value in {"1000", "1001", "1013", "InternalError", "ServiceUnavailable"}:
        return "provider_service_unavailable"
    return default
