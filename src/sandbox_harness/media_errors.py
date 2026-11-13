from __future__ import annotations


MEDIA_ERROR_GUIDANCE = {
    "provider_input_invalid": "Correct the declared parameters or media constraints before creating a new draft.",
    "provider_content_policy_rejected": "Do not retry unchanged. Use different authorized content or a local non-generative method.",
    "provider_authentication_failed": "Provider credentials or account access require operator action; do not retry from the Agent.",
    "provider_permission_denied": "The Provider account lacks this capability; request operator action or choose another enabled capability.",
    "provider_rate_limited": "The platform will retry with backoff. Do not create a duplicate Job.",
    "provider_service_unavailable": "The Provider is temporarily unavailable. Keep the existing Job; do not duplicate the request.",
    "provider_submission_outcome_unknown": "Submission may have succeeded. Never resubmit without deterministic reconciliation.",
    "provider_processing_failed": "The Provider accepted the Job but processing failed. Change the input or method before retrying.",
    "provider_job_expired": "The Provider Job expired before producing a result. Create a new draft only after reviewing scope and limits.",
    "provider_result_unresolved": "The Provider ID is retained but no terminal result was confirmed. Use the existing Job refresh path.",
    "provider_output_unavailable": "The Provider result exists but download is temporarily unavailable. The platform will retry it.",
    "provider_result_invalid": "The returned file or manifest failed deterministic validation; do not present it as a deliverable.",
    "media_reference_unavailable": "The frozen reference could not be published or registered. Keep the same ProjectFile and inspect platform status.",
    "media_reference_policy_rejected": "Ark rejected the reference content. Do not retry the same bytes; use another authorized asset or local editing.",
    "voice_clone_input_invalid": "Use one clear 10-300 second MP3, M4A or WAV reference no larger than 20 MB.",
    "voice_clone_permission_denied": "The Provider account cannot clone voices; operator action is required.",
    "voice_clone_failed": "Voice extraction failed. Review reference quality and authorization before creating a new draft.",
}


def media_error_guidance(error_code: str) -> str:
    return MEDIA_ERROR_GUIDANCE.get(error_code,
        "Inspect the Job status and error category before deciding whether to retry.")


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
    if http_status in {400, 404, 409, 415, 422} or value in {
            "1042", "1043", "2013", "InvalidParameter", "InvalidArgument"}:
        return "provider_input_invalid"
    if http_status >= 500 or value in {"1000", "1001", "1013", "InternalError", "ServiceUnavailable"}:
        return "provider_service_unavailable"
    return default
