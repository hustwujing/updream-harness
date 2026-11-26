from __future__ import annotations

from sandbox_harness.common.contracts.job_errors import (JOB_ERROR_CONTRACTS)

MEDIA_ERROR_CODES=("provider_input_invalid","provider_content_policy_rejected",
    "provider_authentication_failed","provider_permission_denied","provider_rate_limited",
    "provider_endpoint_unavailable",
    "provider_service_unavailable","provider_submission_outcome_unknown",
    "provider_processing_failed","provider_job_expired","provider_result_unresolved",
    "provider_output_unavailable","provider_result_invalid","media_reference_unavailable",
    "media_reference_policy_rejected","voice_clone_input_invalid",
    "voice_clone_permission_denied","voice_clone_failed")
MEDIA_ERROR_GUIDANCE={code:JOB_ERROR_CONTRACTS[code].guidance for code in MEDIA_ERROR_CODES}


def media_error_guidance(error_code: str) -> str:
    return MEDIA_ERROR_GUIDANCE.get(error_code,
        "Inspect the Job status and error category before deciding whether to retry.")


