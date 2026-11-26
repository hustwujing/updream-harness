"""Public failure projections that never contain internal diagnostic text."""
from __future__ import annotations

from .errors import error_reference, public_failure
from .job_errors import normalize_job_error


_INTERNAL_ERROR_FIELDS = frozenset({
    "error_detail", "exception", "exception_type", "raw_error", "stacktrace",
    "upstream_body", "provider_response", "provider_error",
})


def public_job_result(value: dict | None, *, identity: str, origin: str) -> dict:
    source = dict(value or {})
    safe = {key: item for key, item in source.items() if key not in _INTERNAL_ERROR_FIELDS}
    code = str(source.get("error_code") or "")
    if not code:
        return safe
    normalized = normalize_job_error(source)
    safe.update({key: normalized[key] for key in (
        "error_code", "error_message", "error_guidance", "failed_phase",
        "retryable", "retry_class", "side_effect_state",
    )})
    safe["troubleshooting_id"] = error_reference(origin, identity, code)
    return safe


def public_turn_failure(*, code: str, detail: str, turn_id: str) -> dict:
    if not code:
        return {"error_code": "", "error_message": "", "error_action": "",
            "troubleshooting_id": ""}
    public_code, message = public_failure(code, "")
    return {
        "error_code": public_code,
        "error_message": message,
        "error_action": "请按提示处理；若问题持续，请提供查询编号联系管理员。",
        "troubleshooting_id": error_reference("broker", turn_id, public_code),
    }
