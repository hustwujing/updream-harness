"""Public failure projections that never contain internal diagnostic text."""
from __future__ import annotations

import json
import re

from .errors import error_reference, public_failure
from .job_errors import normalize_job_error


_INTERNAL_ERROR_FIELDS = frozenset({
    "error_detail", "exception", "exception_type", "raw_error", "stacktrace",
    "upstream_body", "provider_response", "provider_error", "error_reason",
})

_EXTERNAL_ERROR_SCHEMA="external_error.v1"
_PROVIDER_CODE=re.compile(r"[A-Za-z0-9_.-]{1,100}")
_REQUEST_ID=re.compile(r"(?i)(?:request|trace|correlation)[ _-]*id\s*[:=]\s*[A-Za-z0-9_.:-]+")
_URL=re.compile(r"(?i)https?://\S+")
_CREDENTIAL=re.compile(
    r"(?i)(?:authorization|bearer|api[ _-]*key|access[ _-]*key|cookie|password|secret)\s*[:=]\s*\S+")


def _sanitize_external_reason(value:object)->str:
    text=" ".join(str(value or "").replace("\x00","").split())
    boundaries=[match.start() for pattern in (_REQUEST_ID,_URL,_CREDENTIAL)
        if (match:=pattern.search(text))]
    if boundaries:text=text[:min(boundaries)]
    text=re.sub(r"(?i)(prompt\s*[:=]\s*)\S.*",r"\1<redacted>",text)
    text=text.rstrip(" ;,.:-")
    encoded=text.encode()[:500]
    return encoded.decode(errors="ignore").rstrip(" ;,.:-")


def external_error_detail(provider_code:object,provider_reason:object)->str:
    """Create the only diagnostic envelope eligible for public projection."""
    code=str(provider_code or "").strip()
    if not _PROVIDER_CODE.fullmatch(code):code="unreported"
    reason=_sanitize_external_reason(provider_reason)
    return json.dumps({"schema":_EXTERNAL_ERROR_SCHEMA,"provider_code":code,
        "reason":reason},ensure_ascii=False,sort_keys=True,separators=(",",":"))


def public_error_reason(detail:object)->str:
    try:value=json.loads(str(detail or ""))
    except (TypeError,ValueError):return ""
    if (not isinstance(value,dict) or value.get("schema")!=_EXTERNAL_ERROR_SCHEMA
            or not _PROVIDER_CODE.fullmatch(str(value.get("provider_code") or ""))):
        return ""
    reason=value.get("reason")
    if not isinstance(reason,str) or reason!=_sanitize_external_reason(reason):return ""
    return reason


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
    reason=public_error_reason(source.get("error_detail"))
    if reason:safe["error_reason"]=reason
    safe["troubleshooting_id"] = error_reference(origin, identity, code)
    return safe


def public_turn_failure(*, code: str, detail: str, turn_id: str) -> dict:
    if not code:
        return {"error_code": "", "error_message": "", "error_action": "",
            "troubleshooting_id": ""}
    public_code, message = public_failure(code, "")
    action = ({
        "sandbox_workspace_restore_unavailable": (
            "请稍后重新发送本轮消息；系统会从上一个已保存的工作区继续。"),
        "checkpoint_restore_failed": "请提供查询编号联系管理员，不要删除已有工作区数据。",
        "checkpoint_version_rolled_back": "系统已回退到上一验证版本，请发送“继续”重试。",
    }.get(public_code) or "请按提示处理；若问题持续，请提供查询编号联系管理员。")
    return {
        "error_code": public_code,
        "error_message": message,
        "error_action": action,
        "troubleshooting_id": error_reference("broker", turn_id, public_code),
    }
