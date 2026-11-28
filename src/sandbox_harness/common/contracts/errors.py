"""Bounded internal failures and stable public failure projections."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
from uuid import uuid4


ERROR_PHASES = frozenset({
    "input_validation", "authorization", "capacity", "submission", "status",
    "execution", "collection", "validation", "review", "cancellation",
    "reconciliation", "cleanup", "internal",
})
SIDE_EFFECT_STATES = frozenset({"none", "possible", "accepted", "completed", "unknown"})
_SAFE_CODE = re.compile(r"[a-z][a-z0-9_]{0,79}")
_REFERENCE_CODE = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,79}")
_SAFE_ORIGIN = re.compile(r"[a-z][a-z0-9-]{0,63}")
_SAFE_CLASS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,119}")
_SAFE_MARKER = re.compile(r"[a-z][a-z0-9_.:-]{0,159}")


@dataclass(frozen=True)
class ErrorEnvelope:
    """Internal diagnostic contract that cannot contain raw external content."""

    code: str
    origin: str
    phase: str
    retryable: bool
    retry_class: str
    side_effect_state: str
    exception_type: str = ""
    internal_ref: str = ""
    safe_detail: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not _SAFE_CODE.fullmatch(self.code):
            raise ValueError("error_code_invalid")
        if not isinstance(self.origin, str) or not _SAFE_ORIGIN.fullmatch(self.origin):
            raise ValueError("error_origin_invalid")
        if self.phase not in ERROR_PHASES:
            raise ValueError("error_phase_invalid")
        if type(self.retryable) is not bool:
            raise ValueError("error_retryable_invalid")
        if (not isinstance(self.retry_class, str)
                or not _SAFE_MARKER.fullmatch(self.retry_class)):
            raise ValueError("error_retry_class_invalid")
        if self.side_effect_state not in SIDE_EFFECT_STATES:
            raise ValueError("error_side_effect_state_invalid")
        if (self.exception_type and (not isinstance(self.exception_type, str)
                or not _SAFE_CLASS.fullmatch(self.exception_type))):
            raise ValueError("error_exception_type_invalid")
        if (not isinstance(self.internal_ref, str)
                or not re.fullmatch(r"[0-9a-f]{32}", self.internal_ref)):
            raise ValueError("error_internal_ref_invalid")
        if (self.safe_detail and (not isinstance(self.safe_detail, str)
                or not _SAFE_MARKER.fullmatch(self.safe_detail))):
            raise ValueError("error_safe_detail_invalid")

    def model_dump(self) -> dict:
        return asdict(self)

    def model_dump_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class PublicError:
    code: str
    message: str
    action: str
    troubleshooting_id: str

    def model_dump(self) -> dict:
        return asdict(self)

    def model_dump_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


def error_envelope(exc: BaseException | None = None, *, code: str, origin: str,
                   phase: str, retryable: bool = False, retry_class: str = "none",
                   side_effect_state: str = "unknown", safe_detail: str = "",
                   internal_ref: str = "") -> ErrorEnvelope:
    """Create an internal failure without copying an exception message."""
    return ErrorEnvelope(code=code, origin=origin, phase=phase, retryable=retryable,
        retry_class=retry_class, side_effect_state=side_effect_state,
        exception_type=type(exc).__name__ if exc is not None else "",
        internal_ref=internal_ref or uuid4().hex, safe_detail=safe_detail)


_PUBLIC_ERRORS = {
    "provider_service_unavailable": (
        "provider_service_unavailable",
        "外部服务暂时不可用，系统将继续处理原任务。",
        "无需重复提交；稍后刷新任务状态。",
    ),
    "provider_rate_limited": (
        "provider_rate_limited", "外部服务当前请求较多。", "无需重复提交；系统会按原任务退避处理。"),
    "provider_submission_outcome_unknown": (
        "provider_submission_pending", "外部服务是否已接受任务尚未确认。", "请等待系统对账，切勿重复提交。"),
    "infrastructure_capacity_wait": (
        "infrastructure_capacity_wait", "执行资源暂时不足。", "无需重复提交；系统会在资源恢复后继续。"),
    "model_token_budget_exhausted": (
        "model_token_budget_exhausted", "本轮模型预算已达到上限。", "请减少长工具输出或图片预览后继续。"),
    "model_request_too_large": (
        "model_request_too_large", "本轮发送给模型的内容超过大小上限。", "请减少图片预览或长工具输出后继续。"),
}


def public_error(value: ErrorEnvelope) -> PublicError:
    code, message, action = _PUBLIC_ERRORS.get(value.code, (
        "execution_failed", "本次执行未能完成。", "请保留查询编号并稍后重试；若问题持续，请联系管理员。"))
    return PublicError(code=code, message=message, action=action,
        troubleshooting_id=value.internal_ref)


def error_reference(origin: str, identity: str, code: str) -> str:
    """Stable troubleshooting identity for one persisted failure."""
    if not _SAFE_ORIGIN.fullmatch(origin) or not identity or not _REFERENCE_CODE.fullmatch(code):
        raise ValueError("error_reference_invalid")
    return hashlib.sha256((origin + "\0" + identity + "\0" + code).encode()).hexdigest()[:32]


def public_exception_code(exc: BaseException, fallback: str) -> str:
    """Expose only an intentional stable code, never arbitrary exception text."""
    if not _SAFE_CODE.fullmatch(fallback):
        raise ValueError("public_error_fallback_invalid")
    value = str(exc)
    return value if _SAFE_CODE.fullmatch(value) else fallback


def public_failure(code: str, detail: str) -> tuple[str, str]:
    text = detail.lower()
    if code == 'infrastructure_capacity_wait':
        return code, '沙箱容量暂时不足，本轮未能启动执行环境。请稍后继续；这不是模型限流。'
    if code == 'model_token_budget_exhausted':
        return code, '本轮模型上下文与输出预算已达到上限。请精简长工具输出或图片预览后继续；这是本地预算限制，并非上游 429。'
    if code == 'model_request_too_large':
        return code, '本轮发送给模型的内容超过大小上限。请减少图片预览或长工具输出后继续；已保存的成果仍可在工作区查看。'
    if code == 'sandbox_workspace_restore_unavailable':
        return code, '恢复工作区时与沙箱的连接暂时中断，工作区快照仍安全保存，本轮尚未开始执行。'
    if code == 'checkpoint_restore_failed':
        return code, '沙箱可达，但已保存的工作区快照校验或解包失败，本轮尚未开始执行。'
    if re.search(r'\b503\b', text) or 'servers are currently overloaded' in text:
        return 'model_service_overloaded', '模型服务繁忙（503），本轮未能完成。已保存的阶段文件可在工作区查看，稍后可以继续。'
    if re.search(r'\b429\b', text):
        return 'model_rate_limited', '模型服务限流（429），请稍后继续。'
    messages = {
        'RuntimeStreamInterrupted': '与沙箱的连接中断，自动重连未能取回执行结果。本轮已停止；部分操作可能已执行，请核对工作区后再继续。',
        'HTTPStatusError': '沙箱服务返回异常，本轮未能完成。请稍后继续。',
        'CheckpointRestoreError': '会话工作区恢复失败，请稍后重试。',
        'ConnectError': '暂时无法连接沙箱，请稍后重试。',
        'harness_result_missing': '执行进程已退出，但没有确认完成。本轮未计为成功，请检查已保存的文件后继续。',
        'native_session_unavailable': '保存的 Codex 原生会话状态不可用，无法在原会话中继续。已保存的文件仍可查看；请新建会话继续，系统不会自动重放旧消息。',
        'harness_protocol_error': '执行进程返回了无法处理的消息，本轮已停止，请检查已保存的文件后重试。',
        'input_delivery_failed': '消息未能确认送达执行进程，本轮已停止，请重新发送。',
        'harness_exit': '执行进程异常退出，请检查已保存的文件后继续。',
        'model_response_timeout': '模型服务在规定时间内没有回复，请稍后重试或切换模型。',
        'harness_idle_timeout': '执行长时间没有新进展，本轮已停止。',
        'cleanup_failed': '运行结果保存或环境清理失败，请稍后重试。',
        'checkpoint_too_large': '工作区快照超过保存上限，本轮无法安全收尾。请减少工作区内容后重试。',
        'artifact_too_large': '本轮成果超过保存上限，系统已停止接收且不会发布不完整成果。',
        'artifact_count_limit_exceeded': '本轮输出文件数量超过 50000 个，系统未发布不完整清单。请合并或压缩文件后重试。',
        'runtime_artifact_identity_mismatch': '沙箱输出在清单生成后发生变化，系统拒绝发布不一致的成果。',
    }
    if code == 'RuntimeError' and 'stream ended without a result' in text:
        code = 'RuntimeStreamInterrupted'
    return code, messages.get(code, '本轮执行遇到异常，请检查已保存的文件后再继续。')
