"""Canonical, payload-free field contract for operational telemetry."""
from __future__ import annotations

from enum import Enum
import re
from typing import Mapping


BUSINESS_FIELDS = frozenset({
    "address_family", "allocation_id", "ark", "artifact_id", "attempt", "broker_id",
    "bundle_version", "bytes", "cache_read_input_tokens", "cache_write_input_tokens",
    "cancelled", "capture_mode", "capture_session", "checkpointed", "count",
    "discovered_count", "disk_errors", "dropped_count", "duration_ms", "elapsed_seconds",
    "error_code", "execution_id", "exit_code", "expired", "export_failures", "external_execution_id",
    "failure_origin", "failure_stage", "failures", "file_count", "file_id", "files", "final_state",
    "first_output_ms", "generation", "gap_count", "group_count", "group_id", "harness",
    "header_ms", "healthy_count", "ingress", "input_id", "input_tokens", "interaction_id",
    "image_digest", "item_id", "job_count", "job_id", "keep_attached", "kind",
    "last_export_http_status", "last_sequence",
    "maximum_attempts", "missing_field_count", "missing_fields", "model", "model_request_id",
    "namespace", "node_id", "object_digest",
    "operation", "ordinary_input_tokens", "origin", "outcome", "output_tokens", "owner_generation", "partial", "peer_service",
    "phase", "previous_state", "project_id", "provider", "provider_job_id", "provider_status",
    "queue_ms", "reason", "reasoning_effort", "reference_sha256", "request_bytes", "request_id", "response_bytes",
    "retained_from", "retry_attempt", "retry_count", "retry_seconds", "reused",
    "returncode", "runtime_bundle_digest", "runtime_sequence", "sandbox_id", "schema_event_name", "skill_count", "source",
    "source_event_id", "source_instance", "source_service", "source_span_id", "source_trace_id",
    "state", "state_after", "state_before", "thread_generation", "thread_id", "tool_id", "tool_name", "turn_id",
    "unknown_field_count", "unknown_fields", "upstream_request_id", "usage_source",
    "violation_kind", "worker_id",
})

STANDARD_FIELDS = frozenset({
    "code.file.path", "code.function.name", "code.line.number", "error.type",
    "exception.stacktrace", "exception.type", "http.request.method", "http.request.resend_count",
    "http.response.status_code", "http.route", "server.address",
})

ALIASES = {
    "exception_type": "exception.type",
    "http_status_code": "http.response.status_code",
    "sha256": "object_digest",
    "status": "state",
}

EVENT_REQUIRED_FIELDS = {
    "http.client.request.started": frozenset({"peer_service","http.request.method","http.route","server.address"}),
    "http.client.request.completed": frozenset({"peer_service","http.request.method","http.route",
        "server.address","http.response.status_code","duration_ms","request_bytes","response_bytes"}),
    "http.client.request.failed": frozenset({"peer_service","http.request.method","http.route",
        "server.address","duration_ms","request_bytes","response_bytes"}),
    "job.attempt.started": frozenset({"job_id","worker_id","generation","state_before"}),
    "job.submission.started": frozenset({"job_id","worker_id","generation","state_after"}),
    "job.submission.capacity_wait": frozenset({"job_id","worker_id","generation","state_after","error_code"}),
    "job.submission.resumed": frozenset({"job_id","worker_id","generation","state_before","state_after"}),
    "job.submission.accepted": frozenset({"job_id","worker_id","generation","external_execution_id","state_after"}),
    "job.state.changed": frozenset({"job_id","generation","state_before","state_after","outcome"}),
    "sandbox.allocation.create.started": frozenset({"allocation_id","job_id","generation","provider","state_after"}),
    "sandbox.allocation.create.completed": frozenset({"allocation_id","job_id","generation","provider",
        "sandbox_id","state_after","outcome","duration_ms"}),
    "sandbox.allocation.create.rejected": frozenset({"allocation_id","job_id","generation","provider",
        "state_after","outcome","error_code","duration_ms"}),
    "sandbox.allocation.create.unknown": frozenset({"allocation_id","job_id","generation","provider",
        "state_after","outcome"}),
    "sandbox.allocation.reconcile.started": frozenset({"allocation_id","job_id","generation","provider",
        "retry_count","state_before"}),
    "sandbox.allocation.reconcile.completed": frozenset({"allocation_id","job_id","generation","provider",
        "retry_count","state_after","outcome"}),
    "turn.state.changed": frozenset({"turn_id","thread_id","generation","state_after"}),
}

SECRET = re.compile(
    r"(?i)(?:bearer\s+\S+|sk-[a-z0-9_-]{8,}|"
    r"(?:password|secret|token|api[_-]?key)\s*[=:]\s*\S+)"
)
SENSITIVE_FIELD_NAME = re.compile(
    r"(?i)(?:authorization|cookie|password|passwd|secret|token|api[_-]?key|prompt|content|command|body)"
)
IDENTIFIER = re.compile(r"[A-Za-z0-9_.:@/-]{1,256}")


def canonical_name(key: str) -> tuple[str, str] | None:
    """Return persisted key and canonical unprefixed name, or None if unknown."""
    unprefixed = key.removeprefix("updream.")
    canonical = ALIASES.get(unprefixed, unprefixed)
    if canonical in BUSINESS_FIELDS:
        return "updream." + canonical, canonical
    if canonical in STANDARD_FIELDS:
        return canonical, canonical
    return None


def unknown_fields(fields: Mapping[str, object]) -> tuple[str, ...]:
    return tuple(sorted(key for key in fields if canonical_name(key) is None))


def reportable_unknown_fields(fields: tuple[str, ...]) -> str:
    return ",".join("[REDACTED_FIELD]" if SENSITIVE_FIELD_NAME.search(key) else key for key in fields)


def missing_required_fields(event_name: str, fields: Mapping[str, object]) -> tuple[str, ...]:
    present={names[1] for key in fields if (names:=canonical_name(key)) is not None
        and fields[key] is not None}
    return tuple(sorted(EVENT_REQUIRED_FIELDS.get(event_name,frozenset())-present))


def sanitize_fields(fields: Mapping[str, object], *, strict: bool = False) -> dict[str, object]:
    unknown = unknown_fields(fields)
    if strict and unknown:
        raise ValueError("unknown_telemetry_fields:" + ",".join(unknown))
    result: dict[str, object] = {}
    for key, value in fields.items():
        names = canonical_name(key)
        if names is None or value is None:
            continue
        persisted, canonical = names
        if isinstance(value, Enum):
            value = value.value
        if not isinstance(value, (str, int, float, bool)):
            continue
        if isinstance(value, str):
            value = SECRET.sub("[REDACTED]", value)
            if canonical.endswith("_id") and not IDENTIFIER.fullmatch(value):
                value = "invalid-id"
            limit = 2048 if persisted == "exception.stacktrace" else 512 if canonical == "unknown_fields" else 256
            value = value.replace("\r", " ").replace("\n", " ")[:limit]
        result[persisted] = value
    return result
