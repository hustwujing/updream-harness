"""Versioned payload and receipt contracts for durable cross-domain commands."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class LifecycleCommand(_StrictModel):
    operation_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,64}$")
    owner_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,64}$")
    thread_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{0,32}$")
    generation: int = Field(ge=0)
    action: Literal["archive", "unarchive", "delete", "register"]
    target_state: Literal["archived", "open", "deleted"]
    owner_generation: int = Field(ge=0)

    @model_validator(mode="after")
    def valid_transition(self):
        states = {"archive": "archived", "unarchive": "open", "delete": "deleted", "register": "open"}
        if states[self.action] != self.target_state or (self.action == "register") != (self.thread_id == ""):
            raise ValueError("lifecycle_transition_invalid")
        return self


class LifecycleResultCommand(_StrictModel):
    operation_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,64}$")
    state: Literal["pending", "completed", "refused"]
    error_code: str = Field(pattern=r"^$|^[a-z][a-z0-9_]{0,79}$")


class LifecyclePrepareResult(_StrictModel):
    prepared: bool
    error_code: str = Field(pattern=r"^$|^[a-z][a-z0-9_]{0,79}$")


class LifecycleDecisionResult(_StrictModel):
    state: Literal["committed", "aborted"]


class LifecycleRequestResult(LifecycleResultCommand):
    pass


class RecordedResult(_StrictModel):
    recorded: Literal[True]


class MediaSourceAuthorize(_StrictModel):
    dispatch_id: str = Field(min_length=1, max_length=64)
    media_job_id: str = Field(min_length=1, max_length=32)
    source_job_id: str = Field(min_length=1, max_length=32)
    source_generation: int = Field(ge=0)
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_id: str = Field(min_length=1, max_length=64)
    thread_id: str = Field(min_length=1, max_length=32)
    kind: Literal["image", "video", "audio", "model3d"]
    model: str = Field(min_length=1, max_length=160)
    generation_mode: str = Field(min_length=1, max_length=80)
    provider: str = Field(min_length=1, max_length=80)
    adapter: str = Field(min_length=1, max_length=80)
    execution_mode: Literal["sync", "async"]
    cancellation_mode: Literal["supported", "unsupported"]
    parameters: dict[str, Any]
    references: list[dict[str, Any]] = Field(max_length=100)
    output_contracts: list[dict[str, Any]] = Field(min_length=1, max_length=100)


class MediaSourceAuthorizeResult(_StrictModel):
    media_job_id: str = Field(min_length=1, max_length=32)
    provider: str = Field(min_length=1, max_length=80)
    adapter: str = Field(min_length=1, max_length=80)
    execution_mode: Literal["sync", "async"]


class MediaSourceCancel(_StrictModel):
    dispatch_id: str = Field(min_length=1, max_length=64)
    source_job_id: str = Field(min_length=1, max_length=32)
    source_generation: int = Field(ge=0)


class CancelResult(_StrictModel):
    cancelled: Literal[True]


class MediaTurnScope(_StrictModel):
    owner_id: str = Field(min_length=1, max_length=64)
    thread_id: str = Field(min_length=1, max_length=32)
    turn_id: str = Field(min_length=1, max_length=32)
    owner_generation: int = Field(ge=0)
    thread_generation: int = Field(ge=0)


class MediaTurnDraft(MediaTurnScope):
    draft_id: str = Field(min_length=1, max_length=32)
    kind: Literal["image", "video", "audio", "model3d"]
    model: str = Field(min_length=1, max_length=160)
    generation_mode: str = Field(min_length=1, max_length=80)
    parameters: dict[str, Any]
    references: list[dict[str, Any]] = Field(max_length=15)
    idempotency_key: str = Field(min_length=8, max_length=160)
    output_role: Literal["candidate", "workspace", "delivery"]
    expires_at: str = Field(min_length=1, max_length=64)


class MediaTurnFinished(MediaTurnScope):
    origin_finished: Literal[True]


class DraftResult(_StrictModel):
    draft_id: str = Field(min_length=1, max_length=32)


class MediaBatchResult(_StrictModel):
    turn_id: str = Field(min_length=1, max_length=32)
    status: Literal["open", "sealed", "delivering", "continued", "empty"]
    draft_count: int | None = Field(default=None, ge=0)


class PromotionOutput(_StrictModel):
    id: str = Field(min_length=1, max_length=32)
    role: str = Field(min_length=1, max_length=80)
    filename: str = Field(min_length=1, max_length=255)
    object_ref: str = Field(min_length=1, max_length=1000)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    mime: str = Field(min_length=1, max_length=200)
    project_file_id: str = Field(min_length=1, max_length=32)


class MediaPromotionAck(_StrictModel):
    job_id: str = Field(min_length=1, max_length=32)
    thread_id: str = Field(min_length=1, max_length=32)
    output_role: Literal["workspace", "delivery"]
    project_path: str = Field(min_length=1, max_length=1000)
    outputs: list[PromotionOutput] = Field(min_length=1, max_length=100)


class PromotionResult(_StrictModel):
    job_id: str = Field(min_length=1, max_length=32)
    output_role: Literal["workspace", "delivery"]
    project_path: str = Field(min_length=1, max_length=1000)


class SiteFile(_StrictModel):
    path: str = Field(min_length=1, max_length=1000)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    mime: str = Field(min_length=1, max_length=200)
    object_ref: str = Field(min_length=1, max_length=1000)


class SiteTurnDraft(_StrictModel):
    name: str = Field(min_length=1, max_length=160)
    idempotency_key: str = Field(min_length=8, max_length=160)
    archive_object_ref: str = Field(min_length=1, max_length=1000)
    bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    archive_size: int = Field(ge=1)
    file_count: int = Field(ge=1)
    total_size: int = Field(ge=1)
    public_base_path: str = Field(min_length=1, max_length=1000)
    files: list[SiteFile] = Field(min_length=1, max_length=50000)
    owner_id: str = Field(min_length=1, max_length=64)
    thread_id: str = Field(min_length=1, max_length=32)
    turn_id: str = Field(max_length=32)
    owner_generation: int = Field(ge=0)
    thread_generation: int = Field(ge=0)
    draft_id: str = Field(min_length=1, max_length=32)
    project_id: str = Field(min_length=1, max_length=32)
    requested_slug: str = Field(min_length=1, max_length=80)
    expires_at: str = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def valid_files(self):
        if (len(self.files) != self.file_count
                or sum(value.size for value in self.files) != self.total_size
                or len({value.path for value in self.files}) != self.file_count
                or not any(value.path == "index.html" for value in self.files)):
            raise ValueError("site_files_invalid")
        return self


@dataclass(frozen=True)
class DomainCommandContract:
    kind: str
    source: str
    targets: tuple[str, ...]
    payload_model: type[BaseModel]
    result_model: type[BaseModel]
    version: int = 1

    @property
    def digest(self) -> str:
        value = {
            "kind": self.kind,
            "source": self.source,
            "targets": self.targets,
            "version": self.version,
            "payload": self.payload_model.model_json_schema(),
            "result": self.result_model.model_json_schema(),
        }
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode()).hexdigest()

    def validate_payload(self, source: str, target: str, payload: dict) -> None:
        if source != self.source or target not in self.targets:
            raise ValueError("domain_command_route_invalid")
        try:
            self.payload_model.model_validate(payload)
        except (ValidationError, TypeError, ValueError) as exc:
            raise ValueError("domain_command_payload_invalid") from exc

    def validate_result(self, result: dict) -> None:
        try:
            self.result_model.model_validate(result)
        except (ValidationError, TypeError, ValueError) as exc:
            raise ValueError("domain_command_result_invalid") from exc


def _contract(kind, source, targets, payload, result):
    return DomainCommandContract(kind, source, tuple(targets), payload, result)


DOMAIN_COMMAND_CONTRACTS = {
    value.kind: value for value in (
        _contract("lifecycle.prepare", "core", ("accounts", "media", "sites"), LifecycleCommand, LifecyclePrepareResult),
        _contract("lifecycle.commit", "core", ("accounts", "media", "sites"), LifecycleCommand, LifecycleDecisionResult),
        _contract("lifecycle.abort", "core", ("accounts", "media", "sites"), LifecycleCommand, LifecycleDecisionResult),
        _contract("lifecycle.request", "accounts", ("core",), LifecycleCommand, LifecycleRequestResult),
        _contract("lifecycle.result", "core", ("accounts",), LifecycleResultCommand, RecordedResult),
        _contract("media.source.authorize", "core", ("media",), MediaSourceAuthorize, MediaSourceAuthorizeResult),
        _contract("media.source.cancel", "core", ("media",), MediaSourceCancel, CancelResult),
        _contract("media.turn.draft", "core", ("media",), MediaTurnDraft, DraftResult),
        _contract("media.turn.seal", "core", ("media",), MediaTurnScope, MediaBatchResult),
        _contract("media.turn.finished", "core", ("media",), MediaTurnFinished, MediaBatchResult),
        _contract("media.promotion.ack", "core", ("media",), MediaPromotionAck, PromotionResult),
        _contract("site.turn.draft", "core", ("sites",), SiteTurnDraft, DraftResult),
    )
}


def validate_command_payload(kind: str, source: str, target: str, payload: dict,
                             *, contracts=None) -> DomainCommandContract:
    contract = (contracts or DOMAIN_COMMAND_CONTRACTS).get(kind)
    if contract is None:
        raise ValueError("handoff_kind_unsupported")
    contract.validate_payload(source, target, payload)
    return contract


def validate_command_result(kind: str, result: dict, *, contracts=None) -> None:
    contract = (contracts or DOMAIN_COMMAND_CONTRACTS).get(kind)
    if contract is None:
        raise ValueError("handoff_kind_unsupported")
    contract.validate_result(result)


def command_manifest() -> dict[str, dict]:
    return {kind: {"version": contract.version, "digest": contract.digest,
            "source": contract.source, "target": list(contract.targets)}
        for kind, contract in sorted(DOMAIN_COMMAND_CONTRACTS.items())}
