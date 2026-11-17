from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


RESERVED_INFRASTRUCTURE_FIELDS = {
    "base_url", "credential", "credentials", "gpu_device", "gpu_ids", "image",
    "image_digest", "node_id", "retry_count", "sandbox_id", "worker_id",
}


def _reserved_schema_fields(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        properties = value.get("properties")
        if isinstance(properties, dict):
            found.update(RESERVED_INFRASTRUCTURE_FIELDS.intersection(properties))
        for child in value.values():
            found.update(_reserved_schema_fields(child))
    elif isinstance(value, list):
        for child in value:
            found.update(_reserved_schema_fields(child))
    return found


class OperationDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=160)
    version: str = Field(min_length=1, max_length=64)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_mode: Literal["inline", "durable"]
    timeout_seconds: int = Field(ge=1, le=86400)
    parameter_schema: dict[str, Any]
    input_roles: tuple[str, ...] = ()
    output_roles: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    resources: dict[str, int] = Field(default_factory=dict)
    side_effects: bool
    billable: bool
    creates_artifacts: bool
    retry_policy: dict[str, Any]
    cancellation_policy: dict[str, Any]
    reconciliation_policy: dict[str, Any]
    review_policy: Literal["disabled", "optional", "required"] = "disabled"
    runtime_profile_id: str = ""
    runtime_harness: Literal["codex","claude","deepseek"] = "codex"

    @model_validator(mode="after")
    def valid_execution_boundary(self):
        reserved = _reserved_schema_fields(self.parameter_schema)
        if reserved:
            raise ValueError("reserved infrastructure field: " + ",".join(sorted(reserved)))
        if self.execution_mode == "inline":
            if (self.side_effects or self.billable or self.creates_artifacts
                    or self.timeout_seconds > 30 or self.resources.get("gpu", 0) > 0
                    or self.output_roles):
                raise ValueError("inline Operation crosses a durable execution boundary")
        elif (not self.retry_policy or "max_pre_accept_attempts" not in self.retry_policy
                or not self.cancellation_policy or not self.reconciliation_policy):
            raise ValueError("durable Operation requires explicit retry, cancellation, and reconciliation policies")
        return self


class CapabilityDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=160)
    version: str = Field(min_length=1, max_length=64)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    timeout_seconds: int = Field(ge=1, le=86400)
    operations: tuple[str, ...] = Field(min_length=1)
    parameter_schema: dict[str, Any]
    input_roles: tuple[str, ...] = ()
    output_roles: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    resources: dict[str, int] = Field(default_factory=dict)
    billable: bool = False
    side_effects: bool = True
    retry_policy: dict[str, Any]
    cancellation_policy: dict[str, Any]
    reconciliation_policy: dict[str, Any]
    review_policy: Literal["disabled", "optional", "required"] = "disabled"

    @model_validator(mode="after")
    def no_infrastructure_parameters(self):
        reserved = _reserved_schema_fields(self.parameter_schema)
        if reserved:
            raise ValueError("reserved infrastructure field: " + ",".join(sorted(reserved)))
        if not self.retry_policy or not self.cancellation_policy or not self.reconciliation_policy:
            raise ValueError("Capability requires explicit execution policies")
        return self


class JobCatalogPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    operations: tuple[OperationDescriptor, ...] = ()
    capabilities: tuple[CapabilityDescriptor, ...] = ()

    @model_validator(mode="after")
    def unique_identities(self):
        for label, rows in (("Operation", self.operations), ("Capability", self.capabilities)):
            identities = [(row.id, row.version, row.digest) for row in rows]
            if len(identities) != len(set(identities)):
                raise ValueError(f"Duplicate {label} Descriptor identity")
        return self


class JobCatalog:
    def __init__(self, payload: JobCatalogPayload, digest: str):
        self.payload = payload
        self.digest = digest
        self.operations = {(row.id, row.version, row.digest): row for row in payload.operations}
        self.capabilities = {(row.id, row.version, row.digest): row for row in payload.capabilities}

    @classmethod
    def from_json(cls, raw: str, *, expected_digest: str) -> "JobCatalog":
        actual = hashlib.sha256(raw.encode()).hexdigest()
        if actual != expected_digest:
            raise ValueError("catalog digest mismatch")
        return cls(JobCatalogPayload.model_validate(json.loads(raw)), actual)

    @classmethod
    def from_file(cls, path: Path, *, expected_digest: str) -> "JobCatalog":
        return cls.from_json(path.read_text(), expected_digest=expected_digest)

    def operation(self, descriptor_id: str, version: str, digest: str) -> OperationDescriptor:
        try:
            return self.operations[(descriptor_id, version, digest)]
        except KeyError as exc:
            raise KeyError("Unknown Operation Descriptor identity") from exc

    def capability(self, descriptor_id: str, version: str, digest: str) -> CapabilityDescriptor:
        try:
            return self.capabilities[(descriptor_id, version, digest)]
        except KeyError as exc:
            raise KeyError("Unknown Capability Descriptor identity") from exc
