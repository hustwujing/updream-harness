from __future__ import annotations

import time

from pydantic import BaseModel, ConfigDict, Field

from sandbox_harness.common.security.capability import (CapabilityCodec)
from sandbox_harness.common.contracts.domain import (CapabilityClaims, Harness)


class TurnGrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    thread_id: str
    turn_id: str
    sandbox_id: str
    harness: Harness
    model: str
    routes: tuple[str, ...]
    network_access: str = "restricted"
    ttl_seconds: int = Field(ge=1)
    max_requests: int = Field(ge=1)
    max_public_requests: int = Field(ge=1)
    token_budget: int = Field(ge=1)
    authorization_version: int = Field(default=0, ge=0)
    artifact_ids: tuple[str, ...] = ()
    skill_ids: tuple[str, ...] = ()
    capability_ids: tuple[str, ...] = ()
    operation_ids: tuple[str, ...] = ()
    child_profile_ids: tuple[str, ...] = ()
    runtime_profile_ids: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    output_scopes: tuple[str, ...] = ()
    budget: dict[str, int] = Field(default_factory=dict)


class JobGrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    group_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    job_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1)
    source_identity: str = Field(min_length=1)
    descriptor_id: str = Field(min_length=1)
    descriptor_version: str = Field(min_length=1)
    descriptor_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    operations: tuple[str, ...] = Field(min_length=1)
    artifact_ids: tuple[str, ...] = ()
    skill_ids: tuple[str, ...] = ()
    capability_ids: tuple[str, ...] = ()
    operation_ids: tuple[str, ...] = ()
    child_profile_id: str = ""
    runtime_profile_id: str = ""
    network_origins: tuple[str, ...] = ()
    output_scopes: tuple[str, ...] = ()
    parameter_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    budget: dict[str, int] = Field(default_factory=dict)
    routes: tuple[str, ...] = Field(min_length=1)
    # None explicitly inherits the parent's remaining lifetime.
    ttl_seconds: int | None = Field(ge=1)
    max_requests: int = Field(ge=1)
    max_public_requests: int = Field(ge=1)
    token_budget: int = Field(ge=1)


def _routes_within(child: tuple[str, ...], parent: list[str]) -> bool:
    return all(any(route == prefix or route.startswith(prefix.rstrip("/") + "/")
        for prefix in parent) for route in child)


class GrantAuthority:
    def __init__(self, signing_key: str, *, kid: str = "current",
                 previous_keys: dict[str,str] | None = None):
        self.codec = CapabilityCodec(signing_key,kid=kid,previous_keys=previous_keys)

    @classmethod
    def from_settings(cls,settings):
        return cls(settings.gateway_signing_key,kid=settings.gateway_signing_key_id,
            previous_keys=settings.gateway_previous_signing_keys)

    def issue_turn(self, value: TurnGrantRequest) -> tuple[str, CapabilityClaims]:
        return self.codec.issue(
            thread_id=value.thread_id, turn_id=value.turn_id, sandbox_id=value.sandbox_id,
            harness=value.harness, model=value.model, routes=value.routes,
            ttl_seconds=value.ttl_seconds, max_requests=value.max_requests,
            max_public_requests=value.max_public_requests, token_budget=value.token_budget,
            network_access=value.network_access, scope="turn",
            authorization_version=value.authorization_version,
            artifact_ids=value.artifact_ids, skill_ids=value.skill_ids,
            capability_ids=value.capability_ids, operation_ids=value.operation_ids,
            child_profile_ids=value.child_profile_ids, runtime_profile_ids=value.runtime_profile_ids,
            network_origins=value.network_origins, output_scopes=value.output_scopes,
            budget=value.budget,
        )

    def issue_job(self, parent: CapabilityClaims,
                  value: JobGrantRequest) -> tuple[str, CapabilityClaims]:
        if parent.scope not in {"turn", "group"}:
            raise ValueError("Job grant requires a Turn or Group parent grant")
        issued_at=int(time.time())
        remaining_ttl = parent.expires_at - issued_at
        ttl_seconds=remaining_ttl if value.ttl_seconds is None else value.ttl_seconds
        subset_checks = (
            set(value.artifact_ids) <= set(parent.artifact_ids),
            set(value.skill_ids) <= set(parent.skill_ids),
            set(value.capability_ids) <= set(parent.capability_ids),
            set(value.operation_ids) <= set(parent.operation_ids),
            (not value.child_profile_id or value.child_profile_id in parent.child_profile_ids),
            (not value.runtime_profile_id or value.runtime_profile_id in parent.runtime_profile_ids),
            value.descriptor_id in set(parent.capability_ids) | set(parent.operation_ids),
            set(value.network_origins) <= set(parent.network_origins),
            set(value.output_scopes) <= set(parent.output_scopes),
            all(value.budget.get(key, 0) <= parent.budget.get(key, 0) for key in value.budget),
            _routes_within(value.routes, parent.routes),
            value.max_requests <= parent.max_requests,
            value.max_public_requests <= parent.max_public_requests,
            value.token_budget <= parent.token_budget,
            remaining_ttl > 0 and ttl_seconds <= remaining_ttl,
            value.source_identity == parent.sandbox_id,
        )
        if not all(subset_checks):
            raise ValueError("Job grant exceeds parent grant")
        return self.codec.issue(
            thread_id=parent.thread_id, turn_id=parent.turn_id, sandbox_id=parent.sandbox_id,
            harness=parent.harness, model=parent.model, routes=value.routes,
            ttl_seconds=ttl_seconds, issued_at=issued_at, max_requests=value.max_requests,
            max_public_requests=value.max_public_requests, token_budget=value.token_budget,
            parent_nonce=parent.nonce, parent_token_budget=parent.token_budget,
            network_access=parent.network_access, scope="job",
            authorization_version=parent.authorization_version,
            group_id=value.group_id, job_id=value.job_id, generation=value.generation,
            source_identity=value.source_identity, descriptor_id=value.descriptor_id,
            descriptor_version=value.descriptor_version, descriptor_digest=value.descriptor_digest,
            operations=value.operations, artifact_ids=value.artifact_ids, skill_ids=value.skill_ids,
            capability_ids=value.capability_ids, operation_ids=value.operation_ids,
            child_profile_id=value.child_profile_id, runtime_profile_id=value.runtime_profile_id,
            network_origins=value.network_origins, output_scopes=value.output_scopes,
            parameter_digest=value.parameter_digest, budget=value.budget,
        )

    def verify_call(self, token: str, *, route: str, thread_id: str, turn_id: str,
                    sandbox_id: str, now: int | None = None,
                    revoked_nonces: set[str] | None = None, group_id: str = "", job_id: str = "",
                    generation: int = 0, source_identity: str = "", descriptor_id: str = "",
                    descriptor_version: str = "", descriptor_digest: str = "", operation: str = "",
                    artifact_ids: set[str] | None = None, skill_ids: set[str] | None = None,
                    capability_id: str = "", operation_id: str = "", network_origin: str = "",
                    output_scope: str = "", parameter_digest: str = "", child_profile_id: str = "",
                    runtime_profile_id: str = "") -> CapabilityClaims:
        claims = self.codec.verify(token, route=route, now=now)
        if revoked_nonces and claims.nonce in revoked_nonces:
            raise ValueError("Capability grant revoked")
        exact = (
            claims.thread_id == thread_id,
            claims.turn_id == turn_id,
            claims.sandbox_id == sandbox_id,
        )
        if not all(exact):
            raise ValueError("Call is outside grant scope")
        if claims.scope == "job":
            job_exact = (
                claims.group_id == group_id, claims.job_id == job_id,
                claims.generation == generation, claims.source_identity == source_identity,
                claims.descriptor_id == descriptor_id,
                claims.descriptor_version == descriptor_version,
                claims.descriptor_digest == descriptor_digest,
                claims.child_profile_id == child_profile_id,
                claims.runtime_profile_id == runtime_profile_id,
                operation in claims.operations,
                not capability_id or capability_id in claims.capability_ids,
                not operation_id or operation_id in claims.operation_ids,
                not network_origin or network_origin in claims.network_origins,
                not output_scope or output_scope in claims.output_scopes,
                claims.parameter_digest == parameter_digest,
                (artifact_ids or set()) <= set(claims.artifact_ids),
                (skill_ids or set()) <= set(claims.skill_ids),
            )
            if not all(job_exact):
                raise ValueError("Call is outside grant scope")
        return claims
