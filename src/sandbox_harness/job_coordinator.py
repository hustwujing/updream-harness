from __future__ import annotations

from datetime import timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from .domain import now
from .job_catalog import JobCatalog
from .profiles import ProfileRequirements, ProfileResolver
from .job_contracts import (
    JobGroupCreate, JobGroupDraftCreate, JobGroupDraftView, JobGroupView,
    JobKind, JobRequest, JobView,
)


class JobGroupDraftUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    jobs: tuple[JobRequest, ...] = Field(min_length=1, max_length=100)
    authorization_snapshot: dict | None = None


class PreparedGroup(BaseModel):
    mode: Literal["draft", "committed"]
    draft: JobGroupDraftView | None = None
    group: JobGroupView | None = None
    jobs: list[JobView] = Field(default_factory=list)


class ConfirmedGroup(BaseModel):
    group: JobGroupView
    jobs: list[JobView]


class JobCoordinator:
    def __init__(self, store, catalog: JobCatalog, *, profile_resolver: ProfileResolver | None = None,
                 draft_ttl_seconds: int = 86400):
        self.store = store
        self.catalog = catalog
        self.profile_resolver = profile_resolver
        self.draft_ttl_seconds = draft_ttl_seconds

    def _normalize(self, value: JobGroupCreate) -> JobGroupCreate:
        jobs=[]
        for request in value.jobs:
            if request.kind!=JobKind.HARNESS:
                descriptor=(self.catalog.operation(request.descriptor_id,request.descriptor_version,
                    request.descriptor_digest) if request.kind==JobKind.OPERATION else
                    self.catalog.capability(request.descriptor_id,request.descriptor_version,
                        request.descriptor_digest))
                if request.kind==JobKind.OPERATION and descriptor.execution_mode=='inline':
                    raise ValueError("inline Operation cannot be submitted as a Durable Job")
                if request.adapter!=descriptor.id:raise ValueError('Job adapter must match Descriptor identity')
                if request.reconciles_job_id and descriptor.reconciliation_policy.get('supported') is not True:
                    raise ValueError('Descriptor does not support reconciliation')
                try:Draft202012Validator(descriptor.parameter_schema).validate(request.parameters)
                except ValidationError as exc:
                    path='.'.join(str(value) for value in exc.absolute_path)
                    raise ValueError('Job parameter schema violation'+(' at '+path if path else '')) from exc
                if not {item.artifact_role for item in request.input_artifacts}<=set(descriptor.input_roles):
                    raise ValueError('Job input artifact role is outside Descriptor')
                if not {item.artifact_role for item in request.expected_outputs}<=set(descriptor.output_roles):
                    raise ValueError('Job output artifact role is outside Descriptor')
                resources=dict(descriptor.resources)
                resources['required_capability_ids']=(list(descriptor.required_capabilities)
                    if request.kind==JobKind.OPERATION else [descriptor.id])
                updates={}
                outputs=list(request.expected_outputs)
                if descriptor.review_policy in {"optional","required"}:
                    outputs=[output.model_copy(update={"requires_review":True,
                        "review_policy":descriptor.review_policy}) for output in outputs]
                if request.kind==JobKind.OPERATION and descriptor.runtime_profile_id:
                    if not self.profile_resolver:raise ValueError('Operation Runtime ProfileResolver is unavailable')
                    try:runtime=self.profile_resolver.runtimes[descriptor.runtime_profile_id]
                    except KeyError as exc:raise ValueError('Operation Runtime Profile is unavailable') from exc
                    if descriptor.runtime_harness not in runtime.supported_harnesses:
                        raise ValueError('Operation Runtime Harness is unavailable')
                    resources.update({'harness':descriptor.runtime_harness,
                        'image_digest':runtime.image_digests[descriptor.runtime_harness],
                        'delegation_depth':1})
                    updates={'runtime_profile_id':runtime.id,'child_profile_id':'operation-controlled'}
                jobs.append(JobRequest.model_validate({**request.model_dump(),
                    'network_origins':list(descriptor.network_origins),'resource_requirements':resources,
                    'expected_outputs':[output.model_dump() for output in outputs],
                    'timeout_seconds':descriptor.timeout_seconds,
                    'retry_policy':descriptor.retry_policy,'cancellation_policy':descriptor.cancellation_policy,
                    'reconciliation_policy':descriptor.reconciliation_policy,**updates}))
                continue
            if not self.profile_resolver:
                raise ValueError("Harness delegation ProfileResolver is unavailable")
            harness=str(request.parameters.get("harness", ""))
            resources=request.resource_requirements
            resolved=self.profile_resolver.resolve(harness,request.child_profile_id,request.skill_ids,
                ProfileRequirements(commands=tuple(resources.get("commands",())),
                    python_modules=tuple(resources.get("python_modules",())),
                    node_modules=tuple(resources.get("node_modules",())),
                    required_capability_ids=tuple(resources.get("required_capability_ids",())),
                    network_origins=request.network_origins,
                    requires_gpu=bool(resources.get("requires_gpu",False))))
            sanitized={key:int(resources[key]) for key in ("cpu","memory_mb","disk_mb") if key in resources}
            sanitized.update({"harness":harness,"image_digest":resolved.runtime_profile.image_digests[harness],
                "delegation_depth":1,"required_bundle_digests":list(resources.get("required_bundle_digests",()))})
            parameters={**request.parameters,"skill_digests":resolved.skill_digests}
            jobs.append(JobRequest.model_validate({**request.model_dump(),"parameters":parameters,
                "runtime_profile_id":resolved.runtime_profile.id,"resource_requirements":sanitized}))
        return JobGroupCreate.model_validate({**value.model_dump(),"jobs":[job.model_dump() for job in jobs]})

    def _validate(self, requests: tuple[JobRequest, ...]) -> tuple[str, ...]:
        sensitive: list[str] = []
        for request in requests:
            if request.kind == JobKind.OPERATION:
                descriptor = self.catalog.operation(
                    request.descriptor_id, request.descriptor_version, request.descriptor_digest)
                if descriptor.execution_mode == "inline":
                    raise ValueError("inline Operation cannot be submitted as a Durable Job")
                if descriptor.billable or descriptor.side_effects or descriptor.resources.get("gpu", 0) > 0:
                    sensitive.append(request.local_job_id)
            elif request.kind == JobKind.PROVIDER:
                descriptor = self.catalog.capability(
                    request.descriptor_id, request.descriptor_version, request.descriptor_digest)
                if descriptor.billable or descriptor.side_effects:
                    sensitive.append(request.local_job_id)
            elif request.kind == JobKind.HARNESS:
                if not request.child_profile_id or not request.runtime_profile_id:
                    raise ValueError("Harness Job requires resolved execution Profiles")
                if any(value>0 for value in request.budget_requirements.values()):
                    sensitive.append(request.local_job_id)
            else:
                raise ValueError(f"Job kind {request.kind.value} requires a registered specialized coordinator")
        return tuple(sensitive)

    async def prepare_group(self, owner_id: str, value: JobGroupCreate) -> PreparedGroup:
        value=self._normalize(value)
        await self.store.validate_job_input_artifacts(owner_id,value.thread_id,value.jobs)
        value.validate_member_limit()
        sensitive = self._validate(value.jobs)
        if not sensitive:
            group, jobs = await self.store.create_job_group(owner_id, value)
            return PreparedGroup(mode="committed", group=group, jobs=jobs)
        draft_value = JobGroupDraftCreate.model_validate({
            **value.model_dump(),
            "expires_at": now() + timedelta(seconds=self.draft_ttl_seconds),
            "requires_confirmation": True,
            "authorization_gaps": ["confirmation_required:" + identity for identity in sensitive],
        })
        draft, _ = await self.store.create_job_group_draft(owner_id, draft_value)
        return PreparedGroup(mode="draft", draft=draft)

    async def edit_group(self, draft_id: str, owner_id: str,
                         value: JobGroupDraftUpdate) -> JobGroupDraftView:
        current = await self.store.get_job_group_draft_request(draft_id, owner_id)
        if not current:
            raise KeyError("job_group_draft_not_found")
        normalized_group=self._normalize(JobGroupCreate.model_validate({
            "thread_id":current.thread_id,"origin_turn_id":current.origin_turn_id,
            "idempotency_key":current.idempotency_key,"jobs":[job.model_dump() for job in value.jobs],
            "authorization_snapshot":(value.authorization_snapshot
                if value.authorization_snapshot is not None else current.authorization_snapshot),
            "suppress_continuation":current.suppress_continuation}))
        sensitive = self._validate(normalized_group.jobs)
        await self.store.validate_job_input_artifacts(owner_id,current.thread_id,normalized_group.jobs)
        updated = JobGroupDraftCreate.model_validate({
            **current.model_dump(),
            "jobs": [job.model_dump() for job in normalized_group.jobs],
            "authorization_snapshot": normalized_group.authorization_snapshot,
            "requires_confirmation": True,
            "authorization_gaps": ["confirmation_required:" + identity for identity in sensitive],
        })
        return await self.store.update_job_group_draft(
            draft_id, owner_id, expected_digest=value.expected_digest, value=updated)

    async def confirm_group(self, draft_id: str, owner_id: str) -> ConfirmedGroup:
        current = await self.store.get_job_group_draft_request(draft_id, owner_id)
        if not current:
            raise KeyError("job_group_draft_not_found")
        current=self._normalize(JobGroupCreate.model_validate(current.model_dump(exclude={
            "expires_at","requires_confirmation","authorization_gaps"})))
        self._validate(current.jobs)
        await self.store.validate_job_input_artifacts(owner_id,current.thread_id,current.jobs)
        group, jobs = await self.store.confirm_job_group_draft(draft_id, owner_id)
        return ConfirmedGroup(group=group, jobs=jobs)

    async def reject_group(self, draft_id: str, owner_id: str) -> JobGroupDraftView:
        return await self.store.reject_job_group_draft(draft_id, owner_id)

    async def cancel_job(self, job_id: str, owner_id: str) -> JobView:
        return await self.store.request_job_cancel(job_id, owner_id)

    async def cancel_group(self, group_id: str, owner_id: str) -> list[JobView]:
        return await self.store.request_job_group_cancel(group_id, owner_id)
