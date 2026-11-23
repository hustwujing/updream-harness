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
    FROZEN_HARNESS_DESCRIPTORS,JobGroupCreate, JobGroupDraftCreate, JobGroupDraftView, JobGroupView,
    JobKind, JobRequest, JobView,TERMINAL_JOB_STATES,
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
                 draft_ttl_seconds: int = 86400,readiness=None,provider_preflight=None):
        self.store = store
        self.catalog = catalog
        self.profile_resolver = profile_resolver
        self.draft_ttl_seconds = draft_ttl_seconds
        self.readiness=readiness;self.provider_preflight=provider_preflight

    async def _preflight(self,value:JobGroupCreate)->None:
        for request in value.jobs:
            if request.kind==JobKind.HARNESS:continue
            if self.readiness:await self.readiness.require_available(request.descriptor_id)
            if request.kind==JobKind.PROVIDER and self.provider_preflight:
                await self.provider_preflight.validate(request)

    async def _inherit_delegated_runtime(self,owner_id:str,value:JobGroupCreate)->JobGroupCreate:
        delegated=[request for request in value.jobs if request.adapter=='child-harness'
            and request.descriptor_id=='delegate.harness']
        if not delegated:return value
        origin=await self.store.get_turn_for_owner(value.origin_turn_id,owner_id)
        if not origin or origin.thread_id!=value.thread_id:
            raise ValueError('Harness delegation origin Turn is unavailable')
        jobs=[]
        for request in value.jobs:
            if request not in delegated:
                jobs.append(request);continue
            parameters={**request.parameters,'harness':origin.harness.value,'model':origin.model,
                'reasoning_effort':origin.reasoning_effort}
            jobs.append(request.model_copy(update={'parameters':parameters}))
        return value.model_copy(update={'jobs':tuple(jobs)})

    @staticmethod
    def scope_child_group(value:JobGroupCreate,claims)->JobGroupCreate:
        if not getattr(claims,'job_id',''):return value
        allowed_operations=set(getattr(claims,'operation_ids',()))
        allowed_capabilities=set(getattr(claims,'capability_ids',()))
        for request in value.jobs:
            if request.kind==JobKind.HARNESS:
                raise ValueError('child_harness_recursion_forbidden')
            allowed=(allowed_operations if request.kind==JobKind.OPERATION else allowed_capabilities)
            if request.descriptor_id not in allowed:
                raise ValueError('child_descriptor_outside_scope')
        snapshot={**value.authorization_snapshot,'parent_job_id':claims.job_id,
            'parent_generation':claims.generation,'delegation_depth':1}
        return JobGroupCreate.model_validate({**value.model_dump(),
            'thread_id':claims.thread_id,'origin_turn_id':claims.turn_id,
            'authorization_snapshot':snapshot})

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
                if request.kind==JobKind.OPERATION:
                    for output in request.expected_outputs:
                        allowed=set(descriptor.output_validators.get(output.artifact_role,()))
                        if not set(output.validators)<=allowed:
                            raise ValueError('Job output validator is outside Descriptor for '+output.artifact_role)
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
                        'image_digest':runtime.release(self.profile_resolver.provider,
                            descriptor.runtime_harness).artifact_digest,
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
            if (request.adapter,request.descriptor_id,request.descriptor_version,
                    request.descriptor_digest) not in FROZEN_HARNESS_DESCRIPTORS:
                raise ValueError("Harness Job must use the frozen delegation Descriptor")
            harness=str(request.parameters.get("harness", ""))
            resources=request.resource_requirements
            child_profile=self.profile_resolver.children.get(request.child_profile_id)
            required_operations=set(resources.get('required_operation_ids',()))
            if not child_profile or not required_operations<=set(child_profile.allowed_operation_ids):
                raise ValueError('Operation is not authorized by ChildExecutionProfile')
            resolved=self.profile_resolver.resolve(harness,request.child_profile_id,request.skill_ids,
                ProfileRequirements(commands=tuple(resources.get("commands",())),
                    python_modules=tuple(resources.get("python_modules",())),
                    node_modules=tuple(resources.get("node_modules",())),
                    required_capability_ids=tuple(resources.get("required_capability_ids",())),
                    network_origins=request.network_origins,
                    requires_gpu=bool(resources.get("requires_gpu",False))),allow_fallback=True)
            sanitized={key:int(resources[key]) for key in ("cpu","memory_mb","disk_mb") if key in resources}
            sanitized.update({"harness":harness,
                "image_digest":resolved.provider_release.artifact_digest,
                "delegation_depth":1,"required_bundle_digests":list(resources.get("required_bundle_digests",())),
                "required_capability_ids":list(dict.fromkeys((*resolved.required_capability_ids,
                    *resources.get('required_capability_ids',())))),
                "required_operation_ids":list(dict.fromkeys(resources.get('required_operation_ids',())))})
            parameters={**request.parameters,"skill_digests":resolved.skill_digests}
            retry_policy={**request.retry_policy,'max_collection_attempts':max(2,
                int(request.retry_policy.get('max_collection_attempts',0)))}
            jobs.append(JobRequest.model_validate({**request.model_dump(),"parameters":parameters,
                "runtime_profile_id":resolved.runtime_profile.id,"resource_requirements":sanitized,
                "network_origins":list(resolved.network_origins),'retry_policy':retry_policy}))
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

    @staticmethod
    def _authorization_scope(request:JobRequest)->dict:
        return {
            'kind':request.kind.value,'adapter':request.adapter,
            'descriptor_id':request.descriptor_id,'descriptor_version':request.descriptor_version,
            'descriptor_digest':request.descriptor_digest,'parameters':request.parameters,
            'input_artifacts':sorted((value.model_dump(mode='json') for value in request.input_artifacts),
                key=lambda value:(value['artifact_role'],value['artifact_id'])),
            'expected_outputs':sorted((value.model_dump(mode='json') for value in request.expected_outputs),
                key=lambda value:value['artifact_role']),
            'child_profile_id':request.child_profile_id,'runtime_profile_id':request.runtime_profile_id,
            'skill_ids':sorted(request.skill_ids),'resource_requirements':request.resource_requirements,
            'network_origins':sorted(request.network_origins),
            'budget_requirements':request.budget_requirements,'timeout_seconds':request.timeout_seconds,
            'retry_policy':request.retry_policy,'cancellation_policy':request.cancellation_policy,
            'reconciliation_policy':request.reconciliation_policy,
        }

    @classmethod
    def _scope_contains(cls,parent:dict,child:dict)->bool:
        exact=('kind','adapter','descriptor_id','descriptor_version','descriptor_digest','parameters',
            'child_profile_id','runtime_profile_id','retry_policy','cancellation_policy',
            'reconciliation_policy')
        if any(parent.get(key)!=child.get(key) for key in exact):return False
        if not set(child.get('skill_ids',()))<=set(parent.get('skill_ids',())):return False
        if not set(child.get('network_origins',()))<=set(parent.get('network_origins',())):return False
        parent_inputs={(row['artifact_id'],row['artifact_role']) for row in parent.get('input_artifacts',())}
        child_inputs={(row['artifact_id'],row['artifact_role']) for row in child.get('input_artifacts',())}
        if not child_inputs<=parent_inputs:return False
        parent_resources=parent.get('resource_requirements',{});child_resources=child.get('resource_requirements',{})
        for key,value in child_resources.items():
            previous=parent_resources.get(key)
            if isinstance(value,(int,float)) and isinstance(previous,(int,float)):
                if value>previous:return False
            elif value!=previous:return False
        if int(child.get('timeout_seconds',0))>int(parent.get('timeout_seconds',0)):return False
        for key,value in child.get('budget_requirements',{}).items():
            if value>parent.get('budget_requirements',{}).get(key,0):return False
        parent_outputs={row['artifact_role']:row for row in parent.get('expected_outputs',())}
        visibility={'candidate':0,'workspace':1,'delivery':2}
        for output in child.get('expected_outputs',()):
            previous=parent_outputs.get(output['artifact_role'])
            if not previous:return False
            if visibility[output['output_disposition']]>visibility[previous['output_disposition']]:return False
            for key in ('max_items','max_item_bytes','max_total_bytes'):
                if int(output[key])>int(previous[key]):return False
            allowed=set(previous.get('mime_patterns',()))
            if allowed and not set(output.get('mime_patterns',()))<=allowed:return False
            if not set(previous.get('validators',()))<=set(output.get('validators',())):return False
            if previous.get('requires_review') and not output.get('requires_review'):return False
        return True

    @classmethod
    def _with_authorization_scopes(cls,value:JobGroupCreate,**extra)->JobGroupCreate:
        snapshot={**value.authorization_snapshot,**extra,'scope_version':1,
            'job_scopes':{request.local_job_id:cls._authorization_scope(request)
                for request in value.jobs}}
        return value.model_copy(update={'authorization_snapshot':snapshot})

    async def _inherited_sensitive(self,owner_id:str,value:JobGroupCreate,
                                   sensitive:tuple[str,...])->set[str]:
        inherited=set()
        for request in value.jobs:
            if request.local_job_id not in sensitive or not request.replaces_job_id:continue
            context=await self.store.get_job_authorization_context(
                request.replaces_job_id,owner_id,value.thread_id)
            if not context or context['job'].state not in TERMINAL_JOB_STATES:continue
            parent_scope=context['snapshot'].get('job_scopes',{}).get(
                context['job'].local_job_id)
            if (context['snapshot'].get('scope_version')==1 and isinstance(parent_scope,dict)
                    and self._scope_contains(parent_scope,self._authorization_scope(request))):
                inherited.add(request.local_job_id)
        return inherited

    async def prepare_group(self, owner_id: str, value: JobGroupCreate) -> PreparedGroup:
        value=await self._inherit_delegated_runtime(owner_id,value)
        value=self._normalize(value)
        await self.store.validate_job_input_artifacts(owner_id,value.thread_id,value.jobs)
        await self._preflight(value)
        value.validate_member_limit()
        sensitive = self._validate(value.jobs)
        inherited=await self._inherited_sensitive(owner_id,value,sensitive)
        value=self._with_authorization_scopes(value,
            inherited_replacements=sorted(inherited))
        if not sensitive or set(sensitive)<=inherited:
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
        normalized_group=await self._inherit_delegated_runtime(owner_id,JobGroupCreate.model_validate({
            "thread_id":current.thread_id,"origin_turn_id":current.origin_turn_id,
            "idempotency_key":current.idempotency_key,"jobs":[job.model_dump() for job in value.jobs],
            "authorization_snapshot":(value.authorization_snapshot
                if value.authorization_snapshot is not None else current.authorization_snapshot),
            "suppress_continuation":current.suppress_continuation}))
        normalized_group=self._normalize(normalized_group)
        normalized_group=self._with_authorization_scopes(normalized_group)
        sensitive = self._validate(normalized_group.jobs)
        await self.store.validate_job_input_artifacts(owner_id,current.thread_id,normalized_group.jobs)
        await self._preflight(normalized_group)
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
        current=await self._inherit_delegated_runtime(owner_id,JobGroupCreate.model_validate(
            current.model_dump(exclude={"expires_at","requires_confirmation","authorization_gaps"})))
        current=self._normalize(current)
        self._validate(current.jobs)
        await self.store.validate_job_input_artifacts(owner_id,current.thread_id,current.jobs)
        await self._preflight(current)
        group, jobs = await self.store.confirm_job_group_draft(draft_id, owner_id)
        return ConfirmedGroup(group=group, jobs=jobs)

    async def reject_group(self, draft_id: str, owner_id: str) -> JobGroupDraftView:
        return await self.store.reject_job_group_draft(draft_id, owner_id)

    async def cancel_job(self, job_id: str, owner_id: str) -> JobView:
        return await self.store.request_job_cancel(job_id, owner_id)

    async def cancel_group(self, group_id: str, owner_id: str) -> list[JobView]:
        return await self.store.request_job_group_cancel(group_id, owner_id)
