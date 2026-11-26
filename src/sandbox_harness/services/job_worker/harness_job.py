from __future__ import annotations

import asyncio
from dataclasses import asdict
import httpx
import json
from pathlib import Path, PurePosixPath
import time
from typing import Any, Awaitable, Callable, Protocol

from sandbox_harness.common.storage.files import (staged_file_chunks)
from sandbox_harness.common.contracts.domain import (Harness, valid_attachment_name)
from sandbox_harness.common.sandbox.job_allocation import (SandboxAllocationOwner)
from sandbox_harness.common.security.grants import (GrantAuthority, JobGrantRequest, TurnGrantRequest)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, AdapterSubmission, BaseJobAdapter, JobExecutionContext, PreAcceptCancelled, PreAcceptFailure, SubmissionRecovery, SubmissionOutcomeUnknown, SubmissionInterrupted, reserve_job_resources, recover_resource_reservation)
from sandbox_harness.common.contracts.job_contracts import (JobState, TERMINAL_JOB_STATES)
from sandbox_harness.common.contracts.job_errors import (ArtifactCollectionCardinalityError)
from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
from sandbox_harness.common.scheduling.scheduler import (ResourceRequest, Scheduler)
from sandbox_harness.common.sandbox.sandbox import (Sandbox, SandboxCapacityUnavailable, SandboxNotFound, SandboxRateLimited)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown, SandboxEndpointUnavailable, SandboxHandle, SandboxProfile, SandboxProfileUnavailable, SandboxProviderUnavailable)
from sandbox_harness.common.sandbox.sandbox_factory import (SandboxProfileResolver)
from sandbox_harness.common.storage.objects import (object_chunks)
from sandbox_harness.common.catalog.profiles import (ProviderRelease)
from sandbox_harness.common.observability.http import (observed_async_client)




def verify_runtime_artifact(staged, metadata: dict) -> None:
    if metadata.get("size") != staged.size or metadata.get("sha256") != staged.sha256:
        raise ValueError("runtime_artifact_identity_mismatch")


class ChildExecutionBackend(Protocol):
    async def start(self, context: JobExecutionContext, reservation) -> tuple[str, dict[str, Any]]: ...
    async def recover_submission(self,context:JobExecutionContext,reservation)->SubmissionRecovery: ...
    async def status(self, context: JobExecutionContext, external_execution_id: str,
                     receipt: dict[str, Any]) -> AdapterStatus: ...
    async def cancel(self, context: JobExecutionContext, external_execution_id: str,
                     receipt: dict[str, Any]) -> AdapterStatus: ...
    async def cleanup(self, context: JobExecutionContext, external_execution_id: str,
                      receipt: dict[str, Any]) -> None: ...
    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus,receipt:dict[str,Any])->dict[str,Any]: ...


class JobGrantFactory:
    def __init__(self,settings):self.settings=settings;self.authority=GrantAuthority(settings.gateway_signing_key)
    async def __call__(self,context:JobExecutionContext,reservation,sandbox_id:str):
        request=context.request;harness=Harness(reservation.harness)
        capability_ids=tuple(request.resource_requirements.get('required_capability_ids',()))
        operation_ids=tuple(dict.fromkeys((request.descriptor_id,
            *request.resource_requirements.get('required_operation_ids',()))))
        output_scopes=tuple(f'{value.artifact_role}:{value.output_disposition.value}'
            for value in request.expected_outputs)
        model_routes=((f'/v1/models/{harness.value}',) if request.kind.value=='harness' else ())
        child_catalog_routes=(('/v1/execution/catalog',) if request.adapter=='child-harness' else ())
        tool_routes=(('/v1/operations','/v1/job-group-drafts','/v1/job-groups','/v1/jobs')
            if capability_ids or len(operation_ids)>1 else ())
        routes=(*model_routes,'/v1/job-artifacts','/v1/telemetry',
            *child_catalog_routes,*tool_routes,
            *(('/v1/media',) if capability_ids else ()),
            *(('/v1/public-http',) if request.network_origins else ()))
        token_budget=max(1,int(request.budget_requirements.get('model_tokens',
            self.settings.model_token_budget)))
        _,parent=self.authority.issue_turn(TurnGrantRequest(thread_id=context.job.thread_id,
            turn_id=context.job.origin_turn_id,sandbox_id=sandbox_id,harness=harness,
            model=str(request.parameters.get('model','')),routes=routes,network_access=(
                'public' if request.network_origins else 'restricted'),
            ttl_seconds=self.settings.capability_ttl_seconds,
            max_requests=self.settings.capability_request_limit,
            max_public_requests=self.settings.capability_public_request_limit,
            token_budget=token_budget,artifact_ids=tuple(item.artifact_id for item in request.input_artifacts),
            skill_ids=request.skill_ids,capability_ids=capability_ids,operation_ids=operation_ids,
            child_profile_ids=(request.child_profile_id,),runtime_profile_ids=(request.runtime_profile_id,),
            network_origins=request.network_origins,output_scopes=output_scopes,budget=request.budget_requirements))
        operations=(*(('model',) if request.kind.value=='harness' else ()),
            *(('check',) if child_catalog_routes or tool_routes else ()),
            *(('emit',) if request.expected_outputs else ()))
        return self.authority.issue_job(parent,JobGrantRequest(group_id=context.job.group_id,
            job_id=context.job.id,generation=context.generation,source_identity=sandbox_id,
            descriptor_id=request.descriptor_id,descriptor_version=request.descriptor_version,
            descriptor_digest=request.descriptor_digest,operations=operations,
            artifact_ids=tuple(item.artifact_id for item in request.input_artifacts),
            skill_ids=request.skill_ids,capability_ids=capability_ids,operation_ids=operation_ids,
            child_profile_id=request.child_profile_id,runtime_profile_id=request.runtime_profile_id,
            network_origins=request.network_origins,output_scopes=output_scopes,
            parameter_digest=request.request_digest,budget=request.budget_requirements,routes=routes,
            ttl_seconds=None,
            max_requests=self.settings.capability_request_limit,
            max_public_requests=self.settings.capability_public_request_limit,token_budget=token_budget))


class HarnessJobAdapter(BaseJobAdapter):
    def __init__(self, scheduler: Scheduler, backend: ChildExecutionBackend):
        self.scheduler=scheduler;self.backend=backend

    @staticmethod
    def _materialized_input_path(item,artifact)->str:
        filename=PurePosixPath(artifact.path).name
        if not valid_attachment_name(filename):
            raise ValueError('child_input_filename_invalid')
        return f'assets/inputs/{item.artifact_role}-{artifact.id}/{filename}'

    @staticmethod
    def execution_prompt(request,input_bindings=())->str:
        objective=str(request.parameters.get('objective',''))
        request_inputs=getattr(request,'input_artifacts',())
        if input_bindings and len(input_bindings)!=len(request_inputs):
            raise ValueError('child_input_binding_count_mismatch')
        rows=[]
        for index,value in enumerate(request_inputs):
            if input_bindings:
                item,artifact=input_bindings[index]
                if item!=value or artifact.id!=value.artifact_id:
                    raise ValueError('child_input_binding_identity_mismatch')
                filename=PurePosixPath(artifact.path).name
                rows.append(f'- artifact_id={value.artifact_id}; role={value.artifact_role}; '
                    f'filename={filename}; sha256={artifact.sha256}; '
                    f'materialized_path={HarnessJobAdapter._materialized_input_path(item,artifact)}')
            else:
                rows.append(f'- artifact_id={value.artifact_id}; role={value.artifact_role}; '
                    f'materialized_path=assets/inputs/{value.artifact_role}-{value.artifact_id}')
        inputs='\n'.join(rows)
        if inputs:
            objective+='\n\nFrozen input bindings (use these exact authoritative filenames and materialized paths):\n'+inputs
        if request.adapter=='reviewer-harness':
            context=request.parameters.get('review_context',{})
            return (objective+'\n\nYou are an independent read-only Reviewer. Inspect only the frozen input '
                'Artifacts. Do not modify them, publish, delegate, call Providers, or ask the user. Write exactly one '
                '/workspace/outputs/review-verdict.json object with keys verdict (PASS, REVISE, EVIDENCE_REQUIRED, or BLOCKED), '
                'artifact_digests, summary, findings, and missing_evidence. findings must be an array of JSON objects, never '
                'strings; use objects shaped like {"code":"specific_code","severity":"info|warning|error",'
                '"message":"specific finding","evidence":{}}. missing_evidence must be an array of strings. '
                'Frozen candidate metadata and deterministic validation evidence: '
                +json.dumps(context.get('candidates',[]),sort_keys=True)+'\n'
                'artifact_digests must exactly equal: '
                +json.dumps(context.get('artifact_digests',[]),sort_keys=True))
        contracts=[value.model_dump(mode='json') for value in request.expected_outputs]
        if not contracts:
            return (objective+'\n\nFrozen output contract: text-only. Return the complete result in your final '
                'response. Do not create or copy files under /workspace/outputs and do not publish Artifacts.')
        return (objective+'\n\nFrozen output contract: '+json.dumps(contracts,sort_keys=True)+
            '\nPut final files under /workspace/outputs. When more than one artifact role exists, also write '
            '/workspace/outputs/job-output-manifest.json as {"outputs":[{"path":"relative/file",'
            '"artifact_role":"role","item_key":"stable-key","position":0}]}. Every output file except the manifest '
            'must appear exactly once. For a single artifact role the platform maps files in lexical order automatically.')

    @staticmethod
    def _resources(context: JobExecutionContext) -> ResourceRequest:
        values=context.request.resource_requirements
        depth=int(values.get('delegation_depth',1))
        if depth>1:raise ValueError('recursive_delegation_forbidden')
        return ResourceRequest(child_profile_id=context.request.child_profile_id,
            runtime_profile_id=context.request.runtime_profile_id,
            harness=str(values.get('harness','')),image_digest=str(values.get('image_digest','')),
            cpu=int(values.get('cpu',1)),memory_mb=int(values.get('memory_mb',512)),
            disk_mb=int(values.get('disk_mb',1024)),gpu_count=int(values.get('gpu_count',0)),
            gpu_type=str(values.get('gpu_type','')),gpu_mode=str(values.get('gpu_mode','')),
            gpu_memory_mb=int(values.get('gpu_memory_mb',0)),
            required_bundle_digests=tuple(values.get('required_bundle_digests',())),
            affinity_node_id=str(values.get('affinity_node_id','')))

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        reservation=await reserve_job_resources(context,self.scheduler,self._resources(context))
        execution_id='';receipt={}
        def complete_receipt(value):
            return {'executor':'harness','reservation_id':reservation.id,
                'node_id':reservation.node_id,'node_generation':reservation.node_generation,
                'runtime_profile_id':reservation.runtime_profile_id,
                'image_digest':reservation.image_digest,'gpu_ids':list(reservation.gpu_ids),**value}
        try:
            async with self.scheduler.hold(reservation.id,reservation.node_id,reservation.node_generation):
                try:execution_id,receipt=await self.backend.start(context,reservation)
                except (SubmissionOutcomeUnknown,SubmissionInterrupted) as exc:
                    execution_id=exc.external_execution_id;receipt=exc.acceptance_receipt
                    raise
                await self.scheduler.mark_running(reservation.id,reservation.node_id,reservation.node_generation)
        except (SandboxCapacityUnavailable,SandboxRateLimited) as exc:
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise PreAcceptFailure(str(exc),infrastructure=True,retry_after_seconds=1,
                error_code=exc.code,failed_phase='submission',retry_class='capacity_wait',
                retryable=True) from exc
        except PreAcceptCancelled:
            # Terminal cleanup owns any allocation and the Scheduler reservation.
            raise
        except PreAcceptFailure as exc:
            if not exc.retain_capacity:
                await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise
        except SubmissionOutcomeUnknown as exc:
            raise SubmissionOutcomeUnknown('Child execution acceptance requires reconciliation',
                external_execution_id=exc.external_execution_id,
                acceptance_receipt=complete_receipt(exc.acceptance_receipt)) from exc
        except BaseException as exc:
            if execution_id:
                # Keep evidence across cancellation, hold exit and mark-running failures.
                if isinstance(exc,asyncio.CancelledError):raise
                if receipt.get('allocation_phase') in {
                        'awaiting_capacity','allocating','preparing','absent'}:
                    retain=receipt.get('allocation_phase') in {'allocating','preparing'}
                    if not retain:
                        await self.scheduler.release(reservation.id,reservation.node_id,
                            reservation.node_generation)
                    raise PreAcceptFailure('Allocation ownership was interrupted before execution',
                        error_code='child_allocation_failed',failed_phase='submission',
                        retryable=False,retain_capacity=retain) from exc
                raise SubmissionOutcomeUnknown('Child execution acceptance requires reconciliation',
                    external_execution_id=execution_id,
                    acceptance_receipt=complete_receipt(receipt)) from exc
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise
        return AdapterSubmission(external_execution_id=execution_id,
            acceptance_receipt=complete_receipt(receipt))

    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        if await recover_resource_reservation(context,self.scheduler):
            return SubmissionRecovery(safe_to_retry=True)
        reservation=await self.scheduler.active_reservation(context.job.id)
        if not reservation:
            return SubmissionRecovery(safe_to_retry=(
                context.previous_acceptance_receipt.get('allocation_phase') in {
                    'awaiting_capacity','absent'}))
        async with self.scheduler.hold(reservation.id,reservation.node_id,reservation.node_generation):
            recovery=await self.backend.recover_submission(context,reservation)
        if recovery.submission is not None:
            await self.scheduler.mark_running(reservation.id,reservation.node_id,
                reservation.node_generation)
        elif recovery.safe_to_retry:
            await self.scheduler.release(reservation.id,reservation.node_id,
                reservation.node_generation)
        return recovery

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        receipt=context.previous_acceptance_receipt
        async with self.scheduler.hold(str(receipt.get('reservation_id','')),
                str(receipt.get('node_id','')),int(receipt.get('node_generation') or 0)):
            return await self.backend.status(context,external_execution_id,receipt)

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        receipt=context.previous_acceptance_receipt
        async with self.scheduler.hold(str(receipt.get('reservation_id','')),
                str(receipt.get('node_id','')),int(receipt.get('node_generation') or 0)):
            return await self.backend.cancel(context,external_execution_id,receipt)

    async def pause(self,context:JobExecutionContext,external_execution_id:str)->dict[str,Any]:
        receipt=context.previous_acceptance_receipt
        async with self.scheduler.hold(str(receipt.get('reservation_id','')),
                str(receipt.get('node_id','')),int(receipt.get('node_generation') or 0)):
            return await self.backend.pause(context,external_execution_id,receipt)

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.cleanup_external(context,external_execution_id)
        await self.release_capacity(context)

    async def resolve_cleanup(self,context:JobExecutionContext,external_execution_id:str)->dict:
        resolver=getattr(self.backend,'resolve_cleanup',None)
        if not resolver:return context.previous_acceptance_receipt
        return await resolver(context,external_execution_id,context.previous_acceptance_receipt)

    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        receipt=context.previous_acceptance_receipt
        reservation_id=str(receipt.get('reservation_id',''))
        if not reservation_id:
            await self.backend.cleanup(context,external_execution_id,receipt);return
        node_id=str(receipt.get('node_id',''));generation=int(receipt.get('node_generation') or 0)
        if context.job.state in TERMINAL_JOB_STATES and await self.scheduler.reservation_released(
                reservation_id,node_id,generation,job_id=context.job.id,
                job_generation=context.generation):
            # Idempotent cleanup may still need to revoke access after retirement.
            await self.backend.cleanup(context,external_execution_id,receipt);return
        async with self.scheduler.hold(reservation_id,node_id,generation):
            await self.backend.cleanup(context,external_execution_id,receipt)

    async def release_capacity(self,context:JobExecutionContext)->None:
        receipt=context.previous_acceptance_receipt
        reservation_id=str(receipt.get('reservation_id',''))
        if not reservation_id:return
        node_id=str(receipt.get('node_id',''));generation=int(receipt.get('node_generation') or 0)
        await self.scheduler.release(reservation_id,node_id,generation)

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict[str,Any]:
        collector=getattr(self.backend,'collect',None)
        if collector:
            receipt=context.previous_acceptance_receipt
            async with self.scheduler.hold(str(receipt.get('reservation_id','')),
                    str(receipt.get('node_id','')),int(receipt.get('node_generation') or 0)):
                result=await collector(context,external_execution_id,status,receipt)
        elif status.state==JobState.BLOCKED:
            return {'blocked':True,**status.result}
        else:result=status.result
        if status.state==JobState.BLOCKED:return {'blocked':True,**result}
        return result


class SandboxChildBackend(SandboxAllocationOwner):
    allocation_poll_seconds=1.0
    def __init__(self,*,pool,runtime,store,objects,settings,
                 token_factory:Callable[[JobExecutionContext,Any,str],Awaitable[tuple[str,Any]]],
                 profile_resolver=None,allocation_client=None):
        self.pool=pool;self.runtime=runtime;self.store=store;self.objects=objects
        self.settings=settings;self.token_factory=token_factory;self.profile_resolver=profile_resolver
        self.allocation_client=allocation_client
        self.sessions={}
        self.collectors={}
        self.artifact_gateway=ArtifactGateway(store,objects,
            settings.data_root/'tmp'/'child-job-artifacts',maximum_bytes=settings.max_artifact_bytes)

    @staticmethod
    def _thread_id(job_id:str)->str:return 'child-job-'+job_id

    def _provider_release(self,reservation,harness:Harness,*,provider_ref:str="")->ProviderRelease:
        image=(getattr(reservation,"image_ref","") or provider_ref
            or self.settings.image_for(harness.value))
        if self.profile_resolver:
            return self.profile_resolver.provider_release(reservation.runtime_profile_id,
                harness.value,provider_ref=image,artifact_digest=reservation.image_digest)
        return ProviderRelease(ref=image,revision=image,
            artifact_digest=reservation.image_digest)





    async def _sandbox(self,context:JobExecutionContext,receipt:dict):
        identity=str(receipt.get('sandbox_id',''))
        try:handle=SandboxHandle.from_mapping(receipt.get('sandbox_handle') or {})
        except ValueError as exc:raise RuntimeError('child_sandbox_handle_invalid') from exc
        cached=self.sessions.get(context.job.id)
        if cached and cached.handle==handle:return cached
        attached=await self.pool.get_attached(self._thread_id(context.job.id))
        if attached and attached.handle==handle:
            self.sessions[context.job.id]=attached;return attached
        try:candidate=await self.pool.backend.get(handle)
        except SandboxNotFound as exc:raise RuntimeError('child_sandbox_unavailable') from exc
        adopted=await self.pool.adopt(candidate,thread_id=self._thread_id(context.job.id),
            turn_id=context.job.id,expected_profile=candidate.profile)
        self.sessions[context.job.id]=adopted;return adopted

    @staticmethod
    def _preparation_failure(exc:Exception)->PreAcceptFailure:
        retryable=isinstance(exc,(httpx.TimeoutException,httpx.TransportError))
        if isinstance(exc,httpx.HTTPStatusError):
            retryable=exc.response.status_code in {408,429,500,502,503,504}
        return PreAcceptFailure('Child runtime preparation failed before execution',
            error_code='child_preparation_failed',failed_phase='submission',
            retry_class='bounded_pre_accept' if retryable else 'none',retryable=retryable)

    async def start(self,context:JobExecutionContext,reservation)->tuple[str,dict[str,Any]]:
        harness=Harness(reservation.harness)
        release=self._provider_release(reservation,harness);image=release.ref
        if len(context.request.input_artifacts)>getattr(self.settings,'child_input_max_count',100):
            raise ValueError('child_input_count_exceeded')
        inputs=[];total_bytes=0
        for item in context.request.input_artifacts:
            artifact=await self.store.get_artifact(item.artifact_id)
            if not artifact:raise RuntimeError('child_input_artifact_missing')
            if artifact.size>self.settings.max_artifact_bytes:raise ValueError('child_input_item_too_large')
            total_bytes+=artifact.size;inputs.append((item,artifact))
        if total_bytes>getattr(self.settings,'child_input_max_total_bytes',8*1024*1024*1024):
            raise ValueError('child_input_total_too_large')
        if ((total_bytes+1024*1024-1)//(1024*1024)+512)>reservation.disk_mb:
            raise ValueError('child_input_exceeds_reserved_disk')
        execution_id=context.job.id+':'+str(context.generation)
        sandbox,allocation=await self.acquire_allocation(context,reservation,execution_id)
        try:
            token,claims=await self.token_factory(context,reservation,sandbox.id)
            allocation["capability_nonce"]=claims.nonce
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id=execution_id,receipt=allocation)
            paused=await self.store.get_pause_participant(context.job.thread_id,
                'child_harness',context.job.id)
            restoring=bool(paused and paused.checkpoint_ref and paused.state.value=='RESUMED')
        except BaseException as exc:
            await self.release_prepared_allocation(sandbox,allocation)
            if isinstance(exc,(PreAcceptCancelled,PreAcceptFailure)):raise
            if isinstance(exc,Exception):raise self._preparation_failure(exc) from exc
            raise
        execution_requested=False
        try:
            route=self.pool.backend.runtime_route(sandbox)
            await self.runtime.inject(sandbox,capability_token=token,gateway_url=route.gateway_url,
                proxy_url=route.proxy_url,harness=harness.value,
                model=str(context.request.parameters.get('model','')),
                model_start_timeout_seconds=self.settings.runtime_model_start_timeout_seconds,
                event_idle_timeout_seconds=self.settings.runtime_event_idle_timeout_seconds,
                telemetry_metrics_enabled=self.settings.runtime_metrics_enabled,
                runtime_bundle_digest=self.settings.runtime_bundle_digest,
                runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest,
                runtime_profile_id=reservation.runtime_profile_id,
                skill_ids=list(context.request.skill_ids),
                skill_digests=dict(context.request.parameters.get('skill_digests',{})),
                defer_skill_install=restoring,child_mode=True,delegation_depth=1,
                logical_thread_id=context.job.thread_id,
                logical_turn_id=context.job.origin_turn_id,
                logical_job_id=context.job.id)
            if restoring:
                archive_size=await asyncio.to_thread(self.objects.size,paused.checkpoint_ref,
                    maximum=self.settings.max_checkpoint_bytes)
                await self.runtime.restore(sandbox,object_chunks(self.objects,paused.checkpoint_ref,
                    maximum=self.settings.max_checkpoint_bytes),size=archive_size)
            for item,artifact in inputs:
                iterator=self.objects.iter_bytes(artifact.object_ref,
                    maximum=self.settings.max_artifact_bytes)
                sentinel=object()
                async def chunks(iterator=iterator,sentinel=sentinel):
                    while True:
                        value=await asyncio.to_thread(next,iterator,sentinel)
                        if value is sentinel:return
                        yield value
                await self.runtime.put_project_file_stream(sandbox,
                    HarnessJobAdapter._materialized_input_path(item,artifact),chunks(),
                    size=artifact.size,mime=artifact.mime)
            message=HarnessJobAdapter.execution_prompt(context.request,inputs)
            if restoring:
                message=('Continue the interrupted Job from its durable checkpoint. Preserve completed '
                    'work, do not repeat external side effects, and finish this frozen objective:\n\n'+message)
            await self._check_cancelled(context)
            allocation['allocation_phase']='runtime_submission'
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id=execution_id,receipt=allocation)
            execution_requested=True
            accepted=await self.runtime.start_execution(sandbox,
                message=message, attachments=[],native_thread_id=(
                    paused.native_thread_id if restoring else ''),reasoning_effort=str(
                    context.request.parameters.get('reasoning_effort','high')),
                execution_id=execution_id,input_id=context.job.id)
            self.sessions[context.job.id]=sandbox
            self._ensure_collector(context,sandbox,execution_id)
            return execution_id,{**allocation,'sandbox_id':sandbox.id,
                'sandbox_handle':sandbox.handle.to_mapping(),'capability_nonce':claims.nonce,
                'runtime_acceptance':accepted}
        except PreAcceptCancelled:raise
        except Exception as exc:
            if isinstance(exc,RuntimeError) and str(exc)=='job_cancelled_before_acceptance':
                raise PreAcceptCancelled() from exc
            if execution_requested:
                self.sessions[context.job.id]=sandbox
                raise SubmissionOutcomeUnknown('Child execution response unavailable',
                    external_execution_id=execution_id,acceptance_receipt={**allocation,
                        'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping(),
                        'capability_nonce':claims.nonce}) from exc
            await self.release_prepared_allocation(sandbox,allocation)
            raise self._preparation_failure(exc) from exc
        except asyncio.CancelledError as exc:
            self.sessions[context.job.id]=sandbox
            raise SubmissionInterrupted(execution_id,allocation) from exc
        except BaseException:
            if not execution_requested:
                await self.release_prepared_allocation(sandbox,allocation)
            raise

    async def recover_submission(self,context:JobExecutionContext,reservation)->SubmissionRecovery:
        recovery=await self.recover_allocation(context,reservation)
        if recovery is not None:return recovery
        harness=Harness(reservation.harness)
        matches=[];raw=context.previous_acceptance_receipt.get("sandbox_handle") or {}
        if raw:
            try:matches=[await self.pool.backend.get(SandboxHandle.from_mapping(raw))]
            except (SandboxNotFound,ValueError):pass
        if not matches:return SubmissionRecovery()
        release=self._provider_release(reservation,harness,
            provider_ref=matches[0].profile.provider_ref)
        ready=[value for value in matches if value.health_state=="ready"
            and value.harness==harness and value.handle.revision==release.revision]
        if len(matches)!=1 or len(ready)!=1:return SubmissionRecovery()
        sandbox=await self.pool.adopt(ready[0],thread_id=self._thread_id(context.job.id),
            turn_id=context.job.id,expected_profile=ready[0].profile)
        execution_id=context.job.id+":"+str(reservation.job_generation)
        try:value=await self._execution_status(sandbox,execution_id)
        except httpx.HTTPStatusError as exc:
            try:detail=str(exc.response.json().get('detail') or '')
            except (ValueError,AttributeError):detail=''
            if exc.response.status_code!=503 or detail!='Runtime Code Bundle is not active':raise
            await self.release_prepared_allocation(sandbox,context.previous_acceptance_receipt)
            return SubmissionRecovery(safe_to_retry=True)
        if not value:
            await self.release_prepared_allocation(sandbox,context.previous_acceptance_receipt)
            return SubmissionRecovery(safe_to_retry=True)
        self.sessions[context.job.id]=sandbox
        self._ensure_collector(context,sandbox,execution_id)
        return SubmissionRecovery(submission=AdapterSubmission(external_execution_id=execution_id,
            acceptance_receipt={**context.previous_acceptance_receipt,
                "executor":"harness","reservation_id":reservation.id,
                "node_id":reservation.node_id,"node_generation":reservation.node_generation,
                "runtime_profile_id":reservation.runtime_profile_id,
                "image_digest":reservation.image_digest,"gpu_ids":list(reservation.gpu_ids),
                "sandbox_id":sandbox.id,"sandbox_handle":sandbox.handle.to_mapping(),
                "capability_nonce":str(
                    context.previous_acceptance_receipt.get("capability_nonce") or ""),
                "runtime_acceptance":{"accepted":True,"created":False,"recovered":True,
                    "execution_id":execution_id}}))

    async def _execution_status(self,sandbox,execution_id,retry_seconds:float=30):
        deadline=time.monotonic()+retry_seconds;delay=.25
        while True:
            try:return await self.runtime.execution_status(sandbox,execution_id)
            except httpx.HTTPStatusError as exc:
                try:detail=str(exc.response.json().get('detail') or '')
                except (ValueError,AttributeError):detail=''
                if detail=='Runtime Code Bundle is not active':raise
                if (exc.response.status_code not in {408,429,500,502,503,504}
                        or time.monotonic()>=deadline):raise
                await asyncio.sleep(delay);delay=min(2,delay*2)

    def _ensure_collector(self,context:JobExecutionContext,sandbox,execution_id:str)->None:
        existing=self.collectors.get(context.job.id)
        if existing and not existing.done():return
        self.collectors[context.job.id]=asyncio.create_task(
            self._collect(context,sandbox,execution_id),name='child-activities-'+context.job.id)

    async def _collect(self,context:JobExecutionContext,sandbox,execution_id:str)->None:
        rows=await self.store.list_job_activities(context.job.id,0,1000)
        cursor=rows[-1]['sequence'] if rows else 0
        async for value in self.runtime.resume(sandbox,execution_id,cursor):
            sequence=getattr(value,'runtime_sequence',0)
            if not sequence:continue
            if hasattr(value,'kind'):
                kind=value.kind.value if hasattr(value.kind,'value') else str(value.kind)
                status=('completed' if kind in {'step_finished','tool_finished'} else
                    'failed' if kind=='error' else 'in_progress')
                await self.store.append_job_activity(context.job.id,sequence,kind,value.payload,status=status)

    async def status(self,context:JobExecutionContext,external_execution_id:str,
                     receipt:dict[str,Any])->AdapterStatus:
        try:sandbox=await self._sandbox(context,receipt)
        except RuntimeError as exc:
            if str(exc)!='child_sandbox_unavailable':raise
            return AdapterStatus(True,JobState.FAILED,{
                'error_code':'child_sandbox_lost','sandbox_id':str(receipt.get('sandbox_id',''))})
        continuation_method=getattr(self.store,'child_continuation',None)
        continuation=await continuation_method(context.job.id) if continuation_method else None
        current_execution_id=(continuation.get('resume_execution_id')
            if continuation and continuation.get('resume_execution_id') else external_execution_id)
        value=await self._execution_status(sandbox,current_execution_id)
        if not value:
            try:await self.pool.backend.get(sandbox.id)
            except SandboxNotFound:
                self.sessions.pop(context.job.id,None)
                return AdapterStatus(True,JobState.FAILED,{
                    'error_code':'child_sandbox_lost','sandbox_id':sandbox.id})
            return AdapterStatus(False)
        self._ensure_collector(context,sandbox,current_execution_id)
        result=value.get('result')
        if result is None:return AdapterStatus(False)
        if continuation and continuation['state']=='ready':
            resumed_id=f"{context.job.id}:{context.generation}:group:{continuation['group_id']}"
            materialized=[]
            for artifact in await self.store.child_continuation_artifacts(continuation['group_id']):
                path=f"artifacts/subjobs/{artifact['job_id']}/{artifact['filename']}"
                iterator=self.objects.iter_bytes(artifact['object_ref'],maximum=self.settings.max_artifact_bytes)
                sentinel=object()
                async def chunks(iterator=iterator,sentinel=sentinel):
                    while True:
                        chunk=await asyncio.to_thread(next,iterator,sentinel)
                        if chunk is sentinel:return
                        yield chunk
                await self.runtime.put_project_file_stream(sandbox,path,chunks(),
                    size=artifact['size'],mime=artifact['mime'])
                materialized.append({**artifact,'materialized_path':path})
            message=("[subordinate_job_group_completed] Factual durable tool results. Inspect the "
                "returned Artifacts, continue the frozen objective, and do not repeat the tool call: "+
                json.dumps({**continuation['result'],'materialized_artifacts':materialized},
                    ensure_ascii=False,sort_keys=True,separators=(',',':')))
            await self.runtime.start_execution(sandbox,message=message,attachments=[],
                native_thread_id=str(result.get('native_thread_id') or ''),
                reasoning_effort=str(context.request.parameters.get('reasoning_effort','high')),
                execution_id=resumed_id,input_id=continuation['outbox_id'])
            await self.store.mark_child_continuation_resumed(continuation['id'],resumed_id)
            self._ensure_collector(context,sandbox,resumed_id)
            return AdapterStatus(False)
        has_open=getattr(self.store,'child_has_open_group',None)
        if has_open and await has_open(context.job.id):return AdapterStatus(False)
        status=result.get('status')
        state={'completed':JobState.SUCCEEDED,'failed':JobState.FAILED,
            'cancelled':JobState.CANCELLED,'blocked':JobState.BLOCKED,
            'waiting_user':JobState.BLOCKED}.get(status,JobState.UNKNOWN)
        return AdapterStatus(True,state,result)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str,
                     receipt:dict[str,Any])->AdapterStatus:
        sandbox=await self._sandbox(context,receipt);await self.runtime.cancel(sandbox)
        return AdapterStatus(True,JobState.CANCELLED,{'cancelled':True})

    async def pause(self,context:JobExecutionContext,external_execution_id:str,
                    receipt:dict[str,Any])->dict[str,Any]:
        sandbox=await self._sandbox(context,receipt)
        stopped=await self.runtime.cancel(sandbox)
        archive=await self.runtime.checkpoint(sandbox,maximum=self.settings.max_checkpoint_bytes)
        try:
            checkpoint_ref,_=await asyncio.to_thread(self.objects.put_file,archive.path,
                digest=archive.sha256,size=archive.size)
        finally:archive.remove()
        return {'checkpoint_ref':checkpoint_ref,
            'native_thread_id':str(stopped.get('native_thread_id') or ''),
            'external_execution_id':external_execution_id}

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus,receipt:dict[str,Any])->dict[str,Any]:
        list_activities=getattr(self.store,'list_job_activities',None)
        activities=(await list_activities(context.job.id,0,1000)
            if list_activities and getattr(context,'job',None) else [])
        child_summary=''.join(str(row.get('summary') or '') for row in activities
            if row.get('kind')=='text_delta')[-65536:]
        if status.state!=JobState.SUCCEEDED:return {**status.result,
            **({'child_summary':child_summary} if child_summary else {})}
        sandbox=await self._sandbox(context,receipt)
        items=await self.runtime.artifacts(sandbox,context.job.id)
        by_path={str(item['path']):item for item in items}
        if context.request.adapter=='reviewer-harness':
            if set(by_path)!={'review-verdict.json'}:raise ValueError('reviewer_output_contract_invalid')
            staged=await self.runtime.artifact(sandbox,'review-verdict.json',maximum=1024*1024)
            try:
                verify_runtime_artifact(staged,by_path['review-verdict.json'])
                value=json.loads(await asyncio.to_thread(staged.path.read_bytes))
            except (ValueError,TypeError) as exc:raise ValueError('reviewer_verdict_invalid') from exc
            finally:staged.remove()
            return {'review':value}
        contracts={value.artifact_role:value for value in context.request.expected_outputs}
        if not contracts:
            if by_path:raise ValueError('unexpected_child_artifacts')
            return {**status.result,**({'child_summary':child_summary} if child_summary else {})}
        manifest_path='job-output-manifest.json';manifest=None
        if manifest_path in by_path:
            staged=await self.runtime.artifact(sandbox,manifest_path,maximum=1024*1024)
            try:
                verify_runtime_artifact(staged,by_path[manifest_path])
                manifest=json.loads(await asyncio.to_thread(staged.path.read_bytes)).get('outputs')
            except (ValueError,AttributeError,TypeError) as exc:raise ValueError('job_output_manifest_invalid') from exc
            finally:staged.remove()
            if not isinstance(manifest,list):raise ValueError('job_output_manifest_invalid')
        artifact_paths=sorted(set(by_path)-{manifest_path})
        if manifest is None:
            if len(contracts)!=1:raise ValueError('job_output_manifest_required')
            role=next(iter(contracts));manifest=[{'path':path,'artifact_role':role,
                'item_key':path,'position':position} for position,path in enumerate(artifact_paths)]
        if {str(row.get('path','')) for row in manifest}!=set(artifact_paths) or len(manifest)!=len(artifact_paths):
            raise ValueError('job_output_manifest_mismatch')
        emitted=[];roles=set()
        for row in manifest:
            path=str(row.get('path',''));role=str(row.get('artifact_role',''))
            contract=contracts.get(role)
            if not contract:raise ValueError('job_output_role_not_granted')
            metadata=by_path[path]
            staged=await self.runtime.artifact(sandbox,path,maximum=self.settings.max_artifact_bytes)
            try:
                verify_runtime_artifact(staged,metadata)
                output=await self.artifact_gateway.emit(ArtifactEmission(job_id=context.job.id,
                    generation=context.generation,artifact_role=role,item_key=str(row.get('item_key',path)),
                    position=int(row.get('position',0)),filename=str(row.get('filename') or Path(path).name),
                    output_disposition=contract.output_disposition,declared_mime=str(metadata.get('mime') or 'application/octet-stream')),
                    staged_file_chunks(staged.path))
            finally:staged.remove()
            emitted.append(output.model_dump(mode='json'));roles.add(role)
        collections=[]
        try:
            for role in sorted(contracts):
                contract=contracts[role]
                unsupported=set(contract.validators)-{'nonempty'}
                if unsupported:
                    raise ValueError('child_output_validator_unsupported:'+','.join(sorted(unsupported)))
                role_outputs=[row for row in emitted if row['artifact_role']==role]
                receipts=[]
                if 'nonempty' in contract.validators:
                    receipts.append({'validator':'nonempty',
                        'status':'pass' if role_outputs and all(row['size']>0 for row in role_outputs) else 'fail',
                        'artifact_digests':[row['sha256'] for row in role_outputs]})
                collections.append(await self.store.finalize_job_artifact_collection(
                    context.job.id,context.generation,role,
                    validation_receipts=receipts))
        except ArtifactCollectionCardinalityError as exc:
            exc.result['child_summary']=child_summary[:65536]
            raise
        return {**status.result,'child_summary':child_summary,
            'artifacts':emitted,'collections':collections}



    async def cleanup(self,context:JobExecutionContext,external_execution_id:str,
                      receipt:dict[str,Any])->None:
        sandbox=None;identity=str(receipt.get('sandbox_id',''))
        unstarted=receipt.get('allocation_phase') in {'allocating','absent'}
        if receipt.get('allocation_profile') and receipt.get('sandbox_handle'):
            sandbox=self._unstarted_allocation(context,receipt)
        elif identity:
            try:sandbox=await self._sandbox(context,receipt)
            except RuntimeError as exc:
                if str(exc)!='child_sandbox_unavailable':raise
        collector=self.collectors.pop(context.job.id,None) if not unstarted else None
        if collector:
            try:await asyncio.wait_for(asyncio.shield(collector),timeout=5)
            except (asyncio.TimeoutError,Exception):
                collector.cancel();await asyncio.gather(collector,return_exceptions=True)
        nonce=str(receipt.get('capability_nonce',''))
        if nonce:
            async with observed_async_client('sandbox-gateway',timeout=10,trust_env=False) as client:
                response=await client.post(self.settings.gateway_url.rstrip('/')+'/internal/revoke/'+nonce,
                    headers={'X-Sandbox-Harness-Control':self.settings.gateway_control_key,
                        'X-Updream-Thread-Id':context.job.thread_id,
                        'X-Updream-Turn-Id':context.job.origin_turn_id,
                        'X-Updream-Sandbox-Id':sandbox.id if sandbox else identity})
                response.raise_for_status()
        if sandbox:
            retire=getattr(self.pool,'retire',None)
            if retire:
                await retire(sandbox)
                if await self.pool.retirement_pending(sandbox):
                    raise SandboxProviderUnavailable('Allocation deletion is pending',
                        provider=sandbox.handle.provider)
            else:await self.pool.release(sandbox,keep_attached=False)
        if self.allocation_client and identity:await self.allocation_client.released(receipt)
        cached=self.sessions.get(context.job.id)
        if cached and sandbox and cached.handle==sandbox.handle:self.sessions.pop(context.job.id,None)


# Temporary import compatibility for external real-case scripts.
