from __future__ import annotations

import asyncio
import httpx
import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from .domain import Harness
from .grants import GrantAuthority,JobGrantRequest,TurnGrantRequest
from .job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext,
    PreAcceptFailure,SubmissionRecovery)
from .job_contracts import JobState
from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .scheduler import ResourceRequest, Scheduler
from .sandbox import SandboxNotFound
from .objects import object_chunks


async def staged_file_chunks(path: Path, chunk_size: int = 1024 * 1024):
    with path.open("rb") as stream:
        while chunk := await asyncio.to_thread(stream.read, chunk_size):
            yield chunk


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
        routes=(*model_routes,'/v1/job-artifacts','/v1/telemetry',
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
            ttl_seconds=self.settings.capability_ttl_seconds,
            max_requests=self.settings.capability_request_limit,
            max_public_requests=self.settings.capability_public_request_limit,token_budget=token_budget))


class HarnessJobAdapter(BaseJobAdapter):
    def __init__(self, scheduler: Scheduler, backend: ChildExecutionBackend):
        self.scheduler=scheduler;self.backend=backend

    @staticmethod
    def execution_prompt(request)->str:
        objective=str(request.parameters.get('objective',''))
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
        if not contracts:return objective
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
        try:reservation=await self.scheduler.reserve(
            context.job.id,context.generation,self._resources(context))
        except RuntimeError as exc:
            if str(exc)=='no_compatible_node_capacity':
                raise PreAcceptFailure(str(exc),infrastructure=True,retry_after_seconds=1) from exc
            raise
        try:
            async with self.scheduler.hold(reservation.id,reservation.node_id,reservation.node_generation):
                execution_id,receipt=await self.backend.start(context,reservation)
                await self.scheduler.mark_running(reservation.id,reservation.node_id,reservation.node_generation)
        except BaseException:
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise
        return AdapterSubmission(external_execution_id=execution_id,acceptance_receipt={
            'executor':'harness','reservation_id':reservation.id,'node_id':reservation.node_id,
            'node_generation':reservation.node_generation,'runtime_profile_id':reservation.runtime_profile_id,
            'image_digest':reservation.image_digest,'gpu_ids':list(reservation.gpu_ids),**receipt})

    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        reservation=await self.scheduler.active_reservation(context.job.id)
        if not reservation:return SubmissionRecovery()
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

    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        receipt=context.previous_acceptance_receipt
        reservation_id=str(receipt.get('reservation_id',''))
        if not reservation_id:
            await self.backend.cleanup(context,external_execution_id,receipt);return
        node_id=str(receipt.get('node_id',''));generation=int(receipt.get('node_generation') or 0)
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


class OpenSandboxChildBackend:
    def __init__(self,*,pool,runtime,store,objects,settings,
                 token_factory:Callable[[JobExecutionContext,Any,str],Awaitable[tuple[str,Any]]]):
        self.pool=pool;self.runtime=runtime;self.store=store;self.objects=objects
        self.settings=settings;self.token_factory=token_factory;self.sessions={}
        self.collectors={}
        self.artifact_gateway=ArtifactGateway(store,objects,
            settings.data_root/'tmp'/'child-job-artifacts',maximum_bytes=settings.max_artifact_bytes)

    @staticmethod
    def _thread_id(job_id:str)->str:return 'child-job-'+job_id

    async def _sandbox(self,context:JobExecutionContext,receipt:dict):
        identity=str(receipt.get('sandbox_id',''))
        cached=self.sessions.get(context.job.id)
        if cached and cached.id==identity:return cached
        attached=await self.pool.get_attached(self._thread_id(context.job.id))
        if attached and attached.id==identity:
            self.sessions[context.job.id]=attached;return attached
        try:candidate=await self.pool.backend.get(identity)
        except SandboxNotFound as exc:raise RuntimeError('child_sandbox_unavailable') from exc
        adopted=await self.pool.adopt(candidate,thread_id=self._thread_id(context.job.id),turn_id=context.job.id)
        self.sessions[context.job.id]=adopted;return adopted

    async def start(self,context:JobExecutionContext,reservation)->tuple[str,dict[str,Any]]:
        harness=Harness(reservation.harness)
        image=reservation.image_ref or self.settings.image_for(harness.value)
        if not image.endswith('@sha256:'+reservation.image_digest):
            raise RuntimeError('child_image_digest_mismatch')
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
        sandbox=await self.pool.acquire(thread_id=self._thread_id(context.job.id),turn_id=context.job.id,
            harness=harness,environment={},image=image)
        token,claims=await self.token_factory(context,reservation,sandbox.id)
        paused=await self.store.get_pause_participant(context.job.thread_id,
            'child_harness',context.job.id)
        restoring=bool(paused and paused.checkpoint_ref and paused.state.value=='RESUMED')
        try:
            await self.runtime.inject(sandbox,capability_token=token,gateway_url=self.settings.gateway_url,
                proxy_url=self.settings.public_proxy_url,harness=harness.value,
                model=str(context.request.parameters.get('model','')),
                model_start_timeout_seconds=self.settings.runtime_model_start_timeout_seconds,
                event_idle_timeout_seconds=self.settings.runtime_event_idle_timeout_seconds,
                runtime_bundle_digest=self.settings.runtime_bundle_digest,
                runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest,
                runtime_profile_id=reservation.runtime_profile_id,
                skill_ids=list(context.request.skill_ids),
                skill_digests=dict(context.request.parameters.get('skill_digests',{})),
                defer_skill_install=restoring,child_mode=True,delegation_depth=1)
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
                    f"assets/inputs/{item.artifact_role}-{artifact.id}",chunks(),
                    size=artifact.size,mime=artifact.mime)
            execution_id=context.job.id+':'+str(context.generation)
            message=HarnessJobAdapter.execution_prompt(context.request)
            if restoring:
                message=('Continue the interrupted Job from its durable checkpoint. Preserve completed '
                    'work, do not repeat external side effects, and finish this frozen objective:\n\n'+message)
            accepted=await self.runtime.start_execution(sandbox,
                message=message, attachments=[],native_thread_id=(
                    paused.native_thread_id if restoring else ''),reasoning_effort=str(
                    context.request.parameters.get('reasoning_effort','high')),
                execution_id=execution_id,input_id=context.job.id)
            self.sessions[context.job.id]=sandbox
            self._ensure_collector(context,sandbox,execution_id)
            return execution_id,{'sandbox_id':sandbox.id,'capability_nonce':claims.nonce,
                'runtime_acceptance':accepted}
        except BaseException:
            await self.pool.release(sandbox,keep_attached=False)
            raise

    async def recover_submission(self,context:JobExecutionContext,reservation)->SubmissionRecovery:
        matches=await self.pool.backend.find_by_turn(context.job.id)
        if not matches:return SubmissionRecovery(safe_to_retry=True)
        ready=[value for value in matches if value.health_state=="ready"
            and value.harness.value==reservation.harness
            and value.image.endswith("@sha256:"+reservation.image_digest)]
        if len(matches)!=1 or len(ready)!=1:return SubmissionRecovery()
        sandbox=await self.pool.adopt(ready[0],thread_id=self._thread_id(context.job.id),
            turn_id=context.job.id)
        execution_id=context.job.id+":"+str(reservation.job_generation)
        value=await self.runtime.execution_status(sandbox,execution_id)
        if not value:
            await self.pool.release(sandbox,keep_attached=False)
            return SubmissionRecovery(safe_to_retry=True)
        self.sessions[context.job.id]=sandbox
        self._ensure_collector(context,sandbox,execution_id)
        return SubmissionRecovery(submission=AdapterSubmission(external_execution_id=execution_id,
            acceptance_receipt={"executor":"harness","reservation_id":reservation.id,
                "node_id":reservation.node_id,"node_generation":reservation.node_generation,
                "runtime_profile_id":reservation.runtime_profile_id,
                "image_digest":reservation.image_digest,"gpu_ids":list(reservation.gpu_ids),
                "sandbox_id":sandbox.id,"capability_nonce":"",
                "runtime_acceptance":{"accepted":True,"created":False,"recovered":True,
                    "execution_id":execution_id}}))

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
        value=await self.runtime.execution_status(sandbox,external_execution_id)
        if not value:
            try:await self.pool.backend.get(sandbox.id)
            except SandboxNotFound:
                self.sessions.pop(context.job.id,None)
                return AdapterStatus(True,JobState.FAILED,{
                    'error_code':'child_sandbox_lost','sandbox_id':sandbox.id})
            return AdapterStatus(False)
        self._ensure_collector(context,sandbox,external_execution_id)
        result=value.get('result')
        if result is None:return AdapterStatus(False)
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
        if status.state!=JobState.SUCCEEDED:return status.result
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
            return status.result
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
        for role in sorted(roles):
            collections.append(await self.store.finalize_job_artifact_collection(
                context.job.id,context.generation,role))
        return {**status.result,'artifacts':emitted,'collections':collections}

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str,
                      receipt:dict[str,Any])->None:
        sandbox=None;identity=str(receipt.get('sandbox_id',''))
        if identity:
            try:sandbox=await self._sandbox(context,receipt)
            except RuntimeError as exc:
                if str(exc)!='child_sandbox_unavailable':raise
        else:
            matches=[value for value in await self.pool.backend.list_namespace()
                if value.turn_id==context.job.id or value.metadata.get(
                    'sandbox-harness.io/turn')==context.job.id]
            for match in matches:
                adopted=await self.pool.adopt(match,thread_id=self._thread_id(context.job.id),
                    turn_id=context.job.id)
                await self.pool.release(adopted,keep_attached=False)
        collector=self.collectors.pop(context.job.id,None)
        if collector:
            try:await asyncio.wait_for(asyncio.shield(collector),timeout=5)
            except (asyncio.TimeoutError,Exception):
                collector.cancel();await asyncio.gather(collector,return_exceptions=True)
        nonce=str(receipt.get('capability_nonce',''))
        if nonce:
            async with httpx.AsyncClient(timeout=10,trust_env=False) as client:
                response=await client.post(self.settings.gateway_url.rstrip('/')+'/internal/revoke/'+nonce,
                    headers={'X-Sandbox-Harness-Control':self.settings.gateway_control_key,
                        'X-Updream-Thread-Id':context.job.thread_id,
                        'X-Updream-Turn-Id':context.job.origin_turn_id,
                        'X-Updream-Sandbox-Id':sandbox.id if sandbox else identity})
                response.raise_for_status()
        if sandbox:await self.pool.release(sandbox,keep_attached=False)
        self.sessions.pop(context.job.id,None)
