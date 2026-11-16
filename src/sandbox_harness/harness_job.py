from __future__ import annotations

import httpx
from typing import Any, Awaitable, Callable, Protocol

from .domain import Harness
from .job_adapters import AdapterStatus, AdapterSubmission, BaseJobAdapter, JobExecutionContext
from .job_contracts import JobState
from .scheduler import ResourceRequest, Scheduler


class ChildExecutionBackend(Protocol):
    async def start(self, context: JobExecutionContext, reservation) -> tuple[str, dict[str, Any]]: ...
    async def status(self, context: JobExecutionContext, external_execution_id: str,
                     receipt: dict[str, Any]) -> AdapterStatus: ...
    async def cancel(self, context: JobExecutionContext, external_execution_id: str,
                     receipt: dict[str, Any]) -> AdapterStatus: ...
    async def cleanup(self, context: JobExecutionContext, external_execution_id: str,
                      receipt: dict[str, Any]) -> None: ...


class HarnessJobAdapter(BaseJobAdapter):
    def __init__(self, scheduler: Scheduler, backend: ChildExecutionBackend):
        self.scheduler=scheduler;self.backend=backend

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
        reservation=await self.scheduler.reserve(context.job.id,context.generation,self._resources(context))
        execution_id,receipt=await self.backend.start(context,reservation)
        await self.scheduler.mark_running(reservation.id,reservation.node_id,reservation.node_generation)
        return AdapterSubmission(external_execution_id=execution_id,acceptance_receipt={
            'executor':'harness','reservation_id':reservation.id,'node_id':reservation.node_id,
            'node_generation':reservation.node_generation,'runtime_profile_id':reservation.runtime_profile_id,
            'image_digest':reservation.image_digest,'gpu_ids':list(reservation.gpu_ids),**receipt})

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        receipt=context.previous_acceptance_receipt
        await self.scheduler.renew(str(receipt.get('reservation_id','')),
            str(receipt.get('node_id','')),int(receipt.get('node_generation') or 0))
        return await self.backend.status(context,external_execution_id,
            receipt)

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.backend.cancel(context,external_execution_id,
            context.previous_acceptance_receipt)

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        receipt=context.previous_acceptance_receipt
        await self.backend.cleanup(context,external_execution_id,receipt)
        reservation_id=str(receipt.get('reservation_id',''))
        if reservation_id:
            await self.scheduler.release(reservation_id,str(receipt.get('node_id','')),
                int(receipt.get('node_generation') or 0))

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict[str,Any]:
        if status.state==JobState.BLOCKED:
            return {'blocked':True,**status.result}
        return status.result


class OpenSandboxChildBackend:
    def __init__(self,*,pool,runtime,store,objects,settings,
                 token_factory:Callable[[JobExecutionContext,Any,str],Awaitable[tuple[str,Any]]]):
        self.pool=pool;self.runtime=runtime;self.store=store;self.objects=objects
        self.settings=settings;self.token_factory=token_factory;self.sessions={}

    @staticmethod
    def _thread_id(job_id:str)->str:return 'child-job-'+job_id

    async def _sandbox(self,context:JobExecutionContext,receipt:dict):
        identity=str(receipt.get('sandbox_id',''))
        cached=self.sessions.get(context.job.id)
        if cached and cached.id==identity:return cached
        attached=await self.pool.get_attached(self._thread_id(context.job.id))
        if attached and attached.id==identity:
            self.sessions[context.job.id]=attached;return attached
        matches=[value for value in await self.pool.backend.list_namespace() if value.id==identity]
        if len(matches)!=1:raise RuntimeError('child_sandbox_unavailable')
        adopted=await self.pool.adopt(matches[0],thread_id=self._thread_id(context.job.id),turn_id=context.job.id)
        self.sessions[context.job.id]=adopted;return adopted

    async def start(self,context:JobExecutionContext,reservation)->tuple[str,dict[str,Any]]:
        harness=Harness(reservation.harness)
        image=reservation.image_ref or self.settings.image_for(harness.value)
        if not image.endswith('@sha256:'+reservation.image_digest):
            raise RuntimeError('child_image_digest_mismatch')
        sandbox=await self.pool.acquire(thread_id=self._thread_id(context.job.id),turn_id=context.job.id,
            harness=harness,environment={},image=image)
        token,claims=await self.token_factory(context,reservation,sandbox.id)
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
                child_mode=True,delegation_depth=1)
            for item in context.request.input_artifacts:
                artifact=await self.store.get_artifact(item.artifact_id)
                if not artifact:raise RuntimeError('child_input_artifact_missing')
                data=await __import__('asyncio').to_thread(self.objects.get,artifact.object_ref,
                    maximum=self.settings.max_artifact_bytes)
                await self.runtime.put_project_file(sandbox,
                    f"assets/inputs/{item.artifact_role}-{artifact.id}",data,artifact.mime)
            execution_id='child:'+context.job.id+':'+str(context.generation)
            accepted=await self.runtime.start_execution(sandbox,
                message=str(context.request.parameters.get('objective','')),
                attachments=[],native_thread_id='',reasoning_effort=str(
                    context.request.parameters.get('reasoning_effort','high')),
                execution_id=execution_id,input_id=context.job.id)
            self.sessions[context.job.id]=sandbox
            return execution_id,{'sandbox_id':sandbox.id,'capability_nonce':claims.nonce,
                'runtime_acceptance':accepted}
        except BaseException:
            await self.pool.release(sandbox,keep_attached=False)
            raise

    async def status(self,context:JobExecutionContext,external_execution_id:str,
                     receipt:dict[str,Any])->AdapterStatus:
        sandbox=await self._sandbox(context,receipt)
        value=await self.runtime.execution_status(sandbox,external_execution_id)
        if not value:return AdapterStatus(False)
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

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str,
                      receipt:dict[str,Any])->None:
        sandbox=await self._sandbox(context,receipt)
        nonce=str(receipt.get('capability_nonce',''))
        if nonce:
            async with httpx.AsyncClient(timeout=10,trust_env=False) as client:
                response=await client.post(self.settings.gateway_url.rstrip('/')+'/internal/revoke/'+nonce,
                    headers={'X-Sandbox-Harness-Control':self.settings.gateway_control_key,
                        'X-Updream-Thread-Id':context.job.thread_id,
                        'X-Updream-Turn-Id':context.job.origin_turn_id,
                        'X-Updream-Sandbox-Id':sandbox.id})
                response.raise_for_status()
        await self.pool.release(sandbox,keep_attached=False)
        self.sessions.pop(context.job.id,None)
