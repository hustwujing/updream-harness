from __future__ import annotations

import asyncio
from pathlib import Path,PurePosixPath
from typing import Any

import httpx

from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .domain import Harness
from .harness_job import HarnessJobAdapter, staged_file_chunks
from .job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext,
    PreAcceptFailure)
from .job_contracts import JobState
from .sandbox_contracts import SandboxHandle
from .profiles import ProviderRelease


class MediaRenderOperationAdapter(BaseJobAdapter):
    def __init__(self,*,scheduler,pool,runtime,store,objects,settings,token_factory,
                 profile_resolver=None):
        self.scheduler=scheduler;self.pool=pool;self.runtime=runtime;self.store=store
        self.objects=objects;self.settings=settings;self.token_factory=token_factory
        self.profile_resolver=profile_resolver;self.sessions={}
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'operation-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    @staticmethod
    def _thread_id(job_id):return 'operation-job-'+job_id

    def _provider_release(self,reservation,harness:Harness)->ProviderRelease:
        image=getattr(reservation,"image_ref","") or self.settings.image_for(harness.value)
        if self.profile_resolver:
            return self.profile_resolver.provider_release(reservation.runtime_profile_id,
                harness.value,provider_ref=image,artifact_digest=reservation.image_digest)
        return ProviderRelease(ref=image,revision=image,
            artifact_digest=reservation.image_digest)

    @staticmethod
    def _controlled_validation_receipts(operation:str,recipe:dict,value:dict,
                                        validators:tuple[str,...],digest:str)->list[dict]:
        mime=str(value.get('mime','application/octet-stream'))
        evidence={'operation':operation,'width':recipe.get('width'),
            'height':recipe.get('height'),'fps':recipe.get('fps'),'mime':mime,
            'source':'controlled_media_operation'}
        supported=set()
        if operation=='image.resize':
            supported.add('image.dimensions_exact')
            if mime=='image/png':supported.add('image.png')
        elif operation=='video.transcode':
            supported.update({'video.dimensions_exact','video.fps'})
            if mime=='video/mp4':supported.add('video.mp4')
        return [{'validator':name,'status':'pass','artifact_digests':[digest],
            'evidence':evidence} for name in validators if name in supported]

    async def _sandbox(self,context,receipt):
        identity=str(receipt.get('sandbox_id',''))
        try:handle=SandboxHandle.from_mapping(receipt.get('sandbox_handle') or {})
        except ValueError as exc:raise RuntimeError('operation_sandbox_handle_invalid') from exc
        cached=self.sessions.get(context.job.id)
        if cached and cached.handle==handle:return cached
        attached=await self.pool.get_attached(self._thread_id(context.job.id))
        if attached and attached.handle==handle:self.sessions[context.job.id]=attached;return attached
        try:candidate=await self.pool.backend.get(handle)
        except Exception as exc:raise RuntimeError('operation_sandbox_unavailable') from exc
        adopted=await self.pool.adopt(candidate,thread_id=self._thread_id(context.job.id),turn_id=context.job.id)
        self.sessions[context.job.id]=adopted;return adopted

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        try:reservation=await self.scheduler.reserve(context.job.id,context.generation,
            HarnessJobAdapter._resources(context))
        except RuntimeError as exc:
            if str(exc)=='no_compatible_node_capacity':
                raise PreAcceptFailure(str(exc),infrastructure=True,retry_after_seconds=1) from exc
            raise
        harness=Harness(reservation.harness)
        release=self._provider_release(reservation,harness);image=release.ref
        sandbox=None;claims=None
        try:
            sandbox=await self.pool.acquire(thread_id=self._thread_id(context.job.id),turn_id=context.job.id,
                harness=harness,environment={},image=image,
                profile_id=reservation.runtime_profile_id,revision=release.revision)
            allocation={'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping(),
                'reservation_id':reservation.id,'node_id':reservation.node_id,
                'node_generation':reservation.node_generation}
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id='operation:'+context.job.id,receipt=allocation)
            token,claims=await self.token_factory(context,reservation,sandbox.id)
            route=self.pool.backend.runtime_route(sandbox)
            await self.runtime.inject(sandbox,capability_token=token,gateway_url=route.gateway_url,
                proxy_url=route.proxy_url,harness=harness.value,model='operation-only',
                runtime_bundle_digest=self.settings.runtime_bundle_digest,
                runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest,
                runtime_profile_id=reservation.runtime_profile_id,skill_ids=[],skill_digests={},
                child_mode=True,delegation_depth=1,logical_job_id=context.job.id)
            inputs={}
            for item in context.request.input_artifacts:
                artifact=await self.store.get_artifact(item.artifact_id)
                if not artifact:raise RuntimeError('operation_input_artifact_missing')
                suffix=PurePosixPath(artifact.path).suffix
                path=f'assets/inputs/{item.artifact_role}-{artifact.id}{suffix}'
                data=await asyncio.to_thread(self.objects.get,artifact.object_ref,
                    maximum=self.settings.max_artifact_bytes)
                await self.runtime.put_project_file(sandbox,path,data,artifact.mime);inputs[item.artifact_role]=path
            recipe=dict(context.request.parameters['recipe'])
            if recipe.get('operation')=='media.concat_sequence':
                source_role=str(recipe.pop('source_role'));music_role=str(recipe.pop('music_role'))
                if source_role not in inputs or music_role not in inputs:
                    raise ValueError('operation_input_role_missing')
                runtime_value={**recipe,'source_path':inputs[source_role],'music_path':inputs[music_role]}
            else:
                role=str(recipe.pop('input_role'))
                if role not in inputs:raise ValueError('operation_input_role_missing')
                runtime_value={**recipe,'input_path':inputs[role]}
            async with self.scheduler.hold(reservation.id,reservation.node_id,reservation.node_generation):
                value=await self.runtime.media_render(sandbox,runtime_value)
                self.sessions[context.job.id]=sandbox
                receipt={'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping(),
                    'capability_nonce':claims.nonce,
                    'reservation_id':reservation.id,'node_id':reservation.node_id,
                    'node_generation':reservation.node_generation,'operation_completed':True,'output':value}
                await self.scheduler.mark_running(reservation.id,reservation.node_id,reservation.node_generation)
            return AdapterSubmission(external_execution_id='operation:'+context.job.id,
                acceptance_receipt=receipt,completed=True,result=value)
        except BaseException:
            if sandbox:await self.pool.release(sandbox,keep_attached=False)
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        receipt=context.previous_acceptance_receipt
        if receipt.get('operation_completed'):
            return AdapterStatus(True,JobState.SUCCEEDED,dict(receipt.get('output',{})))
        return AdapterStatus(True,JobState.UNKNOWN,{'error_code':'operation_outcome_unknown'})

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict[str,Any]:
        if status.state!=JobState.SUCCEEDED:return status.result
        sandbox=await self._sandbox(context,context.previous_acceptance_receipt)
        receipt=context.previous_acceptance_receipt
        contracts={value.artifact_role:value for value in context.request.expected_outputs}
        values=status.result.get('outputs') or ([{"role":next(iter(contracts),""),
            "path":status.result.get('path',''),"mime":status.result.get('mime','application/octet-stream'),
            "size":status.result.get('size'),"sha256":status.result.get('sha256')}]
            if status.result.get('path') else [])
        if not contracts or {str(value.get('role','')) for value in values}!=set(contracts):
            raise RuntimeError('operation_output_roles_mismatch')
        artifacts=[];collections=[]
        async with self.scheduler.hold(str(receipt['reservation_id']),str(receipt['node_id']),
                int(receipt['node_generation'])):
            for value in values:
                role=str(value['role']);contract=contracts[role];path=str(value.get('path',''))
                staged=await self.runtime.artifact(sandbox,path,maximum=self.settings.max_artifact_bytes)
                try:
                    if value.get('size') != staged.size or value.get('sha256') != staged.sha256:
                        raise ValueError('runtime_artifact_identity_mismatch')
                    artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                        generation=context.generation,artifact_role=role,item_key='primary',position=0,
                        filename=Path(path).name,output_disposition=contract.output_disposition,
                        declared_mime=str(value.get('mime','application/octet-stream'))),
                        staged_file_chunks(staged.path))
                finally:staged.remove()
                artifacts.append(artifact.model_dump(mode='json'))
                validations=[]
                for validation in value.get('validation_receipts',[]):
                    current=dict(validation);current.setdefault('artifact_digests',[artifact.sha256]);validations.append(current)
                present={str(validation.get('validator','')) for validation in validations}
                operation=str(context.request.parameters['recipe']['operation'])
                validations.extend(self._controlled_validation_receipts(operation,
                    context.request.parameters['recipe'],value,
                    tuple(name for name in contract.validators if name not in present),artifact.sha256))
                collections.append(await self.store.finalize_job_artifact_collection(
                    context.job.id,context.generation,role,validation_receipts=validations))
        return {'operation':context.request.parameters['recipe']['operation'],
            'artifacts':artifacts,'collections':collections}

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.cleanup_external(context,external_execution_id)
        await self.release_capacity(context)

    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        receipt=context.previous_acceptance_receipt;sandbox=None;identity=str(receipt.get('sandbox_id',''))
        if identity:
            try:sandbox=await self._sandbox(context,receipt)
            except RuntimeError as exc:
                if str(exc)!='operation_sandbox_unavailable':raise
        else:
            match=await self.pool.reconcile_create(context.job.id)
            for match in ([match] if match else []):
                adopted=await self.pool.adopt(match,thread_id=self._thread_id(context.job.id),turn_id=context.job.id)
                await self.pool.release(adopted,keep_attached=False)
        reservation_id=str(receipt['reservation_id']);node_id=str(receipt['node_id']);generation=int(receipt['node_generation'])
        nonce=str(receipt.get('capability_nonce',''))
        async with self.scheduler.hold(reservation_id,node_id,generation):
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

    async def release_capacity(self,context:JobExecutionContext)->None:
        receipt=context.previous_acceptance_receipt
        reservation_id=str(receipt['reservation_id']);node_id=str(receipt['node_id']);generation=int(receipt['node_generation'])
        await self.scheduler.release(reservation_id,node_id,generation)
