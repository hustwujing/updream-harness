from __future__ import annotations

import asyncio
from pathlib import Path,PurePosixPath
from typing import Any

import httpx

from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .domain import Harness
from .harness_job import HarnessJobAdapter
from .job_adapters import AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext
from .job_contracts import JobState


class MediaRenderOperationAdapter(BaseJobAdapter):
    def __init__(self,*,scheduler,pool,runtime,store,objects,settings,token_factory):
        self.scheduler=scheduler;self.pool=pool;self.runtime=runtime;self.store=store
        self.objects=objects;self.settings=settings;self.token_factory=token_factory;self.sessions={}
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'operation-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    @staticmethod
    def _thread_id(job_id):return 'operation-job-'+job_id

    async def _sandbox(self,context,receipt):
        identity=str(receipt.get('sandbox_id',''));cached=self.sessions.get(context.job.id)
        if cached and cached.id==identity:return cached
        attached=await self.pool.get_attached(self._thread_id(context.job.id))
        if attached and attached.id==identity:self.sessions[context.job.id]=attached;return attached
        matches=[value for value in await self.pool.backend.list_namespace() if value.id==identity]
        if len(matches)!=1:raise RuntimeError('operation_sandbox_unavailable')
        adopted=await self.pool.adopt(matches[0],thread_id=self._thread_id(context.job.id),turn_id=context.job.id)
        self.sessions[context.job.id]=adopted;return adopted

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        reservation=await self.scheduler.reserve(context.job.id,context.generation,
            HarnessJobAdapter._resources(context));harness=Harness(reservation.harness)
        image=reservation.image_ref or self.settings.image_for(harness.value)
        if not image.endswith('@sha256:'+reservation.image_digest):
            raise RuntimeError('operation_image_digest_mismatch')
        sandbox=None;claims=None
        try:
            sandbox=await self.pool.acquire(thread_id=self._thread_id(context.job.id),turn_id=context.job.id,
                harness=harness,environment={},image=image)
            token,claims=await self.token_factory(context,reservation,sandbox.id)
            await self.runtime.inject(sandbox,capability_token=token,gateway_url=self.settings.gateway_url,
                proxy_url=self.settings.public_proxy_url,harness=harness.value,model='operation-only',
                runtime_bundle_digest=self.settings.runtime_bundle_digest,
                runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest,
                runtime_profile_id=reservation.runtime_profile_id,skill_ids=[],skill_digests={},
                child_mode=True,delegation_depth=1)
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
            value=await self.runtime.media_render(sandbox,runtime_value)
            self.sessions[context.job.id]=sandbox
            receipt={'sandbox_id':sandbox.id,'capability_nonce':claims.nonce,
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
        contracts={value.artifact_role:value for value in context.request.expected_outputs}
        values=status.result.get('outputs') or ([{"role":next(iter(contracts),""),
            "path":status.result.get('path',''),"mime":status.result.get('mime','application/octet-stream')}]
            if status.result.get('path') else [])
        if not contracts or {str(value.get('role','')) for value in values}!=set(contracts):
            raise RuntimeError('operation_output_roles_mismatch')
        artifacts=[];collections=[]
        for value in values:
            role=str(value['role']);contract=contracts[role];path=str(value.get('path',''))
            data=await self.runtime.artifact(sandbox,path)
            async def chunks(data=data):yield data
            artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                generation=context.generation,artifact_role=role,item_key='primary',position=0,
                filename=Path(path).name,output_disposition=contract.output_disposition,
                declared_mime=str(value.get('mime','application/octet-stream'))),chunks())
            artifacts.append(artifact.model_dump(mode='json'))
            receipts=[]
            for receipt in value.get('validation_receipts',[]):
                current=dict(receipt);current.setdefault('artifact_digests',[artifact.sha256]);receipts.append(current)
            collections.append(await self.store.finalize_job_artifact_collection(
                context.job.id,context.generation,role,validation_receipts=receipts))
        return {'operation':context.request.parameters['recipe']['operation'],
            'artifacts':artifacts,'collections':collections}

    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        receipt=context.previous_acceptance_receipt;sandbox=await self._sandbox(context,receipt)
        nonce=str(receipt.get('capability_nonce',''))
        if nonce:
            async with httpx.AsyncClient(timeout=10,trust_env=False) as client:
                response=await client.post(self.settings.gateway_url.rstrip('/')+'/internal/revoke/'+nonce,
                    headers={'X-Sandbox-Harness-Control':self.settings.gateway_control_key,
                        'X-Updream-Thread-Id':context.job.thread_id,
                        'X-Updream-Turn-Id':context.job.origin_turn_id,
                        'X-Updream-Sandbox-Id':sandbox.id})
                response.raise_for_status()
        await self.pool.release(sandbox,keep_attached=False);self.sessions.pop(context.job.id,None)
        await self.scheduler.release(str(receipt['reservation_id']),str(receipt['node_id']),
            int(receipt['node_generation']))
