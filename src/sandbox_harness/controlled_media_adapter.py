from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any,Awaitable,Callable

from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .harness_job import staged_file_chunks
from .job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext,
    PreAcceptFailure,SubmissionRecovery)
from .job_contracts import JobState
from .scheduler import ResourceRequest


Runner=Callable[[str,Path,Path,dict,tuple[str,...],Path],Awaitable[dict[str,Any]]]


class ControlledMediaOperationAdapter(BaseJobAdapter):
    ADAPTERS=frozenset({'image.segment','audio.transcribe'})

    def __init__(self,*,scheduler,store,objects,settings,runner:Runner|None=None,gpu_health=None):
        self.scheduler=scheduler;self.store=store;self.objects=objects;self.settings=settings
        self.runner=runner or self._default_runner
        self.gpu_health=gpu_health
        self.root=(settings.data_root/'controlled-operations').resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'controlled-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    async def _default_runner(self,operation,work,source,parameters,gpu_ids,runtime_root):
        from .controlled_media_runner import run_controlled_operation
        return await asyncio.to_thread(run_controlled_operation,operation,work,source,parameters,
            gpu_ids,runtime_root)

    def _work(self,job_id:str)->Path:
        if len(job_id)!=32 or any(value not in '0123456789abcdef' for value in job_id):
            raise ValueError('invalid_controlled_job_id')
        return self.root/job_id

    @staticmethod
    def _resources(context:JobExecutionContext,digest:str)->ResourceRequest:
        values=context.request.resource_requirements
        return ResourceRequest(child_profile_id='controlled-operation',
            runtime_profile_id='host-controlled',harness='codex',image_digest=digest,
            cpu=int(values.get('cpu',1)),memory_mb=int(values.get('memory_mb',512)),
            disk_mb=int(values.get('disk_mb',1024)),gpu_count=int(values.get('gpu',0)),
            gpu_type=str(values.get('gpu_type','')),gpu_mode=str(values.get('gpu_mode','')),
            gpu_memory_mb=int(values.get('gpu_memory_mb',0)))

    async def _source(self,context:JobExecutionContext,work:Path)->Path:
        bindings=[value for value in context.request.input_artifacts if value.artifact_role=='source']
        if len(bindings)!=1:raise PreAcceptFailure('controlled_source_binding_invalid')
        artifact=await self.store.get_artifact(bindings[0].artifact_id)
        if not artifact:raise PreAcceptFailure('controlled_source_artifact_missing')
        suffix=Path(artifact.path).suffix or '.bin';target=work/('source'+suffix)
        data=await asyncio.to_thread(self.objects.get,artifact.object_ref,
            maximum=self.settings.max_artifact_bytes)
        target.write_bytes(data);return target

    @staticmethod
    def _validated_result(work:Path,value:dict)->dict:
        rows=value.get('outputs')
        if not isinstance(rows,list) or not rows:raise RuntimeError('controlled_output_manifest_missing')
        result=[]
        for row in rows:
            relative=Path(str(row.get('path','')))
            if relative.name!=str(relative) or not relative.name:raise RuntimeError('controlled_output_path_invalid')
            path=work/relative
            if not path.is_file() or path.is_symlink():raise RuntimeError('controlled_output_missing')
            data=path.read_bytes();digest=hashlib.sha256(data).hexdigest()
            result.append({**row,'path':relative.name,'size':len(data),'sha256':digest})
        return {'outputs':result}

    def _existing(self,work:Path)->dict|None:
        manifest=work/'result.json'
        if not manifest.is_file():return None
        try:return self._validated_result(work,json.loads(manifest.read_text()))
        except (OSError,ValueError,RuntimeError,json.JSONDecodeError):return None

    @staticmethod
    def _receipt(reservation,result):
        return {'reservation_id':reservation.id,'node_id':reservation.node_id,
            'node_generation':reservation.node_generation,'runtime_profile_id':reservation.runtime_profile_id,
            'runtime_digest':reservation.image_digest,'gpu_ids':list(reservation.gpu_ids),
            'operation_completed':True,'result':result}

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        if context.request.adapter not in self.ADAPTERS:raise PreAcceptFailure('controlled_adapter_invalid')
        digest=str(self.settings.controlled_runtime_digest)
        try:reservation=await self.scheduler.reserve(context.job.id,context.generation,
            self._resources(context,digest))
        except RuntimeError as exc:
            if str(exc)=='no_compatible_node_capacity':
                raise PreAcceptFailure(str(exc),infrastructure=True,retry_after_seconds=1) from exc
            raise
        work=self._work(context.job.id);existing=self._existing(work)
        try:
            async with self.scheduler.hold(reservation.id,reservation.node_id,reservation.node_generation):
                if existing is None:
                    if work.exists():shutil.rmtree(work)
                    work.mkdir(parents=True,mode=0o700);source=await self._source(context,work)
                    try:
                        value=await self.runner(context.request.adapter,work,source,
                            dict(context.request.parameters),reservation.gpu_ids,
                            Path(self.settings.controlled_runtime_root))
                    except RuntimeError as exc:
                        if str(exc)=='no_selection':
                            raise PreAcceptFailure('No target matched the frozen selector',
                                error_code='no_target_found') from exc
                        from .controlled_media_runner import ControlledOperationError
                        if isinstance(exc,ControlledOperationError):
                            if exc.retry_class=='node_health_change' and self.gpu_health:
                                self.gpu_health.invalidate(reservation.gpu_ids)
                            raise PreAcceptFailure(str(exc),infrastructure=exc.infrastructure,
                                error_code=exc.error_code,failed_phase=exc.failed_phase,
                                retry_class=exc.retry_class,retryable=exc.retryable,
                                node_health_generation=int(getattr(self.gpu_health,'generation',0)),
                                retry_after_seconds=max(1,self.settings.job_node_heartbeat_seconds*2)) from exc
                        raise
                    result=self._validated_result(work,value)
                    temporary=work/'.result.json.tmp';temporary.write_text(json.dumps(result,sort_keys=True))
                    temporary.replace(work/'result.json')
                else:result=existing
                await self.scheduler.mark_running(reservation.id,reservation.node_id,reservation.node_generation)
        except BaseException:
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
            raise
        external_id='controlled:'+context.job.id
        return AdapterSubmission(external_execution_id=external_id,
            acceptance_receipt=self._receipt(reservation,result),completed=True,result=result)

    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        reservation=await self.scheduler.active_reservation(context.job.id)
        existing=self._existing(self._work(context.job.id))
        if reservation and existing:
            return SubmissionRecovery(submission=AdapterSubmission(
                external_execution_id='controlled:'+context.job.id,
                acceptance_receipt=self._receipt(reservation,existing),completed=True,result=existing))
        if reservation:
            await self.scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
        return SubmissionRecovery(safe_to_retry=True)

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        result=context.previous_acceptance_receipt.get('result') or self._existing(self._work(context.job.id))
        return (AdapterStatus(True,JobState.SUCCEEDED,result) if result else
            AdapterStatus(True,JobState.UNKNOWN,{'error_code':'controlled_result_unresolved'}))

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        result=self._existing(self._work(context.job.id))
        return (AdapterStatus(True,JobState.SUCCEEDED,result) if result else
            AdapterStatus(True,JobState.CANCELLED,{'cancelled':True}))

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict[str,Any]:
        if status.state!=JobState.SUCCEEDED:return status.result
        work=self._work(context.job.id);contracts={value.artifact_role:value
            for value in context.request.expected_outputs};values=status.result.get('outputs',[])
        values_by_role={str(row.get('role','')):row for row in values}
        if not contracts or not set(contracts).issubset(values_by_role):
            raise RuntimeError('controlled_output_roles_mismatch')
        artifacts=[];collections=[]
        for role,contract in contracts.items():
            value=values_by_role[role];path=work/str(value['path'])
            artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                generation=context.generation,artifact_role=role,item_key='primary',position=0,
                filename=path.name,output_disposition=contract.output_disposition,
                declared_mime=str(value.get('mime','application/octet-stream'))),staged_file_chunks(path))
            artifacts.append(artifact.model_dump(mode='json'));validations=[]
            for receipt in value.get('validation_receipts',[]):
                current=dict(receipt);current.setdefault('artifact_digests',[artifact.sha256]);validations.append(current)
            collections.append(await self.store.finalize_job_artifact_collection(context.job.id,
                context.generation,role,validation_receipts=validations))
        return {'operation':context.request.adapter,'artifacts':artifacts,'collections':collections}

    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        shutil.rmtree(self._work(context.job.id),ignore_errors=True)

    async def release_capacity(self,context:JobExecutionContext)->None:
        receipt=context.previous_acceptance_receipt
        reservation_id=str(receipt.get('reservation_id',''))
        if reservation_id:
            await self.scheduler.release(reservation_id,str(receipt.get('node_id','')),
                int(receipt.get('node_generation') or 0))
