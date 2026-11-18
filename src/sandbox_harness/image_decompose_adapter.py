from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

import httpx

from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .harness_job import staged_file_chunks
from .job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext,
    PreAcceptFailure,SubmissionOutcomeUnknown,SubmissionRecovery)
from .job_contracts import JobState


class ImageDecomposeAdapter(BaseJobAdapter):
    def __init__(self,settings,store,objects,*,transport=None):
        parsed=urlsplit(settings.image_decompose_base_url)
        if parsed.scheme!='http' or parsed.hostname not in {'127.0.0.1','::1'} or parsed.path not in {'','/'}:
            raise ValueError('image_decompose_base_url_must_be_loopback')
        self.settings=settings;self.store=store;self.objects=objects
        self.root=(settings.data_root/'image-decompose').resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.receipts=self.root/'receipts';self.receipts.mkdir(exist_ok=True)
        self.jobs=self.root/'jobs';self.jobs.mkdir(exist_ok=True)
        cookie=self.root/'cookie.txt';headers={}
        if cookie.is_file():headers['Cookie']=cookie.read_text().strip()
        self.cookie_path=cookie;self.client=httpx.AsyncClient(base_url=settings.image_decompose_base_url,
            headers=headers,timeout=120,trust_env=False,transport=transport)
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'image-decompose-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    def _receipt(self,job_id:str)->Path:return self.receipts/(job_id+'.json')
    def _work(self,job_id:str)->Path:
        if not re.fullmatch(r'[0-9a-f]{32}',job_id):raise ValueError('invalid_decompose_job_id')
        return self.jobs/job_id

    def _persist_cookie(self)->None:
        values=[f'{key}={value}' for key,value in self.client.cookies.items()]
        if values:
            temporary=self.cookie_path.with_suffix('.tmp');temporary.write_text('; '.join(values))
            temporary.chmod(0o600);temporary.replace(self.cookie_path)

    def _write_receipt(self,job_id:str,external_id:str)->None:
        temporary=self._receipt(job_id).with_suffix('.tmp')
        temporary.write_text(json.dumps({'external_id':external_id},separators=(',',':')))
        temporary.chmod(0o600);temporary.replace(self._receipt(job_id))

    async def _active(self)->list[str]:
        try:response=await self.client.get('/api/jobs');response.raise_for_status();value=response.json()
        except (httpx.HTTPError,ValueError):return []
        return [str(row.get('id')) for row in value.get('jobs',[]) if row.get('status') not in {
            'succeeded','failed','cancelled'} and re.fullmatch(
                r'[A-Za-z0-9_-]{1,100}',str(row.get('id') or ''))]

    async def _source(self,context:JobExecutionContext):
        bindings=[row for row in context.request.input_artifacts if row.artifact_role=='source']
        if len(bindings)!=1:raise PreAcceptFailure('decompose_source_binding_invalid',
            error_code='provider_input_invalid',retryable=False)
        artifact=await self.store.get_artifact(bindings[0].artifact_id)
        if not artifact or not artifact.mime.startswith('image/'):
            raise PreAcceptFailure('decompose_source_artifact_invalid',
                error_code='provider_input_invalid',retryable=False)
        data=await asyncio.to_thread(self.objects.get,artifact.object_ref,maximum=64*1024*1024)
        return artifact,data

    @staticmethod
    def _parameters(value:dict)->dict[str,str]:
        layer_count=int(value.get('layer_count',4));resolution=int(value.get('resolution',640))
        quality=str(value.get('quality','balanced'));seed=int(value.get('seed',777))
        if layer_count not in {3,4,6,8} or resolution not in {640,1024}:
            raise PreAcceptFailure('decompose_shape_parameters_invalid',
                error_code='provider_input_invalid',retryable=False)
        if quality not in {'balanced','fine','original'} or not 0<=seed<=2**31-1:
            raise PreAcceptFailure('decompose_quality_parameters_invalid',
                error_code='provider_input_invalid',retryable=False)
        return {'prompt':str(value.get('prompt',''))[:2000],'layer_count':str(layer_count),
            'resolution':str(resolution),'quality':quality,'seed':str(seed),
            'reconstruction_guard':str(bool(value.get('reconstruction_guard',True))).lower()}

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        artifact,data=await self._source(context);parameters=self._parameters(context.request.parameters)
        try:response=await self.client.post('/api/jobs',data=parameters,files={
            'source_image':(Path(artifact.path).name,data,artifact.mime)})
        except (httpx.TimeoutException,httpx.TransportError) as exc:
            raise SubmissionOutcomeUnknown('image_decompose_submission_outcome_unknown',
                acceptance_receipt={'request_digest':context.request.request_digest}) from exc
        self._persist_cookie()
        try:value=response.json()
        except ValueError:value={}
        if response.status_code==409:
            active=await self._active()
            if len(active)==1:
                self._write_receipt(context.job.id,active[0]);return AdapterSubmission(active[0],{
                    'external_id':active[0],'adopted_after_conflict':True})
        if response.status_code in {401,403}:raise PreAcceptFailure('image_decompose_authentication_failed',
            error_code='provider_authentication_failed',retryable=False)
        if response.status_code==429:raise PreAcceptFailure('image_decompose_rate_limited',
            error_code='provider_rate_limited',retry_after_seconds=5,retryable=True)
        if response.status_code>=500:raise SubmissionOutcomeUnknown(
            'image_decompose_service_unavailable',acceptance_receipt={'status_code':response.status_code})
        if response.status_code!=202:raise PreAcceptFailure('image_decompose_request_rejected',
            error_code='provider_input_invalid',retryable=False)
        external_id=str(value.get('id') or '')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',external_id):
            raise SubmissionOutcomeUnknown('image_decompose_external_id_invalid')
        self._write_receipt(context.job.id,external_id)
        return AdapterSubmission(external_id,{'external_id':external_id,'provider':'local-qwen-layered'})

    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        receipt=self._receipt(context.job.id)
        if receipt.is_file():
            external_id=str(json.loads(receipt.read_text()).get('external_id') or '')
            if external_id:return SubmissionRecovery(submission=AdapterSubmission(external_id,{
                'external_id':external_id,'recovered':True}))
        active=await self._active()
        if len(active)==1:
            self._write_receipt(context.job.id,active[0]);return SubmissionRecovery(
                submission=AdapterSubmission(active[0],{'external_id':active[0],'recovered':True}))
        return SubmissionRecovery(safe_to_retry=not active)

    async def _download(self,context:JobExecutionContext,external_id:str,value:dict)->dict:
        work=self._work(context.job.id);work.mkdir(parents=True,exist_ok=True);rows=[];total=0
        for position,item in enumerate((value.get('artifacts') or [])[:30]):
            identity=str(item.get('id') or '')
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',identity):continue
            response=await self.client.get(f'/api/jobs/{external_id}/artifacts/{identity}')
            if not response.is_success:raise RuntimeError('provider_output_unavailable')
            data=response.content;total+=len(data)
            if len(data)>64*1024**2 or total>128*1024**2:raise RuntimeError('provider_output_too_large')
            name=Path(str(item.get('name') or identity)).name
            if not name or (work/name).exists():name=f'{position:02d}-{identity}.bin'
            path=work/name;path.write_bytes(data);rows.append({'path':name,'position':position,
                'mime':str(item.get('media_type') or response.headers.get('content-type') or
                    'application/octet-stream').split(';',1)[0],
                'sha256':hashlib.sha256(data).hexdigest(),'size':len(data)})
        if not rows:raise RuntimeError('provider_output_unavailable')
        manifest={'external_id':external_id,'outputs':rows};(work/'result.json').write_text(
            json.dumps(manifest,sort_keys=True));return manifest

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        try:response=await self.client.get('/api/jobs/'+external_execution_id)
        except (httpx.TimeoutException,httpx.TransportError):return AdapterStatus(False,
            JobState.WAITING_EXTERNAL,{'error_code':'provider_poll_unavailable'})
        if response.status_code==404:return AdapterStatus(True,JobState.UNKNOWN,{
            'error_code':'provider_result_unresolved'})
        if response.status_code>=500:return AdapterStatus(False,JobState.WAITING_EXTERNAL,{
            'error_code':'provider_service_unavailable'})
        if not response.is_success:return AdapterStatus(True,JobState.FAILED,{
            'error_code':'provider_processing_failed','error_detail':'status_request_rejected'})
        value=response.json();state=str(value.get('status') or '').lower()
        if state in {'queued','running'}:return AdapterStatus(False,JobState.WAITING_EXTERNAL,{
            'provider_state':state})
        if state=='cancelled':return AdapterStatus(True,JobState.CANCELLED,{'external_id':external_execution_id})
        if state=='failed':return AdapterStatus(True,JobState.FAILED,{
            'error_code':'provider_processing_failed',
            'error_detail':str(value.get('error_code') or 'remote_task_failed')[:200]})
        if state!='succeeded':return AdapterStatus(True,JobState.UNKNOWN,{
            'error_code':'provider_result_unresolved'})
        manifest=self._work(context.job.id)/'result.json'
        result=(json.loads(manifest.read_text()) if manifest.is_file()
            else await self._download(context,external_execution_id,value))
        return AdapterStatus(True,JobState.SUCCEEDED,result)

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        try:response=await self.client.post(f'/api/jobs/{external_execution_id}/cancel')
        except (httpx.TimeoutException,httpx.TransportError):return AdapterStatus(True,JobState.UNKNOWN,{
            'error_code':'provider_cancel_outcome_unknown'})
        return (AdapterStatus(True,JobState.CANCELLED,{'external_id':external_execution_id})
            if response.is_success else AdapterStatus(True,JobState.UNKNOWN,{
                'error_code':'provider_cancel_outcome_unknown'}))

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict:
        if status.state!=JobState.SUCCEEDED:return status.result
        if len(context.request.expected_outputs)!=1:
            raise RuntimeError('image_decompose_requires_one_output_collection')
        contract=context.request.expected_outputs[0];artifacts=[]
        for row in status.result['outputs']:
            path=self._work(context.job.id)/row['path']
            artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                generation=context.generation,artifact_role=contract.artifact_role,
                item_key=row['sha256'][:16],position=row['position'],filename=path.name,
                output_disposition=contract.output_disposition,
                declared_mime=row['mime']),staged_file_chunks(path))
            artifacts.append(artifact.model_dump(mode='json'))
        collection=await self.store.finalize_job_artifact_collection(context.job.id,
            context.generation,contract.artifact_role)
        return {'external_id':external_execution_id,'artifacts':artifacts,'collections':[collection]}

    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        __import__('shutil').rmtree(self._work(context.job.id),ignore_errors=True)
