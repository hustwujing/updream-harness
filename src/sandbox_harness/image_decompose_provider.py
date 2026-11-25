"""Authenticated HTTP transport for the independently deployed layering service."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit

import httpx

from .artifact_gateway import MimeRegistry
from .media_provider import DownloadedMedia, ProviderResult


class ImageDecomposeProvider:
    def __init__(self, settings, objects, *, transport=None):
        self.base_url=getattr(settings,'image_decompose_base_url','http://127.0.0.1:8912')
        parsed=urlsplit(self.base_url)
        if (parsed.scheme!='http' or parsed.hostname not in {'127.0.0.1','::1'}
                or parsed.path not in {'','/'} or parsed.username or parsed.password
                or parsed.query or parsed.fragment):
            raise ValueError('image_decompose_base_url_must_be_loopback')
        self.objects=objects;self.transport=transport
        self.root=Path(getattr(settings,'data_root',Path.cwd()/'.data'))/'image-decompose'
        self.cookie_path=self.root/'cookie.txt'

    def _client(self):
        headers={}
        if self.cookie_path.is_file():headers['Cookie']=self.cookie_path.read_text().strip()
        return httpx.AsyncClient(base_url=self.base_url,headers=headers,timeout=120,
            trust_env=False,follow_redirects=False,transport=self.transport)

    @staticmethod
    def _id(value):
        if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',value):
            raise ValueError('provider_identity_invalid')
        return value

    async def prepare(self,job):
        references=job.get('references') or []
        if (len(references)!=1 or references[0].get('role')!='source'
                or not references[0].get('mime','').startswith('image/')
                or not 0<references[0].get('size',0)<=64*1024**2):
            return ProviderResult('failed',error_code='provider_input_invalid')
        # Establish and durably retain the session BEFORE a create can be sent.
        # This lock coordinates first use across replacement/concurrent workers.
        self.root.mkdir(parents=True,exist_ok=True)
        with (self.root/'session.lock').open('a') as lock:
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                return ProviderResult('pending',error_code='provider_service_unavailable')
            if self.cookie_path.is_file() and self.cookie_path.read_text().strip():return None
            try:
                async with self._client() as client:
                    response=await client.get('/api/jobs');response.raise_for_status()
                    cookie='; '.join(f'{key}={value}' for key,value in client.cookies.items())
            except httpx.HTTPError:
                return ProviderResult('pending',error_code='provider_service_unavailable')
            if not cookie:return ProviderResult('failed',error_code='provider_authentication_failed')
            temporary=self.cookie_path.with_suffix('.tmp')
            with temporary.open('w') as stream:
                os.fchmod(stream.fileno(),0o600);stream.write(cookie);stream.flush();os.fsync(stream.fileno())
            temporary.replace(self.cookie_path)
        return None

    async def submit(self,job):
        source=job['references'][0]
        data=await asyncio.to_thread(self.objects.get,source['object_ref'],maximum=64*1024**2)
        if len(data)!=source['size'] or hashlib.sha256(data).hexdigest()!=source['sha256']:
            return ProviderResult('failed',error_code='provider_input_invalid')
        try:
            async with self._client() as client:
                response=await client.post('/api/jobs',data=job['parameters'],files={
                    'source_image':(source['filename'],data,source['mime'])})
        except httpx.ConnectError:
            return ProviderResult('pending',error_code='provider_service_unavailable')
        except httpx.HTTPError:
            return ProviderResult('unknown',error_code='provider_submission_outcome_unknown')
        # A native 409 rejects creation before allocating a job. Never adopt the
        # conflicting task: it can belong to a different source Operation.
        if response.status_code in {409,429}:
            return ProviderResult('pending',error_code='provider_rate_limited')
        if response.status_code in {401,403}:
            return ProviderResult('failed',error_code='provider_authentication_failed')
        if response.status_code>=500:
            return ProviderResult('unknown',error_code='provider_submission_outcome_unknown')
        if response.status_code!=202:
            return ProviderResult('failed',error_code='provider_input_invalid')
        try:identity=self._id(response.json().get('id'))
        except (ValueError,AttributeError):
            return ProviderResult('unknown',error_code='provider_submission_outcome_unknown')
        return ProviderResult('accepted',provider_job_id=identity)

    async def poll(self,job):
        identity=self._id(job['provider_job_id'])
        try:
            async with self._client() as client:response=await client.get('/api/jobs/'+identity)
        except httpx.HTTPError:return ProviderResult('pending',provider_job_id=identity,
            error_code='provider_poll_unavailable')
        if response.status_code in {408,429,500,502,503,504}:
            return ProviderResult('pending',provider_job_id=identity,error_code='provider_poll_unavailable')
        if not response.is_success:
            return ProviderResult('unknown',provider_job_id=identity,error_code='provider_result_unresolved')
        try:value=response.json();state=value.get('status')
        except (ValueError,AttributeError):
            return ProviderResult('unknown',provider_job_id=identity,error_code='provider_result_unresolved')
        if state in {'queued','running'}:
            return ProviderResult('pending',provider_job_id=identity)
        if state=='cancelled':return ProviderResult('cancelled',provider_job_id=identity)
        if state=='failed':return ProviderResult('failed',provider_job_id=identity,
            error_code='provider_processing_failed')
        if state!='succeeded':return ProviderResult('unknown',provider_job_id=identity,
            error_code='provider_result_unresolved')
        artifacts=value.get('artifacts')
        if not isinstance(artifacts,list) or not 1<=len(artifacts)<=30:
            return ProviderResult('failed',provider_job_id=identity,error_code='provider_result_invalid')
        outputs=[];names=set();identities=set()
        for position,item in enumerate(artifacts):
            artifact_id=self._id(item.get('id'))
            name=Path(str(item.get('name') or artifact_id)).name
            if not name or name in {'.','..'} or artifact_id in identities:
                raise ValueError('provider_output_identity_invalid')
            if name in names:name=f'{position:04d}-{name}'
            identities.add(artifact_id);names.add(name)
            outputs.append({'role':f'output_{position:04d}','filename':name,
                'artifact_id':artifact_id,'mime':str(item.get('media_type') or 'application/octet-stream')})
        return ProviderResult('success',provider_job_id=identity,outputs=outputs)

    async def cancel(self,job):
        identity=self._id(job['provider_job_id'])
        try:
            async with self._client() as client:
                response=await client.post(f'/api/jobs/{identity}/cancel')
                response.raise_for_status();state=response.json().get('status')
        except (httpx.HTTPError,ValueError,AttributeError):
            return ProviderResult('pending',provider_job_id=identity,error_code='provider_cancel_outcome_unknown')
        # The endpoint waits for its process group to exit. A finished native job
        # is also quiescent, and a winning platform cancellation forbids publication.
        if state in {'cancelled','failed','succeeded'}:
            return ProviderResult('cancelled',provider_job_id=identity)
        return ProviderResult('pending',provider_job_id=identity,error_code='provider_cancel_outcome_unknown')

    async def download(self,job,output,*,staging_root,prefix,maximum_bytes):
        identity=self._id(job['provider_job_id']);artifact_id=self._id(output['artifact_id'])
        staging_root.mkdir(parents=True,exist_ok=True)
        with NamedTemporaryFile(dir=staging_root,prefix=prefix,delete=False) as stream:
            path=Path(stream.name);digest=hashlib.sha256();size=0
            try:
                async with self._client() as client:
                    async with client.stream('GET',f'/api/jobs/{identity}/artifacts/{artifact_id}') as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_bytes():
                            size+=len(chunk)
                            if size>min(maximum_bytes,64*1024**2):raise ValueError('provider_output_too_large')
                            stream.write(chunk);digest.update(chunk)
                if not size:raise ValueError('provider_output_empty')
                stream.flush();os.fsync(stream.fileno())
                mime=MimeRegistry.detect(path,output['mime'])
                return DownloadedMedia(path,size,digest.hexdigest(),mime)
            except BaseException:
                path.unlink(missing_ok=True);raise
