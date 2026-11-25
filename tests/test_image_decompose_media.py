import asyncio
import io

import httpx
import pytest
from PIL import Image

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.image_decompose_adapter import ImageDecomposeAdapter
from sandbox_harness.job_adapters import JobExecutionContext
from sandbox_harness.job_contracts import JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobRequest
from sandbox_harness.objects import ObjectStore
from sandbox_harness.store import Store


def png():
    stream=io.BytesIO();Image.new('RGB',(32,32),'blue').save(stream,format='PNG')
    return stream.getvalue()


async def setup(tmp_path,mime_patterns=(),*,database_url=None,max_item_bytes=128*1024**2):
    settings=Settings(backend='fake',data_root=tmp_path/'data',
        database_url=database_url or 'sqlite+aiosqlite:///'+str(tmp_path/'media.db'))
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        title='decompose-media'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='decompose',
        idempotency_key='decompose-origin'),profile,'owner')
    source=png();reference,digest=objects.put(source)
    artifact=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(source),
        mime='image/png',object_ref=reference)
    request=JobRequest(local_job_id='decompose',kind='operation',adapter='image.decompose',
        descriptor_id='image.decompose',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='decompose-request',parameters={'layer_count':3,'resolution':640},
        input_artifacts=(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),),
        expected_outputs=(JobExpectedOutput(artifact_role='layer_package',output_disposition='delivery',
            min_items=2,max_items=30,mime_patterns=mime_patterns,max_item_bytes=max_item_bytes),),
        cancellation_policy={'supported':True},reconciliation_policy={'supported':True})
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='decompose-group',jobs=[request]))
    attempt=await store.claim_job(jobs[0].id,'worker','node',60)
    await store.mark_job_submission_started(jobs[0].id,'worker',attempt.generation)
    job=await store.get_job(jobs[0].id)
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    return settings,store,objects,context


async def test_decompose_submission_queues_media_job_without_provider_http(tmp_path,monkeypatch):
    settings,store,objects,context=await setup(tmp_path)
    requests=[];clients=[];client_type=httpx.AsyncClient
    def response(request):
        requests.append(request.method)
        return httpx.Response(202,json={'id':'provider-1'})
    def client(*args,**kwargs):
        kwargs['transport']=httpx.MockTransport(response)
        value=client_type(*args,**kwargs);clients.append(value);return value
    monkeypatch.setattr(httpx,'AsyncClient',client)
    try:
        adapter=ImageDecomposeAdapter(settings,store,objects)
        submission=await adapter.submit(context)
        assert requests==[], 'JobWorker still invokes the external provider directly'
        media=await store.get_media_job(submission.external_execution_id,'owner')
        assert media and media.status=='created' and media.provider=='local-qwen-layered'
        recovery=await adapter.recover_submission(context)
        assert recovery.submission.external_execution_id==media.id
        assert await store.list_media_jobs(context.job.thread_id,'owner')==[]
    finally:
        await asyncio.gather(*(value.aclose() for value in clients));await store.close()


async def test_media_worker_owns_decompose_execution_and_multiple_outputs(tmp_path,monkeypatch):
    from sandbox_harness.media_worker import MediaWorker
    from sandbox_harness.job_contracts import JobState
    settings,store,objects,context=await setup(tmp_path)
    calls=[];client_type=httpx.AsyncClient
    cookie=settings.data_root/'image-decompose'/'cookie.txt'
    cookie.parent.mkdir(parents=True,exist_ok=True);cookie.write_text('session=test')
    def handler(request):
        calls.append((request.method,request.url.path))
        assert request.headers['cookie']=='session=test'
        if request.method=='POST' and request.url.path=='/api/jobs':
            assert b'source_image' in request.content
            return httpx.Response(202,json={'id':'remote-1'})
        if request.url.path=='/api/jobs/remote-1':
            return httpx.Response(200,json={'status':'succeeded','artifacts':[
                {'id':'one','name':'one.png','media_type':'image/png'},
                {'id':'two','name':'two.png','media_type':'image/png'}]})
        if '/artifacts/' in request.url.path:return httpx.Response(200,content=png())
        raise AssertionError(request.url.path)
    def client(*args,**kwargs):
        kwargs['transport']=httpx.MockTransport(handler);return client_type(*args,**kwargs)
    monkeypatch.setattr(httpx,'AsyncClient',client)
    try:
        adapter=ImageDecomposeAdapter(settings,store,objects)
        submission=await adapter.submit(context)
        worker=MediaWorker(settings,store,objects,None)
        job=(await store.claim_media_jobs(worker.owner))[0]
        await worker.process(job)
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='succeeded'
        assert calls.count(('POST','/api/jobs'))==1
        status=await adapter.status(context,submission.external_execution_id)
        assert status.state==JobState.SUCCEEDED
        first=await adapter.collect(context,submission.external_execution_id,status)
        second=await adapter.collect(context,submission.external_execution_id,status)
        assert [(row['artifact_id'],row['position']) for row in first['artifacts']]==[
            (row['artifact_id'],row['position']) for row in second['artifacts']]
        assert [row['filename'] for row in first['artifacts']]==['one.png','two.png']
        assert [row['position'] for row in first['artifacts']]==[0,1]
        assert len(value.outputs)==2
    finally:await store.close()


async def queued_worker(tmp_path,handler,mime_patterns=(),*,max_item_bytes=128*1024**2):
    from sandbox_harness.image_decompose_provider import ImageDecomposeProvider
    from sandbox_harness.media_worker import MediaWorker
    settings,store,objects,context=await setup(tmp_path,mime_patterns,max_item_bytes=max_item_bytes)
    cookie=settings.data_root/'image-decompose'/'cookie.txt'
    cookie.parent.mkdir(parents=True,exist_ok=True);cookie.write_text('session=test')
    adapter=ImageDecomposeAdapter(settings,store,objects);submission=await adapter.submit(context)
    provider=ImageDecomposeProvider(settings,objects,transport=httpx.MockTransport(handler))
    worker=MediaWorker(settings,store,objects,None,external_providers={'image_layered_http':provider})
    return store,objects,context,adapter,submission,worker


async def test_cancel_before_media_claim_prevents_native_submission(tmp_path):
    calls=[]
    def handler(request):calls.append(request);raise AssertionError('no provider work permitted')
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await store.request_job_cancel(context.job.id,'owner')
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        assert (await store.get_media_job(submission.external_execution_id,'owner')).status=='cancelled'
        assert calls==[]
    finally:await store.close()


async def test_lost_create_response_never_adopts_or_resubmits(tmp_path):
    calls=[]
    def handler(request):
        calls.append((request.method,request.url.path))
        raise httpx.ReadError('response intentionally lost',request=request)
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='unknown' and value.error_code=='provider_submission_outcome_unknown'
        assert not await store.claim_media_jobs('replacement')
        assert (await adapter.recover_submission(context)).submission.external_execution_id==value.id
        assert calls==[('POST','/api/jobs')]
    finally:await store.close()


async def test_replacement_media_worker_polls_exact_accepted_handle(tmp_path):
    from sandbox_harness.media_worker import MediaWorker
    calls=[];interrupted=True
    def handler(request):
        nonlocal interrupted
        calls.append((request.method,request.url.path))
        if request.method=='POST':return httpx.Response(202,json={'id':'accepted-1'})
        if interrupted:
            interrupted=False;raise asyncio.CancelledError()
        if '/artifacts/' in request.url.path:return httpx.Response(200,content=png())
        assert request.url.path=='/api/jobs/accepted-1'
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':'a','name':'a.png','media_type':'image/png'},
            {'id':'b','name':'b.png','media_type':'image/png'}]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        with pytest.raises(asyncio.CancelledError):
            await worker.process((await store.claim_media_jobs(worker.owner))[0])
        await store.release_media_job_leases(worker.owner)
        replacement=MediaWorker(worker.settings,store,objects,None,external_providers=worker.external_providers)
        job=(await store.claim_media_jobs(replacement.owner))[0]
        assert job['provider_job_id']=='accepted-1' and job['generation']==2
        await replacement.process(job)
        assert (await store.get_media_job(submission.external_execution_id,'owner')).status=='succeeded'
        assert calls.count(('POST','/api/jobs'))==1
    finally:await store.close()


@pytest.mark.parametrize('boundary',['submit','download'])
async def test_source_cancel_wins_during_external_io(tmp_path,boundary):
    calls=[]
    async def handler(request):
        calls.append((request.method,request.url.path))
        if request.url.path.endswith('/cancel'):
            return httpx.Response(200,json={'status':'cancelled'})
        if request.method=='POST':
            if boundary=='submit':await store.request_job_cancel(context.job.id,'owner')
            return httpx.Response(202,json={'id':'accepted-1'})
        if '/artifacts/' in request.url.path:
            if boundary=='download':await store.request_job_cancel(context.job.id,'owner')
            return httpx.Response(200,content=png())
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':'a','name':'a.png','media_type':'image/png'}]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='cancelled' and value.outputs==[]
        assert calls.count(('POST','/api/jobs/accepted-1/cancel'))==1
        assert await store.list_job_artifacts(context.job.id)==[]
    finally:await store.close()


async def test_partial_download_retry_reuses_receipt_and_staged_bytes(tmp_path):
    calls=[];broken=True
    def handler(request):
        nonlocal broken
        calls.append((request.method,request.url.path))
        if request.method=='POST':return httpx.Response(202,json={'id':'accepted-1'})
        if request.url.path.endswith('/artifacts/b') and broken:
            broken=False;return httpx.Response(503)
        if '/artifacts/' in request.url.path:return httpx.Response(200,content=png())
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':'a','name':'a.png','media_type':'image/png'},
            {'id':'b','name':'b.png','media_type':'image/png'}]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='running' and value.phase=='provider_output_retry'
        await store.request_media_refresh(value.id,'owner')
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        assert (await store.get_media_job(value.id,'owner')).status=='succeeded'
        assert calls.count(('POST','/api/jobs'))==1
        assert calls.count(('GET','/api/jobs/accepted-1'))==1
        assert calls.count(('GET','/api/jobs/accepted-1/artifacts/a'))==1
        assert calls.count(('GET','/api/jobs/accepted-1/artifacts/b'))==2
    finally:await store.close()


async def test_fenced_enqueue_is_idempotent_and_rejects_stale_generation(tmp_path):
    from dataclasses import replace
    settings,store,objects,context=await setup(tmp_path)
    adapter=ImageDecomposeAdapter(settings,store,objects)
    try:
        values=await asyncio.gather(*(adapter.submit(context) for _ in range(8)))
        assert len({value.external_execution_id for value in values})==1
        with pytest.raises(RuntimeError,match='stale_job_generation'):
            await adapter.submit(replace(context,generation=context.generation+1))
        assert await store.list_media_jobs(context.job.thread_id,'owner')==[]
    finally:await store.close()


async def test_cancel_unknown_submission_preserves_ambiguity(tmp_path):
    calls=[]
    def handler(request):
        calls.append((request.method,request.url.path))
        raise httpx.ReadError('lost accepted response',request=request)
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        await store.request_media_cancel(submission.external_execution_id,'owner')
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='unknown'
        assert value.error_code=='provider_submission_outcome_unknown'
        assert calls==[('POST','/api/jobs')]
    finally:await store.close()


async def test_selected_formats_only_are_stored_and_positions_are_dense(tmp_path):
    import hashlib
    import zipfile
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as archive:
        archive.writestr('mimetype','image/openraster',compress_type=zipfile.ZIP_STORED)
        archive.writestr('stack.xml','<image w="32" h="32"><stack/></image>')
    payloads={'psd':('layers.psd','image/vnd.adobe.photoshop',b'8BPS\x00\x01'+b'\x00'*30),
        'png':('layer.png','image/png',png()),
        'ora':('layers.ora','application/octet-stream',stream.getvalue()),
        'tiff':('layers.tiff','image/tiff',b'II*\x00'+b'\x00'*32)}
    def handler(request):
        if request.method=='POST':return httpx.Response(202,json={'id':'accepted-1'})
        if '/artifacts/' in request.url.path:
            return httpx.Response(200,content=payloads[request.url.path.rsplit('/',1)[-1]][2])
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':key,'name':name,'media_type':mime} for key,(name,mime,data) in payloads.items()]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler,
        ('image/png','image/openraster','application/zip'))
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        status=await adapter.status(context,submission.external_execution_id)
        result=await adapter.collect(context,submission.external_execution_id,status)
        assert [row['filename'] for row in result['artifacts']]==['layer.png','layers.ora']
        assert [row['position'] for row in result['artifacts']]==[0,1]
        assert result['collections'][0]['state']=='published'
        for key in ('psd','tiff'):
            assert not objects._path(hashlib.sha256(payloads[key][2]).hexdigest()).exists()
    finally:await store.close()


@pytest.mark.parametrize('rejection',[409,429,'connect'])
async def test_definitive_rejection_retries_without_adopting_other_task(tmp_path,rejection):
    calls=[];rejected=False
    def handler(request):
        nonlocal rejected
        calls.append((request.method,request.url.path))
        if request.method=='POST':
            if not rejected:
                rejected=True
                if rejection=='connect':raise httpx.ConnectError('refused',request=request)
                return httpx.Response(rejection,json={'id':'someone-elses-job'})
            return httpx.Response(202,json={'id':'our-job'})
        assert request.url.path=='/api/jobs/our-job'
        return httpx.Response(200,json={'status':'running'})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        await store.request_media_refresh(submission.external_execution_id,'owner')
        job=(await store.claim_media_jobs(worker.owner))[0]
        assert job['provider_job_id']=='' and job['provider_started_at'] is None
        await worker.process(job)
        assert calls==[('POST','/api/jobs'),('POST','/api/jobs'),('GET','/api/jobs/our-job')]
    finally:await store.close()


async def test_legacy_import_requires_recorded_exact_handle(tmp_path):
    settings,store,objects,context=await setup(tmp_path)
    adapter=ImageDecomposeAdapter(settings,store,objects)
    try:
        with pytest.raises(RuntimeError,match='unrecorded_media_execution_identity'):
            await adapter.status(context,'unrelated-native-job')
        await store.record_job_acceptance(context.job.id,'worker',context.generation,
            external_execution_id='legacy-job',receipt={'external_id':'legacy-job'})
        await adapter.status(context,'legacy-job')
        job=(await store.claim_media_jobs('replacement'))[0]
        assert job['provider_job_id']=='legacy-job' and job['phase']=='provider_polling'
        with pytest.raises(RuntimeError,match='media_provider_identity_conflict'):
            await adapter.status(context,'unrelated-native-job')
    finally:await store.close()


@pytest.mark.parametrize('url',['http://10.0.0.8:8912','https://127.0.0.1:8912',
    'http://user:password@127.0.0.1:8912','http://127.0.0.1:8912/path'])
def test_transport_rejects_non_loopback_or_credentialled_configuration(tmp_path,url):
    from sandbox_harness.image_decompose_provider import ImageDecomposeProvider
    settings=Settings(backend='fake',data_root=tmp_path,image_decompose_base_url=url)
    with pytest.raises(ValueError,match='loopback'):ImageDecomposeProvider(settings,None)


async def test_media_link_schema_upgrade_preserves_old_rows_and_is_idempotent(tmp_path):
    from datetime import timedelta
    from sqlalchemy import text
    from sandbox_harness.domain import MediaDraftCreate,now
    from sandbox_harness.image_decompose_adapter import DECOMPOSE_SPEC
    settings,store,objects,context=await setup(tmp_path)
    try:
        draft,_=await store.create_media_draft('owner',context.job.thread_id,MediaDraftCreate(
            kind='image',model='legacy',parameters={},idempotency_key='old-media'),
            expires_at=now()+timedelta(hours=1),provider_spec=DECOMPOSE_SPEC)
        old=await store.confirm_media_draft(draft.id,'owner',provider_spec=DECOMPOSE_SPEC)
        async with store.engine.begin() as db:
            await db.execute(text('DROP INDEX ix_media_jobs_source_job_id'))
            await db.execute(text('ALTER TABLE media_jobs DROP COLUMN source_job_id'))
        await store.initialize();await store.initialize()
        assert (await store.get_media_job(old.id,'owner')).id==old.id
        value=await ImageDecomposeAdapter(settings,store,objects).submit(context)
        assert value.external_execution_id!=old.id
    finally:await store.close()


@pytest.mark.parametrize('already_finalized',[False,True])
async def test_legacy_partial_collection_keeps_original_artifact_identity(tmp_path,already_finalized):
    import hashlib
    from sandbox_harness.artifact_gateway import ArtifactEmission
    from sandbox_harness.image_decompose_provider import ImageDecomposeProvider
    from sandbox_harness.media_worker import MediaWorker
    settings,store,objects,context=await setup(tmp_path)
    adapter=ImageDecomposeAdapter(settings,store,objects)
    blue=png();stream=io.BytesIO();Image.new('RGB',(32,32),'red').save(stream,format='PNG')
    payloads={'a':blue,'b':stream.getvalue()};previous=[];calls=[]
    def handler(request):
        calls.append(request.method);assert request.method=='GET'
        if '/artifacts/' in request.url.path:return httpx.Response(200,content=payloads[request.url.path[-1]])
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':key,'name':key+'.png','media_type':'image/png'} for key in payloads]})
    try:
        await store.record_job_acceptance(context.job.id,'worker',context.generation,
            external_execution_id='legacy-job',receipt={'external_id':'legacy-job'})
        for position,(key,data) in enumerate(payloads.items()):
            if position and not already_finalized:break
            async def chunks():yield data
            value=await adapter.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                generation=context.generation,artifact_role='layer_package',
                item_key=hashlib.sha256(data).hexdigest()[:16],position=position,
                filename=key+'.png',output_disposition=context.request.expected_outputs[0].output_disposition,
                declared_mime='image/png'),chunks())
            previous.append(value.artifact_id)
        if already_finalized:
            await store.finalize_job_artifact_collection(context.job.id,context.generation,'layer_package')
        await adapter.status(context,'legacy-job')
        provider=ImageDecomposeProvider(settings,objects,transport=httpx.MockTransport(handler))
        worker=MediaWorker(settings,store,objects,None,external_providers={'image_layered_http':provider})
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        status=await adapter.status(context,'legacy-job')
        result=await adapter.collect(context,'legacy-job',status)
        assert [row['artifact_id'] for row in result['artifacts']][:len(previous)]==previous
        assert len(await store.list_job_artifacts(context.job.id))==2
        assert calls==['GET','GET','GET']
    finally:await store.close()


async def test_unselected_large_format_does_not_apply_selected_item_limit(tmp_path):
    payloads={'a':('image/png',png()),'b':('image/png',png()),
        'psd':('image/vnd.adobe.photoshop',b'8BPS'+b'\x00'*508)}
    def handler(request):
        if request.method=='POST':return httpx.Response(202,json={'id':'accepted-1'})
        if '/artifacts/' in request.url.path:
            return httpx.Response(200,content=payloads[request.url.path.rsplit('/',1)[-1]][1])
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':key,'name':key+'.bin','media_type':mime} for key,(mime,data) in payloads.items()]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler,
        ('image/png',),max_item_bytes=128)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        value=await store.get_media_job(submission.external_execution_id,'owner')
        assert value.status=='succeeded' and len(value.outputs)==2
    finally:await store.close()


async def test_delegated_media_cannot_bypass_job_publication(tmp_path):
    def handler(request):
        if request.method=='POST':return httpx.Response(202,json={'id':'accepted-1'})
        if '/artifacts/' in request.url.path:return httpx.Response(200,content=png())
        return httpx.Response(200,json={'status':'succeeded','artifacts':[
            {'id':'a','name':'a.png','media_type':'image/png'},
            {'id':'b','name':'b.png','media_type':'image/png'}]})
    store,objects,context,adapter,submission,worker=await queued_worker(tmp_path,handler)
    try:
        await worker.process((await store.claim_media_jobs(worker.owner))[0])
        assert await store.media_job_output_for_thread(submission.external_execution_id,
            context.job.thread_id) is None
        with pytest.raises(KeyError,match='media_job_not_found'):
            await store.promote_media_job_for_thread(submission.external_execution_id,
                context.job.thread_id,'delivery')
        assert await store.list_media_jobs(context.job.thread_id,'owner')==[]
        assert await store.list_media_drafts(context.job.thread_id,'owner')==[]
    finally:await store.close()


async def test_handoff_rejects_artifact_from_another_owner_and_thread(tmp_path):
    settings,store,objects,context=await setup(tmp_path)
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        foreign=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            title='foreign source'),profile,'different-owner')
        turn,_,_=await store.create_turn(foreign.id,TurnCreate(message='foreign',
            idempotency_key='foreign-origin'),profile,'different-owner')
        data=png();reference,digest=objects.put(data)
        artifact=await store.add_artifact(turn.id,path='foreign.png',sha256=digest,
            size=len(data),mime='image/png',object_ref=reference)
        request=context.request.model_copy(update={'idempotency_key':'foreign-binding-request',
            'input_artifacts':(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),)})
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=context.job.thread_id,
            origin_turn_id=context.job.origin_turn_id,idempotency_key='foreign-binding-group',jobs=(request,)))
        attempt=await store.claim_job(jobs[0].id,'worker','node',60)
        await store.mark_job_submission_started(jobs[0].id,'worker',attempt.generation)
        scope=JobExecutionContext(job=await store.get_job(jobs[0].id),request=request,generation=attempt.generation)
        with pytest.raises(RuntimeError,match='job_input_artifact_outside_scope'):
            await ImageDecomposeAdapter(settings,store,objects).submit(scope)
        assert await store.media_execution_for_job(jobs[0].id,'owner') is None
        assert (await store.get_artifact_for_owner(artifact.id,'different-owner')).id==artifact.id
    finally:await store.close()
