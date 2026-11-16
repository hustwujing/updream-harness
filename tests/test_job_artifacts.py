import hashlib

import pytest

from sandbox_harness.artifact_gateway import ArtifactEmission, ArtifactGateway, MimeRegistry
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness,ItemType,ThreadCreate,TurnCreate
from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition
from sandbox_harness.objects import ObjectStore
from sandbox_harness.store import Store
from sandbox_harness.store import ProjectFileRow
from sqlalchemy import select


async def chunks(*values):
    for value in values:yield value


async def setup(tmp_path,output):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'artifacts.db'));await store.initialize()
    request=JobRequest(local_job_id='render',kind=JobKind.OPERATION,adapter='render',
        descriptor_id='render',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='artifact-render-key',parameters={},expected_outputs=[output])
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
        origin_turn_id='c'*32,idempotency_key='artifact-group-key',jobs=[request]))
    attempt=await store.claim_job(jobs[0].id,'worker','node',30)
    gateway=ArtifactGateway(store,ObjectStore(tmp_path/'objects'),tmp_path/'stage',maximum_bytes=1024*1024)
    return store,gateway,jobs[0],attempt


async def test_role_and_disposition_are_distinct_and_collection_publishes_atomically(tmp_path):
    output={'artifact_role':'generated_images','output_disposition':'delivery','min_items':2,'max_items':2,
        'mime_patterns':['image/*'],'validators':['image.verify']}
    store,gateway,job,attempt=await setup(tmp_path,output)
    try:
        png=b'\x89PNG\r\n\x1a\n'+b'x'*20
        for index in range(2):
            emitted=await gateway.emit(ArtifactEmission(job.id,attempt.generation,'generated_images',
                f'image-{index}',index,f'image-{index}.png',OutputDisposition.DELIVERY,'image/png'),chunks(png))
            assert emitted.artifact_role=='generated_images'
            assert emitted.output_disposition==OutputDisposition.CANDIDATE
        assert await store.current_project_files('b'*32,'owner')==[]
        result=await store.finalize_job_artifact_collection(job.id,attempt.generation,'generated_images',
            validation_receipts=[{'validator':'image.verify','status':'pass',
                'artifact_digests':[hashlib.sha256(png).hexdigest()]*2}])
        assert result['state']=='published' and result['output_disposition']=='delivery'
        assert result['manifest']['count']==2
        async with store.connections() as db:
            files=(await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id=='b'*32).order_by(ProjectFileRow.path))).scalars().all()
        assert [value.path for value in files]==[
            'artifacts/jobs/'+job.id+'/image-0.png','artifacts/jobs/'+job.id+'/image-1.png']
    finally:await store.close()


async def test_delivery_job_artifacts_become_visible_atomically_at_group_terminal(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'delivery-items.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Delivery'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='deliver',
            idempotency_key='delivery-origin'),profile,'owner')
        output={'artifact_role':'files','output_disposition':'delivery','min_items':2,'max_items':2,
            'mime_patterns':['text/*']}
        request=JobRequest(local_job_id='deliver',kind=JobKind.OPERATION,adapter='render',
            descriptor_id='render',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='delivery-job-key',expected_outputs=[output])
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='delivery-group-key',jobs=[request]))
        attempt=await store.claim_job(jobs[0].id,'worker','node',30)
        gateway=ArtifactGateway(store,ObjectStore(tmp_path/'objects'),tmp_path/'stage',maximum_bytes=1024)
        for index in range(2):
            await gateway.emit(ArtifactEmission(jobs[0].id,attempt.generation,'files',f'file-{index}',index,
                f'file-{index}.txt',OutputDisposition.DELIVERY,'text/plain'),chunks(f'value-{index}'.encode()))
        await store.finalize_job_artifact_collection(jobs[0].id,attempt.generation,'files')
        assert not [row for row in await store.items_after(thread.id) if row.type==ItemType.ARTIFACT]
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result={'ok':True})
        visible=[row for row in await store.items_after(thread.id) if row.type==ItemType.ARTIFACT]
        assert [row.summary for row in visible]==['file-0.txt','file-1.txt']
        assert all(row.payload['job_id']==jobs[0].id and not row.payload['partial'] for row in visible)
    finally:await store.close()


async def test_required_review_keeps_complete_collection_candidate_until_digest_bound_pass(tmp_path):
    output={'artifact_role':'report','output_disposition':'delivery','min_items':1,'max_items':1,
        'mime_patterns':['application/pdf'],'requires_review':True}
    store,gateway,job,attempt=await setup(tmp_path,output)
    try:
        data=b'%PDF-1.7\nreport';digest=hashlib.sha256(data).hexdigest()
        await gateway.emit(ArtifactEmission(job.id,attempt.generation,'report','main',0,'report.pdf',
            OutputDisposition.DELIVERY,'application/pdf'),chunks(data))
        waiting=await store.finalize_job_artifact_collection(job.id,attempt.generation,'report')
        assert waiting['state']=='awaiting_review' and waiting['output_disposition']=='candidate'
        with pytest.raises(RuntimeError,match='digest'):
            await store.finalize_job_artifact_collection(job.id,attempt.generation,'report',
                review_receipt={'verdict':'PASS','artifact_digests':['0'*64]})
        passed=await store.finalize_job_artifact_collection(job.id,attempt.generation,'report',
            review_receipt={'verdict':'PASS','artifact_digests':[digest]})
        assert passed['state']=='published'
    finally:await store.close()


async def test_binding_is_idempotent_after_object_first_crash_and_conflicts_are_rejected(tmp_path):
    output={'artifact_role':'data','output_disposition':'workspace','min_items':1,'max_items':1,
        'mime_patterns':['text/*']}
    store,gateway,job,attempt=await setup(tmp_path,output)
    try:
        value=ArtifactEmission(job.id,attempt.generation,'data','main',0,'data.txt',
            OutputDisposition.WORKSPACE,'text/plain')
        first=await gateway.emit(value,chunks(b'hello'))
        repeated=await gateway.emit(value,chunks(b'hello'))
        assert repeated.artifact_id==first.artifact_id
        with pytest.raises(RuntimeError,match='conflict'):
            await gateway.emit(value,chunks(b'changed'))
    finally:await store.close()


async def test_object_first_crash_leaves_reusable_content_addressed_object(tmp_path):
    output={'artifact_role':'data','output_disposition':'workspace','min_items':1,'max_items':1,
        'mime_patterns':['text/*']}
    store,gateway,job,attempt=await setup(tmp_path,output)
    objects=gateway.objects
    class CrashingStore:
        async def bind_job_artifact(self,*args,**kwargs):raise RuntimeError('database unavailable')
    crashing=ArtifactGateway(CrashingStore(),objects,tmp_path/'stage-crash',maximum_bytes=1024)
    value=ArtifactEmission(job.id,attempt.generation,'data','main',0,'data.txt',
        OutputDisposition.WORKSPACE,'text/plain')
    try:
        with pytest.raises(RuntimeError,match='database unavailable'):
            await crashing.emit(value,chunks(b'reusable'))
        reference='sha256:'+hashlib.sha256(b'reusable').hexdigest()
        assert objects.get(reference,maximum=100)==b'reusable'
        bound=await gateway.emit(value,chunks(b'reusable'))
        assert bound.sha256==hashlib.sha256(b'reusable').hexdigest()
    finally:await store.close()


def test_mime_registry_sniffs_aliases_and_unknowns(tmp_path):
    pdf=tmp_path/'report.bin';pdf.write_bytes(b'%PDF-1.7\n')
    assert MimeRegistry.detect(pdf,'application/x-pdf')=='application/pdf'
    unknown=tmp_path/'unknown.bin';unknown.write_bytes(b'\x00\x01\x02')
    assert MimeRegistry.detect(unknown)=="application/octet-stream"
    with pytest.raises(ValueError,match='does not match'):
        MimeRegistry.detect(pdf,'image/png')
    svg=tmp_path/'shape.svg';svg.write_text(
        '<svg xmlns="http://www.w3.org/2000/svg"><rect width="1" height="1"/></svg>')
    assert MimeRegistry.detect(svg,'image/svg+xml')=='image/svg+xml'
    document=tmp_path/'report.json';document.write_text('{"status":"succeeded"}\n')
    assert MimeRegistry.detect(document,'application/json')=='application/json'
    mp3=tmp_path/'audio.mp3';mp3.write_bytes(b'ID3\x04\x00\x00\x00\x00\x00\x00')
    assert MimeRegistry.detect(mp3,'audio/mpeg')=='audio/mpeg'
    m4a=tmp_path/'audio.m4a';m4a.write_bytes(b'\x00\x00\x00\x18ftypM4A \x00\x00\x00\x00')
    assert MimeRegistry.detect(m4a,'audio/mp4')=='audio/mp4'
    invalid=tmp_path/'invalid.svg';invalid.write_text('<svg>')
    with pytest.raises(ValueError,match='does not match'):
        MimeRegistry.detect(invalid,'image/svg+xml')


async def test_collection_rejects_cardinality_order_and_aggregate_limits(tmp_path):
    output={'artifact_role':'parts','output_disposition':'workspace','min_items':2,'max_items':3,
        'max_item_bytes':10,'max_total_bytes':12,'mime_patterns':['text/*']}
    store,gateway,job,attempt=await setup(tmp_path,output)
    try:
        await gateway.emit(ArtifactEmission(job.id,attempt.generation,'parts','two',1,'two.txt',
            OutputDisposition.WORKSPACE,'text/plain'),chunks(b'123456'))
        with pytest.raises(RuntimeError,match='cardinality'):
            await store.finalize_job_artifact_collection(job.id,attempt.generation,'parts')
        await gateway.emit(ArtifactEmission(job.id,attempt.generation,'parts','one',0,'one.txt',
            OutputDisposition.WORKSPACE,'text/plain'),chunks(b'1234567'))
        with pytest.raises(ValueError,match='collection_too_large'):
            await store.finalize_job_artifact_collection(job.id,attempt.generation,'parts')
    finally:await store.close()
