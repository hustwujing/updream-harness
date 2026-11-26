import hashlib
import zipfile

import pytest

from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway, MimeRegistry)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, ItemType, ThreadCreate, TurnCreate)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.db.store import (Store)
from sandbox_harness.common.db.store import (ProjectFileRow)
from sqlalchemy import select


async def chunks(*values):
    for value in values:yield value


def ebml_header(elements):
    return b'\x1a\x45\xdf\xa3'+bytes([0x80|len(elements)])+elements


@pytest.mark.parametrize('document,mime',[(b'matroska','video/x-matroska'),(b'webm','video/webm')])
async def test_ebml_video_content_satisfies_its_frozen_output_contract(tmp_path,document,mime):
    # EBML header fields from an FFmpeg-produced Matroska file (RFC 8794).
    fields=bytes.fromhex('4286810142f7810142f2810442f38108')
    fields+=b'\x42\x82'+bytes([0x80|len(document)])+document
    fields+=bytes.fromhex('4287810442858102')
    data=ebml_header(fields);path=tmp_path/'no-extension';path.write_bytes(data)
    assert MimeRegistry.detect(path)==mime
    assert MimeRegistry.detect(path,mime)==mime
    with pytest.raises(ValueError,match='Declared MIME'):
        MimeRegistry.detect(path,'video/mp4')
    store,gateway,job,attempt=await setup(tmp_path,{
        'artifact_role':'video','output_disposition':'workspace','min_items':1,'max_items':1,
        'mime_patterns':[mime]})
    try:
        result=await gateway.emit(ArtifactEmission(job.id,attempt.generation,'video','main',0,
            'result.bin',OutputDisposition.WORKSPACE,mime),chunks(data))
        assert result.mime==mime
    finally:await store.close()


@pytest.mark.parametrize('data',[
    b'\x1a\x45\xdf\xa3\xff',
    b'\x1a\x45\xdf\xa3\xa0\x42\x82\x88matroska',
    ebml_header(b'\x42\x82\x88matrosk'),
    ebml_header(b'\x42\x82\x85other'),
    ebml_header(b'\xec\x8b\x42\x82\x88matroska'),
    ebml_header(b'\x42\x82\x88matroska\x42\x82\x84webm'),
])
def test_ebml_detection_rejects_unknown_truncated_and_embedded_doctypes(tmp_path,data):
    path=tmp_path/'spoof.mkv';path.write_bytes(data)
    assert MimeRegistry.detect(path,'video/x-matroska')=='application/octet-stream'


@pytest.mark.parametrize('width',[2,8])
def test_ebml_detection_accepts_multibyte_sizes_and_padded_doctype(tmp_path,width):
    def size(value):return ((1<<(7*width))|value).to_bytes(width,'big')
    document=b'matroska\x00\x00'
    fields=b'\x42\x82'+size(len(document))+document
    path=tmp_path/'no-extension'
    path.write_bytes(b'\x1a\x45\xdf\xa3'+size(len(fields))+fields)
    assert MimeRegistry.detect(path)=='video/x-matroska'


@pytest.mark.parametrize('data,mime',[
    (b'8BPS\x00\x01'+b'\x00'*30,'image/vnd.adobe.photoshop'),
    (b'II*\x00'+b'\x00'*32,'image/tiff'),
    (b'MM\x00*'+b'\x00'*32,'image/tiff'),
])
def test_binary_layer_formats_are_detected_from_content(tmp_path,data,mime):
    path=tmp_path/'no-extension';path.write_bytes(data)
    assert MimeRegistry.detect(path)==mime


def test_openraster_detection_requires_bounded_metadata_and_safe_paths(tmp_path):
    for suffix,mimetype,extra,expected in [
        ('valid',b'image/openraster','layer.png','image/openraster'),
        ('oversize',b'image/openraster'+b' '*1024,'layer.png','application/zip'),
        ('unsafe',b'image/openraster','../layer.png','application/zip'),
    ]:
        path=tmp_path/(suffix+'.bin')
        with zipfile.ZipFile(path,'w') as archive:
            archive.writestr('mimetype',mimetype)
            archive.writestr('stack.xml','<image><stack/></image>')
            archive.writestr(extra,b'layer')
        assert MimeRegistry.detect(path)==expected


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


async def test_optional_format_selection_preserves_strict_validation_and_minimum(tmp_path,monkeypatch):
    output={'artifact_role':'images','output_disposition':'workspace','min_items':1,'max_items':2,
        'mime_patterns':['image/png']}
    store,gateway,job,attempt=await setup(tmp_path,output)
    value=ArtifactEmission(job.id,attempt.generation,'images','source-1',0,'layers.psd',
        OutputDisposition.WORKSPACE,'application/octet-stream')
    data=b'8BPS\x00\x01'+b'\x00'*30
    try:
        put=gateway.objects.put_file
        def unexpected_write(*args,**kwargs):raise AssertionError('excluded content was persisted')
        monkeypatch.setattr(gateway.objects,'put_file',unexpected_write)
        assert await gateway.emit_matching(value,chunks(data),mime_patterns=('image/png',)) is None
        with pytest.raises(RuntimeError,match='cardinality'):
            await store.finalize_job_artifact_collection(job.id,attempt.generation,'images')
        spoof=ArtifactEmission(job.id,attempt.generation,'images','source-2',0,'spoof.png',
            OutputDisposition.WORKSPACE,'image/png')
        with pytest.raises(ValueError,match='Declared MIME'):
            await gateway.emit_matching(spoof,chunks(data),mime_patterns=('application/pdf',))
        monkeypatch.setattr(gateway.objects,'put_file',put)
        with pytest.raises(ValueError,match='artifact_mime_not_allowed'):
            await gateway.emit(value,chunks(data))
        with pytest.raises(ValueError,match='artifact_mime_not_allowed'):
            await gateway.emit_matching(value,chunks(data),mime_patterns=())
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
        assert all(row.payload['job_id']==jobs[0].id and not row.payload['partial']
            and row.payload['local_job_id']=='deliver' for row in visible)
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
    script=tmp_path/'render.sh';script.write_text('#!/bin/sh\necho rendered\n')
    assert MimeRegistry.detect(script,'text/x-sh')=='text/x-shellscript'
    assert MimeRegistry.matches('text/x-sh',('text/x-shellscript',))
    assert MimeRegistry.matches('application/x-sh',('text/x-shellscript',))
    assert not MimeRegistry.matches('text/plain',('text/x-shellscript',))
    mp3=tmp_path/'audio.mp3';mp3.write_bytes(b'ID3\x04\x00\x00\x00\x00\x00\x00')
    assert MimeRegistry.detect(mp3,'audio/mpeg')=='audio/mpeg'
    m4a=tmp_path/'audio.m4a';m4a.write_bytes(b'\x00\x00\x00\x18ftypM4A \x00\x00\x00\x00')
    assert MimeRegistry.detect(m4a,'audio/mp4')=='audio/mp4'
    webp=tmp_path/'image.webp';webp.write_bytes(b'RIFF\x04\x00\x00\x00WEBPVP8 ')
    assert MimeRegistry.detect(webp,'image/webp')=='image/webp'
    gzip=tmp_path/'product.tar.gz';gzip.write_bytes(b'\x1f\x8b\x08\x00'+b'\x00'*20)
    assert MimeRegistry.detect(gzip,'application/x-tar')=='application/gzip'
    assert MimeRegistry.detect(gzip,'application/x-gzip')=='application/gzip'
    invalid=tmp_path/'invalid.svg';invalid.write_text('<svg>')
    with pytest.raises(ValueError,match='does not match'):
        MimeRegistry.detect(invalid,'image/svg+xml')


@pytest.mark.parametrize(('mime','member'),[
    ('model/obj+zip','model/model.obj'),('model/gltf+zip','scene/model.gltf'),
    ('model/vnd.usdz+zip','scene/model.usdc'),('model/3mf','3D/model.model')])
def test_mime_registry_validates_declared_3d_archives(tmp_path,mime,member):
    path=tmp_path/'model.zip'
    with zipfile.ZipFile(path,'w') as archive:
        if mime=='model/3mf':archive.writestr('[Content_Types].xml','<Types/>')
        archive.writestr(member,'model data')
    assert MimeRegistry.detect(path,mime)==mime


def test_mime_registry_rejects_spoofed_or_unsafe_3d_archives(tmp_path):
    plain=tmp_path/'plain.zip'
    with zipfile.ZipFile(plain,'w') as archive:archive.writestr('readme.txt','not a model')
    with pytest.raises(ValueError,match='does not match'):
        MimeRegistry.detect(plain,'model/obj+zip')
    unsafe=tmp_path/'unsafe.zip'
    with zipfile.ZipFile(unsafe,'w') as archive:archive.writestr('../model.obj','v 0 0 0')
    with pytest.raises(ValueError,match='does not match'):
        MimeRegistry.detect(unsafe,'model/obj+zip')


def test_mime_registry_detects_native_3d_payloads(tmp_path):
    values=[('model.glb',b'glTF'+b'\0'*20,'model/gltf-binary'),
        ('model.fbx',b'Kaydara FBX Binary  \x00'+b'\0'*20,'application/vnd.autodesk.fbx'),
        ('model.obj',b'v 0 0 0\nf 1 1 1\n','model/obj'),
        ('model.gltf',b'{"asset":{"version":"2.0"}}','model/gltf+json'),
        ('model.stl',b'solid cube\nendsolid cube\n','model/stl')]
    for name,data,mime in values:
        path=tmp_path/name;path.write_bytes(data)
        assert MimeRegistry.detect(path,mime)==mime


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
