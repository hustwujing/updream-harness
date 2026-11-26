import hashlib

import httpx
import pytest

from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (Sandbox)
from test_runtime_boundaries import runtime

CHUNK=4*1024*1024


@pytest.mark.parametrize('streaming',[False,True])
async def test_large_managed_file_crosses_bounded_ingress_and_lost_replies(tmp_path,monkeypatch,streaming):
    app=runtime(tmp_path,monkeypatch,'')
    upstream=httpx.ASGITransport(app=app)
    dropped=set();bodies=[]
    class Ingress(httpx.AsyncBaseTransport):
        async def handle_async_request(self,request):
            body=await request.aread();bodies.append(len(body))
            if len(body)>CHUNK:return httpx.Response(413)
            response=await upstream.handle_async_request(request)
            key=('chunk' if '/chunks/' in request.url.path else
                 'commit' if request.url.path.endswith('/commit') else '')
            if key and key not in dropped and response.is_success:
                dropped.add(key);await response.aread()
                raise httpx.ReadError('reply lost',request=request)
            return response
    client=RuntimeClient(transport=Ingress())
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    data=b'0123456789abcdef'*(CHUNK//8)+b'end'
    async def source():
        for offset in range(0,len(data),77777):yield data[offset:offset+77777]
    if streaming:
        await client.put_project_file_stream(sandbox,'artifacts/large.bin',source(),size=len(data),mime='application/octet-stream')
    else:await client.put_project_file(sandbox,'artifacts/large.bin',data,'application/octet-stream')
    assert (tmp_path/'workspace/project/artifacts/large.bin').read_bytes()==data
    assert max(bodies)<=CHUNK
    assert dropped=={'chunk','commit'}
    assert not list((tmp_path/'workspace/tmp/project-uploads').glob('*.part'))


def make_sandbox():
    return Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})


@pytest.mark.parametrize('size_difference',[-1,1])
async def test_incomplete_or_excess_stream_aborts_without_replacing_target(tmp_path,monkeypatch,size_difference):
    app=runtime(tmp_path,monkeypatch,'')
    target=tmp_path/'workspace/project/assets/file.bin';target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'original')
    client=RuntimeClient(transport=httpx.ASGITransport(app=app))
    data=b'x'*(CHUNK+10)
    async def source():yield data
    with pytest.raises(ValueError,match='runtime_transfer_size_mismatch'):
        await client.put_project_file_stream(make_sandbox(),'assets/file.bin',source(),size=len(data)+size_difference,mime='application/octet-stream')
    assert target.read_bytes()==b'original'
    assert not list((tmp_path/'workspace/tmp/project-uploads').glob('*.part'))


async def test_chunk_conflict_commit_digest_and_replayed_old_receipt(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'');manager=app.state.project_uploads
    target=tmp_path/'workspace/project/assets/file.bin'
    manager.begin('a'*32,'assets/file.bin',6)
    manager.chunk('a'*32,0,b'abc')
    assert not target.exists()
    from fastapi import HTTPException
    for offset,data in [(0,b'xyz'),(4,b'ef'),(2,b'cdef')]:
        with pytest.raises(HTTPException) as exc:manager.chunk('a'*32,offset,data)
        assert exc.value.status_code==409
    with pytest.raises(HTTPException):manager.commit('a'*32,hashlib.sha256(b'abcdef').hexdigest())
    manager.chunk('a'*32,0,b'abc');manager.chunk('a'*32,3,b'def')
    with pytest.raises(HTTPException):manager.commit('a'*32,'0'*64)
    digest=hashlib.sha256(b'abcdef').hexdigest();receipt=manager.commit('a'*32,digest)
    assert target.read_bytes()==b'abcdef'
    manager.begin('b'*32,'assets/file.bin',3);manager.chunk('b'*32,0,b'new')
    manager.commit('b'*32,hashlib.sha256(b'new').hexdigest())
    assert manager.commit('a'*32,digest)==receipt
    manager.abort('a'*32)
    assert target.read_bytes()==b'new'


@pytest.mark.parametrize('replacement',['chunk','single','reset'])
async def test_new_writer_or_reset_fences_unfinished_upload(tmp_path,monkeypatch,replacement):
    app=runtime(tmp_path,monkeypatch,'');manager=app.state.project_uploads
    manager.begin('a'*32,'assets/file.bin',3);manager.chunk('a'*32,0,b'old')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://runtime') as client:
        if replacement=='chunk':
            manager.begin('b'*32,'assets/./file.bin',3);manager.chunk('b'*32,0,b'new')
            manager.commit('b'*32,hashlib.sha256(b'new').hexdigest())
        elif replacement=='single':
            response=await client.put('/v1/project-files/file',params={'path':'assets/file.bin','size':3},content=b'new');assert response.is_success
        else:assert (await client.post('/v1/project-files/reset')).is_success
        response=await client.post('/v1/project-files/uploads/'+('a'*32)+'/commit',params={'sha256':hashlib.sha256(b'old').hexdigest()})
        assert response.status_code==409
    target=tmp_path/'workspace/project/assets/file.bin'
    assert not target.exists() if replacement=='reset' else target.read_bytes()==b'new'


async def test_staging_budgets_expiry_and_runtime_restart(tmp_path,monkeypatch):
    from fastapi import HTTPException
    from sandbox_runtime import project_uploads as module
    app=runtime(tmp_path,monkeypatch,'');manager=app.state.project_uploads
    monkeypatch.setattr(module,'MAX_BYTES',10)
    manager.begin('a'*32,'assets/a',6)
    with pytest.raises(HTTPException) as exc:manager.begin('b'*32,'assets/b',5)
    assert exc.value.status_code==429
    manager.abort('a'*32)
    for number in range(4):manager.begin(f'{number:032x}',f'assets/{number}',1)
    with pytest.raises(HTTPException) as exc:manager.begin('e'*32,'assets/e',1)
    assert exc.value.status_code==429
    for value in manager.records.values():value['touched']-=601
    manager.begin('f'*32,'assets/f',1)
    assert len(list(manager.root.glob('*.part')))==1
    runtime(tmp_path,monkeypatch,'')
    assert not list(manager.root.glob('*.part'))


@pytest.mark.parametrize('path',['../outside','assets/../../outside','work/file','/assets/file'])
async def test_chunked_upload_rejects_unmanaged_paths(tmp_path,monkeypatch,path):
    from fastapi import HTTPException
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    with pytest.raises(HTTPException) as exc:manager.begin('a'*32,path,1)
    assert exc.value.status_code==400


async def test_commit_rechecks_managed_path_links(tmp_path,monkeypatch):
    from fastapi import HTTPException
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    manager.begin('a'*32,'assets/folder/file',3);manager.chunk('a'*32,0,b'abc')
    outside=tmp_path/'outside';outside.mkdir()
    assets=tmp_path/'workspace/project/assets';assets.mkdir(parents=True,exist_ok=True)
    (assets/'folder').symlink_to(outside,target_is_directory=True)
    with pytest.raises(HTTPException) as exc:manager.commit('a'*32,hashlib.sha256(b'abc').hexdigest())
    assert exc.value.status_code==400
    assert not list(outside.iterdir())


async def test_cancel_large_stream_aborts_owned_stage(tmp_path,monkeypatch):
    import asyncio
    app=runtime(tmp_path,monkeypatch,'');entered=asyncio.Event()
    client=RuntimeClient(transport=httpx.ASGITransport(app=app))
    async def source():
        yield b'x'*CHUNK
        entered.set();await asyncio.Event().wait()
    task=asyncio.create_task(client.put_project_file_stream(make_sandbox(),'assets/file',source(),size=CHUNK+1,mime='application/octet-stream'))
    await asyncio.wait_for(entered.wait(),2);task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert not list((tmp_path/'workspace/tmp/project-uploads').glob('*.part'))
    assert not (tmp_path/'workspace/project/assets/file').exists()


async def test_legacy_runtime_does_not_receive_oversized_fallback():
    requests=[]
    def respond(request):requests.append(request);return httpx.Response(404)
    client=RuntimeClient(transport=httpx.MockTransport(respond))
    with pytest.raises(httpx.HTTPStatusError):
        await client.put_project_file(make_sandbox(),'assets/file',b'x'*(CHUNK+1),'application/octet-stream')
    assert all(not request.content for request in requests)
    assert all(request.url.path.startswith('/v1/project-files/uploads') for request in requests)


async def test_oversized_chunk_never_changes_staged_file(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'');manager=app.state.project_uploads
    manager.begin('a'*32,'assets/file',CHUNK+1)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://runtime') as client:
        response=await client.put('/v1/project-files/uploads/'+('a'*32)+'/chunks/0',content=b'x'*(CHUNK+1))
    assert response.status_code==413
    assert manager.records['a'*32]['written']==0


async def test_receipt_bound_does_not_evict_the_commit_being_replayed(tmp_path,monkeypatch):
    from sandbox_runtime import project_uploads as module
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    monkeypatch.setattr(module,'MAX_RECEIPTS',2)
    digest=hashlib.sha256(b'').hexdigest()
    for identity in ('a'*32,'b'*32):
        manager.begin(identity,'assets/'+identity,0);manager.commit(identity,digest)
    assert manager.commit('a'*32,digest)['sha256']==digest
    assert manager.begin('a'*32,'assets/'+('a'*32),0)['upload_id']=='a'*32
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:manager.begin('c'*32,'assets/c',0)
    assert exc.value.status_code==429
    assert len(manager.records)==2


async def test_receipt_pressure_cannot_revive_superseded_writer(tmp_path,monkeypatch):
    from fastapi import HTTPException
    from sandbox_runtime import project_uploads as module
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    monkeypatch.setattr(module,'MAX_RECEIPTS',2)
    manager.begin('a'*32,'assets/file',3)
    manager.begin('b'*32,'assets/file',3);manager.chunk('b'*32,0,b'new')
    manager.commit('b'*32,hashlib.sha256(b'new').hexdigest())
    with pytest.raises(HTTPException):manager.begin('c'*32,'assets/another',3)
    with pytest.raises(HTTPException) as exc:manager.begin('a'*32,'assets/file',3)
    assert exc.value.status_code==409
    assert (tmp_path/'workspace/project/assets/file').read_bytes()==b'new'


async def test_cancel_during_post_commit_cleanup_still_propagates(tmp_path,monkeypatch):
    import asyncio
    app=runtime(tmp_path,monkeypatch,'');upstream=httpx.ASGITransport(app=app)
    entered=asyncio.Event()
    class Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self,request):
            if request.method=='DELETE':
                entered.set();await asyncio.Event().wait()
            return await upstream.handle_async_request(request)
    client=RuntimeClient(transport=Transport())
    task=asyncio.create_task(client.put_project_file(make_sandbox(),'assets/file',b'x'*(CHUNK+1),'application/octet-stream'))
    await asyncio.wait_for(entered.wait(),3);task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert (tmp_path/'workspace/project/assets/file').stat().st_size==CHUNK+1


async def test_expired_begin_cannot_revive_after_receipt_collection(tmp_path,monkeypatch):
    from fastapi import HTTPException
    from sandbox_runtime import project_uploads as module
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    clock=[1000]
    monkeypatch.setattr(module.time,'time',lambda:clock[0])
    manager.begin('a'*32,'assets/file',3,1000)
    manager.begin('b'*32,'assets/file',3,1000);manager.chunk('b'*32,0,b'new')
    manager.commit('b'*32,hashlib.sha256(b'new').hexdigest())
    clock[0]=1601
    for value in manager.records.values():value['touched']-=601
    with pytest.raises(HTTPException) as exc:manager.begin('a'*32,'assets/file',3,1000)
    assert exc.value.status_code==410
    assert not manager.records
    assert (tmp_path/'workspace/project/assets/file').read_bytes()==b'new'


async def test_runtime_replacement_rejects_prior_initialization_epoch(tmp_path,monkeypatch):
    from fastapi import HTTPException
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads
    epoch=manager.epoch
    manager.begin('a'*32,'assets/file',3,epoch=epoch)
    replacement=runtime(tmp_path,monkeypatch,'').state.project_uploads
    replacement.begin('b'*32,'assets/file',3,epoch=replacement.epoch)
    replacement.chunk('b'*32,0,b'new');replacement.commit('b'*32,hashlib.sha256(b'new').hexdigest())
    with pytest.raises(HTTPException) as exc:replacement.begin('a'*32,'assets/file',3,epoch=epoch)
    assert exc.value.status_code==409
    assert (tmp_path/'workspace/project/assets/file').read_bytes()==b'new'


async def test_reset_rejects_initialization_that_has_not_arrived_yet(tmp_path,monkeypatch):
    from fastapi import HTTPException
    manager=runtime(tmp_path,monkeypatch,'').state.project_uploads;epoch=manager.epoch
    manager.reset()
    with pytest.raises(HTTPException) as exc:manager.begin('a'*32,'assets/file',3,epoch=epoch)
    assert exc.value.status_code==409
    assert not manager.records
