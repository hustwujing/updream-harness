import asyncio
import sqlite3
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import update

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,MediaDraftCreate,ThreadCreate,TurnCreate,TurnState,now
from sandbox_harness.media_catalog import MediaCatalog
from sandbox_harness.media_worker import DownloadedMedia,MediaWorker,ProviderResult
from sandbox_harness.objects import ObjectStore
from sandbox_harness.store import MediaJobRow,Store


async def setup_job(tmp_path,name="media"):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/(name+".db")),max_artifact_bytes=64,
        provider_output_allowlist=["allowed.test","cdn.test"])
    store=Store(settings.database_url);await store.initialize();owner="a"*64
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="Media"),profile,owner)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message="image",
        idempotency_key=name+"-turn-key"),profile,owner)
    spec=SimpleNamespace(provider="ark",adapter="ark_image",execution="sync",cancellation="unsupported")
    draft,_=await store.create_media_draft(owner,thread.id,MediaDraftCreate(kind="image",
        model="seedream-5.0",turn_id=turn.id,generation_mode="text_to_image",
        parameters={"prompt":"test"},idempotency_key=name+"-draft-key"),
        expires_at=now()+timedelta(minutes=5),provider_spec=spec)
    job=await store.confirm_media_draft(draft.id,owner,provider_spec=spec)
    return settings,store,thread,turn,job


async def test_sync_provider_receipt_survives_download_retry_without_resubmit(tmp_path,monkeypatch):
    settings,store,thread,turn,job=await setup_job(tmp_path,"receipt")
    worker=MediaWorker(settings,store,ObjectStore(tmp_path/"objects"),MediaCatalog())
    submits=0;downloads=0
    async def execute(_job):
        nonlocal submits;submits+=1
        return ProviderResult("success",url="https://allowed.test/image.png")
    async def download(*args,**kwargs):
        nonlocal downloads;downloads+=1
        if downloads==1:
            request=httpx.Request("GET","https://allowed.test/image.png")
            raise httpx.HTTPStatusError("temporary",request=request,response=httpx.Response(503,request=request))
        path=tmp_path/"image.png";data=b"\x89PNG\r\n\x1a\nvalid";path.write_bytes(data)
        return DownloadedMedia(path,len(data),__import__('hashlib').sha256(data).hexdigest(),"image/png")
    monkeypatch.setattr(worker,"_execute",execute);monkeypatch.setattr(worker,"_download",download)
    try:
        first=(await store.claim_media_jobs(worker.owner,lease_seconds=1))[0];await worker.process(first)
        async with store.connections.begin() as db:
            await db.execute(update(MediaJobRow).where(MediaJobRow.id==job.id).values(lease_expires_at=now()))
        second=(await store.claim_media_jobs(worker.owner,lease_seconds=1))[0];await worker.process(second)
        current=await store.get_media_job(job.id,"a"*64)
        assert submits==1 and downloads==2 and current.status=="succeeded"
        assert len(current.artifact_ids)==1
    finally:await store.close()


async def test_staged_object_receipt_skips_download_after_restart(tmp_path,monkeypatch):
    settings,store,thread,turn,job=await setup_job(tmp_path,"staged")
    objects=ObjectStore(tmp_path/"objects");worker=MediaWorker(settings,store,objects,MediaCatalog())
    result=ProviderResult("success",url="https://allowed.test/image.png")
    claimed=(await store.claim_media_jobs(worker.owner,lease_seconds=1))[0]
    await store.persist_media_provider_result(job.id,worker.owner,claimed["generation"],
        __import__('dataclasses').asdict(result))
    data=b"\x89PNG\r\n\x1a\nstaged";ref,digest=objects.put(data)
    await store.persist_media_staged_output(job.id,worker.owner,claimed["generation"],{
        "role":"primary","filename":"image.png","object_ref":ref,"mime":"image/png","size":len(data)})
    async with store.connections.begin() as db:
        await db.execute(update(MediaJobRow).where(MediaJobRow.id==job.id).values(
            lease_owner="",lease_expires_at=None))
    calls=0
    async def no_download(*args,**kwargs):
        nonlocal calls;calls+=1;raise AssertionError("staged output must be reused")
    monkeypatch.setattr(worker,"_download",no_download)
    try:
        recovered=(await store.claim_media_jobs(worker.owner))[0];await worker.process(recovered)
        assert calls==0 and (await store.get_media_job(job.id,"a"*64)).status=="succeeded"
    finally:await store.close()


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self,chunks):self.chunks=iter(chunks);self.reads=0
    def __aiter__(self):return self
    async def __anext__(self):
        try:chunk=next(self.chunks)
        except StopIteration:raise StopAsyncIteration
        self.reads+=1;return chunk
    async def aclose(self):pass


class DownloadClient:
    def __init__(self,responses):self.responses=list(responses);self.requests=[]
    async def __aenter__(self):return self
    async def __aexit__(self,*args):pass
    def build_request(self,*args,**kwargs):return httpx.Request(*args,**kwargs)
    async def send(self,request,stream=True):
        self.requests.append(request);response=self.responses.pop(0);response.request=request;return response


async def test_provider_download_blocks_ssrf_redirects_and_streams_bounded(tmp_path,monkeypatch):
    settings=Settings(backend="fake",data_root=tmp_path,max_artifact_bytes=16,
        provider_output_allowlist=["allowed.test","cdn.test"])
    worker=MediaWorker(settings,SimpleNamespace(),ObjectStore(tmp_path/"objects"),MediaCatalog())
    async def resolve(host,port):
        if host in {"127.0.0.1","::1","private.test"}:raise ValueError("provider_output_destination_not_public")
        return {"allowed.test":"1.1.1.1","cdn.test":"8.8.8.8"}[host]
    monkeypatch.setattr(worker,"_resolve_output",resolve)
    client=DownloadClient([httpx.Response(302,headers={"location":"https://127.0.0.1/metadata"})])
    monkeypatch.setattr(worker,"_client",lambda:asyncio.sleep(0,result=client))
    with pytest.raises(ValueError):await worker._download("https://allowed.test/start",expected_kind="image")
    assert len(client.requests)==1
    for bad in ("http://allowed.test/a","https://user:pass@allowed.test/a",
                "https://allowed.test:444/a","https://not-allowed.test/a#fragment","https://[::1]/a"):
        with pytest.raises(ValueError):await worker._download(bad,expected_kind="image")

    async def mixed_getaddrinfo(*args,**kwargs):
        return [(socket.AF_INET,socket.SOCK_STREAM,6,"",("8.8.8.8",443)),
            (socket.AF_INET,socket.SOCK_STREAM,6,"",("127.0.0.1",443))]
    import socket
    loop=asyncio.get_running_loop();original=loop.getaddrinfo
    monkeypatch.setattr(loop,"getaddrinfo",mixed_getaddrinfo)
    with pytest.raises(ValueError,match="not_public"):
        await MediaWorker(settings,SimpleNamespace(),ObjectStore(tmp_path/"mixed"),MediaCatalog())._resolve_output(
            "mixed.test",443)
    monkeypatch.setattr(loop,"getaddrinfo",original)

    stream=ChunkStream([b"\x89PNG\r\n",b"\x1a\nvalid"])
    client=DownloadClient([httpx.Response(200,headers={"content-type":"image/png"},stream=stream)])
    monkeypatch.setattr(worker,"_client",lambda:asyncio.sleep(0,result=client))
    downloaded=await worker._download("https://cdn.test/result.png",expected_kind="image")
    try:
        assert not isinstance(downloaded,bytes) and downloaded.path.read_bytes()==b"\x89PNG\r\n\x1a\nvalid"
        assert client.requests[0].url.host=="8.8.8.8" and client.requests[0].headers["host"]=="cdn.test"
    finally:downloaded.remove()

    declared=ChunkStream([b"must-not-read"])
    client=DownloadClient([httpx.Response(200,headers={"content-type":"image/png",
        "content-length":"100"},stream=declared)])
    monkeypatch.setattr(worker,"_client",lambda:asyncio.sleep(0,result=client))
    with pytest.raises(ValueError,match="too_large"):
        await worker._download("https://cdn.test/declared.png",expected_kind="image")
    assert declared.reads==0

    overflow=ChunkStream([b"123456789012",b"abcdefghijkl",b"unread"])
    client=DownloadClient([httpx.Response(200,headers={"content-type":"image/png"},stream=overflow)])
    monkeypatch.setattr(worker,"_client",lambda:asyncio.sleep(0,result=client))
    with pytest.raises(ValueError,match="too_large"):
        await worker._download("https://cdn.test/large.png",expected_kind="image")
    assert overflow.reads==2 and not list(worker.staging_root.glob(worker.stage_prefix+"*"))


async def test_media_lease_heartbeat_prevents_live_worker_takeover(tmp_path,monkeypatch):
    settings,store,thread,turn,job=await setup_job(tmp_path,"heartbeat")
    settings=settings.model_copy(update={"media_job_lease_seconds":1})
    worker=MediaWorker(settings,store,ObjectStore(tmp_path/"objects"),MediaCatalog());started=asyncio.Event();release=asyncio.Event()
    async def execute(_job):started.set();await release.wait();return ProviderResult("pending",provider_job_id="task")
    monkeypatch.setattr(worker,"_execute",execute)
    try:
        claimed=(await store.claim_media_jobs(worker.owner,lease_seconds=1))[0]
        task=asyncio.create_task(worker.process(claimed));await started.wait();await asyncio.sleep(1.2)
        assert await store.claim_media_jobs("second",lease_seconds=1)==[]
        release.set();await task
    finally:release.set();await store.close()


def test_catalog_rejects_advertised_supported_cancellation():
    raw=__import__('json').dumps([{"kind":"video","model":"cancelable",
        "provider":"x","adapter":"x","route":"/x","execution":"async",
        "cancellation":"supported","progress":"provider","credential_env":"KEY"}])
    with pytest.raises(ValueError,match="not implemented"):
        MediaCatalog(raw)


async def test_media_generation_schema_upgrade_is_idempotent(tmp_path):
    database=tmp_path/"media-upgrade.db";url="sqlite+aiosqlite:///"+str(database)
    store=Store(url);await store.initialize();await store.close()
    with sqlite3.connect(database) as connection:connection.execute("ALTER TABLE media_jobs DROP COLUMN generation")
    for _ in range(2):
        store=Store(url);await store.initialize();await store.close()
    with sqlite3.connect(database) as connection:
        columns={row[1] for row in connection.execute("PRAGMA table_info(media_jobs)")}
    assert "generation" in columns


async def test_media_archive_and_batch_unarchive_barrier(tmp_path):
    settings,store,thread,turn,job=await setup_job(tmp_path,"archive-media")
    try:
        await store.set_turn_state(turn.id,TurnState.COMPLETED)
        with pytest.raises(RuntimeError,match="active_media_prevents_archival"):
            await store.set_thread_archived(thread.id,"a"*64,archived=True)
        claimed=(await store.claim_media_jobs("worker"))[0]
        data=b"\x89PNG\r\n\x1a\nmedia";ref="sha256:"+__import__('hashlib').sha256(data).hexdigest()
        await store.complete_media_job(job.id,"worker",provider_job_id="",outputs=[{
            "role":"primary","filename":"image.png","object_ref":ref,"mime":"image/png","size":len(data)}],
            generation=claimed["generation"])
        with pytest.raises(RuntimeError,match="active_media_prevents_archival"):
            await store.set_thread_archived(thread.id,"a"*64,archived=True)
        batch=(await store.claim_media_batches("dispatcher"))[0]
        await store.finish_media_batch(batch["turn_id"],"dispatcher",delivered=False,
            error="thread_archived",permanent=True)
        await store.set_thread_archived(thread.id,"a"*64,archived=True)
        assert await store.claim_media_batches("other")==[]
        await store.set_thread_archived(thread.id,"a"*64,archived=False)
        assert (await store.claim_media_batches("other"))[0]["turn_id"]==turn.id
    finally:await store.close()
