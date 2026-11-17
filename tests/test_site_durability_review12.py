import asyncio
from datetime import timedelta
import hashlib
import io
from pathlib import Path
import sqlite3
import tarfile
import tracemalloc

import pytest
from sqlalchemy import update

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,now
from sandbox_harness.objects import ObjectStore
from sandbox_harness.site_bundle import build_site_archive_from_files,stage_site_archive
from sandbox_harness.site_worker import SiteWorker
from sandbox_harness.store import SiteDeployJobRow,Store


async def prepared(tmp_path,name="site",*,max_attempts=4):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/(name+".db")),
        site_job_lease_seconds=10,site_job_max_attempts=max_attempts,
        site_job_retry_base_seconds=1)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/(name+"-objects"))
    owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title=name),profile,owner)
    archive,files=build_site_archive_from_files({"index.html":b"<!doctype html><h1>site</h1>",
        "app.js":b"document.body.dataset.ready='yes'"})
    archive_ref,digest=objects.put(archive);staged=[]
    for value in files:
        object_ref,_=objects.put(value.data);staged.append({"path":value.path,"sha256":value.sha256,
            "size":value.size,"mime":value.mime,"object_ref":object_ref})
    draft,_=await store.create_site_draft(owner,thread.id,"",name=name,slug="review-site",
        idempotency_key=name+"-draft-key",archive_object_ref=archive_ref,bundle_sha256=digest,
        archive_size=len(archive),file_count=len(files),total_size=sum(value.size for value in files),
        expires_at=now()+timedelta(hours=1),public_base_path="/sites",files=staged)
    job=await store.confirm_site_draft(draft.id,owner)
    return settings,store,objects,owner,thread,draft,job


async def make_retry_ready(store,job_id):
    async with store.connections.begin() as db:
        await db.execute(update(SiteDeployJobRow).where(SiteDeployJobRow.id==job_id).values(
            next_attempt_at=now()-timedelta(seconds=1)))


async def test_transient_object_failure_retries_same_job_and_version(tmp_path,monkeypatch):
    settings,store,objects,owner,thread,draft,job=await prepared(tmp_path,"transient")
    worker=SiteWorker(settings,store,objects);original=objects.iter_bytes;failed=False
    def flaky(reference,**kwargs):
        nonlocal failed
        if not failed:
            failed=True;raise OSError("object store unavailable")
        yield from original(reference,**kwargs)
    monkeypatch.setattr(objects,"iter_bytes",flaky)
    try:
        first=(await store.claim_site_jobs(worker.owner,1,lease_seconds=10))[0]
        await worker.process(first);waiting=await store.get_site_job(job.id,owner)
        assert waiting.status=="retry_wait" and waiting.error_code=="site_dependency_unavailable"
        assert waiting.error_detail and waiting.attempt_count==1
        await make_retry_ready(store,job.id)
        second=(await store.claim_site_jobs("replacement",1,lease_seconds=10))[0]
        worker.owner="replacement";await worker.process(second)
        completed=await store.get_site_job(job.id,owner)
        projects=await store.list_site_projects(thread.id,owner)
        assert completed.status=="succeeded" and completed.id==job.id
        assert len(projects)==1 and len(projects[0].versions)==1
    finally:await store.close()


async def test_activation_failure_retries_without_duplicate_version(tmp_path,monkeypatch):
    settings,store,objects,owner,thread,draft,job=await prepared(tmp_path,"activation")
    worker=SiteWorker(settings,store,objects);complete=store.complete_site_job;calls=0
    async def flaky(*args,**kwargs):
        nonlocal calls;calls+=1
        if calls==1:raise OSError("database activation unavailable")
        return await complete(*args,**kwargs)
    monkeypatch.setattr(store,"complete_site_job",flaky)
    try:
        first=(await store.claim_site_jobs(worker.owner,1))[0];await worker.process(first)
        assert (await store.get_site_job(job.id,owner)).status=="retry_wait"
        await make_retry_ready(store,job.id)
        second=(await store.claim_site_jobs(worker.owner,1))[0];await worker.process(second)
        project=(await store.list_site_projects(thread.id,owner))[0]
        assert calls==2 and len(project.versions)==1 and project.status=="active"
    finally:await store.close()


async def test_retry_exhaustion_is_visible_and_owner_can_resume(tmp_path,monkeypatch):
    settings,store,objects,owner,thread,draft,job=await prepared(tmp_path,"manual",max_attempts=1)
    worker=SiteWorker(settings,store,objects);original=objects.iter_bytes
    monkeypatch.setattr(objects,"iter_bytes",lambda *args,**kwargs:(_ for _ in ()).throw(OSError("down")))
    try:
        claimed=(await store.claim_site_jobs(worker.owner,1))[0];await worker.process(claimed)
        exhausted=await store.get_site_job(job.id,owner)
        assert exhausted.status=="retry_exhausted" and exhausted.retryable and exhausted.error_detail
        resumed=await store.retry_site_job(job.id,owner)
        assert resumed.status=="retry_wait" and not resumed.retryable
        monkeypatch.setattr(objects,"iter_bytes",original);await make_retry_ready(store,job.id)
        claimed=(await store.claim_site_jobs(worker.owner,1))[0];await worker.process(claimed)
        assert (await store.get_site_job(job.id,owner)).status=="succeeded"
    finally:await store.close()


async def test_site_generation_fences_same_owner_stale_writes(tmp_path):
    settings,store,objects,owner,thread,draft,job=await prepared(tmp_path,"generation")
    try:
        first=(await store.claim_site_jobs("worker",1,lease_seconds=1))[0]
        async with store.connections.begin() as db:
            await db.execute(update(SiteDeployJobRow).where(SiteDeployJobRow.id==job.id).values(
                lease_expires_at=now()-timedelta(seconds=1)))
        second=(await store.claim_site_jobs("worker",1,lease_seconds=1))[0]
        assert (first["generation"],second["generation"])==(1,2)
        with pytest.raises(RuntimeError,match="stale_site_job_lease"):
            await store.update_site_job(job.id,"worker",1,phase="stale",progress=40)
        with pytest.raises(RuntimeError,match="stale_site_job_lease"):
            await store.fail_site_job(job.id,"worker",1,error_code="stale")
        with pytest.raises(RuntimeError,match="stale_site_job_lease"):
            await store.complete_site_job(job.id,"worker",1,bundle_sha256=first["bundle_sha256"],
                files=first["files"])
        assert await store.update_site_job(job.id,"worker",2,phase="current",progress=50)
    finally:await store.close()


async def test_site_heartbeat_failure_cancels_business_task(tmp_path,monkeypatch):
    class StoreStub:
        async def renew_site_job_lease(self,*args,**kwargs):raise RuntimeError("database unavailable")
    worker=SiteWorker(type("Settings",(),{"site_worker_concurrency":1,"data_root":tmp_path,
        "site_job_lease_seconds":10})(),StoreStub(),ObjectStore(tmp_path/"objects"))
    worker.lease_seconds=.03;started=asyncio.Event();cancelled=asyncio.Event()
    async def blocked(_job):
        started.set()
        try:await asyncio.Event().wait()
        finally:cancelled.set()
    monkeypatch.setattr(worker,"_process_claimed",blocked)
    task=asyncio.create_task(worker.process({"id":"job","generation":3}))
    await started.wait();await asyncio.wait_for(task,1)
    assert cancelled.is_set()


async def test_missing_and_corrupt_public_objects_are_repaired_before_activation(tmp_path):
    settings,store,objects,owner,thread,draft,job=await prepared(tmp_path,"repair")
    worker=SiteWorker(settings,store,objects)
    try:
        claimed=(await store.claim_site_jobs(worker.owner,1))[0]
        first,second=claimed["files"]
        objects._path(first["sha256"]).unlink()
        objects._path(second["sha256"]).write_bytes(b"corrupt")
        await worker.process(claimed)
        assert (await store.get_site_job(job.id,owner)).status=="succeeded"
        project=(await store.list_site_projects(thread.id,owner))[0]
        for value in claimed["files"]:
            public=await store.public_site_file(project.slug,value["path"])
            data=objects.get(public["object_ref"],maximum=public["size"])
            assert len(data)==public["size"] and hashlib.sha256(data).hexdigest()==public["sha256"]
    finally:await store.close()


async def test_concurrent_draft_and_slug_allocation_is_database_idempotent(tmp_path):
    url="sqlite+aiosqlite:///"+str(tmp_path/"concurrent.db")
    first=Store(url);second=Store(url);await first.initialize();await second.initialize()
    owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    one=await first.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="one"),profile,owner)
    two=await first.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="two"),profile,owner)
    archive,files=build_site_archive_from_files({"index.html":b"ok"});digest=hashlib.sha256(archive).hexdigest()
    staged=[{"path":value.path,"sha256":value.sha256,"size":value.size,"mime":value.mime,
        "object_ref":"sha256:"+value.sha256} for value in files]
    def values(thread,key):return dict(owner_id=owner,thread_id=thread.id,turn_id="",name="Site",
        slug="shared-site",idempotency_key=key,archive_object_ref="sha256:"+digest,
        bundle_sha256=digest,archive_size=len(archive),file_count=1,total_size=2,
        expires_at=now()+timedelta(hours=1),public_base_path="/sites",files=staged)
    try:
        same=values(one,"same-key")
        results=await asyncio.gather(first.create_site_draft(**same),second.create_site_draft(**same))
        assert len({value[0].id for value in results})==1 and sorted(value[1] for value in results)==[False,True]
        with pytest.raises(RuntimeError,match="idempotency_conflict"):
            await second.create_site_draft(**{**same,"name":"Changed"})
        left,right=await asyncio.gather(first.create_site_draft(**values(one,"other-key")),
            second.create_site_draft(**values(two,"two-key")))
        assert left[0].slug!=right[0].slug
    finally:await first.close();await second.close()


def test_large_site_archive_stages_with_bounded_python_memory(tmp_path):
    archive_path=tmp_path/"large.tar";payload=tmp_path/"payload.bin"
    with payload.open("wb") as stream:
        block=b"x"*(1024*1024)
        for _ in range(30):stream.write(block)
    with tarfile.open(archive_path,"w") as archive:
        index=b"<!doctype html><h1>large</h1>";info=tarfile.TarInfo("index.html");info.size=len(index)
        archive.addfile(info,io.BytesIO(index));archive.add(payload,arcname="payload.bin",recursive=False)
    tracemalloc.start();package=stage_site_archive(archive_path,tmp_path/"stages");_,peak=tracemalloc.get_traced_memory()
    try:
        assert sum(value.size for value in package.files)>30*1024*1024
        assert all(not hasattr(value,"data") for value in package.files)
        assert peak<16*1024*1024
    finally:package.remove();tracemalloc.stop()
    assert not list((tmp_path/"stages").glob("site-stage-*"))


async def test_site_schema_upgrade_is_idempotent(tmp_path):
    database=tmp_path/"site-upgrade.db";url="sqlite+aiosqlite:///"+str(database)
    store=Store(url);await store.initialize();await store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE site_deploy_jobs DROP COLUMN generation")
        connection.execute("ALTER TABLE site_deploy_jobs DROP COLUMN attempt_count")
        connection.execute("ALTER TABLE site_deploy_jobs DROP COLUMN next_attempt_at")
        connection.execute("ALTER TABLE site_drafts DROP COLUMN request_digest")
    for _ in range(2):
        store=Store(url);await store.initialize();await store.close()
    with sqlite3.connect(database) as connection:
        job_columns={row[1] for row in connection.execute("PRAGMA table_info(site_deploy_jobs)")}
        draft_columns={row[1] for row in connection.execute("PRAGMA table_info(site_drafts)")}
    assert {"generation","attempt_count","next_attempt_at"}<=job_columns
    assert "request_digest" in draft_columns
