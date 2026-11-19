import asyncio
import hashlib
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, MediaDraftCreate, ThreadCreate, TurnCreate, now
from sandbox_harness.media_reference_publisher import MediaReferencePreparationError, MediaReferencePublisher
from sandbox_harness.store import MediaReferenceAssetRow,Store


DATA=b"reference-media-bytes"
DIGEST=hashlib.sha256(DATA).hexdigest()


class Objects:
    def __init__(self):self.uploads=0
    def prepare_media_upload(self, *, kind, digest, mime, size, expires_seconds):
        key=f"aniforge/{kind}/{digest[:2]}/{digest}.png"
        return {"object_key":key,"public_url":"https://boss-shjd.biliapi.net/updream/"+key,
            "put_url":"https://boss-shjd.biliapi.net/signed-put","headers":{"Content-Type":mime,
                "x-amz-meta-sha256":digest,"x-amz-meta-size":str(size),"x-amz-meta-kind":kind}}
    def get(self, reference, *, maximum):return DATA
    def size(self,reference,*,maximum):return len(DATA)
    def iter_bytes(self,reference,*,maximum,chunk_size):yield DATA
    def head_media_upload(self, key):
        return {"size":len(DATA),"mime":"image/png","metadata":{
            "sha256":DIGEST,"size":str(len(DATA)),"kind":"image"}}
    def verify_media_upload(self, key, *, digest, expected_size):
        assert digest==DIGEST and expected_size==len(DATA);self.uploads+=1


class HttpClient:
    async def __aenter__(self):return self
    async def __aexit__(self,*_):return None
    async def put(self,*_,**__):return SimpleNamespace(is_success=True,status_code=200)


class Ark:
    configured=True
    def __init__(self):self.creates=0;self.gets=0
    async def find(self,digest):return None
    async def create(self,reference):self.creates+=1;return "asset-test"
    async def get(self,asset_id):
        self.gets+=1;return {"Status":"Processing" if self.gets==1 else "Active"}


def settings(**values):
    base={"ark_control_endpoint":"https://ark.test","ark_access_key":"ak","ark_secret_key":"sk",
        "ark_project_name":"default","ark_asset_group_id":"group","ark_asset_group_type":"AIGC"}
    base.update(values);return SimpleNamespace(**base)


async def job(store, owner="worker"):
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="Publisher"),profile,"a"*64)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message="generate",
        idempotency_key="publisher-turn"),profile,"a"*64)
    reference={"project_file_id":"f"*32,"path":"assets/ref.png","version":1,
        "sha256":DIGEST,"size":len(DATA),"mime":"image/png","kind":"image",
        "object_ref":"sha256:"+DIGEST,"role":"reference_image","usage":"subject",
        "width":640,"height":480,"duration_seconds":None,"fps":None}
    spec=SimpleNamespace(provider="ark",adapter="ark_video_v2",execution="async",cancellation="unsupported")
    draft,_=await store.create_media_draft("a"*64,thread.id,MediaDraftCreate(kind="video",model="sed2",
        generation_mode="multimodal_reference",parameters={"prompt":"move","duration":5},
        references=[],idempotency_key="publisher-draft",turn_id=turn.id),
        expires_at=now()+timedelta(minutes=10),provider_spec=spec,reference_snapshots=[reference])
    created=await store.confirm_media_draft(draft.id,"a"*64,provider_spec=spec)
    claimed=await store.claim_media_jobs(owner)
    assert claimed[0]["id"]==created.id
    return claimed[0]


@pytest.mark.asyncio
async def test_reference_publisher_uploads_activates_and_reuses_cached_asset(tmp_path,monkeypatch):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher.db"));await store.initialize()
    try:
        value=await job(store);objects=Objects();publisher=MediaReferencePublisher(settings(),store,objects,"worker")
        ark=Ark();publisher.ark=ark
        monkeypatch.setattr("sandbox_harness.media_reference_publisher.httpx.AsyncClient",lambda **_:HttpClient())
        async def no_sleep(_):return None
        monkeypatch.setattr("sandbox_harness.media_reference_publisher.asyncio.sleep",no_sleep)
        ready=await publisher.prepare(value,require_ark=True)
        assert ready[0]["asset_uri"]=="asset://asset-test"
        assert ready[0]["public_url"].startswith("https://boss-shjd.biliapi.net/updream/aniforge/image/")
        assert objects.uploads==1 and ark.creates==1 and ark.gets==2
        repeated=await publisher.prepare(value,require_ark=True)
        assert repeated[0]["asset_uri"]=="asset://asset-test"
        assert objects.uploads==1 and ark.creates==1
    finally:await store.close()


@pytest.mark.asyncio
async def test_reference_publisher_fails_before_seedance_when_ark_credentials_missing(tmp_path,monkeypatch):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher-missing.db"));await store.initialize()
    try:
        value=await job(store);objects=Objects();publisher=MediaReferencePublisher(settings(
            ark_access_key="",ark_secret_key="",ark_asset_group_id=""),store,objects,"worker")
        monkeypatch.setattr("sandbox_harness.media_reference_publisher.httpx.AsyncClient",lambda **_:HttpClient())
        with pytest.raises(MediaReferencePreparationError,match="credentials_unavailable"):
            await publisher.prepare(value,require_ark=True)
        cached=await store.get_media_reference_asset(DIGEST)
        assert cached["boss_status"]=="active" and cached["ark_status"]=="pending"
        assert cached["lease_owner"]==""
    finally:await store.close()


@pytest.mark.asyncio
async def test_reference_asset_claim_prevents_duplicate_creators(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher-claim.db"));await store.initialize()
    try:
        reference={"sha256":DIGEST,"kind":"image","mime":"image/png","size":len(DATA),
            "object_ref":"sha256:"+DIGEST}
        await store.ensure_media_reference_asset(reference,object_key="aniforge/image/ref.png",
            public_url="https://boss-shjd.biliapi.net/updream/aniforge/image/ref.png")
        _,first=await store.claim_media_reference_asset(DIGEST,"worker-a")
        current,second=await store.claim_media_reference_asset(DIGEST,"worker-b")
        assert first is True and second is False and current["lease_owner"]=="worker-a"
    finally:await store.close()


@pytest.mark.asyncio
async def test_reference_asset_creation_is_atomic_for_parallel_provider_jobs(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher-create-race.db"));await store.initialize()
    reference={"sha256":DIGEST,"kind":"image","mime":"image/png","size":len(DATA),
        "object_ref":"sha256:"+DIGEST}
    try:
        values=await asyncio.gather(*(store.ensure_media_reference_asset(reference,
            object_key="aniforge/image/ref.png",
            public_url="https://boss-shjd.biliapi.net/updream/aniforge/image/ref.png")
            for _ in range(8)))
        assert {value["sha256"] for value in values}=={DIGEST}
        async with store.connections() as db:
            rows=(await db.execute(select(MediaReferenceAssetRow))).scalars().all()
        assert len(rows)==1
    finally:await store.close()


@pytest.mark.asyncio
async def test_reference_publisher_recovers_lost_create_receipt_by_asset_name(tmp_path,monkeypatch):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher-recover.db"));await store.initialize()
    try:
        value=await job(store);publisher=MediaReferencePublisher(settings(),store,Objects(),"worker")
        class ExistingArk:
            configured=True
            async def find(self,digest):return {"Id":"asset-recovered","Status":"Active"}
            async def create(self,reference):raise AssertionError("must reuse recovered asset")
            async def get(self,asset_id):return {"Status":"Active"}
        publisher.ark=ExistingArk()
        monkeypatch.setattr("sandbox_harness.media_reference_publisher.httpx.AsyncClient",lambda **_:HttpClient())
        ready=await publisher.prepare(value,require_ark=True)
        assert ready[0]["asset_uri"]=="asset://asset-recovered"
    finally:await store.close()


@pytest.mark.asyncio
async def test_reference_publisher_stops_before_seedance_on_ark_failed_status(tmp_path,monkeypatch):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"publisher-failed.db"));await store.initialize()
    try:
        value=await job(store);publisher=MediaReferencePublisher(settings(),store,Objects(),"worker")
        class FailedArk:
            configured=True
            async def find(self,digest):return None
            async def create(self,reference):return "asset-failed"
            async def get(self,asset_id):return {"Status":"Failed","Error":{"Code":"InvalidMedia"}}
        publisher.ark=FailedArk()
        monkeypatch.setattr("sandbox_harness.media_reference_publisher.httpx.AsyncClient",lambda **_:HttpClient())
        with pytest.raises(MediaReferencePreparationError,match="InvalidMedia"):
            await publisher.prepare(value,require_ark=True)
        cached=await store.get_media_reference_asset(DIGEST)
        assert cached["ark_status"]=="failed" and cached["ark_error"]=="InvalidMedia"
    finally:await store.close()
