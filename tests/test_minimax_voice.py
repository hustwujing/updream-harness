import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from sandbox_harness.services.media_worker.minimax_voice import (MiniMaxVoiceClones)


class Response:
    def __init__(self, body, status_code=200):
        self.body = body; self.status_code = status_code
        self.is_success = 200 <= status_code < 300

    def json(self):
        return self.body


class Client:
    def __init__(self, responses):
        self.responses = list(responses); self.posts = []

    async def __aenter__(self): return self
    async def __aexit__(self, *_): return None

    async def post(self, url, **kwargs):
        self.posts.append((url, kwargs)); return self.responses.pop(0)


class Objects:
    def __init__(self, data): self.data = data
    def get(self, reference, *, maximum):
        assert reference.startswith("sha256:") and len(self.data) <= maximum
        return self.data


class Store:
    def __init__(self): self.record = None; self.job_updates = []; self.touches = 0

    async def ensure_media_voice_clone(self, owner_id, reference):
        if self.record is None:
            self.record = {"id":"clone", "owner_id":owner_id, "sha256":reference["sha256"],
                "mime":reference["mime"], "size":reference["size"],
                "object_ref":reference["object_ref"], "voice_id":"updream_test_voice",
                "provider_file_id":"", "revision":1, "status":"pending", "error_code":"",
                "lease_owner":"", "lease_expires_at":None, "last_used_at":None}
        return dict(self.record)

    async def claim_media_voice_clone(self, owner_id, digest, lease_owner, lease_seconds=120):
        self.record["lease_owner"] = lease_owner
        return dict(self.record), True

    async def update_media_voice_clone(self, owner_id, digest, lease_owner, **values):
        assert self.record["lease_owner"] == lease_owner
        for key in ("status", "provider_file_id", "error_code"):
            if key in values: self.record[key] = values[key]
        if values.get("release"): self.record["lease_owner"] = ""
        return dict(self.record)

    async def get_media_voice_clone(self, owner_id, digest): return dict(self.record)

    async def rotate_media_voice_clone(self, owner_id, digest, lease_owner):
        assert self.record["lease_owner"] == lease_owner
        self.record["revision"] += 1; self.record["voice_id"] = "updream_rotated_voice"
        self.record["provider_file_id"] = ""; self.record["status"] = "pending"
        return dict(self.record)

    async def touch_media_voice_clone(self, owner_id, digest, voice_id):
        assert voice_id == self.record["voice_id"]
        self.record["status"] = "active"
        self.record["last_used_at"] = datetime.now(timezone.utc)
        self.touches += 1
        return True

    async def update_media_job(self, job_id, owner, generation, **values):
        self.job_updates.append(values); return True


def reference(data):
    digest = hashlib.sha256(data).hexdigest()
    return {"sha256":digest, "size":len(data), "mime":"audio/mpeg",
        "object_ref":"sha256:" + digest, "role":"reference_audio", "usage":"audio"}


@pytest.mark.asyncio
async def test_voice_clone_uploads_once_and_reuses_active_cache(monkeypatch):
    data = b"synthetic speech bytes"; store = Store()
    value = MiniMaxVoiceClones(SimpleNamespace(upstream_audio="https://audio.test/v1"),
        store, Objects(data), "worker")
    upload = Client([Response({"file":{"file_id":123},"base_resp":{"status_code":0}})])
    clone = Client([Response({"demo_audio":"https://result.test/demo.mp3",
        "base_resp":{"status_code":0}})])
    clients = [upload, clone]
    async def factory(): return clients.pop(0)
    monkeypatch.setattr(value, "_client", factory)
    job={"id":"job","generation":1, "owner_id":"a"*64, "model":"speech-2.8-turbo",
        "references":[reference(data)]}
    voice_id = await value.prepare(job)
    assert voice_id == "updream_test_voice" and store.record["status"] == "active"
    assert store.touches == 1 and store.record["provider_file_id"] == "123"
    assert upload.posts[0][1]["data"] == {"purpose":"voice_clone"}
    assert clone.posts[0][1]["json"]["voice_id"] == voice_id
    assert clone.posts[0][1]["json"]["model"] == "speech-2.8-turbo"
    async def must_not_call(): raise AssertionError("active clone must be reused")
    monkeypatch.setattr(value, "_client", must_not_call)
    assert await value.prepare(job) == voice_id


@pytest.mark.asyncio
async def test_uncertain_clone_reconciles_by_deterministic_voice_id(monkeypatch):
    data=b"synthetic speech bytes";store=Store();ref=reference(data)
    await store.ensure_media_voice_clone("a"*64,ref)
    store.record.update({"status":"uncertain","provider_file_id":"123",
        "lease_owner":"","last_used_at":None})
    value=MiniMaxVoiceClones(SimpleNamespace(upstream_audio="https://audio.test/v1"),
        store,Objects(data),"worker")
    async def exists(voice_id): return voice_id=="updream_test_voice"
    monkeypatch.setattr(value,"voice_exists",exists)
    result=await value.prepare({"id":"job","generation":1,"owner_id":"a"*64,"model":"speech-2.8-turbo",
        "references":[ref]})
    assert result=="updream_test_voice" and store.record["status"]=="active"
    assert store.record["lease_owner"]==""


@pytest.mark.asyncio
async def test_expired_active_clone_rotates_voice_id_before_recreation(monkeypatch):
    data=b"synthetic speech bytes";store=Store();ref=reference(data)
    await store.ensure_media_voice_clone("a"*64,ref)
    store.record.update({"status":"active","provider_file_id":"old-file",
        "last_used_at":datetime.fromtimestamp(0,timezone.utc)})
    value=MiniMaxVoiceClones(SimpleNamespace(upstream_audio="https://audio.test/v1"),
        store,Objects(data),"worker")
    async def missing(voice_id):return False
    monkeypatch.setattr(value,"voice_exists",missing)
    upload=Client([Response({"file":{"file_id":456},"base_resp":{"status_code":0}})])
    clone=Client([Response({"demo_audio":"https://result.test/demo.mp3",
        "base_resp":{"status_code":0}})])
    clients=[upload,clone]
    async def factory():return clients.pop(0)
    monkeypatch.setattr(value,"_client",factory)
    result=await value.prepare({"id":"job","generation":1,"owner_id":"a"*64,"model":"speech-2.8-turbo",
        "references":[ref]})
    assert result=="updream_rotated_voice" and store.record["revision"]==2
    assert clone.posts[0][1]["json"]["voice_id"]=="updream_rotated_voice"


@pytest.mark.asyncio
async def test_duplicate_voice_id_rotates_once_and_retries_with_same_frozen_audio(monkeypatch):
    data=b"synthetic speech bytes";store=Store();ref=reference(data)
    value=MiniMaxVoiceClones(SimpleNamespace(upstream_audio="https://audio.test/v1"),
        store,Objects(data),"worker")
    async def missing(_):return False
    monkeypatch.setattr(value,"voice_exists",missing)
    upload_one=Client([Response({"file":{"file_id":123},"base_resp":{"status_code":0}})])
    duplicate=Client([Response({"demo_audio":"","base_resp":{"status_code":2039,
        "status_msg":"voice clone voice id duplicate"}})])
    upload_two=Client([Response({"file":{"file_id":456},"base_resp":{"status_code":0}})])
    clone_two=Client([Response({"demo_audio":"https://result.test/demo.mp3",
        "base_resp":{"status_code":0}})])
    clients=[upload_one,duplicate,upload_two,clone_two]
    async def factory():return clients.pop(0)
    monkeypatch.setattr(value,"_client",factory)
    result=await value.prepare({"id":"job","generation":1,"owner_id":"a"*64,
        "model":"speech-2.8-turbo","references":[ref]})
    assert result=="updream_rotated_voice"
    assert store.record["revision"]==2 and store.record["status"]=="active"
    assert duplicate.posts[0][1]["json"]["voice_id"]=="updream_test_voice"
    assert clone_two.posts[0][1]["json"]["voice_id"]=="updream_rotated_voice"
