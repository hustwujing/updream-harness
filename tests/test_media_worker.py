from types import SimpleNamespace

import pytest

from sandbox_harness.media_worker import MediaWorker
from sandbox_harness.media_reference_publisher import MediaReferencePreparationError


class Response:
    def __init__(self, body, status_code=200):
        self.body = body
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300

    def json(self):
        return self.body


class Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.posts = []
        self.gets = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.responses.pop(0)

    async def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return self.responses.pop(0)


class Store:
    def __init__(self):
        self.updates = []

    async def update_media_job(self, job_id, owner, **values):
        self.updates.append((job_id, owner, values))
        return True


class Objects:
    def signed_get_url(self, reference, *, expires_seconds):
        return "https://objects.test/" + reference.removeprefix("sha256:")
    def get(self, reference, *, maximum):
        return b"\x89PNG\r\n\x1a\nreference"


def worker(store=None, objects=None):
    settings = SimpleNamespace(upstream_image="http://image.test", upstream_video="http://video.test",
        upstream_audio="http://audio.test", ark_control_endpoint="https://ark.test",
        ark_access_key="", ark_secret_key="", ark_project_name="default",
        ark_asset_group_id="", ark_asset_group_type="AIGC",
        tripo_base_url="https://tripo.test/v3", tripo_api_key="test-key",
        tripo_poll_seconds=1, tripo_task_timeout_seconds=3,
        tripo_worker_concurrency=2, tripo_max_output_bytes=1024 * 1024)
    return MediaWorker(settings, store or Store(), objects or Objects(), None)


@pytest.mark.asyncio
async def test_video_recovery_polls_persisted_provider_job_without_resubmit(monkeypatch):
    value = worker()
    client = Client([Response({"status": "succeeded", "content": {"video_url": "https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep", no_sleep)
    result = await value._ark_video({"id": "job", "model": "sed2", "provider_job_id": "provider-video",
        "parameters": {"prompt": "test", "duration": 5}})
    assert result.status == "success" and result.provider_job_id == "provider-video"
    assert not client.posts and len(client.gets) == 1


@pytest.mark.asyncio
async def test_audio_recovery_polls_persisted_provider_job_without_resubmit(monkeypatch):
    value = worker()
    client = Client([
        Response({"status": "success", "file_id": "file"}),
        Response({"file": {"download_url": "https://result.test/audio.mp3"}}),
    ])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep", no_sleep)
    result = await value._minimax_audio({"id": "job", "model": "speech-2.8-turbo",
        "provider_job_id": "provider-audio", "parameters": {"text": "test"}})
    assert result.status == "success" and result.provider_job_id == "provider-audio"
    assert not client.posts and len(client.gets) == 2


@pytest.mark.asyncio
async def test_image_provider_failure_preserves_safe_http_and_business_code(monkeypatch):
    value = worker(); client = Client([Response({"error":{"code":"InvalidParameter"}}, 400)])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    result = await value._ark_image({"id":"job", "model":"seedream-5.0",
        "parameters":{"prompt":"test", "size":"2560x1440"}})
    assert result.status == "failed" and result.error == "image_http_400:InvalidParameter"


@pytest.mark.asyncio
async def test_seedream_reference_payload_uses_frozen_object_urls(monkeypatch):
    value = worker(); client = Client([Response({"data":[{"url":"https://result.test/image.png"}]})])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    refs=[{"object_ref":"sha256:"+"1"*64,"role":"reference_image","usage":"subject","mime":"image/png","size":100,"public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/one.png"},
        {"object_ref":"sha256:"+"2"*64,"role":"reference_image","usage":"style","mime":"image/png","size":100,"public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/two.png"}]
    result=await value._ark_image({"id":"job","model":"seedream-5.0",
        "generation_mode":"reference_image","references":refs,
        "parameters":{"prompt":"merge","size":"2K"}})
    payload=client.posts[0][1]["json"]
    assert result.status=="success" and payload["image"]==[refs[0]["public_url"],refs[1]["public_url"]]
    assert payload["sequential_image_generation"]=="disabled"


@pytest.mark.asyncio
async def test_seedance_edit_payload_orders_source_and_compiles_constraints(monkeypatch):
    value=worker();client=Client([Response({"id":"provider-job"}),
        Response({"status":"succeeded","content":{"video_url":"https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep",no_sleep)
    references=[
        {"object_ref":"sha256:"+"1"*64,"role":"reference_image","usage":"style","mime":"image/png","size":100,"asset_uri":"asset://asset-image-1"},
        {"object_ref":"sha256:"+"2"*64,"role":"reference_video","usage":"edit_source","mime":"video/mp4","size":100,"asset_uri":"asset://asset-video-1"},
    ]
    result=await value._ark_video({"id":"job","model":"sed2","provider_job_id":"",
        "generation_mode":"edit_video","references":references,
        "parameters":{"prompt":"remove the logo","duration":-1,"aspect_ratio":"adaptive"}})
    payload=client.posts[0][1]["json"]
    assert result.status=="success" and payload["duration"]==-1 and payload["ratio"]=="adaptive"
    assert payload["content"][0]["text"].startswith("严格编辑视频1")
    assert payload["content"][1]["type"]=="video_url" and payload["content"][1]["role"]=="reference_video"
    assert payload["content"][1]["video_url"]["url"]=="asset://asset-video-1"
    assert payload["content"][2]["type"]=="image_url"


@pytest.mark.asyncio
async def test_seedance_first_last_frame_payload_preserves_roles(monkeypatch):
    value=worker();client=Client([Response({"id":"provider-job"}),
        Response({"status":"succeeded","video_url":"https://result.test/video.mp4"})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep",no_sleep)
    refs=[{"object_ref":"sha256:"+"3"*64,"role":"first_frame","usage":"composition","mime":"image/png","size":100,"asset_uri":"asset://asset-first"},
        {"object_ref":"sha256:"+"4"*64,"role":"last_frame","usage":"composition","mime":"image/png","size":100,"asset_uri":"asset://asset-last"}]
    await value._ark_video({"id":"job","model":"sed2","provider_job_id":"",
        "generation_mode":"first_last_frame","references":refs,
        "parameters":{"prompt":"transition","duration":5,"aspect_ratio":"adaptive"}})
    content=client.posts[0][1]["json"]["content"]
    assert [item.get("role") for item in content[1:]]==["first_frame","last_frame"]


@pytest.mark.asyncio
@pytest.mark.parametrize("url",[
    "http://shjd-boss.bilibili.co/updream/aniforge/image/ref.png",
    "https://boss-shjd.biliapi.net/updream/aniforge/image/ref.png?X-Amz-Signature=secret",
])
async def test_provider_reference_rejects_internal_or_presigned_urls(url):
    value=worker()
    with pytest.raises(MediaReferencePreparationError,match="unstable_or_internal"):
        await value._reference_url({"public_url":url})


@pytest.mark.asyncio
async def test_requesting_job_without_provider_id_becomes_unknown(monkeypatch):
    store = Store(); value = worker(store)
    async def should_not_execute(_):
        raise AssertionError("ambiguous provider request must not be replayed")
    monkeypatch.setattr(value, "_execute", should_not_execute)
    await value.process({"id": "job", "status": "running", "phase": "requesting",
        "provider_job_id": "", "provider": "ark", "model": "seedream-5.0"})
    assert store.updates[-1][2]["status"] == "unknown"
    assert store.updates[-1][2]["error_code"] == "provider_submission_outcome_unknown"


@pytest.mark.asyncio
async def test_reference_preparation_phase_is_safe_to_resume(monkeypatch):
    store=Store();value=worker(store)
    async def failed(_):return __import__('sandbox_harness.media_worker',fromlist=['ProviderResult']).ProviderResult(
        "failed",error="reference preparation resumed")
    monkeypatch.setattr(value,"_execute",failed)
    await value.process({"id":"job","kind":"video","model":"sed2","provider":"ark",
        "adapter":"ark_video_v2","status":"running","phase":"preparing_references",
        "provider_job_id":"","references":[{"sha256":"1"*64}]})
    assert store.updates[-1][2]["status"]=="failed"
    assert store.updates[-1][2]["error_code"]=="provider_failed"
    assert all(update[2].get("error_code")!="provider_submission_outcome_unknown" for update in store.updates)


def test_media_worker_lease_owner_is_instance_unique():
    first = worker(); second = worker()
    assert first.owner.startswith("media-worker-") and first.owner != second.owner


@pytest.mark.asyncio
async def test_media_worker_refills_free_slot_without_waiting_for_long_batch(monkeypatch):
    class QueueStore(Store):
        def __init__(self):
            super().__init__(); self.jobs = [{"id": value} for value in ("long", "quick", "next")]
        async def claim_media_jobs(self, owner, limit, **filters):
            if filters.get("adapter"):
                return []
            claimed, self.jobs = self.jobs[:limit], self.jobs[limit:]
            return claimed
    store = QueueStore(); value = worker(store); value.concurrency = 2
    started, release = [], __import__('asyncio').Event()
    async def process(job):
        started.append(job["id"])
        if job["id"] == "long": await release.wait()
        if job["id"] == "next": value.stop_event.set()
    monkeypatch.setattr(value, "process", process)
    await __import__('asyncio').wait_for(value.run(), 2)
    assert started[:2] == ["long", "quick"] and "next" in started


@pytest.mark.asyncio
async def test_tripo_submission_persists_task_and_returns_model_and_preview(monkeypatch):
    value = worker()
    client = Client([Response({"code": 0, "data": {"task_id": "tripo-task"}}),
        Response({"code": 0, "data": {"status": "success", "progress": 100,
            "output": {"model_url": "https://result.test/model.glb",
                "rendered_image_url": "https://result.test/preview.png"},
            "credits_consumed": 10}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "model": "v3.1-20260211",
        "provider_job_id": "", "generation_mode": "text_to_model",
        "parameters": {"prompt": "a small cube", "texture": False, "pbr": False}})
    assert result.status == "success" and result.provider_job_id == "tripo-task"
    assert [item["role"] for item in result.outputs] == ["primary", "preview"]
    assert result.credits_consumed == 10
    assert value.store.updates[-1][2]["provider_job_id"] == "tripo-task"
    assert client.posts[0][1]["json"] == {"prompt": "a small cube", "texture": False,
        "pbr": False, "model": "v3.1-20260211"}


@pytest.mark.asyncio
async def test_tripo_recovery_only_polls_persisted_task(monkeypatch):
    value = worker()
    client = Client([Response({"code": 0, "data": {"status": "success",
        "output": {"model_url": "https://result.test/model.glb"}}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "model": "v3.1-20260211",
        "provider_job_id": "persisted-task", "generation_mode": "text_to_model",
        "parameters": {"prompt": "ignored"}})
    assert result.status == "success" and not client.posts and len(client.gets) == 1


def test_tripo_glb_signature_is_required():
    assert MediaWorker._validated_mime(b"glTF" + b"\0" * 16,
        "application/octet-stream", "model3d") == "model/gltf-binary"
    with pytest.raises(ValueError, match="mime_mismatch"):
        MediaWorker._validated_mime(b"<html>not a model</html>",
            "application/octet-stream", "model3d")


@pytest.mark.parametrize(("payload", "kind", "mime"), [
    (b"Kaydara FBX Binary  \x00" + b"\0" * 32, "model3d:fbx", "application/vnd.autodesk.fbx"),
    (b"# mesh\nv 0 0 0\nv 1 0 0\nf 1 2 1\n", "model3d:obj", "model/obj"),
    (b"solid mesh\nendsolid mesh\n", "model3d:stl", "model/stl"),
    (b"PK\x03\x04" + b"\0" * 32, "model3d:usdz", "model/vnd.usdz+zip"),
])
def test_tripo_converted_model_signatures(payload, kind, mime):
    assert MediaWorker._validated_mime(payload, "application/octet-stream", kind) == mime


@pytest.mark.parametrize("url", [
    "http://cdn.tripo3d.ai/model.glb",
    "https://127.0.0.1/model.glb",
    "https://shjd-boss.bilibili.co/model.glb",
    "https://user:secret@cdn.tripo3d.ai/model.glb",
])
def test_tripo_output_url_must_be_public_https(url):
    with pytest.raises(ValueError, match="not_public"):
        MediaWorker._validated_tripo_output_url(url)


def test_tripo_output_url_accepts_public_cdn():
    assert MediaWorker._validated_tripo_output_url("https://cdn.tripo3d.ai/model.glb?token=short-lived")
