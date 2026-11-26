from types import SimpleNamespace
from datetime import datetime, timezone

import httpx
import pytest

from sandbox_harness.services.media_worker.media_worker import (MediaWorker)
from sandbox_harness.services.media_worker.media_reference_publisher import (MediaReferencePreparationError)
from sandbox_harness.common.contracts.media_provider import ProviderResult


class Response:
    def __init__(self, body, status_code=200, headers=None):
        self.body = body
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self.headers = headers or {}

    def json(self):
        if isinstance(self.body, Exception): raise self.body
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
        response = self.responses.pop(0)
        if isinstance(response, Exception): raise response
        return response

    async def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception): raise response
        return response


class Store:
    def __init__(self):
        self.updates = []
        self.releases = []

    async def update_media_job(self, job_id, owner, generation, **values):
        values["generation"] = generation
        self.updates.append((job_id, owner, values))
        return True

    async def resolve_tripo_input_job(self, job_id, input_job_id):
        assert job_id == "job" and input_job_id == "a" * 32
        return "provider-source-task"

    async def release_media_job_lease(self, job_id, owner, generation, **values):
        values["generation"] = generation
        self.releases.append((job_id, owner, values))
        return True
    async def renew_media_job_lease(self,*args,**kwargs):return True
    async def persist_media_provider_result(self,job_id,owner,generation,value):return value
    async def persist_media_staged_output(self,job_id,owner,generation,value):return [value]
    async def complete_media_job(self,*args,**kwargs):return True


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
        tripo_worker_concurrency=2, tripo_submit_max_retries=5,
        tripo_max_output_bytes=1024 * 1024, audio_task_timeout_seconds=7200,
        video_task_timeout_seconds=7200,
        dashscope_video_create_url="https://dashscope.test/video-synthesis",
        dashscope_video_task_url="https://dashscope.test/tasks")
    return MediaWorker(settings, store or Store(), objects or Objects(), None)


@pytest.mark.asyncio
async def test_terminal_provider_result_cancelled_before_collection_never_calls_cancel_adapter(monkeypatch):
    class CancelAfterResultStore(Store):
        def __init__(self):super().__init__();self.cancel_checks=0
        async def media_cancel_requested(self,*args,**kwargs):
            self.cancel_checks+=1
            return self.cancel_checks>=2
    store=CancelAfterResultStore();value=worker(store)
    async def completed(_):return ProviderResult('success',url='https://result.test/image.png')
    async def unexpected_cancel(_):raise AssertionError('terminal result must not call cancel adapter')
    monkeypatch.setattr(value,'_execute',completed)
    monkeypatch.setattr(value,'_cancel_external',unexpected_cancel)
    await value._process_claimed({'id':'job','generation':1,'source_job_id':'source',
        'status':'created','phase':'prepared','adapter':'ark_image','provider':'ark',
        'model':'seedream-5.0','kind':'image','parameters':{},'references':[],
        'provider_job_id':'','provider_result':{},'staged_outputs':[]})
    assert store.updates[-1][2]['status']=='cancelled'
    assert store.updates[-1][2]['phase']=='terminal'


@pytest.mark.asyncio
async def test_seedream_uses_dedicated_image_request_timeout():
    value=worker();value.settings.image_task_timeout_seconds=321
    client=await value._image_client()
    try:
        assert client.timeout.connect==10 and client.timeout.read==321
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_video_recovery_polls_persisted_provider_job_without_resubmit(monkeypatch):
    value = worker()
    client = Client([Response({"status": "succeeded", "content": {"video_url": "https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._ark_video({"id": "job", "generation": 1, "model": "sed2", "provider_job_id": "provider-video",
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
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._minimax_audio({"id": "job", "generation": 1, "model": "speech-2.8-turbo",
        "provider_job_id": "provider-audio", "parameters": {"text": "test"}})
    assert result.status == "success" and result.provider_job_id == "provider-audio"
    assert not client.posts and len(client.gets) == 2


@pytest.mark.asyncio
async def test_audio_reference_uses_host_derived_cloned_voice(monkeypatch):
    value=worker();submitted=Client([Response({"task_id":"provider-audio"})])
    polling=Client([Response({"status":"success","file_id":"file"})])
    retrieved=Client([Response({"file":{"download_url":"https://result.test/audio.mp3"}})])
    clients=[submitted,polling,retrieved]
    async def client_factory():return clients.pop(0)
    async def no_sleep(_):return None
    class Voices:
        def __init__(self):self.touched=[]
        async def prepare(self,job):return "updream_cloned_voice"
        async def touch(self,job,voice_id):self.touched.append(voice_id)
        async def voice_exists(self,voice_id):return True
        async def invalidate(self,job,voice_id):raise AssertionError("active clone must not be invalidated")
    voices=Voices();value.voice_clones=voices
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep",no_sleep)
    result=await value._minimax_audio({"id":"job","generation":1,"owner_id":"a"*64,
        "model":"speech-2.8-turbo","provider_job_id":"","parameters":{"text":"hello"},
        "references":[{"sha256":"1"*64,"role":"reference_audio","usage":"audio"}]})
    assert result.status=="success"
    assert submitted.posts[0][1]["json"]["voice_setting"]["voice_id"]=="updream_cloned_voice"
    assert voices.touched==["updream_cloned_voice"]


@pytest.mark.asyncio
async def test_audio_submit_ambiguous_provider_failure_is_unknown(monkeypatch):
    value=worker();client=Client([Response({"base_resp":{"status_code":1013}},500)])
    async def client_factory():return client
    monkeypatch.setattr(value,"_client",client_factory)
    result=await value._minimax_audio({"id":"job","generation":1,"owner_id":"a"*64,
        "model":"speech-2.8-turbo","provider_job_id":"",
        "parameters":{"text":"hello"},"references":[]})
    assert result.status=="unknown" and not result.provider_job_id
    assert result.error=="audio_submission_outcome_unknown:1013"


@pytest.mark.asyncio
async def test_audio_processing_keeps_polling_same_provider_job(monkeypatch):
    value = worker()
    processing = Client([Response({"status":"processing"})])
    completed = Client([Response({"status":"success", "file_id":"file"}),
        Response({"file":{"download_url":"https://result.test/audio.mp3"}})])
    clients = [processing, completed, completed]
    async def client_factory(): return clients.pop(0)
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    job={"id":"job","generation":1, "model":"speech-2.8-turbo", "provider_job_id":"provider-audio",
        "provider_started_at":datetime.now(timezone.utc), "parameters":{"text":"test"}}
    pending = await value._minimax_audio(job)
    assert pending.status == "pending" and pending.provider_job_id == "provider-audio"
    result = await value._minimax_audio(job)
    assert result.status == "success" and result.provider_job_id == "provider-audio"
    assert not processing.posts and not completed.posts


@pytest.mark.asyncio
async def test_audio_processing_past_reconciliation_window_returns_unknown(monkeypatch):
    value = worker()
    client = Client([Response({"status": "processing"})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._minimax_audio({"id": "job", "generation": 1, "model": "speech-2.8-turbo",
        "provider_job_id": "provider-audio",
        "provider_started_at": datetime.fromtimestamp(0, timezone.utc),
        "parameters": {"text": "test"}})
    assert result.status == "unknown"
    assert result.provider_job_id == "provider-audio"
    assert result.error == "audio_processing_outlived_reconciliation_window"
    assert not client.posts and len(client.gets) == 1


@pytest.mark.asyncio
async def test_audio_poll_http_failure_past_reconciliation_window_returns_unknown(monkeypatch):
    value = worker()
    client = Client([Response({}, status_code=503)])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._minimax_audio({"id": "job", "generation": 1, "model": "speech-2.8-turbo",
        "provider_job_id": "provider-audio",
        "provider_started_at": datetime.fromtimestamp(0, timezone.utc),
        "parameters": {"text": "test"}})
    assert result.status == "unknown" and result.provider_job_id == "provider-audio"
    assert result.error == "audio_poll_http_503_after_reconciliation_window"


@pytest.mark.asyncio
async def test_audio_poll_authentication_failure_is_terminal(monkeypatch):
    value=worker();client=Client([Response({},status_code=401)])
    async def client_factory():return client
    async def no_sleep(_):return None
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep",no_sleep)
    result=await value._minimax_audio({"id":"job","generation":1,"model":"speech-2.8-turbo",
        "provider_job_id":"provider-audio","provider_started_at":datetime.now(timezone.utc),
        "parameters":{"text":"test"}})
    assert result.status=="failed" and result.error_code=="provider_authentication_failed"


@pytest.mark.asyncio
async def test_image_provider_failure_preserves_safe_http_and_business_code(monkeypatch):
    value = worker(); client = Client([Response({"error":{"code":"InvalidParameter"}}, 400)])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    result = await value._ark_image({"id":"job","generation":1, "model":"seedream-5.0",
        "parameters":{"prompt":"test", "size":"2560x1440"}})
    assert result.status == "failed" and result.error == "image_http_400:InvalidParameter"
    assert result.error_code == "provider_input_invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize(('requested','provider_size'),(
    ('2560x1920','2K'),('3072x2048','3K'),('3K','3K')))
async def test_seedream_direct_model_uses_supported_size_tiers(monkeypatch,requested,provider_size):
    value=worker();client=Client([Response({'data':[{'url':'https://result.test/image.png'}]})])
    async def client_factory():return client
    monkeypatch.setattr(value,'_client',client_factory)
    monkeypatch.delenv('SANDBOX_HARNESS_ARK_IMAGE_MODEL_SEEDREAM_5_0',raising=False)
    result=await value._ark_image({'id':'job','generation':1,'model':'seedream-5.0',
        'parameters':{'prompt':'test','size':requested}})
    payload=client.posts[0][1]['json']
    assert result.status=='success'
    assert payload['model']=='doubao-seedream-5-0-pro-260628'
    assert payload['size']==provider_size


@pytest.mark.asyncio
@pytest.mark.parametrize('requested',('4K','3840x2160'))
async def test_seedream_rejects_size_outside_deployed_model_contract(monkeypatch,requested):
    value=worker();client=Client([Response({'data':[{'url':'https://result.test/image.png'}]})])
    async def client_factory():return client
    monkeypatch.setattr(value,'_client',client_factory)
    with pytest.raises(ValueError,match='deployed model'):
        await value._ark_image({'id':'job','generation':1,'model':'seedream-5.0',
            'parameters':{'prompt':'test','size':requested}})
    assert client.posts==[]


@pytest.mark.asyncio
async def test_image_rate_limit_is_deferred_with_stable_error(monkeypatch):
    value=worker();client=Client([Response({"error":{"code":"RateLimitExceeded"}},429)])
    async def client_factory():return client
    monkeypatch.setattr(value,"_client",client_factory)
    result=await value._ark_image({"id":"job","generation":1,"model":"seedream-5.0",
        "parameters":{"prompt":"test","size":"2K"}})
    assert result.status=="pending" and result.error_code=="provider_rate_limited"


@pytest.mark.asyncio
async def test_image_service_failure_without_result_is_ambiguous_not_retried(monkeypatch):
    value=worker();client=Client([Response({"error":{"code":"ServiceUnavailable"}},503)])
    async def client_factory():return client
    monkeypatch.setattr(value,"_client",client_factory)
    result=await value._ark_image({"id":"job","generation":1,"model":"seedream-5.0",
        "parameters":{"prompt":"test","size":"2K"}})
    assert result.status=="unknown"
    assert result.error_code=="provider_submission_outcome_unknown"


@pytest.mark.asyncio
async def test_seedream_reference_payload_uses_frozen_object_urls(monkeypatch):
    value = worker(); client = Client([Response({"data":[{"url":"https://result.test/image.png"}]})])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    refs=[{"object_ref":"sha256:"+"1"*64,"role":"reference_image","usage":"subject","mime":"image/png","size":100,"public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/one.png"},
        {"object_ref":"sha256:"+"2"*64,"role":"reference_image","usage":"style","mime":"image/png","size":100,"public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/two.png"}]
    result=await value._ark_image({"id":"job","generation":1,"model":"seedream-5.0",
        "generation_mode":"reference_image","references":refs,
        "parameters":{"prompt":"merge","size":"2K"}})
    payload=client.posts[0][1]["json"]
    assert result.status=="success" and payload["image"]==[refs[0]["public_url"],refs[1]["public_url"]]
    assert "sequential_image_generation" not in payload


@pytest.mark.asyncio
async def test_seedance_edit_payload_orders_source_and_compiles_constraints(monkeypatch):
    value=worker();client=Client([Response({"id":"provider-job"}),
        Response({"status":"succeeded","content":{"video_url":"https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep",no_sleep)
    references=[
        {"object_ref":"sha256:"+"1"*64,"role":"reference_image","usage":"style","mime":"image/png","size":100,"asset_uri":"asset://asset-image-1"},
        {"object_ref":"sha256:"+"2"*64,"role":"reference_video","usage":"edit_source","mime":"video/mp4","size":100,"asset_uri":"asset://asset-video-1"},
    ]
    result=await value._ark_video({"id":"job","generation":1,"model":"sed2","provider_job_id":"",
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
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep",no_sleep)
    refs=[{"object_ref":"sha256:"+"3"*64,"role":"first_frame","usage":"composition","mime":"image/png","size":100,"asset_uri":"asset://asset-first"},
        {"object_ref":"sha256:"+"4"*64,"role":"last_frame","usage":"composition","mime":"image/png","size":100,"asset_uri":"asset://asset-last"}]
    await value._ark_video({"id":"job","generation":1,"model":"sed2","provider_job_id":"",
        "generation_mode":"first_last_frame","references":refs,
        "parameters":{"prompt":"transition","duration":5,"aspect_ratio":"adaptive"}})
    content=client.posts[0][1]["json"]["content"]
    assert [item.get("role") for item in content[1:]]==["first_frame","last_frame"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("adapter", "model_id"), [
    ("happyhorse_video_edit", "happyhorse-1.0-video-edit"),
    ("wan27_video_edit", "wan2.7-videoedit"),
])
async def test_dashscope_video_edit_submits_persists_and_returns_output(
        monkeypatch, adapter, model_id):
    value = worker()
    client = Client([Response({"output": {"task_id": "provider-video"}}),
        Response({"output": {"task_status": "SUCCEEDED",
            "video_url": "https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    references = [
        {"kind": "video", "role": "reference_video", "usage": "edit_source",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"},
        {"kind": "image", "role": "reference_image", "usage": "style",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/image/style.png"},
    ]
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": adapter,
        "provider_job_id": "", "generation_mode": "edit_video", "references": references,
        "parameters": {"prompt": "sync lips", "resolution": "720p"}})
    payload = client.posts[0][1]["json"]
    assert result.status == "success" and result.provider_job_id == "provider-video"
    assert payload["model"] == model_id
    assert payload["input"]["media"] == [
        {"type": "video", "url": references[0]["public_url"]},
        {"type": "reference_image", "url": references[1]["public_url"]},
    ]
    assert payload["input"]["prompt"].startswith("严格编辑视频1")
    assert payload["parameters"]["resolution"] == "720P"
    assert payload["parameters"]["audio_setting"] == "origin"
    assert value.store.updates[-1][2]["provider_job_id"] == "provider-video"


@pytest.mark.asyncio
async def test_dashscope_non_success_receipt_with_task_id_is_persisted(monkeypatch):
    value = worker()
    client = Client([Response({"output": {"task_id": "accepted-task"}}, 503),
        Response({"output": {"task_status": "RUNNING"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "wan27_video_edit",
        "provider_job_id": "", "references": [{"kind": "video", "role": "reference_video",
            "usage": "edit_source", "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "pending" and result.provider_job_id == "accepted-task"
    assert value.store.updates[-1][2]["provider_job_id"] == "accepted-task"


@pytest.mark.asyncio
async def test_dashscope_video_edit_recovery_only_polls_persisted_job(monkeypatch):
    value = worker()
    client = Client([Response({"output": {"task_status": "SUCCEEDED",
        "video_url": "https://result.test/video.mp4"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "happyhorse_video_edit",
        "provider_job_id": "persisted-task", "generation_mode": "edit_video",
        "references": [{"kind": "video", "role": "reference_video", "usage": "edit_source",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "sync lips", "resolution": "720p"}})
    assert result.status == "success" and not client.posts and len(client.gets) == 1


@pytest.mark.asyncio
async def test_dashscope_video_edit_submit_transport_failure_is_unknown(monkeypatch):
    value = worker(); client = Client([httpx.ReadTimeout("unknown submission")])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "happyhorse_video_edit",
        "provider_job_id": "", "references": [{"kind": "video", "role": "reference_video", "usage": "edit_source",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "unknown" and not result.provider_job_id
    assert result.error_code == "provider_submission_outcome_unknown"


@pytest.mark.asyncio
async def test_dashscope_video_edit_invalid_success_receipt_is_unknown(monkeypatch):
    value = worker(); client = Client([Response(ValueError("invalid json"))])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "happyhorse_video_edit",
        "provider_job_id": "", "references": [{"kind": "video", "role": "reference_video", "usage": "edit_source",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "unknown" and result.error_code == "provider_submission_outcome_unknown"


@pytest.mark.asyncio
async def test_dashscope_video_edit_invalid_poll_receipt_retries_same_job(monkeypatch):
    value = worker(); client = Client([Response(ValueError("invalid json"))])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "wan27_video_edit",
        "provider_job_id": "persisted", "references": [{"kind": "video", "role": "reference_video",
            "usage": "edit_source", "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "pending" and result.provider_job_id == "persisted"
    assert result.error_code == "provider_poll_unavailable" and not client.posts


@pytest.mark.asyncio
async def test_dashscope_video_edit_invalid_poll_past_window_becomes_unknown(monkeypatch):
    value = worker(); client = Client([Response(ValueError("invalid json"))])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "wan27_video_edit",
        "provider_job_id": "persisted", "provider_started_at": datetime.fromtimestamp(0, timezone.utc),
        "references": [{"kind": "video", "role": "reference_video", "usage": "edit_source",
            "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "unknown" and result.provider_job_id == "persisted"
    assert result.error_code == "provider_result_unresolved"


@pytest.mark.asyncio
async def test_dashscope_video_edit_provider_unknown_requires_reconciliation(monkeypatch):
    value = worker(); client = Client([Response({"output": {"task_status": "UNKNOWN"}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._dashscope_video_edit({"id": "job", "generation": 1, "adapter": "wan27_video_edit",
        "provider_job_id": "persisted", "references": [{"kind": "video", "role": "reference_video",
            "usage": "edit_source", "public_url": "https://boss-shjd.biliapi.net/updream/aniforge/video/source.mp4"}],
        "parameters": {"prompt": "edit"}})
    assert result.status == "unknown" and result.provider_job_id == "persisted"
    assert result.error_code == "provider_result_unresolved"


@pytest.mark.asyncio
async def test_video_poll_service_unavailable_releases_existing_provider_job(monkeypatch):
    value=worker();client=Client([Response({"error":{"code":"ServiceUnavailable"}},503)])
    async def client_factory():return client
    async def no_sleep(_):return None
    monkeypatch.setattr(value,"_client",client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep",no_sleep)
    result=await value._ark_video({"id":"job","generation":1,"model":"sed2",
        "provider_job_id":"provider-video","parameters":{"prompt":"test","duration":5},
        "references":[]})
    assert result.status=="pending" and result.provider_job_id=="provider-video"
    assert result.error_code=="provider_service_unavailable"


@pytest.mark.asyncio
async def test_video_submission_content_policy_is_not_retryable(monkeypatch):
    value=worker();client=Client([Response({"error":{
        "code":"OutputVideoSensitiveContentDetected.PolicyViolation"}},400)])
    async def client_factory():return client
    monkeypatch.setattr(value,"_client",client_factory)
    result=await value._ark_video({"id":"job","generation":1,"model":"sed2","provider_job_id":"",
        "parameters":{"prompt":"test","duration":5},"references":[]})
    assert result.status=="failed" and result.error_code=="provider_content_policy_rejected"


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
    await value.process({"id": "job", "generation": 1, "status": "running", "phase": "requesting",
        "provider_job_id": "", "provider": "ark", "model": "seedream-5.0"})
    assert store.updates[-1][2]["status"] == "unknown"
    assert store.updates[-1][2]["error_code"] == "provider_submission_outcome_unknown"


@pytest.mark.asyncio
async def test_known_provider_result_unknown_has_distinct_error_and_releases_lease(monkeypatch):
    store = Store(); value = worker(store)
    async def unresolved(_):
        return __import__('sandbox_harness.services.media_worker.media_worker', fromlist=['ProviderResult']).ProviderResult(
            "unknown", provider_job_id="provider-audio", error="still processing")
    monkeypatch.setattr(value, "_execute", unresolved)
    await value.process({"id":"job","generation":1, "kind":"audio", "model":"speech-2.8-turbo",
        "provider":"minimax", "adapter":"minimax_t2a", "status":"running",
        "phase":"provider_polling", "provider_job_id":"provider-audio", "references":[]})
    assert store.updates[-1][2]["status"] == "unknown"
    assert store.updates[-1][2]["error_code"] == "provider_result_unresolved"


@pytest.mark.asyncio
async def test_pending_provider_poll_releases_lease_with_delay(monkeypatch):
    store = Store(); value = worker(store)
    async def pending(_):
        return __import__('sandbox_harness.services.media_worker.media_worker', fromlist=['ProviderResult']).ProviderResult(
            "pending", provider_job_id="provider-audio", error="still processing")
    monkeypatch.setattr(value, "_execute", pending)
    await value.process({"id":"job","generation":1, "kind":"audio", "model":"speech-2.8-turbo",
        "provider":"minimax", "adapter":"minimax_t2a", "status":"running",
        "phase":"provider_polling", "provider_job_id":"provider-audio", "references":[]})
    assert store.releases[-1][2]["retry_after_seconds"] == 15


@pytest.mark.asyncio
async def test_audio_poll_transport_timeout_releases_lease_with_delay(monkeypatch):
    store = Store(); value = worker(store)
    async def timeout(_):
        raise httpx.ReadTimeout("provider query timed out")
    monkeypatch.setattr(value, "_execute", timeout)
    await value.process({"id":"job","generation":1, "kind":"audio", "model":"speech-2.8-turbo",
        "provider":"minimax", "adapter":"minimax_t2a", "status":"running",
        "phase":"provider_polling", "provider_job_id":"provider-audio",
        "provider_started_at":datetime.now(timezone.utc), "references":[]})
    assert store.releases[-1][2]["error_code"] == "provider_poll_unavailable"
    assert store.releases[-1][2]["retry_after_seconds"] == 15


@pytest.mark.asyncio
async def test_audio_poll_transport_timeout_past_window_becomes_unknown(monkeypatch):
    store = Store(); value = worker(store)
    async def timeout(_):
        raise httpx.ReadTimeout("provider query timed out")
    monkeypatch.setattr(value, "_execute", timeout)
    await value.process({"id":"job","generation":1, "kind":"audio", "model":"speech-2.8-turbo",
        "provider":"minimax", "adapter":"minimax_t2a", "status":"running",
        "phase":"provider_polling", "provider_job_id":"provider-audio",
        "provider_started_at":datetime.fromtimestamp(0, timezone.utc), "references":[]})
    assert store.updates[-1][2]["status"] == "unknown"
    assert store.updates[-1][2]["error_code"] == "provider_result_unresolved"


@pytest.mark.asyncio
async def test_reference_preparation_phase_is_safe_to_resume(monkeypatch):
    store=Store();value=worker(store)
    async def failed(_):return __import__('sandbox_harness.services.media_worker.media_worker',fromlist=['ProviderResult']).ProviderResult(
        "failed",error="reference preparation resumed")
    monkeypatch.setattr(value,"_execute",failed)
    await value.process({"id":"job","generation":1,"kind":"video","model":"sed2","provider":"ark",
        "adapter":"ark_video_v2","status":"running","phase":"preparing_references",
        "provider_job_id":"","references":[{"sha256":"1"*64}]})
    assert store.updates[-1][2]["status"]=="failed"
    assert store.updates[-1][2]["error_code"]=="provider_processing_failed"
    assert all(update[2].get("error_code")!="provider_submission_outcome_unknown" for update in store.updates)


@pytest.mark.asyncio
async def test_reference_policy_rejection_is_a_deterministic_failure_category(monkeypatch):
    store=Store();value=worker(store)
    async def rejected(_):
        raise MediaReferencePreparationError(
            "ark_asset_processing_failed:InputVideoSensitiveContentDetected.PolicyViolation")
    monkeypatch.setattr(value,"_execute",rejected)
    await value.process({"id":"job","generation":1,"kind":"video","model":"sed2","provider":"ark",
        "adapter":"ark_video_v2","status":"running","phase":"preparing_references",
        "provider_job_id":"","references":[{"sha256":"1"*64}]})
    assert store.updates[-1][2]["status"]=="failed"
    assert store.updates[-1][2]["error_code"]=="media_reference_policy_rejected"


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
        async def count_media_provider_jobs(self, adapter):
            return 0
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
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "generation": 1, "model": "v3.1-20260211",
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
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "provider_job_id": "persisted-task", "generation_mode": "text_to_model",
        "parameters": {"prompt": "ignored"}})
    assert result.status == "success" and not client.posts and len(client.gets) == 1


@pytest.mark.asyncio
async def test_tripo_model_url_suffix_overrides_default_glb_expectation(monkeypatch):
    value = worker()
    client = Client([Response({"code": 0, "data": {"status": "success",
        "output": {"model_url": "https://result.test/model.fbx",
            "rendered_image_url": "https://result.test/preview.webp"}}})])
    async def client_factory(): return client
    async def no_sleep(_): return None
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "provider_job_id": "persisted-task", "generation_mode": "text_to_model",
        "parameters": {"prompt": "phone"}})
    assert result.status == "success"
    assert result.outputs == [
        {"role": "primary", "filename": "model-job.fbx", "url": "https://result.test/model.fbx",
            "kind": "model3d:fbx"},
        {"role": "preview", "filename": "preview-job.webp", "url": "https://result.test/preview.webp",
            "kind": "image"},
    ]


@pytest.mark.asyncio
async def test_deterministic_provider_result_validation_error_is_explicit_failure(monkeypatch):
    store = Store(); value = worker(store)
    async def execute(_):
        return __import__('sandbox_harness.services.media_worker.media_worker', fromlist=['ProviderResult']).ProviderResult(
            "success", outputs=[{"role": "primary", "filename": "bad.glb",
                "url": "https://result.test/bad.glb", "kind": "model3d"}])
    async def invalid_download(*_args, **_kwargs):
        raise ValueError("provider_result_mime_mismatch")
    monkeypatch.setattr(value, "_execute", execute)
    monkeypatch.setattr(value, "_download", invalid_download)
    await value.process({"id": "job", "generation": 1, "kind": "model3d", "model": "v3.1-20260211",
        "provider": "tripo", "adapter": "tripo_v3", "status": "running",
        "phase": "provider_polling", "provider_job_id": "task", "references": []})
    assert store.updates[-1][2]["status"] == "failed"
    assert store.updates[-1][2]["error_code"] == "provider_result_invalid"


def test_tripo_glb_signature_is_required():
    assert MediaWorker._validated_mime(b"glTF" + b"\0" * 16,
        "application/octet-stream", "model3d") == "model/gltf-binary"
    with pytest.raises(ValueError, match="mime_mismatch"):
        MediaWorker._validated_mime(b"<html>not a model</html>",
            "application/octet-stream", "model3d")


@pytest.mark.parametrize(("payload", "kind", "mime"), [
    (b"Kaydara FBX Binary  \x00" + b"\0" * 32, "model3d:fbx", "application/vnd.autodesk.fbx"),
    (b"# mesh\nv 0 0 0\nv 1 0 0\nf 1 2 1\n", "model3d:obj", "model/obj"),
    (b"PK\x03\x04" + b"\0" * 32, "model3d:obj", "model/obj+zip"),
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


def test_provider_filename_uses_validated_content_mime_not_url_suffix():
    assert MediaWorker._filename_for_mime("preview-job.webp", "image/png") == "preview-job.png"
    assert MediaWorker._filename_for_mime("model-job.bin", "application/vnd.autodesk.fbx") == "model-job.fbx"
    assert MediaWorker._filename_for_mime("model-job.obj", "model/obj+zip") == "model-job.zip"


@pytest.mark.asyncio
async def test_tripo_image_and_multiview_payloads_use_stable_public_references(monkeypatch):
    value = worker()
    async def prepare(job, require_ark):
        assert require_ark is False
        return [{**item, "public_url": f"https://boss-shjd.biliapi.net/updream/aniforge/image/{item['role']}.png"}
            for item in job["references"]]
    monkeypatch.setattr(value.reference_publisher, "prepare", prepare)
    endpoint, payload = await value._tripo_payload({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "generation_mode": "image_to_model", "parameters": {"texture": False},
        "references": [{"kind": "image", "role": "reference_image"}]})
    assert endpoint == "generation/image-to-model"
    assert payload["input"].endswith("/reference_image.png") and payload["model"] == "v3.1-20260211"
    endpoint, payload = await value._tripo_payload({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "generation_mode": "multiview_to_model", "parameters": {}, "references": [
            {"kind": "image", "role": "front_view"}, {"kind": "image", "role": "right_view"}]})
    assert endpoint == "generation/multiview-to-model"
    assert payload["inputs"] == [{"front": "https://boss-shjd.biliapi.net/updream/aniforge/image/front_view.png"},
        {"right": "https://boss-shjd.biliapi.net/updream/aniforge/image/right_view.png"}]


@pytest.mark.asyncio
async def test_tripo_postprocess_resolves_owned_platform_job_to_provider_task():
    value = worker()
    endpoint, payload = await value._tripo_payload({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "generation_mode": "convert_model", "parameters": {"input_job_id": "a" * 32,
            "target_format": "FBX", "export_orientation": "+x"}, "references": []})
    assert endpoint == "models/convert"
    assert payload == {"input": "provider-source-task", "format": "FBX", "export_orientation": "+x"}


@pytest.mark.asyncio
async def test_tripo_text_payload_derives_no_pbr_and_preserves_reproducibility_fields():
    value = worker()
    endpoint, payload = await value._tripo_payload({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "generation_mode": "text_to_model", "parameters": {"prompt": "cube", "texture": False,
            "model_seed": 3, "image_seed": 4, "export_uv": False, "export_orientation": "+x"},
        "references": []})
    assert endpoint == "generation/text-to-model"
    assert payload == {"prompt": "cube", "texture": False, "pbr": False, "model_seed": 3,
        "image_seed": 4, "export_uv": False, "export_orientation": "+x", "model": "v3.1-20260211"}


@pytest.mark.asyncio
async def test_tripo_submit_429_honors_retry_after_without_losing_request(monkeypatch):
    value = worker()
    client = Client([Response({"code": 1007}, 429, {"Retry-After": "7"}),
        Response({"code": 0, "data": {"task_id": "tripo-task"}}),
        Response({"code": 0, "data": {"status": "success",
            "output": {"model_url": "https://result.test/model.glb"}}})])
    sleeps = []
    async def client_factory(): return client
    async def no_sleep(seconds): sleeps.append(seconds)
    monkeypatch.setattr(value, "_client", client_factory)
    monkeypatch.setattr("sandbox_harness.services.media_worker.media_worker.asyncio.sleep", no_sleep)
    result = await value._tripo({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "provider_job_id": "", "generation_mode": "text_to_model",
        "parameters": {"prompt": "cube"}})
    assert result.status == "success" and len(client.posts) == 2
    assert sleeps[:2] == [7.0, 1.0]
    assert any(update[2].get("phase") == "provider_rate_limited" for update in value.store.updates)


@pytest.mark.asyncio
async def test_tripo_success_without_task_id_is_unknown_not_rejected(monkeypatch):
    value = worker(); client = Client([Response({"code": 0, "data": {}})])
    async def client_factory(): return client
    monkeypatch.setattr(value, "_client", client_factory)
    result = await value._tripo({"id": "job", "generation": 1, "model": "v3.1-20260211",
        "provider_job_id": "", "generation_mode": "text_to_model",
        "parameters": {"prompt": "cube"}})
    assert result.status == "unknown" and result.error == "tripo_submit_failed:0"
    assert len(client.posts) == 1


@pytest.mark.asyncio
async def test_provider_output_http_failure_schedules_safe_download_retry(monkeypatch):
    store = Store(); value = worker(store)
    async def execute(_):
        return __import__('sandbox_harness.services.media_worker.media_worker', fromlist=['ProviderResult']).ProviderResult(
            "success", provider_job_id="provider-task", outputs=[{"role": "primary",
                "filename": "model.glb", "url": "https://result.test/model.glb", "kind": "model3d"}])
    async def failed_download(*_args, **_kwargs):
        request = __import__('httpx').Request("GET", "https://result.test/model.glb")
        response = __import__('httpx').Response(503, request=request)
        raise __import__('httpx').HTTPStatusError("temporary", request=request, response=response)
    monkeypatch.setattr(value, "_execute", execute)
    monkeypatch.setattr(value, "_download", failed_download)
    await value.process({"id": "job", "generation": 1, "kind": "model3d", "model": "v3.1-20260211",
        "provider": "tripo", "adapter": "tripo_v3", "status": "running",
        "phase": "provider_polling", "provider_job_id": "provider-task", "references": []})
    assert store.releases[-1][2] == {"phase": "provider_output_retry",
        "error_code": "provider_output_unavailable", "error_detail": "download_primary:HTTPStatusError",
        "retry_after_seconds": 15,"generation":1}
    assert not any(update[2].get("status") == "unknown" for update in store.updates)
