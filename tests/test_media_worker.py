from types import SimpleNamespace

import pytest

from sandbox_harness.media_worker import MediaWorker


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


def worker(store=None):
    settings = SimpleNamespace(upstream_image="http://image.test", upstream_video="http://video.test",
        upstream_audio="http://audio.test")
    return MediaWorker(settings, store or Store(), None, None)


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
async def test_requesting_job_without_provider_id_becomes_unknown(monkeypatch):
    store = Store(); value = worker(store)
    async def should_not_execute(_):
        raise AssertionError("ambiguous provider request must not be replayed")
    monkeypatch.setattr(value, "_execute", should_not_execute)
    await value.process({"id": "job", "status": "running", "phase": "requesting",
        "provider_job_id": "", "provider": "ark", "model": "seedream-5.0"})
    assert store.updates[-1][2]["status"] == "unknown"
    assert store.updates[-1][2]["error_code"] == "provider_submission_outcome_unknown"


def test_media_worker_lease_owner_is_instance_unique():
    first = worker(); second = worker()
    assert first.owner.startswith("media-worker-") and first.owner != second.owner


@pytest.mark.asyncio
async def test_media_worker_refills_free_slot_without_waiting_for_long_batch(monkeypatch):
    class QueueStore(Store):
        def __init__(self):
            super().__init__(); self.jobs = [{"id": value} for value in ("long", "quick", "next")]
        async def claim_media_jobs(self, owner, limit):
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
