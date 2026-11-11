from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin

import httpx

from .media_catalog import MediaCatalog
from .observability import event
from .objects import ObjectStore, S3ObjectStore
from .store import Store


@dataclass
class ProviderResult:
    status: str
    url: str = ""
    provider_job_id: str = ""
    progress: int | None = None
    error: str = ""


class MediaWorker:
    """Durable media executor for the real, probed Provider protocols."""

    def __init__(self, settings, store: Store, objects: ObjectStore | S3ObjectStore, catalog: MediaCatalog):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.catalog = catalog
        self.owner = "media-worker"
        self.stop_event = asyncio.Event()
        self.continuation_callback: Callable[[dict], Awaitable[None]] | None = None

    async def run(self) -> None:
        while not self.stop_event.is_set():
            jobs = await self.store.claim_media_jobs(self.owner)
            if jobs:
                await asyncio.gather(*(self.process(job) for job in jobs), return_exceptions=True)
            if self.continuation_callback:
                for outbox in await self.store.claim_media_outbox(self.owner):
                    try:
                        await self.continuation_callback(outbox)
                    except Exception as exc:
                        await self.store.finish_media_outbox(outbox["outbox_id"], self.owner, delivered=False, error=type(exc).__name__)
                    else:
                        await self.store.finish_media_outbox(outbox["outbox_id"], self.owner, delivered=True)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass

    async def process(self, job: dict[str, Any]) -> None:
        try:
            if job["status"] == "cancel_requested":
                await self.store.update_media_job(job["id"], self.owner, status="failed", phase="cancel_refused",
                    error_code="media_cancel_not_supported", error_detail="Provider cancellation is not supported")
                return
            await self.store.update_media_job(job["id"], self.owner, status="running", phase="requesting")
            result = await self._execute(job)
            if result.status != "success":
                await self.store.update_media_job(job["id"], self.owner, status="failed", phase="terminal",
                    provider_job_id=result.provider_job_id, error_code="provider_failed", error_detail=result.error[:2000])
                return
            data, mime = await self._download(result.url, expected_kind=job["kind"])
            object_ref, _ = await asyncio.to_thread(self.objects.put, data)
            await self.store.update_media_job(job["id"], self.owner, status="succeeded", phase="committed",
                provider_job_id=result.provider_job_id, output_object_ref=object_ref,
                output_mime=mime, output_size=len(data), progress=100)
            event("media.job.succeeded", job_id=job["id"], provider=job["provider"], model=job["model"], bytes=len(data))
        except Exception as exc:
            await self.store.update_media_job(job["id"], self.owner, status="unknown", phase="recovery_required",
                error_code="worker_error", error_detail=type(exc).__name__)
            event("media.job.unknown", level=40, job_id=job["id"], provider=job["provider"], model=job["model"])

    async def _execute(self, job: dict[str, Any]) -> ProviderResult:
        if job["adapter"] == "ark_image":
            return await self._ark_image(job)
        if job["adapter"] == "ark_video_v2":
            return await self._ark_video(job)
        if job["adapter"] == "minimax_t2a":
            return await self._minimax_audio(job)
        return ProviderResult("failed", error="media_adapter_not_configured")

    async def _client(self):
        return httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=True, trust_env=False)

    async def _ark_image(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_image.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY", "")
        p = job["parameters"]
        model = {"seedream-5.0": "doubao-seedream-5-0-260128"}.get(job["model"], job["model"])
        payload = {"model": model, "prompt": p.get("prompt", ""), "response_format": "url",
                   "size": p.get("size", "2048x2048"), "watermark": False}
        async with await self._client() as client:
            response = await client.post(urljoin(base + "/", "images/generations"),
                headers={"Authorization": "Bearer " + key}, json=payload)
        raw = response.json()
        url = next((item.get("url", "") for item in raw.get("data", []) if item.get("url")), "")
        return ProviderResult("success" if response.is_success and url else "failed", url=url,
            error="image_result_missing" if not url else "")

    async def _ark_video(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_video.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY", "")
        p = job["parameters"]
        model = {"sed2": p.get("provider_model", "") or os.environ.get("SANDBOX_HARNESS_ARK_VIDEO_MODEL_SED2", "seedance-2.0")}.get(job["model"], job["model"])
        payload = {"model": model, "content": [{"type": "text", "text": p.get("prompt", "")}],
                   "duration": p.get("duration", 5), "ratio": p.get("aspect_ratio", "16:9"),
                   "resolution": p.get("resolution", "480p"), "generate_audio": bool(p.get("generate_audio", False))}
        async with await self._client() as client:
            response = await client.post(urljoin(base + "/", "contents/generations/tasks"),
                headers={"Authorization": "Bearer " + key}, json=payload)
        raw = response.json(); task_id = str(raw.get("id") or raw.get("task_id") or "")
        if not response.is_success or not task_id:
            return ProviderResult("failed", error="video_submit_failed")
        for _ in range(360):
            await asyncio.sleep(5)
            async with await self._client() as client:
                poll = await client.get(urljoin(base + "/", f"contents/generations/tasks/{task_id}"),
                    headers={"Authorization": "Bearer " + key})
            body = poll.json(); status = str(body.get("status", "")).lower()
            if status in {"succeeded", "success", "completed"}:
                url = body.get("content", {}).get("video_url", "") or body.get("video_url", "")
                return ProviderResult("success" if url else "failed", url=url, provider_job_id=task_id)
            if status in {"failed", "cancelled", "canceled"}:
                return ProviderResult("failed", provider_job_id=task_id, error="video_provider_failed")
        return ProviderResult("failed", provider_job_id=task_id, error="video_timeout")

    async def _minimax_audio(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_audio.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY", "")
        p = job["parameters"]
        payload = {"model": job["model"], "text": p.get("text", ""),
                   "voice_setting": {"voice_id": p.get("voice_id", "male-qn-qingse"), "speed": p.get("speed", 1), "vol": 1, "pitch": 0},
                   "audio_setting": {"format": p.get("audio_format", "mp3"), "audio_sample_rate": 32000, "bitrate": 128000, "channel": 2}}
        headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
        async with await self._client() as client:
            response = await client.post(urljoin(base + "/", "t2a_async_v2"), headers=headers, json=payload)
        raw = response.json(); task_id = str(raw.get("task_id") or "")
        if not response.is_success or not task_id:
            return ProviderResult("failed", error="audio_submit_failed")
        for _ in range(120):
            await asyncio.sleep(5)
            async with await self._client() as client:
                poll = await client.get(urljoin(base + "/", "query/t2a_async_query_v2"), headers=headers, params={"task_id": task_id})
            body = poll.json(); status = str(body.get("status", "")).lower()
            if status == "success":
                file_id = body.get("file_id")
                async with await self._client() as client:
                    file_resp = await client.get(urljoin(base + "/", "files/retrieve"), headers=headers, params={"file_id": file_id})
                url = file_resp.json().get("file", {}).get("download_url", "")
                return ProviderResult("success" if url else "failed", url=url, provider_job_id=task_id)
            if status in {"failed", "expired"}:
                return ProviderResult("failed", provider_job_id=task_id, error="audio_provider_failed")
        return ProviderResult("failed", provider_job_id=task_id, error="audio_timeout")

    @staticmethod
    def _validated_mime(data: bytes, reported: str, expected_kind: str) -> str:
        detected = ""
        if data.startswith(b"\xff\xd8\xff"):
            detected = "image/jpeg"
        elif data.startswith(b"\x89PNG\r\n\x1a\n"):
            detected = "image/png"
        elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
            detected = "image/webp"
        elif data.startswith(b"RIFF") and data[8:12] == b"WAVE":
            detected = "audio/wav"
        elif data.startswith(b"fLaC"):
            detected = "audio/flac"
        elif data.startswith(b"OggS"):
            detected = "audio/ogg"
        elif data.startswith(b"ID3") or (len(data) >= 2 and data[0] == 0xFF and data[1] & 0xE0 == 0xE0):
            detected = "audio/mpeg"
        elif len(data) >= 12 and data[4:8] == b"ftyp":
            detected = "audio/mp4" if expected_kind == "audio" else "video/mp4"
        elif data.startswith(b"\x1a\x45\xdf\xa3"):
            detected = "audio/webm" if expected_kind == "audio" else "video/webm"
        prefix = expected_kind + "/"
        if detected.startswith(prefix):
            return detected
        normalized = reported.split(";", 1)[0].strip().lower()
        if normalized.startswith(prefix):
            return normalized
        raise ValueError("provider_result_mime_mismatch")

    async def _download(self, url: str, *, expected_kind: str) -> tuple[bytes, str]:
        if not url or not url.startswith(("http://", "https://")):
            raise ValueError("invalid_provider_result_url")
        async with await self._client() as client:
            response = await client.get(url)
        response.raise_for_status()
        data = response.content
        return data, self._validated_mime(data,
            response.headers.get("content-type", "application/octet-stream"), expected_kind)
