from __future__ import annotations

import asyncio
import io
import ipaddress
import os
import re
import tarfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx
import av

from .media_catalog import MediaCatalog
from .media_reference_publisher import MediaReferencePreparationError, MediaReferencePublisher
from .minimax_voice import MiniMaxVoiceCloneError, MiniMaxVoiceClonePending, MiniMaxVoiceClones
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
    outputs: list[dict[str, str]] | None = None
    credits_consumed: float | None = None


class MediaWorker:
    """Durable media executor for the real, probed Provider protocols."""

    def __init__(self, settings, store: Store, objects: ObjectStore | S3ObjectStore, catalog: MediaCatalog):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.catalog = catalog
        self.owner = "media-worker-" + uuid.uuid4().hex
        self.reference_publisher = MediaReferencePublisher(settings, store, objects, self.owner)
        self.voice_clones = MiniMaxVoiceClones(settings, store, objects, self.owner)
        self.stop_event = asyncio.Event()
        self.concurrency = getattr(settings, "media_worker_concurrency", 6)
        self.tripo_concurrency = getattr(settings, "tripo_worker_concurrency", 2)
        self.audio_concurrency = getattr(settings, "audio_worker_concurrency", 6)

    async def run(self) -> None:
        active: set[asyncio.Task] = set()
        tripo_active: set[asyncio.Task] = set()
        audio_active: set[asyncio.Task] = set()
        try:
            while not self.stop_event.is_set():
                finished = {task for task in active if task.done()}
                active.difference_update(finished)
                tripo_active.difference_update(finished)
                audio_active.difference_update(finished)
                for task in finished:
                    if not task.cancelled() and task.exception():
                        event("media.worker.task_failed", level=40,
                            exception_type=type(task.exception()).__name__)
                tripo_available = min(self.tripo_concurrency - len(tripo_active),
                    max(0, self.concurrency - len(active)))
                if tripo_available > 0:
                    jobs = await self.store.claim_media_jobs(self.owner, limit=tripo_available,
                        adapter="tripo_v3")
                    for job in jobs:
                        task = asyncio.create_task(self.process(job), name="media-job-" + job["id"])
                        active.add(task); tripo_active.add(task)
                audio_available = min(self.audio_concurrency - len(audio_active),
                    max(0, self.concurrency - len(active)))
                if audio_available > 0:
                    jobs = await self.store.claim_media_jobs(self.owner, limit=audio_available,
                        adapter="minimax_t2a", has_provider_job=True)
                    for job in jobs:
                        task = asyncio.create_task(self.process(job), name="media-job-" + job["id"])
                        active.add(task); audio_active.add(task)
                    audio_available -= len(jobs)
                if audio_available > 0:
                    outstanding = await self.store.count_media_provider_jobs("minimax_t2a")
                    submit_available = min(audio_available,
                        max(0, self.audio_concurrency - outstanding))
                    if submit_available > 0:
                        jobs = await self.store.claim_media_jobs(self.owner, limit=submit_available,
                            adapter="minimax_t2a", has_provider_job=False)
                        for job in jobs:
                            task = asyncio.create_task(self.process(job), name="media-job-" + job["id"])
                            active.add(task); audio_active.add(task)
                available = self.concurrency - len(active)
                if available > 0:
                    jobs = await self.store.claim_media_jobs(self.owner, limit=available,
                        exclude_adapters={"tripo_v3", "minimax_t2a"})
                    for job in jobs:
                        active.add(asyncio.create_task(self.process(job), name="media-job-" + job["id"]))
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
        finally:
            for task in active:
                task.cancel()
            if active:
                await asyncio.gather(*active, return_exceptions=True)

    async def process(self, job: dict[str, Any]) -> None:
        stage = "claimed"
        try:
            if job["status"] == "cancel_requested":
                await self.store.update_media_job(job["id"], self.owner, status="failed", phase="cancel_refused",
                    error_code="media_cancel_not_supported", error_detail="Provider cancellation is not supported")
                return
            if (job["status"] == "running" and job["phase"] == "requesting"
                    and not job.get("provider_job_id")):
                await self.store.update_media_job(job["id"], self.owner, status="unknown", phase="recovery_required",
                    error_code="provider_submission_outcome_unknown",
                    error_detail="Provider request may have been submitted; automatic resubmission is disabled")
                return
            stage = "provider_request" if not job.get("provider_job_id") else "provider_poll"
            await self.store.update_media_job(job["id"], self.owner, status="running",
                phase="provider_polling" if job.get("provider_job_id") else (
                    "preparing_voice_clone" if job.get("references") and job.get("adapter") == "minimax_t2a"
                    else "preparing_references" if job.get("references") else "requesting"))
            result = await self._execute(job)
            if result.status == "unknown":
                error_code = ("provider_result_unresolved" if result.provider_job_id
                    else "provider_submission_outcome_unknown")
                await self.store.update_media_job(job["id"], self.owner, status="unknown",
                    phase="recovery_required", provider_job_id=result.provider_job_id,
                    error_code=error_code, error_detail=result.error[:2000])
                return
            if result.status == "pending":
                await self.store.release_media_job_lease(job["id"], self.owner,
                    phase="provider_polling", error_code="provider_poll_deferred",
                    error_detail=result.error[:2000], retry_after_seconds=15)
                return
            if result.status != "success":
                await self.store.update_media_job(job["id"], self.owner, status="failed", phase="terminal",
                    provider_job_id=result.provider_job_id, error_code="provider_failed", error_detail=result.error[:2000])
                return
            if result.outputs:
                completed = []
                for output in result.outputs:
                    stage = "download_" + output["role"]
                    data, mime = await self._download(output["url"], expected_kind=output["kind"])
                    stage = "object_store_" + output["role"]
                    object_ref, _ = await asyncio.to_thread(self.objects.put, data)
                    completed.append({"role": output["role"],
                        "filename": self._filename_for_mime(output["filename"], mime),
                        "object_ref": object_ref, "mime": mime, "size": len(data)})
                stage = "commit"
                await self.store.complete_media_job(job["id"], self.owner,
                    provider_job_id=result.provider_job_id, outputs=completed,
                    credits_consumed=result.credits_consumed)
                total_bytes = sum(output["size"] for output in completed)
            else:
                stage = "download"
                data, mime = await self._download(result.url, expected_kind=job["kind"])
                stage = "object_store"
                object_ref, _ = await asyncio.to_thread(self.objects.put, data)
                stage = "commit"
                await self.store.update_media_job(job["id"], self.owner, status="succeeded", phase="committed",
                    provider_job_id=result.provider_job_id, output_object_ref=object_ref,
                    output_mime=mime, output_size=len(data), progress=100)
                total_bytes = len(data)
            event("media.job.succeeded", job_id=job["id"], provider=job["provider"], model=job["model"],
                bytes=total_bytes)
        except MediaReferencePreparationError as exc:
            await self.store.update_media_job(job["id"], self.owner, status="failed", phase="terminal",
                error_code="media_reference_unavailable", error_detail=str(exc)[:200])
            event("media.reference.failed", level=30, job_id=job["id"], error_code=str(exc)[:80])
        except httpx.HTTPError as exc:
            if stage.startswith("download"):
                await self.store.release_media_job_lease(job["id"], self.owner,
                    phase="provider_output_retry", error_code="provider_output_unavailable",
                    error_detail=f"{stage}:{type(exc).__name__}", retry_after_seconds=15)
                event("media.output.retry_scheduled", level=30, job_id=job["id"], provider=job["provider"],
                    model=job["model"], failure_stage=stage, exception_type=type(exc).__name__)
            elif (stage == "provider_poll" and job.get("adapter") == "minimax_t2a"
                    and job.get("provider_job_id")):
                started = job.get("provider_started_at")
                if isinstance(started, str):
                    started = datetime.fromisoformat(started)
                if isinstance(started, datetime) and started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                elapsed = ((datetime.now(timezone.utc) - started).total_seconds()
                    if isinstance(started, datetime) else 0)
                if elapsed >= self.settings.audio_task_timeout_seconds:
                    await self.store.update_media_job(job["id"], self.owner, status="unknown",
                        phase="recovery_required", provider_job_id=job["provider_job_id"],
                        error_code="provider_result_unresolved",
                        error_detail=f"provider_poll:{type(exc).__name__}:after_reconciliation_window")
                else:
                    await self.store.release_media_job_lease(job["id"], self.owner,
                        phase="provider_polling", error_code="provider_poll_unavailable",
                        error_detail=f"provider_poll:{type(exc).__name__}", retry_after_seconds=15)
                event("media.audio.poll_transport_failure", level=30, job_id=job["id"],
                    provider=job["provider"], model=job["model"], elapsed_seconds=round(elapsed),
                    exception_type=type(exc).__name__)
            else:
                await self.store.update_media_job(job["id"], self.owner, status="unknown",
                    phase="recovery_required", error_code="worker_error",
                    error_detail=f"{stage}:{type(exc).__name__}")
                event("media.job.unknown", level=40, job_id=job["id"], provider=job["provider"],
                    model=job["model"], failure_stage=stage, exception_type=type(exc).__name__)
        except ValueError as exc:
            await self.store.update_media_job(job["id"], self.owner, status="failed", phase="terminal",
                error_code="provider_result_invalid", error_detail=f"{stage}:{str(exc)[:160]}")
            event("media.job.invalid_result", level=40, job_id=job["id"], provider=job["provider"],
                model=job["model"], failure_stage=stage, error_code=str(exc)[:80])
        except Exception as exc:
            await self.store.update_media_job(job["id"], self.owner, status="unknown", phase="recovery_required",
                error_code="worker_error", error_detail=f"{stage}:{type(exc).__name__}")
            event("media.job.unknown", level=40, job_id=job["id"], provider=job["provider"], model=job["model"],
                failure_stage=stage, exception_type=type(exc).__name__)

    async def _execute(self, job: dict[str, Any]) -> ProviderResult:
        if job.get("references") and job["adapter"] not in {"tripo_v3", "minimax_t2a"}:
            job = {**job, "references": await self.reference_publisher.prepare(job,
                require_ark=job["adapter"] == "ark_video_v2")}
        if job["adapter"] == "ark_image":
            return await self._ark_image(job)
        if job["adapter"] == "ark_video_v2":
            return await self._ark_video(job)
        if job["adapter"] == "minimax_t2a":
            return await self._minimax_audio(job)
        if job["adapter"] == "tripo_v3":
            return await self._tripo(job)
        return ProviderResult("failed", error="media_adapter_not_configured")

    async def _client(self):
        return httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=True, trust_env=False)

    async def _reference_url(self, reference: dict[str, Any]) -> str:
        value = reference.get("asset_uri") or reference.get("public_url") or ""
        if not value:
            raise MediaReferencePreparationError("media_reference_not_published")
        if value.startswith("asset://"):
            if not re.fullmatch(r"asset://asset-[A-Za-z0-9-]{4,160}", value):
                raise MediaReferencePreparationError("invalid_ark_asset_uri")
            return value
        parsed = urlsplit(value)
        prefix = "/updream/aniforge/" + reference.get("kind", "image") + "/"
        if (parsed.scheme != "https" or parsed.netloc != "boss-shjd.biliapi.net"
                or not parsed.path.startswith(prefix) or parsed.query or parsed.fragment):
            raise MediaReferencePreparationError("unstable_or_internal_media_reference_url")
        return value

    @staticmethod
    def _ordered_references(job: dict[str, Any]) -> list[dict[str, Any]]:
        source = {"edit_video": "edit_source", "extend_video": "extend_source"}.get(
            job.get("generation_mode", ""))
        values = list(job.get("references") or [])
        ordered = sorted(enumerate(values),
            key=lambda item: (0 if source and item[1].get("usage") == source else 1, item[0]))
        return [item[1] for item in ordered]

    @staticmethod
    def _video_prompt(mode: str, parameters: dict[str, Any]) -> str:
        prompt = parameters.get("prompt", "")
        if mode == "edit_video":
            return "严格编辑视频1，未提及的内容保持不变。" + prompt
        if mode == "extend_video":
            direction = "向前" if parameters.get("extend_direction") == "forward" else "向后"
            return f"{direction}延长视频1。" + prompt
        return prompt

    async def _ark_image(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_image.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY", "")
        p = job["parameters"]
        model = {"seedream-5.0": os.environ.get("SANDBOX_HARNESS_ARK_IMAGE_MODEL_SEEDREAM_5_0",
            "doubao-seedream-5-0-260128")}.get(job["model"], job["model"])
        payload = {"model": model, "prompt": p.get("prompt", ""), "response_format": "url",
                   "size": p.get("size", "2048x2048"), "watermark": False}
        references = self._ordered_references(job)
        if references:
            urls = [await self._reference_url(item) for item in references]
            payload["image"] = urls[0] if len(urls) == 1 else urls
            payload["sequential_image_generation"] = "disabled"
        if not await self.store.update_media_job(job["id"], self.owner, status="running", phase="requesting"):
            return ProviderResult("failed", error="media_job_lease_lost")
        async with await self._client() as client:
            response = await client.post(urljoin(base + "/", "images/generations"),
                headers={"Authorization": "Bearer " + key}, json=payload)
        raw = response.json()
        url = next((item.get("url", "") for item in raw.get("data", []) if item.get("url")), "")
        provider_error = raw.get("error") if isinstance(raw, dict) else None
        provider_code = (provider_error.get("code") if isinstance(provider_error, dict) else None) or (
            raw.get("code") if isinstance(raw, dict) else None)
        error = "" if url else f"image_http_{response.status_code}"
        if provider_code:
            error += ":" + re.sub(r"[^A-Za-z0-9_.-]", "_", str(provider_code))[:100]
        elif response.is_success:
            error = "image_result_missing"
        return ProviderResult("success" if response.is_success and url else "failed", url=url,
            error=error)

    async def _ark_video(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_video.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY", "")
        p = job["parameters"]
        model = {"sed2": os.environ.get("SANDBOX_HARNESS_ARK_VIDEO_MODEL_SED2", "seedance-2.0")}.get(job["model"], job["model"])
        task_id = str(job.get("provider_job_id") or "")
        if task_id:
            event("media.provider.resumed", job_id=job["id"], provider="ark", provider_job_id=task_id)
        if not task_id:
            mode = job.get("generation_mode") or "text_to_video"
            content = [{"type": "text", "text": self._video_prompt(mode, p)}]
            for reference in self._ordered_references(job):
                url = await self._reference_url(reference)
                role = reference["role"]
                if role in {"reference_image", "first_frame", "last_frame"}:
                    content.append({"type": "image_url", "image_url": {"url": url}, "role": role})
                elif role == "reference_video":
                    content.append({"type": "video_url", "video_url": {"url": url}, "role": role})
                else:
                    content.append({"type": "audio_url", "audio_url": {"url": url}, "role": role})
            duration = -1 if mode == "edit_video" else p.get("duration", 5)
            ratio = "adaptive" if mode in {"edit_video", "extend_video"} else p.get("aspect_ratio", "16:9")
            payload = {"model": model, "content": content,
                       "duration": duration, "ratio": ratio,
                       "resolution": p.get("resolution", "480p"), "generate_audio": bool(p.get("generate_audio", False))}
            if not await self.store.update_media_job(job["id"], self.owner, status="running", phase="requesting"):
                return ProviderResult("failed", error="media_job_lease_lost")
            async with await self._client() as client:
                response = await client.post(urljoin(base + "/", "contents/generations/tasks"),
                    headers={"Authorization": "Bearer " + key}, json=payload)
            raw = response.json(); task_id = str(raw.get("id") or raw.get("task_id") or "")
            if not response.is_success or not task_id:
                return ProviderResult("failed", error="video_submit_failed")
            if not await self.store.update_media_job(job["id"], self.owner, status="running",
                    phase="provider_polling", provider_job_id=task_id):
                return ProviderResult("failed", provider_job_id=task_id, error="media_job_lease_lost")
            event("media.provider.submitted", job_id=job["id"], provider="ark", provider_job_id=task_id)
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
            progress = body.get("progress")
            await self.store.update_media_job(job["id"], self.owner, status="running", phase="provider_polling",
                provider_job_id=task_id, progress=progress if isinstance(progress, int) else None)
        return ProviderResult("failed", provider_job_id=task_id, error="video_timeout")

    async def _minimax_audio(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.upstream_audio.rstrip("/")
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY", "")
        p = job["parameters"]
        voice_id = p.get("voice_id", "male-qn-qingse")
        if job.get("references"):
            try:
                voice_id = await self.voice_clones.prepare(job)
            except MiniMaxVoiceClonePending as exc:
                return ProviderResult("pending", error=str(exc))
            except MiniMaxVoiceCloneError as exc:
                return ProviderResult("failed", error=str(exc))
        payload = {"model": job["model"], "text": p.get("text", ""),
                   "voice_setting": {"voice_id": voice_id, "speed": p.get("speed", 1), "vol": 1, "pitch": 0},
                   "audio_setting": {"format": p.get("audio_format", "mp3"), "audio_sample_rate": 32000, "bitrate": 128000, "channel": 2}}
        headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
        task_id = str(job.get("provider_job_id") or "")
        provider_started_at = job.get("provider_started_at")
        if isinstance(provider_started_at, str):
            provider_started_at = datetime.fromisoformat(provider_started_at)
        if isinstance(provider_started_at, datetime) and provider_started_at.tzinfo is None:
            provider_started_at = provider_started_at.replace(tzinfo=timezone.utc)
        if task_id:
            event("media.provider.resumed", job_id=job["id"], provider="minimax", provider_job_id=task_id)
        if not task_id:
            async with await self._client() as client:
                response = await client.post(urljoin(base + "/", "t2a_async_v2"), headers=headers, json=payload)
            raw = response.json(); task_id = str(raw.get("task_id") or "")
            if not response.is_success or not task_id:
                code = raw.get("base_resp", {}).get("status_code", "unreported")
                if response.status_code == 429 or code == 1002:
                    return ProviderResult("pending", error="audio_submit_rate_limited")
                if response.status_code >= 500 or code in {1000, 1001, 1013}:
                    return ProviderResult("unknown", error=f"audio_submission_outcome_unknown:{code}")
                if job.get("references"):
                    exists = await self.voice_clones.voice_exists(voice_id)
                    if exists is False:
                        await self.voice_clones.invalidate(job, voice_id)
                        return ProviderResult("pending", error="voice_clone_revalidation_required")
                    if exists is None:
                        return ProviderResult("pending", error="voice_clone_lookup_unavailable")
                return ProviderResult("failed", error=f"audio_submit_failed:{code}")
            if not await self.store.update_media_job(job["id"], self.owner, status="running",
                    phase="provider_polling", provider_job_id=task_id):
                return ProviderResult("failed", provider_job_id=task_id, error="media_job_lease_lost")
            provider_started_at = datetime.now(timezone.utc)
            if job.get("references"):
                await self.voice_clones.touch(job, voice_id)
            event("media.provider.submitted", job_id=job["id"], provider="minimax", provider_job_id=task_id)
        await asyncio.sleep(5)
        async with await self._client() as client:
            poll = await client.get(urljoin(base + "/", "query/t2a_async_query_v2"),
                headers=headers, params={"task_id": task_id})
        elapsed = ((datetime.now(timezone.utc) - provider_started_at).total_seconds()
            if isinstance(provider_started_at, datetime) else 0)
        if not poll.is_success:
            if elapsed >= self.settings.audio_task_timeout_seconds:
                return ProviderResult("unknown", provider_job_id=task_id,
                    error=f"audio_poll_http_{poll.status_code}_after_reconciliation_window")
            return ProviderResult("pending", provider_job_id=task_id,
                error=f"audio_poll_http_{poll.status_code}")
        body = poll.json(); status = str(body.get("status", "")).lower()
        if status == "success":
            file_id = body.get("file_id")
            async with await self._client() as client:
                file_resp = await client.get(urljoin(base + "/", "files/retrieve"),
                    headers=headers, params={"file_id": file_id})
            if not file_resp.is_success:
                status = f"file_http_{file_resp.status_code}"
            else:
                url = file_resp.json().get("file", {}).get("download_url", "")
                if url:
                    return ProviderResult("success", url=url, provider_job_id=task_id)
                status = "file_url_unreported"
        if status in {"failed", "expired"}:
            return ProviderResult("failed", provider_job_id=task_id, error="audio_provider_failed")
        if elapsed >= self.settings.audio_task_timeout_seconds:
            return ProviderResult("unknown", provider_job_id=task_id,
                error="audio_processing_outlived_reconciliation_window")
        event("media.audio.poll_deferred", job_id=job["id"], provider="minimax",
            provider_job_id=task_id, elapsed_seconds=round(elapsed), provider_status=status or "unreported")
        return ProviderResult("pending", provider_job_id=task_id, error="audio_still_processing")

    @staticmethod
    def _filename_for_mime(filename: str, mime: str) -> str:
        extension = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp",
            "model/gltf-binary": "glb", "application/vnd.autodesk.fbx": "fbx",
            "model/obj": "obj", "model/stl": "stl", "model/vnd.usdz+zip": "usdz",
            "model/3mf": "3mf", "model/gltf+zip": "zip", "model/gltf+json": "gltf",
            "model/obj+zip": "zip"}.get(mime)
        if not extension:
            return filename
        stem = filename.rsplit(".", 1)[0] if "." in filename else filename
        return f"{stem}.{extension}"

    @staticmethod
    def _validated_tripo_output_url(value: str) -> str:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").lower()
        if (parsed.scheme != "https" or not host or parsed.username or parsed.password
                or host == "localhost" or host.endswith((".local", ".internal", ".bilibili.co"))):
            raise ValueError("tripo_result_url_not_public")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address is not None and not address.is_global:
            raise ValueError("tripo_result_url_not_public")
        return value

    @staticmethod
    def _tripo_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
        allowed = {"prompt", "negative_prompt", "face_limit", "texture", "pbr",
            "texture_quality", "geometry_quality", "auto_size", "quad", "smart_low_poly",
            "model_seed", "image_seed", "texture_seed", "enable_image_autofix", "texture_alignment",
            "orientation", "smart_uv", "bake", "export_orientation", "export_uv"}
        result = {key: value for key, value in parameters.items() if key in allowed}
        if result.get("texture") is False:
            result["pbr"] = False
        return result

    @staticmethod
    def _tripo_retry_delay(response: Any, attempt: int) -> float:
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return min(32.0, max(1.0, float(retry_after)))
            except ValueError:
                pass
        reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
        if reset:
            try:
                return min(32.0, max(1.0, float(reset) - time.time()))
            except ValueError:
                pass
        return min(32.0, float(2 ** attempt))

    async def _tripo_payload(self, job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        mode = job.get("generation_mode") or "text_to_model"
        parameters = job["parameters"]
        payload = self._tripo_parameters(parameters)
        if mode in {"text_to_model", "image_to_model", "multiview_to_model"}:
            payload["model"] = job["model"]
        endpoints = {
            "text_to_model": "generation/text-to-model",
            "image_to_model": "generation/image-to-model",
            "multiview_to_model": "generation/multiview-to-model",
            "texture_model": "models/texture",
            "decimate_model": "mesh/decimate",
            "convert_model": "models/convert",
        }
        if mode not in endpoints:
            raise ValueError("tripo_generation_mode_invalid")
        references = job.get("references") or []
        if mode in {"image_to_model", "multiview_to_model"}:
            prepared = await self.reference_publisher.prepare(job, require_ark=False)
            values = []
            role_to_view = {"front_view": "front", "left_view": "left",
                "back_view": "back", "right_view": "right"}
            for reference in prepared:
                url = await self._reference_url(reference)
                if mode == "image_to_model":
                    payload["input"] = url
                else:
                    values.append({role_to_view[reference["role"]]: url})
            if mode == "multiview_to_model":
                payload["inputs"] = values
        elif mode in {"texture_model", "decimate_model", "convert_model"}:
            payload["input"] = await self.store.resolve_tripo_input_job(job["id"], parameters["input_job_id"])
            if mode == "texture_model":
                payload["model"] = "v3.0-20250812"
                if parameters.get("texture_prompt"):
                    payload["texture_prompt"] = {"text": parameters["texture_prompt"]}
            elif mode == "decimate_model":
                payload["model"] = parameters.get("topology_model", "v3.0")
            else:
                payload["format"] = parameters["target_format"]
        return endpoints[mode], payload

    async def _tripo(self, job: dict[str, Any]) -> ProviderResult:
        base = self.settings.tripo_base_url.rstrip("/") + "/"
        headers = {"Authorization": "Bearer " + self.settings.tripo_api_key,
            "Content-Type": "application/json"}
        task_id = str(job.get("provider_job_id") or "")
        if task_id:
            event("media.provider.resumed", job_id=job["id"], provider="tripo",
                provider_job_id=task_id)
        if not task_id:
            endpoint, payload = await self._tripo_payload(job)
            if not await self.store.update_media_job(job["id"], self.owner, status="running",
                    phase="requesting"):
                return ProviderResult("failed", error="media_job_lease_lost")
            maximum_retries = int(getattr(self.settings, "tripo_submit_max_retries", 5))
            for attempt in range(maximum_retries + 1):
                try:
                    async with await self._client() as client:
                        response = await client.post(urljoin(base, endpoint), headers=headers, json=payload)
                except (httpx.TimeoutException, httpx.TransportError):
                    return ProviderResult("unknown", error="tripo_submit_transport_error")
                if response.status_code != 429:
                    break
                if attempt >= maximum_retries:
                    return ProviderResult("failed", error="tripo_submit_rate_limited")
                delay = self._tripo_retry_delay(response, attempt)
                await self.store.update_media_job(job["id"], self.owner, status="running",
                    phase="provider_rate_limited")
                event("media.provider.rate_limited", level=30, job_id=job["id"], provider="tripo",
                    retry_seconds=delay, retry_attempt=attempt + 1)
                await asyncio.sleep(delay)
            try:
                raw = response.json()
            except ValueError:
                return ProviderResult("unknown" if response.is_success or response.status_code >= 500 else "failed",
                    error="tripo_submit_invalid_response")
            task_id = str((raw.get("data") or {}).get("task_id") or "") if isinstance(raw, dict) else ""
            if not task_id:
                code = raw.get("code") if isinstance(raw, dict) else None
                suffix = re.sub(r"[^A-Za-z0-9_.-]", "_", str(code))[:80] if code is not None else "missing"
                return ProviderResult("unknown" if response.is_success or response.status_code >= 500 else "failed",
                    error="tripo_submit_failed:" + suffix)
            if not await self.store.update_media_job(job["id"], self.owner, status="running",
                    phase="provider_polling", provider_job_id=task_id):
                return ProviderResult("failed", provider_job_id=task_id, error="media_job_lease_lost")
            event("media.provider.submitted", job_id=job["id"], provider="tripo",
                provider_job_id=task_id)
        poll_seconds = float(self.settings.tripo_poll_seconds)
        attempts = max(1, int(self.settings.tripo_task_timeout_seconds / poll_seconds))
        unknown_count = 0
        for _ in range(attempts):
            await asyncio.sleep(poll_seconds)
            try:
                async with await self._client() as client:
                    response = await client.get(urljoin(base, "tasks/" + task_id), headers=headers)
                if response.status_code == 429:
                    delay = self._tripo_retry_delay(response, 0)
                    await self.store.update_media_job(job["id"], self.owner, status="running",
                        phase="provider_rate_limited", provider_job_id=task_id)
                    await asyncio.sleep(delay)
                    continue
                body = response.json()
            except (httpx.TimeoutException, httpx.TransportError, ValueError):
                continue
            data = body.get("data") if isinstance(body, dict) else None
            if not response.is_success or not isinstance(data, dict):
                continue
            status = str(data.get("status") or "").lower()
            progress = data.get("progress")
            if status == "success":
                output = data.get("output") or {}
                model_url = str(output.get("model_url") or "")
                preview_url = str(output.get("rendered_image_url") or output.get("generated_image_url") or "")
                if not model_url:
                    return ProviderResult("failed", provider_job_id=task_id,
                        error="tripo_result_model_missing")
                parameters = job.get("parameters", {})
                target = (str(parameters.get("target_format") or "GLB").upper()
                    if job.get("generation_mode") == "convert_model"
                    else "FBX" if parameters.get("quad") else "GLB")
                suffix = urlsplit(model_url).path.rsplit(".", 1)[-1].lower()
                target = {"glb": "GLB", "gltf": "GLTF", "fbx": "FBX", "obj": "OBJ",
                    "stl": "STL", "usdz": "USDZ", "3mf": "3MF"}.get(suffix, target)
                extension = {"GLB": "glb", "GLTF": "gltf", "FBX": "fbx", "USDZ": "usdz",
                    "OBJ": "obj", "STL": "stl", "3MF": "3mf"}.get(target, "glb")
                expected = "model3d" if target == "GLB" else "model3d:" + target.lower()
                outputs = [{"role": "primary", "filename": f"model-{job['id'][:8]}.{extension}",
                    "url": self._validated_tripo_output_url(model_url), "kind": expected}]
                if preview_url:
                    suffix = urlsplit(preview_url).path.rsplit(".", 1)[-1].lower()
                    preview_extension = suffix if suffix in {"png", "jpg", "jpeg", "webp"} else "png"
                    outputs.append({"role": "preview", "filename": f"preview-{job['id'][:8]}.{preview_extension}",
                        "url": self._validated_tripo_output_url(preview_url), "kind": "image"})
                credits = data.get("credits_consumed")
                return ProviderResult("success", provider_job_id=task_id, outputs=outputs,
                    credits_consumed=float(credits) if isinstance(credits, (int, float)) else None)
            if status in {"failed", "cancelled", "canceled", "banned", "expired"}:
                code = re.sub(r"[^A-Za-z0-9_.-]", "_", str(data.get("error_code") or "unknown"))[:80]
                return ProviderResult("failed", provider_job_id=task_id,
                    error="tripo_provider_failed:" + code)
            if status == "unknown" or not status:
                unknown_count += 1
            await self.store.update_media_job(job["id"], self.owner, status="running",
                phase="provider_polling", provider_job_id=task_id,
                progress=progress if isinstance(progress, int) else None)
        return ProviderResult("pending", provider_job_id=task_id,
            error="tripo_poll_timeout" if not unknown_count else "tripo_status_unresolved")

    @staticmethod
    def _unwrap_provider_media(data: bytes, reported: str, expected_kind: str,
                               maximum: int) -> tuple[bytes, str]:
        normalized = reported.split(";", 1)[0].strip().lower()
        if expected_kind != "audio" or normalized not in {"application/x-tar", "application/tar"}:
            return data, reported
        try:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
                members = archive.getmembers()
                if len(members) > 64:
                    raise ValueError("provider_audio_archive_too_many_members")
                extensions = {".mp3": "audio/mpeg", ".wav": "audio/wav", ".flac": "audio/flac",
                    ".m4a": "audio/mp4", ".mp4": "audio/mp4", ".ogg": "audio/ogg",
                    ".webm": "audio/webm"}
                candidates = [member for member in members if member.isfile()
                    and os.path.splitext(member.name)[1].lower() in extensions]
                if len(candidates) != 1:
                    raise ValueError("provider_audio_archive_requires_one_audio_file")
                member = candidates[0]
                if member.size <= 0 or member.size > maximum:
                    raise ValueError("provider_audio_archive_member_too_large")
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError("provider_audio_archive_member_missing")
                payload = stream.read(maximum + 1)
                if len(payload) != member.size or len(payload) > maximum:
                    raise ValueError("provider_audio_archive_member_size_mismatch")
                return payload, extensions[os.path.splitext(member.name)[1].lower()]
        except tarfile.TarError as exc:
            raise ValueError("provider_audio_archive_invalid") from exc

    @staticmethod
    def _validated_mime(data: bytes, reported: str, expected_kind: str) -> str:
        model_format = expected_kind.split(":", 1)[1].upper() if expected_kind.startswith("model3d:") else "GLB"
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
        elif data.startswith(b"glTF"):
            detected = "model/gltf-binary"
        elif expected_kind.startswith("model3d"):
            stripped = data.lstrip()
            if data.startswith(b"Kaydara FBX Binary") or stripped.startswith(b"; FBX"):
                detected = "application/vnd.autodesk.fbx"
            elif model_format == "OBJ" and data.startswith(b"PK\x03\x04"):
                detected = "model/obj+zip"
            elif model_format == "OBJ" and re.search(rb"(?m)^(?:v|vn|vt|f)\s+", data[:65536]):
                detected = "model/obj"
            elif model_format == "STL" and (stripped.startswith(b"solid ") or len(data) >= 84):
                detected = "model/stl"
            elif model_format in {"USDZ", "3MF", "GLTF"} and data.startswith(b"PK\x03\x04"):
                detected = {"USDZ": "model/vnd.usdz+zip", "3MF": "model/3mf",
                    "GLTF": "model/gltf+zip"}[model_format]
            elif model_format == "GLTF" and stripped.startswith(b"{") and b'"asset"' in data[:4096]:
                detected = "model/gltf+json"
        prefix = "model/" if expected_kind.startswith("model3d") else expected_kind + "/"
        if expected_kind.startswith("model3d") and detected:
            return detected
        if detected.startswith(prefix):
            return detected
        normalized = reported.split(";", 1)[0].strip().lower()
        if normalized.startswith(prefix):
            return normalized
        if expected_kind.startswith("model3d"):
            raise ValueError("provider_result_mime_mismatch")
        try:
            with av.open(io.BytesIO(data)) as container:
                stream_types = {stream.type for stream in container.streams}
                formats = set(container.format.name.split(","))
            if expected_kind in stream_types:
                if expected_kind == "audio" and "mp3" in formats:
                    return "audio/mpeg"
                if expected_kind == "audio" and formats.intersection({"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}):
                    return "audio/mp4"
                if expected_kind == "video" and formats.intersection({"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}):
                    return "video/mp4"
                if "webm" in formats:
                    return expected_kind + "/webm"
        except (av.error.FFmpegError, ValueError):
            pass
        raise ValueError("provider_result_mime_mismatch")

    async def _download(self, url: str, *, expected_kind: str) -> tuple[bytes, str]:
        if not url or not url.startswith(("http://", "https://")):
            raise ValueError("invalid_provider_result_url")
        async with await self._client() as client:
            response = await client.get(url)
        response.raise_for_status()
        data = response.content
        maximum = (getattr(self.settings, "tripo_max_output_bytes", 512 * 1024 * 1024)
            if expected_kind.startswith("model3d") else getattr(self.settings, "max_artifact_bytes", 2 * 1024 * 1024 * 1024))
        if len(data) > maximum:
            raise ValueError("provider_result_too_large")
        data, reported = self._unwrap_provider_media(data,
            response.headers.get("content-type", "application/octet-stream"), expected_kind, maximum)
        return data, self._validated_mime(data,
            reported, expected_kind)
