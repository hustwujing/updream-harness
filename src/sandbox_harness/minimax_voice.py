from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import timedelta
from urllib.parse import urljoin

import httpx

from .domain import now
from .media_errors import classify_provider_error
from .observability import event


class MiniMaxVoiceCloneError(RuntimeError):
    pass


class MiniMaxVoiceClonePending(RuntimeError):
    pass


class MiniMaxVoiceClones:
    fresh_for = timedelta(days=6)
    transient_codes = {1000, 1001, 1002, 1013}

    @staticmethod
    def _failure_code(http_status: int, body: dict) -> str:
        base = body.get("base_resp", {}) if isinstance(body, dict) else {}
        message = base.get("status_msg", "")
        by_message = classify_provider_error(http_status, message)
        return (by_message if by_message != "provider_processing_failed"
            else classify_provider_error(http_status, base.get("status_code")))

    def __init__(self, settings, store, objects, worker_owner: str):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.worker_owner = worker_owner

    async def _client(self):
        return httpx.AsyncClient(timeout=httpx.Timeout(120, connect=10),
            follow_redirects=True, trust_env=False)

    @property
    def _base(self) -> str:
        return self.settings.upstream_audio.rstrip("/")

    @staticmethod
    def _headers() -> dict[str, str]:
        return {"Authorization": "Bearer " + os.environ.get(
            "SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY", "")}

    async def voice_exists(self, voice_id: str) -> bool | None:
        try:
            async with await self._client() as client:
                response = await client.post(urljoin(self._base + "/", "get_voice"),
                    headers={**self._headers(), "Content-Type": "application/json"},
                    json={"voice_type": "voice_cloning"})
            body = response.json()
            if (not response.is_success
                    or body.get("base_resp", {}).get("status_code", 0) != 0):
                return None
            return any(item.get("voice_id") == voice_id for item in body.get("voice_cloning", []))
        except (httpx.HTTPError, ValueError):
            return None

    async def _upload(self, record: dict, data: bytes) -> tuple[str, int]:
        extension = {"audio/mpeg": "mp3", "audio/mp4": "m4a", "audio/wav": "wav"}[record["mime"]]
        try:
            async with await self._client() as client:
                response = await client.post(urljoin(self._base + "/", "files/upload"),
                    headers=self._headers(), data={"purpose": "voice_clone"},
                    files={"file": ("reference." + extension, data, record["mime"])})
            body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise MiniMaxVoiceClonePending("voice_clone_upload_transport") from exc
        code = int(body.get("base_resp", {}).get("status_code", 0) or 0)
        file_id = str(body.get("file", {}).get("file_id") or "")
        if response.is_success and code == 0 and file_id:
            return file_id, 0
        failure = self._failure_code(response.status_code, body)
        if failure in {"provider_rate_limited", "provider_service_unavailable"}:
            raise MiniMaxVoiceClonePending("voice_clone_rate_limited" if failure == "provider_rate_limited"
                else "voice_clone_upload_transient")
        raise MiniMaxVoiceCloneError("voice_clone_permission_denied" if code == 2038
            else "voice_clone_input_invalid" if failure == "provider_input_invalid"
            else failure)

    async def _clone(self, record: dict, model: str) -> None:
        payload = {"file_id": int(record["provider_file_id"]), "voice_id": record["voice_id"],
            "text": "你好，这是音色验证。", "model": model,
            "need_noise_reduction": False, "need_volume_normalization": True}
        try:
            async with await self._client() as client:
                response = await client.post(urljoin(self._base + "/", "voice_clone"),
                    headers={**self._headers(), "Content-Type": "application/json"}, json=payload)
            body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            exists = await self.voice_exists(record["voice_id"])
            if exists:
                return
            raise MiniMaxVoiceClonePending("voice_clone_outcome_unresolved") from exc
        code = int(body.get("base_resp", {}).get("status_code", 0) or 0)
        if response.is_success and code == 0:
            return
        exists = await self.voice_exists(record["voice_id"])
        if exists:
            return
        failure = self._failure_code(response.status_code, body)
        if exists is None or failure in {"provider_rate_limited", "provider_service_unavailable"}:
            raise MiniMaxVoiceClonePending("voice_clone_rate_limited" if failure == "provider_rate_limited"
                else "voice_clone_provider_transient")
        raise MiniMaxVoiceCloneError("voice_clone_permission_denied" if code == 2038
            else "voice_clone_input_invalid" if failure == "provider_input_invalid"
            else failure if failure != "provider_processing_failed" else "voice_clone_failed")

    async def prepare(self, job: dict) -> str:
        references = job.get("references") or []
        if len(references) != 1:
            raise MiniMaxVoiceCloneError("voice_clone_input_invalid")
        reference = references[0]
        record = await self.store.ensure_media_voice_clone(job["owner_id"], reference)
        if (record["status"] == "active" and record["last_used_at"]
                and record["last_used_at"] >= now() - self.fresh_for):
            return record["voice_id"]
        for _ in range(120):
            record, claimed = await self.store.claim_media_voice_clone(job["owner_id"],
                reference["sha256"], self.worker_owner)
            if not claimed:
                await self.store.update_media_job(job["id"], self.worker_owner, job["generation"],
                    status="running", phase="waiting_voice_clone")
                await asyncio.sleep(1)
                record = await self.store.get_media_voice_clone(job["owner_id"], reference["sha256"])
                if (record and record["status"] == "active" and record["last_used_at"]
                        and record["last_used_at"] >= now() - self.fresh_for):
                    return record["voice_id"]
                continue
            try:
                if record["status"] == "failed":
                    raise MiniMaxVoiceCloneError(record["error_code"] or "voice_clone_failed")
                if record["status"] in {"active", "uncertain"}:
                    previous_status = record["status"]
                    exists = await self.voice_exists(record["voice_id"])
                    if exists:
                        await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                            self.worker_owner, status="active", error_code="", release=True)
                        return record["voice_id"]
                    if exists is None:
                        await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                            self.worker_owner, status="uncertain", error_code="voice_lookup_unavailable",
                            release=True)
                        raise MiniMaxVoiceClonePending("voice_lookup_unavailable")
                    if previous_status == "active":
                        record = await self.store.rotate_media_voice_clone(job["owner_id"],
                            reference["sha256"], self.worker_owner)
                    else:
                        record = await self.store.update_media_voice_clone(job["owner_id"],
                            reference["sha256"], self.worker_owner, status="pending", error_code="")
                data = await asyncio.to_thread(self.objects.get, reference["object_ref"],
                    maximum=20 * 1024 * 1024)
                if (len(data) != reference["size"]
                        or hashlib.sha256(data).hexdigest() != reference["sha256"]):
                    raise MiniMaxVoiceCloneError("voice_clone_input_invalid")
                if not record["provider_file_id"]:
                    await self.store.update_media_job(job["id"], self.worker_owner, job["generation"],
                        status="running", phase="uploading_voice_reference")
                    file_id, _ = await self._upload(record, data)
                    record = await self.store.update_media_voice_clone(job["owner_id"],
                        reference["sha256"], self.worker_owner, status="uploaded",
                        provider_file_id=file_id, error_code="")
                await self.store.update_media_job(job["id"], self.worker_owner, job["generation"],
                    status="running", phase="cloning_voice")
                await self._clone(record, job["model"])
                await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                    self.worker_owner, status="active", error_code="", release=True)
                await self.store.touch_media_voice_clone(job["owner_id"], reference["sha256"],
                    record["voice_id"])
                event("media.voice_clone.ready", job_id=job["id"],
                    reference_sha256=reference["sha256"][:12])
                return record["voice_id"]
            except MiniMaxVoiceClonePending as exc:
                await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                    self.worker_owner, status="uncertain", error_code=str(exc), release=True)
                raise
            except MiniMaxVoiceCloneError as exc:
                await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                    self.worker_owner, status="failed", error_code=str(exc), release=True)
                raise
            except Exception as exc:
                try:
                    await self.store.update_media_voice_clone(job["owner_id"], reference["sha256"],
                        self.worker_owner, status="uncertain",
                        error_code="voice_clone_internal_error", release=True)
                except RuntimeError:
                    pass
                raise MiniMaxVoiceClonePending("voice_clone_internal_error") from exc
        raise MiniMaxVoiceClonePending("voice_clone_wait_timeout")

    async def touch(self, job: dict, voice_id: str) -> None:
        reference = (job.get("references") or [None])[0]
        if reference:
            await self.store.touch_media_voice_clone(job["owner_id"], reference["sha256"], voice_id)

    async def invalidate(self, job: dict, voice_id: str) -> None:
        reference = (job.get("references") or [None])[0]
        if reference:
            await self.store.invalidate_media_voice_clone(job["owner_id"], reference["sha256"], voice_id)
