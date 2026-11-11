from __future__ import annotations

import asyncio
import hashlib

import httpx

from .ark_assets import ArkAssetError, ArkAssets
from .observability import event


class MediaReferencePreparationError(RuntimeError):
    pass


class MediaReferencePublisher:
    def __init__(self, settings, store, objects, worker_owner: str):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.worker_owner = worker_owner
        self.ark = ArkAssets(settings)
        self.poll_seconds = getattr(settings, "ark_asset_poll_seconds", 2)
        self.max_wait_seconds = getattr(settings, "ark_asset_max_wait_seconds", 900)

    async def _publish_boss(self, record: dict, prepared: dict) -> dict:
        try:
            data = await asyncio.to_thread(self.objects.get, record["object_ref"], maximum=record["size"])
            if len(data) != record["size"] or hashlib.sha256(data).hexdigest() != record["sha256"]:
                raise MediaReferencePreparationError("media_reference_source_integrity_failed")
            async with httpx.AsyncClient(timeout=120, trust_env=False) as client:
                response = await client.put(prepared["put_url"], content=data, headers=prepared["headers"])
            if not response.is_success and response.status_code not in {409, 412}:
                raise MediaReferencePreparationError(f"boss_reference_upload_http_{response.status_code}")
            head = await asyncio.to_thread(self.objects.head_media_upload, record["object_key"])
            metadata = head["metadata"]
            if (head["size"] != record["size"] or head["mime"] != record["mime"]
                    or metadata.get("sha256") != record["sha256"]
                    or metadata.get("size") != str(record["size"])
                    or metadata.get("kind") != record["kind"]):
                raise MediaReferencePreparationError("boss_reference_upload_verification_failed")
            await asyncio.to_thread(self.objects.verify_media_upload, record["object_key"],
                digest=record["sha256"], expected_size=record["size"])
        except MediaReferencePreparationError:
            raise
        except Exception as exc:
            raise MediaReferencePreparationError("boss_reference_upload_failed:" + type(exc).__name__) from exc
        return await self.store.update_media_reference_asset(record["sha256"], self.worker_owner,
            boss_status="active")

    async def _activate_ark(self, record: dict, job_id: str) -> dict:
        if not self.ark.configured:
            raise MediaReferencePreparationError("ark_asset_credentials_unavailable")
        asset_id = record.get("ark_asset_id") or ""
        if not asset_id:
            existing = await self.ark.find(record["sha256"])
            if existing:
                asset_id = str(existing.get("Id") or "")
                status = str(existing.get("Status") or "Processing")
            else:
                asset_id = await self.ark.create(record)
                status = "Processing"
            record = await self.store.update_media_reference_asset(record["sha256"], self.worker_owner,
                ark_asset_id=asset_id, ark_status=status.lower())
        for _ in range(max(1, int(self.max_wait_seconds / self.poll_seconds))):
            value = await self.ark.get(asset_id)
            status = str(value.get("Status") or "").lower()
            error = value.get("Error") if isinstance(value.get("Error"), dict) else {}
            if status == "active":
                return await self.store.update_media_reference_asset(record["sha256"], self.worker_owner,
                    ark_status="active", ark_error="")
            if status == "failed":
                code = str(error.get("Code") or "ark_asset_processing_failed")
                await self.store.update_media_reference_asset(record["sha256"], self.worker_owner,
                    ark_status="failed", ark_error=code, release=True)
                raise MediaReferencePreparationError("ark_asset_processing_failed:" + code[:80])
            await self.store.update_media_reference_asset(record["sha256"], self.worker_owner,
                ark_status="processing")
            await self.store.update_media_job(job_id, self.worker_owner, status="running",
                phase="registering_assets")
            await asyncio.sleep(self.poll_seconds)
        raise MediaReferencePreparationError("ark_asset_activation_timeout")

    async def prepare(self, job: dict, *, require_ark: bool) -> list[dict]:
        output: list[dict] = []
        for reference in job.get("references") or []:
            prepared = await asyncio.to_thread(self.objects.prepare_media_upload,
                kind=reference["kind"], digest=reference["sha256"], mime=reference["mime"],
                size=reference["size"], expires_seconds=900)
            record = await self.store.ensure_media_reference_asset(reference,
                object_key=prepared["object_key"], public_url=prepared["public_url"])
            for _ in range(240):
                if record["boss_status"] == "active" and (not require_ark or record["ark_status"] == "active"):
                    break
                if require_ark and record["ark_status"] == "failed":
                    raise MediaReferencePreparationError("ark_asset_processing_failed:" + record["ark_error"][:80])
                record, claimed = await self.store.claim_media_reference_asset(reference["sha256"], self.worker_owner)
                if not claimed:
                    await self.store.update_media_job(job["id"], self.worker_owner, status="running",
                        phase="waiting_reference_asset")
                    await asyncio.sleep(1)
                    record = await self.store.get_media_reference_asset(reference["sha256"])
                    continue
                try:
                    if record["boss_status"] != "active":
                        record = await self._publish_boss(record, prepared)
                    if require_ark and record["ark_status"] != "active":
                        record = await self._activate_ark(record, job["id"])
                    record = await self.store.update_media_reference_asset(reference["sha256"], self.worker_owner,
                        release=True)
                except (ArkAssetError, MediaReferencePreparationError) as exc:
                    try:
                        await self.store.update_media_reference_asset(reference["sha256"], self.worker_owner,
                            ark_error=str(exc), release=True)
                    except RuntimeError:
                        pass
                    raise MediaReferencePreparationError(str(exc)) from exc
                break
            else:
                raise MediaReferencePreparationError("media_reference_asset_wait_timeout")
            item = {**reference, "public_url": record["public_url"]}
            if require_ark:
                item["asset_uri"] = "asset://" + record["ark_asset_id"]
            output.append(item)
            event("media.reference.ready", job_id=job["id"], sha256=reference["sha256"][:12],
                kind=reference["kind"], ark=require_ark)
        return output
