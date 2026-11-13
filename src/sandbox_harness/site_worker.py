from __future__ import annotations

import asyncio
import hashlib
import uuid

from .observability import event
from .site_bundle import MAX_SITE_ARCHIVE_BYTES, validate_site_archive


class SiteWorker:
    def __init__(self, settings, store, objects):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.owner = "site-worker-" + uuid.uuid4().hex
        self.stop_event = asyncio.Event()
        self.concurrency = settings.site_worker_concurrency

    async def run(self) -> None:
        active: set[asyncio.Task] = set()
        try:
            while not self.stop_event.is_set():
                finished = {task for task in active if task.done()}
                active.difference_update(finished)
                for task in finished:
                    if not task.cancelled() and task.exception():
                        event("site.worker.task_failed", level=40,
                            exception_type=type(task.exception()).__name__)
                available = self.concurrency - len(active)
                if available > 0:
                    for job in await self.store.claim_site_jobs(self.owner, limit=available):
                        active.add(asyncio.create_task(self.process(job), name="site-job-" + job["id"]))
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
        finally:
            for task in active:
                task.cancel()
            if active:
                await asyncio.gather(*active, return_exceptions=True)

    async def process(self, job: dict) -> None:
        try:
            archive = await asyncio.to_thread(self.objects.get, job["archive_object_ref"],
                maximum=MAX_SITE_ARCHIVE_BYTES)
            if len(archive) != job["archive_size"] or hashlib.sha256(archive).hexdigest() != job["bundle_sha256"]:
                raise ValueError("site_bundle_identity_mismatch")
            files = await asyncio.to_thread(validate_site_archive, archive)
            if (len(files) != job["file_count"]
                    or sum(value.size for value in files) != job["total_size"]):
                raise ValueError("site_bundle_manifest_mismatch")
            staged = {value["path"]:value for value in job.get("files", [])}
            if set(staged) != {value.path for value in files}:
                raise ValueError("site_staged_files_mismatch")
            if not await self.store.update_site_job(job["id"], self.owner,
                    phase="uploading", progress=15):
                return
            uploaded = []
            for index, value in enumerate(files):
                existing = staged[value.path]
                if (existing["sha256"] != value.sha256 or existing["size"] != value.size
                        or existing["mime"] != value.mime
                        or existing["object_ref"] != "sha256:" + value.sha256):
                    raise ValueError("site_file_identity_mismatch")
                uploaded.append({"path":value.path, "sha256":value.sha256,
                    "size":value.size, "mime":value.mime, "object_ref":existing["object_ref"]})
                progress = 15 + int(75 * (index + 1) / len(files))
                if not await self.store.update_site_job(job["id"], self.owner,
                        phase="uploading", progress=progress):
                    return
            if not await self.store.update_site_job(job["id"], self.owner,
                    phase="activating", progress=95):
                return
            if await self.store.complete_site_job(job["id"], self.owner,
                    bundle_sha256=job["bundle_sha256"], files=uploaded):
                event("site.job.succeeded", job_id=job["id"], project_id=job["project_id"],
                    files=len(uploaded), bytes=sum(value["size"] for value in uploaded))
        except ValueError as exc:
            await self.store.fail_site_job(job["id"], self.owner,
                error_code=str(exc)[:100], error_detail="Site bundle validation failed")
            event("site.job.invalid", level=30, job_id=job["id"], error_code=str(exc)[:100])
        except Exception as exc:
            await self.store.fail_site_job(job["id"], self.owner,
                error_code="site_worker_error", error_detail=type(exc).__name__)
            event("site.job.failed", level=40, job_id=job["id"],
                exception_type=type(exc).__name__)
