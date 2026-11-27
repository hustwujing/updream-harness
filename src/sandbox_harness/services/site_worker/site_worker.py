from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
import uuid

from sandbox_harness.common.observability.observability import (add_link, event, operation)
from sandbox_harness.common.storage.directories import (ensure_private_directory)
from sandbox_harness.common.sites.site_bundle import (MAX_SITE_ARCHIVE_BYTES, stage_site_archive)


class SiteWorker:
    def __init__(self, settings, store, objects):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.owner = "site-worker-" + uuid.uuid4().hex
        self.stop_event = asyncio.Event()
        self.concurrency = settings.site_worker_concurrency
        self.lease_seconds = int(getattr(settings, "site_job_lease_seconds", 60))
        self.max_attempts = int(getattr(settings, "site_job_max_attempts", 4))
        self.retry_base = int(getattr(settings, "site_job_retry_base_seconds", 5))
        data_root = Path(settings.data_root)
        self.staging_root = data_root / "tmp" / "site-stages"

    async def run(self) -> None:
        active: set[asyncio.Task] = set()
        try:
            while not self.stop_event.is_set():
                finished = {task for task in active if task.done()}
                active.difference_update(finished)
                for task in finished:
                    if not task.cancelled() and task.exception():
                        event("site.worker.task_failed", level=40,
                            exc=task.exception())
                available = self.concurrency - len(active)
                if available > 0:
                    jobs = await self.store.claim_site_jobs(self.owner, limit=available,
                        lease_seconds=self.lease_seconds)
                    for job in jobs:
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
        generation=job.get("generation")
        if not isinstance(generation,int) or generation<1:
            raise ValueError("site_job_generation_required")
        lost=asyncio.Event();owner=asyncio.current_task()
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.1,self.lease_seconds/3))
                try:
                    renewed=await self.store.renew_site_job_lease(job["id"],self.owner,
                        generation,self.lease_seconds)
                except Exception as exc:
                    renewed=False
                    event("site.worker.heartbeat_failed",level=40,job_id=job["id"],
                        generation=generation,exc=exc)
                if not renewed:
                    lost.set();owner.cancel();return
        heartbeat_task=asyncio.create_task(heartbeat(),name="site-heartbeat-"+job["id"])
        try:
            with operation('site.worker.claim',new_trace=True,job_id=job['id'],
                    generation=job['generation']):
                add_link(job.get('traceparent',''))
                await self._process_claimed(job)
        except asyncio.CancelledError:
            if not lost.is_set():raise
            event("site.worker.lease_lost",level=30,job_id=job["id"],generation=generation)
        finally:
            heartbeat_task.cancel();await asyncio.gather(heartbeat_task,return_exceptions=True)

    def _copy_archive(self, job: dict) -> Path:
        ensure_private_directory(self.staging_root,boundary=self.staging_root.parent)
        target=None
        try:
            with NamedTemporaryFile(dir=self.staging_root,
                    prefix=f"site-archive-{os.getpid()}-",delete=False) as output:
                target=Path(output.name);digest=hashlib.sha256();size=0
                for chunk in self.objects.iter_bytes(job["archive_object_ref"],maximum=MAX_SITE_ARCHIVE_BYTES):
                    size+=len(chunk);digest.update(chunk);output.write(chunk)
        except Exception:
            if target:target.unlink(missing_ok=True)
            raise
        os.chmod(target,0o600)
        if size!=job["archive_size"] or digest.hexdigest()!=job["bundle_sha256"]:
            target.unlink(missing_ok=True);raise ValueError("site_bundle_identity_mismatch")
        return target

    def _verify_or_repair(self, value, expected: dict) -> None:
        if (expected["sha256"]!=value.sha256 or expected["size"]!=value.size
                or expected["mime"]!=value.mime or expected["object_ref"]!="sha256:"+value.sha256):
            raise ValueError("site_file_identity_mismatch")
        try:
            size=sum(len(chunk) for chunk in self.objects.iter_bytes(
                expected["object_ref"],maximum=value.size))
            if size!=value.size:raise ValueError("site_file_identity_mismatch")
        except (FileNotFoundError,ValueError):
            self.objects.put_file(value.source,digest=value.sha256,size=value.size,repair=True)
            size=sum(len(chunk) for chunk in self.objects.iter_bytes(
                expected["object_ref"],maximum=value.size))
            if size!=value.size:raise ValueError("site_file_identity_mismatch")

    async def _process_claimed(self, job: dict) -> None:
        archive_path=None;package=None;generation=job["generation"]
        try:
            archive_path=await asyncio.to_thread(self._copy_archive,job)
            package=await asyncio.to_thread(stage_site_archive,archive_path,self.staging_root)
            files=package.files
            if len(files)!=job["file_count"] or sum(value.size for value in files)!=job["total_size"]:
                raise ValueError("site_bundle_manifest_mismatch")
            staged={value["path"]:value for value in job.get("files",[])}
            if set(staged)!={value.path for value in files}:
                raise ValueError("site_staged_files_mismatch")
            await self.store.update_site_job(job["id"],self.owner,generation,
                phase="verifying_files",progress=15)
            uploaded=[]
            for index,value in enumerate(files):
                await asyncio.to_thread(self._verify_or_repair,value,staged[value.path])
                uploaded.append({"path":value.path,"sha256":value.sha256,"size":value.size,
                    "mime":value.mime,"object_ref":staged[value.path]["object_ref"]})
                await self.store.update_site_job(job["id"],self.owner,generation,
                    phase="verifying_files",progress=15+int(75*(index+1)/len(files)))
            await self.store.update_site_job(job["id"],self.owner,generation,
                phase="activating",progress=95)
            if not await self.store.complete_site_job(job["id"],self.owner,generation,
                    bundle_sha256=job["bundle_sha256"],files=uploaded):
                raise RuntimeError("stale_site_job_lease")
            event("site.job.succeeded",job_id=job["id"],project_id=job["project_id"],
                files=len(uploaded),bytes=sum(value["size"] for value in uploaded))
        except PermissionError:
            await self.store.fail_site_job(job["id"],self.owner,generation,
                error_code="site_staging_unwritable",
                error_detail="站点发布暂存目录不可写，请修复部署目录权属后继续发布。")
            event("site.job.staging_unwritable",level=40,job_id=job["id"],
                error_code="site_staging_unwritable")
        except ValueError as exc:
            await self.store.fail_site_job(job["id"],self.owner,generation,
                error_code=str(exc)[:100],error_detail="站点包内容无效，请修正文件后重新准备部署。")
            event("site.job.invalid",level=30,job_id=job["id"],error_code=str(exc)[:100])
        except Exception as exc:
            delay=min(300,self.retry_base*(2**max(0,job.get("attempt_count",1)-1)))
            await self.store.defer_site_job(job["id"],self.owner,generation,
                error_code="site_dependency_unavailable",
                error_detail="发布依赖暂时不可用，系统已自动重试；若重试耗尽可手动继续。",
                retry_after_seconds=delay,max_attempts=self.max_attempts)
            event("site.job.retry_scheduled",level=30,job_id=job["id"],
                exc=exc,retry_seconds=delay)
        finally:
            if package:package.remove()
            if archive_path:archive_path.unlink(missing_ok=True)
