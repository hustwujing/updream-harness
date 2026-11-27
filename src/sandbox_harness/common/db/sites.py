"""Sites persistence; transaction boundaries are preserved."""
from __future__ import annotations
import hashlib
from collections import (defaultdict)
from datetime import (datetime, timedelta)
from pathlib import (PurePosixPath)
from sqlalchemy import (or_, func, select, text, update)
from sandbox_harness.common.contracts.domain import (identifier, now, SiteDeployJobView, SiteDraftView, SiteProjectView, SiteVersionView)
from sandbox_harness.common.contracts.public_errors import public_job_result
from .models import (ArtifactRow, SiteDeployJobRow, SiteDraftFileRow, SiteDraftRow, SiteFileRow, SiteProjectRow, SiteVersionRow, ThreadRow, TurnRow, canonical_digest, utc_datetime)


class SitesTransactions:
    @staticmethod
    def _site_draft_view(row: SiteDraftRow) -> SiteDraftView:
        status = ("expired" if row.status == "awaiting_confirmation"
            and utc_datetime(row.expires_at) <= now() else row.status)
        return SiteDraftView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id,
            project_id=row.project_id, name=row.name, slug=row.slug, status=status,
            file_count=row.file_count, total_size=row.total_size,
            bundle_sha256=row.bundle_sha256, expires_at=utc_datetime(row.expires_at),
            job_id=row.job_id or "")


    @staticmethod
    def _site_job_view(row: SiteDeployJobRow, public_url: str = "") -> SiteDeployJobView:
        error=public_job_result({"error_code":row.error_code,"error_detail":row.error_detail},
            identity=row.id,origin="site-worker") if row.error_code else {}
        return SiteDeployJobView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id,
            project_id=row.project_id, draft_id=row.draft_id, name=row.name, slug=row.slug,
            status=row.status, phase=row.phase or "", progress=row.progress,
            error_code=row.error_code or "", error_detail=row.error_detail or "",
            error_message=error.get("error_message",""),
            error_reason=error.get("error_reason",""),
            error_action=error.get("error_guidance",""),
            troubleshooting_id=error.get("troubleshooting_id",""),
            attempt_count=row.attempt_count, retryable=row.status=="retry_exhausted",
            public_url=public_url,
            created_at=utc_datetime(row.created_at),
            finished_at=utc_datetime(row.finished_at) if row.finished_at else None)


    async def create_site_draft(self, owner_id: str, thread_id: str, turn_id: str, *,
                                name: str, slug: str, idempotency_key: str,
                                archive_object_ref: str, bundle_sha256: str,
                                archive_size: int, file_count: int, total_size: int,
                                expires_at: datetime, public_base_path: str,
                                files: list[dict] | None = None) -> tuple[SiteDraftView, bool]:
        files = files or []
        if (len(files) != file_count or sum(value.get("size", -1) for value in files) != total_size
                or len({value.get("path") for value in files}) != file_count
                or not any(value.get("path") == "index.html" for value in files)):
            raise ValueError("site_draft_files_invalid")
        request_digest=canonical_digest({"turn_id":turn_id,"name":name,"requested_slug":slug,
            "archive_object_ref":archive_object_ref,"bundle_sha256":bundle_sha256,
            "archive_size":archive_size,"file_count":file_count,"total_size":total_size,
            "files":sorted(files,key=lambda value:value["path"])})
        timestamp = now(); draft_id = identifier()
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if db.bind.dialect.name=="sqlite":
                await db.execute(update(ThreadRow).where(ThreadRow.id==thread_id).values(
                    updated_at=ThreadRow.updated_at))
            elif db.bind.dialect.name=="postgresql":
                lock_id=int.from_bytes(hashlib.sha256(slug.encode()).digest()[:8],"big",signed=True)
                await db.execute(text("SELECT pg_advisory_xact_lock(:lock_id)"),{"lock_id":lock_id})
            if turn_id:
                turn = await db.get(TurnRow, turn_id)
                if not turn or turn.thread_id != thread_id:
                    raise KeyError("turn_not_found")
            existing = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id,
                SiteDraftRow.idempotency_key == idempotency_key).with_for_update())).scalar_one_or_none()
            if existing:
                if ((existing.request_digest and existing.request_digest!=request_digest)
                        or (not existing.request_digest and (existing.bundle_sha256 != bundle_sha256
                            or existing.name != name or existing.archive_size != archive_size))):
                    raise RuntimeError("site_draft_idempotency_conflict")
                return self._site_draft_view(existing), False
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id, SiteProjectRow.owner_id == owner_id,
                SiteProjectRow.slug == slug).with_for_update())).scalar_one_or_none()
            if project is None:
                project_id = identifier(); candidate = slug
                collision = (await db.execute(select(SiteProjectRow.id).where(
                    SiteProjectRow.slug == candidate))).scalar_one_or_none()
                if collision:
                    candidate = f"{slug[:41]}-{project_id[:6]}"
                project = SiteProjectRow(id=project_id, owner_id=owner_id, thread_id=thread_id,
                    name=name, slug=candidate, status="draft", current_version_id="",
                    public_url=public_base_path.rstrip("/") + "/" + candidate + "/",
                    created_at=timestamp, updated_at=timestamp)
                db.add(project)
            elif project.status == "unpublished":
                project.name = name; project.updated_at = timestamp
            row = SiteDraftRow(id=draft_id, owner_id=owner_id, thread_id=thread_id,
                turn_id=turn_id, project_id=project.id, name=name, slug=project.slug,
                archive_object_ref=archive_object_ref, bundle_sha256=bundle_sha256,
                archive_size=archive_size, file_count=file_count, total_size=total_size,
                idempotency_key=idempotency_key, request_digest=request_digest,
                status="awaiting_confirmation",
                expires_at=expires_at, job_id="", created_at=timestamp, updated_at=timestamp)
            db.add(row)
            for value in files:
                db.add(SiteDraftFileRow(id=identifier(), draft_id=row.id, path=value["path"],
                    sha256=value["sha256"], size=value["size"], mime=value["mime"],
                    object_ref=value["object_ref"], created_at=timestamp))
        return self._site_draft_view(row), True


    async def get_site_draft(self, draft_id: str, owner_id: str) -> SiteDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return self._site_draft_view(row) if row else None


    async def site_draft_by_bundle(self, thread_id: str, bundle_sha256: str) -> SiteDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id,
                SiteDraftRow.bundle_sha256 == bundle_sha256,
            ).order_by(SiteDraftRow.created_at).limit(1))).scalar_one_or_none()
        return self._site_draft_view(row) if row else None


    async def artifact_site_file(self, artifact_id: str, path: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            root = (await db.execute(select(ArtifactRow).join(TurnRow,
                TurnRow.id == ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(ArtifactRow.id == artifact_id,
                    ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not root or PurePosixPath(root.path).name.lower() != "index.html":
                return None
            prefix = PurePosixPath(root.path).parent
            target = (prefix / path).as_posix() if prefix.as_posix() != "." else path
            row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == root.turn_id, ArtifactRow.path == target))).scalar_one_or_none()
        return ({"object_ref":row.object_ref, "size":row.size, "sha256":row.sha256,
            "mime":row.mime} if row else None)


    async def get_site_draft_archive(self, draft_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return ({"object_ref":row.archive_object_ref, "size":row.archive_size,
            "sha256":row.bundle_sha256} if row else None)


    async def get_site_draft_file(self, draft_id: str, path: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftFileRow).join(SiteDraftRow,
                SiteDraftRow.id == SiteDraftFileRow.draft_id).where(
                    SiteDraftFileRow.draft_id == draft_id, SiteDraftFileRow.path == path,
                    SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return ({"object_ref":row.object_ref, "size":row.size, "sha256":row.sha256,
            "mime":row.mime} if row else None)


    async def list_site_drafts(self, thread_id: str, owner_id: str) -> list[SiteDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id, SiteDraftRow.owner_id == owner_id,
            ).order_by(SiteDraftRow.created_at.desc()))).scalars().all()
        return [self._site_draft_view(row) for row in rows]


    async def cancel_site_draft(self, draft_id: str, owner_id: str) -> SiteDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("site_draft_not_found")
            if row.status == "confirmed":
                raise RuntimeError("site_draft_already_confirmed")
            if row.status == "awaiting_confirmation":
                row.status = "cancelled"; row.updated_at = now()
        return self._site_draft_view(row)


    async def confirm_site_draft(self, draft_id: str, owner_id: str) -> SiteDeployJobView:
        timestamp = now()
        async with self.connections.begin() as db:
            draft = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not draft:
                raise KeyError("site_draft_not_found")
            project = await db.get(SiteProjectRow, draft.project_id)
            if draft.job_id:
                job = await db.get(SiteDeployJobRow, draft.job_id)
                if job:
                    return self._site_job_view(job, project.public_url if project else "")
            if draft.status != "awaiting_confirmation" or utc_datetime(draft.expires_at) <= timestamp:
                raise RuntimeError("site_draft_not_confirmable")
            job = SiteDeployJobRow(id=identifier(), owner_id=owner_id,
                thread_id=draft.thread_id, turn_id=draft.turn_id, project_id=draft.project_id,
                draft_id=draft.id, name=draft.name, slug=draft.slug, status="created",
                phase="queued", progress=0,traceparent=draft.traceparent,
                created_at=timestamp, updated_at=timestamp)
            db.add(job); draft.status = "confirmed"; draft.job_id = job.id; draft.updated_at = timestamp
        return self._site_job_view(job, project.public_url if project else "")


    async def claim_site_jobs(self, owner: str, limit: int = 2,
                              lease_seconds: int = 60) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.status.in_(["created", "running", "retry_wait"]),
                or_(SiteDeployJobRow.next_attempt_at.is_(None),
                    SiteDeployJobRow.next_attempt_at <= now()),
                or_(SiteDeployJobRow.lease_expires_at.is_(None),
                    SiteDeployJobRow.lease_expires_at < now()),
            ).order_by(SiteDeployJobRow.created_at).limit(limit).with_for_update())).scalars().all()
            result = []
            for row in rows:
                draft = await db.get(SiteDraftRow, row.draft_id)
                if not draft:
                    row.status = "failed"; row.phase = "terminal"
                    row.error_code = "site_draft_missing"; row.finished_at = now(); row.updated_at = now()
                    continue
                row.status = "running"; row.phase = "validating"; row.progress = 5
                row.generation += 1; row.attempt_count += 1; row.next_attempt_at=None
                row.lease_owner = owner
                row.lease_expires_at = now() + timedelta(seconds=lease_seconds)
                row.updated_at = now()
                result.append({"id":row.id, "project_id":row.project_id,
                    "draft_id":row.draft_id, "thread_id":row.thread_id, "owner_id":row.owner_id,
                    "generation":row.generation,"attempt_count":row.attempt_count,
                    "traceparent":row.traceparent,
                    "archive_object_ref":draft.archive_object_ref,
                    "archive_size":draft.archive_size, "bundle_sha256":draft.bundle_sha256,
                    "file_count":draft.file_count, "total_size":draft.total_size,
                    "files":[{"path":value.path, "sha256":value.sha256, "size":value.size,
                        "mime":value.mime, "object_ref":value.object_ref} for value in
                        (await db.execute(select(SiteDraftFileRow).where(
                            SiteDraftFileRow.draft_id == draft.id))).scalars().all()]})
            return result


    async def renew_site_job_lease(self,job_id:str,owner:str,generation:int,
                                   lease_seconds:int=60)->bool:
        async with self.connections.begin() as db:
            result=await db.execute(update(SiteDeployJobRow).where(
                SiteDeployJobRow.id==job_id,SiteDeployJobRow.lease_owner==owner,
                SiteDeployJobRow.generation==generation,SiteDeployJobRow.status=="running").values(
                    lease_expires_at=now()+timedelta(seconds=lease_seconds),updated_at=now()))
            return bool(result.rowcount)


    async def update_site_job(self, job_id: str, owner: str, generation: int, *, phase: str,
                              progress: int, error_code: str = "", error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
                SiteDeployJobRow.generation == generation,
            ).with_for_update())).scalar_one_or_none()
            if not row:
                raise RuntimeError("stale_site_job_lease")
            row.phase = phase; row.progress = progress; row.error_code = error_code
            row.error_detail = error_detail[:1000]; row.updated_at = now()
            return True


    async def fail_site_job(self, job_id: str, owner: str, generation: int, *, error_code: str,
                            error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
                SiteDeployJobRow.generation == generation,
            ).with_for_update())).scalar_one_or_none()
            if not row:
                raise RuntimeError("stale_site_job_lease")
            row.status = "failed"; row.phase = "terminal"; row.progress = None
            row.error_code = error_code; row.error_detail = error_detail[:1000]
            row.finished_at = now(); row.updated_at = row.finished_at
            row.lease_owner = ""; row.lease_expires_at = None
            return True


    async def defer_site_job(self,job_id:str,owner:str,generation:int,*,error_code:str,
                             error_detail:str,retry_after_seconds:int,max_attempts:int)->bool:
        async with self.connections.begin() as db:
            row=(await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id==job_id,SiteDeployJobRow.lease_owner==owner,
                SiteDeployJobRow.generation==generation).with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_site_job_lease")
            exhausted=row.attempt_count>=max_attempts
            row.status="retry_exhausted" if exhausted else "retry_wait"
            row.phase="retry_exhausted" if exhausted else "retry_wait"
            row.progress=None;row.error_code=error_code;row.error_detail=error_detail[:1000]
            row.next_attempt_at=None if exhausted else now()+timedelta(seconds=retry_after_seconds)
            row.finished_at=now() if exhausted else None;row.updated_at=now()
            row.lease_owner="";row.lease_expires_at=None
            return True


    async def complete_site_job(self, job_id: str, owner: str, generation: int, *, bundle_sha256: str,
                                files: list[dict]) -> bool:
        if not files or not any(value["path"] == "index.html" for value in files):
            raise ValueError("site_files_invalid")
        timestamp = now()
        async with self.connections.begin() as db:
            job = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
                SiteDeployJobRow.generation == generation,
            ).with_for_update())).scalar_one_or_none()
            if not job:
                raise RuntimeError("stale_site_job_lease")
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == job.project_id).with_for_update())).scalar_one_or_none()
            draft = await db.get(SiteDraftRow, job.draft_id)
            if not project or not draft or draft.bundle_sha256 != bundle_sha256:
                raise ValueError("site_job_contract_mismatch")
            existing = (await db.execute(select(SiteVersionRow).where(
                SiteVersionRow.project_id == project.id,
                SiteVersionRow.bundle_sha256 == bundle_sha256).with_for_update())).scalar_one_or_none()
            if existing is None:
                latest = (await db.execute(select(func.max(SiteVersionRow.version)).where(
                    SiteVersionRow.project_id == project.id))).scalar_one() or 0
                existing = SiteVersionRow(id=identifier(), project_id=project.id, job_id=job.id,
                    version=latest + 1, bundle_sha256=bundle_sha256, file_count=len(files),
                    total_size=sum(value["size"] for value in files), created_at=timestamp)
                db.add(existing)
                for value in files:
                    db.add(SiteFileRow(id=identifier(), version_id=existing.id, path=value["path"],
                        sha256=value["sha256"], size=value["size"], mime=value["mime"],
                        object_ref=value["object_ref"], created_at=timestamp))
            project.current_version_id = existing.id; project.status = "active"
            project.name = job.name; project.updated_at = timestamp
            job.status = "succeeded"; job.phase = "activated"; job.progress = 100
            job.error_code = ""; job.error_detail = ""; job.finished_at = timestamp
            job.updated_at = timestamp; job.lease_owner = ""; job.lease_expires_at = None
            draft.status = "deployed"; draft.updated_at = timestamp
            return True


    async def retry_site_job(self,job_id:str,owner_id:str)->SiteDeployJobView:
        async with self.connections.begin() as db:
            row=(await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id==job_id,SiteDeployJobRow.owner_id==owner_id)
                .with_for_update())).scalar_one_or_none()
            if not row:raise KeyError("site_job_not_found")
            if row.status!="retry_exhausted":raise RuntimeError("site_job_not_retryable")
            row.status="retry_wait";row.phase="retry_wait";row.next_attempt_at=now()
            row.attempt_count=0;row.finished_at=None;row.updated_at=now()
            project=await db.get(SiteProjectRow,row.project_id)
        return self._site_job_view(row,project.public_url if project else "")


    async def release_site_job_leases(self, owner: str) -> int:
        async with self.connections.begin() as db:
            result = await db.execute(update(SiteDeployJobRow).where(
                SiteDeployJobRow.lease_owner == owner,
                SiteDeployJobRow.status == "running",
            ).values(lease_owner="", lease_expires_at=None, updated_at=now()))
            return result.rowcount or 0


    async def get_site_job(self, job_id: str, owner_id: str) -> SiteDeployJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id,
                SiteDeployJobRow.owner_id == owner_id))).scalar_one_or_none()
            project = await db.get(SiteProjectRow, row.project_id) if row else None
        return self._site_job_view(row, project.public_url if project else "") if row else None


    async def get_site_job_for_thread(self, job_id: str, thread_id: str) -> SiteDeployJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id,
                SiteDeployJobRow.thread_id == thread_id))).scalar_one_or_none()
            project = await db.get(SiteProjectRow, row.project_id) if row else None
        return self._site_job_view(row, project.public_url if project else "") if row else None


    async def list_site_jobs(self, thread_id: str, owner_id: str) -> list[SiteDeployJobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.thread_id == thread_id,
                SiteDeployJobRow.owner_id == owner_id,
            ).order_by(SiteDeployJobRow.created_at.desc()))).scalars().all()
            projects = {value.id:value for value in (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id.in_([row.project_id for row in rows])))).scalars().all()} if rows else {}
        return [self._site_job_view(row, projects[row.project_id].public_url) for row in rows]


    async def list_site_projects(self, thread_id: str, owner_id: str) -> list[SiteProjectView]:
        async with self.connections() as db:
            projects = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id, SiteProjectRow.owner_id == owner_id,
            ).order_by(SiteProjectRow.updated_at.desc()))).scalars().all()
            versions = (await db.execute(select(SiteVersionRow).where(
                SiteVersionRow.project_id.in_([row.id for row in projects]),
            ).order_by(SiteVersionRow.version.desc()))).scalars().all() if projects else []
        grouped: dict[str, list[SiteVersionView]] = defaultdict(list)
        current = {row.id:row.current_version_id for row in projects}
        for row in versions:
            grouped[row.project_id].append(SiteVersionView(id=row.id, version=row.version,
                bundle_sha256=row.bundle_sha256, file_count=row.file_count,
                total_size=row.total_size, created_at=utc_datetime(row.created_at),
                active=current[row.project_id] == row.id))
        return [SiteProjectView(id=row.id, thread_id=row.thread_id, name=row.name,
            slug=row.slug, status=row.status, public_url=row.public_url,
            current_version_id=row.current_version_id, versions=grouped[row.id],
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at))
            for row in projects]


    async def activate_site_version(self, project_id: str, version_id: str,
                                    owner_id: str) -> SiteProjectView:
        async with self.connections.begin() as db:
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == project_id, SiteProjectRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            version = await db.get(SiteVersionRow, version_id)
            if not project or not version or version.project_id != project.id:
                raise KeyError("site_version_not_found")
            project.current_version_id = version.id; project.status = "active"; project.updated_at = now()
            row = project
        projects = await self.list_site_projects(row.thread_id, owner_id)
        return next(value for value in projects if value.id == row.id)


    async def unpublish_site(self, project_id: str, owner_id: str) -> SiteProjectView:
        async with self.connections.begin() as db:
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == project_id, SiteProjectRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not project:
                raise KeyError("site_project_not_found")
            project.status = "unpublished"; project.updated_at = now(); thread_id = project.thread_id
        projects = await self.list_site_projects(thread_id, owner_id)
        return next(value for value in projects if value.id == project_id)


    async def public_site_file(self, slug: str, path: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteFileRow, SiteProjectRow).join(SiteProjectRow,
                SiteProjectRow.current_version_id == SiteFileRow.version_id).where(
                    SiteProjectRow.slug == slug, SiteProjectRow.status == "active",
                    SiteFileRow.path == path))).first()
        if not row:
            return None
        file, project = row
        return {"project_id":project.id, "version_id":project.current_version_id,
            "path":file.path, "sha256":file.sha256, "size":file.size,
            "mime":file.mime, "object_ref":file.object_ref}
