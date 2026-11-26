"""Sites authority used by the independently deployed SiteWorker."""
from datetime import datetime
import hashlib

from sqlalchemy import delete, func, select

from sandbox_harness.common.contracts.domain import identifier, now
from .domain_database import DomainDatabase
from .lifecycle import LifecycleParticipant
from .models import (SiteDeployJobRow, SiteDraftFileRow, SiteDraftRow, SiteFileRow,
                     SiteProjectRow, SiteVersionRow, canonical_digest)
from .sites import SitesTransactions


class SiteRepository(DomainDatabase, SitesTransactions):
    def __init__(self, database_url: str, *, handoff_max_attempts: int = 8,
                 handoff_max_age_seconds: int = 900):
        super().__init__("sites", database_url)
        self.lifecycle = LifecycleParticipant(self, self._check_lifecycle, self._apply_lifecycle,
            handoff_max_attempts=handoff_max_attempts,
            handoff_max_age_seconds=handoff_max_age_seconds)
        self.handoff = self.lifecycle.handoff
        self.handoff.handlers["site.turn.draft"] = self._receive_turn_draft

    async def _check_lifecycle(self, db, proposal):
        query = select(SiteDeployJobRow.id).where(
            SiteDeployJobRow.owner_id == proposal.owner_id,
            SiteDeployJobRow.status.in_(["created", "running", "retry_wait"]))
        if proposal.thread_id:
            query = query.where(SiteDeployJobRow.thread_id == proposal.thread_id)
        return "active_site_jobs_prevent_lifecycle" if await db.scalar(query.limit(1)) else ""

    async def _apply_lifecycle(self, db, proposal):
        if proposal.action != "delete":
            return
        projects = select(SiteProjectRow.id).where(SiteProjectRow.thread_id == proposal.thread_id)
        drafts = select(SiteDraftRow.id).where(SiteDraftRow.thread_id == proposal.thread_id)
        versions = select(SiteVersionRow.id).where(SiteVersionRow.project_id.in_(projects))
        await db.execute(delete(SiteFileRow).where(SiteFileRow.version_id.in_(versions)))
        await db.execute(delete(SiteVersionRow).where(SiteVersionRow.id.in_(versions)))
        await db.execute(delete(SiteDeployJobRow).where(
            SiteDeployJobRow.thread_id == proposal.thread_id))
        await db.execute(delete(SiteDraftFileRow).where(SiteDraftFileRow.draft_id.in_(drafts)))
        await db.execute(delete(SiteDraftRow).where(SiteDraftRow.id.in_(drafts)))
        await db.execute(delete(SiteProjectRow).where(SiteProjectRow.id.in_(projects)))

    async def _receive_turn_draft(self, db, envelope):
        value = envelope.payload
        if (envelope.source != "core" or envelope.owner_id != value.get("owner_id")
                or envelope.generation != value.get("thread_generation")):
            raise ValueError("site_turn_envelope_invalid")
        request_digest = canonical_digest({key:value[key] for key in (
            "turn_id","name","requested_slug","archive_object_ref","bundle_sha256",
            "archive_size","file_count","total_size","files")})
        existing = await db.scalar(select(SiteDraftRow).where(
            SiteDraftRow.thread_id == value["thread_id"],
            SiteDraftRow.idempotency_key == value["idempotency_key"]).with_for_update())
        if existing:
            if existing.id != value["draft_id"] or existing.request_digest != request_digest:
                raise ValueError("site_draft_idempotency_conflict")
            return {"draft_id":existing.id}
        if db.bind.dialect.name == "postgresql":
            key = int.from_bytes(hashlib.sha256(value["requested_slug"].encode()).digest()[:8],
                                 "big", signed=True)
            await db.execute(select(func.pg_advisory_xact_lock(key)))
        timestamp = now()
        project = await db.scalar(select(SiteProjectRow).where(
            SiteProjectRow.thread_id == value["thread_id"],
            SiteProjectRow.owner_id == value["owner_id"],
            SiteProjectRow.slug == value["requested_slug"]).with_for_update())
        if project is None:
            project_id = value["project_id"]; candidate = value["requested_slug"]
            if await db.scalar(select(SiteProjectRow.id).where(SiteProjectRow.slug == candidate)):
                candidate = f"{candidate[:41]}-{project_id[:6]}"
            project = SiteProjectRow(id=project_id, owner_id=value["owner_id"],
                thread_id=value["thread_id"], name=value["name"], slug=candidate,
                status="draft", current_version_id="",
                public_url=value["public_base_path"].rstrip("/")+"/"+candidate+"/",
                created_at=timestamp, updated_at=timestamp)
            db.add(project)
        elif project.status == "unpublished":
            project.name=value["name"];project.updated_at=timestamp
        draft=SiteDraftRow(id=value["draft_id"],owner_id=value["owner_id"],
            thread_id=value["thread_id"],turn_id=value["turn_id"],project_id=project.id,
            name=value["name"],slug=project.slug,archive_object_ref=value["archive_object_ref"],
            bundle_sha256=value["bundle_sha256"],archive_size=value["archive_size"],
            file_count=value["file_count"],total_size=value["total_size"],
            idempotency_key=value["idempotency_key"],request_digest=request_digest,
            status="awaiting_confirmation",expires_at=datetime.fromisoformat(value["expires_at"]),
            job_id="",created_at=timestamp,updated_at=timestamp)
        db.add(draft)
        for item in value["files"]:
            db.add(SiteDraftFileRow(id=identifier(),draft_id=draft.id,path=item["path"],
                sha256=item["sha256"],size=item["size"],mime=item["mime"],
                object_ref=item["object_ref"],created_at=timestamp))
        await db.flush()
        return {"draft_id":draft.id}
