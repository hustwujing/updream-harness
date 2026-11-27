"""Media authority used by MediaWorker without core database access."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import PurePosixPath

from sqlalchemy import delete, func, or_, select, update

from sandbox_harness.common.contracts.domain import identifier, now
from sandbox_harness.common.contracts.media_provider import MediaCancellationRequested
from .domain_database import DomainDatabase
from .lifecycle import LifecycleParticipant
from .media_drafts import MediaDraftsTransactions
from .media_jobs import MediaJobsTransactions
from .media_models import MediaSourcePermitRow
from .media_references import MediaReferencesTransactions
from .models import (MediaBatchRow, MediaDraftRow, MediaExecutionRow, MediaJobOutputRow,
                     MediaJobRow, MediaOutboxRow, MediaReferenceUploadRow, canonical_digest,
                     canonical_json, utc_datetime)


class MediaRepository(DomainDatabase, MediaReferencesTransactions,
                      MediaDraftsTransactions, MediaJobsTransactions):
    def __init__(self, database_url: str, *, handoff_max_attempts: int = 8,
                 handoff_max_age_seconds: int = 900):
        super().__init__("media", database_url)
        self.lifecycle = LifecycleParticipant(self, self._check_lifecycle, self._apply_lifecycle,
            handoff_max_attempts=handoff_max_attempts,
            handoff_max_age_seconds=handoff_max_age_seconds)
        self.handoff = self.lifecycle.handoff
        self.handoff.handlers.update({
            "media.source.authorize": self._receive_source_job,
            "media.source.cancel": self._receive_source_cancel,
            "media.turn.draft": self._receive_turn_draft,
            "media.turn.seal": self._receive_turn_seal,
            "media.turn.finished": self._receive_turn_finished,
            "media.promotion.ack": self._receive_promotion_ack,
        })

    async def _check_lifecycle(self, db, proposal):
        job_query = select(MediaJobRow.id).where(
            MediaJobRow.owner_id == proposal.owner_id,
            MediaJobRow.status.not_in(["succeeded", "failed", "cancelled", "unknown"]))
        upload_query = select(MediaReferenceUploadRow.id).where(
            MediaReferenceUploadRow.owner_id == proposal.owner_id,
            MediaReferenceUploadRow.status == "pending",
            MediaReferenceUploadRow.expires_at > now())
        batch_query = select(MediaBatchRow.turn_id).where(
            MediaBatchRow.owner_id == proposal.owner_id,
            MediaBatchRow.status.in_(["open", "sealed", "delivering"]))
        if proposal.thread_id:
            job_query = job_query.where(MediaJobRow.thread_id == proposal.thread_id)
            upload_query = upload_query.where(MediaReferenceUploadRow.thread_id == proposal.thread_id)
            batch_query = batch_query.where(MediaBatchRow.thread_id == proposal.thread_id)
        if await db.scalar(job_query.limit(1)):
            return "active_media_jobs_prevent_lifecycle"
        if await db.scalar(upload_query.limit(1)):
            return "pending_media_uploads_prevent_lifecycle"
        if await db.scalar(batch_query.limit(1)):
            return "pending_media_batches_prevent_lifecycle"
        return ""

    async def _apply_lifecycle(self, db, proposal):
        if proposal.action != "delete":
            return
        jobs = select(MediaJobRow.id).where(MediaJobRow.thread_id == proposal.thread_id)
        drafts = select(MediaDraftRow.id).where(MediaDraftRow.thread_id == proposal.thread_id)
        await db.execute(delete(MediaJobOutputRow).where(MediaJobOutputRow.job_id.in_(jobs)))
        await db.execute(delete(MediaExecutionRow).where(MediaExecutionRow.job_id.in_(jobs)))
        await db.execute(delete(MediaOutboxRow).where(MediaOutboxRow.job_id.in_(jobs)))
        await db.execute(delete(MediaSourcePermitRow).where(MediaSourcePermitRow.media_job_id.in_(jobs)))
        await db.execute(delete(MediaJobRow).where(MediaJobRow.thread_id == proposal.thread_id))
        await db.execute(delete(MediaDraftRow).where(MediaDraftRow.id.in_(drafts)))
        await db.execute(delete(MediaBatchRow).where(MediaBatchRow.thread_id == proposal.thread_id))
        await db.execute(delete(MediaReferenceUploadRow).where(
            MediaReferenceUploadRow.thread_id == proposal.thread_id))

    async def _local_media_view(self, db, row):
        if not row:return None
        outputs=await self._media_job_outputs(db,row.id)
        artifact_ids=[value.artifact_id for value in outputs if value.artifact_id]
        return self._media_job_view(row,artifact_ids,outputs)

    async def get_media_job(self, job_id, owner_id):
        async with self.connections() as db:
            row=await db.scalar(select(MediaJobRow).where(
                MediaJobRow.id==job_id,MediaJobRow.owner_id==owner_id))
            return await self._local_media_view(db,row)

    async def get_media_job_for_thread(self, job_id, thread_id):
        async with self.connections() as db:
            row=await db.scalar(select(MediaJobRow).where(
                MediaJobRow.id==job_id,MediaJobRow.thread_id==thread_id))
            return await self._local_media_view(db,row)

    async def list_media_jobs(self, thread_id, owner_id):
        async with self.connections() as db:
            rows=list((await db.scalars(select(MediaJobRow).where(
                MediaJobRow.thread_id==thread_id,MediaJobRow.owner_id==owner_id)
                .order_by(MediaJobRow.created_at.desc()))).all())
            return [await self._local_media_view(db,row) for row in rows]

    async def create_media_reference_upload(self, owner_id, thread_id, value, *, object_key,
                                            public_url, expires_at):
        timestamp = now()
        async with self.transaction() as db:
            row = MediaReferenceUploadRow(id=identifier(), owner_id=owner_id, thread_id=thread_id,
                name=value.name, kind=value.kind, mime=value.mime, size=value.size,
                sha256=value.sha256, object_key=object_key, public_url=public_url,
                status="pending", expires_at=expires_at, created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return self._media_reference_upload_view(row)

    async def finish_media_reference_upload(self, upload_id, owner_id, project_file_id):
        async with self.transaction() as db:
            row = await db.scalar(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id).with_for_update())
            if not row:
                raise KeyError("media_reference_upload_not_found")
            if row.status == "completed":
                if row.project_file_id != project_file_id:
                    raise RuntimeError("media_reference_upload_identity_conflict")
                return self._media_reference_upload_view(row)
            if row.status != "pending" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_reference_upload_expired")
            row.status = "completed"
            row.project_file_id = project_file_id
            row.updated_at = now()
        return self._media_reference_upload_view(row)

    async def _dispatch_lock(self, db, identity: str):
        if db.bind.dialect.name == "postgresql":
            key = int(canonical_digest(["media-dispatch", identity])[:16], 16)
            if key >= 2**63:
                key -= 2**64
            await db.execute(select(func.pg_advisory_xact_lock(key)))

    async def accept_source_job(self, value: dict):
        async with self.transaction() as db:
            return await self._accept_source_job(db, value)

    async def _accept_source_job(self, db, value: dict, *, traceparent: str = ""):
        required = {"dispatch_id", "media_job_id", "source_job_id", "source_generation",
            "request_digest", "owner_id", "thread_id", "kind", "model", "generation_mode",
            "provider", "adapter",
            "execution_mode", "cancellation_mode", "parameters", "references", "output_contracts"}
        if set(value) != required:
            raise ValueError("media_source_command_invalid")
        digest = canonical_digest(value)
        timestamp = now()
        await self._dispatch_lock(db, value["dispatch_id"])
        permit = await db.get(MediaSourcePermitRow, value["dispatch_id"], with_for_update=True)
        if permit:
            if permit.command_digest != digest:
                raise ValueError("media_source_command_conflict")
            return self._media_job_view(await db.get(MediaJobRow, permit.media_job_id))
        if (not re.fullmatch(r"[a-f0-9]{64}", value["request_digest"])
                or type(value["source_generation"]) is not int or value["source_generation"] < 0):
            raise ValueError("media_source_command_invalid")
        if await db.scalar(select(MediaSourcePermitRow.dispatch_id).where(
                MediaSourcePermitRow.source_job_id == value["source_job_id"])):
            raise ValueError("media_source_command_conflict")
        parameters = canonical_json(value["parameters"])
        references = canonical_json(value["references"])
        draft_id = identifier()
        db.add(MediaDraftRow(id=draft_id, owner_id=value["owner_id"], thread_id=value["thread_id"],
            turn_id="", kind=value["kind"], model=value["model"],
            generation_mode=value["generation_mode"],
            parameters_json=parameters, references_json=references,
            idempotency_key="source-" + value["source_job_id"], status="confirmed",
            expires_at=timestamp, job_id=value["media_job_id"], output_role="candidate",
            traceparent=traceparent,
            created_at=timestamp, updated_at=timestamp))
        row = MediaJobRow(id=value["media_job_id"], owner_id=value["owner_id"],
            thread_id=value["thread_id"], turn_id="", draft_id=draft_id,
            source_job_id=value["source_job_id"], kind=value["kind"], model=value["model"],
            generation_mode=value["generation_mode"], provider=value["provider"], adapter=value["adapter"],
            execution_mode=value["execution_mode"], cancellation_mode=value["cancellation_mode"],
            status="created", phase="prepared", parameters_json=parameters,
            references_json=references, provider_job_id="",
            idempotency_key="source-" + value["source_job_id"], output_role="candidate",
            traceparent=traceparent,
            created_at=timestamp, updated_at=timestamp)
        db.add(row)
        db.add(MediaSourcePermitRow(dispatch_id=value["dispatch_id"], media_job_id=row.id,
            source_job_id=value["source_job_id"], source_generation=value["source_generation"],
            request_digest=value["request_digest"], command_digest=digest, state="AUTHORIZED",
            output_contracts_json=canonical_json(value["output_contracts"]),
            created_at=timestamp, updated_at=timestamp))
        await db.flush()
        return self._media_job_view(row)

    async def cancel_source_job(self, dispatch_id: str, source_job_id: str, source_generation: int):
        async with self.transaction() as db:
            await self._cancel_source_job(db, dispatch_id, source_job_id, source_generation)

    async def _cancel_source_job(self, db, dispatch_id, source_job_id, source_generation):
        permit = await db.get(MediaSourcePermitRow, dispatch_id, with_for_update=True)
        if (not permit or permit.source_job_id != source_job_id
                or permit.source_generation != source_generation):
            raise ValueError("media_source_command_conflict")
        permit.state = "CANCELLED"
        permit.updated_at = now()
        row = await db.get(MediaJobRow, permit.media_job_id, with_for_update=True)
        if row.status not in {"succeeded", "failed", "cancelled", "unknown"}:
            row.status = "cancel_requested"
            row.updated_at = now()

    async def _receive_source_job(self, db, envelope):
        value = envelope.payload
        if (envelope.source != "core" or envelope.target != "media"
                or envelope.command_id != value.get("dispatch_id")
                or envelope.aggregate_id != value.get("source_job_id")
                or envelope.owner_id != value.get("owner_id")
                or envelope.generation != value.get("source_generation")):
            raise ValueError("media_source_envelope_invalid")
        view = await self._accept_source_job(db, value, traceparent=envelope.traceparent)
        return {"media_job_id":view.id,"provider":view.provider,"adapter":view.adapter,
                "execution_mode":view.execution_mode}

    async def _receive_source_cancel(self, db, envelope):
        value = envelope.payload
        if (envelope.source != "core" or envelope.target != "media"
                or envelope.aggregate_id != value.get("source_job_id")
                or envelope.generation != value.get("source_generation")):
            raise ValueError("media_source_envelope_invalid")
        await self._cancel_source_job(db, value["dispatch_id"], value["source_job_id"],
                                      value["source_generation"])
        return {"cancelled":True}

    async def _receive_turn_draft(self, db, envelope):
        value = envelope.payload
        if (envelope.source != "core" or envelope.owner_id != value.get("owner_id")
                or envelope.generation != value.get("thread_generation")):
            raise ValueError("media_turn_envelope_invalid")
        existing = await db.scalar(select(MediaDraftRow).where(
            MediaDraftRow.thread_id == value["thread_id"],
            MediaDraftRow.idempotency_key == value["idempotency_key"]).with_for_update())
        request_digest = canonical_digest({key:value[key] for key in (
            "turn_id","kind","model","generation_mode","parameters","references","output_role")})
        if existing:
            existing_digest = canonical_digest({"turn_id":existing.turn_id,"kind":existing.kind,
                "model":existing.model,"generation_mode":existing.generation_mode,
                "parameters":json.loads(existing.parameters_json),
                "references":json.loads(existing.references_json),"output_role":existing.output_role})
            if existing.id != value["draft_id"] or existing_digest != request_digest:
                raise ValueError("media_draft_idempotency_conflict")
            return {"draft_id":existing.id}
        timestamp = now()
        batch = await db.get(MediaBatchRow, value["turn_id"], with_for_update=True)
        if batch and batch.status != "open":
            raise RuntimeError("media_batch_sealed")
        if not batch:
            db.add(MediaBatchRow(turn_id=value["turn_id"], thread_id=value["thread_id"],
                owner_id=value["owner_id"], status="open", created_at=timestamp, updated_at=timestamp))
        db.add(MediaDraftRow(id=value["draft_id"],owner_id=value["owner_id"],
            thread_id=value["thread_id"],turn_id=value["turn_id"],kind=value["kind"],
            model=value["model"],generation_mode=value["generation_mode"],
            parameters_json=canonical_json(value["parameters"]),
            references_json=canonical_json(value["references"]),
            idempotency_key=value["idempotency_key"],status="awaiting_confirmation",
            expires_at=datetime.fromisoformat(value["expires_at"]),job_id="",
            output_role=value["output_role"],traceparent=envelope.traceparent,
            created_at=timestamp,updated_at=timestamp))
        return {"draft_id":value["draft_id"]}

    async def _seal_turn_batch(self, db, turn_id):
        batch = await db.get(MediaBatchRow, turn_id, with_for_update=True)
        if not batch:
            raise RuntimeError("media_batch_empty")
        count = await db.scalar(select(func.count()).select_from(MediaDraftRow).where(
            MediaDraftRow.turn_id == turn_id))
        if not count:
            raise RuntimeError("media_batch_empty")
        if batch.status == "open":
            batch.status = "sealed"; batch.sealed_at = now(); batch.updated_at = now()
        elif batch.status not in {"sealed", "delivering", "continued"}:
            raise RuntimeError("media_batch_not_sealable")
        return {"turn_id":turn_id,"status":batch.status,"draft_count":count}

    async def _receive_turn_seal(self, db, envelope):
        value = envelope.payload
        if (envelope.source != "core" or envelope.owner_id != value.get("owner_id")
                or envelope.generation != value.get("thread_generation")):
            raise ValueError("media_turn_envelope_invalid")
        return await self._seal_turn_batch(db, value["turn_id"])

    async def _receive_turn_finished(self, db, envelope):
        value = envelope.payload
        if envelope.source != "core" or envelope.owner_id != value.get("owner_id"):
            raise ValueError("media_turn_envelope_invalid")
        batch = await db.get(MediaBatchRow, value["turn_id"], with_for_update=True)
        if batch:
            batch.origin_finished = True
            if batch.status == "open":
                batch.status = "sealed"; batch.sealed_at = now()
            batch.updated_at = now()
        return {"turn_id":value["turn_id"],"status":batch.status if batch else "empty"}

    async def media_batch_status(self, turn_id, owner_id):
        async with self.connections() as db:
            batch = await db.scalar(select(MediaBatchRow).where(
                MediaBatchRow.turn_id == turn_id, MediaBatchRow.owner_id == owner_id))
            if not batch:return None
            count = await db.scalar(select(func.count()).select_from(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id))
            return {"turn_id":turn_id,"status":batch.status,"draft_count":count}

    async def media_continuation_group(self, turn_id):
        terminal = {"succeeded", "failed", "cancelled", "unknown"}
        async with self.connections() as db:
            batch = await db.get(MediaBatchRow, turn_id)
            drafts = list((await db.scalars(select(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id).order_by(
                    MediaDraftRow.created_at, MediaDraftRow.id))).all())
            rows = list((await db.scalars(select(MediaJobRow).where(
                MediaJobRow.turn_id == turn_id).order_by(
                    MediaJobRow.created_at, MediaJobRow.id))).all())
            output_rows = list((await db.scalars(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id.in_([row.id for row in rows] or [""]),
                MediaJobOutputRow.role == "primary"))).all())
        jobs = {row.draft_id: row for row in rows}
        outputs = {row.job_id: row for row in output_rows}
        states = [("expired" if draft.status == "awaiting_confirmation"
            and utc_datetime(draft.expires_at) <= now() else draft.status) for draft in drafts]
        complete = all(state in {"cancelled", "expired"} or state == "confirmed"
            and draft.id in jobs and jobs[draft.id].status in terminal
            for draft, state in zip(drafts, states))
        ready = bool(batch and batch.origin_finished and batch.status in {"sealed", "delivering"}
                     and drafts and complete)
        group = []
        for draft, draft_state in zip(drafts, states):
            row = jobs.get(draft.id)
            output = outputs.get(row.id) if row else None
            group.append({
                "id": row.id if row else draft.id, "job_id": row.id if row else "",
                "draft_id": draft.id, "thread_id": draft.thread_id, "turn_id": draft.turn_id,
                "kind": draft.kind, "model": draft.model,
                "status": row.status if row else draft_state,
                "output_object_ref": row.output_object_ref if row else "",
                "output_mime": row.output_mime if row else "",
                "output_size": row.output_size if row else 0,
                "error_code": row.error_code if row else "",
                "error_detail": row.error_detail if row else "",
                "error_guidance": "", "output_role": draft.output_role,
                "project_path": self._media_project_path(row.id, row.output_mime)
                    if row and output and output.project_file_id
                    and row.output_role in {"workspace", "delivery"} else "",
            })
        return ready, group

    async def claim_media_batches(self, owner, limit=20):
        async with self.transaction() as db:
            rows = list((await db.scalars(select(MediaBatchRow).where(
                MediaBatchRow.origin_finished.is_(True),
                MediaBatchRow.status.in_(["sealed", "delivering"]),
                or_(MediaBatchRow.lease_expires_at.is_(None),
                    MediaBatchRow.lease_expires_at < now())).order_by(
                        MediaBatchRow.created_at).limit(limit).with_for_update())).all())
            result = []
            for row in rows:
                row.status = "delivering"; row.lease_owner = owner
                row.lease_expires_at = now() + timedelta(seconds=60)
                row.attempt_count += 1; row.updated_at = now()
                result.append({"turn_id":row.turn_id,"thread_id":row.thread_id,
                               "owner_id":row.owner_id})
            return result

    async def promotion_manifest(self, job_id, thread_id):
        async with self.connections() as db:
            row = await db.scalar(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id, MediaJobRow.source_job_id.is_(None)))
            if not row:raise KeyError("media_job_not_found")
            if row.status != "succeeded":raise RuntimeError("media_job_output_unavailable")
            outputs = list((await db.scalars(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id == row.id).order_by(MediaJobOutputRow.created_at))).all())
            if not outputs:raise RuntimeError("media_job_output_unavailable")
            return {"job_id":row.id,"thread_id":row.thread_id,"turn_id":row.turn_id,
                "outputs":[{"id":item.id,"role":item.role,"filename":item.filename,
                    "object_ref":item.object_ref,"sha256":item.sha256,"size":item.size,
                    "mime":item.mime,"project_path":self._media_output_project_path(
                        row.id,item.role,item.filename,item.mime)} for item in outputs]}

    async def _receive_promotion_ack(self, db, envelope):
        value=envelope.payload
        if envelope.source != "core" or envelope.aggregate_id != value.get("job_id"):
            raise ValueError("media_promotion_envelope_invalid")
        row=await db.get(MediaJobRow,value["job_id"],with_for_update=True)
        if not row or row.thread_id != value["thread_id"] or row.status != "succeeded":
            raise ValueError("media_promotion_scope_invalid")
        outputs={item.id:item for item in (await db.scalars(select(MediaJobOutputRow).where(
            MediaJobOutputRow.job_id == row.id).with_for_update())).all()}
        for item in value["outputs"]:
            output=outputs.get(item["id"])
            if (not output or output.sha256 != item["sha256"]
                    or output.object_ref != item["object_ref"]):
                raise ValueError("media_promotion_identity_conflict")
            output.project_file_id=item["project_file_id"]
        row.output_role=value["output_role"];row.updated_at=now()
        return {"job_id":row.id,"output_role":row.output_role,
                "project_path":value["project_path"]}

    async def claim_media_jobs(self, owner: str, limit: int = 4, *, adapter: str | None = None,
                               exclude_adapter: str | None = None,
                               exclude_adapters: set[str] | None = None,
                               has_provider_job: bool | None = None,
                               lease_seconds: int = 60) -> list[dict]:
        async with self.transaction() as db:
            permitted = select(MediaSourcePermitRow.dispatch_id).where(
                MediaSourcePermitRow.media_job_id == MediaJobRow.id,
                MediaSourcePermitRow.state.in_(["AUTHORIZED", "CONSUMED", "CANCELLED"])).exists()
            query = select(MediaJobRow).where(
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
                or_(MediaJobRow.lease_expires_at.is_(None), MediaJobRow.lease_expires_at < now()),
                or_(MediaJobRow.source_job_id.is_(None), permitted))
            if adapter is not None: query = query.where(MediaJobRow.adapter == adapter)
            if exclude_adapter is not None: query = query.where(MediaJobRow.adapter != exclude_adapter)
            if exclude_adapters: query = query.where(MediaJobRow.adapter.not_in(exclude_adapters))
            if has_provider_job is True: query = query.where(MediaJobRow.provider_job_id != "")
            elif has_provider_job is False: query = query.where(MediaJobRow.provider_job_id == "")
            rows = list((await db.scalars(query.order_by(MediaJobRow.created_at).limit(limit)
                .with_for_update(skip_locked=True))).all())
            expiry = now() + timedelta(seconds=lease_seconds)
            result = []
            for row in rows:
                permit = await db.scalar(select(MediaSourcePermitRow).where(
                    MediaSourcePermitRow.media_job_id == row.id)) if row.source_job_id else None
                if permit and permit.state == "CANCELLED": row.status = "cancel_requested"
                row.generation += 1; row.lease_owner = owner; row.lease_expires_at = expiry; row.updated_at = now()
                execution = await db.get(MediaExecutionRow, row.id)
                result.append({"id":row.id,"kind":row.kind,"model":row.model,"provider":row.provider,
                    "adapter":row.adapter,"status":row.status,"phase":row.phase,
                    "generation_mode":row.generation_mode or "","parameters":json.loads(row.parameters_json or "{}"),
                    "references":json.loads(row.references_json or "[]"),"provider_job_id":row.provider_job_id,
                    "thread_id":row.thread_id,"owner_id":row.owner_id,"turn_id":row.turn_id,
                    "output_role":row.output_role,"created_at":utc_datetime(row.created_at),
                    "traceparent":row.traceparent,
                    "generation":row.generation,"source_job_id":row.source_job_id,
                    "output_contracts":json.loads(permit.output_contracts_json) if permit else [],
                    "provider_result":json.loads(execution.provider_result_json) if execution else {},
                    "staged_outputs":json.loads(execution.staged_outputs_json) if execution else [],
                    "provider_started_at":utc_datetime(row.provider_started_at) if row.provider_started_at else None})
            return result

    async def _locked_job_and_permit(self, db, job_id, *, owner="", generation=0):
        # Source cancellation uses this same permit -> job order. Locking a Job
        # first and then its permit deadlocks with a simultaneous cancel command.
        permit = await db.scalar(select(MediaSourcePermitRow).where(
            MediaSourcePermitRow.media_job_id == job_id).with_for_update())
        query = select(MediaJobRow).where(MediaJobRow.id == job_id)
        if owner: query = query.where(MediaJobRow.lease_owner == owner)
        if generation: query = query.where(MediaJobRow.generation == generation)
        row = await db.scalar(query.with_for_update())
        if not row: raise RuntimeError("stale_media_job_lease")
        if row.source_job_id and not permit:
            raise RuntimeError("media_source_permit_missing")
        return row, permit

    async def media_cancel_requested(self, job_id: str, owner: str, generation: int,
                                     *, begin_submission: bool = False) -> bool:
        async with self.transaction() as db:
            row, permit = await self._locked_job_and_permit(
                db, job_id, owner=owner, generation=generation)
            cancelled = row.status == "cancel_requested" or bool(permit and permit.state == "CANCELLED")
            if cancelled: row.status = "cancel_requested"
            if begin_submission and not cancelled:
                if permit and permit.state == "CONSUMED" and row.provider_started_at and not row.provider_job_id:
                    raise RuntimeError("media_submission_outcome_unknown")
                if permit: permit.state = "CONSUMED"; permit.updated_at = now()
                row.status = "running"; row.phase = "requesting"; row.updated_at = now()
                row.provider_started_at = row.provider_started_at or now()
            return cancelled

    async def record_media_submission_rejection(self, job_id: str, owner: str, generation: int):
        async with self.transaction() as db:
            row, permit = await self._locked_job_and_permit(
                db, job_id, owner=owner, generation=generation)
            if row.provider_job_id: raise RuntimeError("media_provider_already_accepted")
            if permit and permit.state == "CONSUMED": permit.state = "AUTHORIZED"; permit.updated_at = now()
            row.provider_started_at = None; row.phase = "prepared"; row.updated_at = now()

    async def update_media_job(self, job_id: str, owner: str, generation: int, *, status: str,
                               phase: str = "", progress: int | None = None, provider_job_id: str = "",
                               output_object_ref: str = "", output_mime: str = "", output_size: int = 0,
                               error_code: str = "", error_detail: str = "") -> bool:
        async with self.transaction() as db:
            row = await db.scalar(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.lease_owner == owner, MediaJobRow.generation == generation).with_for_update())
            if not row: raise RuntimeError("stale_media_job_lease")
            if row.status == "cancel_requested":
                status = "cancelled" if status in {"succeeded", "failed"} else (
                    status if status in {"cancelled", "unknown"} else "cancel_requested")
            row.status = status; row.phase = phase or row.phase; row.progress = progress
            if provider_job_id: row.provider_job_id = provider_job_id
            if provider_job_id and row.provider_started_at is None: row.provider_started_at = now()
            if output_object_ref:
                row.output_object_ref, row.output_mime, row.output_size = output_object_ref, output_mime, output_size
            row.error_code, row.error_detail, row.updated_at = error_code, error_detail[:2000], now()
            if status in {"succeeded", "failed", "cancelled", "unknown"}:
                row.finished_at = row.finished_at or row.updated_at
                row.lease_owner, row.lease_expires_at = "", None
                await self._terminal_outbox(db, row)
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
            return True

    async def _terminal_outbox(self, db, row):
        existing = await db.scalar(select(MediaOutboxRow.id).where(
            MediaOutboxRow.job_id == row.id, MediaOutboxRow.event_type == "media.job.terminal"))
        if not existing:
            db.add(MediaOutboxRow(id=identifier(), job_id=row.id, event_type="media.job.terminal",
                status="pending", created_at=now(), updated_at=now()))

    async def complete_media_job(self, job_id: str, owner: str, *, provider_job_id: str,
                                 outputs: list[dict], credits_consumed: float | None = None,
                                 generation: int = 0) -> bool:
        if not outputs or sum(value.get("role") == "primary" for value in outputs) != 1:
            raise ValueError("media_outputs_require_one_primary")
        async with self.transaction() as db:
            row, permit = await self._locked_job_and_permit(
                db, job_id, owner=owner, generation=generation)
            if row.status == "cancel_requested" or permit and permit.state == "CANCELLED":
                raise MediaCancellationRequested("media_cancellation_requested")
            if await db.scalar(select(MediaJobOutputRow.id).where(MediaJobOutputRow.job_id == row.id)):
                raise RuntimeError("media_outputs_already_committed")
            timestamp = now(); seen_roles = set(); seen_names = set()
            for value in outputs:
                role = str(value.get("role") or ""); filename = PurePosixPath(str(value.get("filename") or "")).name
                object_ref = str(value.get("object_ref") or ""); digest = object_ref.removeprefix("sha256:")
                mime = str(value.get("mime") or "application/octet-stream"); size = int(value.get("size") or 0)
                valid_role = role in {"primary", "preview", "auxiliary"} or bool(
                    row.source_job_id and re.fullmatch(r"output_[0-9]{4}", role))
                if (not valid_role or role in seen_roles or not filename or filename in seen_names
                        or size <= 0 or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
                    raise ValueError("invalid_media_output")
                seen_roles.add(role); seen_names.add(filename)
                db.add(MediaJobOutputRow(id=identifier(), job_id=row.id, role=role, filename=filename,
                    object_ref=object_ref, sha256=digest, size=size, mime=mime,
                    artifact_id="", project_file_id="", created_at=timestamp))
            primary = next(value for value in outputs if value["role"] == "primary")
            row.provider_job_id = provider_job_id or row.provider_job_id
            row.output_object_ref, row.output_mime, row.output_size = (
                primary["object_ref"], primary["mime"], int(primary["size"]))
            row.credits_consumed = "" if credits_consumed is None else str(credits_consumed)
            row.status, row.phase, row.progress = "succeeded", "committed", 100
            row.error_code = row.error_detail = ""; row.updated_at = timestamp
            row.finished_at = row.finished_at or timestamp; row.lease_owner = ""; row.lease_expires_at = None
            await self._terminal_outbox(db, row)
            return True
