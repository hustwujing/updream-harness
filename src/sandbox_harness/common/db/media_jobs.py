"""Media Jobs persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import re
from datetime import (timedelta)
from pathlib import (PurePosixPath)
from sqlalchemy import (case, or_, func, select, update)
from sandbox_harness.common.contracts.domain import (ItemType, TERMINAL, identifier, now, MediaDraftView, MediaJobView, MediaJobOutputView)
from sandbox_harness.common.contracts.media_errors import (media_error_guidance)
from sandbox_harness.common.contracts.media_provider import (MediaCancellationRequested)
from sandbox_harness.common.contracts.job_contracts import (JobState, JobRequest, TERMINAL_JOB_STATES)
from .models import (ArtifactRow, ItemRow, JobAttemptRow, JobLeaseRow, JobRow, MediaBatchRow, MediaDraftRow, MediaExecutionRow, MediaJobOutputRow, MediaJobRow, MediaOutboxRow, ProjectFileRow, ThreadRow, TurnRow, canonical_digest, canonical_json, utc_datetime)


class MediaJobsTransactions:
    async def enqueue_job_media_execution(self, job_id: str, generation: int, *,
                                           request_digest: str, provider_spec,
                                           parameters: dict,
                                           provider_job_id: str = "") -> MediaJobView:
        """Atomically hand an authorized, leased Job to the media executor.

        The source Job remains the publication/continuation authority. An exact
        legacy provider receipt may be imported for observation, never submission.
        """
        async with self.job_lock("media-source:"+job_id):
            async with self.connections.begin() as db:
                source=await db.get(JobRow,job_id,with_for_update=True)
                lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
                if (not source or not lease or source.generation!=generation
                        or lease.generation!=generation or utc_datetime(lease.expires_at)<=now()
                        or source.request_digest!=request_digest):
                    raise RuntimeError("stale_job_generation")
                existing=(await db.execute(select(MediaJobRow).where(
                    MediaJobRow.source_job_id==job_id))).scalar_one_or_none()
                if existing:
                    if provider_job_id and existing.provider_job_id!=provider_job_id:
                        raise RuntimeError("media_provider_identity_conflict")
                    return self._media_job_view(existing)
                request=JobRequest.model_validate_json(source.request_json)
                if not provider_job_id:
                    attempt=await db.get(JobAttemptRow,lease.attempt_id)
                    if source.cancel_requested:raise RuntimeError("job_cancel_requested")
                    if (JobState(source.state) in TERMINAL_JOB_STATES
                            or not attempt or attempt.outcome!="SUBMITTING"):
                        raise RuntimeError("job_submission_intent_conflict")
                references=[]
                for binding in request.input_artifacts:
                    artifact=(await db.execute(select(ArtifactRow).join(TurnRow,
                        TurnRow.id==ArtifactRow.turn_id).join(ThreadRow,
                        ThreadRow.id==TurnRow.thread_id).where(ArtifactRow.id==binding.artifact_id,
                        TurnRow.thread_id==source.thread_id,ThreadRow.owner_id==source.owner_id)
                        )).scalar_one_or_none()
                    if not artifact:raise RuntimeError("job_input_artifact_outside_scope")
                    references.append({"artifact_id":artifact.id,"role":binding.artifact_role,
                        "filename":PurePosixPath(artifact.path).name,"object_ref":artifact.object_ref,
                        "sha256":artifact.sha256,"size":artifact.size,"mime":artifact.mime})
                timestamp=now();identity=identifier();draft_id=identifier()
                key="job-"+source.id
                generation_mode=str(request.parameters.get("generation_mode") or "")
                encoded=canonical_json(parameters);frozen=canonical_json(references)
                db.add(MediaDraftRow(id=draft_id,owner_id=source.owner_id,
                    thread_id=source.thread_id,turn_id="",kind=provider_spec.kind,
                    model=provider_spec.model,generation_mode=generation_mode,parameters_json=encoded,
                    references_json="[]",idempotency_key=key,status="confirmed",job_id=identity,
                    output_role="candidate",expires_at=timestamp,traceparent="",
                    created_at=timestamp,updated_at=timestamp))
                row=MediaJobRow(id=identity,owner_id=source.owner_id,thread_id=source.thread_id,
                    turn_id="",draft_id=draft_id,source_job_id=source.id,kind=provider_spec.kind,
                    model=provider_spec.model,generation_mode=generation_mode,provider=provider_spec.provider,
                    adapter=provider_spec.adapter,execution_mode=provider_spec.execution,
                    cancellation_mode=provider_spec.cancellation,
                    status=("cancel_requested" if source.cancel_requested else
                        "running" if provider_job_id else "created"),
                    phase="provider_polling" if provider_job_id else "prepared",
                    parameters_json=encoded,references_json=frozen,provider_job_id=provider_job_id,
                    provider_started_at=timestamp if provider_job_id else None,
                    idempotency_key=key,output_role="candidate",traceparent="",
                    created_at=timestamp,updated_at=timestamp)
                db.add(row);await db.flush()
                return self._media_job_view(row)


    async def media_execution_for_job(self, job_id: str, owner_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row=(await db.execute(select(MediaJobRow).where(MediaJobRow.source_job_id==job_id,
                MediaJobRow.owner_id==owner_id))).scalar_one_or_none()
            return self._media_job_view(row) if row else None


    async def media_job_outputs_for_source(self, media_job_id: str, job_id: str,
                                          owner_id: str) -> list[dict]:
        async with self.connections() as db:
            rows=(await db.execute(select(MediaJobOutputRow).join(MediaJobRow,
                MediaJobRow.id==MediaJobOutputRow.job_id).where(MediaJobRow.id==media_job_id,
                MediaJobRow.source_job_id==job_id,MediaJobRow.owner_id==owner_id,
                MediaJobRow.status=="succeeded").order_by(
                    case((MediaJobOutputRow.role=="primary",0),else_=1),
                    MediaJobOutputRow.role))).scalars().all()
            return [{key:getattr(row,key) for key in
                ("role","filename","object_ref","sha256","size","mime")} for row in rows]


    async def confirm_media_draft(self, draft_id: str, owner_id: str, *, provider_spec) -> MediaJobView:
        async with self.connections.begin() as db:
            draft = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not draft: raise KeyError("media_draft_not_found")
            if draft.job_id:
                job = await db.get(MediaJobRow, draft.job_id)
                if job: return self._media_job_view(job)
            if draft.status != "awaiting_confirmation" or utc_datetime(draft.expires_at) <= now():
                raise RuntimeError("media_draft_not_confirmable")
            job = MediaJobRow(id=identifier(), owner_id=owner_id, thread_id=draft.thread_id, turn_id=draft.turn_id, draft_id=draft.id,
                kind=draft.kind, model=draft.model, generation_mode=draft.generation_mode,
                provider=provider_spec.provider, adapter=provider_spec.adapter,
                execution_mode=provider_spec.execution, cancellation_mode=provider_spec.cancellation,
                status="created", phase="prepared", parameters_json=draft.parameters_json,
                references_json=draft.references_json,
                idempotency_key=draft.idempotency_key, output_role=draft.output_role,
                traceparent=draft.traceparent,created_at=now(), updated_at=now())
            db.add(job); draft.status = "confirmed"; draft.job_id = job.id; draft.updated_at = now()
            if draft.turn_id:
                await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == draft.turn_id,
                    MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
        return self._media_job_view(job)


    async def confirm_media_batch(self,turn_id:str,owner_id:str,*,provider_specs:dict[str,object])->list[MediaJobView]:
        async with self.connections.begin() as db:
            batch=await db.get(MediaBatchRow,turn_id,with_for_update=True)
            if not batch or batch.owner_id!=owner_id:
                raise KeyError("media_batch_not_found")
            if batch.status not in {"sealed","delivering","continued"}:
                raise RuntimeError("media_batch_not_confirmable")
            drafts=list((await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.turn_id==turn_id,MediaDraftRow.owner_id==owner_id)
                .order_by(MediaDraftRow.created_at,MediaDraftRow.id).with_for_update())).scalars().all())
            if not drafts or any(draft.status not in {"awaiting_confirmation","confirmed"}
                    or draft.status=="awaiting_confirmation" and utc_datetime(draft.expires_at)<=now()
                    for draft in drafts):
                raise RuntimeError("media_batch_not_confirmable")
            pending=[draft for draft in drafts if not draft.job_id]
            if any(draft.id not in provider_specs for draft in pending):
                raise RuntimeError("media_batch_provider_spec_missing")
            jobs=[];timestamp=now()
            for draft in drafts:
                job=await db.get(MediaJobRow,draft.job_id) if draft.job_id else None
                if not job:
                    spec=provider_specs[draft.id]
                    if spec.kind!=draft.kind or spec.model!=draft.model:
                        raise RuntimeError("media_batch_provider_spec_mismatch")
                    job=MediaJobRow(id=identifier(),owner_id=owner_id,thread_id=draft.thread_id,
                        turn_id=draft.turn_id,draft_id=draft.id,kind=draft.kind,model=draft.model,
                        generation_mode=draft.generation_mode,provider=spec.provider,adapter=spec.adapter,
                        execution_mode=spec.execution,cancellation_mode=spec.cancellation,
                        status="created",phase="prepared",parameters_json=draft.parameters_json,
                        references_json=draft.references_json,idempotency_key=draft.idempotency_key,
                        output_role=draft.output_role,traceparent=draft.traceparent,
                        created_at=timestamp,updated_at=timestamp)
                    db.add(job);draft.status="confirmed";draft.job_id=job.id;draft.updated_at=timestamp
                jobs.append(job)
            batch.lease_expires_at=None;batch.updated_at=timestamp
        return [self._media_job_view(job) for job in jobs]


    async def cancel_media_batch(self,turn_id:str,owner_id:str)->list[MediaDraftView]:
        async with self.connections.begin() as db:
            batch=await db.get(MediaBatchRow,turn_id,with_for_update=True)
            if not batch or batch.owner_id!=owner_id:
                raise KeyError("media_batch_not_found")
            drafts=list((await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.turn_id==turn_id,MediaDraftRow.owner_id==owner_id)
                .order_by(MediaDraftRow.created_at,MediaDraftRow.id).with_for_update())).scalars().all())
            if not drafts or any(draft.status not in {"awaiting_confirmation","cancelled"} for draft in drafts):
                raise RuntimeError("media_batch_not_cancellable")
            timestamp=now()
            for draft in drafts:
                if draft.status=="awaiting_confirmation":
                    draft.status="cancelled";draft.updated_at=timestamp
            batch.lease_expires_at=None;batch.updated_at=timestamp
        return [self._media_draft_view(draft) for draft in drafts]


    async def get_media_job(self, job_id: str, owner_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
            outputs = await self._media_job_outputs(db, row.id) if row else []
        return self._media_job_view(row, artifact_ids, outputs) if row else None


    async def get_media_job_for_thread(self, job_id: str, thread_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id,MediaJobRow.source_job_id.is_(None)))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
            outputs = await self._media_job_outputs(db, row.id) if row else []
        return self._media_job_view(row, artifact_ids, outputs) if row else None


    async def media_job_output_for_thread(self, job_id: str, thread_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id,MediaJobRow.source_job_id.is_(None)))).scalar_one_or_none()
            if not row or row.status != "succeeded" or not row.output_object_ref:
                return None
            primary=(await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id==row.id,MediaJobOutputRow.role=="primary")
                .order_by(MediaJobOutputRow.created_at,MediaJobOutputRow.id).limit(1))).scalar_one_or_none()
            if primary:
                return {"object_ref":primary.object_ref,"mime":primary.mime,
                    "size":primary.size,"filename":primary.filename}
            return {"object_ref": row.output_object_ref, "mime": row.output_mime,
                "size": row.output_size, "filename": self._media_project_path(row.id, row.output_mime).rsplit("/", 1)[-1]}


    async def promote_media_job_for_thread(self, job_id: str, thread_id: str, role: str) -> dict:
        if role not in {"workspace", "delivery"}:
            raise ValueError("media_output_role_invalid")
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id,MediaJobRow.source_job_id.is_(None))
                .with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_job_not_found")
            if row.status != "succeeded" or not row.output_object_ref:
                raise RuntimeError("media_job_output_unavailable")
            outputs = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id == row.id).order_by(MediaJobOutputRow.created_at))).scalars().all()
            if outputs:
                primary_path = ""
                for output in outputs:
                    path = f"artifacts/3d/{row.id}/{output.filename}"
                    value, _ = await self._register_project_file_locked(db, row.thread_id, path=path,
                        source_type="artifact", source_turn_id=row.turn_id, sha256=output.sha256,
                        size=output.size, mime=output.mime, object_ref=output.object_ref)
                    output.project_file_id = value.id
                    if output.role == "primary":
                        primary_path = value.path
                if not primary_path:
                    raise RuntimeError("media_job_output_invalid")
            else:
                digest = row.output_object_ref.removeprefix("sha256:")
                if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                    raise RuntimeError("media_job_output_invalid")
                path = self._media_project_path(row.id, row.output_mime)
                value, _ = await self._register_project_file_locked(db, row.thread_id, path=path,
                    source_type="artifact", source_turn_id=row.turn_id, sha256=digest,
                    size=row.output_size, mime=row.output_mime or "application/octet-stream",
                    object_ref=row.output_object_ref)
                primary_path = value.path
            row.output_role = role; row.updated_at = now()
            return {"job_id": row.id, "output_role": role, "project_path": primary_path}


    async def list_media_jobs(self, thread_id: str, owner_id: str) -> list[MediaJobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(MediaJobRow).where(MediaJobRow.thread_id == thread_id,
                MediaJobRow.owner_id == owner_id,MediaJobRow.source_job_id.is_(None))
                .order_by(MediaJobRow.created_at.desc()))).scalars().all()
            paths = {f"artifacts/{row.id}": row.id for row in rows if row.turn_id}
            artifact_ids: dict[str, list[str]] = {row.id: [] for row in rows}
            if paths:
                artifacts = (await db.execute(select(ArtifactRow.id, ArtifactRow.path).where(
                    ArtifactRow.path.in_(paths),
                ).order_by(ArtifactRow.created_at))).all()
                for artifact_id, path in artifacts:
                    artifact_ids[paths[path]].append(artifact_id)
            output_rows = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id.in_([row.id for row in rows]),
            ).order_by(MediaJobOutputRow.created_at, MediaJobOutputRow.id))).scalars().all() if rows else []
            outputs: dict[str, list[MediaJobOutputView]] = {row.id: [] for row in rows}
            for output in output_rows:
                outputs[output.job_id].append(MediaJobOutputView(id=output.id, role=output.role,
                    filename=output.filename, mime=output.mime, size=output.size,
                    artifact_id=output.artifact_id, project_file_id=output.project_file_id))
                if output.artifact_id:
                    artifact_ids[output.job_id].append(output.artifact_id)
        return [self._media_job_view(row, artifact_ids[row.id], outputs[row.id]) for row in rows]


    async def media_continuation_group(self, turn_id: str) -> tuple[bool, list[dict]]:
        terminal = {"succeeded", "failed", "cancelled", "unknown"}
        async with self.connections() as db:
            origin_state = (await db.execute(select(TurnRow.state).where(
                TurnRow.id == turn_id,
            ))).scalar_one_or_none()
            batch_status = (await db.execute(select(MediaBatchRow.status).where(
                MediaBatchRow.turn_id == turn_id,
            ))).scalar_one_or_none()
            drafts = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id,
            ).order_by(MediaDraftRow.created_at, MediaDraftRow.id))).scalars().all()
            rows = (await db.execute(select(MediaJobRow).where(
                MediaJobRow.turn_id == turn_id,
            ).order_by(MediaJobRow.created_at, MediaJobRow.id))).scalars().all()
            job_ids = [row.id for row in rows]
            primary_paths = {}
            if job_ids:
                values = (await db.execute(select(MediaJobOutputRow.job_id, ProjectFileRow.path)
                    .join(ProjectFileRow, ProjectFileRow.id == MediaJobOutputRow.project_file_id)
                    .where(MediaJobOutputRow.job_id.in_(job_ids),
                        MediaJobOutputRow.role == "primary"))).all()
                primary_paths = dict(values)
        origin_finished = origin_state in {state.value for state in TERMINAL}
        jobs = {row.draft_id: row for row in rows}
        states = [("expired" if draft.status == "awaiting_confirmation"
            and utc_datetime(draft.expires_at) <= now() else draft.status) for draft in drafts]
        complete = all(state in {"cancelled", "expired"} or state == "confirmed"
            and draft.id in jobs and jobs[draft.id].status in terminal
            for draft, state in zip(drafts, states))
        ready = (origin_finished and batch_status in {"sealed", "delivering"}
            and bool(drafts) and complete)
        group = []
        for draft, draft_state in zip(drafts, states):
            row = jobs.get(draft.id)
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
                "error_guidance": media_error_guidance(row.error_code) if row and row.error_code else "",
                "output_role": draft.output_role,
                "project_path": (primary_paths.get(row.id) or self._media_project_path(row.id, row.output_mime))
                    if row and row.output_object_ref and row.output_role in {"workspace", "delivery"} else "",
            })
        return ready, group


    async def claim_media_batches(self, owner: str, limit: int = 20) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(MediaBatchRow).join(ThreadRow,
                ThreadRow.id==MediaBatchRow.thread_id).where(ThreadRow.archived_at.is_(None),
                MediaBatchRow.status.in_(["sealed", "delivering"]),
                or_(MediaBatchRow.lease_expires_at.is_(None), MediaBatchRow.lease_expires_at < now()),
            ).order_by(MediaBatchRow.created_at).limit(limit).with_for_update())).scalars().all()
            result = []
            for row in rows:
                row.status = "delivering"; row.lease_owner = owner
                row.lease_expires_at = now() + timedelta(seconds=60)
                row.attempt_count += 1; row.updated_at = now()
                result.append({"turn_id": row.turn_id, "thread_id": row.thread_id,
                    "owner_id": row.owner_id})
            return result


    async def finish_media_batch(self, turn_id: str, owner: str, *, delivered: bool,
                                 error: str = "", permanent: bool = False) -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaBatchRow).where(MediaBatchRow.turn_id == turn_id,
                MediaBatchRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row:
                return
            row.status = "continued" if delivered else "blocked" if permanent else "sealed"
            row.continued_at = now() if delivered else None
            row.last_error = error[:2000]; row.lease_owner = ""
            row.lease_expires_at = None if delivered or permanent else now() + timedelta(seconds=15)
            row.updated_at = now()
            if delivered:
                job_ids = select(MediaJobRow.id).where(MediaJobRow.turn_id == turn_id)
                await db.execute(update(MediaOutboxRow).where(MediaOutboxRow.job_id.in_(job_ids)).values(
                    status="delivered", lease_owner="", lease_expires_at=None, updated_at=now()))


    async def request_media_cancel(self, job_id: str, owner_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_job_not_found")
            if not (row.execution_mode == "async" and row.cancellation_mode == "supported"):
                raise RuntimeError("media_cancel_not_supported")
            if row.status in {"succeeded", "failed", "cancelled"}:
                return self._media_job_view(row)
            row.status = "cancel_requested"; row.updated_at = now()
        return self._media_job_view(row)


    async def request_media_cancel_for_thread(self, job_id: str, thread_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_job_not_found")
            if not (row.execution_mode == "async" and row.cancellation_mode == "supported"):
                raise RuntimeError("media_cancel_not_supported")
            if row.status in {"succeeded", "failed", "cancelled"}:
                return self._media_job_view(row)
            row.status = "cancel_requested"; row.updated_at = now()
        return self._media_job_view(row)


    async def request_media_refresh(self, job_id: str, owner_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_job_not_found")
            recoverable_audio = (row.adapter == "minimax_t2a" and bool(row.provider_job_id)
                and (row.status == "unknown" or (row.status == "failed"
                    and row.error_detail == "audio_timeout")))
            if recoverable_audio:
                row.status = "running"; row.phase = "provider_polling"; row.progress = None
                row.error_code = ""; row.error_detail = ""; row.finished_at = None
                row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            elif row.status in {"created", "submitted", "running", "cancel_requested"}:
                if not row.lease_owner or not row.lease_expires_at or utc_datetime(row.lease_expires_at) <= now():
                    row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            return self._media_job_view(row)


    async def claim_media_jobs(self, owner: str, limit: int = 4, *, adapter: str | None = None,
                               exclude_adapter: str | None = None,
                               exclude_adapters: set[str] | None = None,
                               has_provider_job: bool | None = None,
                               lease_seconds: int = 60) -> list[dict]:
        async with self.connections.begin() as db:
            query = select(MediaJobRow).where(
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
                or_(MediaJobRow.lease_expires_at.is_(None), MediaJobRow.lease_expires_at < now())
            )
            if adapter is not None:
                query = query.where(MediaJobRow.adapter == adapter)
            if exclude_adapter is not None:
                query = query.where(MediaJobRow.adapter != exclude_adapter)
            if exclude_adapters:
                query = query.where(MediaJobRow.adapter.not_in(exclude_adapters))
            if has_provider_job is True:
                query = query.where(MediaJobRow.provider_job_id != "")
            elif has_provider_job is False:
                query = query.where(MediaJobRow.provider_job_id == "")
            rows = (await db.execute(query.order_by(MediaJobRow.created_at.asc()).limit(limit)
                .with_for_update())).scalars().all()
            expiry = now() + timedelta(seconds=lease_seconds)
            result = []
            for row in rows:
                source=await db.get(JobRow,row.source_job_id) if row.source_job_id else None
                if source and (source.cancel_requested or JobState(source.state) in TERMINAL_JOB_STATES):
                    row.status="cancel_requested"
                row.generation+=1
                row.lease_owner = owner; row.lease_expires_at = expiry; row.updated_at = now()
                execution=await db.get(MediaExecutionRow,row.id)
                result.append({"id": row.id, "kind": row.kind, "model": row.model, "provider": row.provider,
                    "adapter": row.adapter, "status": row.status, "phase": row.phase,
                    "generation_mode": row.generation_mode or "", "parameters": json.loads(row.parameters_json or "{}"),
                    "references": json.loads(row.references_json or "[]"), "provider_job_id": row.provider_job_id,
                    "thread_id": row.thread_id, "owner_id": row.owner_id, "turn_id": row.turn_id,
                    "output_role":row.output_role, "created_at":utc_datetime(row.created_at),
                    "generation":row.generation,"source_job_id":row.source_job_id,
                    "output_contracts":(json.loads(source.request_json).get("expected_outputs",[])
                        if source else []),
                    "provider_result":json.loads(execution.provider_result_json) if execution else {},
                    "staged_outputs":json.loads(execution.staged_outputs_json) if execution else [],
                    "provider_started_at":(utc_datetime(row.provider_started_at)
                        if row.provider_started_at else None)})
        return result


    async def _media_source_cancelled_locked(self, db, row) -> bool:
        source=await db.get(JobRow,row.source_job_id,with_for_update=True) if row.source_job_id else None
        cancelled=(row.status=="cancel_requested" or bool(source and
            (source.cancel_requested or JobState(source.state) in TERMINAL_JOB_STATES)))
        if cancelled:row.status="cancel_requested"
        return cancelled


    async def media_cancel_requested(self, job_id: str, owner: str, generation: int,
                                     *, begin_submission: bool = False) -> bool:
        async with self.connections.begin() as db:
            row=(await db.execute(select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_media_job_lease")
            cancelled=await self._media_source_cancelled_locked(db,row)
            if begin_submission and not cancelled:
                row.status="running";row.phase="requesting";row.updated_at=now()
                row.provider_started_at=row.provider_started_at or now()
            return cancelled


    async def record_media_submission_rejection(self,job_id:str,owner:str,generation:int)->None:
        """Clear ambiguity only after an authoritative rejection before acceptance."""
        async with self.connections.begin() as db:
            row=(await db.execute(select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_media_job_lease")
            if row.provider_job_id:raise RuntimeError("media_provider_already_accepted")
            row.provider_started_at=None;row.phase="prepared";row.updated_at=now()


    async def renew_media_job_lease(self,job_id:str,owner:str,generation:int,
                                    lease_seconds:int=60)->bool:
        async with self.connections.begin() as db:
            result=await db.execute(update(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation).values(
                    lease_expires_at=now()+timedelta(seconds=lease_seconds),updated_at=now()))
            return bool(result.rowcount)


    async def persist_media_provider_result(self,job_id:str,owner:str,generation:int,
                                            value:dict)->dict:
        encoded=canonical_json(value)
        async with self.connections.begin() as db:
            job=(await db.execute(select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            if not job:raise RuntimeError("stale_media_job_lease")
            row=await db.get(MediaExecutionRow,job_id,with_for_update=True)
            if row and row.provider_result_json not in {"{}",encoded}:
                raise RuntimeError("media_provider_result_conflict")
            if not row:
                row=MediaExecutionRow(job_id=job_id,provider_result_json=encoded,
                    staged_outputs_json="[]",created_at=now(),updated_at=now());db.add(row)
            else:row.provider_result_json=encoded;row.updated_at=now()
            job.phase="response_received";job.updated_at=now()
            records=[("provider-operation:"+job.id,"provider_operations",1,"operation")]
            credits=value.get("credits_consumed")
            if isinstance(credits,(int,float)) and not isinstance(credits,bool) and credits>=0:
                records.append(("provider-credits:"+job.id,"provider_credits",
                    round(float(credits)*1_000_000),"microcredit"))
            for key,kind,amount,unit in records if hasattr(self,"handoff") else ():
                payload={"idempotency_key":key,"owner_id":job.owner_id,
                    "thread_id":job.thread_id,"turn_id":job.turn_id or "","job_id":job.id,
                    "kind":kind,"amount":amount,"unit":unit,"source":job.provider}
                await self.handoff.enqueue(db,command_id=canonical_digest(["usage",key]),
                    target="core",kind="usage.record",aggregate_id=job.id,
                    owner_id=job.owner_id,generation=job.generation,payload=payload)
            return value


    async def persist_media_staged_output(self,job_id:str,owner:str,generation:int,
                                          value:dict)->list[dict]:
        async with self.connections.begin() as db:
            job=(await db.execute(select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            if not job:raise RuntimeError("stale_media_job_lease")
            row=await db.get(MediaExecutionRow,job_id,with_for_update=True)
            if not row:raise RuntimeError("media_provider_result_missing")
            values=json.loads(row.staged_outputs_json);by_role={item["role"]:item for item in values}
            existing=by_role.get(value["role"])
            if existing and existing!=value:raise RuntimeError("media_staged_output_conflict")
            if not existing:values.append(value)
            row.staged_outputs_json=canonical_json(values);row.updated_at=now()
            job.phase="object_staged";job.updated_at=now()
            return values


    async def count_media_provider_jobs(self, adapter: str) -> int:
        async with self.connections() as db:
            return int((await db.execute(select(func.count()).select_from(MediaJobRow).where(
                MediaJobRow.adapter == adapter, MediaJobRow.provider_job_id != "",
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
            ))).scalar_one())


    async def release_media_job_lease(self, job_id: str, owner: str, generation: int, *, phase: str,
                                      error_code: str = "", error_detail: str = "",
                                      retry_after_seconds: int = 0) -> bool:
        async with self.connections.begin() as db:
            query=select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
            row=(await db.execute(query.with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_media_job_lease")
            if row.status!="cancel_requested":row.status="running"
            row.phase = phase; row.error_code = error_code
            row.error_detail = error_detail[:2000]; row.lease_owner = ""
            row.lease_expires_at = (now() + timedelta(seconds=retry_after_seconds)
                if retry_after_seconds > 0 else None); row.updated_at = now()
            return True


    async def resolve_tripo_input_job(self, job_id: str, input_job_id: str) -> str:
        async with self.connections() as db:
            current = await db.get(MediaJobRow, job_id)
            source = await db.get(MediaJobRow, input_job_id)
            if (not current or not source or source.owner_id != current.owner_id
                    or source.thread_id != current.thread_id or source.adapter != "tripo_v3"
                    or source.kind != "model3d" or source.status != "succeeded"
                    or not source.provider_job_id):
                raise ValueError("tripo_input_job_unavailable")
            return source.provider_job_id


    async def release_media_job_leases(self, owner: str) -> int:
        async with self.connections.begin() as db:
            result = await db.execute(update(MediaJobRow).where(
                MediaJobRow.lease_owner == owner,
                MediaJobRow.status.not_in(["succeeded", "failed", "cancelled", "unknown"]),
            ).values(lease_owner="", lease_expires_at=None, updated_at=now()))
            return result.rowcount or 0


    async def update_media_job(self, job_id: str, owner: str, generation: int, *,
                               status: str, phase: str = "",
                               progress: int | None = None, provider_job_id: str = "",
                               output_object_ref: str = "", output_mime: str = "", output_size: int = 0,
                               error_code: str = "", error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            query=select(MediaJobRow).where(MediaJobRow.id==job_id,
                MediaJobRow.lease_owner==owner,MediaJobRow.generation==generation)
            row=(await db.execute(query.with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_media_job_lease")
            if row.status=="cancel_requested":
                status=("cancelled" if status in {"succeeded","failed"} else status
                    if status in {"cancelled","unknown"} else "cancel_requested")
            row.status=status
            row.phase = phase or row.phase; row.progress = progress
            if provider_job_id: row.provider_job_id = provider_job_id
            if provider_job_id and row.provider_started_at is None: row.provider_started_at = now()
            if output_object_ref: row.output_object_ref = output_object_ref; row.output_mime = output_mime; row.output_size = output_size
            row.error_code = error_code; row.error_detail = error_detail; row.updated_at = now()
            if status in {"succeeded", "failed", "cancelled", "unknown"}:
                if row.finished_at is None: row.finished_at = row.updated_at
                row.lease_owner = ""; row.lease_expires_at = None
                if row.turn_id:
                    await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                        MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
                if status == "succeeded" and output_object_ref and row.turn_id:
                    digest = output_object_ref.removeprefix("sha256:")
                    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                        raise ValueError("Invalid media output object reference")
                    path = f"artifacts/{row.id}"
                    existing_artifact = (await db.execute(select(ArtifactRow.id).where(
                        ArtifactRow.turn_id == row.turn_id, ArtifactRow.path == path,
                    ).limit(1))).scalar_one_or_none()
                    if not existing_artifact:
                        db.add(ArtifactRow(id=identifier(), turn_id=row.turn_id, path=path,
                            sha256=digest, size=output_size, mime=output_mime or "application/octet-stream",
                            object_ref=output_object_ref, created_at=now()))
                    if row.output_role in {"workspace", "delivery"}:
                        project_path = self._media_project_path(row.id, output_mime)
                        await self._register_project_file_locked(db, row.thread_id, path=project_path,
                            source_type="artifact", source_turn_id=row.turn_id, sha256=digest,
                            size=output_size, mime=output_mime or "application/octet-stream",
                            object_ref=output_object_ref)
                existing = (await db.execute(select(MediaOutboxRow).where(
                    MediaOutboxRow.job_id == row.id, MediaOutboxRow.event_type == "media.job.terminal"))).scalar_one_or_none()
                if not existing:
                    db.add(MediaOutboxRow(id=identifier(), job_id=row.id, event_type="media.job.terminal",
                        status="pending", created_at=now(), updated_at=now()))
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
            return True


    async def complete_media_job(self, job_id: str, owner: str, *, provider_job_id: str,
                                 outputs: list[dict], credits_consumed: float | None = None,
                                 generation: int = 0) -> bool:
        if not outputs or sum(output.get("role") == "primary" for output in outputs) != 1:
            raise ValueError("media_outputs_require_one_primary")
        async with self.connections.begin() as db:
            query=select(MediaJobRow).where(MediaJobRow.id==job_id,MediaJobRow.lease_owner==owner)
            if generation:query=query.where(MediaJobRow.generation==generation)
            row=(await db.execute(query.with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_media_job_lease")
            if await self._media_source_cancelled_locked(db,row):
                raise MediaCancellationRequested("media_cancellation_requested")
            existing = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id == row.id))).scalars().all()
            if existing:
                raise RuntimeError("media_outputs_already_committed")
            timestamp = now()
            primary = next(output for output in outputs if output["role"] == "primary")
            artifact_ids: list[str] = []
            seen_roles: set[str] = set()
            seen_names: set[str] = set()
            for output in outputs:
                role = str(output.get("role") or "")
                filename = PurePosixPath(str(output.get("filename") or "")).name
                object_ref = str(output.get("object_ref") or "")
                digest = object_ref.removeprefix("sha256:")
                mime = str(output.get("mime") or "application/octet-stream")
                size = int(output.get("size") or 0)
                valid_role=(role in {"primary","preview","auxiliary"} or
                    bool(row.source_job_id and re.fullmatch(r"output_[0-9]{4}",role)))
                if (not valid_role or role in seen_roles
                        or not filename or filename in seen_names or size <= 0 or len(digest) != 64
                        or any(character not in "0123456789abcdef" for character in digest)):
                    raise ValueError("invalid_media_output")
                seen_roles.add(role); seen_names.add(filename)
                artifact_id = identifier() if row.turn_id else ""
                artifact_path = f"artifacts/{row.id}/{filename}"
                if row.turn_id:
                    db.add(ArtifactRow(id=artifact_id, turn_id=row.turn_id, path=artifact_path,
                        sha256=digest, size=size, mime=mime, object_ref=object_ref, created_at=timestamp))
                    artifact_ids.append(artifact_id)
                project_file_id = ""
                if row.output_role in {"workspace", "delivery"}:
                    project, _ = await self._register_project_file_locked(db, row.thread_id,
                        path=f"artifacts/3d/{row.id}/{filename}", source_type="artifact",
                        source_turn_id=row.turn_id, sha256=digest, size=size, mime=mime,
                        object_ref=object_ref)
                    project_file_id = project.id
                if row.turn_id:
                    sequence = await self.next_item_sequence(db, row.thread_id)
                    db.add(ItemRow(id=identifier(), thread_id=row.thread_id, turn_id=row.turn_id,
                        sequence=sequence, type=ItemType.ARTIFACT.value, status="completed",
                        summary=filename, payload_json=json.dumps({"artifact_id": artifact_id,
                            "mime": mime, "size": size}, ensure_ascii=False),
                        created_at=timestamp, updated_at=timestamp))
                db.add(MediaJobOutputRow(id=identifier(), job_id=row.id, role=role,
                    filename=filename, object_ref=object_ref, sha256=digest, size=size, mime=mime,
                    artifact_id=artifact_id, project_file_id=project_file_id, created_at=timestamp))
            row.provider_job_id = provider_job_id or row.provider_job_id
            row.output_object_ref = primary["object_ref"]
            row.output_mime = primary["mime"]
            row.output_size = int(primary["size"])
            row.credits_consumed = "" if credits_consumed is None else str(credits_consumed)
            row.status = "succeeded"; row.phase = "committed"; row.progress = 100
            row.error_code = ""; row.error_detail = ""; row.updated_at = timestamp
            row.finished_at = row.finished_at or timestamp
            row.lease_owner = ""; row.lease_expires_at = None
            if row.turn_id:
                await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                    MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=timestamp))
            outbox = (await db.execute(select(MediaOutboxRow).where(MediaOutboxRow.job_id == row.id,
                MediaOutboxRow.event_type == "media.job.terminal"))).scalar_one_or_none()
            if not outbox:
                db.add(MediaOutboxRow(id=identifier(), job_id=row.id,
                    event_type="media.job.terminal", status="pending",
                    created_at=timestamp, updated_at=timestamp))
            return True


    async def claim_media_outbox(self, owner: str, limit: int = 20) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(MediaOutboxRow, MediaJobRow).join(MediaJobRow, MediaJobRow.id == MediaOutboxRow.job_id).where(
                MediaOutboxRow.status.in_(["pending", "delivering"]),
                or_(MediaOutboxRow.lease_expires_at.is_(None), MediaOutboxRow.lease_expires_at < now())
            ).order_by(MediaOutboxRow.created_at.asc()).limit(limit).with_for_update())).all()
            result = []
            for outbox, job in rows:
                outbox.status = "delivering"; outbox.lease_owner = owner; outbox.lease_expires_at = now() + timedelta(seconds=60)
                outbox.attempt_count += 1; outbox.updated_at = now()
                result.append({"outbox_id": outbox.id, "job_id": job.id, "thread_id": job.thread_id,
                    "owner_id": job.owner_id, "turn_id": job.turn_id, "kind": job.kind, "model": job.model, "status": job.status,
                    "output_object_ref": job.output_object_ref, "output_mime": job.output_mime, "output_size": job.output_size,
                    "error_code": job.error_code, "error_detail": job.error_detail})
        return result


    async def finish_media_outbox(self, outbox_id: str, owner: str, *, delivered: bool, error: str = "") -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaOutboxRow).where(MediaOutboxRow.id == outbox_id,
                MediaOutboxRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row: return
            row.status = "delivered" if delivered else "pending"; row.last_error = error[:2000]
            row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
