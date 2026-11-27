"""Media Drafts persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from datetime import (datetime)
from sqlalchemy import (func, select, update)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sandbox_harness.common.contracts.domain import (TurnState, TERMINAL, identifier, now, MediaDraftView, MediaJobView, MediaJobOutputView)
from sandbox_harness.common.contracts.job_errors import (job_error_payload)
from sandbox_harness.common.contracts.errors import error_reference
from .models import (ArtifactRow, MediaBatchRow, MediaDraftRow, MediaJobOutputRow, MediaJobRow, ThreadRow, TurnRow, utc_datetime)


class MediaDraftsTransactions:
    @staticmethod
    def _media_draft_view(row: MediaDraftRow) -> MediaDraftView:
        status = "expired" if row.status == "awaiting_confirmation" and utc_datetime(row.expires_at) <= now() else row.status
        return MediaDraftView(id=row.id, thread_id=row.thread_id, kind=row.kind, model=row.model,
            generation_mode=row.generation_mode or {"image":"text_to_image","video":"text_to_video",
                "audio":"text_to_audio", "model3d":"text_to_model"}[row.kind],
            parameters=json.loads(row.parameters_json or "{}"), references=json.loads(row.references_json or "[]"), status=status,
            expires_at=utc_datetime(row.expires_at), job_id=row.job_id or "", output_role=row.output_role)


    @staticmethod
    def _media_job_view(row: MediaJobRow, artifact_ids: list[str] | None = None,
                        outputs: list[MediaJobOutputView] | None = None) -> MediaJobView:
        reason = "" if row.cancellation_mode == "supported" and row.execution_mode == "async" else "provider_not_cancelable"
        try:
            credits = float(row.credits_consumed) if row.credits_consumed else None
        except ValueError:
            credits = None
        error=(job_error_payload(row.error_code,detail=row.error_detail)
            if row.error_code else {})
        if row.provider_job_id and error.get('side_effect_state') in {'none','possible'}:
            error['side_effect_state']='accepted'
        return MediaJobView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id or "", kind=row.kind,
            model=row.model, generation_mode=row.generation_mode or "",
            references=[] if row.source_job_id else json.loads(row.references_json or "[]"),
            provider=row.provider, adapter=row.adapter, execution_mode=row.execution_mode,
            cancellation_mode=row.cancellation_mode, status=row.status, phase=row.phase or "",
            progress=row.progress, error_code=row.error_code or "", error_detail=row.error_detail or "",
            error_message=error.get('error_message',''),
            error_guidance=error.get('error_guidance',''),failed_phase=error.get('failed_phase',''),
            troubleshooting_id=(error_reference("media-worker",row.id,row.error_code)
                if row.error_code else ""),
            retryable=bool(error.get('retryable',False)),retry_class=error.get('retry_class',''),
            side_effect_state=error.get('side_effect_state',''),
            cancelable=row.execution_mode == "async" and row.cancellation_mode == "supported",
            cancel_reason=reason, artifact_ids=artifact_ids or [], outputs=outputs or [],
            credits_consumed=credits, output_role=row.output_role,
            created_at=row.created_at, finished_at=row.finished_at)


    @staticmethod
    def _media_project_path(job_id: str, mime: str) -> str:
        extension = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp",
            "video/mp4": "mp4", "video/webm": "webm", "audio/mpeg": "mp3",
            "audio/wav": "wav", "audio/flac": "flac", "audio/mp4": "m4a",
            "audio/webm": "webm", "model/gltf-binary": "glb"}.get(mime, "bin")
        return f"artifacts/media/{job_id}.{extension}"

    @classmethod
    def _media_output_project_path(cls, job_id: str, role: str,
                                   filename: str, mime: str) -> str:
        if role == "primary":
            return cls._media_project_path(job_id, mime)
        return f"artifacts/media/{job_id}/{filename}"


    @staticmethod
    async def _media_job_outputs(db: AsyncSession, job_id: str) -> list[MediaJobOutputView]:
        rows = (await db.execute(select(MediaJobOutputRow).where(
            MediaJobOutputRow.job_id == job_id,
        ).order_by(MediaJobOutputRow.created_at, MediaJobOutputRow.id))).scalars().all()
        return [MediaJobOutputView(id=row.id, role=row.role, filename=row.filename, mime=row.mime,
            size=row.size, artifact_id=row.artifact_id, project_file_id=row.project_file_id) for row in rows]


    @staticmethod
    async def _media_artifact_ids(db: AsyncSession, row: MediaJobRow) -> list[str]:
        output_ids = list((await db.execute(select(MediaJobOutputRow.artifact_id).where(
            MediaJobOutputRow.job_id == row.id, MediaJobOutputRow.artifact_id != "",
        ).order_by(MediaJobOutputRow.created_at))).scalars().all())
        if output_ids:
            return output_ids
        if not row.turn_id:
            return []
        return list((await db.execute(select(ArtifactRow.id).where(
            ArtifactRow.turn_id == row.turn_id,
            ArtifactRow.path == f"artifacts/{row.id}",
        ).order_by(ArtifactRow.created_at))).scalars().all())


    async def create_media_draft(self, owner_id: str, thread_id: str, value, *, expires_at: datetime,
                                 provider_spec, reference_snapshots: list[dict] | None = None) -> tuple[MediaDraftView, bool]:
        reference_snapshots = reference_snapshots or []
        async with self.connections.begin() as db:
            turn = None
            if value.turn_id:
                turn = await db.get(TurnRow, value.turn_id, with_for_update=True)
                thread = await db.get(ThreadRow, thread_id)
                if not turn or not thread or turn.thread_id != thread_id or thread.owner_id != owner_id:
                    raise KeyError("media_turn_not_found")
            existing = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id == thread_id, MediaDraftRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                if (existing.owner_id != owner_id or existing.kind != value.kind or existing.model != value.model
                        or json.loads(existing.parameters_json or "{}") != value.parameters
                        or (existing.generation_mode or "") != (value.generation_mode or "")
                        or json.loads(existing.references_json or "[]") != reference_snapshots
                        or existing.output_role != value.output_role):
                    raise RuntimeError("media_draft_idempotency_conflict")
                return self._media_draft_view(existing), False
            if turn and TurnState(turn.state) in TERMINAL:
                raise RuntimeError("media_turn_terminal")
            if turn:
                batch = await db.get(MediaBatchRow, turn.id, with_for_update=True)
                if batch and batch.status != "open":
                    raise RuntimeError("media_batch_sealed")
                if not batch:
                    db.add(MediaBatchRow(turn_id=turn.id, thread_id=thread_id, owner_id=owner_id,
                        status="open", created_at=now(), updated_at=now()))
            row = MediaDraftRow(id=identifier(), owner_id=owner_id, thread_id=thread_id, turn_id=value.turn_id,
                kind=value.kind, model=value.model, generation_mode=value.generation_mode or "",
                parameters_json=json.dumps(value.parameters, ensure_ascii=False),
                references_json=json.dumps(reference_snapshots, ensure_ascii=False),
                idempotency_key=value.idempotency_key, status="awaiting_confirmation", expires_at=expires_at,
                output_role=value.output_role, created_at=now(), updated_at=now())
            db.add(row)
        return self._media_draft_view(row), True


    async def seal_media_batch(self, turn_id: str, thread_id: str, owner_id: str) -> dict:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            thread = await db.get(ThreadRow, thread_id)
            if not turn or not thread or turn.thread_id != thread_id or thread.owner_id != owner_id:
                raise KeyError("media_turn_not_found")
            batch = await db.get(MediaBatchRow, turn_id, with_for_update=True)
            if not batch:
                raise RuntimeError("media_batch_empty")
            count = (await db.execute(select(func.count()).select_from(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id,
            ))).scalar_one()
            if not count:
                raise RuntimeError("media_batch_empty")
            if batch.status == "open":
                batch.status = "sealed"; batch.sealed_at = now(); batch.updated_at = now()
            elif batch.status not in {"sealed", "delivering", "continued"}:
                raise RuntimeError("media_batch_not_sealable")
            return {"turn_id": turn_id, "status": batch.status, "draft_count": count}


    async def get_media_draft(self, draft_id: str, owner_id: str) -> MediaDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return self._media_draft_view(row) if row else None


    async def get_media_draft_snapshot(self, draft_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id))).scalar_one_or_none()
        if not row:
            return None
        return {"kind": row.kind, "model": row.model,
            "generation_mode": row.generation_mode or {"image":"text_to_image","video":"text_to_video",
                "audio":"text_to_audio", "model3d":"text_to_model"}[row.kind],
            "parameters": json.loads(row.parameters_json or "{}"),
            "references": json.loads(row.references_json or "[]")}


    async def list_media_drafts(self, thread_id: str, owner_id: str) -> list[MediaDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id == thread_id, MediaDraftRow.owner_id == owner_id,
                MediaDraftRow.id.not_in(select(MediaJobRow.draft_id).where(
                    MediaJobRow.source_job_id.is_not(None))),
            ).order_by(MediaDraftRow.created_at.desc()))).scalars().all()
        return [self._media_draft_view(row) for row in rows]


    async def list_media_drafts_for_turn(self, turn_id: str, owner_id: str) -> list[MediaDraftView]:
        async with self.connections() as db:
            rows=(await db.execute(select(MediaDraftRow).where(MediaDraftRow.turn_id==turn_id,
                MediaDraftRow.owner_id==owner_id).order_by(MediaDraftRow.created_at,
                    MediaDraftRow.id))).scalars().all()
        return [self._media_draft_view(row) for row in rows]


    async def owner_id_for_thread(self, thread_id: str) -> str:
        async with self.connections() as db:
            return (await db.execute(select(ThreadRow.owner_id).where(ThreadRow.id == thread_id))).scalar_one_or_none() or ""


    async def update_media_draft(self, draft_id: str, owner_id: str, *, model: str | None,
                                 generation_mode: str | None, parameters: dict | None,
                                 reference_snapshots: list[dict] | None) -> MediaDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_draft_not_found")
            if row.status != "awaiting_confirmation" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_draft_not_editable")
            if model is not None: row.model = model
            if generation_mode is not None: row.generation_mode = generation_mode
            if parameters is not None: row.parameters_json = json.dumps(parameters, ensure_ascii=False)
            if reference_snapshots is not None:
                row.references_json = json.dumps(reference_snapshots, ensure_ascii=False)
            row.updated_at = now()
        return self._media_draft_view(row)


    async def cancel_media_draft(self, draft_id: str, owner_id: str) -> MediaDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_draft_not_found")
            if row.status == "confirmed":
                raise RuntimeError("media_draft_already_confirmed")
            if row.status == "awaiting_confirmation":
                row.status = "cancelled"; row.updated_at = now()
                if row.turn_id:
                    await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                        MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
        return self._media_draft_view(row)
