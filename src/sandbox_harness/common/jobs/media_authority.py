"""Durable core-to-media source Job authorization and cancellation."""
from __future__ import annotations

from sqlalchemy import select

from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.contracts.job_contracts import JobState, TERMINAL_JOB_STATES
from sandbox_harness.common.db.models import (ArtifactRow, JobAttemptRow, JobLeaseRow, JobRow,
                                              ThreadRow, TurnRow, canonical_digest, utc_datetime)


class MediaAuthorityClient:
    def __init__(self, core, media):
        self.core, self.media = core, media

    @staticmethod
    def dispatch_id(job_id, generation):
        return canonical_digest(["media.source", job_id, generation])

    async def _source(self, db, context):
        source = await db.get(JobRow, context.job.id, with_for_update=True)
        lease = await db.get(JobLeaseRow, context.job.id, with_for_update=True)
        attempt = await db.get(JobAttemptRow, lease.attempt_id) if lease else None
        if (not source or not lease or source.generation != context.generation
                or lease.generation != context.generation or utc_datetime(lease.expires_at) <= now()
                or source.request_digest != context.job.request_digest):
            raise RuntimeError("stale_job_generation")
        return source, attempt

    async def submit(self, context, validated):
        dispatch_id = self.dispatch_id(context.job.id, context.generation)
        media_job_id = canonical_digest([dispatch_id, "media-job"])[:32]
        payload = {"dispatch_id":dispatch_id,"media_job_id":media_job_id,
            "source_job_id":context.job.id,"source_generation":context.generation,
            "request_digest":context.job.request_digest,"owner_id":context.job.owner_id,
            "thread_id":context.job.thread_id,"kind":validated.kind,"model":validated.model,
            "provider":validated.spec.provider,"adapter":validated.spec.adapter,
            "execution_mode":validated.spec.execution,"cancellation_mode":validated.spec.cancellation,
            "parameters":validated.parameters,"references":validated.references,
            "output_contracts":[value.model_dump(mode="json")
                for value in context.request.expected_outputs]}
        async with self.core.transaction() as db:
            source, attempt = await self._source(db, context)
            if (source.cancel_requested or JobState(source.state) in TERMINAL_JOB_STATES):
                raise RuntimeError("job_cancel_requested")
            if not attempt or attempt.accepted_at or attempt.outcome != "SUBMITTING":
                raise RuntimeError("job_submission_intent_conflict")
            for reference in payload["references"]:
                artifact = await db.scalar(select(ArtifactRow).join(TurnRow,
                    TurnRow.id == ArtifactRow.turn_id).join(ThreadRow,
                    ThreadRow.id == TurnRow.thread_id).where(
                        ArtifactRow.id == reference.get("artifact_id"),
                        TurnRow.thread_id == source.thread_id,
                        ThreadRow.owner_id == source.owner_id))
                if (not artifact or reference.get("object_ref") != artifact.object_ref
                        or reference.get("sha256") != artifact.sha256
                        or reference.get("size") != artifact.size):
                    raise RuntimeError("job_input_artifact_outside_scope")
            await self.core.enqueue_remote(db, command_id=dispatch_id, target="media",
                kind="media.source.authorize", aggregate_id=source.id, owner_id=source.owner_id,
                thread_id=source.thread_id, generation=context.generation, payload=payload)
        await self.core.relay_remote({"media":self.media.handoff})
        value = await self.media.media_execution_for_job(context.job.id, context.job.owner_id)
        if not value or value.id != media_job_id:
            raise RuntimeError("accepted_media_job_missing")
        return value

    async def cancel(self, context):
        dispatch_id = self.dispatch_id(context.job.id, context.generation)
        command_id = canonical_digest(["media.source.cancel", dispatch_id])
        payload = {"dispatch_id":dispatch_id,"source_job_id":context.job.id,
                   "source_generation":context.generation}
        async with self.core.transaction() as db:
            source, _ = await self._source(db, context)
            if not source.cancel_requested and JobState(source.state) not in TERMINAL_JOB_STATES:
                raise RuntimeError("media_source_cancel_not_authorized")
            await self.core.enqueue_remote(db, command_id=command_id, target="media",
                kind="media.source.cancel", aggregate_id=source.id, owner_id=source.owner_id,
                thread_id=source.thread_id, generation=context.generation, payload=payload)
        await self.core.relay_remote({"media":self.media.handoff})

    async def get_media_job(self, media_job_id, owner_id):
        return await self.media.get_media_job(media_job_id, owner_id)

    async def media_job_output_for_thread(self, media_job_id, thread_id):
        return await self.media.media_job_output_for_thread(media_job_id, thread_id)

    async def media_execution_for_job(self, source_job_id, owner_id):
        return await self.media.media_execution_for_job(source_job_id, owner_id)

    async def media_job_outputs_for_source(self, media_job_id, source_job_id, owner_id):
        return await self.media.media_job_outputs_for_source(media_job_id, source_job_id, owner_id)

    async def source_references(self, context):
        result = []
        async with self.core.connections() as db:
            for binding in context.request.input_artifacts:
                artifact = await db.scalar(select(ArtifactRow).join(TurnRow,
                    TurnRow.id == ArtifactRow.turn_id).join(ThreadRow,
                    ThreadRow.id == TurnRow.thread_id).where(
                        ArtifactRow.id == binding.artifact_id,
                        TurnRow.thread_id == context.job.thread_id,
                        ThreadRow.owner_id == context.job.owner_id))
                if not artifact:
                    raise RuntimeError("job_input_artifact_outside_scope")
                result.append({"artifact_id":artifact.id,"role":binding.artifact_role,
                    "filename":artifact.path.rsplit("/",1)[-1],"object_ref":artifact.object_ref,
                    "sha256":artifact.sha256,"size":artifact.size,"mime":artifact.mime})
        return result
