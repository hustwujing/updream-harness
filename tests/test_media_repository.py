import pytest
from sqlalchemy import func, inspect, select
from types import SimpleNamespace

from sandbox_harness.common.db.models import MediaJobOutputRow, MediaJobRow, MediaOutboxRow


def source_payload():
    return {
        "dispatch_id": "dispatch-1", "media_job_id": "media-job", "source_job_id": "source-job",
        "source_generation": 3, "request_digest": "a" * 64, "owner_id": "owner",
        "thread_id": "thread", "kind": "image", "model": "image/model", "provider": "provider",
        "adapter": "image_api", "execution_mode": "async", "cancellation_mode": "supported",
        "parameters": {"prompt": "opaque"}, "references": [],
        "output_contracts": [{"role": "primary", "mime_patterns": ["image/*"],
                              "max_item_bytes": 1024}],
    }


async def test_media_source_permit_fences_submission_and_recovery(tmp_path):
    from sandbox_harness.common.db.media_repository import MediaRepository
    repository = MediaRepository("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    try:
        await repository.initialize()
        first = await repository.accept_source_job(source_payload())
        assert (await repository.accept_source_job(source_payload())).id == first.id
        claimed = (await repository.claim_media_jobs("worker"))[0]
        assert claimed["source_job_id"] == "source-job" and claimed["output_contracts"]
        assert not await repository.media_cancel_requested(claimed["id"], "worker", claimed["generation"],
                                                            begin_submission=True)
        with pytest.raises(RuntimeError, match="media_submission_outcome_unknown"):
            await repository.media_cancel_requested(claimed["id"], "worker", claimed["generation"],
                                                    begin_submission=True)
        await repository.record_media_submission_rejection(claimed["id"], "worker", claimed["generation"])
        assert not await repository.media_cancel_requested(claimed["id"], "worker", claimed["generation"],
                                                            begin_submission=True)
    finally:
        await repository.close()


async def test_media_cancel_command_is_idempotent_and_visible_to_worker(tmp_path):
    from sandbox_harness.common.db.media_repository import MediaRepository
    repository = MediaRepository("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    try:
        await repository.initialize()
        await repository.accept_source_job(source_payload())
        await repository.cancel_source_job("dispatch-1", "source-job", 3)
        await repository.cancel_source_job("dispatch-1", "source-job", 3)
        claimed = (await repository.claim_media_jobs("worker"))[0]
        assert claimed["status"] == "cancel_requested"
        assert await repository.media_cancel_requested(claimed["id"], "worker", claimed["generation"])
        with pytest.raises(ValueError, match="media_source_command_conflict"):
            await repository.cancel_source_job("dispatch-1", "source-job", 4)
    finally:
        await repository.close()


async def test_media_completion_is_local_and_publishes_one_receipt(tmp_path):
    from sandbox_harness.common.db.media_repository import MediaRepository
    repository = MediaRepository("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    output = {"role": "primary", "filename": "image.png", "object_ref": "sha256:" + "b" * 64,
              "sha256": "b" * 64, "mime": "image/png", "size": 12}
    try:
        await repository.initialize()
        await repository.accept_source_job(source_payload())
        claimed = (await repository.claim_media_jobs("worker"))[0]
        await repository.media_cancel_requested(claimed["id"], "worker", claimed["generation"],
                                                begin_submission=True)
        assert await repository.complete_media_job(claimed["id"], "worker", provider_job_id="provider-job",
                                                   outputs=[output], generation=claimed["generation"])
        async with repository.connections() as session:
            assert await session.scalar(select(func.count()).select_from(MediaJobOutputRow)) == 1
            assert await session.scalar(select(func.count()).select_from(MediaOutboxRow)) == 1
            row = await session.get(MediaJobRow, claimed["id"])
            assert row.status == "succeeded" and row.provider_job_id == "provider-job"
        async with repository.engine.connect() as connection:
            tables = await connection.run_sync(lambda value: set(inspect(value).get_table_names()))
        assert not {"jobs", "artifacts", "project_files", "items", "threads"}.intersection(tables)
    finally:
        await repository.close()


def test_media_service_uses_explicit_media_database():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    source = (root / "src/sandbox_harness/services/media_worker/media_service.py").read_text()
    assert 'for_service("media-worker")["media"]' in source
    assert "settings.database_url" not in source
    assert "common.db.store" not in source


async def test_core_media_authority_replays_one_dispatch_and_orders_cancel(tmp_path):
    from sandbox_harness.common.catalog.catalog import HarnessCatalog
    from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate
    from sandbox_harness.common.contracts.job_contracts import (
        JobExpectedOutput, JobGroupCreate, JobKind, JobRequest)
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.media_repository import MediaRepository
    from sandbox_harness.common.execution.job_adapters import JobExecutionContext
    from sandbox_harness.common.jobs.media_authority import MediaAuthorityClient
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="media-authority")
    media = MediaRepository("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    try:
        await core.initialize(); await media.initialize()
        profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
        thread = await core.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model, title="media"),
                                          profile, "owner")
        turn, _, _ = await core.create_turn(thread.id, TurnCreate(message="generate",
            idempotency_key="media-authority-turn"), profile, "owner")
        request = JobRequest(local_job_id="image", kind=JobKind.PROVIDER, adapter="image.generate",
            descriptor_id="image.generate", descriptor_version="1", descriptor_digest="c" * 64,
            idempotency_key="media-authority-job", parameters={},
            expected_outputs=(JobExpectedOutput(artifact_role="images", mime_patterns=("image/png",)),))
        _, jobs = await core.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="media-authority-group", jobs=[request]))
        attempt = (await core.claim_available_jobs("worker", "node", 60, limit=1,
                                                  adapters={"image.generate"}))[0]
        await core.mark_job_submission_started(jobs[0].id, "worker", attempt.generation)
        job = await core.get_job(jobs[0].id)
        context = JobExecutionContext(job=job, request=request, generation=attempt.generation)
        validated = SimpleNamespace(kind="image", model="image/model", parameters={"prompt":"opaque"},
            references=[], spec=SimpleNamespace(provider="provider", adapter="image_api",
                execution="async", cancellation="supported"))
        authority = MediaAuthorityClient(core, media)
        first = await authority.submit(context, validated)
        second = await authority.submit(context, validated)
        assert first.id == second.id
        async with media.connections() as session:
            assert await session.scalar(select(func.count()).select_from(MediaJobRow)) == 1
        await core.request_job_cancel(job.id, "owner")
        context = JobExecutionContext(job=await core.get_job(job.id), request=request,
                                      generation=attempt.generation)
        await authority.cancel(context)
        claimed = (await media.claim_media_jobs("media-worker"))[0]
        assert claimed["status"] == "cancel_requested"
    finally:
        await core.close(); await media.close()


async def test_media_worker_executes_authorized_source_job_with_media_database_only(tmp_path):
    import hashlib
    from sandbox_harness.common.configuration.config import Settings
    from sandbox_harness.common.contracts.media_provider import DownloadedMedia, ProviderResult
    from sandbox_harness.common.db.media_repository import MediaRepository
    from sandbox_harness.common.storage.objects import ObjectStore
    from sandbox_harness.services.media_worker.media_worker import MediaWorker

    class Provider:
        submissions = 0

        async def prepare(self, job): return None
        async def submit(self, job):
            self.submissions += 1
            return ProviderResult("accepted", provider_job_id="provider-job")
        async def poll(self, job):
            return ProviderResult("success", provider_job_id="provider-job",
                outputs=[{"role":"primary","filename":"result.png","url":"provider://result","kind":"image"}])
        async def cancel(self, job): return ProviderResult("cancelled", provider_job_id="provider-job")
        async def download(self, job, output, *, staging_root, prefix, maximum_bytes):
            data=b"\x89PNG\r\n\x1a\nmedia-repository"
            path=staging_root/(prefix+"result.png");path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            return DownloadedMedia(path,len(data),hashlib.sha256(data).hexdigest(),"image/png")

    repository = MediaRepository("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    objects = ObjectStore(tmp_path / "objects")
    provider = Provider()
    settings = Settings(backend="fake", data_root=tmp_path, media_job_lease_seconds=30)
    worker = MediaWorker(settings, repository, objects, None,
                         external_providers={"image_api":provider})
    try:
        await repository.initialize(); await repository.accept_source_job(source_payload())
        await worker.process((await repository.claim_media_jobs(worker.owner))[0])
        value = await repository.get_media_job("media-job", "owner")
        assert value.status == "succeeded" and provider.submissions == 1
        assert len(value.outputs) == 1 and value.outputs[0].artifact_id == ""
    finally:
        await repository.close()
