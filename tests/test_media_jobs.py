from datetime import timedelta
import io
import tarfile
from types import SimpleNamespace

import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, MediaDraftCreate, ThreadCreate, TurnCreate, TurnState, now)
from sandbox_harness.services.media_worker.media_worker import (MediaWorker)
from sandbox_harness.services.platform_worker.platform_service import (
    media_group_has_executed_job, media_group_needs_agent_continuation,
)
from test_support.legacy_store import (ArtifactRow, ItemRow, MediaJobOutputRow, MediaOutboxRow, Store)


@pytest.mark.asyncio
async def test_media_draft_confirm_is_idempotent_and_snapshots_provider(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    await store.initialize()
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Media"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="generate", idempotency_key="media-turn-1"), profile, owner)
        value = MediaDraftCreate(kind="video", model="sed2", parameters={"duration": 5},
            idempotency_key="media-key-1", turn_id=turn.id, output_role="workspace")
        spec = SimpleNamespace(provider="ark", adapter="ark_video_v2", execution="async", cancellation="unsupported")
        draft, created = await store.create_media_draft(owner, thread.id, value,
            expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        assert created and draft.status == "awaiting_confirmation"
        cancel_value = value.model_copy(update={"idempotency_key": "media-key-cancel"})
        cancellable, _ = await store.create_media_draft(owner, thread.id, cancel_value,
            expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        first = await store.confirm_media_draft(draft.id, owner, provider_spec=spec)
        second = await store.confirm_media_draft(draft.id, owner, provider_spec=spec)
        assert first.id == second.id and first.provider == "ark"
        assert first.created_at and first.finished_at is None
        assert not first.cancelable
        ready, _ = await store.media_continuation_group(turn.id)
        assert ready is False
        cancelled = await store.cancel_media_draft(cancellable.id, owner)
        assert cancelled.status == "cancelled"
        assert {item.id for item in await store.list_media_drafts(thread.id, owner)} == {draft.id, cancellable.id}
        claimed = await store.claim_media_jobs("worker")
        assert claimed[0]["id"] == first.id
        assert await store.release_media_job_leases("worker") == 1
        reclaimed = await store.claim_media_jobs("worker-restarted")
        assert reclaimed[0]["id"] == first.id
        await store.update_media_job(first.id, "worker-restarted", reclaimed[0]["generation"],
            status="succeeded", phase="committed",
            output_object_ref="sha256:" + "1" * 64, output_mime="video/mp4", output_size=1234)
        ready, _ = await store.media_continuation_group(turn.id)
        assert ready is False
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        ready, group = await store.media_continuation_group(turn.id)
        assert ready is True and [item["id"] for item in group] == [first.id, cancellable.id]
        assert [item["status"] for item in group] == ["succeeded", "cancelled"]
        assert group[0]["project_path"] == f"artifacts/media/{first.id}.mp4"
        project_files = await store.current_project_files(thread.id, owner)
        assert [item.path for item in project_files] == [f"artifacts/media/{first.id}.mp4"]
        completed = await store.get_media_job(first.id, owner)
        listed = await store.list_media_jobs(thread.id, owner)
        assert completed and len(completed.artifact_ids) == 1
        assert completed.finished_at and completed.finished_at >= completed.created_at
        assert listed[0].artifact_ids == completed.artifact_ids
        async with store.connections() as db:
            rows = (await db.execute(__import__('sqlalchemy').select(MediaOutboxRow))).scalars().all()
            artifacts = (await db.execute(__import__('sqlalchemy').select(ArtifactRow))).scalars().all()
        assert len(rows) == 1 and rows[0].status == "pending"
        assert len(artifacts) == 1 and artifacts[0].id == completed.artifact_ids[0]
        assert artifacts[0].object_ref == "sha256:" + "1" * 64
        with pytest.raises(RuntimeError, match="media_turn_terminal"):
            await store.create_media_draft(owner, thread.id,
                value.model_copy(update={"idempotency_key": "media-key-after-terminal"}),
                expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        with pytest.raises(KeyError, match="media_turn_not_found"):
            await store.create_media_draft(owner, "b" * 32,
                value.model_copy(update={"idempotency_key": "media-key-cross-thread"}),
                expires_at=now() + timedelta(minutes=10), provider_spec=spec)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_media_batch_confirm_is_atomic_and_idempotent(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"media-batch-confirm.db"));await store.initialize()
    try:
        owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Batch confirm"),profile,owner)
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message="batch",
            idempotency_key="media-batch-confirm-turn"),profile,owner)
        spec=SimpleNamespace(kind="image",model="seedream-5.0",provider="ark",adapter="ark_image",execution="sync",cancellation="unsupported")
        drafts=[]
        for index in range(3):
            draft,_=await store.create_media_draft(owner,thread.id,MediaDraftCreate(kind="image",
                model="seedream-5.0",turn_id=turn.id,idempotency_key=f"batch-draft-{index}"),
                expires_at=now()+timedelta(minutes=10),provider_spec=spec)
            drafts.append(draft)
        await store.seal_media_batch(turn.id,thread.id,owner)
        specs={draft.id:spec for draft in drafts}
        first=await store.confirm_media_batch(turn.id,owner,provider_specs=specs)
        second=await store.confirm_media_batch(turn.id,owner,provider_specs=specs)
        assert [job.id for job in first]==[job.id for job in second]
        assert len(first)==3 and len(await store.list_media_jobs(thread.id,owner))==3
    finally:await store.close()


@pytest.mark.asyncio
async def test_media_batch_confirm_rejects_entire_batch_when_one_draft_expired(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"media-batch-expired.db"));await store.initialize()
    try:
        owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Batch expiry"),profile,owner)
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message="batch",
            idempotency_key="media-batch-expiry-turn"),profile,owner)
        spec=SimpleNamespace(kind="image",model="seedream-5.0",provider="ark",adapter="ark_image",execution="sync",cancellation="unsupported")
        valid,_=await store.create_media_draft(owner,thread.id,MediaDraftCreate(kind="image",
            model="seedream-5.0",turn_id=turn.id,idempotency_key="batch-valid"),
            expires_at=now()+timedelta(minutes=10),provider_spec=spec)
        expired,_=await store.create_media_draft(owner,thread.id,MediaDraftCreate(kind="image",
            model="seedream-5.0",turn_id=turn.id,idempotency_key="batch-expired"),
            expires_at=now()-timedelta(seconds=1),provider_spec=spec)
        await store.seal_media_batch(turn.id,thread.id,owner)
        with pytest.raises(RuntimeError,match="media_batch_not_confirmable"):
            await store.confirm_media_batch(turn.id,owner,
                provider_specs={valid.id:spec,expired.id:spec})
        assert await store.list_media_jobs(thread.id,owner)==[]
        assert {draft.status for draft in await store.list_media_drafts(thread.id,owner)}=={
            "awaiting_confirmation","expired"}
    finally:await store.close()


@pytest.mark.asyncio
async def test_expired_media_draft_cannot_confirm(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "media-expired.db"))
    await store.initialize()
    try:
        value = MediaDraftCreate(kind="image", model="seedream-5.0", idempotency_key="media-key-2")
        spec = SimpleNamespace(provider="ark", adapter="ark_image", execution="sync", cancellation="unsupported")
        draft, _ = await store.create_media_draft("a" * 64, "b" * 32, value,
            expires_at=now() - timedelta(seconds=1), provider_spec=spec)
        with pytest.raises(RuntimeError, match="not_confirmable"):
            await store.confirm_media_draft(draft.id, "a" * 64, provider_spec=spec)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_media_continuation_waits_for_every_image_and_video_job(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "media-group.db"))
    await store.initialize()
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Media group"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="generate a media set", idempotency_key="media-group-turn"), profile, owner)
        spec = SimpleNamespace(provider="ark", adapter="media", execution="async", cancellation="unsupported")
        jobs = []
        for index, kind in enumerate(("image", "video", "image", "video")):
            draft, _ = await store.create_media_draft(owner, thread.id,
                MediaDraftCreate(kind=kind, model="test-model", turn_id=turn.id,
                    idempotency_key=f"media-group-{index}"),
                expires_at=now() + timedelta(minutes=10), provider_spec=spec)
            jobs.append(await store.confirm_media_draft(draft.id, owner, provider_spec=spec))
        sealed = await store.seal_media_batch(turn.id, thread.id, owner)
        assert sealed == {"turn_id": turn.id, "status": "sealed", "draft_count": 4}
        with pytest.raises(RuntimeError, match="media_batch_sealed"):
            await store.create_media_draft(owner, thread.id,
                MediaDraftCreate(kind="image", model="test-model", turn_id=turn.id,
                    idempotency_key="media-group-after-seal"),
                expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        claimed = await store.claim_media_jobs("worker", limit=10)
        assert {item["id"] for item in claimed} == {job.id for job in jobs}
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        generations={item["id"]:item["generation"] for item in claimed}
        for index, job in enumerate(jobs[:-1]):
            await store.update_media_job(job.id, "worker", generations[job.id],
                status="succeeded", phase="committed",
                output_object_ref="sha256:" + str(index + 1) * 64,
                output_mime="image/png" if job.kind == "image" else "video/mp4", output_size=10)
        ready, group = await store.media_continuation_group(turn.id)
        assert ready is False and len(group) == 4
        await store.update_media_job(jobs[-1].id, "worker", generations[jobs[-1].id],
            status="failed", phase="failed",
            error_code="provider_processing_failed", error_detail="provider failed")
        ready, group = await store.media_continuation_group(turn.id)
        assert ready is True
        failed = next(item for item in group if item["status"] == "failed")
        assert failed["error_detail"] == "provider failed" and failed["error_guidance"]
        assert [item["status"] for item in group].count("succeeded") == 3
        assert [item["status"] for item in group].count("failed") == 1
        assert await store.current_project_files(thread.id, owner) == []
        output = await store.media_job_output_for_thread(jobs[0].id, thread.id)
        assert output and output["mime"] == "image/png" and output["size"] == 10
        promoted = await store.promote_media_job_for_thread(jobs[0].id, thread.id, "workspace")
        repeated = await store.promote_media_job_for_thread(jobs[0].id, thread.id, "workspace")
        assert promoted == repeated
        assert [item.path for item in await store.current_project_files(thread.id, owner)] == [promoted["project_path"]]
        with pytest.raises(RuntimeError, match="media_job_output_unavailable"):
            await store.promote_media_job_for_thread(jobs[-1].id, thread.id, "delivery")
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_cancelled_media_only_batch_still_becomes_ready(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "media-cancelled-group.db"))
    await store.initialize()
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Cancelled media"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="generate", idempotency_key="cancelled-group-turn"), profile, owner)
        spec = SimpleNamespace(provider="ark", adapter="media", execution="async", cancellation="unsupported")
        draft, _ = await store.create_media_draft(owner, thread.id,
            MediaDraftCreate(kind="image", model="test-model", turn_id=turn.id,
                idempotency_key="cancelled-group-draft"),
            expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        await store.cancel_media_draft(draft.id, owner)
        await store.seal_media_batch(turn.id, thread.id, owner)
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        ready, group = await store.media_continuation_group(turn.id)
        assert ready is True and group[0]["status"] == "cancelled" and not group[0]["job_id"]
        assert not media_group_has_executed_job(group)
        assert not media_group_needs_agent_continuation(group)
        claimed = await store.claim_media_batches("dispatcher")
        assert claimed == [{"turn_id": turn.id, "thread_id": thread.id, "owner_id": owner}]
        await store.finish_media_batch(turn.id, "dispatcher", delivered=True)
        assert await store.claim_media_batches("dispatcher") == []
    finally:
        await store.close()


def test_expired_media_group_requires_agent_continuation_without_a_job():
    group = [{"draft_id": "draft", "job_id": "", "status": "expired"}]
    assert not media_group_has_executed_job(group)
    assert media_group_needs_agent_continuation(group)


def test_media_worker_corrects_bad_provider_mime_from_file_signature():
    assert MediaWorker._validated_mime(b"ID3" + b"\0" * 20, "application/x-tar", "audio") == "audio/mpeg"
    assert MediaWorker._validated_mime(b"\0\0\0\x18ftypmp42" + b"\0" * 12,
        "application/octet-stream", "video") == "video/mp4"
    assert MediaWorker._validated_mime(b"\xff\xd8\xff" + b"\0" * 20,
        "application/octet-stream", "image") == "image/jpeg"
    with pytest.raises(ValueError, match="mime_mismatch"):
        MediaWorker._validated_mime(b"not media", "application/x-tar", "audio")


def test_media_worker_extracts_single_audio_member_from_provider_tar():
    audio = b"ID3" + b"\0" * 32
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as value:
        member = tarfile.TarInfo("result/audio.mp3"); member.size = len(audio)
        value.addfile(member, io.BytesIO(audio))
        metadata = b"timestamps"; member = tarfile.TarInfo("result/audio.titles"); member.size = len(metadata)
        value.addfile(member, io.BytesIO(metadata))
    payload, mime = MediaWorker._unwrap_provider_media(
        archive.getvalue(), "application/x-tar", "audio", 1024)
    assert payload == audio and mime == "audio/mpeg"
    assert MediaWorker._validated_mime(payload, mime, "audio") == "audio/mpeg"


@pytest.mark.asyncio
async def test_tripo_multi_output_commit_is_atomic_and_recoverable(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "tripo-output.db"))
    await store.initialize()
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="3D"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="make a model", idempotency_key="tripo-turn"), profile, owner)
        spec = SimpleNamespace(provider="tripo", adapter="tripo_v3", execution="async",
            cancellation="unsupported")
        draft, _ = await store.create_media_draft(owner, thread.id,
            MediaDraftCreate(kind="model3d", model="v3.1-20260211", turn_id=turn.id,
                generation_mode="text_to_model", parameters={"prompt": "cube"},
                idempotency_key="tripo-draft", output_role="workspace"),
            expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        job = await store.confirm_media_draft(draft.id, owner, provider_spec=spec)
        claimed = (await store.claim_media_jobs("tripo-worker", adapter="tripo_v3"))[0]
        assert claimed["thread_id"] == thread.id and claimed["output_role"] == "workspace"
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        with pytest.raises(RuntimeError, match="active_thread"):
            await store.delete_thread(thread.id, owner)
        outputs = [{"role": "primary", "filename": "model.glb",
            "object_ref": "sha256:" + "1" * 64, "mime": "model/gltf-binary", "size": 400},
            {"role": "preview", "filename": "preview.png",
                "object_ref": "sha256:" + "2" * 64, "mime": "image/png", "size": 200}]
        assert await store.complete_media_job(job.id, "tripo-worker",
            provider_job_id="provider-task", outputs=outputs, credits_consumed=10)
        completed = await store.get_media_job(job.id, owner)
        assert completed and completed.status == "succeeded" and completed.credits_consumed == 10
        assert len(completed.outputs) == len(completed.artifact_ids) == 2
        primary=await store.media_job_output_for_thread(job.id,thread.id)
        assert primary=={"object_ref":"sha256:"+"1"*64,"mime":"model/gltf-binary",
            "size":400,"filename":"model.glb"}
        assert await store.resolve_tripo_input_job(job.id, job.id) == "provider-task"
        files = await store.current_project_files(thread.id, owner)
        assert {item.path for item in files} == {
            f"artifacts/3d/{job.id}/model.glb", f"artifacts/3d/{job.id}/preview.png"}
        _, group = await store.media_continuation_group(turn.id)
        assert group[0]["project_path"] == f"artifacts/3d/{job.id}/model.glb"
        promoted = await store.promote_media_job_for_thread(job.id, thread.id, "delivery")
        assert promoted["project_path"] == f"artifacts/3d/{job.id}/model.glb"
        async with store.connections() as db:
            assert len((await db.execute(__import__('sqlalchemy').select(MediaJobOutputRow))).scalars().all()) == 2
            items = (await db.execute(__import__('sqlalchemy').select(ItemRow).where(
                ItemRow.type == "artifact"))).scalars().all()
        assert {item.summary for item in items} == {"model.glb", "preview.png"}
        with pytest.raises(RuntimeError,match="active_media_prevents_thread_delete"):
            await store.delete_thread(thread.id,owner)
        batch=(await store.claim_media_batches("dispatcher"))[0]
        await store.finish_media_batch(batch["turn_id"],"dispatcher",delivered=True)
        assert await store.delete_thread(thread.id, owner) == [turn.id]
        async with store.connections() as db:
            assert not (await db.execute(__import__('sqlalchemy').select(MediaJobOutputRow))).scalars().all()
    finally:
        await store.close()
def test_media_worker_rejects_ambiguous_provider_audio_tar():
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as value:
        for name in ("one.mp3", "two.wav"):
            payload = b"ID3"; member = tarfile.TarInfo(name); member.size = len(payload)
            value.addfile(member, io.BytesIO(payload))
    with pytest.raises(ValueError, match="one_audio_file"):
        MediaWorker._unwrap_provider_media(archive.getvalue(), "application/x-tar", "audio", 1024)


def test_media_draft_rejects_malformed_turn_id():
    with pytest.raises(ValueError):
        MediaDraftCreate(kind="image", model="seedream-5.0", parameters={"prompt": "test"},
            idempotency_key="media-key-bad-turn", turn_id="not-a-turn")


def test_initialize_does_not_migrate_media_reference_columns(tmp_path):
    import asyncio
    import sqlite3
    database=tmp_path/"media-reference-migration.db";url="sqlite+aiosqlite:///"+str(database)
    async def create():
        store=Store(url);await store.initialize();await store.close()
    asyncio.run(create())
    with sqlite3.connect(database) as connection:
        for table in ("media_drafts","media_jobs"):
            connection.execute(f"ALTER TABLE {table} DROP COLUMN generation_mode")
            connection.execute(f"ALTER TABLE {table} DROP COLUMN references_json")
    asyncio.run(create())
    with sqlite3.connect(database) as connection:
        for table in ("media_drafts","media_jobs"):
            columns={row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
            assert not {"generation_mode","references_json"} & columns
