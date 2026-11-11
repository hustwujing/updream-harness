from datetime import timedelta
from types import SimpleNamespace

import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, MediaDraftCreate, ThreadCreate, TurnCreate, now
from sandbox_harness.media_worker import MediaWorker
from sandbox_harness.store import ArtifactRow, MediaOutboxRow, Store


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
            idempotency_key="media-key-1", turn_id=turn.id)
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
        assert not first.cancelable
        ready, _ = await store.media_continuation_group(turn.id)
        assert ready is False
        cancelled = await store.cancel_media_draft(cancellable.id, owner)
        assert cancelled.status == "cancelled"
        assert {item.id for item in await store.list_media_drafts(thread.id, owner)} == {draft.id, cancellable.id}
        claimed = await store.claim_media_jobs("worker")
        assert claimed[0]["id"] == first.id
        await store.update_media_job(first.id, "worker", status="succeeded", phase="committed",
            output_object_ref="sha256:" + "1" * 64, output_mime="video/mp4", output_size=1234)
        ready, group = await store.media_continuation_group(turn.id)
        assert ready is True and [item["id"] for item in group] == [first.id]
        assert group[0]["project_path"] == f"artifacts/media/{first.id}.mp4"
        project_files = await store.current_project_files(thread.id, owner)
        assert [item.path for item in project_files] == [f"artifacts/media/{first.id}.mp4"]
        completed = await store.get_media_job(first.id, owner)
        listed = await store.list_media_jobs(thread.id, owner)
        assert completed and len(completed.artifact_ids) == 1
        assert listed[0].artifact_ids == completed.artifact_ids
        async with store.connections() as db:
            rows = (await db.execute(__import__('sqlalchemy').select(MediaOutboxRow))).scalars().all()
            artifacts = (await db.execute(__import__('sqlalchemy').select(ArtifactRow))).scalars().all()
        assert len(rows) == 1 and rows[0].status == "pending"
        assert len(artifacts) == 1 and artifacts[0].id == completed.artifact_ids[0]
        assert artifacts[0].object_ref == "sha256:" + "1" * 64
        with pytest.raises(KeyError, match="media_turn_not_found"):
            await store.create_media_draft(owner, "b" * 32,
                value.model_copy(update={"idempotency_key": "media-key-cross-thread"}),
                expires_at=now() + timedelta(minutes=10), provider_spec=spec)
    finally:
        await store.close()


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


def test_media_worker_corrects_bad_provider_mime_from_file_signature():
    assert MediaWorker._validated_mime(b"ID3" + b"\0" * 20, "application/x-tar", "audio") == "audio/mpeg"
    assert MediaWorker._validated_mime(b"\0\0\0\x18ftypmp42" + b"\0" * 12,
        "application/octet-stream", "video") == "video/mp4"
    assert MediaWorker._validated_mime(b"\xff\xd8\xff" + b"\0" * 20,
        "application/octet-stream", "image") == "image/jpeg"
    with pytest.raises(ValueError, match="mime_mismatch"):
        MediaWorker._validated_mime(b"not media", "application/x-tar", "audio")


def test_media_draft_rejects_malformed_turn_id():
    with pytest.raises(ValueError):
        MediaDraftCreate(kind="image", model="seedream-5.0", parameters={"prompt": "test"},
            idempotency_key="media-key-bad-turn", turn_id="not-a-turn")
