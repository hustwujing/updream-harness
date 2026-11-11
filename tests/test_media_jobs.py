from datetime import timedelta
from types import SimpleNamespace

import pytest

from sandbox_harness.domain import MediaDraftCreate, now
from sandbox_harness.store import MediaOutboxRow, Store


@pytest.mark.asyncio
async def test_media_draft_confirm_is_idempotent_and_snapshots_provider(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    await store.initialize()
    try:
        owner = "a" * 64
        value = MediaDraftCreate(kind="video", model="sed2", parameters={"duration": 5}, idempotency_key="media-key-1")
        spec = SimpleNamespace(provider="ark", adapter="ark_video_v2", execution="async", cancellation="unsupported")
        draft, created = await store.create_media_draft(owner, "b" * 32, value,
            expires_at=now() + timedelta(minutes=10), provider_spec=spec)
        assert created and draft.status == "awaiting_confirmation"
        first = await store.confirm_media_draft(draft.id, owner, provider_spec=spec)
        second = await store.confirm_media_draft(draft.id, owner, provider_spec=spec)
        assert first.id == second.id and first.provider == "ark"
        assert not first.cancelable
        claimed = await store.claim_media_jobs("worker")
        await store.update_media_job(first.id, "worker", status="succeeded", phase="committed")
        async with store.connections() as db:
            rows = (await db.execute(__import__('sqlalchemy').select(MediaOutboxRow))).scalars().all()
        assert len(rows) == 1 and rows[0].status == "pending"
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
