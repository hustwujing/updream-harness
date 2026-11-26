import asyncio

import pytest

from sandbox_harness.common.db.core import CoreRepository


async def test_usage_ledger_is_exact_across_concurrent_replay(tmp_path):
    store=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"usage.db"),namespace="usage")
    await store.initialize()
    values=dict(idempotency_key="model-request:one",owner_id="owner",thread_id="thread",
        turn_id="turn",job_id="",kind="model_tokens",amount=123,unit="token",
        source="gateway")
    try:
        rows=await asyncio.gather(*(store.record_usage(**values) for _ in range(10)))
        assert sum(value["created"] for value in rows)==1
        totals=await store.usage_totals("owner")
        assert totals==[{"kind":"model_tokens","unit":"token","amount":123}]
        with pytest.raises(ValueError,match="usage_identity_conflict"):
            await store.record_usage(**{**values,"amount":124})
    finally:await store.close()


async def test_usage_ledger_separates_kinds_and_owners(tmp_path):
    store=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"usage-kinds.db"),namespace="usage")
    await store.initialize()
    try:
        await store.record_usage(idempotency_key="artifact:one",owner_id="owner-a",
            thread_id="thread",turn_id="turn",job_id="job",kind="object_bytes",
            amount=4096,unit="byte",source="artifact")
        await store.record_usage(idempotency_key="sandbox:one",owner_id="owner-a",
            thread_id="thread",turn_id="turn",job_id="",kind="sandbox_seconds",
            amount=30,unit="second",source="broker")
        await store.record_usage(idempotency_key="artifact:other",owner_id="owner-b",
            thread_id="other",turn_id="other",job_id="job",kind="object_bytes",
            amount=999,unit="byte",source="artifact")
        assert await store.usage_totals("owner-a")==[
            {"kind":"object_bytes","unit":"byte","amount":4096},
            {"kind":"sandbox_seconds","unit":"second","amount":30},
        ]
    finally:await store.close()


async def test_media_usage_handoff_is_idempotent_in_core_ledger(tmp_path):
    from sandbox_harness.common.db.media_repository import MediaRepository
    from sandbox_harness.common.db.models import canonical_digest
    core=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"usage-core.db"),namespace="usage")
    media=MediaRepository("sqlite+aiosqlite:///"+str(tmp_path/"usage-media.db"))
    await core.initialize();await media.initialize()
    payload={"idempotency_key":"provider-operation:"+"a"*32,"owner_id":"owner",
        "thread_id":"b"*32,"turn_id":"c"*32,"job_id":"a"*32,
        "kind":"provider_operations","amount":1,"unit":"operation","source":"ark"}
    try:
        async with media.transaction() as db:
            await media.handoff.enqueue(db,command_id=canonical_digest(["usage","one"]),
                target="core",kind="usage.record",aggregate_id="a"*32,owner_id="owner",
                generation=1,payload=payload)
        assert (await media.handoff.relay({"core":core.handoff})).delivered==1
        assert (await media.handoff.relay({"core":core.handoff})).delivered==0
        assert await core.usage_totals("owner")==[{
            "kind":"provider_operations","unit":"operation","amount":1}]
    finally:await media.close();await core.close()
