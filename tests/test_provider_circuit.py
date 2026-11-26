import asyncio
from datetime import timedelta

from sqlalchemy import update

from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.db.core import CoreRepository


async def test_provider_circuit_opens_and_allows_one_half_open_probe(tmp_path):
    url="sqlite+aiosqlite:///"+str(tmp_path/"circuit.db")
    first=CoreRepository(url,namespace="circuit");second=CoreRepository(url,namespace="circuit")
    await first.initialize()
    key="aliyun:profile-a"
    try:
        assert await first.allow_provider_create(key,"worker-one",probe_seconds=10)
        assert not await first.record_provider_failure(key,threshold=2,open_seconds=30)
        assert await first.record_provider_failure(key,threshold=2,open_seconds=30)
        assert not await second.allow_provider_create(key,"worker-two",probe_seconds=10)
        from sandbox_harness.common.db.models import ProviderCircuitRow
        async with first.connections.begin() as db:
            await db.execute(update(ProviderCircuitRow).where(ProviderCircuitRow.id==key).values(
                opened_until=now()-timedelta(seconds=1)))
        results=await asyncio.gather(
            first.allow_provider_create(key,"worker-one",probe_seconds=10),
            second.allow_provider_create(key,"worker-two",probe_seconds=10))
        assert sorted(results)==[False,True]
        winner="worker-one" if results[0] else "worker-two"
        assert await first.record_provider_success(key,winner)
        assert await second.allow_provider_create(key,"worker-two",probe_seconds=10)
        status=await first.provider_circuit_status(key)
        assert status["state"]=="closed" and status["failure_count"]==0
    finally:await first.close();await second.close()


async def test_provider_circuit_probe_failure_reopens(tmp_path):
    store=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"probe.db"),namespace="circuit")
    await store.initialize();key="aliyun:profile-b"
    try:
        await store.record_provider_failure(key,threshold=1,open_seconds=1)
        from sandbox_harness.common.db.models import ProviderCircuitRow
        async with store.connections.begin() as db:
            await db.execute(update(ProviderCircuitRow).where(ProviderCircuitRow.id==key).values(
                opened_until=now()-timedelta(seconds=1)))
        assert await store.allow_provider_create(key,"probe",probe_seconds=10)
        assert await store.record_provider_failure(key,threshold=1,open_seconds=30,owner="probe")
        assert not await store.allow_provider_create(key,"other",probe_seconds=10)
    finally:await store.close()
