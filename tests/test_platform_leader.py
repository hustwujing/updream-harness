import asyncio
from datetime import timedelta

from sqlalchemy import update

from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.db.core import CoreRepository


async def stores(tmp_path):
    url="sqlite+aiosqlite:///"+str(tmp_path/"platform-leader.db")
    first=CoreRepository(url,namespace="platform-leader")
    second=CoreRepository(url,namespace="platform-leader")
    await first.initialize()
    return first,second


async def test_platform_leader_is_fenced_across_process_connections(tmp_path):
    first,second=await stores(tmp_path)
    try:
        one=await first.claim_platform_leader("platform-coordinator","worker-one",30)
        assert one==1
        assert await second.claim_platform_leader(
            "platform-coordinator","worker-two",30) is None
        assert await first.renew_platform_leader(
            "platform-coordinator","worker-one",one,30)
        assert not await second.release_platform_leader(
            "platform-coordinator","worker-two",one)
        assert await first.release_platform_leader(
            "platform-coordinator","worker-one",one)
        two=await second.claim_platform_leader("platform-coordinator","worker-two",30)
        assert two==2
        assert not await first.renew_platform_leader(
            "platform-coordinator","worker-one",one,30)
    finally:
        await first.close();await second.close()


async def test_expired_platform_leader_has_exactly_one_successor(tmp_path):
    from sandbox_harness.common.db.models import PlatformLeaderRow

    first,second=await stores(tmp_path)
    third=CoreRepository(first.url,namespace="platform-leader")
    try:
        assert await first.claim_platform_leader(
            "platform-coordinator","dead-worker",30)==1
        async with first.connections.begin() as db:
            await db.execute(update(PlatformLeaderRow).where(
                PlatformLeaderRow.id=="platform-coordinator").values(
                    expires_at=now()-timedelta(seconds=1)))
        results=await asyncio.gather(
            second.claim_platform_leader("platform-coordinator","worker-two",30),
            third.claim_platform_leader("platform-coordinator","worker-three",30),
        )
        assert sorted(value for value in results if value is not None)==[2]
        status=await first.platform_leader_status("platform-coordinator")
        assert status["generation"]==2
        assert status["owner"] in {"worker-two","worker-three"}
    finally:
        await first.close();await second.close();await third.close()
