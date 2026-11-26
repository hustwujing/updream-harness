import asyncio

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate, TurnState
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.job_attempts import JobAttemptsTransactions


def test_job_candidates_round_robin_owners_by_oldest_work():
    rows=[("a1","owner-a",1),("a2","owner-a",2),("a3","owner-a",3),
        ("b1","owner-b",4),("c1","owner-c",5)]
    assert JobAttemptsTransactions._fair_job_identities(rows)==[
        "a1","b1","c1","a2","a3"]


async def test_turn_owner_quota_does_not_block_another_owner(tmp_path):
    store=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"owners.db"),namespace="owners")
    await store.initialize();catalog=HarnessCatalog();profile=catalog.resolve("codex","gpt-5.6","standard")
    async def create(owner,index):
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title=f"owner {index}"),profile,owner)
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message="work",
            idempotency_key=f"owner-turn-{index}"),profile,owner)
        return turn
    try:
        first=await create("owner-a",1);second=await create("owner-a",2)
        other=await create("owner-b",3)
        assert await store.claim_turn(first.id,"worker-one",30,owner_limit=1)
        assert await store.claim_turn(second.id,"worker-two",30,owner_limit=1) is None
        assert await store.claim_turn(other.id,"worker-two",30,owner_limit=1)
        await store.set_turn_state(first.id,TurnState.COMPLETED,lease_owner="worker-one",
            recovery_generation=1)
        await store.release_turn_lease(first.id,"worker-one",1)
        assert await store.claim_turn(second.id,"worker-two",30,owner_limit=1)
    finally:await store.close()


async def test_turn_owner_quota_claims_oldest_even_when_tasks_arrive_reversed(tmp_path):
    store=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"oldest.db"),namespace="owners")
    await store.initialize();catalog=HarnessCatalog();profile=catalog.resolve("codex","gpt-5.6","standard")
    turns=[]
    try:
        for index in range(3):
            thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
                reasoning_level="standard",title=f"oldest {index}"),profile,"owner")
            value,_,_=await store.create_turn(thread.id,TurnCreate(message="work",
                idempotency_key=f"oldest-turn-{index}"),profile,"owner")
            turns.append(value)
            await asyncio.sleep(.002)
        results=await asyncio.gather(*(store.claim_turn(value.id,f"worker-{index}",30,
            owner_limit=2) for index,value in reversed(list(enumerate(turns)))))
        claimed={value.id for value in results if value}
        assert claimed=={turns[0].id,turns[1].id}
        assert turns[2].id not in claimed
    finally:await store.close()
