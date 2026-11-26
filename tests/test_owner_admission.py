from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate
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
        await store.release_turn_lease(first.id,"worker-one",1)
        assert await store.claim_turn(second.id,"worker-two",30,owner_limit=1)
    finally:await store.close()
