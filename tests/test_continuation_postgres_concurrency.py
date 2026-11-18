import asyncio
import os

import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate,TurnState,identifier
from sandbox_harness.job_continuation import ContinuationDispatcher
from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
from sandbox_harness.store import Store
from test_job_continuation import StoreBroker


POSTGRES_URL=os.environ.get('PARALLEL_TEST_POSTGRES_URL','')
pytestmark=pytest.mark.skipif(not POSTGRES_URL,reason='PARALLEL_TEST_POSTGRES_URL is not configured')


async def test_concurrent_dispatchers_create_one_continuation_turn():
    primary=Store(POSTGRES_URL);await primary.initialize()
    stores=[Store(POSTGRES_URL) for _ in range(8)]
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        owner='continuation-'+identifier()
        thread=await primary.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
            reasoning_level='standard',title='PG continuation'),profile,owner)
        origin,_,_=await primary.create_turn(thread.id,TurnCreate(message='start',
            idempotency_key='origin-'+identifier()),profile,owner)
        request=JobRequest(local_job_id='one',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='job-'+identifier(),parameters={})
        group,jobs=await primary.create_job_group(owner,JobGroupCreate(thread_id=thread.id,
            origin_turn_id=origin.id,idempotency_key='group-'+identifier(),jobs=[request]))
        await primary.transition_job(jobs[0].id,JobState.SUCCEEDED,expected_generation=0,result={'ok':True})
        await primary.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await primary.claim_next_turn(thread.id,origin.id) is None
        dispatchers=[ContinuationDispatcher(store,StoreBroker(store),owner='dispatcher-'+identifier())
            for store in stores]
        counts=await asyncio.gather(*(dispatcher.run_once() for dispatcher in dispatchers))
        assert sum(counts)==1
        internal=[turn for turn in await primary.list_turns(thread.id,owner) if turn.internal]
        assert len(internal)==1
        outbox=await primary.get_job_outbox(group.id)
        assert outbox.state=='delivered' and outbox.continuation_turn_id==internal[0].id
    finally:
        await asyncio.gather(*(store.close() for store in stores));await primary.close()
