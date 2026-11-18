import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate, TurnInputCreate, TurnState
from sandbox_harness.job_continuation import ContinuationDispatcher
from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest, JobState
from sandbox_harness.store import Store


class StoreBroker:
    def __init__(self,store):self.store=store;self.catalog=HarnessCatalog();self.submissions=[]
    async def submit(self,thread_id,value,network_access='restricted',internal=False):
        thread=await self.store.get_thread(thread_id);profile=self.catalog.resolve(thread.harness,thread.model,thread.reasoning_level)
        turn,created,_=await self.store.create_turn(thread_id,value,profile,thread.owner_id,
            network_access=network_access,internal=internal)
        self.submissions.append((turn.id,created,value.idempotency_key,value.message))
        return turn


async def setup(tmp_path,*,active_origin=False):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'continuation.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Continuation'),profile,'owner')
    origin,_,_=await store.create_turn(thread.id,TurnCreate(message='start',
        idempotency_key='origin-turn-key'),profile,'owner')
    if not active_origin:
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await store.claim_next_turn(thread.id,origin.id) is None
    return store,profile,thread,origin


async def terminal_group(store,thread,origin,suffix,state=JobState.SUCCEEDED,suppress=False,jobs=1):
    requests=[JobRequest(local_job_id=f'{suffix}-{index}',kind=JobKind.OPERATION,adapter='fake',
        descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key=f'{suffix}-job-{index}',parameters={}) for index in range(jobs)]
    group,values=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=origin.id,idempotency_key=f'{suffix}-group-key',jobs=requests,
        suppress_continuation=suppress))
    for value in values:
        await store.transition_job(value.id,state,expected_generation=0,result={'value':value.local_job_id})
    return group,values


async def test_user_turn_queued_before_completion_precedes_factual_continuation(tmp_path):
    store,profile,thread,origin=await setup(tmp_path,active_origin=True)
    try:
        user,_,_=await store.create_turn(thread.id,TurnCreate(message='change direction',
            idempotency_key='queued-user-key',delivery_mode='queue'),profile,'owner')
        await terminal_group(store,thread,origin,'later')
        dispatcher=ContinuationDispatcher(store,StoreBroker(store))
        assert await dispatcher.run_once()==0
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await dispatcher.run_once()==0
        assert (await store.claim_next_turn(thread.id,origin.id)).id==user.id
        assert await dispatcher.run_once()==1
        assert "result_available=true" in dispatcher.broker.submissions[-1][3]
        assert "sandbox-job job --job-id JOB_ID" in dispatcher.broker.submissions[-1][3]
        turns=await store.list_turns(thread.id,'owner')
        continuation=next(value for value in turns if value.internal)
        assert [value.id for value in turns]==[origin.id,user.id,continuation.id]
        await store.set_turn_state(user.id,TurnState.COMPLETED)
        assert (await store.claim_next_turn(thread.id,user.id)).id==continuation.id
    finally:await store.close()


async def test_continuation_snapshots_inputs_only_after_origin_settles(tmp_path):
    store,_,thread,origin=await setup(tmp_path,active_origin=True)
    try:
        changed,_=await store.create_turn_input(origin.id,TurnInputCreate(
            message='改为rocket在左、portrait在右。',
            idempotency_key='reorder-before-continuation'),thread.owner_id)
        assert await store.set_input_status(changed.id,'delivered')
        await terminal_group(store,thread,origin,'early-result')
        broker=StoreBroker(store);dispatcher=ContinuationDispatcher(store,broker)
        assert await dispatcher.run_once()==0
        await store.apply_delivered_inputs(origin.id)
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await store.claim_next_turn(thread.id,origin.id) is None
        assert await dispatcher.run_once()==1
        message=broker.submissions[-1][3]
        assert '改为rocket在左、portrait在右。' in message
        assert '"applied_inputs":[]' not in message
    finally:await store.close()


async def test_continuation_carries_authoritative_applied_user_inputs(tmp_path):
    store,_,thread,origin=await setup(tmp_path,active_origin=True)
    try:
        changed,_=await store.create_turn_input(origin.id,TurnInputCreate(
            message='把最终尺寸改为256x128，并同时交付PNG和WebP。',
            idempotency_key='resize-format-change'),thread.owner_id)
        assert await store.set_input_status(changed.id,'applied')
        await terminal_group(store,thread,origin,'changed-result')
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await store.claim_next_turn(thread.id,origin.id) is None
        broker=StoreBroker(store);dispatcher=ContinuationDispatcher(store,broker)
        assert await dispatcher.run_once()==1
        message=broker.submissions[-1][3]
        assert '[authoritative_user_inputs]' in message
        assert '把最终尺寸改为256x128，并同时交付PNG和WebP。' in message
        assert 'These program-recorded inputs are authoritative' in message
    finally:await store.close()


async def test_crash_after_turn_insert_replays_exactly_one_continuation(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        group,_=await terminal_group(store,thread,origin,'crash')
        calls=0
        async def crash(turn,batch):
            nonlocal calls;calls+=1
            if calls==1:raise RuntimeError('crash_after_turn_insert')
        broker=StoreBroker(store);dispatcher=ContinuationDispatcher(store,broker,after_submit=crash)
        with pytest.raises(RuntimeError,match='crash_after_turn_insert'):
            await dispatcher.run_once()
        new_group,_=await terminal_group(store,thread,origin,'arrived-after-crash')
        assert await dispatcher.run_once()==1
        internal=[value for value in await store.list_turns(thread.id,'owner') if value.internal]
        assert len(internal)==1
        assert broker.submissions[0][0]==broker.submissions[1][0]
        outbox=await store.get_job_outbox(group.id)
        assert outbox.state=='delivered' and outbox.continuation_turn_id==internal[0].id
        assert (await store.get_job_outbox(new_group.id)).state=='ready'
        assert await dispatcher.run_once()==1
        assert len([value for value in await store.list_turns(thread.id,'owner') if value.internal])==2
    finally:await store.close()


async def test_batch_respects_group_and_job_limits(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        for index in range(3):await terminal_group(store,thread,origin,f'batch-{index}',jobs=2)
        dispatcher=ContinuationDispatcher(store,StoreBroker(store),max_groups=2,max_jobs=4)
        assert await dispatcher.run_once()==2
        assert await dispatcher.run_once()==1
        assert len([value for value in await store.list_turns(thread.id,'owner') if value.internal])==2
    finally:await store.close()


async def test_oversized_job_results_use_bounded_outbox_facts_without_suppression(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        requests=[JobRequest(local_job_id=f"large-{index}",kind=JobKind.OPERATION,
            adapter="fake",descriptor_id="fake",descriptor_version="1",descriptor_digest="a"*64,
            idempotency_key=f"large-result-{index}") for index in range(3)]
        group,jobs=await store.create_job_group("owner",JobGroupCreate(thread_id=thread.id,
            origin_turn_id=origin.id,idempotency_key="large-result-group",jobs=requests))
        for job in jobs:
            await store.transition_job(job.id,JobState.SUCCEEDED,expected_generation=0,
                result={"payload":"x"*(700*1024),"stable":"fact"})
        broker=StoreBroker(store);dispatcher=ContinuationDispatcher(store,broker,max_groups=1,
            max_jobs=10,max_bytes=1024*1024)
        assert await dispatcher.run_once()==1
        message=broker.submissions[-1][3]
        assert len(message.encode())<1024*1024
        assert "result_available=true" in message
        assert "sandbox-job group --group-id GROUP_ID" in message
        assert "x"*100 not in message
        assert (await store.get_job_outbox(group.id)).state=="delivered"
    finally:await store.close()
