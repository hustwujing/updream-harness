import json
import logging

import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, TurnInputCreate, TurnState)
from sandbox_harness.common.contracts.public_errors import (external_error_detail)
from sandbox_harness.services.platform_worker.job_continuation import (ContinuationDispatcher)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
from test_support.legacy_store import (Store)
import sandbox_harness.common.observability.observability as obs


class StoreBroker:
    def __init__(self,store):self.store=store;self.catalog=HarnessCatalog();self.submissions=[]
    async def submit(self,thread_id,value,network_access='restricted',internal=False):
        thread=await self.store.get_thread(thread_id);profile=self.catalog.resolve(thread.harness,thread.model,thread.reasoning_level)
        turn,created,_=await self.store.create_turn(thread_id,value,profile,thread.owner_id,
            network_access=network_access,internal=internal)
        self.submissions.append((turn.id,created,value.idempotency_key,value.message))
        return turn


async def setup(tmp_path,*,active_origin=False,traceparent=""):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'continuation.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Continuation'),profile,'owner')
    origin,_,_=await store.create_turn(thread.id,TurnCreate(message='start',
        idempotency_key='origin-turn-key'),profile,'owner',traceparent=traceparent)
    if not active_origin:
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        assert await store.claim_next_turn(thread.id,origin.id) is None
    return store,profile,thread,origin


def telemetry_rows(value):
    assert value.flush(5)
    return [json.loads(line) for path in value.sink.root.glob("*.telemetry.jsonl")
        for line in path.read_text().splitlines()]


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
        assert "sandbox-artifact fetch --artifact-id ID --output PATH" in dispatcher.broker.submissions[-1][3]
        assert "do not rerun a successful Job" in dispatcher.broker.submissions[-1][3]
        turns=await store.list_turns(thread.id,'owner')
        continuation=next(value for value in turns if value.internal)
        assert [value.id for value in turns]==[origin.id,user.id,continuation.id]
        await store.set_turn_state(user.id,TurnState.COMPLETED)
        assert (await store.claim_next_turn(thread.id,user.id)).id==continuation.id
    finally:await store.close()


async def test_continuation_dispatch_links_durable_origin_trace(tmp_path,monkeypatch):
    telemetry=obs.Telemetry("continuation-test",tmp_path/"logs")
    monkeypatch.setattr(obs,"_PROVIDER",telemetry.provider)
    monkeypatch.setattr(obs,"_TELEMETRY",telemetry)
    root=logging.getLogger();old_level=root.level;root.setLevel(logging.INFO);root.addHandler(telemetry.handler)
    store=None
    try:
        with obs.operation("test.origin.submit",new_trace=True):
            expected=obs.carrier()["traceparent"]
            store,_,thread,origin=await setup(tmp_path,traceparent=expected)
        group,_=await terminal_group(store,thread,origin,"linked")
        claimed=await store.claim_job_outbox_batch("inspect")
        assert claimed[0]["traceparent"]==expected
        await store.finish_job_outbox_batch([claimed[0]["outbox_id"]],"inspect",
            continuation_turn_id="",delivered=False)

        assert await ContinuationDispatcher(store,StoreBroker(store)).run_once()==1
        span=next(row for row in telemetry_rows(telemetry)
            if row["signal"]=="span" and row["event_name"]=="job.continuation.dispatch")
        assert span["trace_id"]!=expected.split("-")[1]
        assert span["links"]==[{
            "trace_id":expected.split("-")[1],"span_id":expected.split("-")[2]}]
        assert (await store.get_job_outbox(group.id)).state=="delivered"
    finally:
        root.removeHandler(telemetry.handler);root.setLevel(old_level);telemetry.close()
        if store is not None:await store.close()


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


async def test_failed_async_job_continuation_carries_safe_reason_and_guidance(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        request=JobRequest(local_job_id='image',kind=JobKind.PROVIDER,
            adapter='image.generate',descriptor_id='image.generate',descriptor_version='1',
            descriptor_digest='a'*64,idempotency_key='failed-image-job')
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=origin.id,idempotency_key='failed-image-group',jobs=[request]))
        await store.transition_job(jobs[0].id,JobState.FAILED,expected_generation=0,result={
            'error_code':'provider_input_invalid','error_detail':external_error_detail(
                'InvalidParameter','The parameter `size` must be WIDTHxHEIGHT; request id: hidden')})
        broker=StoreBroker(store)
        assert await ContinuationDispatcher(store,broker).run_once()==1
        message=broker.submissions[-1][3]
        assert 'The parameter `size` must be WIDTHxHEIGHT' in message
        assert '请检查素材格式、参数范围和引用关系' in message
        assert 'hidden' not in message
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
