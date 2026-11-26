from sandbox_harness.common.contracts.domain import (TurnCreate)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
from sandbox_harness.services.platform_worker.job_continuation import (ContinuationDispatcher)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.execution.job_adapters import (BaseJobAdapter)
from test_job_continuation import StoreBroker,setup,terminal_group


class NeverSubmittedAdapter(BaseJobAdapter):
    async def submit(self,context):raise AssertionError('cancelled Job must not submit')


async def test_group_cancel_suppresses_all_cancelled_wake(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        group,jobs=await terminal_group(store,thread,origin,'placeholder',suppress=True)
        assert (await store.get_job_outbox(group.id)).state=='suppressed'
        dispatcher=ContinuationDispatcher(store,StoreBroker(store))
        assert await dispatcher.run_once()==0
    finally:await store.close()


async def test_cancel_before_acceptance_is_terminal_without_adapter_submit(tmp_path):
    store,_,thread,origin=await setup(tmp_path)
    try:
        request=JobRequest(local_job_id='cancel-me',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='cancel-before-acceptance',parameters={})
        group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=origin.id,idempotency_key='cancel-before-group',jobs=[request]))
        cancelled=await store.request_job_group_cancel(group.id,'owner')
        assert cancelled[0].state==JobState.CANCELLING
        worker=JobWorker(store,{'fake':NeverSubmittedAdapter()},node_id='node',worker_id='worker')
        assert await worker.run_once()==1
        assert (await store.get_job(jobs[0].id)).state==JobState.CANCELLED
        assert (await store.get_job_outbox(group.id)).state=='suppressed'
    finally:await store.close()


async def test_thread_stop_all_suppresses_ready_and_attaches_facts_to_next_user_turn(tmp_path):
    store,profile,thread,origin=await setup(tmp_path)
    try:
        group,_=await terminal_group(store,thread,origin,'stopped-result')
        assert (await store.get_job_outbox(group.id)).state=='ready'
        await store.stop_all_jobs(thread.id,'owner')
        assert (await store.get_job_outbox(group.id)).state=='suppressed'
        public_message='use any useful finished result'
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message=public_message,
            idempotency_key='post-stop-user',delivery_mode='queue'),profile,'owner')
        assert await store.attach_suppressed_job_context(turn.id)==1
        restored=await store.get_turn(turn.id)
        assert restored.message==public_message
        assert restored.runtime_message.startswith('[suppressed_job_results]')
        assert restored.runtime_message.endswith(public_message)
        duplicate,created,_=await store.create_turn(thread.id,TurnCreate(message=public_message,
            idempotency_key='post-stop-user',delivery_mode='queue'),profile,'owner')
        assert not created and duplicate.id==turn.id
        item=next(value for value in await store.items_after(thread.id) if value.turn_id==turn.id)
        assert item.summary==public_message and 'suppressed_job_results' not in item.summary
        assert (await store.get_job_outbox(group.id)).state=='delivered'
        assert await ContinuationDispatcher(store,StoreBroker(store)).run_once()==0
    finally:await store.close()
