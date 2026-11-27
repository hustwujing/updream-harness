from sandbox_harness.common.contracts.domain import (identifier)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, AdapterSubmission, BaseJobAdapter, PreAcceptFailure, SubmissionOutcomeUnknown)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from test_support.legacy_store import (JobAttemptRow, Store)
from sqlalchemy import select
import httpx
import pytest


class FakeAdapter(BaseJobAdapter):
    def __init__(self, submissions, statuses=()):
        self.submissions=list(submissions);self.statuses=list(statuses)
        self.preflight_calls=0;self.submit_calls=0;self.status_calls=0;self.reconcile_calls=0;self.cleanup_calls=0
        self.call_order=[]
    async def preflight(self,context):
        self.preflight_calls+=1;self.call_order.append('preflight')
    async def submit(self,context):
        self.submit_calls+=1;self.call_order.append('submit');value=self.submissions.pop(0)
        if isinstance(value,Exception):raise value
        return value
    async def status(self,context,external_execution_id):
        self.status_calls+=1
        value=self.statuses.pop(0) if self.statuses else AdapterStatus(False)
        if isinstance(value,Exception):raise value
        return value
    async def reconcile(self,context,external_execution_id):
        self.reconcile_calls+=1
        return self.statuses.pop(0)
    async def cleanup(self,context,external_execution_id):self.cleanup_calls+=1


async def test_full_worker_keeps_cleanup_progressing(tmp_path):
    import asyncio
    store,_,job=await setup(tmp_path,request('waiting'))
    waiting=asyncio.Event();cleaned=asyncio.Event()
    class Adapter(FakeAdapter):
        async def submit(self,context):
            waiting.set()
            raise PreAcceptFailure('capacity',infrastructure=True,retryable=True,
                retry_class='capacity_wait',retry_after_seconds=.02)
        async def cleanup(self,context,external_execution_id):cleaned.set()
    worker=JobWorker(store,{'fake':Adapter([])},node_id='node',concurrency=1)
    task=asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(waiting.wait(),2)
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
            origin_turn_id='c'*32,idempotency_key='completed-while-busy',jobs=[request('completed')]))
        attempt=await store.claim_job(jobs[0].id,'old','node',30)
        await store.record_job_acceptance(jobs[0].id,'old',attempt.generation,
            external_execution_id='completed-external',receipt={})
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,expected_generation=attempt.generation)
        await asyncio.wait_for(cleaned.wait(),2)
        assert (await store.get_job(job.id)).generation==1
    finally:
        worker.stop_event.set();await asyncio.wait_for(task,3);await store.close()


async def test_full_worker_recovers_accepted_execution_without_resubmission(tmp_path):
    import asyncio
    store,_,job=await setup(tmp_path,request('waiting'))
    waiting=asyncio.Event();recovered=asyncio.Event()
    class Adapter(FakeAdapter):
        async def submit(self,context):
            assert context.job.id==job.id
            waiting.set()
            raise PreAcceptFailure('capacity',infrastructure=True,retryable=True,
                retry_class='capacity_wait',retry_after_seconds=.02)
        async def status(self,context,external_execution_id):
            assert external_execution_id=='accepted-external'
            recovered.set();return AdapterStatus(True,JobState.SUCCEEDED,{'ok':True})
    worker=JobWorker(store,{'fake':Adapter([])},node_id='node',concurrency=1)
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
        origin_turn_id='c'*32,idempotency_key='accepted-before-restart',jobs=[request('accepted')]))
    old=await store.claim_job(jobs[0].id,'old','node',30)
    await store.record_job_acceptance(jobs[0].id,'old',old.generation,
        external_execution_id='accepted-external',receipt={})
    task=asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(waiting.wait(),2)
        await store.release_job_lease(jobs[0].id,'old',old.generation)
        await asyncio.wait_for(recovered.wait(),2)
        assert (await store.get_job(job.id)).generation==1
    finally:
        worker.stop_event.set();await asyncio.wait_for(task,3);await store.close()


async def test_recovery_lane_holds_one_generation_while_polling_accepted_job(tmp_path):
    import asyncio
    store,_,job=await setup(tmp_path,request('accepted-polling'))
    old=await store.claim_job(job.id,'old','node',30)
    await store.record_job_acceptance(job.id,'old',old.generation,
        external_execution_id='accepted-external',receipt={})
    await store.defer_accepted_job(job.id,'old',old.generation)
    adapter=FakeAdapter([],statuses=[AdapterStatus(False),AdapterStatus(False),
        AdapterStatus(True,JobState.SUCCEEDED,{'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='replacement',
        concurrency=1,recovery_concurrency=1,poll_seconds=.01)
    task=asyncio.create_task(worker.run())
    try:
        for _ in range(300):
            current=await store.get_job(job.id)
            if current.state==JobState.SUCCEEDED:break
            await asyncio.sleep(.01)
        assert current.state==JobState.SUCCEEDED
        assert current.generation==2 and adapter.status_calls==3
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job.id))).scalars())
        assert [row.generation for row in attempts]==[1,2]
    finally:
        worker.stop_event.set();await asyncio.wait_for(task,3);await store.close()


async def test_recovery_after_resume_keeps_newly_accepted_execution(tmp_path):
    from datetime import timedelta
    from sandbox_harness.common.contracts.domain import (now)
    from test_support.legacy_store import (PauseParticipantRow)
    store,_,job=await setup(tmp_path,request('resumed',kind=JobKind.HARNESS))
    before=now()-timedelta(seconds=1)
    async with store.connections.begin() as db:
        db.add(PauseParticipantRow(id=identifier(),owner_id='owner',thread_id=job.thread_id,
            generation=1,kind='child_harness',participant_id=job.id,state='RESUMED',
            external_execution_id='old-before-pause',created_at=before,updated_at=before))
    attempt=await store.claim_job(job.id,'old','node',30)
    await store.record_job_acceptance(job.id,'old',attempt.generation,
        external_execution_id='new-after-resume',receipt={})
    await store.defer_accepted_job(job.id,'old',attempt.generation)
    adapter=FakeAdapter([],statuses=[AdapterStatus(True,JobState.SUCCEEDED,{'ok':True})])
    try:
        worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='replacement')
        await worker.run_once()
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert adapter.submit_calls==0 and adapter.status_calls==1
    finally:await store.close()


@pytest.mark.parametrize('accepted_after_resume',[False,True])
async def test_recovery_claim_requires_current_acceptance_after_resume(tmp_path,accepted_after_resume):
    from datetime import timedelta
    from sandbox_harness.common.contracts.domain import (now)
    from test_support.legacy_store import (PauseParticipantRow)
    store,_,job=await setup(tmp_path,request('resumed',kind=JobKind.HARNESS))
    try:
        attempt=await store.claim_job(job.id,'old','node',30)
        await store.record_job_acceptance(job.id,'old',attempt.generation,
            external_execution_id='accepted-external',receipt={})
        await store.defer_accepted_job(job.id,'old',attempt.generation)
        timestamp=now()+timedelta(seconds=-1 if accepted_after_resume else 1)
        async with store.connections.begin() as db:
            db.add(PauseParticipantRow(id=identifier(),owner_id='owner',thread_id=job.thread_id,
                generation=1,kind='child_harness',participant_id=job.id,state='RESUMED',
                external_execution_id='accepted-external',created_at=timestamp,updated_at=timestamp))
        claims=await store.claim_available_jobs('recovery','node',30,accepted_only=True)
        assert [c.job_id for c in claims]==([job.id] if accepted_after_resume else [])
    finally:await store.close()


async def test_recovery_lane_never_submits_unaccepted_job(tmp_path):
    store,_,job=await setup(tmp_path,request('unaccepted'))
    adapter=FakeAdapter([]);worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='recovery')
    try:
        attempt=await store.claim_job(job.id,'recovery','node',30)
        await worker.process(attempt,recovery_only=True)
        assert adapter.submit_calls==0 and await store.latest_job_acceptance(job.id) is None
    finally:await store.close()


async def test_worker_query_failure_waits_for_owned_cleanup(monkeypatch):
    import asyncio
    entered=asyncio.Event();release=asyncio.Event()
    class StoreStub:
        async def claim_available_jobs(self,*args,**kwargs):raise OSError('database unavailable')
    worker=JobWorker(StoreStub(),{},node_id='node')
    async def cleanup(**kwargs):entered.set();await release.wait();return 0
    monkeypatch.setattr(worker,'run_cleanup_once',cleanup)
    task=asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(),1)
        assert not task.done(), 'Worker exited while its cleanup still owned resources'
        release.set()
        with pytest.raises(OSError):await task
    finally:release.set();await asyncio.gather(task,return_exceptions=True)


async def test_empty_adapter_registration_cannot_claim_jobs(tmp_path):
    store,_,job=await setup(tmp_path,request('unregistered'))
    try:
        assert await store.claim_available_jobs('empty','node',30,adapters=set())==[]
        assert (await store.get_job(job.id)).generation==0
    finally:await store.close()


def request(local_id='job',kind=JobKind.PROVIDER,**updates):
    value=dict(local_job_id=local_id,kind=kind,adapter='fake',descriptor_id='fake',
        descriptor_version='1',descriptor_digest='a'*64,idempotency_key=f'{local_id}-request-key',
        parameters={},retry_policy={'max_pre_accept_attempts':2},
        cancellation_policy={'supported':True},reconciliation_policy={'supported':True})
    value.update(updates);return JobRequest(**value)


async def setup(tmp_path,job_request,authorization_snapshot=None):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/(identifier()+'.db')));await store.initialize()
    group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
        origin_turn_id='c'*32,idempotency_key='group-'+identifier(),jobs=[job_request],
        authorization_snapshot=authorization_snapshot or {}))
    return store,group,jobs[0]


async def test_every_async_adapter_preflights_before_submission(tmp_path):
    store,_,job=await setup(tmp_path,request())
    adapter=FakeAdapter([AdapterSubmission('external-1',{'accepted':True},completed=True,
        result={'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,worker_id='worker')
    try:
        assert await worker.run_once()==1
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert adapter.preflight_calls==1 and adapter.call_order[:2]==['preflight','submit']
    finally:await store.close()


async def test_pre_accept_failure_retries_without_acceptance_or_budget(tmp_path):
    store,_,job=await setup(tmp_path,request())
    adapter=FakeAdapter([PreAcceptFailure('network before send'),
        AdapterSubmission('external-1',{'accepted':True},completed=True,result={'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,worker_id='worker')
    try:
        assert await worker.run_once()==1
        assert (await store.get_job(job.id)).state==JobState.QUEUED
        assert await store.latest_job_acceptance(job.id) is None
        assert await worker.run_once()==1
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert adapter.submit_calls==2 and adapter.cleanup_calls==1
    finally:await store.close()


async def test_pre_accept_failure_preserves_stable_domain_error_code(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        retry_policy={'max_pre_accept_attempts':1}))
    adapter=FakeAdapter([PreAcceptFailure('selector matched nothing',
        error_code='no_target_found')])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,worker_id='worker')
    try:
        assert await worker.run_once()==1
        failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED
        assert failed.result['error_code']=='no_target_found'
        assert failed.result['error_detail']=='selector matched nothing'
    finally:await store.close()


async def test_domain_payload_rejection_before_outbox_is_not_unknown_submission(tmp_path):
    from sandbox_harness.common.contracts.domain_commands import DomainCommandPayloadError
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION))
    adapter=FakeAdapter([DomainCommandPayloadError("domain_command_payload_invalid")])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,worker_id='worker')
    try:
        assert await worker.run_once()==1
        failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED
        assert failed.result['error_code']=='operation_start_failed'
        assert failed.result['side_effect_state']=='none'
        assert await store.latest_job_acceptance(job.id) is None
    finally:await store.close()


async def test_semantic_no_target_failure_never_retries_frozen_selector(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        retry_policy={'max_pre_accept_attempts':5,'deterministic':True}))
    adapter=FakeAdapter([PreAcceptFailure('selector matched nothing',
        error_code='no_target_found',failed_phase='execution',retryable=False)])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED and adapter.submit_calls==1
        assert failed.result['error_code']=='no_target_found'
        assert failed.result['retryable'] is False
    finally:await store.close()


async def test_nonretryable_infrastructure_failure_is_structured_and_terminal(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        retry_policy={'max_pre_accept_attempts':5,'deterministic':True}))
    adapter=FakeAdapter([PreAcceptFailure('Controlled CUDA runtime is unavailable',
        infrastructure=True,error_code='gpu_runtime_unavailable',
        failed_phase='execution',retry_class='node_health_change',retryable=False)])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED and adapter.submit_calls==1
        assert failed.result['error_code']=='gpu_runtime_unavailable'
        assert failed.result['error_detail']=='Controlled CUDA runtime is unavailable'
        assert failed.result['failed_phase']=='execution'
        assert failed.result['retry_class']=='node_health_change'
        assert failed.result['retryable'] is False and failed.result['node_health_generation']==0
        assert failed.result['side_effect_state']=='none'
        assert failed.result['error_message'] and failed.result['error_guidance']
    finally:await store.close()


async def test_node_health_failure_retries_same_immutable_job_as_new_attempt(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        retry_policy={'max_pre_accept_attempts':2,'deterministic':True}))
    adapter=FakeAdapter([PreAcceptFailure('GPU health changed',infrastructure=True,
        error_code='gpu_runtime_unavailable',failed_phase='execution',
        retry_class='node_health_change',retryable=True),
        AdapterSubmission('native-1',{'accepted':True},completed=True,result={'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();queued=await store.get_job(job.id)
        assert queued.state==JobState.QUEUED and queued.generation==1
        await worker.run_once();completed=await store.get_job(job.id)
        assert completed.state==JobState.SUCCEEDED and completed.generation==2
        assert completed.id==job.id and adapter.submit_calls==2
    finally:await store.close()


async def test_infrastructure_pre_accept_failure_does_not_consume_business_retry_limit(tmp_path):
    store,_,job=await setup(tmp_path,request(retry_policy={'max_pre_accept_attempts':1}))
    adapter=FakeAdapter([PreAcceptFailure('no capacity',infrastructure=True),
        PreAcceptFailure('no capacity',infrastructure=True),
        AdapterSubmission('external-1',{'accepted':True},completed=True,result={'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();assert (await store.get_job(job.id)).state==JobState.QUEUED
        await worker.run_once();assert (await store.get_job(job.id)).state==JobState.QUEUED
        await worker.run_once();assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert adapter.submit_calls==3
    finally:await store.close()


@pytest.mark.parametrize('kind',[JobKind.OPERATION,JobKind.PROVIDER])
async def test_service_capacity_wait_reuses_one_attempt_generation(tmp_path,kind,monkeypatch):
    import sandbox_harness.common.execution.job_worker as job_worker_module
    lifecycle=[]
    monkeypatch.setattr(job_worker_module,'event',lambda name,**fields:lifecycle.append((name,fields)))
    store,_,job=await setup(tmp_path,request(kind=kind,
        retry_policy={'max_pre_accept_attempts':1,'deterministic':True}))
    allocation_errors=[]
    class AllocatingAdapter(FakeAdapter):
        async def submit(self,context):
            submission=await super().submit(context)
            try:
                await store.record_job_submission_allocation(context.job.id,context.generation,
                    external_execution_id=submission.external_execution_id,
                    receipt=submission.acceptance_receipt)
            except RuntimeError as exc:
                allocation_errors.append(str(exc))
                raise
            return submission
    adapter=AllocatingAdapter([PreAcceptFailure('no capacity',infrastructure=True,
            retry_after_seconds=.01),
        PreAcceptFailure('no capacity',infrastructure=True,retry_after_seconds=.01),
        AdapterSubmission('external-1',{'accepted':True},completed=True,result={'ok':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker',poll_seconds=.01)
    try:
        task=__import__('asyncio').create_task(worker.run())
        for _ in range(200):
            current=await store.get_job(job.id)
            if current.state in {JobState.SUCCEEDED,JobState.FAILED,JobState.UNKNOWN}:break
            await __import__('asyncio').sleep(.01)
        else:raise AssertionError('capacity wait did not recover')
        worker.stop_event.set();await task
        assert allocation_errors==[]
        assert current.state==JobState.SUCCEEDED,current.result
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job.id))).scalars().all())
        assert len(attempts)==1 and (await store.get_job(job.id)).generation==1
        assert adapter.submit_calls==3
        assert adapter.cleanup_calls==1
        assert attempts[0].external_execution_id=='external-1'
        assert attempts[0].accepted_at is not None
        assert [name for name,_ in lifecycle].count('job.submission.capacity_wait')==2
        assert [name for name,_ in lifecycle].count('job.submission.resumed')==2
        accepted_event=next(fields for name,fields in lifecycle if name=='job.submission.accepted')
        assert accepted_event['generation']==1 and accepted_event['external_execution_id']=='external-1'
        terminal_event=next(fields for name,fields in lifecycle if name=='job.state.changed')
        assert terminal_event['state_after']==JobState.SUCCEEDED
    finally:await store.close()


@pytest.mark.parametrize('retry_started',[False,True])
async def test_capacity_wait_deadline_is_known_preaccept_timeout(tmp_path,retry_started):
    import asyncio
    from datetime import datetime,timedelta,timezone
    store,_,job=await setup(tmp_path,request(timeout_seconds=1))
    class CapacityAdapter(FakeAdapter):
        async def submit(self,context):
            if self.submit_calls:
                self.submit_calls+=1
                await asyncio.sleep(10)
            return await super().submit(context)
    adapter=CapacityAdapter([PreAcceptFailure('provider capacity exhausted',
        infrastructure=True,retry_after_seconds=.01 if retry_started else 1)])
    async def deadline(*args):return datetime.now(timezone.utc)+timedelta(seconds=.25)
    store.job_deadline=deadline
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        attempt=await store.claim_job(job.id,'worker','node',30)
        await worker.process(attempt,continuous=True)
        final=await store.get_job(job.id)
        assert final.state==(JobState.UNKNOWN if retry_started else JobState.FAILED),final.result
        assert final.result['error_code']==('job_timeout_submission_outcome_unknown'
            if retry_started else 'job_timeout_before_submission')
        assert final.result['side_effect_state']==('unknown' if retry_started else 'none')
        assert await store.latest_job_acceptance(job.id) is None
        assert adapter.submit_calls==(2 if retry_started else 1) and adapter.status_calls==0
    finally:await store.close()


async def test_cleanup_worker_claims_only_registered_adapters(tmp_path):
    from test_support.legacy_store import (JobCleanupRow)
    store,_,job=await setup(tmp_path,request('owned'))
    _,others=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,origin_turn_id='c'*32,
        idempotency_key='other-cleanup-group',jobs=[request('other',adapter='other-service')]))
    adapter=FakeAdapter([])
    try:
        for current in [job,others[0]]:
            attempt=await store.claim_job(current.id,'producer','node',30)
            await store.record_job_acceptance(current.id,'producer',attempt.generation,
                external_execution_id='fixture-'+current.id,receipt={})
            await store.transition_job(current.id,JobState.SUCCEEDED,expected_generation=attempt.generation)
        worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='cleanup')
        assert await worker.run_cleanup_once(limit=10)==1
        assert adapter.cleanup_calls==1
        async with store.connections() as db:
            other=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==others[0].id))).one()
            assert other.state=='pending' and other.attempt_count==0 and other.claim_owner==''
    finally:await store.close()


async def test_successful_collection_dispatches_frozen_policy_review_before_terminal(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION))
    adapter=FakeAdapter([AdapterSubmission('external-1',{'accepted':True},completed=True,
        result={'collections':[{'artifact_role':'report','state':'awaiting_review'}]})])
    observed=[]
    async def dispatch(context,result):
        current=await store.get_job(context.job.id)
        assert current.state==JobState.WAITING_EXTERNAL
        observed.append((context.request.idempotency_key,result))
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,
        worker_id='worker',review_dispatcher=dispatch)
    try:
        await worker.run_once()
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert observed==[("job-request-key",{'collections':[
            {'artifact_role':'report','state':'awaiting_review'}]})]
    finally:await store.close()


async def test_policy_dispatch_failure_does_not_rewrite_successful_source_job(tmp_path):
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION))
    adapter=FakeAdapter([AdapterSubmission('external-1',{'accepted':True},completed=True,
        result={'collections':[{'artifact_role':'report','state':'awaiting_review'}]})])
    recorded=[]
    async def unavailable(*args):raise ConnectionError("review coordinator unavailable")
    async def record(job_id,generation,error):
        recorded.append((job_id,generation,error));return [{"state":"required_unavailable"}]
    store.record_policy_review_unavailable=record
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker',
        review_dispatcher=unavailable)
    try:
        await worker.run_once();current=await store.get_job(job.id)
        assert current.state==JobState.SUCCEEDED
        assert current.result['review_dispatch']['state']=='unavailable'
        assert recorded==[(job.id,1,'ConnectionError')]
    finally:await store.close()


async def test_external_handle_is_resumed_without_duplicate_submit(tmp_path):
    store,_,job=await setup(tmp_path,request())
    adapter=FakeAdapter([AdapterSubmission('external-1',{'provider_id':'external-1'})],[
        AdapterStatus(False),AdapterStatus(True,JobState.SUCCEEDED,{'asset':'done'})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,worker_id='worker')
    try:
        await worker.run_once();assert (await store.get_job(job.id)).state==JobState.WAITING_EXTERNAL
        await worker.run_once();value=await store.get_job(job.id)
        assert value.state==JobState.SUCCEEDED and value.result=={'asset':'done'}
        assert adapter.submit_calls==1 and adapter.status_calls==2
    finally:await store.close()


async def test_service_polling_holds_one_generation_and_releases_on_stop(tmp_path):
    store,_,job=await setup(tmp_path,request())
    adapter=FakeAdapter([AdapterSubmission('external-1',{'provider_id':'external-1'})],[
        AdapterStatus(False),AdapterStatus(False),AdapterStatus(True,JobState.SUCCEEDED,{'asset':'done'})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,
        worker_id='worker',poll_seconds=.01)
    try:
        task=__import__('asyncio').create_task(worker.run())
        for _ in range(200):
            if (await store.get_job(job.id)).state==JobState.SUCCEEDED:break
            await __import__('asyncio').sleep(.01)
        else:raise AssertionError('continuous polling did not finish')
        worker.stop_event.set();await task
        value=await store.get_job(job.id)
        assert value.generation==1 and adapter.submit_calls==1 and adapter.status_calls==3
    finally:await store.close()


async def test_service_stop_releases_continuous_poll_lease_for_takeover(tmp_path):
    store,_,job=await setup(tmp_path,request())
    adapter=FakeAdapter([AdapterSubmission('external-1',{'provider_id':'external-1'})],
        [AdapterStatus(False)]*20)
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,
        worker_id='worker-a',poll_seconds=.01)
    try:
        task=__import__('asyncio').create_task(worker.run())
        for _ in range(200):
            if (await store.get_job(job.id)).state==JobState.RUNNING:break
            await __import__('asyncio').sleep(.01)
        worker.stop_event.set();await task
        claimed=await store.claim_job(job.id,'worker-b','node',30,retry_kind='recovery')
        assert claimed.generation==2
    finally:await store.close()


@pytest.mark.parametrize('error',[
    RuntimeError('broken status'),
    *[httpx.HTTPStatusError('permanent status failure',
        request=httpx.Request('GET','http://runtime/execution'),
        response=httpx.Response(code)) for code in [400,401,403,404,422]],
])
async def test_one_job_exception_does_not_crash_service_worker(tmp_path,error):
    store,_,job=await setup(tmp_path,request(kind=JobKind.HARNESS))
    adapter=FakeAdapter([AdapterSubmission('external-1',{'accepted':True})],
        [error])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,
        worker_id='worker',poll_seconds=.01)
    try:
        task=__import__('asyncio').create_task(worker.run())
        for _ in range(200):
            if (await store.get_job(job.id)).state==JobState.UNKNOWN:break
            await __import__('asyncio').sleep(.01)
        else:raise AssertionError('isolated failure was not recorded')
        assert not task.done()
        worker.stop_event.set();await task
        result=(await store.get_job(job.id)).result
        assert result['error_code']=='external_status_unavailable'
        assert result['failed_phase']=='status' and result['side_effect_state']=='unknown'
        assert result['retryable'] and result['retry_class']=='reconcile_only'
    finally:await store.close()


@pytest.mark.parametrize('error',[
    httpx.ConnectError('runtime endpoint temporarily unavailable'),
    httpx.ReadError('status response interrupted'),
    *[httpx.HTTPStatusError('temporary status failure',
        request=httpx.Request('GET','http://runtime/execution'),
        response=httpx.Response(code)) for code in [408,429,500,502,503,504]],
])
async def test_transient_status_transport_error_preserves_execution_for_reconcile(tmp_path,error):
    store,_,job=await setup(tmp_path,request(kind=JobKind.HARNESS))
    adapter=FakeAdapter([AdapterSubmission('external-1',{'accepted':True})],[
        error,
        AdapterStatus(True,JobState.SUCCEEDED,{'done':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',concurrency=1,
        worker_id='worker',poll_seconds=.01)
    try:
        assert await worker.run_once()==1
        waiting=await store.get_job(job.id)
        assert waiting.state==JobState.WAITING_EXTERNAL
        assert adapter.submit_calls==1 and adapter.status_calls==1 and adapter.cleanup_calls==0

        assert await worker.run_once()==1
        completed=await store.get_job(job.id)
        assert completed.state==JobState.SUCCEEDED
        assert adapter.submit_calls==1 and adapter.status_calls==2 and adapter.cleanup_calls==1
        accepted=await store.latest_job_acceptance(job.id)
        assert accepted.external_execution_id=='external-1'
    finally:await store.close()


async def test_collection_retry_survives_worker_restart_and_reuses_accepted_execution(tmp_path):
    class FlakyCollector(FakeAdapter):
        collect_calls=0
        async def collect(self,*args):
            self.collect_calls+=1
            if self.collect_calls==1:raise RuntimeError('secret provider download body')
            return await super().collect(*args)
    store,_,job=await setup(tmp_path,request(kind=JobKind.PROVIDER,
        retry_policy={'max_pre_accept_attempts':2,'max_collection_attempts':2}))
    adapter=FlakyCollector([AdapterSubmission('external-1',{'accepted':True},
        completed=True,result={'done':True})],[AdapterStatus(True,JobState.SUCCEEDED,{'done':True})])
    first=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker-a')
    second=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker-b')
    try:
        await first.run_once();waiting=await store.get_job(job.id)
        assert waiting.state==JobState.WAITING_EXTERNAL and adapter.submit_calls==1
        await second.run_once();completed=await store.get_job(job.id)
        assert completed.state==JobState.SUCCEEDED and adapter.submit_calls==1
        assert adapter.collect_calls==2 and adapter.status_calls==1
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job.id).order_by(JobAttemptRow.generation))).scalars().all())
        assert [row.outcome for row in attempts]==['COLLECTION_RETRY','SUCCEEDED']
        assert [row.retry_kind for row in attempts]==['initial','collection']
        assert {row.external_execution_id for row in attempts if row.external_execution_id}=={'external-1'}
    finally:await store.close()


async def test_collection_retry_exhaustion_is_structured_without_resubmission(tmp_path):
    class BrokenCollector(FakeAdapter):
        async def collect(self,*_):raise RuntimeError('secret provider download body')
    store,_,job=await setup(tmp_path,request(kind=JobKind.PROVIDER,
        retry_policy={'max_pre_accept_attempts':2,'max_collection_attempts':2}))
    adapter=BrokenCollector([AdapterSubmission('external-1',{'accepted':True},
        completed=True,result={'done':True})],[AdapterStatus(True,JobState.SUCCEEDED,{'done':True})])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();assert (await store.get_job(job.id)).state==JobState.WAITING_EXTERNAL
        await worker.run_once();failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED and adapter.submit_calls==1
        assert failed.result['error_code']=='artifact_collection_failed'
        assert failed.result['failed_phase']=='collection'
        assert failed.result['side_effect_state']=='completed'
        assert failed.result['retry_class']=='collect_same_execution'
        assert 'secret provider' not in str(failed.result)
    finally:await store.close()


@pytest.mark.parametrize('data,declared,allowed,item_limit,transfer_limit,total_limit,error_code',[
    (b'result','text/plain','image/png',1024,1024,2048,'artifact_mime_not_allowed'),
    (b'%PDF-1.7\nreport','image/png','image/png',1024,1024,2048,'artifact_mime_mismatch'),
    (b'result','text/plain','text/plain',4,1024,2048,'artifact_item_too_large'),
    (b'result','text/plain','text/plain',1024,4,2048,'artifact_item_too_large'),
    (b'result','text/plain','text/plain',1024,1024,4,'artifact_collection_too_large'),
])
async def test_deterministic_artifact_rejection_never_retries_collection(
        tmp_path,data,declared,allowed,item_limit,transfer_limit,total_limit,error_code):
    from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
    from sandbox_harness.common.contracts.job_contracts import (OutputDisposition)
    from sandbox_harness.common.storage.objects import (ObjectStore)
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        expected_outputs=[{'artifact_role':'files','output_disposition':'delivery',
            'min_items':1,'max_items':1,'mime_patterns':[allowed],'max_item_bytes':item_limit,
            'max_total_bytes':total_limit}],
        retry_policy={'max_pre_accept_attempts':2,'max_collection_attempts':3}))
    gateway=ArtifactGateway(store,ObjectStore(tmp_path/'objects'),tmp_path/'stage',
        maximum_bytes=transfer_limit)
    async def chunks():yield data
    class Collector(FakeAdapter):
        collect_calls=0
        async def collect(self,context,external_id,status):
            self.collect_calls+=1
            await gateway.emit(ArtifactEmission(context.job.id,context.generation,'files',
                'result',0,'result.bin',OutputDisposition.DELIVERY,declared),chunks())
            return await store.finalize_job_artifact_collection(context.job.id,context.generation,'files')
    adapter=Collector([AdapterSubmission('external-1',{'accepted':True},completed=True)])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED
        assert failed.result['error_code']==error_code
        assert failed.result['failed_phase']=='validation'
        assert failed.result['retryable'] is False and failed.result['retry_class']=='none'
        assert failed.result['side_effect_state']=='completed'
        assert await store.current_project_files(job.thread_id,'owner')==[]
        await worker.run_once()
        assert adapter.submit_calls==adapter.collect_calls==1
    finally:await store.close()


@pytest.mark.parametrize('file_kind',['text','zip'])
async def test_mime_io_failure_retries_collection_of_the_same_execution(tmp_path,monkeypatch,file_kind):
    import io
    import zipfile
    from pathlib import Path
    from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
    from sandbox_harness.common.contracts.job_contracts import (OutputDisposition)
    from sandbox_harness.common.storage.objects import (ObjectStore)
    data=b'result';mime='text/plain'
    if file_kind=='zip':
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:archive.writestr('result.txt',data)
        data=stream.getvalue();mime='application/zip'
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        expected_outputs=[{'artifact_role':'files','output_disposition':'delivery',
            'min_items':1,'max_items':1,'mime_patterns':[mime]}],
        retry_policy={'max_pre_accept_attempts':2,'max_collection_attempts':3}))
    gateway=ArtifactGateway(store,ObjectStore(tmp_path/'objects'),tmp_path/'stage',maximum_bytes=1024)
    remaining=1
    original=Path.read_text if file_kind=='text' else zipfile.ZipFile
    def read_once(path,*args,**kwargs):
        nonlocal remaining
        if isinstance(path,Path) and path.parent==gateway.staging_root and remaining:
            remaining-=1
            raise OSError('controlled transient read failure')
        return original(path,*args,**kwargs)
    if file_kind=='text':monkeypatch.setattr(Path,'read_text',read_once)
    else:monkeypatch.setattr(zipfile,'ZipFile',read_once)
    async def chunks():yield data
    class Collector(FakeAdapter):
        collect_calls=0
        async def collect(self,context,external_id,status):
            self.collect_calls+=1
            await gateway.emit(ArtifactEmission(context.job.id,context.generation,'files',
                'result',0,'result.bin',OutputDisposition.DELIVERY,mime),chunks())
            return {}
    adapter=Collector([AdapterSubmission('external-1',{'accepted':True},completed=True)],
        [AdapterStatus(True,JobState.SUCCEEDED)])
    first=JobWorker(store,{'fake':adapter},node_id='node',worker_id='before-restart')
    second=JobWorker(store,{'fake':adapter},node_id='node',worker_id='after-restart')
    try:
        await first.run_once();waiting=await store.get_job(job.id)
        assert waiting.state==JobState.WAITING_EXTERNAL
        assert waiting.result['error_code']=='artifact_collection_failed'
        await second.run_once()
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
        assert adapter.submit_calls==1 and adapter.collect_calls==2
        assert (await store.latest_job_acceptance(job.id)).external_execution_id=='external-1'
    finally:await store.close()


@pytest.mark.parametrize('actual_count',[0,2])
async def test_frozen_collection_cardinality_failure_never_retries_immutable_result(tmp_path,actual_count):
    from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
    from sandbox_harness.common.contracts.job_contracts import (OutputDisposition)
    from sandbox_harness.common.storage.objects import (ObjectStore)
    from test_support.legacy_store import (JobArtifactRow, ProjectFileRow)
    store,_,job=await setup(tmp_path,request(kind=JobKind.OPERATION,
        expected_outputs=[{'artifact_role':'files','output_disposition':'delivery',
            'min_items':1,'max_items':1}],
        retry_policy={'max_pre_accept_attempts':2,'max_collection_attempts':3}))
    gateway=ArtifactGateway(store,ObjectStore(tmp_path/'objects'),tmp_path/'stage',maximum_bytes=1024)
    async def chunks():yield b'result'
    class Collector(FakeAdapter):
        collect_calls=0
        async def collect(self,context,external_id,status):
            self.collect_calls+=1
            for position in range(actual_count):
                await gateway.emit(ArtifactEmission(context.job.id,context.generation,'files',
                    str(position),position,f'{position}.txt',OutputDisposition.DELIVERY,'text/plain'),chunks())
            return await store.finalize_job_artifact_collection(context.job.id,context.generation,'files')
    adapter=Collector([AdapterSubmission('external-1',{'accepted':True},completed=True)])
    worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
    try:
        await worker.run_once();failed=await store.get_job(job.id)
        assert failed.state==JobState.FAILED
        assert failed.result['error_code']=='artifact_collection_cardinality_invalid'
        assert failed.result['failed_phase']=='validation'
        assert failed.result['retryable'] is False and failed.result['retry_class']=='none'
        assert failed.result['side_effect_state']=='completed'
        assert failed.result['output_contract']=={'artifact_role':'files','min_items':1,
            'max_items':1,'actual_items':actual_count}
        assert 'max_items' in failed.result['error_guidance']
        await worker.run_once()
        assert adapter.submit_calls==adapter.collect_calls==1
        assert adapter.cleanup_calls==1
        async with store.connections() as db:
            attempts=(await db.scalars(select(JobAttemptRow).where(JobAttemptRow.job_id==job.id))).all()
            assert len(attempts)==1 and attempts[0].outcome=='FAILED'
            artifacts=(await db.scalars(select(JobArtifactRow).where(JobArtifactRow.job_id==job.id))).all()
            assert len(artifacts)==actual_count
            assert all(row.output_disposition=='candidate' and row.state=='committed' for row in artifacts)
            assert (await db.scalars(select(ProjectFileRow))).all()==[]
    finally:await store.close()


async def test_unknown_submission_without_handle_is_terminal_and_consumes_acceptance_budget(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'unknown.db'));await store.initialize()
    try:
        account=await store.configure_budget('owner','b'*32,'provider.submissions',limit=1)
        req=request(budget_requirements={'provider.submissions':1})
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
            origin_turn_id='c'*32,idempotency_key='unknown-group',jobs=[req],
            authorization_snapshot={'budget_reservations':[{'account_id':account.id,'amount':1}]}))
        adapter=FakeAdapter([SubmissionOutcomeUnknown('response lost')])
        worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
        await worker.run_once()
        assert (await store.get_job(jobs[0].id)).state==JobState.UNKNOWN
        budget=await store.get_budget_account(account.id)
        assert budget.consumed==1 and budget.reserved==0
        ledger=await store.list_budget_ledger((await store.get_budget_reservations_for_group(
            jobs[0].group_id))[0].id)
        assert [row.action for row in ledger]==['reserve','consume']
    finally:await store.close()


async def test_non_idempotent_provider_exception_is_unknown_but_deterministic_operation_retries(tmp_path):
    provider_store,_,provider_job=await setup(tmp_path,request(local_id='provider'))
    provider=FakeAdapter([RuntimeError('socket lost')])
    provider_worker=JobWorker(provider_store,{'fake':provider},node_id='node',worker_id='provider-worker')
    try:
        await provider_worker.run_once()
        assert (await provider_store.get_job(provider_job.id)).state==JobState.UNKNOWN
    finally:await provider_store.close()
    operation_store,_,operation_job=await setup(tmp_path,request(local_id='operation',kind=JobKind.OPERATION,
        retry_policy={'max_pre_accept_attempts':2,'deterministic':True}))
    operation=FakeAdapter([RuntimeError('process did not start'),
        AdapterSubmission('native-1',{'accepted':True},completed=True,result={'ok':True})])
    operation_worker=JobWorker(operation_store,{'fake':operation},node_id='node',worker_id='operation-worker')
    try:
        await operation_worker.run_once();assert (await operation_store.get_job(operation_job.id)).state==JobState.QUEUED
        async with operation_store.connections() as db:
            attempt=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==operation_job.id))).scalar_one()
        assert attempt.error_code=='operation_start_failed'
        assert attempt.error_detail=='RuntimeError: process did not start'
        await operation_worker.run_once();assert (await operation_store.get_job(operation_job.id)).state==JobState.SUCCEEDED
    finally:await operation_store.close()


async def test_reconciliation_job_queries_original_handle_without_submit(tmp_path):
    store,_,original=await setup(tmp_path,request(local_id='original'))
    try:
        attempt=await store.claim_job(original.id,'seed-worker','node',30)
        await store.record_job_acceptance(original.id,'seed-worker',attempt.generation,
            external_execution_id='external-original',receipt={'accepted':True})
        await store.transition_job(original.id,JobState.UNKNOWN,expected_generation=attempt.generation,
            result={'error_code':'provider_result_unresolved'})
        reconcile=request(local_id='reconcile',reconciles_job_id=original.id)
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
            origin_turn_id='d'*32,idempotency_key='reconcile-group',jobs=[reconcile]))
        adapter=FakeAdapter([],[AdapterStatus(True,JobState.SUCCEEDED,{'found':True})])
        worker=JobWorker(store,{'fake':adapter},node_id='node',worker_id='worker')
        await worker.run_once()
        assert (await store.get_job(jobs[0].id)).state==JobState.SUCCEEDED
        assert adapter.submit_calls==0 and adapter.reconcile_calls==1
    finally:await store.close()
