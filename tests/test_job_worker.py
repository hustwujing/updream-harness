from sandbox_harness.domain import identifier
from sandbox_harness.job_adapters import (
    AdapterStatus, AdapterSubmission, BaseJobAdapter, PreAcceptFailure,
    SubmissionOutcomeUnknown,
)
from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest, JobState
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.store import JobAttemptRow,Store
from sqlalchemy import select
import httpx
import pytest


class FakeAdapter(BaseJobAdapter):
    def __init__(self, submissions, statuses=()):
        self.submissions=list(submissions);self.statuses=list(statuses)
        self.submit_calls=0;self.status_calls=0;self.reconcile_calls=0;self.cleanup_calls=0
    async def submit(self,context):
        self.submit_calls+=1;value=self.submissions.pop(0)
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
async def test_service_capacity_wait_reuses_one_attempt_generation(tmp_path,kind):
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
