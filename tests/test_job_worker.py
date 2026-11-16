from sandbox_harness.domain import identifier
from sandbox_harness.job_adapters import (
    AdapterStatus, AdapterSubmission, BaseJobAdapter, PreAcceptFailure,
    SubmissionOutcomeUnknown,
)
from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest, JobState
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.store import Store


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
        return self.statuses.pop(0) if self.statuses else AdapterStatus(False)
    async def reconcile(self,context,external_execution_id):
        self.reconcile_calls+=1
        return self.statuses.pop(0)
    async def cleanup(self,context,external_execution_id):self.cleanup_calls+=1


def request(local_id='job',kind=JobKind.PROVIDER,**updates):
    value=dict(local_job_id=local_id,kind=kind,adapter='fake',descriptor_id='fake',
        descriptor_version='1',descriptor_digest='a'*64,idempotency_key=f'{local_id}-request-key',
        parameters={},retry_policy={'max_pre_accept_attempts':2})
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
