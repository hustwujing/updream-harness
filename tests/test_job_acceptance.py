import pytest

from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest
from sandbox_harness.store import Store


async def test_recovery_generation_cannot_double_consume_business_acceptance(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'acceptance.db'));await store.initialize()
    try:
        account=await store.configure_budget('owner','a'*32,'provider.submissions',limit=1)
        request=JobRequest(local_job_id='provider',kind=JobKind.PROVIDER,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='b'*64,
            idempotency_key='acceptance-job-key',parameters={},
            budget_requirements={'provider.submissions':1})
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='a'*32,
            origin_turn_id='c'*32,idempotency_key='acceptance-group-key',jobs=[request],
            authorization_snapshot={'budget_reservations':[{'account_id':account.id,'amount':1}]}))
        first=await store.claim_job(jobs[0].id,'worker-one','node',30)
        accepted=await store.record_job_acceptance(jobs[0].id,'worker-one',first.generation,
            external_execution_id='external-1',receipt={'accepted':True})
        assert accepted.accepted_at is not None
        await store.defer_accepted_job(jobs[0].id,'worker-one',first.generation)
        second=await store.claim_job(jobs[0].id,'worker-two','node',30)
        with pytest.raises(RuntimeError,match='stale_job_generation'):
            await store.record_job_acceptance(jobs[0].id,'worker-one',first.generation,
                external_execution_id='external-1',receipt={'accepted':True})
        repeated=await store.record_job_acceptance(jobs[0].id,'worker-two',second.generation,
            external_execution_id='external-1',receipt={'reattached':True})
        assert repeated.generation==second.generation
        budget=await store.get_budget_account(account.id)
        assert budget.consumed==1 and budget.reserved==0
        reservation=(await store.get_budget_reservations_for_group(jobs[0].group_id))[0]
        ledger=await store.list_budget_ledger(reservation.id)
        assert [row.action for row in ledger]==['reserve','consume']
    finally:await store.close()
