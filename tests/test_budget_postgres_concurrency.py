import asyncio
import os
from datetime import timedelta

import pytest

from sandbox_harness.domain import identifier
from sandbox_harness.domain import now
from sandbox_harness.job_contracts import JobGroupDraftCreate, JobKind, JobRequest
from sandbox_harness.store import Store


POSTGRES_URL=os.environ.get('PARALLEL_TEST_POSTGRES_URL','')
pytestmark=pytest.mark.skipif(not POSTGRES_URL,reason='PARALLEL_TEST_POSTGRES_URL is not configured')


async def test_budget_reservations_cannot_overspend_and_release_exactly_once():
    primary=Store(POSTGRES_URL);await primary.initialize()
    stores=[Store(POSTGRES_URL) for _ in range(8)]
    owner='budget-'+identifier();thread_id=identifier();budget_key='image.generate.submissions'
    try:
        account=await primary.configure_budget(owner,thread_id,budget_key,limit=5)
        async def reserve(store,index):
            try:
                return await store.reserve_budget(account.id,group_id=identifier(),
                    authorization_snapshot_id=identifier(),amount=3,
                    idempotency_key=f'reserve-{index}-{identifier()}')
            except RuntimeError as exc:
                return exc
        results=await asyncio.gather(*(reserve(store,index) for index,store in enumerate(stores)))
        successes=[value for value in results if not isinstance(value,Exception)]
        assert len(successes)==1
        reservation=successes[0]
        job_id=identifier()
        await primary.consume_budget(reservation.id,job_id=job_id,amount=2,
            idempotency_key='consume-once')
        await primary.consume_budget(reservation.id,job_id=job_id,amount=2,
            idempotency_key='consume-once')
        released=await primary.release_budget(reservation.id,idempotency_key='release-once')
        repeated=await primary.release_budget(reservation.id,idempotency_key='release-once')
        assert released.released==1 and repeated.released==1 and repeated.consumed==2
        view=await primary.get_budget_account(account.id)
        assert view.limit==5 and view.reserved==0 and view.consumed==2
        ledger=await primary.list_budget_ledger(reservation.id)
        assert [(row.action,row.amount) for row in ledger]==[('reserve',3),('consume',2),('release',1)]
    finally:
        await asyncio.gather(*(store.close() for store in stores));await primary.close()


async def test_concurrent_draft_confirmation_reserves_budget_and_jobs_atomically():
    primary=Store(POSTGRES_URL);await primary.initialize()
    workers=[Store(POSTGRES_URL) for _ in range(2)]
    owner='confirm-'+identifier();thread_id=identifier()
    try:
        account=await primary.configure_budget(owner,thread_id,'provider.submissions',limit=5)
        drafts=[]
        for index in range(2):
            request=JobRequest(local_job_id=f'provider-{index}',kind=JobKind.PROVIDER,
                adapter='image.generate',descriptor_id='image.generate',descriptor_version='1',
                descriptor_digest='a'*64,idempotency_key=f'provider-job-{index}-{identifier()}',
                parameters={'prompt':f'image {index}'},budget_requirements={'provider.submissions':3})
            draft,_=await primary.create_job_group_draft(owner,JobGroupDraftCreate(
                thread_id=thread_id,origin_turn_id=identifier(),
                idempotency_key=f'budget-draft-{index}-{identifier()}',jobs=[request],
                authorization_snapshot={'budget_reservations':[{'account_id':account.id,'amount':3}]},
                expires_at=now()+timedelta(minutes=5),requires_confirmation=True))
            drafts.append(draft)
        results=await asyncio.gather(*(
            worker.confirm_job_group_draft(draft.id,owner)
            for worker,draft in zip(workers,drafts)),return_exceptions=True)
        successes=[value for value in results if not isinstance(value,Exception)]
        failures=[value for value in results if isinstance(value,Exception)]
        assert len(successes)==len(failures)==1 and str(failures[0])=='budget_exhausted'
        assert len(successes[0][1])==1
        budget=await primary.get_budget_account(account.id)
        assert budget.reserved==3 and budget.consumed==0
        assert len(await primary.list_jobs_for_thread(thread_id))==1
    finally:
        await asyncio.gather(*(worker.close() for worker in workers));await primary.close()
