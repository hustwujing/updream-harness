import asyncio

import pytest

from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,
    SubmissionRecovery)
from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.store import (BudgetAccountRow,BudgetLedgerRow,BudgetReservationRow,
    JobOutboxRow,Store)
from sandbox_harness.scheduler import GPUInventory,NodeInventory,NodeProfile,ResourceRequest,Scheduler


def request(local="job",**updates):
    value=dict(local_job_id=local,kind=JobKind.PROVIDER,adapter="provider",
        descriptor_id="provider",descriptor_version="1",descriptor_digest="a"*64,
        idempotency_key=local+"-durable-key",parameters={},timeout_seconds=10,
        retry_policy={"max_pre_accept_attempts":1},cancellation_policy={"supported":True},
        reconciliation_policy={"supported":True})
    value.update(updates);return JobRequest(**value)


async def setup(tmp_path,value=None,snapshot=None):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/((value.local_job_id if value else "job")+".db")))
    await store.initialize();group,jobs=await store.create_job_group("owner",JobGroupCreate(
        thread_id="b"*32,origin_turn_id="c"*32,idempotency_key="group-"+(value.local_job_id if value else "job"),
        jobs=[value or request()],authorization_snapshot=snapshot or {}))
    return store,group,jobs[0]


class BlockingAdapter(BaseJobAdapter):
    def __init__(self):
        self.started=asyncio.Event();self.release=asyncio.Event();self.submit_calls=0;self.cleanup_calls=0
    async def submit(self,context):
        self.submit_calls+=1;self.started.set();await self.release.wait()
        return AdapterSubmission("external",{"id":"external"},completed=True,result={"ok":True})
    async def collect(self,context,external,status):return status.result
    async def cleanup(self,context,external):self.cleanup_calls+=1


async def test_submit_heartbeat_prevents_second_worker_duplicate(tmp_path):
    store,_,job=await setup(tmp_path);adapter=BlockingAdapter()
    first=JobWorker(store,{"provider":adapter},node_id="node",worker_id="first",lease_seconds=1)
    second=JobWorker(store,{"provider":adapter},node_id="node",worker_id="second",lease_seconds=1)
    try:
        task=asyncio.create_task(first.run_once());await adapter.started.wait();await asyncio.sleep(1.2)
        assert await second.run_once()==0 and adapter.submit_calls==1
        adapter.release.set();await task
        assert (await store.get_job(job.id)).state==JobState.SUCCEEDED
    finally:adapter.release.set();await store.close()


async def test_process_death_after_submit_intent_never_resubmits(tmp_path):
    store,_,job=await setup(tmp_path,request("death"));adapter=BlockingAdapter()
    dead=JobWorker(store,{"provider":adapter},node_id="node",worker_id="dead",lease_seconds=1)
    replacement=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement",lease_seconds=1)
    try:
        task=asyncio.create_task(dead.run_once());await adapter.started.wait()
        task.cancel();await asyncio.gather(task,return_exceptions=True);await asyncio.sleep(1.1)
        adapter.release.set();assert await replacement.run_once()==1
        current=await store.get_job(job.id)
        assert adapter.submit_calls==1 and current.state==JobState.UNKNOWN
        assert current.result["error_code"]=="submission_outcome_unknown_after_worker_loss"
    finally:adapter.release.set();await store.close()


class RecoverableHarnessAdapter(BlockingAdapter):
    def __init__(self,recovery):super().__init__();self.recovery=recovery
    async def recover_submission(self,context):return self.recovery
    async def submit(self,context):
        self.submit_calls+=1
        if self.submit_calls==1:
            self.started.set();await self.release.wait()
        return AdapterSubmission("new-execution",{"id":"new"},completed=True,result={"ok":True})


async def test_harness_worker_loss_retries_only_after_adapter_proves_no_acceptance(tmp_path):
    store,_,job=await setup(tmp_path,request("harness-retry",kind=JobKind.HARNESS))
    adapter=RecoverableHarnessAdapter(SubmissionRecovery(safe_to_retry=True))
    dead=JobWorker(store,{"provider":adapter},node_id="node",worker_id="dead",lease_seconds=1)
    replacement=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement",lease_seconds=1)
    try:
        task=asyncio.create_task(dead.run_once());await adapter.started.wait()
        task.cancel();await asyncio.gather(task,return_exceptions=True);await asyncio.sleep(1.1)
        adapter.release.set();await replacement.run_once()
        current=await store.get_job(job.id)
        assert current.state==JobState.SUCCEEDED and current.result=={"ok":True}
        assert adapter.submit_calls==2
    finally:adapter.release.set();await store.close()


async def test_harness_worker_loss_adopts_proven_acceptance_without_resubmit(tmp_path):
    store,_,job=await setup(tmp_path,request("harness-adopt",kind=JobKind.HARNESS))
    recovered=AdapterSubmission("existing-execution",{"id":"existing"},completed=True,
        result={"recovered":True})
    adapter=RecoverableHarnessAdapter(SubmissionRecovery(submission=recovered))
    dead=JobWorker(store,{"provider":adapter},node_id="node",worker_id="dead",lease_seconds=1)
    replacement=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement",lease_seconds=1)
    try:
        task=asyncio.create_task(dead.run_once());await adapter.started.wait()
        task.cancel();await asyncio.gather(task,return_exceptions=True);await asyncio.sleep(1.1)
        adapter.release.set();await replacement.run_once()
        current=await store.get_job(job.id)
        assert current.state==JobState.SUCCEEDED and current.result=={"recovered":True}
        assert adapter.submit_calls==1
        assert (await store.latest_job_acceptance(job.id)).external_execution_id=="existing-execution"
    finally:adapter.release.set();await store.close()


async def test_cooperative_lease_loss_cancels_stale_submit_and_replacement_does_not_replay(tmp_path,monkeypatch):
    store,_,job=await setup(tmp_path,request("lease-loss"));adapter=BlockingAdapter()
    stale=JobWorker(store,{"provider":adapter},node_id="node",worker_id="stale",lease_seconds=1)
    replacement=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement",lease_seconds=1)
    original=store.renew_job_lease
    async def lose(job_id,worker_id,generation,seconds):
        if worker_id=="stale":return False
        return await original(job_id,worker_id,generation,seconds)
    monkeypatch.setattr(store,"renew_job_lease",lose)
    try:
        task=asyncio.create_task(stale.run_once());await adapter.started.wait();await task
        await asyncio.sleep(1.1);adapter.release.set();await replacement.run_once()
        assert adapter.submit_calls==1
        assert (await store.get_job(job.id)).state==JobState.UNKNOWN
    finally:adapter.release.set();await store.close()


class CleanupAdapter(BaseJobAdapter):
    def __init__(self):self.cleanup_calls=0;self.fail=True
    async def submit(self,context):return AdapterSubmission("external",{"sandbox":"one"},
        completed=True,result={"ok":True})
    async def collect(self,context,external,status):return status.result
    async def cleanup(self,context,external):
        self.cleanup_calls+=1
        if self.fail:raise RuntimeError("release failed")


async def test_cleanup_failure_is_durable_and_retried_after_worker_restart(tmp_path):
    store,_,job=await setup(tmp_path,request("cleanup"));adapter=CleanupAdapter()
    first=JobWorker(store,{"provider":adapter},node_id="node",worker_id="first")
    try:
        await first.run_once();current=await store.get_job(job.id);cleanup=await store.get_job_cleanup(job.id)
        assert current.state==JobState.SUCCEEDED
        assert cleanup["state"]=="pending" and cleanup["attempt_count"]==1
        adapter.fail=False
        restarted=JobWorker(store,{"provider":adapter},node_id="node",worker_id="restarted")
        assert await restarted.run_once()>=1
        cleanup=await store.get_job_cleanup(job.id)
        assert cleanup["state"]=="completed" and cleanup["attempt_count"]==2
        assert adapter.cleanup_calls==2
    finally:await store.close()


class SoftCancelAdapter(BaseJobAdapter):
    def __init__(self):self.cancel_calls=0;self.status_calls=0
    async def submit(self,context):return AdapterSubmission("external",{"id":"external"})
    async def status(self,context,external):
        self.status_calls+=1
        return (AdapterStatus(False) if self.status_calls==1 else
            AdapterStatus(True,JobState.SUCCEEDED,{"asset":"late-success"}))
    async def cancel(self,context,external):self.cancel_calls+=1;return AdapterStatus(False)
    async def collect(self,context,external,status):return status.result


async def test_unsupported_cancel_is_soft_and_late_success_is_collected(tmp_path):
    value=request("soft",cancellation_policy={"supported":False})
    store,_,job=await setup(tmp_path,value);adapter=SoftCancelAdapter()
    worker=JobWorker(store,{"provider":adapter},node_id="node",worker_id="worker")
    try:
        await worker.run_once();await store.request_job_cancel(job.id,"owner");await worker.run_once()
        current=await store.get_job(job.id)
        assert adapter.cancel_calls==0 and current.state==JobState.SUCCEEDED
        assert current.result=={"asset":"late-success","soft_cancel_requested":True}
    finally:await store.close()


async def test_timeout_is_durable_across_worker_restart_before_submission(tmp_path):
    value=request("timeout",timeout_seconds=1);store,_,job=await setup(tmp_path,value)
    adapter=BlockingAdapter();first=await store.claim_job(job.id,"dead","node",1)
    await store.release_job_lease(job.id,"dead",first.generation);await asyncio.sleep(1.1)
    worker=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement")
    try:
        await worker.run_once();current=await store.get_job(job.id)
        assert adapter.submit_calls==0 and current.state==JobState.FAILED
        assert current.result["error_code"]=="job_timeout_before_submission"
    finally:adapter.release.set();await store.close()


async def test_timeout_during_submission_is_unknown_and_never_retried(tmp_path):
    value=request("submit-timeout",timeout_seconds=1);store,_,job=await setup(tmp_path,value)
    adapter=BlockingAdapter();worker=JobWorker(store,{"provider":adapter},node_id="node",worker_id="worker")
    try:
        await worker.run_once();current=await store.get_job(job.id)
        assert adapter.submit_calls==1 and current.state==JobState.UNKNOWN
        assert current.result["error_code"]=="job_timeout_submission_outcome_unknown"
    finally:adapter.release.set();await store.close()


async def test_reconciliation_policy_and_lineage_are_enforced_before_commit(tmp_path):
    source=request("source",reconciliation_policy={"supported":False})
    store,_,job=await setup(tmp_path,source)
    try:
        attempt=await store.claim_job(job.id,"worker","node",30)
        await store.transition_job(job.id,JobState.UNKNOWN,
            expected_generation=attempt.generation,result={"unknown":True})
        reconcile=request("reconcile",reconciles_job_id=job.id,
            reconciliation_policy={"supported":True})
        with pytest.raises(RuntimeError,match="reconciliation_not_supported"):
            await store.create_job_group("owner",JobGroupCreate(thread_id="b"*32,
                origin_turn_id="c"*32,idempotency_key="reconcile-policy-group",jobs=[reconcile]))
    finally:await store.close()


async def test_group_terminal_releases_budget_in_same_transaction_and_repair_is_idempotent(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"budget.db"));await store.initialize()
    account=await store.configure_budget("owner","b"*32,"submissions",limit=10)
    snapshot={"budget_reservations":[{"account_id":account.id,"amount":2}]}
    group,jobs=await store.create_job_group("owner",JobGroupCreate(thread_id="b"*32,
        origin_turn_id="c"*32,idempotency_key="budget-group",jobs=[request("budget",
            budget_requirements={"submissions":1})],authorization_snapshot=snapshot))
    try:
        attempt=await store.claim_job(jobs[0].id,"worker","node",30)
        await store.record_job_acceptance(jobs[0].id,"worker",attempt.generation,
            external_execution_id="external",receipt={"accepted":True})
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result={"ok":True})
        reservation=(await store.get_budget_reservations_for_group(group.id))[0]
        current=await store.get_budget_account(account.id)
        assert (reservation.consumed,reservation.released,current.reserved,current.consumed)==(1,1,0,1)
        async with store.connections.begin() as db:
            row=await db.get(BudgetReservationRow,reservation.id,with_for_update=True)
            account_row=await db.get(BudgetAccountRow,account.id,with_for_update=True)
            row.released=0;row.state="partial";account_row.reserved=1
            await db.execute(__import__('sqlalchemy').delete(BudgetLedgerRow).where(
                BudgetLedgerRow.reservation_id==reservation.id,BudgetLedgerRow.action=="release"))
        await store.reconcile_terminal_group_budgets();await store.reconcile_terminal_group_budgets()
        repaired=(await store.get_budget_reservations_for_group(group.id))[0]
        current=await store.get_budget_account(account.id)
        assert (repaired.released,current.reserved,current.consumed)==(1,0,1)
    finally:await store.close()


async def test_archive_blocks_active_jobs_and_pending_continuation_then_unarchive_recovers(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"archive.db"));await store.initialize()
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="Archive"),profile,"owner")
    group,jobs=await store.create_job_group("owner",JobGroupCreate(thread_id=thread.id,
        origin_turn_id="c"*32,idempotency_key="archive-group",jobs=[request("archive")]))
    job=jobs[0]
    try:
        with pytest.raises(RuntimeError,match="active_jobs_prevent_archival"):
            await store.set_thread_archived(thread.id,"owner",archived=True)
        attempt=await store.claim_job(job.id,"worker","node",30)
        await store.transition_job(job.id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result={"ok":True})
        with pytest.raises(RuntimeError,match="active_jobs_prevent_archival"):
            await store.set_thread_archived(thread.id,"owner",archived=True)
        async with store.connections.begin() as db:
            outbox=await db.get(JobOutboxRow,(await store.get_job_outbox(group.id)).id,
                with_for_update=True);outbox.state="blocked"
        await store.set_thread_archived(thread.id,"owner",archived=True)
        assert await store.claim_job_outbox_batch("dispatcher")==[]
        await store.set_thread_archived(thread.id,"owner",archived=False)
        assert (await store.get_job_outbox(group.id)).state=="ready"
    finally:await store.close()


async def test_resource_hold_covers_ttl_and_releases_exclusive_gpu_last(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"resource-hold.db"));await store.initialize()
    scheduler=Scheduler(store,reservation_seconds=.3)
    await scheduler.register_node(NodeInventory(node_id="gpu-node",generation=1,cpu_total=8,
        memory_mb_total=16000,disk_mb_total=100000,profiles=(NodeProfile(
            runtime_profile_id="gpu",harness="codex",image_digest="d"*64),),
        gpus=(GPUInventory(id="gpu0",type="A100",memory_mb=40000),)))
    request_value=ResourceRequest(child_profile_id="child",runtime_profile_id="gpu",
        harness="codex",image_digest="d"*64,gpu_count=1,gpu_type="A100")
    first=await scheduler.reserve("1"*32,1,request_value)
    try:
        async with scheduler.hold(first.id,first.node_id,first.node_generation):
            await asyncio.sleep(.75)
            with pytest.raises(RuntimeError,match="no_compatible_node_capacity"):
                await scheduler.reserve("2"*32,1,request_value)
        assert await scheduler.release(first.id,first.node_id,first.node_generation)
        assert not await scheduler.release(first.id,first.node_id,first.node_generation)
        second=await scheduler.reserve("2"*32,1,request_value)
        assert second.gpu_ids==("gpu0",)
    finally:await store.close()


async def test_thread_delete_blocks_generic_cleanup_then_removes_complete_job_history(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"delete-jobs.db"));await store.initialize()
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="Delete jobs"),profile,"owner")
    group,jobs=await store.create_job_group("owner",JobGroupCreate(thread_id=thread.id,
        origin_turn_id="c"*32,idempotency_key="delete-job-group",jobs=[request("delete")],
        suppress_continuation=True));job=jobs[0]
    try:
        with pytest.raises(RuntimeError,match="active_jobs_prevent_thread_delete"):
            await store.delete_thread(thread.id,"owner")
        attempt=await store.claim_job(job.id,"worker","node",30)
        await store.record_job_acceptance(job.id,"worker",attempt.generation,
            external_execution_id="external",receipt={"accepted":True})
        await store.transition_job(job.id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result={"ok":True})
        with pytest.raises(RuntimeError,match="active_jobs_prevent_thread_delete"):
            await store.delete_thread(thread.id,"owner")
        cleanup=(await store.claim_job_cleanups("cleanup",30,limit=1))[0]
        await store.finish_job_cleanup(cleanup["cleanup_id"],"cleanup",succeeded=True)
        await store.delete_thread(thread.id,"owner")
        assert await store.get_thread(thread.id) is None
        assert await store.get_job(job.id) is None and await store.get_job_group(group.id) is None
    finally:await store.close()


class PhasedCleanupAdapter(BaseJobAdapter):
    def __init__(self):self.external_calls=0;self.release_calls=0;self.release_effects=0;self.released=False
    async def cleanup_external(self,context,external):self.external_calls+=1
    async def release_capacity(self,context):
        self.release_calls+=1
        if not self.released:self.released=True;self.release_effects+=1


class PreAcceptCleanupAdapter(BaseJobAdapter):
    def __init__(self):self.external_calls=0;self.release_calls=0
    async def cleanup_external(self,context,external):
        self.external_calls+=1
        raise RuntimeError('external cleanup must not run before acceptance')
    async def release_capacity(self,context):self.release_calls+=1


async def test_preaccept_cleanup_skips_external_quiescence_and_recovers_legacy_rows(tmp_path):
    store,_,job=await setup(tmp_path,request('preaccept-cleanup'))
    attempt=await store.claim_job(job.id,'worker','node',30)
    scheduler=Scheduler(store,reservation_seconds=30)
    await scheduler.register_node(NodeInventory(node_id='node',generation=1,cpu_total=8,
        memory_mb_total=16000,disk_mb_total=100000,profiles=(NodeProfile(
            runtime_profile_id='general',harness='claude',image_digest='d'*64),)))
    reservation=await scheduler.reserve(job.id,attempt.generation,ResourceRequest(
        child_profile_id='child',runtime_profile_id='general',harness='claude',
        image_digest='d'*64))
    await scheduler.release(reservation.id,reservation.node_id,reservation.node_generation)
    await store.transition_job(job.id,JobState.FAILED,expected_generation=attempt.generation,
        result={'error_code':'preaccept_failed'})
    adapter=PreAcceptCleanupAdapter();worker=JobWorker(store,{'provider':adapter},
        node_id='node',worker_id='cleanup')
    try:
        cleanup=await store.get_job_cleanup(job.id)
        assert cleanup['phase']=='capacity_release'

        # Old releases wrote this phase even though no external execution existed.
        async with store.connections.begin() as db:
            from sandbox_harness.store import JobCleanupRow
            row=(await db.execute(__import__('sqlalchemy').select(JobCleanupRow).where(
                JobCleanupRow.job_id==job.id).with_for_update())).scalar_one()
            row.phase='external_quiescence'
        await worker.run_cleanup_once(limit=1)
        cleanup=await store.get_job_cleanup(job.id)
        assert cleanup['state']=='completed' and cleanup['phase']=='completed'
        assert adapter.external_calls==0 and adapter.release_calls==1
    finally:await store.close()


async def test_takeover_terminal_state_cleans_older_accepted_generation(tmp_path):
    store,_,job=await setup(tmp_path,request('takeover-cleanup'))
    first=await store.claim_job(job.id,'first','node',30)
    await store.record_job_acceptance(job.id,'first',first.generation,
        external_execution_id='external',receipt={'accepted':True})
    await store.release_job_lease(job.id,'first',first.generation)
    second=await store.claim_job(job.id,'second','node',30,retry_kind='recovery')
    await store.transition_job(job.id,JobState.SUCCEEDED,expected_generation=second.generation,
        result={'recovered':True})
    adapter=PhasedCleanupAdapter();worker=JobWorker(store,{'provider':adapter},
        node_id='node',worker_id='cleanup')
    try:
        cleanup=await store.get_job_cleanup(job.id)
        assert cleanup['generation']==first.generation and cleanup['state']=='pending'
        await worker.run_cleanup_once(limit=1)
        cleanup=await store.get_job_cleanup(job.id)
        assert cleanup['state']=='completed'
        assert adapter.external_calls==1 and adapter.release_calls==1
    finally:await store.close()


async def cleanup_fixture(tmp_path,name):
    store,_,job=await setup(tmp_path,request(name));attempt=await store.claim_job(job.id,"business","node",30)
    await store.record_job_acceptance(job.id,"business",attempt.generation,
        external_execution_id="external",receipt={"accepted":True})
    await store.transition_job(job.id,JobState.SUCCEEDED,
        expected_generation=attempt.generation,result={"ok":True})
    return store,job


async def test_cleanup_phase_recovery_after_external_quiescence_and_capacity_release(tmp_path):
    store,job=await cleanup_fixture(tmp_path,"phase");adapter=PhasedCleanupAdapter()
    try:
        claimed=(await store.claim_job_cleanups("crashed",0,limit=1))[0]
        context_job=await store.get_job(job.id);context_request=await store.get_job_request(job.id)
        from sandbox_harness.job_adapters import JobExecutionContext
        context=JobExecutionContext(job=context_job,request=context_request,generation=claimed["generation"],
            previous_external_execution_id="external",previous_acceptance_receipt={"accepted":True})
        await adapter.cleanup_external(context,"external")
        await store.advance_job_cleanup(claimed["cleanup_id"],"crashed",
            expected_phase="external_quiescence",next_phase="capacity_release")
        replacement=JobWorker(store,{"provider":adapter},node_id="node",worker_id="replacement")
        await replacement.run_cleanup_once(limit=1)
        assert adapter.external_calls==1 and adapter.release_effects==1
        assert (await store.get_job_cleanup(job.id))["state"]=="completed"

        # Re-open the capacity phase to model process death after idempotent release but before completion.
        async with store.connections.begin() as db:
            from sandbox_harness.store import JobCleanupRow
            row=(await db.execute(__import__('sqlalchemy').select(JobCleanupRow).where(
                JobCleanupRow.job_id==job.id).with_for_update())).scalar_one()
            row.state="pending";row.phase="capacity_release";row.claim_owner="";row.claim_expires_at=None
        await replacement.run_cleanup_once(limit=1)
        assert adapter.external_calls==1 and adapter.release_calls==2 and adapter.release_effects==1
        assert (await store.get_job_cleanup(job.id))["state"]=="completed"
    finally:await store.close()
