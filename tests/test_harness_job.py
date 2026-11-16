import pytest

from sandbox_harness.harness_job import HarnessJobAdapter
from sandbox_harness.job_adapters import AdapterStatus
from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.scheduler import NodeInventory,Scheduler
from sandbox_harness.store import Store


class Backend:
    def __init__(self,status):self.next_status=status;self.started=[];self.cancelled=[];self.cleaned=[]
    async def start(self,context,reservation):
        self.started.append((context,reservation));return 'runtime-execution',{'sandbox_id':'sandbox-1'}
    async def status(self,context,external_execution_id,receipt):
        assert receipt['sandbox_id']=='sandbox-1';return self.next_status
    async def cancel(self,context,external_execution_id,receipt):
        self.cancelled.append(external_execution_id);return AdapterStatus(True,JobState.CANCELLED,{'cancelled':True})
    async def cleanup(self,context,external_execution_id,receipt):self.cleaned.append(external_execution_id)


def request(**updates):
    value=dict(local_job_id='child',kind=JobKind.HARNESS,adapter='child-harness',
        descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='child-harness-key',parameters={'objective':'inspect files','harness':'codex'},
        child_profile_id='child-general',runtime_profile_id='general-cpu',skill_ids=('writing-plans',),
        resource_requirements={'harness':'codex','image_digest':'b'*64,'delegation_depth':1,
            'cpu':1,'memory_mb':512,'disk_mb':1024})
    value.update(updates);return JobRequest(**value)


async def setup(tmp_path,status):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'harness.db'));await store.initialize()
    scheduler=Scheduler(store);await scheduler.register_node(NodeInventory(node_id='node',generation=1,
        cpu_total=8,memory_mb_total=16000,disk_mb_total=100000,load=0,
        profiles=[{'runtime_profile_id':'general-cpu','harness':'codex','image_digest':'b'*64,'warm_slots':1}]))
    backend=Backend(status);adapter=HarnessJobAdapter(scheduler,backend)
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='c'*32,
        origin_turn_id='d'*32,idempotency_key='child-group-key',jobs=[request()]))
    return store,adapter,backend,jobs[0]


async def test_child_harness_acceptance_status_result_and_cleanup(tmp_path):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(True,JobState.SUCCEEDED,{'summary':'done'}))
    try:
        worker=JobWorker(store,{'child-harness':adapter},node_id='scheduler',worker_id='worker')
        await worker.run_once();value=await store.get_job(job.id)
        assert value.state==JobState.SUCCEEDED and value.result=={'summary':'done'}
        assert len(backend.started)==1 and backend.cleaned==['runtime-execution']
        reservations=[]
        async with store.connections() as db:
            from sqlalchemy import select
            from sandbox_harness.store import ResourceReservationRow
            reservations=list((await db.execute(select(ResourceReservationRow))).scalars())
        assert reservations[0].state=='released'
    finally:await store.close()


async def test_child_missing_business_fact_returns_blocked_not_failed(tmp_path):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(True,JobState.BLOCKED,
        {'reason_code':'missing_business_input','missing_fields':['brand_name'],'questions':['品牌名是什么？']}))
    try:
        await JobWorker(store,{'child-harness':adapter},node_id='scheduler',worker_id='worker').run_once()
        value=await store.get_job(job.id)
        assert value.state==JobState.BLOCKED and value.result['blocked'] is True
        assert value.result['missing_fields']==['brand_name']
    finally:await store.close()


def test_recursive_child_delegation_is_rejected_before_scheduling():
    from types import SimpleNamespace
    context=SimpleNamespace(request=request(resource_requirements={'harness':'codex',
        'image_digest':'b'*64,'delegation_depth':2}))
    with pytest.raises(ValueError,match='recursive_delegation_forbidden'):
        HarnessJobAdapter._resources(context)
