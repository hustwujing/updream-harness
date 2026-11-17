import pytest
from types import SimpleNamespace

from sandbox_harness.harness_job import HarnessJobAdapter,OpenSandboxChildBackend
from sandbox_harness.job_adapters import AdapterStatus,JobExecutionContext
from sandbox_harness.job_contracts import (JobGroupCreate,JobExpectedOutput,JobKind,JobRequest,
    JobState,OutputDisposition)
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.objects import ObjectStore
from sandbox_harness.sandbox import Sandbox,SandboxNotFound
from sandbox_harness.scheduler import NodeInventory,Scheduler
from sandbox_harness.runtime import StagedRuntimeObject
from sandbox_harness.store import Store


class Backend:
    def __init__(self,status):self.next_status=status;self.started=[];self.cancelled=[];self.cleaned=[];self.collected=[]
    async def start(self,context,reservation):
        self.started.append((context,reservation));return 'runtime-execution',{'sandbox_id':'sandbox-1'}
    async def status(self,context,external_execution_id,receipt):
        assert receipt['sandbox_id']=='sandbox-1';return self.next_status
    async def cancel(self,context,external_execution_id,receipt):
        self.cancelled.append(external_execution_id);return AdapterStatus(True,JobState.CANCELLED,{'cancelled':True})
    async def cleanup(self,context,external_execution_id,receipt):self.cleaned.append(external_execution_id)
    async def collect(self,context,external_execution_id,status,receipt):
        self.collected.append(external_execution_id);return status.result


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
        assert len(backend.started)==1 and backend.collected==['runtime-execution']
        assert backend.cleaned==['runtime-execution']
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


def test_child_prompt_freezes_output_contract_and_reviewer_verdict_path():
    output=JobExpectedOutput(artifact_role='reports',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=2,mime_patterns=('application/pdf',))
    normal=HarnessJobAdapter.execution_prompt(request(expected_outputs=(output,)))
    assert 'job-output-manifest.json' in normal and 'reports' in normal and 'application/pdf' in normal
    reviewer=HarnessJobAdapter.execution_prompt(request(adapter='reviewer-harness',expected_outputs=(),
        parameters={'objective':'Check reports','harness':'codex','review_context':{
            'artifact_digests':['a'*64],'required':True,'trigger':'user'}}))
    assert 'review-verdict.json' in reviewer and 'artifact_digests' in reviewer


async def test_opensandbox_child_collects_output_into_atomic_delivery(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'collect.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Child output'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='delegate',
        idempotency_key='delegate-output-turn'),profile,'owner')
    expected=JobExpectedOutput(artifact_role='images',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('image/png',))
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='collect-child-group',jobs=[request(expected_outputs=(expected,))]))
    attempt=(await store.claim_available_jobs('worker','node',60,limit=1,adapters={'child-harness'}))[0]
    job=await store.get_job(jobs[0].id);frozen=await store.get_job_request(job.id)
    data=b'\x89PNG\r\n\x1a\nchild-output'
    class Runtime:
        async def artifacts(self,sandbox,batch):
            assert batch==job.id;return [{'path':'result.png','size':len(data),'mime':'image/png',
                'sha256':__import__('hashlib').sha256(data).hexdigest()}]
        async def artifact(self,sandbox,path,*,maximum):
            assert path=='result.png';target=tmp_path/'staged-result.png';target.write_bytes(data)
            return StagedRuntimeObject(target,len(data),__import__('hashlib').sha256(data).hexdigest())
    sandbox=Sandbox(id='sandbox-1',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    backend=OpenSandboxChildBackend(pool=SimpleNamespace(),runtime=Runtime(),store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    backend.sessions[job.id]=sandbox
    try:
        result=await backend.collect(JobExecutionContext(job=job,request=frozen,generation=attempt.generation,
            previous_acceptance_receipt={'sandbox_id':sandbox.id}),'execution',
            AdapterStatus(True,JobState.SUCCEEDED,{'summary':'done'}),{'sandbox_id':sandbox.id})
        assert result['collections'][0]['state']=='published'
        artifacts=await store.list_job_artifacts(job.id)
        assert len(artifacts)==1 and artifacts[0].output_disposition==OutputDisposition.DELIVERY
        files=await store.current_project_files(thread.id,'owner')
        assert [value.path for value in files]==['artifacts/jobs/'+job.id+'/result.png']
    finally:await store.close()


async def test_missing_child_sandbox_is_terminal_not_an_infinite_pending_status(tmp_path):
    sandbox=Sandbox(id='sandbox-lost',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    class Runtime:
        async def execution_status(self,*args):return None
    class Backend:
        async def get(self,*args):raise SandboxNotFound('gone')
    backend=OpenSandboxChildBackend(pool=SimpleNamespace(backend=Backend()),runtime=Runtime(),
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)
    backend.sessions['job-lost']=sandbox
    context=SimpleNamespace(job=SimpleNamespace(id='job-lost'))
    value=await backend.status(context,'execution',{'sandbox_id':sandbox.id})
    assert value.terminal and value.state==JobState.FAILED
    assert value.result['error_code']=='child_sandbox_lost'

    class Pool:
        async def get_attached(self,*args):return None
    async def missing(*args):raise SandboxNotFound('gone')
    Pool.backend=SimpleNamespace(get=missing)
    uncached=OpenSandboxChildBackend(pool=Pool(),runtime=Runtime(),store=SimpleNamespace(),
        objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    value=await uncached.status(context,'execution',{'sandbox_id':sandbox.id})
    assert value.terminal and value.result['error_code']=='child_sandbox_lost'


async def test_failed_child_result_does_not_reopen_missing_sandbox(tmp_path):
    backend=OpenSandboxChildBackend(pool=SimpleNamespace(),runtime=SimpleNamespace(),
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)
    result=await backend.collect(SimpleNamespace(), 'execution',
        AdapterStatus(True,JobState.FAILED,{'error_code':'child_sandbox_lost'}),{})
    assert result=={'error_code':'child_sandbox_lost'}
