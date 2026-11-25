import pytest
import httpx
import asyncio
from types import SimpleNamespace

from sandbox_harness.harness_job import HarnessJobAdapter,JobGrantFactory,SandboxChildBackend
from sandbox_harness.job_adapters import (AdapterStatus,JobExecutionContext,PreAcceptFailure,
    SubmissionOutcomeUnknown)
from sandbox_harness.job_contracts import (JobGroupCreate,JobExpectedOutput,JobKind,JobRequest,
    JobState,OutputDisposition)
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.objects import ObjectStore
from sandbox_harness.sandbox import Sandbox,SandboxCapacityUnavailable,SandboxNotFound
from sandbox_harness.sandbox_contracts import SandboxRuntimeRoute
from sandbox_harness.scheduler import NodeInventory,Scheduler
from sandbox_harness.runtime import StagedRuntimeObject
from sandbox_harness.store import Store


def sandbox_receipt(sandbox,**extra):
    return {"sandbox_id":sandbox.id,"sandbox_handle":sandbox.handle.to_mapping(),**extra}


def missing_receipt(identity):
    return {"sandbox_id":identity,"sandbox_handle":{"provider":"fake","sandbox_id":identity,
        "allocation_id":"job-allocation","profile_key":"a"*63,"revision":"revision-1"}}


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


async def test_child_harness_grant_can_read_its_scoped_execution_catalog():
    frozen=request()
    context=JobExecutionContext(job=SimpleNamespace(thread_id='a'*32,origin_turn_id='b'*32,
        group_id='c'*32,id='d'*32),request=frozen,generation=1)
    _,claims=await JobGrantFactory(Settings(backend='fake'))(context,
        SimpleNamespace(harness='codex'),'sandbox-1')
    assert '/v1/execution/catalog' in claims.routes
    assert 'check' in claims.operations


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


async def test_child_harness_maps_sandbox_capacity_to_pre_accept_wait(tmp_path):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(True,JobState.SUCCEEDED,{}))
    async def unavailable(context,reservation):
        raise SandboxCapacityUnavailable('provider capacity',provider='opensandbox')
    backend.start=unavailable
    try:
        frozen=await store.get_job_request(job.id)
        with pytest.raises(PreAcceptFailure) as raised:
            await adapter.submit(JobExecutionContext(job=job,request=frozen,generation=1))
        assert raised.value.infrastructure and raised.value.retryable
        assert raised.value.retry_class=='capacity_wait'
        assert raised.value.error_code=='infrastructure_capacity_wait'
    finally:await store.close()


@pytest.mark.parametrize('phase',['configuration','submission','cancelled_submission'])
async def test_child_start_failure_retains_correct_submission_boundary(tmp_path,phase):
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    sandbox=Sandbox(id='child-boundary',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    class Pool:
        released=[]
        backend=SimpleNamespace(runtime_route=lambda value:SandboxRuntimeRoute(
            'http://gateway:29880','http://proxy:29881'))
        async def acquire(self,**kwargs):return sandbox
        async def release(self,value,**kwargs):self.released.append(value.id)
    class Runtime:
        starts=0
        async def inject(self,*args,**kwargs):
            if phase=='configuration':
                raise httpx.HTTPStatusError('startup unavailable',
                    request=httpx.Request('PUT','http://runtime/bootstrap/code-bundle'),
                    response=httpx.Response(503))
        async def start_execution(self,*args,**kwargs):
            self.starts+=1
            if phase=='cancelled_submission':raise asyncio.CancelledError()
            raise httpx.ReadError('accepted response lost')
    async def token(*args):return 'token',SimpleNamespace(nonce='nonce')
    pool=Pool();runtime=Runtime()
    adapter.backend=SandboxChildBackend(pool=pool,runtime=runtime,store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path,
            runtime_image_codex='image'),
        token_factory=token)
    try:
        attempt=await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',attempt.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=attempt.generation)
        if phase=='configuration':
            with pytest.raises(PreAcceptFailure) as raised:await adapter.submit(context)
            assert raised.value.error_code=='child_preparation_failed'
            assert raised.value.retryable and raised.value.retry_class=='bounded_pre_accept'
            assert runtime.starts==0 and pool.released==[sandbox.id]
            assert await adapter.scheduler.active_reservation(job.id) is None
            assert await store.latest_job_acceptance(job.id) is None
        elif phase=='submission':
            with pytest.raises(SubmissionOutcomeUnknown) as raised:await adapter.submit(context)
            assert raised.value.external_execution_id==job.id+':1'
            receipt=raised.value.acceptance_receipt
            assert receipt['sandbox_id']==sandbox.id and receipt['reservation_id']
            assert runtime.starts==1 and pool.released==[]
            assert await adapter.scheduler.active_reservation(job.id) is not None
        else:
            with pytest.raises(asyncio.CancelledError):await adapter.submit(context)
            assert runtime.starts==1 and pool.released==[]
            assert await adapter.scheduler.active_reservation(job.id) is not None
            async def cleanup(context,execution_id,receipt):
                assert execution_id==job.id+':1' and receipt['sandbox_id']==sandbox.id
                assert await adapter.scheduler.active_reservation(job.id) is not None
                await pool.release(sandbox)
            adapter.backend.cleanup=cleanup
            worker=JobWorker(store,{'child-harness':adapter},node_id='node',worker_id='worker')
            await worker._timeout(adapter,context,'',submission_started=True)
            assert await worker.run_cleanup_once()==1
            assert pool.released==[sandbox.id]
            assert await store.pending_job_cleanup_ids()==set()
            assert await adapter.scheduler.active_reservation(job.id) is None
    finally:await store.close()


async def test_terminal_preparation_failure_finishes_provisional_cleanup(tmp_path):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(False))
    async def preparation_failed(context,reservation):
        await store.record_job_submission_allocation(context.job.id,context.generation,
            external_execution_id='provisional',receipt={'sandbox_id':'sandbox-1',
                'reservation_id':reservation.id,'node_id':reservation.node_id,
                'node_generation':reservation.node_generation})
        raise PreAcceptFailure('preparation failed',error_code='child_preparation_failed')
    backend.start=preparation_failed
    try:
        worker=JobWorker(store,{'child-harness':adapter},node_id='scheduler',worker_id='worker')
        await worker.run_once()
        assert (await store.get_job(job.id)).state==JobState.FAILED
        assert backend.cleaned==['provisional']
        assert await store.pending_job_cleanup_ids()==set()
        assert await adapter.scheduler.active_reservation(job.id) is None
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
    text_only=HarnessJobAdapter.execution_prompt(request(expected_outputs=()))
    assert 'text-only' in text_only and 'Do not create' in text_only
    assert '/workspace/outputs' in text_only


def test_child_prompt_names_exact_materialized_input_paths():
    bound=request(input_artifacts=({'artifact_id':'a'*32,'artifact_role':'source'},))
    prompt=HarnessJobAdapter.execution_prompt(bound)
    assert 'assets/inputs/source-'+'a'*32 in prompt
    assert 'artifact_id='+'a'*32 in prompt


async def test_opensandbox_child_collects_output_into_atomic_delivery(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'collect.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Child output'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='delegate',
        idempotency_key='delegate-output-turn'),profile,'owner')
    expected=JobExpectedOutput(artifact_role='images',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('image/png',),validators=('nonempty',))
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
    backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=Runtime(),store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    backend.sessions[job.id]=sandbox
    try:
        result=await backend.collect(JobExecutionContext(job=job,request=frozen,generation=attempt.generation,
            previous_acceptance_receipt=sandbox_receipt(sandbox)),'execution',
            AdapterStatus(True,JobState.SUCCEEDED,{'summary':'done'}),sandbox_receipt(sandbox))
        assert result['collections'][0]['state']=='published'
        artifacts=await store.list_job_artifacts(job.id)
        assert len(artifacts)==1 and artifacts[0].output_disposition==OutputDisposition.DELIVERY
        files=await store.current_project_files(thread.id,'owner')
        assert [value.path for value in files]==['artifacts/jobs/'+job.id+'/result.png']
    finally:await store.close()


async def test_opensandbox_child_cannot_succeed_without_required_artifact(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'missing-output.db'));await store.initialize()
    expected=JobExpectedOutput(artifact_role='images',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=2,mime_patterns=('image/png',))
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='c'*32,
        origin_turn_id='d'*32,idempotency_key='missing-output-group',jobs=[request(expected_outputs=(expected,))]))
    job=await store.get_job(jobs[0].id);frozen=await store.get_job_request(job.id)
    class Runtime:
        async def artifacts(self,*args):return []
    sandbox=Sandbox(id='sandbox-1',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=Runtime(),store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None);backend.sessions[job.id]=sandbox
    try:
        await store.append_job_activity(job.id,1,'text_delta',{'text':'缺少受控图片能力'})
        with pytest.raises(RuntimeError,match='required_child_output_missing') as raised:
            await backend.collect(JobExecutionContext(job=job,request=frozen,generation=0,
                previous_acceptance_receipt=sandbox_receipt(sandbox)),'execution',
                AdapterStatus(True,JobState.SUCCEEDED,{}),sandbox_receipt(sandbox))
        assert raised.value.result['child_summary']=='缺少受控图片能力'
    finally:await store.close()


async def test_text_only_child_result_returns_persisted_summary(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'text-output.db'));await store.initialize()
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='c'*32,
        origin_turn_id='d'*32,idempotency_key='text-output-group',jobs=[request()]))
    job=await store.get_job(jobs[0].id);frozen=await store.get_job_request(job.id)
    class Runtime:
        async def artifacts(self,*args):return []
    sandbox=Sandbox(id='sandbox-text',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=Runtime(),store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None);backend.sessions[job.id]=sandbox
    try:
        await store.append_job_activity(job.id,1,'text_delta',{'text':'第一条结论。'})
        await store.append_job_activity(job.id,2,'text_delta',{'text':'第二条结论。'})
        result=await backend.collect(JobExecutionContext(job=job,request=frozen,generation=0,
            previous_acceptance_receipt=sandbox_receipt(sandbox)),'execution',
            AdapterStatus(True,JobState.SUCCEEDED,{'status':'completed'}),sandbox_receipt(sandbox))
        assert result['child_summary']=='第一条结论。第二条结论。'
    finally:await store.close()


async def test_missing_child_sandbox_is_terminal_not_an_infinite_pending_status(tmp_path):
    sandbox=Sandbox(id='sandbox-lost',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    class Runtime:
        async def execution_status(self,*args):return None
    class Backend:
        async def get(self,*args):raise SandboxNotFound('gone')
    backend=SandboxChildBackend(pool=SimpleNamespace(backend=Backend()),runtime=Runtime(),
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)
    backend.sessions['job-lost']=sandbox
    context=SimpleNamespace(job=SimpleNamespace(id='job-lost'))
    value=await backend.status(context,'execution',sandbox_receipt(sandbox))
    assert value.terminal and value.state==JobState.FAILED
    assert value.result['error_code']=='child_sandbox_lost'

    class Pool:
        async def get_attached(self,*args):return None
    async def missing(*args):raise SandboxNotFound('gone')
    Pool.backend=SimpleNamespace(get=missing)
    uncached=SandboxChildBackend(pool=Pool(),runtime=Runtime(),store=SimpleNamespace(),
        objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    value=await uncached.status(context,'execution',sandbox_receipt(sandbox))
    assert value.terminal and value.result['error_code']=='child_sandbox_lost'


async def test_child_submission_recovery_adopts_unique_runtime_execution(tmp_path):
    digest='b'*64;job_id='c'*32
    sandbox=Sandbox(id='sandbox-recovered',harness=Harness.CODEX,
        image='registry/office@sha256:'+digest,runtime_url='http://runtime',metadata={},
        health_state='ready')
    class Inventory:
        async def get(self,handle):assert handle==sandbox.handle;return sandbox
    class Pool:
        backend=Inventory();released=[]
        async def adopt(self,value,**kwargs):
            assert value is sandbox and kwargs['expected_profile'] is sandbox.profile
            return sandbox
        async def release(self,value,**kwargs):self.released.append(value.id)
    class Runtime:
        async def execution_status(self,value,execution_id):
            assert value is sandbox and execution_id==job_id+':1'
            return {'result':None}
    backend=SandboxChildBackend(pool=Pool(),runtime=Runtime(),store=SimpleNamespace(),
        objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    backend._ensure_collector=lambda *args:None
    context=SimpleNamespace(job=SimpleNamespace(id=job_id),
        previous_acceptance_receipt=sandbox_receipt(sandbox,capability_nonce='nonce-old'))
    reservation=SimpleNamespace(id='reservation',job_generation=1,harness='codex',
        image_digest=digest,node_id='node',node_generation=7,runtime_profile_id='office',gpu_ids=())
    recovery=await backend.recover_submission(context,reservation)
    assert recovery.submission.external_execution_id==job_id+':1'
    assert recovery.submission.acceptance_receipt['sandbox_id']==sandbox.id
    assert recovery.submission.acceptance_receipt['capability_nonce']=='nonce-old'
    assert recovery.submission.acceptance_receipt['runtime_acceptance']['recovered'] is True
    assert Pool.released==[]


async def test_child_status_adopts_the_profile_frozen_in_the_persisted_handle(tmp_path):
    job_id='e'*32
    sandbox=Sandbox(id='sandbox-status-recovered',harness=Harness.CODEX,
        image='registry/office@sha256:'+'b'*64,runtime_url='http://runtime',metadata={})
    class Inventory:
        async def get(self,handle):assert handle==sandbox.handle;return sandbox
    class Pool:
        backend=Inventory();adopted_profile=None
        async def get_attached(self,*args):return None
        async def adopt(self,value,**kwargs):
            self.adopted_profile=kwargs.get('expected_profile');return value
    class Runtime:
        async def execution_status(self,value,execution_id):
            assert value is sandbox and execution_id=='execution';return {'result':None}
    pool=Pool();backend=SandboxChildBackend(pool=pool,runtime=Runtime(),store=SimpleNamespace(),
        objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    backend._ensure_collector=lambda *args:None
    context=SimpleNamespace(job=SimpleNamespace(id=job_id))
    status=await backend.status(context,'execution',sandbox_receipt(sandbox))
    assert not status.terminal
    assert pool.adopted_profile is sandbox.profile


async def test_child_submission_recovery_does_not_replay_when_recorded_sandbox_is_missing(tmp_path):
    digest='b'*64;job_id='d'*32
    class Inventory:
        row=None
        async def get(self,identity):
            if self.row is None:raise SandboxNotFound('gone')
            return self.row
    class Pool:
        backend=Inventory();released=[]
        async def adopt(self,value,**kwargs):return value
        async def release(self,value,**kwargs):self.released.append(value.id)
    class Runtime:
        async def execution_status(self,*args):return None
    backend=SandboxChildBackend(pool=Pool(),runtime=Runtime(),store=SimpleNamespace(),
        objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None)
    context=SimpleNamespace(job=SimpleNamespace(id=job_id),
        previous_acceptance_receipt=missing_receipt('recorded-sandbox'))
    reservation=SimpleNamespace(id='reservation',job_generation=1,harness='codex',
        image_digest=digest,node_id='node',node_generation=7,runtime_profile_id='office',gpu_ids=())
    assert (await backend.recover_submission(context,reservation)).safe_to_retry is False
    sandbox=Sandbox(id='sandbox-no-execution',harness=Harness.CODEX,
        image='registry/office@sha256:'+digest,runtime_url='http://runtime',metadata={},
        health_state='ready')
    Pool.backend.row=sandbox
    context.previous_acceptance_receipt=sandbox_receipt(sandbox)
    assert (await backend.recover_submission(context,reservation)).safe_to_retry is True
    assert Pool.released==[sandbox.id]


async def test_failed_child_result_does_not_reopen_missing_sandbox(tmp_path):
    backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=SimpleNamespace(),
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)
    result=await backend.collect(SimpleNamespace(), 'execution',
        AdapterStatus(True,JobState.FAILED,{'error_code':'child_sandbox_lost'}),{})
    assert result=={'error_code':'child_sandbox_lost'}


async def test_child_resumes_same_native_session_with_nested_group_results(tmp_path):
    sandbox=Sandbox(id='sandbox-child',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    class Store:
        marked=[]
        async def child_continuation(self,job_id):return {'id':'continuation','group_id':'g'*32,
            'outbox_id':'o'*32,'state':'ready','result':{'results':[{'state':'SUCCEEDED'}]},
            'resume_execution_id':''}
        async def mark_child_continuation_resumed(self,*args):self.marked.append(args)
        async def child_continuation_artifacts(self,*args):return []
        async def child_has_open_group(self,*args):return False
        async def list_job_activities(self,*args):return []
    class Runtime:
        started=[]
        async def execution_status(self,*args):return {'result':{'status':'completed','native_thread_id':'native-child'}}
        async def start_execution(self,*args,**kwargs):self.started.append(kwargs);return {'accepted':True}
        async def resume(self,*args,**kwargs):
            if False:yield None
    store=Store();runtime=Runtime();backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=runtime,
        store=store,objects=SimpleNamespace(),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None);backend.sessions['j'*32]=sandbox
    context=SimpleNamespace(job=SimpleNamespace(id='j'*32),generation=1,
        request=request(parameters={'objective':'continue','harness':'codex','reasoning_effort':'high'}))
    status=await backend.status(context,'initial-execution',sandbox_receipt(sandbox))
    assert not status.terminal and runtime.started[0]['native_thread_id']=='native-child'
    assert store.marked[0][1]=='j'*32+':1:group:'+'g'*32


async def test_child_inputs_stream_without_object_get_and_enforce_reserved_disk(tmp_path):
    digest="a"*64;image="template-reviewer"
    sandbox=Sandbox(id="sandbox-stream",harness=Harness.CODEX,image=image,
        runtime_url="http://runtime",metadata={})
    chunks=[b"x"*(1024*1024),b"y"*(1024*1024),b"z"*17]
    artifact=SimpleNamespace(id="a"*32,object_ref="sha256:"+"b"*64,
        size=sum(map(len,chunks)),mime="application/octet-stream")
    class Store:
        allocations=[]
        async def get_artifact(self,*args):return artifact
        async def get_pause_participant(self,*args):return None
        async def record_job_submission_allocation(self,*args,**kwargs):
            self.allocations.append(kwargs["receipt"].copy())
    class Objects:
        def get(self,*args,**kwargs):raise AssertionError("full object buffering is forbidden")
        def iter_bytes(self,*args,**kwargs):yield from chunks
    class Pool:
        acquired=0;acquired_kwargs={}
        backend=SimpleNamespace(runtime_route=lambda sandbox:SandboxRuntimeRoute(
            "http://sandbox-gateway:29880","http://sandbox-proxy:29881"))
        async def acquire(self,**kwargs):
            self.acquired+=1;self.acquired_kwargs=kwargs;return sandbox
        async def release(self,*args,**kwargs):pass
    class Runtime:
        sizes=[];injected={}
        async def inject(self,*args,**kwargs):self.injected=kwargs
        async def put_project_file_stream(self,sandbox,path,content,*,size,mime):
            seen=0
            async for value in content:self.sizes.append(len(value));seen+=len(value)
            assert seen==size and path.startswith("assets/inputs/")
        async def start_execution(self,*args,**kwargs):return {"accepted":True}
    async def token(*args):return "token",SimpleNamespace(nonce="nonce")
    class Profiles:
        def provider_release(self,*args,**kwargs):
            assert args==("reviewer-readonly","codex")
            assert kwargs=={"provider_ref":image,"artifact_digest":digest}
            return SimpleNamespace(ref=image,revision="runtime-config-v2",artifact_digest=digest)
    settings=Settings(backend="fake",data_root=tmp_path,max_artifact_bytes=10*1024*1024,
        runtime_image_codex=image)
    backend=SandboxChildBackend(pool=Pool(),runtime=Runtime(),store=Store(),objects=Objects(),
        settings=settings,token_factory=token,profile_resolver=Profiles())
    backend._ensure_collector=lambda *args:None
    input_value=__import__('sandbox_harness.job_contracts',fromlist=['JobArtifactInput']).JobArtifactInput(
        artifact_id=artifact.id,artifact_role="report")
    frozen=request().model_copy(update={"input_artifacts":(input_value,)})
    context=JobExecutionContext(job=SimpleNamespace(id="c"*32,thread_id="d"*32,
        origin_turn_id="e"*32),
        request=frozen,generation=1)
    reservation=SimpleNamespace(harness="codex",image_ref=image,image_digest=digest,
        disk_mb=516,runtime_profile_id="reviewer-readonly",id="reservation",
        node_id="node",node_generation=1)
    execution,_=await backend.start(context,reservation)
    assert execution.startswith("c"*32) and Runtime.sizes==[len(value) for value in chunks]
    assert backend.runtime.injected['logical_thread_id']=='d'*32
    assert backend.runtime.injected['logical_turn_id']=='e'*32
    assert backend.runtime.injected['gateway_url']=='http://sandbox-gateway:29880'
    assert backend.pool.acquired_kwargs['profile_id']=='reviewer-readonly'
    assert backend.pool.acquired_kwargs['revision']=='runtime-config-v2'
    assert backend.store.allocations==[
        {"sandbox_id":sandbox.id,"sandbox_handle":sandbox.handle.to_mapping(),
            "reservation_id":"reservation","node_id":"node",
            "node_generation":1},
        {"sandbox_id":sandbox.id,"sandbox_handle":sandbox.handle.to_mapping(),
            "reservation_id":"reservation","node_id":"node",
            "node_generation":1,"capability_nonce":"nonce"}]

    backend.pool.acquired=0
    with pytest.raises(ValueError,match="reserved_disk"):
        await backend.start(context,SimpleNamespace(**{**reservation.__dict__,"disk_mb":514}))
    assert backend.pool.acquired==0
