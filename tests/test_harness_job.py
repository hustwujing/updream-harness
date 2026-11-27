import pytest
import httpx
import asyncio
from types import SimpleNamespace

from sandbox_harness.services.job_worker.harness_job import (HarnessJobAdapter, JobGrantFactory, SandboxChildBackend)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, JobExecutionContext, PreAcceptFailure, SubmissionOutcomeUnknown, SubmissionInterrupted)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobExpectedOutput, JobKind, JobRequest, JobState, OutputDisposition)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.sandbox import (Sandbox, SandboxCapacityUnavailable, SandboxNotFound)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxRuntimeRoute)
from sandbox_harness.common.scheduling.scheduler import (NodeInventory, Scheduler)
from sandbox_harness.common.sandbox.runtime import (StagedRuntimeObject)
from test_support.legacy_store import (Store)


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


@pytest.mark.parametrize('adapter',['child-harness','reviewer-harness'])
async def test_job_grant_factory_crossing_second_keeps_parent_expiry(monkeypatch,adapter):
    import sandbox_harness.common.security.grants as grants
    import sandbox_harness.common.security.capability as capability
    ticks=iter([1000,1001,1002])
    clock=SimpleNamespace(time=lambda:next(ticks))
    monkeypatch.setattr(grants,'time',clock);monkeypatch.setattr(capability,'time',clock)
    context=JobExecutionContext(job=SimpleNamespace(thread_id='a'*32,origin_turn_id='b'*32,
        group_id='c'*32,id='d'*32),request=request(adapter=adapter),generation=1)
    _,claims=await JobGrantFactory(Settings(backend='fake',capability_ttl_seconds=60))(
        context,SimpleNamespace(harness='codex'),'sandbox-1')
    assert claims.issued_at==1001 and claims.expires_at==1060


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
            from test_support.legacy_store import (ResourceReservationRow)
            reservations=list((await db.execute(select(ResourceReservationRow))).scalars())
        assert reservations[0].state=='released'
    finally:await store.close()


async def test_child_harness_maps_sandbox_capacity_to_pre_accept_wait(tmp_path):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(True,JobState.SUCCEEDED,{}))
    async def unavailable(context,reservation):
        raise SandboxCapacityUnavailable('provider capacity',provider='opensandbox')
    backend.start=unavailable
    try:
        await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',1)
        frozen=await store.get_job_request(job.id)
        with pytest.raises(PreAcceptFailure) as raised:
            await adapter.submit(JobExecutionContext(job=job,request=frozen,generation=1))
        assert raised.value.infrastructure and raised.value.retryable
        assert raised.value.retry_class=='capacity_wait'
        assert raised.value.error_code=='infrastructure_capacity_wait'
    finally:await store.close()


@pytest.mark.parametrize('phase',['grant','grant_cleanup','grant_cancelled',
    'configuration','submission','cancelled_submission'])
async def test_child_start_failure_retains_correct_submission_boundary(tmp_path,phase):
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    sandbox=Sandbox(id='child-boundary',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    class Pool:
        released=[]
        backend=SimpleNamespace(runtime_route=lambda value:SandboxRuntimeRoute(
            'http://gateway:29880','http://proxy:29881'))
        async def acquire(self,**kwargs):return sandbox
        async def release(self,value,**kwargs):
            if phase=='grant_cleanup':raise httpx.ConnectError('controlled cleanup outage')
            self.released.append(value.id)
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
    async def token(*args):
        if phase in {'grant','grant_cleanup'}:raise ValueError('controlled grant rejection')
        if phase=='grant_cancelled':raise asyncio.CancelledError()
        return 'token',SimpleNamespace(nonce='nonce')
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
        if phase in {'grant','configuration'}:
            with pytest.raises(PreAcceptFailure) as raised:await adapter.submit(context)
            assert raised.value.error_code=='child_preparation_failed'
            assert raised.value.retryable==(phase=='configuration')
            assert raised.value.retry_class==('bounded_pre_accept' if phase=='configuration' else 'none')
            assert runtime.starts==0 and pool.released==[sandbox.id]
            assert await adapter.scheduler.active_reservation(job.id) is None
            assert await store.latest_job_acceptance(job.id) is None
        elif phase=='grant_cleanup':
            with pytest.raises(PreAcceptFailure) as raised:await adapter.submit(context)
            assert raised.value.retain_capacity
            assert runtime.starts==0 and pool.released==[]
            assert await adapter.scheduler.active_reservation(job.id) is not None
        elif phase=='grant_cancelled':
            with pytest.raises(asyncio.CancelledError):await adapter.submit(context)
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


@pytest.mark.parametrize('boundary',['mark_running_cancel','mark_running_failure','resource_heartbeat'])
async def test_submission_reservation_survives_failure_after_external_start(tmp_path,boundary):
    store,adapter,backend,job=await setup(tmp_path,AdapterStatus(False))
    async def mark_failed(*args):
        if boundary=='mark_running_cancel':raise asyncio.CancelledError()
        raise RuntimeError('resource_reservation_lost')
    if boundary=='resource_heartbeat':
        adapter.scheduler.reservation_seconds=.03
        renew=adapter.scheduler.renew;renewals=0
        async def lose_resource(*args):
            nonlocal renewals
            renewals+=1
            if renewals>1:raise RuntimeError('resource_reservation_lost')
            return await renew(*args)
        adapter.scheduler.renew=lose_resource
        async def interrupted(context,reservation):
            backend.started.append((context,reservation))
            try:await asyncio.sleep(10)
            except asyncio.CancelledError as exc:
                raise SubmissionInterrupted('runtime-execution',{'sandbox_id':'sandbox-1'}) from exc
        backend.start=interrupted
    else:adapter.scheduler.mark_running=mark_failed
    try:
        await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',1)
        context=JobExecutionContext(job=job,request=await store.get_job_request(job.id),generation=1)
        if boundary=='mark_running_cancel':
            with pytest.raises(asyncio.CancelledError):await adapter.submit(context)
        else:
            with pytest.raises(SubmissionOutcomeUnknown) as raised:await adapter.submit(context)
            assert raised.value.external_execution_id=='runtime-execution'
            assert raised.value.acceptance_receipt['reservation_id']
            assert raised.value.acceptance_receipt['sandbox_id']=='sandbox-1'
        assert len(backend.started)==1
        assert await adapter.scheduler.active_reservation(job.id) is not None
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
    bound=request(input_artifacts=({'artifact_id':'a'*32,'artifact_role':'input_1'},
        {'artifact_id':'c'*32,'artifact_role':'input_2'}))
    artifact=SimpleNamespace(id='a'*32,path='input/assets/portrait.png',sha256='b'*64)
    second=SimpleNamespace(id='c'*32,path='input/assets/rocket.png',sha256='d'*64)
    bindings=((bound.input_artifacts[0],artifact),(bound.input_artifacts[1],second))
    prompt=HarnessJobAdapter.execution_prompt(bound,bindings)
    assert 'assets/inputs/input_1-'+'a'*32+'/portrait.png' in prompt
    assert 'artifact_id='+'a'*32 in prompt
    assert 'filename=portrait.png' in prompt and 'sha256='+'b'*64 in prompt
    assert 'assets/inputs/input_2-'+'c'*32+'/rocket.png' in prompt
    reviewer=HarnessJobAdapter.execution_prompt(bound.model_copy(update={'adapter':'reviewer-harness',
        'parameters':{'objective':'Check inputs','review_context':{}}}),bindings)
    assert 'filename=portrait.png' in reviewer and 'filename=rocket.png' in reviewer


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


async def test_child_manifest_is_publication_allowlist_for_unlisted_sandbox_files(tmp_path):
    import hashlib,json
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'manifest-allowlist.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Manifest allowlist'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='delegate',
        idempotency_key='manifest-allowlist-turn'),profile,'owner')
    outputs=(JobExpectedOutput(artifact_role='primary',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('text/plain',)),
        JobExpectedOutput(artifact_role='report',output_disposition=OutputDisposition.DELIVERY,
            min_items=1,max_items=1,mime_patterns=('text/plain',)))
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='manifest-allowlist-group',
        jobs=[request(expected_outputs=outputs)]))
    attempt=(await store.claim_available_jobs('worker','node',60,limit=1,
        adapters={'child-harness'}))[0]
    job=await store.get_job(jobs[0].id);frozen=await store.get_job_request(job.id)
    manifest=json.dumps({'outputs':[
        {'path':'final.txt','artifact_role':'primary','item_key':'final','position':0},
        {'path':'report.txt','artifact_role':'report','item_key':'report','position':0},
    ]}).encode()
    values={'job-output-manifest.json':(manifest,'application/json'),
        'final.txt':(b'final','text/plain'),'report.txt':(b'report','text/plain'),
        'scratch.tmp':(b'private scratch','text/plain')}
    class Runtime:
        async def artifacts(self,*args):
            return [{'path':path,'size':len(data),'mime':mime,
                'sha256':hashlib.sha256(data).hexdigest()} for path,(data,mime) in values.items()]
        async def artifact(self,sandbox,path,*,maximum):
            data,_=values[path];target=tmp_path/('staged-'+path);target.write_bytes(data)
            return StagedRuntimeObject(target,len(data),hashlib.sha256(data).hexdigest())
    sandbox=Sandbox(id='sandbox-manifest',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    backend=SandboxChildBackend(pool=SimpleNamespace(),runtime=Runtime(),store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path),
        token_factory=lambda *args:None);backend.sessions[job.id]=sandbox
    try:
        result=await backend.collect(JobExecutionContext(job=job,request=frozen,
            generation=attempt.generation,previous_acceptance_receipt=sandbox_receipt(sandbox)),
            'execution',AdapterStatus(True,JobState.SUCCEEDED,{}),sandbox_receipt(sandbox))
        assert {row['artifact_role'] for row in result['collections']}=={'primary','report'}
        artifacts=await store.list_job_artifacts(job.id)
        assert {row.filename for row in artifacts}=={'final.txt','report.txt'}
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
        with pytest.raises(RuntimeError,match='artifact_collection_cardinality_invalid') as raised:
            await backend.collect(JobExecutionContext(job=job,request=frozen,generation=0,
                previous_acceptance_receipt=sandbox_receipt(sandbox)),'execution',
                AdapterStatus(True,JobState.SUCCEEDED,{}),sandbox_receipt(sandbox))
        assert raised.value.result['child_summary']=='缺少受控图片能力'
        assert raised.value.result['output_contract']=={'artifact_role':'images','min_items':1,
            'max_items':2,'actual_items':0}
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
    artifact=SimpleNamespace(id="a"*32,path="input/assets/report.bin",sha256="b"*64,
        object_ref="sha256:"+"b"*64,
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
        sizes=[];injected={};paths=[];messages=[]
        async def inject(self,*args,**kwargs):self.injected=kwargs
        async def put_project_file_stream(self,sandbox,path,content,*,size,mime):
            self.paths.append(path)
            seen=0
            async for value in content:self.sizes.append(len(value));seen+=len(value)
            assert seen==size and path.startswith("assets/inputs/")
        async def start_execution(self,*args,**kwargs):
            self.messages.append(kwargs["message"]);return {"accepted":True}
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
    input_value=__import__('sandbox_harness.common.contracts.job_contracts',fromlist=['JobArtifactInput']).JobArtifactInput(
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
    expected_path="assets/inputs/report-"+artifact.id+"/report.bin"
    assert Runtime.paths==[expected_path]
    assert expected_path in Runtime.messages[0]
    assert "filename=report.bin" in Runtime.messages[0] and "sha256="+artifact.sha256 in Runtime.messages[0]
    assert backend.runtime.injected['logical_thread_id']=='d'*32
    assert backend.runtime.injected['logical_turn_id']=='e'*32
    assert backend.runtime.injected['gateway_url']=='http://sandbox-gateway:29880'
    assert backend.pool.acquired_kwargs['profile_id']=='reviewer-readonly'
    assert backend.pool.acquired_kwargs['revision']=='runtime-config-v2'
    intent=backend.store.allocations[0]
    assert intent['allocation_id']==context.job.id
    assert intent['allocation_phase']=='awaiting_capacity' and 'sandbox_id' not in intent
    final=backend.store.allocations[-1]
    assert final['allocation_phase']=='runtime_submission'
    assert final['sandbox_id']==sandbox.id and final['sandbox_handle']==sandbox.handle.to_mapping()
    assert final['reservation_id']=='reservation' and final['node_generation']==1
    assert final['capability_nonce']=='nonce'

    backend.pool.acquired=0
    with pytest.raises(ValueError,match="reserved_disk"):
        await backend.start(context,SimpleNamespace(**{**reservation.__dict__,"disk_mb":514}))
    assert backend.pool.acquired==0


async def test_child_reconciles_delayed_allocation_before_one_runtime_submission(tmp_path):
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown)
    class LateCreate(FakeSandboxBackend):
        calls=0;lookups=0;delayed=False
        async def create(self,value):
            self.calls+=1
            intent=await store.unresolved_job_submission(job.id,2)
            assert intent['external_execution_id']==job.id+':1'
            assert intent['acceptance_receipt']['allocation_phase']=='allocating'
            assert await store.latest_job_acceptance(job.id) is None
            await super().create(value)
            self.delayed=True
            raise SandboxCreateOutcomeUnknown('create response unavailable',provider=self.provider)
        async def reconcile_create(self,*args):
            if self.delayed:
                self.lookups+=1
                if self.lookups<3:return None
            return await super().reconcile_create(*args)
    class Runtime:
        starts=0
        async def inject(self,*args,**kwargs):pass
        async def start_execution(self,*args,**kwargs):
            self.starts+=1;return {'accepted':True,'created':True}
    async def token(*args):return 'token',SimpleNamespace(nonce='')
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    provider=LateCreate();pool=SandboxPool(provider,{Harness.CODEX:'image'},
        {Harness.CODEX:0},max_sandboxes=1);runtime=Runtime()
    backend=SandboxChildBackend(pool=pool,runtime=runtime,store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',
            data_root=tmp_path,runtime_image_codex='image'),token_factory=token)
    backend._ensure_collector=lambda *args:None;adapter.backend=backend
    try:
        attempt=await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',attempt.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=attempt.generation)
        submission=await asyncio.wait_for(adapter.submit(context),10)
        assert submission.external_execution_id==job.id+':1'
        assert provider.calls==1 and provider.lookups>=3 and runtime.starts==1
        assert await store.latest_job_acceptance(job.id) is None
        assert await adapter.scheduler.active_reservation(job.id) is not None
        await adapter.cleanup(JobExecutionContext(job=context.job,request=context.request,
            generation=context.generation,previous_acceptance_receipt=submission.acceptance_receipt),
            submission.external_execution_id)
        assert not provider.items and await adapter.scheduler.active_reservation(job.id) is None
    finally:await pool.close();await store.close()


@pytest.mark.parametrize('phase',['reserving_resources','legacy_unknown'])
@pytest.mark.parametrize('takeover',[False,True])
async def test_terminal_job_keeps_prior_reservation_cleanup_even_without_allocation(tmp_path,phase,takeover):
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        if phase!='legacy_unknown':
            await store.record_job_submission_allocation(job.id,first.generation,
                external_execution_id=job.id+':1',receipt={'allocation_phase':phase})
        reservation=await adapter.scheduler.reserve(job.id,first.generation,adapter._resources(context))
        generation=first.generation
        if takeover:
            await store.release_job_lease(job.id,'first',first.generation)
            generation=(await store.claim_job(job.id,'replacement','node',30)).generation
        await store.transition_job(job.id,JobState.UNKNOWN,expected_generation=generation)
        cleanups=await store.claim_job_cleanups('cleanup-worker',30)
        assert len(cleanups)==1
        cleanup=cleanups[0]
        assert cleanup['generation']==first.generation
        assert cleanup['acceptance_receipt']['reservation_id']==reservation.id
        assert cleanup['acceptance_receipt']['allocation_phase']==phase
        assert cleanup['phase']==('external_quiescence' if phase=='legacy_unknown' else 'capacity_release')
    finally:await store.close()


@pytest.mark.parametrize('takeover',[False,True])
async def test_unaccepted_allocation_cleanup_keeps_original_reservation_generation(tmp_path,takeover):
    from dataclasses import asdict
    from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        reservation=await adapter.scheduler.reserve(job.id,first.generation,adapter._resources(context))
        profile=FakeSandboxBackend.legacy_profile(Harness.CODEX,'image')
        receipt={'allocation_id':job.id,'allocation_provider':'fake',
            'allocation_profile':asdict(profile),'allocation_phase':'allocating',
            'reservation_id':reservation.id,'node_id':reservation.node_id,
            'node_generation':reservation.node_generation}
        await store.record_job_submission_allocation(job.id,first.generation,
            external_execution_id=job.id+':1',receipt=receipt)
        generation=first.generation
        if takeover:
            await store.release_job_lease(job.id,'first',first.generation)
            generation=(await store.claim_job(job.id,'replacement','node',30)).generation
        await store.transition_job(job.id,JobState.CANCELLED,expected_generation=generation)
        cleanups=await store.claim_job_cleanups('cleanup-worker',30)
        assert len(cleanups)==1
        cleanup=cleanups[0]
        assert cleanup['phase']=='external_quiescence'
        assert cleanup['generation']==first.generation
        assert cleanup['external_execution_id']==job.id+':1'
        assert cleanup['acceptance_receipt']==receipt
        assert await store.latest_job_acceptance(job.id) is None
    finally:await store.close()


@pytest.mark.parametrize(('replace_worker','mode'),[
    (False,'task'),(True,'task'),(False,'user'),(False,'heartbeat'),(True,'recovery_user')])
@pytest.mark.parametrize('reviewer',[False,True])
async def test_cancelled_allocation_retains_durable_cleanup_and_recovery(
        tmp_path,replace_worker,mode,reviewer):
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown)
    from sandbox_harness.services.job_worker.reviewer import (ReviewerJobAdapter)
    from contextlib import asynccontextmanager
    class PendingCreate(FakeSandboxBackend):
        created=asyncio.Event();request_return=asyncio.Event();reconciled=asyncio.Event()
        visible=False;starts=0;pause=True
        async def create(self,value):
            self.starts+=1;sandbox=await super().create(value);self.created.set()
            if self.pause:
                await self.request_return.wait()
                raise SandboxCreateOutcomeUnknown('response lost',provider=self.provider)
            return sandbox
        async def reconcile_create(self,*args):
            if self.created.is_set():self.reconciled.set()
            if not self.visible:return None
            return await super().reconcile_create(*args)
        async def get(self,*args):raise AssertionError('cleanup must not wait for Runtime readiness')
        async def delete(self,value):
            assert await adapter.scheduler.active_reservation(job.id) is not None
            await super().delete(value)
    class Runtime:
        starts=0
        async def inject(self,*args,**kwargs):pass
        async def start_execution(self,*args,**kwargs):self.starts+=1;return {'accepted':True}
    async def token(*args):return 'token',SimpleNamespace(nonce='')
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False));provider=PendingCreate();runtime=Runtime()
    def make_backend():
        pool=SandboxPool(provider,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=1)
        backend=SandboxChildBackend(pool=pool,runtime=runtime,store=store,
            objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',
                data_root=tmp_path,runtime_image_codex='image'),token_factory=token)
        backend.allocation_poll_seconds=.01;backend._ensure_collector=lambda *args:None
        return pool,backend
    pool,backend=make_backend();adapter.backend=backend;old_pool=None
    lifecycle=ReviewerJobAdapter(adapter) if reviewer else adapter
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        if mode=='user':
            async def done(*args):return AdapterStatus(True,JobState.SUCCEEDED)
            async def collect(*args):return {}
            backend.status=done;backend.collect=collect
            worker=JobWorker(store,{'child-harness':lifecycle},node_id='node',worker_id='first')
            task=asyncio.create_task(worker.process(first,continuous=True))
            await provider.created.wait();provider.request_return.set();await asyncio.sleep(.03)
            await store.request_job_cancel(job.id,'owner');provider.visible=True
            await asyncio.wait_for(task,3)
            assert runtime.starts==0
            assert (await store.get_job(job.id)).state==JobState.CANCELLED
            assert await store.latest_job_acceptance(job.id) is None
            assert not provider.items and await adapter.scheduler.active_reservation(job.id) is None
            return
        if mode=='heartbeat':
            @asynccontextmanager
            async def failed_hold(*args):
                try:yield
                except asyncio.CancelledError as exc:
                    raise RuntimeError('resource_reservation_lost') from exc
            adapter.scheduler.hold=failed_hold
        task=asyncio.create_task(lifecycle.submit(context));await provider.created.wait();task.cancel()
        with pytest.raises(PreAcceptFailure if mode=='heartbeat' else asyncio.CancelledError):await task
        assert runtime.starts==0 and provider.starts==1
        assert await store.latest_job_acceptance(job.id) is None
        assert await adapter.scheduler.active_reservation(job.id) is not None
        provisional=await store.unresolved_job_submission(job.id,first.generation,include_current=True)
        assert provisional['acceptance_receipt']['allocation_phase']=='allocating'
        if replace_worker:
            await store.release_job_lease(job.id,'first',first.generation)
            second=await store.claim_job(job.id,'replacement','node',30)
            old_pool=pool;pool,backend=make_backend();adapter.backend=backend
            context=JobExecutionContext(job=await store.get_job(job.id),request=context.request,
                generation=second.generation,previous_external_execution_id=job.id+':1',
                previous_acceptance_receipt=provisional['acceptance_receipt'])
            if mode=='recovery_user':
                worker=JobWorker(store,{'child-harness':lifecycle},node_id='node',worker_id='replacement')
                task=asyncio.create_task(worker.process(second,continuous=True))
                await asyncio.wait_for(provider.reconciled.wait(),1)
                await store.request_job_cancel(job.id,'owner')
                await asyncio.wait_for(task,1)
                assert (await store.get_job(job.id)).state==JobState.CANCELLED
                assert runtime.starts==0 and await store.latest_job_acceptance(job.id) is None
                assert await store.pending_job_cleanup_ids()=={job.id}
                assert await adapter.scheduler.active_reservation(job.id) is not None
                provider.visible=True;await worker.run_cleanup_once()
                assert not provider.items and await adapter.scheduler.active_reservation(job.id) is None
                return
            provider.visible=True
            recovered=await lifecycle.recover_submission(context)
            assert recovered.safe_to_retry and recovered.submission is None
            assert not provider.items and runtime.starts==0
            saved=await store.unresolved_job_submission(job.id,second.generation)
            assert saved['acceptance_receipt']['sandbox_handle']['allocation_id']==job.id
            assert saved['acceptance_receipt']['allocation_phase']=='absent'
            assert await adapter.scheduler.active_reservation(job.id) is None
            # Crash after confirmed deletion/release, before resolving the old attempt.
            await store.release_job_lease(job.id,'replacement',second.generation)
            third=await store.claim_job(job.id,'third','node',30)
            replay=JobExecutionContext(job=await store.get_job(job.id),request=context.request,
                generation=third.generation,previous_external_execution_id=job.id+':1',
                previous_acceptance_receipt=saved['acceptance_receipt'])
            assert (await lifecycle.recover_submission(replay)).safe_to_retry
            assert provider.starts==1 and runtime.starts==0
        else:
            worker=JobWorker(store,{'child-harness':lifecycle},node_id='node',worker_id='first')
            if mode=='heartbeat':
                await store.finish_pre_accept_attempt(job.id,'first',first.generation,retry=False,
                    error_code='child_allocation_failed',error_detail='allocation lease lost')
            else:await worker._timeout(lifecycle,context,'',submission_started=True)
            failed=await store.get_job(job.id)
            assert failed.state==JobState.FAILED and failed.result['error_code']==(
                'child_allocation_failed' if mode=='heartbeat' else 'child_allocation_timeout')
            await worker.run_cleanup_once()
            assert await store.pending_job_cleanup_ids()=={job.id}
            assert await adapter.scheduler.active_reservation(job.id) is not None
            provider.visible=True;await worker.run_cleanup_once()
            assert await store.pending_job_cleanup_ids()==set() and not provider.items
            assert await adapter.scheduler.active_reservation(job.id) is None
    finally:
        provider.visible=True
        await pool.close()
        if old_pool:await old_pool.close()
        await store.close()


async def test_pending_allocation_fences_cleanup_identity_and_provider_switch(tmp_path):
    from dataclasses import asdict,replace
    from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    try:
        attempt=await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',attempt.generation)
        profile=FakeSandboxBackend.legacy_profile(Harness.CODEX,'image')
        receipt={'allocation_id':job.id,'allocation_provider':'fake',
            'allocation_profile':asdict(profile),'allocation_phase':'allocating'}
        await store.record_job_submission_allocation(job.id,attempt.generation,
            external_execution_id=job.id+':1',receipt=receipt)
        assert await store.active_sandbox_allocations()=={('fake',job.id)}
        assert await store.active_sandbox_providers()=={'fake'}
        await store.request_job_cancel(job.id,'owner')
        with pytest.raises(RuntimeError,match='job_cancelled_before_acceptance'):
            await store.record_job_submission_allocation(job.id,attempt.generation,
                external_execution_id=job.id+':1',receipt={**receipt,
                    'allocation_phase':'runtime_submission'})
        await store.transition_job(job.id,JobState.CANCELLED,expected_generation=attempt.generation)
        cleanup=(await store.claim_job_cleanups('cleaner',30))[0]
        assert await store.active_sandbox_allocations()=={('fake',job.id)}
        assert await store.active_sandbox_providers()=={'fake'}
        handle=SandboxHandle(provider='fake',sandbox_id='late-sandbox',allocation_id=job.id,
            profile_key=profile.compatibility_key,revision=profile.revision)
        resolved={**receipt,'sandbox_id':handle.sandbox_id,'sandbox_handle':handle.to_mapping()}
        with pytest.raises(RuntimeError,match='claim_lost'):
            await store.record_job_cleanup_receipt(cleanup['cleanup_id'],'stale-owner',resolved)
        for wrong in [replace(handle,allocation_id='another'),replace(handle,provider='aliyun'),
                replace(handle,profile_key='f'*63)]:
            with pytest.raises(RuntimeError,match='identity_mismatch'):
                await store.record_job_cleanup_receipt(cleanup['cleanup_id'],'cleaner',
                    {**resolved,'sandbox_handle':wrong.to_mapping()})
        await store.record_job_cleanup_receipt(cleanup['cleanup_id'],'cleaner',resolved)
        assert ('fake','late-sandbox') in await store.active_sandbox_keys()
        await store.finish_job_cleanup(cleanup['cleanup_id'],'cleaner',succeeded=True)
        assert await store.active_sandbox_allocations()==set()
        assert await store.active_sandbox_providers()==set()
    finally:await store.close()
