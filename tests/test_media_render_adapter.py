from types import SimpleNamespace
import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, JobExecutionContext)
from sandbox_harness.common.contracts.job_contracts import (JobArtifactInput, JobExpectedOutput, JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition)
from sandbox_harness.services.job_worker.media_render_adapter import (MediaRenderOperationAdapter)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.sandbox import (Sandbox)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxRuntimeRoute)
from sandbox_harness.common.scheduling.scheduler import (NodeInventory, Scheduler)
from sandbox_harness.common.sandbox.runtime import (StagedRuntimeObject)
from test_support.legacy_store import (Store)


@pytest.mark.parametrize('phase',['allocating','preparing'])
async def test_operation_unknown_create_survives_worker_replacement_without_runtime_replay(tmp_path,phase):
    import asyncio
    from test_harness_job import setup
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
    from sandbox_harness.common.execution.job_adapters import (SubmissionInterrupted)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown)
    class Provider(FakeSandboxBackend):
        started=asyncio.Event();calls=0;visible=False
        async def create(self,request):
            self.calls+=1;value=await super().create(request);self.started.set()
            if phase=='preparing':return value
            raise SandboxCreateOutcomeUnknown('response lost',provider=self.provider)
        async def reconcile_create(self,*args):
            from dataclasses import replace
            value=await super().reconcile_create(*args) if self.visible else None
            if value:
                # Inventory may expose a placeholder revision after process loss.
                value=replace(value,profile=replace(value.profile,revision='inventory-placeholder'),
                    handle=replace(value.handle,revision='inventory-placeholder'))
            return value
    class Runtime:
        calls=0;preparing=asyncio.Event()
        async def inject(self,*args,**kwargs):
            self.preparing.set();await asyncio.Event().wait()
        async def media_render(self,*args):self.calls+=1;raise AssertionError('must not execute')
    store,child,_,job=await setup(tmp_path,AdapterStatus(False));provider=Provider();runtime=Runtime()
    def make_adapter():
        async def token(*args):return 'token',SimpleNamespace(nonce='')
        pool=SandboxPool(provider,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=1)
        adapter=MediaRenderOperationAdapter(scheduler=child.scheduler,pool=pool,runtime=runtime,
            store=store,objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',
                data_root=tmp_path,runtime_image_codex='image'),token_factory=token)
        adapter.allocation_poll_seconds=.01
        return adapter
    adapter=make_adapter();replacement=None;task=None
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        task=asyncio.create_task(adapter.submit(context));await provider.started.wait()
        if phase=='preparing':await runtime.preparing.wait()
        task.cancel()
        with pytest.raises(SubmissionInterrupted):await task
        pending=await store.unresolved_job_submission(job.id,first.generation,include_current=True)
        assert pending['acceptance_receipt']['allocation_phase']==phase
        assert await child.scheduler.active_reservation(job.id)
        await store.release_job_lease(job.id,'first',first.generation)
        second=await store.claim_job(job.id,'replacement','node',30)
        replacement=make_adapter();provider.visible=True
        recovered=await replacement.recover_submission(JobExecutionContext(
            job=await store.get_job(job.id),request=context.request,generation=second.generation,
            previous_external_execution_id=pending['external_execution_id'],
            previous_acceptance_receipt=pending['acceptance_receipt']))
        assert recovered.safe_to_retry and provider.calls==1 and runtime.calls==0
        assert not provider.items and await child.scheduler.active_reservation(job.id) is None
        # Crash after deletion/release but before resolving the original attempt.
        await store.release_job_lease(job.id,'replacement',second.generation)
        third=await store.claim_job(job.id,'third','node',30)
        pending=await store.unresolved_job_submission(job.id,third.generation)
        again=await replacement.recover_submission(JobExecutionContext(job=await store.get_job(job.id),
            request=context.request,generation=third.generation,
            previous_acceptance_receipt=pending['acceptance_receipt']))
        assert again.safe_to_retry and provider.calls==1 and runtime.calls==0
    finally:
        if task and not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
        if replacement:await replacement.pool.close()
        await adapter.pool.close();await store.close()


@pytest.mark.parametrize('operation',[False,True])
async def test_worker_loss_after_reserve_recovers_original_capacity_without_submission(tmp_path,operation):
    from test_harness_job import setup
    store,child,_,job=await setup(tmp_path,AdapterStatus(False))
    class WorkerKilled(BaseException):pass
    adapter=(MediaRenderOperationAdapter(scheduler=child.scheduler,pool=None,runtime=None,
        store=store,objects=ObjectStore(tmp_path/'objects'),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=None)
        if operation else child)
    reserve=child.scheduler.reserve
    async def killed(*args):
        await reserve(*args)
        raise WorkerKilled()
    child.scheduler.reserve=killed
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        with pytest.raises(WorkerKilled):await adapter.submit(context)
        pending=await store.unresolved_job_submission(job.id,first.generation,include_current=True)
        assert pending['acceptance_receipt']['allocation_phase']=='reserving_resources'
        original=await child.scheduler.active_reservation(job.id)
        await store.release_job_lease(job.id,'first',first.generation)
        second=await store.claim_job(job.id,'replacement','node',30)
        context=JobExecutionContext(job=await store.get_job(job.id),request=context.request,
            generation=second.generation,previous_external_execution_id=pending['external_execution_id'],
            previous_acceptance_receipt=pending['acceptance_receipt'])
        recovered=await adapter.recover_submission(context)
        assert recovered.safe_to_retry and recovered.submission is None
        assert original.job_generation==first.generation
        assert await child.scheduler.active_reservation(job.id) is None
    finally:await store.close()


@pytest.mark.parametrize('phase',['reserving_resources','awaiting_capacity','absent'])
async def test_stale_recovery_cannot_release_new_worker_reservation(tmp_path,phase):
    from test_harness_job import setup
    from sandbox_harness.common.execution.job_adapters import (reserve_job_resources, recover_resource_reservation)
    store,adapter,_,job=await setup(tmp_path,AdapterStatus(False))
    try:
        first=await store.claim_job(job.id,'first','node',30)
        await store.mark_job_submission_started(job.id,'first',first.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),
            request=await store.get_job_request(job.id),generation=first.generation)
        old=await reserve_job_resources(context,adapter.scheduler,adapter._resources(context))
        await store.record_job_submission_allocation(job.id,first.generation,
            external_execution_id=job.id+':1',receipt={'allocation_phase':phase,
                'reservation_generation':first.generation,'reservation_id':old.id})
        await store.release_job_lease(job.id,'first',first.generation)
        second=await store.claim_job(job.id,'second','node',30)
        pending=await store.unresolved_job_submission(job.id,second.generation)
        stale=JobExecutionContext(job=await store.get_job(job.id),request=context.request,
            generation=second.generation,previous_acceptance_receipt=pending['acceptance_receipt'])
        await store.release_job_lease(job.id,'second',second.generation)
        third=await store.claim_job(job.id,'third','node',30)
        await adapter.scheduler.release(old.id,old.node_id,old.node_generation)
        await store.mark_job_submission_started(job.id,'third',third.generation)
        current=await adapter.scheduler.reserve(job.id,third.generation,adapter._resources(context))
        with pytest.raises(RuntimeError,match='stale_job_generation'):
            await recover_resource_reservation(stale,adapter.scheduler)
        assert (await adapter.scheduler.active_reservation(job.id)).id==current.id
    finally:await store.close()


@pytest.mark.parametrize('failure',['','response_lost','mark_running','cancel_fence'])
async def test_media_render_adapter_reserves_profile_injects_frozen_input_and_binds_output(tmp_path,failure):
    from sandbox_harness.common.execution.job_adapters import (SubmissionOutcomeUnknown)
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'render-adapter.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Render Operation'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='resize',
        idempotency_key='render-operation-turn'),profile,'owner')
    objects=ObjectStore(tmp_path/'objects');source=b'\x89PNG\r\n\x1a\nsource';object_ref,digest=objects.put(source)
    artifact=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(source),
        mime='image/png',object_ref=object_ref)
    output=JobExpectedOutput(artifact_role='rendered_media',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('image/png',),
        validators=('image.dimensions_exact','image.png'))
    request=JobRequest(local_job_id='resize',kind=JobKind.OPERATION,adapter='media.render',
        descriptor_id='media.render',descriptor_version='1',descriptor_digest='b'*64,
        idempotency_key='render-operation-job',parameters={'recipe':{'operation':'image.resize',
            'input_role':'source','width':32,'height':24,'output_filename':'resized.png'}},
        input_artifacts=(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),),
        expected_outputs=(output,),child_profile_id='operation-controlled',runtime_profile_id='media-cpu',
        resource_requirements={'harness':'codex','image_digest':'d'*64,'delegation_depth':1,
            'cpu':1,'memory_mb':512,'disk_mb':1024})
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='render-operation-group',jobs=[request]))
    attempt=(await store.claim_available_jobs('worker','node',60,limit=1,adapters={'media.render'}))[0]
    job=await store.get_job(jobs[0].id);scheduler=Scheduler(store)
    await scheduler.register_node(NodeInventory(node_id='node',generation=1,cpu_total=4,
        memory_mb_total=8192,disk_mb_total=10000,profiles=[{'runtime_profile_id':'media-cpu',
            'harness':'codex','image_digest':'d'*64,
            'image_ref':'template-media-cpu-codex'}]))
    sandbox=Sandbox(id='sandbox-operation',harness=Harness.CODEX,
        image='template-media-cpu-codex',runtime_url='http://runtime',metadata={})
    class Pool:
        acquired={}
        releases=0
        backend=SimpleNamespace(list_namespace=lambda:None,
            runtime_route=lambda sandbox:SandboxRuntimeRoute(
                "http://sandbox-gateway:29880","http://sandbox-proxy:29881"))
        async def acquire(self,**kwargs):
            self.acquired=kwargs
            if kwargs.get('before_create'):await kwargs['before_create'](None)
            return sandbox
        async def get_attached(self,*args):return sandbox
        async def release(self,*args,**kwargs):self.releases+=1
        async def adopt(self,value,**kwargs):return value
    result_data=b'\x89PNG\r\n\x1a\nresized'
    class Runtime:
        def __init__(self):self.files=[];self.rendered=None;self.calls=0
        async def inject(self,*args,**kwargs):self.injected=kwargs
        async def put_project_file(self,sandbox,path,data,mime):self.files.append((path,data,mime))
        async def media_render(self,sandbox,value):
            self.rendered=value;self.calls+=1
            pending=await store.unresolved_job_submission(job.id,attempt.generation,include_current=True)
            assert pending['acceptance_receipt']['allocation_phase']=='runtime_submission'
            assert pending['acceptance_receipt']['capability_nonce']=='nonce'
            if failure=='response_lost':raise ConnectionError('response lost')
            return {'path':'resized.png','size':len(result_data),'mime':'image/png',
                'sha256':__import__('hashlib').sha256(result_data).hexdigest()}
        async def artifact(self,sandbox,path,*,maximum):
            assert path=='resized.png';target=tmp_path/'staged-resized.png';target.write_bytes(result_data)
            return StagedRuntimeObject(target,len(result_data),__import__('hashlib').sha256(result_data).hexdigest())
    runtime=Runtime()
    async def token_factory(*args):return 'token',SimpleNamespace(nonce='nonce')
    class Profiles:
        def provider_release(self,*args,**kwargs):
            assert args==('media-cpu','codex')
            assert kwargs=={'provider_ref':'template-media-cpu-codex','artifact_digest':'d'*64}
            return SimpleNamespace(ref='template-media-cpu-codex',revision='runtime-config-v2',
                artifact_digest='d'*64)
    adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=Pool(),runtime=runtime,store=store,
        objects=objects,settings=Settings(backend='fake',data_root=tmp_path),token_factory=token_factory,
        profile_resolver=Profiles())
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        await store.mark_job_submission_started(job.id,"worker",attempt.generation)
        if failure=='cancel_fence':
            from sandbox_harness.common.execution.job_adapters import (PreAcceptCancelled)
            record=store.record_job_submission_allocation
            async def cancel_before_marker(*args,**kwargs):
                if kwargs['receipt'].get('allocation_phase')=='runtime_submission':
                    await store.request_job_cancel(job.id,'owner')
                await record(*args,**kwargs)
            store.record_job_submission_allocation=cancel_before_marker
            with pytest.raises(PreAcceptCancelled):await adapter.submit(context)
            assert runtime.calls==0 and adapter.pool.releases==0
            return
        if failure=='mark_running':
            async def lost(*args):raise RuntimeError('resource_reservation_lost')
            scheduler.mark_running=lost
        if failure:
            with pytest.raises(SubmissionOutcomeUnknown):await adapter.submit(context)
            pending=await store.unresolved_job_submission(job.id,attempt.generation,include_current=True)
            assert adapter.pool.releases==0 and await scheduler.active_reservation(job.id)
            assert runtime.calls==1
            await store.release_job_lease(job.id,'worker',attempt.generation)
            replacement=await store.claim_job(job.id,'replacement','node',60)
            recovery=await adapter.recover_submission(JobExecutionContext(
                job=await store.get_job(job.id),request=request,generation=replacement.generation,
                previous_external_execution_id=pending['external_execution_id'],
                previous_acceptance_receipt=pending['acceptance_receipt']))
            assert not recovery.safe_to_retry and runtime.calls==1
            assert bool(recovery.submission)==(failure=='mark_running')
            if recovery.submission:
                assert recovery.submission.completed and recovery.submission.result['path']=='resized.png'
            return
        submission=await adapter.submit(context)
        assert submission.completed and runtime.rendered['input_path'].startswith('assets/inputs/source-')
        assert runtime.files[0][1]==source and runtime.injected['runtime_profile_id']=='media-cpu'
        assert runtime.injected['gateway_url']=='http://sandbox-gateway:29880'
        assert adapter.pool.acquired['revision']=='runtime-config-v2'
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=submission.external_execution_id,
            previous_acceptance_receipt=submission.acceptance_receipt)
        result=await adapter.collect(context,submission.external_execution_id,
            AdapterStatus(True,JobState.SUCCEEDED,submission.result))
        assert result['collections'][0]['state']=='published'
        assert (await store.list_job_artifacts(job.id))[0].output_disposition==OutputDisposition.DELIVERY
    finally:await store.close()


async def test_media_render_adapter_collects_role_addressed_operation_outputs(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'multi-output.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Audio Operation'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='audio',
            idempotency_key='audio-operation-turn'),profile,'owner')
        request=JobRequest(local_job_id='audio',kind=JobKind.OPERATION,adapter='audio.process',
            descriptor_id='audio.process',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='audio-operation-job',parameters={'recipe':{'operation':'audio.process'}},
            expected_outputs=(
                JobExpectedOutput(artifact_role='processed_audio',output_disposition='delivery',min_items=1,
                    validators=('audio',)),
                JobExpectedOutput(artifact_role='measurement_report',output_disposition='workspace',min_items=1)))
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='audio-operation-group',jobs=[request]))
        attempt=(await store.claim_available_jobs('worker','node',60,limit=1,
            adapters={'audio.process'}))[0]
        job=await store.get_job(jobs[0].id);objects=ObjectStore(tmp_path/'objects')
        outputs={'processed.wav':b'RIFF\x04\x00\x00\x00WAVE','audio-process-report.json':b'{"status":"succeeded"}'}
        class Runtime:
            async def artifact(self,sandbox,path,*,maximum):
                data=outputs[path];target=tmp_path/('staged-'+path);target.write_bytes(data)
                return StagedRuntimeObject(target,len(data),__import__('hashlib').sha256(data).hexdigest())
        sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='runtime@sha256:'+'d'*64,
            runtime_url='http://runtime',metadata={})
        class Pool:
            async def get_attached(self,*args):return sandbox
        class HoldScheduler:
            @__import__('contextlib').asynccontextmanager
            async def hold(self,*args):yield
        adapter=MediaRenderOperationAdapter(scheduler=HoldScheduler(),pool=Pool(),runtime=Runtime(),store=store,
            objects=objects,settings=Settings(backend='fake',data_root=tmp_path),token_factory=None)
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_acceptance_receipt={'sandbox_id':'sandbox',
                'sandbox_handle':sandbox.handle.to_mapping(),'reservation_id':'reservation',
                'node_id':'node','node_generation':1})
        result=await adapter.collect(context,'operation:'+job.id,AdapterStatus(True,JobState.SUCCEEDED,{
            'outputs':[{'role':'processed_audio','path':'processed.wav','mime':'audio/wav',
                'size':len(outputs['processed.wav']),
                'sha256':__import__('hashlib').sha256(outputs['processed.wav']).hexdigest(),
                'validation_receipts':[{'validator':'audio','status':'pass'}]},
                {'role':'measurement_report','path':'audio-process-report.json','mime':'application/json',
                'size':len(outputs['audio-process-report.json']),
                'sha256':__import__('hashlib').sha256(outputs['audio-process-report.json']).hexdigest()}]}))
        assert {row['artifact_role'] for row in result['artifacts']}=={
            'processed_audio','measurement_report'}
        assert {row['state'] for row in result['collections']}=={'published'}
    finally:await store.close()
