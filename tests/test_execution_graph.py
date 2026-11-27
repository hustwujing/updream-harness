from datetime import timedelta
import json
from types import SimpleNamespace

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (EventKind, Harness, ItemType, ThreadCreate, TurnCreate, TurnState, now)
from sandbox_harness.common.contracts.public_errors import (external_error_detail)
from sandbox_harness.services.api.execution_graph import (ExecutionGraphProjector,aggregate_model_activity)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobGroupDraftCreate, JobKind, JobRequest, JobState)
from sandbox_harness.common.db.accounts import AccountsRepository
from sandbox_harness.common.db.api_repositories import ApiRepositories
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.site_repository import SiteRepository
from sandbox_harness.common.db.models import (ItemRow,JobActivityRow, JobArtifactRow, JobOutboxRow,
    JobRow, MediaBatchRow, MediaDraftRow, MediaJobRow, ReviewGateRow, TurnRow)
from sqlalchemy import select,update


async def graph_store(tmp_path, name):
    store=ApiRepositories(
        AccountsRepository("sqlite+aiosqlite:///"+str(tmp_path/(name+"-accounts.db"))),
        CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/(name+"-core.db")),namespace="graph-test"),
        MediaRepository("sqlite+aiosqlite:///"+str(tmp_path/(name+"-media.db"))),
        SiteRepository("sqlite+aiosqlite:///"+str(tmp_path/(name+"-sites.db"))))
    await store.initialize()
    return store


def test_model_round_aggregation_deduplicates_latest_state_without_reordering():
    started=now()
    def row(sequence,request_id,phase,status,tokens,offset):
        return SimpleNamespace(turn_id='a'*32,sequence=sequence,status=status,
            created_at=started+timedelta(seconds=offset),updated_at=started+timedelta(seconds=offset+1),
            payload_json=json.dumps({'model_request_id':request_id,'job_id':'','model':'gpt-5.6',
                'phase':phase,'elapsed_ms':1000,'ordinary_input_tokens':tokens}))
    values=aggregate_model_activity([
        row(3,'round-1','completed','completed',11,4),
        row(2,'round-2','completed','completed',2,2),
        row(1,'round-1','waiting','in_progress',0,0),
    ])[('a'*32,'')]
    assert [value['request_id'] for value in values['model_rounds']]==['round-1','round-2']
    assert values['model_rounds'][0]['status']=='completed'
    assert values['token_usage']['ordinary_input_tokens']==13


async def test_execution_graph_contains_only_factual_boundaries_and_truthful_locations(tmp_path):
    store=await graph_store(tmp_path,'graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
            reasoning_level='standard',title='Graph'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='parallel work',
            idempotency_key='graph-origin'),profile,'owner')
        await store.append_item(thread.id,turn.id,ItemType.COMMAND,status='completed',summary='ffmpeg')
        await store.append_item(thread.id,turn.id,ItemType.TOOL,status='completed',summary='artifact.inspect')
        provider=JobRequest(local_job_id='generate',kind=JobKind.PROVIDER,adapter='image.generate',
            descriptor_id='image.generate',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='graph-provider',parameters={})
        child=JobRequest(local_job_id='edit',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='b'*64,
            idempotency_key='graph-child',parameters={'harness':'codex','model':'gpt-5.6',
                'objective':'Analyze the source and produce the requested edit'},child_profile_id='child-general',
            runtime_profile_id='general-cpu',resource_requirements={'harness':'codex','image_digest':'c'*64})
        draft,_=await store.create_job_group_draft('owner',JobGroupDraftCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='graph-draft',jobs=[provider,child],
            expires_at=now()+timedelta(minutes=5),requires_confirmation=True))
        group,jobs=await store.confirm_job_group_draft(draft.id,'owner')
        attempt=await store.claim_job(jobs[1].id,'worker','node',30)
        await store.append_job_activity(jobs[1].id,1,'tool_started',{'name':'读取候选图片'})
        for index,(status,phase,usage) in enumerate((
                ('completed','completed',{'ordinary_input_tokens':100,'cache_read_input_tokens':200,
                    'cache_write_input_tokens':300,'output_tokens':400}),
                ('in_progress','streaming',{'ordinary_input_tokens':1,'cache_read_input_tokens':2,
                    'cache_write_input_tokens':3,'output_tokens':4})),1):
            await store.append_item(thread.id,turn.id,ItemType.PLAN,status=status,
                summary='model usage',payload={'model_request_id':f'primary-{index}',
                    'model':'gpt-5.6','phase':phase,'job_id':'','first_output_ms':900,
                    'elapsed_ms':5000 if status=='completed' else 2000,**usage})
        await store.append_item(thread.id,turn.id,ItemType.PLAN,status='failed',
            summary='child model usage',payload={'model_request_id':'child-1','phase':'failed',
                'model':'gpt-5.6','first_output_ms':1200,'elapsed_ms':7000,
                'error_code':'provider_timeout',
                'job_id':jobs[1].id,'ordinary_input_tokens':10,'cache_read_input_tokens':20,
                'cache_write_input_tokens':30,'output_tokens':40})
        await store.upsert_interaction(thread.id,turn.id,'execution','question-1','question',
            {'prompt':'选择风格'})
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        types={node['type'] for node in graph['nodes']}
        assert {'user_turn','primary_harness','job_group_draft','job_group','job','interaction'}<=types
        assert not {'command','tool','file_change'}&types
        provider_node=next(node for node in graph['nodes'] if node.get('job_id')==jobs[0].id)
        child_node=next(node for node in graph['nodes'] if node.get('job_id')==jobs[1].id)
        group_node=next(node for node in graph['nodes'] if node['type']=='job_group')
        assert provider_node['location']=='external_provider'
        assert child_node['role']=='子 Harness' and group_node['role']=='并行任务组'
        assert child_node['location']=='sandbox' and child_node['activity_count']==1
        assert child_node['retries']==0 and child_node['state']=='RUNNING'
        assert child_node['location_label']=='Cloud Sandbox · general-cpu'
        assert child_node['resources']['runtime_profile_id']=='general-cpu'
        assert child_node['resources']['child_profile_id']=='child-general'
        assert child_node['resources']['harness']=='codex'
        assert child_node['harness_view'] is True
        assert child_node['objective']=='Analyze the source and produce the requested edit'
        assert child_node['harness']=='codex' and child_node['model']=='gpt-5.6'
        assert child_node['token_usage']=={'ordinary_input_tokens':10,
            'cache_read_input_tokens':20,'cache_write_input_tokens':30,'output_tokens':40}
        assert child_node['model_rounds'][0]=={
            'index':1,'request_id':'child-1','model':'gpt-5.6','phase':'failed',
            'status':'failed','started_at':child_node['model_rounds'][0]['started_at'],
            'ended_at':child_node['model_rounds'][0]['ended_at'],'duration_ms':7000,
            'first_output_ms':1200,'ordinary_input_tokens':10,'cache_read_input_tokens':20,
            'cache_write_input_tokens':30,'output_tokens':40,'output_characters':0,
            'error_code':'provider_timeout'}
        primary_node=next(node for node in graph['nodes'] if node['type']=='primary_harness')
        assert primary_node['harness_view'] is True and primary_node['objective']=='parallel work'
        assert primary_node['harness']=='codex' and primary_node['model']=='gpt-5.6'
        assert primary_node['token_usage']=={'ordinary_input_tokens':101,
            'cache_read_input_tokens':202,'cache_write_input_tokens':303,'output_tokens':404}
        assert [row['request_id'] for row in primary_node['model_rounds']]==['primary-1','primary-2']
        assert primary_node['model_rounds'][0]['status']=='completed'
        assert primary_node['model_rounds'][0]['ended_at'] is not None
        assert primary_node['model_rounds'][1]['phase']=='streaming'
        assert primary_node['model_rounds'][1]['status']=='in_progress'
        assert primary_node['model_rounds'][1]['ended_at'] is None
        assert primary_node['model_rounds_truncated'] is False
        assert {node['visual_role'] for node in graph['nodes']} >= {
            'user','primary_harness','job_confirmation','job_group','wait_barrier',
            'external_capability','child_harness','user_decision'}
        assert {'submitted_by','contains','requested_interaction'}<={edge['kind'] for edge in graph['edges']}
        unchanged=await ExecutionGraphProjector(store).snapshot(thread.id,'owner',cursor=graph['cursor'])
        assert unchanged=={'cursor':graph['cursor'],'reset':False,'nodes':[],'edges':[]}
    finally:await store.close()


async def test_execution_graph_default_window_and_historical_expansion(tmp_path):
    store=await graph_store(tmp_path,'history')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
            reasoning_level='standard',title='History'),profile,'owner')
        for index in range(7):
            turn,_,_=await store.create_turn(thread.id,TurnCreate(message=f'message {index}',
                idempotency_key=f'history-turn-{index}'),profile,'owner')
            await store.set_turn_state(turn.id,TurnState.COMPLETED)
        recent=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        complete=await ExecutionGraphProjector(store).snapshot(thread.id,'owner',include_history=True)
        assert len([node for node in recent['nodes'] if node['type']=='primary_harness'])==5
        assert recent['historical_available'] is True
        assert len([node for node in complete['nodes'] if node['type']=='primary_harness'])==7
    finally:await store.close()


async def test_execution_graph_marks_bounded_model_rounds_as_partial(tmp_path):
    store=await graph_store(tmp_path,'model-round-limit')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Round limit'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='long execution',
            idempotency_key='round-limit'),profile,'owner')
        timestamp=now()
        async with store.core.connections.begin() as db:
            db.add_all([ItemRow(id=f'{index+1:032x}',thread_id=thread.id,turn_id=turn.id,
                sequence=index+100,type=ItemType.PLAN.value,status='completed',summary='model usage',
                payload_json=json.dumps({'model_request_id':f'round-{index:05d}','job_id':'',
                    'model':'gpt-5.6','phase':'completed','ordinary_input_tokens':1}),
                created_at=timestamp+timedelta(microseconds=index),
                updated_at=timestamp+timedelta(microseconds=index)) for index in range(5001)])
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        primary=next(node for node in graph['nodes'] if node['type']=='primary_harness')
        assert primary['model_rounds_truncated'] is True
        assert primary['token_usage_truncated'] is True
        assert len(primary['model_rounds'])==5000
    finally:await store.close()


async def test_execution_graph_connects_job_group_to_internal_continuation_and_sanitizes_errors(tmp_path):
    store=await graph_store(tmp_path,'continuation-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Continuation'),profile,'owner')
        origin,_,_=await store.create_turn(thread.id,TurnCreate(message='delegate',
            idempotency_key='continuation-origin'),profile,'owner')
        request=JobRequest(local_job_id='child',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='continuation-child',parameters={'harness':'codex','model':'gpt-5.6'},
            child_profile_id='child-general',runtime_profile_id='general-cpu',
            resource_requirements={'harness':'codex','image_digest':'c'*64})
        group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=origin.id,idempotency_key='continuation-group',jobs=[request]))
        await store.transition_job(jobs[0].id,JobState.FAILED,expected_generation=0,
            result={'error_code':'audio_stream_missing','error_detail':external_error_detail(
                'AudioStreamMissing','No audio stream was found; request id: secret-request'),
                'error_message':'输入媒体没有可转写的音轨',
                'error_guidance':'请提供含音轨的视频或独立音频文件后重新创建任务。',
                'failed_phase':'input_validation','retryable':False,
                'retry_class':'none','side_effect_state':'none'})
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        continuation,_,_=await store.create_turn(thread.id,TurnCreate(message='continue',
            idempotency_key='job-continuation'),profile,'owner')
        async with store.core.connections.begin() as db:
            await db.execute(update(TurnRow).where(TurnRow.id==continuation.id).values(internal=True))
            existing=(await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.group_id==group.id))).scalar_one()
            existing.state='delivered';existing.continuation_turn_id=continuation.id
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        child=next(node for node in graph['nodes'] if node.get('job_id')==jobs[0].id)
        assert child['error_code']=='audio_stream_missing'
        assert child['error_summary']=='输入媒体没有可转写的音轨'
        assert child['error_guidance']=='请提供含音轨的视频或独立音频文件后重新创建任务。'
        assert child['error_reason']=='No audio stream was found'
        assert child['failed_phase']=='input_validation' and child['retryable'] is False
        assert child['side_effect_state']=='none'
        assert 'secret-request' not in json.dumps(graph,ensure_ascii=False)
        assert {'from':'wait_barrier:'+group.id,'to':f'turn:{continuation.id}:input','kind':'continued_by'} in graph['edges']
        continued=next(node for node in graph['nodes'] if node['id']==f'turn:{continuation.id}:input')
        assert continued['title']=='并行任务结果已返回'
    finally:await store.close()


async def test_execution_graph_projects_exact_child_wait_progress_and_join_barrier(tmp_path):
    store=await graph_store(tmp_path,'wait-progress')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Wait progress'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='parallel',
            idempotency_key='wait-progress-origin'),profile,'owner')
        requests=[JobRequest(local_job_id=f'child-{index}',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key=f'wait-progress-child-{index}') for index in range(3)]
        group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='wait-progress-group',jobs=requests))
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,expected_generation=0,result={})
        await store.transition_job(jobs[1].id,JobState.FAILED,expected_generation=0,
            result={'error_code':'operation_failed'})
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        group_node=next(node for node in graph['nodes'] if node['id']=='group:'+group.id)
        barrier=next(node for node in graph['nodes'] if node['id']=='wait_barrier:'+group.id)
        assert group_node['progress']=={'terminal':2,'total':3,'succeeded':1,'failed':1,
            'cancelled':0,'blocked':0,'unknown':0,'paused':0}
        assert barrier['state']=='RUNNING' and barrier['title']=='正在等待全部子任务返回 · 2/3'
        assert barrier['progress']==group_node['progress'] and barrier['turn_id']==turn.id
        assert [row['local_job_id'] for row in barrier['remaining_jobs']]==['child-2']
        assert barrier['remaining_jobs'][0]['state']=='CREATED'
        assert barrier['remaining_jobs'][0]['started_at']
        assert await ExecutionGraphProjector(store).waits(thread.id,'owner')==[barrier]
        assert {'from':'group:'+group.id,'to':barrier['id'],'kind':'waits_on'} in graph['edges']
        assert {edge['from'] for edge in graph['edges'] if edge['to']==barrier['id']
            and edge['kind']=='reported_to'}=={'job:'+job.id for job in jobs}

        await store.transition_job(jobs[2].id,JobState.CANCELLED,expected_generation=0,
            result={'error_code':'cancelled'})
        complete=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        barrier=next(node for node in complete['nodes'] if node['id']=='wait_barrier:'+group.id)
        assert barrier['state']=='COMPLETED' and barrier['title']=='全部子任务已返回 · 3/3'
        assert barrier['progress']=={'terminal':3,'total':3,'succeeded':1,'failed':1,
            'cancelled':1,'blocked':0,'unknown':0,'paused':0}
        assert barrier['ended_at'] is not None and barrier['duration_ms']>=0
    finally:await store.close()


async def test_execution_graph_wait_barrier_does_not_spin_for_fully_paused_group(tmp_path):
    store=await graph_store(tmp_path,'paused-wait')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Paused wait'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='pause children',
            idempotency_key='paused-wait-origin'),profile,'owner')
        requests=[JobRequest(local_job_id=f'child-{index}',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key=f'paused-wait-child-{index}') for index in range(2)]
        group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='paused-wait-group',jobs=requests))
        async with store.core.connections.begin() as db:
            await db.execute(update(JobRow).where(JobRow.group_id==group.id).values(state='PAUSED'))
        barrier=(await ExecutionGraphProjector(store).waits(thread.id,'owner'))[0]
        assert barrier['state']=='PAUSED' and barrier['title']=='子任务已暂停 · 0/2'
        assert barrier['progress']['paused']==2 and barrier['progress']['terminal']==0
    finally:await store.close()


async def test_execution_graph_projects_media_jobs_and_connects_media_continuation(tmp_path):
    store=await graph_store(tmp_path,'media-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Media'),profile,'owner')
        origin,_,_=await store.create_turn(thread.id,TurnCreate(message='generate images',
            idempotency_key='media-origin'),profile,'owner')
        await store.set_turn_state(origin.id,TurnState.COMPLETED)
        continuation,_,_=await store.create_turn(thread.id,TurnCreate(message='media results',
            idempotency_key='media-continuation-'+origin.id),profile,'owner')
        timestamp=now();draft_id='1'*32;job_id='2'*32
        async with store.core.connections.begin() as db:
            await db.execute(update(TurnRow).where(TurnRow.id==continuation.id).values(internal=True))
        async with store.media.connections.begin() as db:
            db.add(MediaBatchRow(turn_id=origin.id,thread_id=thread.id,owner_id='owner',status='continued',
                created_at=timestamp,sealed_at=timestamp,continued_at=timestamp,updated_at=timestamp))
            db.add(MediaDraftRow(id=draft_id,owner_id='owner',thread_id=thread.id,turn_id=origin.id,
                kind='image',model='seedream-5.0',generation_mode='text_to_image',
                parameters_json='{}',references_json='[]',idempotency_key='media-draft-test',
                status='confirmed',expires_at=timestamp+timedelta(minutes=5),job_id=job_id,
                output_role='candidate',created_at=timestamp,updated_at=timestamp))
            db.add(MediaJobRow(id=job_id,owner_id='owner',thread_id=thread.id,turn_id=origin.id,
                draft_id=draft_id,kind='image',model='seedream-5.0',generation_mode='text_to_image',
                provider='ark',adapter='ark_image',execution_mode='sync',cancellation_mode='unsupported',
                status='running',phase='provider_running',progress=35,generation=140,
                provider_job_id='provider-task-123',provider_started_at=timestamp,
                parameters_json='{}',references_json='[]',
                idempotency_key='media-job-test',created_at=timestamp,updated_at=timestamp))
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        media=next(node for node in graph['nodes'] if node.get('media_job_id')==job_id)
        assert media['type']=='media_job' and media['state']=='RUNNING'
        assert media['progress']==35 and media['location']=='external_provider'
        assert media['retries']==0 and media['submission_count']==1
        assert media['status_check_count']==139 and media['provider_job_short_id']=='provider'
        assert {'from':f'media-batch:{origin.id}','to':f'turn:{continuation.id}:input',
            'kind':'continued_by'} in graph['edges']
    finally:await store.close()


async def test_primary_retry_count_excludes_first_claim_and_counts_takeover(tmp_path):
    store=await graph_store(tmp_path,'retry-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Retry graph'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='run',
            idempotency_key='retry-graph-turn'),profile,'owner')
        node=lambda graph:next(value for value in graph['nodes'] if value['type']=='primary_harness')
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['recovery_count']==0
        await store.claim_turn(turn.id,'worker-a',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['recovery_count']==0
        await store.claim_turn(turn.id,'worker-a',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['recovery_count']==0
        async with store.core.connections.begin() as db:
            await db.execute(update(TurnRow).where(TurnRow.id==turn.id).values(
                lease_expires_at=now()-timedelta(seconds=1)))
        await store.claim_turn(turn.id,'worker-b',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['recovery_count']==1
    finally:await store.close()


async def test_expired_draft_projects_terminal_warning_and_is_idempotent(tmp_path):
    store=await graph_store(tmp_path,'expired-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Expiry'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='plan',
            idempotency_key='expiry-turn'),profile,'owner')
        request=JobRequest(local_job_id='job',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='expiry-job')
        draft,_=await store.create_job_group_draft('owner',JobGroupDraftCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='expiry-draft',jobs=[request],
            expires_at=now()-timedelta(seconds=1),requires_confirmation=True))
        assert (await store.list_job_group_drafts(thread.id,'owner'))[0].state.value=='EXPIRED'
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        assert next(node for node in graph['nodes'] if node['id']=='draft:'+draft.id)['state']=='EXPIRED'
        assert await store.expire_job_group_drafts()==1
        assert await store.expire_job_group_drafts()==0
        warnings=[item for item in await store.items_after(thread.id) if item.type==ItemType.WARNING]
        assert len(warnings)==1 and warnings[0].payload['code']=='job_group_draft_expired'
    finally:await store.close()


async def test_collapsed_graph_keeps_active_group_ancestor_closure_and_gate(tmp_path,monkeypatch):
    store=await graph_store(tmp_path,'closure-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Closure'),profile,'owner');turns=[]
        for index in range(7):
            turn,_,_=await store.create_turn(thread.id,TurnCreate(message=f'turn {index}',
                idempotency_key=f'closure-turn-{index}'),profile,'owner');turns.append(turn)
            await store.set_turn_state(turn.id,TurnState.COMPLETED)
        source=JobRequest(local_job_id='source',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='closure-source')
        reviewer=JobRequest(local_job_id='review',kind=JobKind.HARNESS,adapter='reviewer-harness',
            descriptor_id='review.submit',descriptor_version='1',descriptor_digest='b'*64,
            idempotency_key='closure-review',parameters={'review_context':{'target':None}},
            child_profile_id='reviewer-readonly',runtime_profile_id='general-cpu',
            resource_requirements={'harness':'codex','image_digest':'c'*64})
        group,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
                origin_turn_id=turns[0].id,idempotency_key='closure-group',jobs=[source,reviewer]))
        async with store.core.connections.begin() as db:
            db.add(ReviewGateRow(reviewer_job_id=jobs[1].id,source_job_id=jobs[0].id,
                source_generation=0,artifact_role='report',artifact_ids_json='[]',
                artifact_digests_json='[]',required=False,state='optional_warning',
                receipt_json='{}',warning='optional_review_unavailable',created_at=now(),updated_at=now()))
        await store.upsert_interaction(thread.id,turns[-1].id,'execution','pending-decision',
            'approval',{'prompt':'确认后续操作'})
        monkeypatch.setattr('sandbox_harness.services.api.execution_graph.MAX_NODES',10)
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner')
        ids={node['id'] for node in graph['nodes']}
        assert f'turn:{turns[0].id}:primary' in ids and 'group:'+group.id in ids
        gate=next(node for node in graph['nodes'] if node['type']=='review_gate')
        assert gate['state']=='OPTIONAL_WARNING' and gate['warning']
        assert graph['metadata']['truncated'] is True
        assert any(node.get('job_id')==jobs[1].id for node in graph['nodes'])
        assert any(node['type']=='interaction' and node['state']=='pending' for node in graph['nodes'])
        assert all(edge['from'] in ids and edge['to'] in ids for edge in graph['edges'])
    finally:await store.close()


async def test_graph_uses_aggregate_counts_and_bounded_artifact_summaries(tmp_path):
    store=await graph_store(tmp_path,'bounded-graph')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Bounds'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='large graph',
            idempotency_key='bounded-turn'),profile,'owner')
        request=JobRequest(local_job_id='large',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='bounded-job')
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='bounded-group',jobs=[request]))
        timestamp=now()
        async with store.core.connections.begin() as db:
            db.add_all([JobActivityRow(id=f'{index:032x}',job_id=jobs[0].id,sequence=index+1,
                kind='tool_started',status='in_progress',summary='tool',
                payload_json=json.dumps({'name':'tool','tool_use_id':f'tool-{index}'}),
                created_at=timestamp,updated_at=timestamp)
                for index in range(1200)])
            db.add_all([JobArtifactRow(id=f'{index+2000:032x}',job_id=jobs[0].id,
                artifact_id=f'{index+4000:032x}',artifact_role='parts',item_key=f'item-{index}',
                filename=f'part-{index}.txt',position=index,requested_disposition='candidate',
                output_disposition='candidate',sha256=f'{index:064x}'[-64:],size=1,mime='text/plain',
                object_ref='sha256:'+f'{index:064x}'[-64:],validation_receipts_json='[]',
                review_receipts_json='[]',state='committed',created_at=timestamp)
                for index in range(250)])
        graph=await ExecutionGraphProjector(store).snapshot(thread.id,'owner',include_history=True)
        node=next(value for value in graph['nodes'] if value.get('job_id')==jobs[0].id)
        assert node['activity_count']==1200 and node['artifact_count']==250
        assert len(node['artifacts'])<=graph['metadata']['artifact_summary_limit']==200
        assert node['artifacts_truncated'] is True
        assert len(graph['nodes'])<=graph['metadata']['node_limit']
        assert len(graph['edges'])<=graph['metadata']['edge_limit']
        assert len(json.dumps(graph,default=str).encode())<1024*1024
    finally:await store.close()


async def test_history_before_pages_are_stable_and_nonoverlapping(tmp_path):
    store=await graph_store(tmp_path,'paged-history')
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Paged history'),profile,'owner')
        turns=[]
        for index in range(120):
            turn,_,_=await store.create_turn(thread.id,TurnCreate(message=f'turn {index}',
                idempotency_key=f'paged-turn-{index}'),profile,'owner');turns.append(turn)
            await store.set_turn_state(turn.id,TurnState.COMPLETED)
            request=JobRequest(local_job_id=f'job-{index}',kind=JobKind.OPERATION,adapter='fake',
                descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
                idempotency_key=f'paged-job-{index}')
            _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
                origin_turn_id=turn.id,idempotency_key=f'paged-group-{index}',jobs=[request]))
            await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
                expected_generation=0,result={'index':index})
        active_request=JobRequest(local_job_id='active-old',kind=JobKind.OPERATION,adapter='fake',
            descriptor_id='fake',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='paged-active-old')
        await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turns[0].id,idempotency_key='paged-active-group',jobs=[active_request]))
        projector=ExecutionGraphProjector(store);before='';pages=[]
        for _ in range(3):
            page=await projector.snapshot(thread.id,'owner',include_history=True,
                history_before=before);pages.append(page)
            before=page['metadata']['next_history_before'] or ''
        turn_sets=[{node['turn_id'] for node in page['nodes'] if node['type']=='primary_harness'}
            for page in pages]
        live={turns[0].id};historical=[values-live for values in turn_sets]
        assert [len(values) for values in turn_sets]==[51,51,20]
        assert not (historical[0]&historical[1] or historical[1]&historical[2] or historical[0]&historical[2])
        assert all(live<=values for values in turn_sets)
        assert pages[-1]['metadata']['next_history_before'] is None
    finally:await store.close()
