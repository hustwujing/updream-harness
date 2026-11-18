from datetime import timedelta
import json

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import EventKind,Harness,ItemType,ThreadCreate,TurnCreate,TurnState,now
from sandbox_harness.execution_graph import ExecutionGraphProjector
from sandbox_harness.job_contracts import JobGroupCreate,JobGroupDraftCreate,JobKind,JobRequest,JobState
from sandbox_harness.store import JobActivityRow,JobArtifactRow,ReviewGateRow,Store,TurnRow
from sqlalchemy import update


async def test_execution_graph_contains_only_factual_boundaries_and_truthful_locations(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'graph.db'));await store.initialize()
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
            idempotency_key='graph-child',parameters={'harness':'codex'},child_profile_id='child-general',
            runtime_profile_id='general-cpu',resource_requirements={'harness':'codex','image_digest':'c'*64})
        draft,_=await store.create_job_group_draft('owner',JobGroupDraftCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='graph-draft',jobs=[provider,child],
            expires_at=now()+timedelta(minutes=5),requires_confirmation=True))
        group,jobs=await store.confirm_job_group_draft(draft.id,'owner')
        attempt=await store.claim_job(jobs[1].id,'worker','node',30)
        await store.append_job_activity(jobs[1].id,1,'tool_started',{'name':'读取候选图片'})
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
        assert child_node['location']=='opensandbox' and child_node['activity_count']==1
        assert child_node['retries']==0 and child_node['state']=='RUNNING'
        assert child_node['location_label']=='OpenSandbox · general-cpu'
        assert child_node['resources']['runtime_profile_id']=='general-cpu'
        assert child_node['resources']['child_profile_id']=='child-general'
        assert child_node['resources']['harness']=='codex'
        assert {'submitted_by','contains','requested_interaction'}<={edge['kind'] for edge in graph['edges']}
        unchanged=await ExecutionGraphProjector(store).snapshot(thread.id,'owner',cursor=graph['cursor'])
        assert unchanged=={'cursor':graph['cursor'],'reset':False,'nodes':[],'edges':[]}
    finally:await store.close()


async def test_execution_graph_default_window_and_historical_expansion(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'history.db'));await store.initialize()
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


async def test_primary_retry_count_excludes_first_claim_and_counts_takeover(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'retry-graph.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Retry graph'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='run',
            idempotency_key='retry-graph-turn'),profile,'owner')
        node=lambda graph:next(value for value in graph['nodes'] if value['type']=='primary_harness')
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['retries']==0
        await store.claim_turn(turn.id,'worker-a',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['retries']==0
        await store.claim_turn(turn.id,'worker-a',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['retries']==0
        async with store.connections.begin() as db:
            await db.execute(update(TurnRow).where(TurnRow.id==turn.id).values(
                lease_expires_at=now()-timedelta(seconds=1)))
        await store.claim_turn(turn.id,'worker-b',1)
        assert node(await ExecutionGraphProjector(store).snapshot(thread.id,'owner'))['retries']==1
    finally:await store.close()


async def test_expired_draft_projects_terminal_warning_and_is_idempotent(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'expired-graph.db'));await store.initialize()
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
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'closure-graph.db'));await store.initialize()
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
        async with store.connections.begin() as db:
            db.add(ReviewGateRow(reviewer_job_id=jobs[1].id,source_job_id=jobs[0].id,
                source_generation=0,artifact_role='report',artifact_ids_json='[]',
                artifact_digests_json='[]',required=False,state='optional_warning',
                receipt_json='{}',warning='optional_review_unavailable',created_at=now(),updated_at=now()))
        await store.upsert_interaction(thread.id,turns[-1].id,'execution','pending-decision',
            'approval',{'prompt':'确认后续操作'})
        monkeypatch.setattr('sandbox_harness.execution_graph.MAX_NODES',10)
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
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'bounded-graph.db'));await store.initialize()
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
        async with store.connections.begin() as db:
            db.add_all([JobActivityRow(id=f'{index:032x}',job_id=jobs[0].id,sequence=index+1,
                kind='tool',status='completed',summary='tool',payload_json='{}',created_at=timestamp,updated_at=timestamp)
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
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'paged-history.db'));await store.initialize()
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
