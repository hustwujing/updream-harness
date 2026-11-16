from datetime import timedelta

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import EventKind,Harness,ItemType,ThreadCreate,TurnCreate,TurnState,now
from sandbox_harness.execution_graph import ExecutionGraphProjector
from sandbox_harness.job_contracts import JobGroupDraftCreate,JobKind,JobRequest
from sandbox_harness.store import Store


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
        assert provider_node['location']=='external_provider'
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
