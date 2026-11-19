from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest
from sandbox_harness.store import Store


async def setup(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'activities.db'));await store.initialize()
    request=JobRequest(local_job_id='child',kind=JobKind.HARNESS,adapter='child-harness',
        descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='1'*64,
        idempotency_key='child-activity-job',parameters={'harness':'codex','model':'gpt-5.6'},
        child_profile_id='child-general',runtime_profile_id='general-cpu')
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='a'*32,
        origin_turn_id='b'*32,idempotency_key='child-activity-group',jobs=(request,)))
    return store,jobs[0]


async def test_text_stream_remains_raw_without_becoming_display_steps(tmp_path):
    store,job=await setup(tmp_path)
    try:
        for sequence in range(1,101):
            await store.append_job_activity(job.id,sequence,'text_delta',{'text':f'chunk-{sequence}'})
        raw=await store.list_job_activities(job.id,0,500)
        display=await store.list_job_display_activities(job.id,0,500)
        assert len(raw)==100
        assert display==[]
    finally:await store.close()


async def test_tool_stream_projects_to_one_stable_bounded_activity(tmp_path):
    store,job=await setup(tmp_path)
    try:
        await store.append_job_activity(job.id,1,'tool_started',{
            'name':'commandExecution','tool_use_id':'call-1',
            'detail':{'command':'ffmpeg -i input.mp4 output.mp4','cwd':'/workspace/project'}})
        for sequence in range(2,102):
            await store.append_job_activity(job.id,sequence,'tool_output',{
                'name':'commandExecution','tool_use_id':'call-1','text':'x'*200})
        await store.append_job_activity(job.id,102,'tool_finished',{
            'name':'commandExecution','tool_use_id':'call-1','result':{'exitCode':0}})
        display=await store.list_job_display_activities(job.id,0,500)
        assert len(display)==1
        activity=display[0]
        assert activity['kind']=='tool'
        assert activity['status']=='completed'
        assert activity['sequence']==1 and activity['cursor_sequence']==102
        assert activity['raw_count']==102
        assert activity['payload']['detail']['command'].startswith('ffmpeg')
        assert activity['payload']['result']=={'exitCode':0}
        assert len(activity['payload']['output_tail'])<=8192
        repeated=await store.list_job_display_activities(job.id,0,500)
        assert repeated==display
    finally:await store.close()


async def test_concurrent_tools_and_errors_keep_distinct_display_identity(tmp_path):
    store,job=await setup(tmp_path)
    try:
        await store.append_job_activity(job.id,1,'tool_started',{
            'name':'Read','tool_use_id':'read-1','detail':{'path':'a.txt'}})
        await store.append_job_activity(job.id,2,'tool_started',{
            'name':'Read','tool_use_id':'read-2','detail':{'path':'b.txt'}})
        await store.append_job_activity(job.id,3,'tool_finished',{
            'name':'Read','tool_use_id':'read-1','result':{'bytes':10}})
        await store.append_job_activity(job.id,4,'error',{'text':'sandbox command failed','code':'command_failed'},status='failed')
        display=await store.list_job_display_activities(job.id,0,500)
        assert len(display)==3
        assert len({row['id'] for row in display})==3
        tools=[row for row in display if row['kind']=='tool']
        assert [row['status'] for row in tools]==['completed','in_progress']
        assert next(row for row in display if row['kind']=='error')['status']=='failed'
    finally:await store.close()


async def test_protocol_bookkeeping_is_hidden_but_plan_progress_is_coalesced(tmp_path):
    store,job=await setup(tmp_path)
    try:
        for sequence,name in enumerate(('Codex session','Input completed','Codex response'),1):
            await store.append_job_activity(job.id,sequence,'step_finished',{'name':name})
        await store.append_job_activity(job.id,4,'step_progress',{
            'name':'Plan','plan':[{'step':'inspect','status':'in_progress'}]})
        await store.append_job_activity(job.id,5,'step_progress',{
            'name':'Plan','plan':[{'step':'inspect','status':'completed'}]})
        display=await store.list_job_display_activities(job.id,0,500)
        assert len(display)==1
        assert display[0]['kind']=='plan' and display[0]['raw_count']==2
        assert display[0]['cursor_sequence']==5
        assert display[0]['payload']['plan']==[{'step':'inspect','status':'completed'}]
    finally:await store.close()


async def test_late_tool_output_cannot_regress_a_completed_activity(tmp_path):
    store,job=await setup(tmp_path)
    try:
        await store.append_job_activity(job.id,1,'tool_started',{
            'name':'commandExecution','tool_use_id':'call-1','detail':{'command':'true'}})
        await store.append_job_activity(job.id,2,'tool_finished',{
            'name':'commandExecution','tool_use_id':'call-1','result':{'exitCode':0}})
        await store.append_job_activity(job.id,3,'tool_output',{
            'name':'tool','tool_use_id':'call-1','text':'late buffered output'})
        activity=(await store.list_job_display_activities(job.id,0,10))[0]
        assert activity['status']=='completed'
        assert activity['summary']=='commandExecution'
        assert activity['cursor_sequence']==3 and activity['payload']['output_tail']=='late buffered output'
    finally:await store.close()
