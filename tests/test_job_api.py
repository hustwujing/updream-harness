from pathlib import Path
import json

from fastapi.testclient import TestClient
import pytest

from sandbox_harness.services.api.app import (create_app)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_runtime import job_cli
from sandbox_runtime import review_cli


ROOT=Path(__file__).resolve().parents[1]
IMAGE_PARAMETERS={'kind':'image','model':'seedream-5.0','generation_mode':'text_to_image',
    'parameters':{'prompt':'one image'}}


def settings(tmp_path):
    return Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'api.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=False,
        job_catalog_path=ROOT/'deploy/job-catalog.example.json',
        profile_catalog_path=ROOT/'deploy/profile-catalog.example.json')


def request(local_id,kind,descriptor,digest,parameters):
    output_roles={'image.generate':'generated_images','media.render':'rendered_media'}
    expected=([{'artifact_role':output_roles[descriptor],'min_items':1}]
        if descriptor in output_roles else [])
    return {'local_job_id':local_id,'kind':kind,'adapter':descriptor,'descriptor_id':descriptor,
        'descriptor_version':'1','descriptor_digest':digest,'idempotency_key':'job-'+local_id+'-key',
        'parameters':parameters,'expected_outputs':expected}


def test_job_group_api_edits_confirms_once_and_enforces_owner(tmp_path):
    app=create_app(settings(tmp_path))
    with TestClient(app) as client:
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Parallel jobs'}).json()
        app.state.broker._schedule=lambda turn_id:None
        turn=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'generate and render','idempotency_key':'job-api-origin-turn'}).json()
        jobs=[request('generate','provider','image.generate','c'*64,IMAGE_PARAMETERS),
            request('render','operation','media.render','b'*64,{'recipe':{'operation':'image.resize',
                'input_role':'source','width':64,'height':64,'output_filename':'rendered.png'}})]
        prepared=client.post('/api/job-group-drafts',json={'thread_id':thread['id'],
            'origin_turn_id':turn['id'],'idempotency_key':'job-api-group-key','jobs':jobs})
        assert prepared.status_code==200 and prepared.json()['mode']=='draft'
        draft=prepared.json()['draft']
        edited=client.patch(f"/api/job-group-drafts/{draft['id']}",json={
            'expected_digest':draft['request_digest'],'jobs':[jobs[0]]})
        assert edited.status_code==200 and edited.json()['request_digest']!=draft['request_digest']
        confirmed=client.post(f"/api/job-group-drafts/{draft['id']}/confirm")
        assert confirmed.status_code==200
        value=confirmed.json();assert value['group']['member_count']==1
        assert [job['local_job_id'] for job in value['jobs']]==['generate']
        job_id=value['jobs'][0]['id']
        client.portal.call(app.state.store.append_job_activity,job_id,1,'text_delta',{'text':'stream chunk'})
        client.portal.call(app.state.store.append_job_activity,job_id,2,'tool_started',
            {'name':'Read','tool_use_id':'read-1','detail':{'path':'source.txt'}})
        client.portal.call(app.state.store.append_job_activity,job_id,3,'tool_finished',
            {'name':'Read','tool_use_id':'read-1','result':{'bytes':12}})
        display=client.get(f'/api/jobs/{job_id}/activities').json()
        raw=client.get(f'/api/jobs/{job_id}/activities?view=raw').json()
        assert len(display)==1 and display[0]['raw_count']==2
        assert display[0]['status']=='completed' and display[0]['cursor_sequence']==3
        assert len(raw)==3 and raw[0]['kind']=='text_delta'
        assert client.get(f'/api/jobs/{job_id}/activities?view=other').status_code==422
        repeated=client.post(f"/api/job-group-drafts/{draft['id']}/confirm")
        assert repeated.status_code==200 and repeated.json()['group']['id']==value['group']['id']
        detail=client.get(f"/api/job-groups/{value['group']['id']}")
        assert detail.status_code==200 and len(detail.json()['jobs'])==1
        cancelled=client.post(f"/api/jobs/{value['jobs'][0]['id']}/cancel")
        assert cancelled.status_code==200 and cancelled.json()['state']=='CANCELLING'
    other_app=create_app(settings(tmp_path/'other'))
    with TestClient(other_app) as other:
        assert other.get(f"/api/job-groups/{value['group']['id']}").status_code==404


def test_job_group_api_rejects_inline_operation_and_origin_turn_mismatch(tmp_path):
    app=create_app(settings(tmp_path))
    with TestClient(app) as client:
        first=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'First'}).json()
        second=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Second'}).json()
        app.state.broker._schedule=lambda turn_id:None
        turn=client.post(f"/api/threads/{first['id']}/turns",json={
            'message':'inspect','idempotency_key':'inline-origin-turn'}).json()
        inline=request('inspect','operation','artifact.inspect','a'*64,{})
        mismatch=client.post('/api/job-group-drafts',json={'thread_id':second['id'],
            'origin_turn_id':turn['id'],'idempotency_key':'mismatch-group','jobs':[inline]})
        assert mismatch.status_code==404
        rejected=client.post('/api/job-group-drafts',json={'thread_id':first['id'],
            'origin_turn_id':turn['id'],'idempotency_key':'inline-group','jobs':[inline]})
        assert rejected.status_code==422 and 'inline Operation' in rejected.json()['detail']


def test_sandbox_job_cli_validates_complete_group_before_gateway_call(tmp_path,monkeypatch,capsys):
    token=tmp_path/'token';token.write_text('x'*32)
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway.test')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    calls=[]
    class Result:
        is_success=True;text='{"mode":"draft"}'
    monkeypatch.setattr(job_cli.httpx,'post',lambda url,**kwargs:calls.append((url,kwargs)) or Result())
    payload={'thread_id':'a'*32,'origin_turn_id':'b'*32,'idempotency_key':'cli-group-key',
        'jobs':[request('generate','provider','image.generate','c'*64,IMAGE_PARAMETERS)]}
    assert job_cli.main(['prepare','--request',__import__('json').dumps(payload)])==0
    assert calls[0][0]=='http://gateway.test/v1/job-group-drafts'
    assert calls[0][1]['json']==payload
    assert '"mode":"draft"' in capsys.readouterr().out


def test_sandbox_job_cli_fills_frozen_identity_lists_catalog_and_blocks_child_delegation(tmp_path,monkeypatch,capsys):
    token=tmp_path/'token';token.write_text('x'*32);calls=[]
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway.test')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    monkeypatch.setenv('HARNESS_THREAD_ID','a'*32);monkeypatch.setenv('HARNESS_TURN_ID','b'*32)
    class Result:
        is_success=True;text='{"capabilities":[]}'
    monkeypatch.setattr(job_cli.httpx,'get',lambda url,**kwargs:calls.append(('get',url,kwargs)) or Result())
    monkeypatch.setattr(job_cli.httpx,'post',lambda url,**kwargs:calls.append(('post',url,kwargs)) or Result())
    assert job_cli.main(['catalog'])==0 and calls[-1][1]=='http://gateway.test/v1/execution/catalog'
    payload={'idempotency_key':'cli-auto-identity','jobs':[
        request('generate','provider','image.generate','c'*64,IMAGE_PARAMETERS)]}
    assert job_cli.main(['prepare','--request',__import__('json').dumps(payload)])==0
    submitted=calls[-1][2]['json'];assert submitted['thread_id']=='a'*32 and submitted['origin_turn_id']=='b'*32
    monkeypatch.setenv('HARNESS_CHILD_MODE','1')
    assert job_cli.main(['prepare','--request',__import__('json').dumps(payload)])==0
    assert job_cli.main(['delegate','--request',__import__('json').dumps({'tasks':[
        {'id':'nested','objective':'nested','result_mode':'text'}]})])==2
    assert 'cannot delegate' in capsys.readouterr().out


def test_sandbox_job_delegate_compiles_metadata_free_tasks_from_catalog(tmp_path,monkeypatch,capsys):
    token=tmp_path/'token';token.write_text('x'*32);calls=[]
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway.test')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    monkeypatch.setenv('HARNESS_THREAD_ID','a'*32);monkeypatch.setenv('HARNESS_TURN_ID','b'*32)
    monkeypatch.setenv('HARNESS_KIND','codex');monkeypatch.setenv('HARNESS_MODEL','gpt-5.6')
    catalog={'skills':[{'id':'plain-skill','version':'1','digest':'e'*64,
        'supported_harnesses':['codex']}], 'child_execution_profiles':[{
        'id':'child-general','supported_harnesses':['codex'],'allowed_skill_ids':['plain-skill'],
        'job_template':{'kind':'harness','adapter':'child-harness',
            'descriptor_id':'delegate.harness','descriptor_version':'1',
            'descriptor_digest':'d'*64,'child_profile_id':'child-general'}}],
        'input_artifacts':[{'artifact_id':'c'*32,'project_path':'assets/source.bin',
            'sha256':'f'*64,'size':123,'mime':'application/octet-stream'}]}
    class Result:
        is_success=True
        def __init__(self,value):self.text=json.dumps(value)
    monkeypatch.setattr(job_cli.httpx,'get',lambda url,**kwargs:
        calls.append(('get',url,kwargs)) or Result(catalog))
    monkeypatch.setattr(job_cli.httpx,'post',lambda url,**kwargs:
        calls.append(('post',url,kwargs)) or Result({'mode':'committed'}))
    request={'tasks':[{'id':'analyze','objective':'Analyze the frozen input',
        'inputs':['assets/source.bin'],'skill_ids':['plain-skill'],
        'output_disposition':'candidate'}]}
    assert job_cli.main(['delegate','--request',json.dumps(request)])==0
    assert calls[0][1]=='http://gateway.test/v1/execution/catalog'
    submitted=calls[1][2]['json'];job=submitted['jobs'][0]
    assert submitted['thread_id']=='a'*32 and submitted['origin_turn_id']=='b'*32
    assert submitted['idempotency_key'].startswith('delegate-')
    assert job['local_job_id']=='analyze' and job['kind']=='harness'
    assert job['parameters']=={'harness':'codex','model':'gpt-5.6',
        'objective':'Analyze the frozen input'}
    assert job['input_artifacts']==[{'artifact_id':'c'*32,'artifact_role':'input_1'}]
    assert job['skill_ids']==['plain-skill']
    assert job['expected_outputs'][0]['artifact_role']=='child_deliverables'
    assert job['expected_outputs'][0]['output_disposition']=='candidate'
    assert '"mode": "committed"' in capsys.readouterr().out


def test_sandbox_job_delegate_rejects_unfrozen_input(tmp_path,monkeypatch):
    token=tmp_path/'token';token.write_text('x'*32)
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway.test')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    monkeypatch.setenv('HARNESS_THREAD_ID','a'*32);monkeypatch.setenv('HARNESS_TURN_ID','b'*32)
    monkeypatch.setenv('HARNESS_KIND','codex');monkeypatch.setenv('HARNESS_MODEL','gpt-5.6')
    class Result:
        is_success=True;text=json.dumps({'skills':[],'input_artifacts':[],
            'child_execution_profiles':[{'id':'child-general','supported_harnesses':['codex'],
                'allowed_skill_ids':[],'job_template':{'kind':'harness','adapter':'child-harness',
                'descriptor_id':'delegate.harness','descriptor_version':'1',
                'descriptor_digest':'d'*64,'child_profile_id':'child-general'}}]})
    monkeypatch.setattr(job_cli.httpx,'get',lambda *args,**kwargs:Result())
    with pytest.raises(SystemExit):job_cli.main(['delegate','--request',json.dumps({'tasks':[
        {'id':'bad','objective':'Read a foreign path','inputs':['work/private.bin']}]})])


def test_sandbox_job_delegate_exposes_compact_example_and_schema_without_capability(capsys):
    assert job_cli.main(['delegate','--example'])==0
    example=json.loads(capsys.readouterr().out)
    assert set(example)=={'tasks'} and len(example['tasks'])==2
    assert set(example['tasks'][0])=={'id','objective','inputs','skill_ids','result_mode'}
    assert job_cli.main(['delegate','--schema'])==0
    schema=json.loads(capsys.readouterr().out)
    assert schema['required']==['tasks'] and 'tasks' in schema['properties']


def test_gateway_exposes_catalog_and_explicit_review_without_user_confirmation_routes(tmp_path):
    from sandbox_harness.services.sandbox_gateway.gateway import (create_gateway)
    gateway_settings=settings(tmp_path/'gateway').model_copy(update={'redis_url':'redis://127.0.0.1:6379/15'})
    paths={getattr(route,'path','') for route in create_gateway(gateway_settings).routes}
    assert '/v1/execution/catalog' in paths and '/v1/reviews' in paths
    assert '/v1/job-group-drafts/{draft_id}/confirm' not in paths


@pytest.mark.parametrize('representation',['short_inline','long_inline','long_file'])
def test_review_cli_uses_host_identity_and_refuses_child_mode(tmp_path,monkeypatch,capsys,representation):
    token=tmp_path/'token';token.write_text('x'*32);calls=[]
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway.test')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    monkeypatch.setenv('HARNESS_KIND','codex');monkeypatch.setenv('HARNESS_MODEL','gpt-5.6')
    class Result:is_success=True;text='{"mode":"committed"}'
    monkeypatch.setattr(review_cli.httpx,'post',lambda url,**kwargs:calls.append((url,kwargs)) or Result())
    payload={'artifact_ids':['a'*32],'rubric':'Check it' if representation=='short_inline' else
        'Verify the complete ordered collection. '*150,'idempotency_key':'review-cli-key'}
    raw=' \n'+json.dumps(payload)
    if representation=='long_file':
        request=tmp_path/'review.json';request.write_text(raw);raw=str(request)
    assert review_cli.main(['--request',raw])==0
    assert calls[0][0]=='http://gateway.test/v1/reviews'
    assert calls[0][1]['json']['harness']=='codex' and calls[0][1]['json']['model']=='gpt-5.6'
    assert calls[0][1]['json']['rubric']==payload['rubric']
    monkeypatch.setenv('HARNESS_CHILD_MODE','1')
    assert review_cli.main(['--request',raw])==2
    assert len(calls)==1
    assert 'cannot create' in capsys.readouterr().out


def test_sandbox_cli_and_gateway_cannot_confirm_user_authorized_group(tmp_path):
    with pytest.raises(SystemExit):
        job_cli.main(['confirm','--draft-id','draft'])
    app=create_app(settings(tmp_path))
    api_paths={getattr(route,'path','') for route in app.routes}
    from sandbox_harness.services.sandbox_gateway.gateway import (create_gateway)
    gateway_settings=settings(tmp_path/'gateway').model_copy(update={'redis_url':'redis://127.0.0.1:6379/15'})
    gateway_paths={getattr(route,'path','') for route in create_gateway(gateway_settings).routes}
    assert '/v1/job-group-drafts/{draft_id}/confirm' not in gateway_paths
    assert '/v1/job-group-drafts/{draft_id}/reject' not in gateway_paths
    assert '/api/job-group-drafts/{draft_id}/confirm' in api_paths
    assert '/api/job-group-drafts/{draft_id}/reject' in api_paths


def test_thread_stop_all_cancels_current_turn_and_every_nonterminal_job(tmp_path):
    app=create_app(settings(tmp_path))
    with TestClient(app) as client:
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Stop all'}).json()
        app.state.broker._schedule=lambda turn_id:None
        turn=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'run in parallel','idempotency_key':'stop-all-origin'}).json()
        request_value=request('long','provider','image.generate','c'*64,IMAGE_PARAMETERS)
        prepared=client.post('/api/job-group-drafts',json={'thread_id':thread['id'],
            'origin_turn_id':turn['id'],'idempotency_key':'stop-all-group-key','jobs':[request_value]}).json()
        confirmed=client.post(f"/api/job-group-drafts/{prepared['draft']['id']}/confirm").json()
        response=client.post(f"/api/threads/{thread['id']}/stop-all")
        assert response.status_code==200 and response.json()['accepted']
        assert response.json()['jobs'][0]['state']=='CANCELLING'


def test_thread_pause_and_resume_are_idempotent_and_visible(tmp_path):
    app=create_app(settings(tmp_path))
    with TestClient(app) as client:
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Pause'}).json()
        app.state.broker._schedule=lambda turn_id:None
        turn=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'run later','idempotency_key':'pause-api-origin'}).json()
        first=client.post(f"/api/threads/{thread['id']}/pause")
        second=client.post(f"/api/threads/{thread['id']}/pause")
        assert first.status_code==200 and first.json()['state']=='PAUSED'
        assert first.json()['operation_id']==second.json()['operation_id']
        assert first.json()['participants'][0]['participant_id']==turn['id']
        status=client.get(f"/api/threads/{thread['id']}/pause").json()
        assert status['state']=='PAUSED' and status['generation']==1
        blocked=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'must wait','idempotency_key':'pause-api-blocked'})
        assert blocked.status_code==409 and blocked.json()['detail']=='thread_paused'
        resumed=client.post(f"/api/threads/{thread['id']}/resume")
        assert resumed.status_code==200 and resumed.json()['state']=='RUNNING'
        accepted=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'continue','idempotency_key':'pause-api-resumed'})
        assert accepted.status_code==200
