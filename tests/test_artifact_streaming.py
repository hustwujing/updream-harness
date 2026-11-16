import os
from pathlib import Path

import pytest

import sandbox_runtime.app as runtime_app
from sandbox_runtime import artifact_cli


def test_runtime_stages_stable_regular_file_and_rejects_symlink_special_and_escape(tmp_path,monkeypatch):
    outputs=tmp_path/'outputs';outputs.mkdir();monkeypatch.setattr(runtime_app,'OUTPUTS',outputs)
    source=outputs/'result.txt';source.write_text('stable')
    staged=runtime_app.stage_regular_file(source,tmp_path/'stage',1024)
    try:assert staged.read_text()=='stable' and staged.parent==tmp_path/'stage'
    finally:staged.unlink()
    link=outputs/'link.txt';link.symlink_to(source)
    with pytest.raises(ValueError,match='regular file'):
        runtime_app.stage_regular_file(link,tmp_path/'stage',1024)
    fifo=outputs/'pipe';os.mkfifo(fifo)
    with pytest.raises(ValueError,match='regular file'):
        runtime_app.stage_regular_file(fifo,tmp_path/'stage',1024)
    outside=tmp_path/'outside.txt';outside.write_text('outside')
    with pytest.raises(ValueError,match='/workspace/outputs'):
        runtime_app.stage_regular_file(outside,tmp_path/'stage',1024)


def test_runtime_rejects_source_changed_during_staging_and_cleans_snapshot(tmp_path,monkeypatch):
    outputs=tmp_path/'outputs';outputs.mkdir();monkeypatch.setattr(runtime_app,'OUTPUTS',outputs)
    source=outputs/'changing.txt';source.write_text('before')
    with pytest.raises(RuntimeError,match='changed during staging'):
        runtime_app.stage_regular_file(source,tmp_path/'stage',1024,
            after_copy=lambda:source.write_text('after-with-different-size'))
    assert list((tmp_path/'stage').iterdir())==[]


def test_artifact_cli_posts_metadata_to_runtime_without_reading_file(tmp_path,monkeypatch,capsys):
    calls=[]
    class Response:is_success=True;text='{"artifact_id":"a"}'
    monkeypatch.setattr(artifact_cli.httpx,'post',lambda url,**kwargs:calls.append((url,kwargs)) or Response())
    monkeypatch.setenv('SANDBOX_RUNTIME_URL','http://runtime.test')
    result=artifact_cli.main(['emit','--job-id','a'*32,'--generation','2','--path','/workspace/outputs/a.pdf',
        '--role','report','--item-key','main','--position','0','--filename','report.pdf',
        '--disposition','delivery','--mime','application/pdf'])
    assert result==0 and calls[0][0]=='http://runtime.test/v1/job-artifacts/stage'
    assert calls[0][1]['json']['artifact_role']=='report'
    assert 'artifact_id' in capsys.readouterr().out


async def test_gateway_streams_job_artifact_under_exact_output_grant(tmp_path,monkeypatch):
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    calls=[]
    class Redis:
        async def eval(self,*args):return 1
    class Store:
        def __init__(self,*args,**kwargs):pass
        async def bind_job_artifact(self,value,**identity):
            calls.append((value,identity));return {'artifact_id':'f'*32,
                'artifact_role':value.artifact_role,'output_disposition':'candidate'}
    class Objects:
        def put_file(self,path,*,digest,size):
            assert path.read_bytes()==b'%PDF-1.7\nstreamed'
            return 'sha256:'+digest,digest
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,'Store',Store)
    monkeypatch.setattr(gateway,'object_store',lambda settings:Objects())
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'gateway.db'),
        redis_url='redis://127.0.0.1:6379/15',data_root=tmp_path)
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id='a'*32,turn_id='b'*32,
        sandbox_id='sandbox-1',harness=Harness.CODEX,model='gpt',routes=['/v1/job-artifacts'],
        ttl_seconds=60,max_requests=10,max_public_requests=1,token_budget=10,scope='job',
        group_id='c'*32,job_id='d'*32,generation=2,source_identity='sandbox-1',
        descriptor_id='render',descriptor_version='1',descriptor_digest='e'*64,
        operations=['emit'],output_scopes=['report:delivery'],parameter_digest='1'*64)
    headers=[(b'authorization',('Bearer '+token).encode()),(b'x-updream-thread-id',b'a'*32),
        (b'x-updream-turn-id',b'b'*32),(b'x-updream-group-id',b'c'*32),
        (b'x-updream-job-id',b'd'*32),(b'x-updream-job-generation',b'2'),
        (b'x-updream-source-sandbox',b'sandbox-1'),(b'x-updream-descriptor-id',b'render'),
        (b'x-updream-descriptor-version',b'1'),(b'x-updream-descriptor-digest',b'e'*64),
        (b'x-updream-operation',b'emit'),(b'x-updream-artifact-ids',b'[]'),
        (b'x-updream-skill-ids',b'[]'),(b'x-updream-output-scope',b'report:delivery')]
    sent=False
    async def receive():
        nonlocal sent
        if sent:return {'type':'http.request','body':b'','more_body':False}
        sent=True;return {'type':'http.request','body':b'%PDF-1.7\nstreamed','more_body':False}
    request=Request({'type':'http','method':'POST','path':'/v1/job-artifacts/emit',
        'query_string':b'','headers':headers},receive)
    endpoint=next(route.endpoint for route in app.routes
        if getattr(route,'path',None)=='/v1/job-artifacts/emit')
    result=await endpoint(request,'report','main',0,'report.pdf','delivery','application/pdf')
    assert result['artifact_id']=='f'*32 and calls[0][0].job_id=='d'*32
