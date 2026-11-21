import asyncio
import pytest


class CapabilityStoreStub:
    async def authorize_capability(self,claims,usage="request"):
        return {"request_count":1,"public_request_count":0,
            "token_remaining":claims.token_budget}
    async def reserve_model_tokens(self,claims,request_id,requested_output):
        return {"reserved":claims.token_budget,"remaining":claims.token_budget-requested_output}
    async def finalize_model_tokens(self,request_id,usage_tokens):
        return {"charged":usage_tokens or 0}
    async def revoke_capability(self,nonce,expires_at):pass
from fastapi import HTTPException

from sandbox_harness.gateway import bytes_with_idle_timeout, send_with_start_timeout, upstream_suffix


def test_gateway_media_validation_reports_field_path_without_echoing_input():
    from pydantic import ValidationError
    from sandbox_harness.domain import MediaDraftCreate
    from sandbox_harness.gateway import validation_detail
    with pytest.raises(ValidationError) as error:
        MediaDraftCreate.model_validate({"kind":"video","model":"sed2",
            "generation_mode":"first_frame","parameters":{"prompt":"test"},
            "references":[{"project_path":"assets/start.png","role":"first_frame",
                "usage":"first_frame"}],"idempotency_key":"gateway-invalid-reference"})
    detail=validation_detail(error.value)
    assert detail["code"]=="invalid_media_draft"
    assert detail["errors"][0]["path"]=="references.0.usage"
    assert "first_frame" not in str(detail)


def test_gateway_normalizes_native_client_v1_paths_against_versioned_base_url():
    prefix = "/v1/models/claude"
    assert upstream_suffix(prefix + "/v1/messages", prefix) == "/messages"
    assert upstream_suffix(prefix + "/messages", prefix) == "/messages"


def test_gateway_adapts_legacy_claude_thinking_for_adaptive_only_models():
    from sandbox_harness.gateway import normalize_model_request
    payload={"model":"ignored","thinking":{"type":"enabled","budget_tokens":32000},
        "messages":[{"role":"user","content":"hello"}]}
    normalized=normalize_model_request(payload,harness="claude",model="claude-opus-5-5",
        suffix="/messages")
    assert normalized["thinking"]=={"type":"adaptive"}
    assert normalized["output_config"]["effort"]=="high"
    assert "budget_tokens" not in normalized["thinking"]
    assert payload["thinking"]=={"type":"enabled","budget_tokens":32000}


def test_gateway_does_not_rewrite_legacy_or_non_claude_thinking():
    from sandbox_harness.gateway import normalize_model_request
    payload={"thinking":{"type":"enabled","budget_tokens":32000}}
    assert normalize_model_request(payload,harness="claude",model="claude-4.6-sonnet",
        suffix="/messages")["thinking"]==payload["thinking"]
    assert normalize_model_request(payload,harness="codex",model="gpt-5.6",
        suffix="/responses")["thinking"]==payload["thinking"]


def test_gateway_rejects_invalid_public_proxy_policy_at_startup(tmp_path):
    from sandbox_harness.config import Settings
    from sandbox_harness.gateway import create_gateway
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'gateway.db'),
        public_proxy_allowlist=['https://example.com'])
    with pytest.raises(ValueError,match='Invalid public proxy destination rule'):
        create_gateway(settings)


async def test_gateway_fails_model_request_when_upstream_never_starts_response():
    class Client:
        async def send(self, request, stream):
            await asyncio.sleep(1)
    with pytest.raises(HTTPException) as error:
        await send_with_start_timeout(Client(), object(), .01)
    assert error.value.status_code == 504
    assert "did not start" in error.value.detail


async def test_gateway_closes_stream_after_idle_timeout():
    from sandbox_harness.gateway import ModelUpstreamIdleTimeout
    class Response:
        async def aiter_bytes(self):
            yield b"first"
            await asyncio.sleep(1)
            yield b"late"
    rows = []
    with pytest.raises(ModelUpstreamIdleTimeout):
        async for chunk in bytes_with_idle_timeout(Response(), .01):
            rows.append(chunk)
    assert rows == [b"first"]


async def test_gateway_media_status_is_capability_and_thread_scoped(tmp_path, monkeypatch):
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    calls = []
    class Redis:
        async def eval(self, *args): return 1
    class Store(CapabilityStoreStub):
        def __init__(self, *args, **kwargs): pass
        async def get_media_job_for_thread(self, job_id, thread_id):
            calls.append((job_id, thread_id)); return {"id": job_id, "status": "running"}
    monkeypatch.setattr(gateway.Redis, "from_url", lambda *args, **kwargs: Redis())
    monkeypatch.setattr(gateway, "Store", Store)
    settings = Settings(backend="fake", database_url="sqlite+aiosqlite:///" + str(tmp_path / "gateway.db"))
    app = gateway.create_gateway(settings)
    token, _ = CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread-1", turn_id="turn-1",
        sandbox_id="sandbox-1", harness=Harness.CODEX, model="gpt-5.6", routes=["/v1/media"],
        ttl_seconds=60, max_requests=10, max_public_requests=1, token_budget=1000)
    request = Request({"type": "http", "method": "GET", "path": "/v1/media/jobs/job-1",
        "query_string": b"", "headers": [(b"authorization", ("Bearer " + token).encode())]})
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/v1/media/jobs/{job_id}")
    assert await endpoint("job-1", request) == {"id": "job-1", "status": "running"}
    assert calls == [("job-1", "thread-1")]


async def test_gateway_job_artifact_fetch_is_turn_scoped_and_redacted(tmp_path,monkeypatch):
    import json
    from types import SimpleNamespace
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    artifact_id='f'*32;payload=b'reviewed-candidate';digest=__import__('hashlib').sha256(payload).hexdigest()
    class Redis:
        async def eval(self,*args):return 1
    class Store(CapabilityStoreStub):
        reviewed=True
        def __init__(self,*args,**kwargs):pass
        async def get_reviewed_job_artifact_for_thread(self,identity,thread_id):
            if not self.reviewed or identity!=artifact_id or thread_id!='a'*32:return None
            return SimpleNamespace(artifact_id=identity,filename='result.bin',mime='application/octet-stream',
                size=len(payload),sha256=digest,object_ref='sha256:private-object')
    class Objects:
        missing=False
        def size(self,object_ref,*,maximum):
            if self.missing:raise FileNotFoundError(object_ref)
            assert object_ref=='sha256:private-object' and maximum==len(payload)
            return len(payload)
        def iter_bytes(self,object_ref,*,maximum):
            assert object_ref=='sha256:private-object' and maximum==len(payload)
            yield payload[:5];yield payload[5:]
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,'Store',Store)
    monkeypatch.setattr(gateway,'object_store',lambda settings:Objects())
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'fetch.db'))
    app=gateway.create_gateway(settings);codec=CapabilityCodec(settings.gateway_signing_key)
    endpoint=next(route.endpoint for route in app.routes
        if getattr(route,'path',None)=='/v1/job-artifacts/{artifact_id}/content')
    turn_token,_=codec.issue(thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/job-artifacts'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100)
    request=Request({'type':'http','method':'GET','path':f'/v1/job-artifacts/{artifact_id}/content',
        'query_string':b'','headers':[(b'authorization',('Bearer '+turn_token).encode())]})
    response=await endpoint(artifact_id,request)
    body=b''.join([part async for part in response.body_iterator])
    assert body==payload and response.headers['content-length']==str(len(payload))
    assert response.headers['x-updream-artifact-sha256']==digest
    assert 'private-object' not in repr(response.headers)+body.decode(errors='ignore')

    foreign_token,_=codec.issue(thread_id='0'*32,turn_id='1'*32,sandbox_id='sandbox-2',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/job-artifacts'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100)
    foreign=Request({'type':'http','method':'GET','path':f'/v1/job-artifacts/{artifact_id}/content',
        'query_string':b'','headers':[(b'authorization',('Bearer '+foreign_token).encode())]})
    with pytest.raises(HTTPException) as hidden:await endpoint(artifact_id,foreign)
    assert hidden.value.status_code==404

    job_token,_=codec.issue(thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/job-artifacts'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100,scope='job',group_id='c'*32,
        job_id='d'*32,generation=1,source_identity='sandbox-1',descriptor_id='delegate.harness',
        descriptor_version='1',descriptor_digest='e'*64,operations=['check'],
        artifact_ids=[artifact_id],parameter_digest='2'*64)
    job_headers=[(b'authorization',('Bearer '+job_token).encode()),
        (b'x-updream-thread-id',b'a'*32),(b'x-updream-turn-id',b'b'*32),
        (b'x-updream-group-id',b'c'*32),(b'x-updream-job-id',b'd'*32),
        (b'x-updream-job-generation',b'1'),(b'x-updream-source-sandbox',b'sandbox-1'),
        (b'x-updream-descriptor-id',b'delegate.harness'),(b'x-updream-descriptor-version',b'1'),
        (b'x-updream-descriptor-digest',b'e'*64),(b'x-updream-operation',b'check'),
        (b'x-updream-artifact-ids',json.dumps([artifact_id]).encode()),
        (b'x-updream-skill-ids',b'[]')]
    job_request=Request({'type':'http','method':'GET',
        'path':f'/v1/job-artifacts/{artifact_id}/content','query_string':b'','headers':job_headers})
    with pytest.raises(HTTPException) as job_hidden:await endpoint(artifact_id,job_request)
    assert job_hidden.value.status_code==404

    Store.reviewed=False
    with pytest.raises(HTTPException) as unreviewed:await endpoint(artifact_id,request)
    assert unreviewed.value.status_code==404
    Store.reviewed=True;Objects.missing=True
    with pytest.raises(HTTPException) as unavailable:await endpoint(artifact_id,request)
    assert unavailable.value.status_code==503


async def test_gateway_continuously_validates_job_grant_headers_and_parameters(tmp_path, monkeypatch):
    import hashlib,json
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    class Redis:
        async def eval(self,*args):return 1
    class Store(CapabilityStoreStub):
        def __init__(self,*args,**kwargs):pass
        async def get_media_job_for_thread(self,job_id,thread_id):
            return {"id":job_id,"thread_id":thread_id,"status":"running"}
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,'Store',Store)
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'job-grant.db'))
    app=gateway.create_gateway(settings)
    parameter_digest=hashlib.sha256(json.dumps({'query':[],'body':None},sort_keys=True,
        separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(
        thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',harness=Harness.CODEX,
        model='gpt-5.6',routes=['/v1/media/jobs'],ttl_seconds=60,max_requests=10,
        max_public_requests=1,token_budget=100,scope='job',group_id='c'*32,job_id='d'*32,
        generation=2,source_identity='sandbox-1',descriptor_id='media.jobs',
        descriptor_version='1',descriptor_digest='e'*64,operations=['status'],
        capability_ids=['media.jobs'],parameter_digest=parameter_digest)
    headers=[(b'authorization',('Bearer '+token).encode()),
        (b'x-updream-thread-id',b'a'*32),(b'x-updream-turn-id',b'b'*32),
        (b'x-updream-group-id',b'c'*32),(b'x-updream-job-id',b'd'*32),
        (b'x-updream-job-generation',b'2'),(b'x-updream-source-sandbox',b'sandbox-1'),
        (b'x-updream-descriptor-id',b'media.jobs'),(b'x-updream-descriptor-version',b'1'),
        (b'x-updream-descriptor-digest',b'e'*64),(b'x-updream-operation',b'status'),
        (b'x-updream-artifact-ids',b'[]'),(b'x-updream-skill-ids',b'[]'),
        (b'x-updream-capability-id',b'media.jobs')]
    endpoint=next(route.endpoint for route in app.routes
        if getattr(route,'path',None)=='/v1/media/jobs/{job_id}')
    async def receive():return {'type':'http.request','body':b'','more_body':False}
    request=Request({'type':'http','method':'GET','path':'/v1/media/jobs/job-1',
        'query_string':b'','headers':headers},receive)
    result=await endpoint('job-1',request)
    assert result['thread_id']=='a'*32
    bad_headers=[row for row in headers if row[0]!=b'x-updream-job-generation']+[
        (b'x-updream-job-generation',b'3')]
    bad=Request({'type':'http','method':'GET','path':'/v1/media/jobs/job-1',
        'query_string':b'','headers':bad_headers},receive)
    with pytest.raises(HTTPException) as error:
        await endpoint('job-1',bad)
    assert error.value.status_code==403 and 'grant scope' in error.value.detail


async def test_job_execution_catalog_is_filtered_to_the_signed_grant(tmp_path,monkeypatch):
    import json
    from types import SimpleNamespace
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    artifact_id='f'*32
    class Redis:
        async def eval(self,*args):return 1
    class Store(CapabilityStoreStub):
        def __init__(self,*args,**kwargs):pass
        async def owner_id_for_thread(self,thread_id):return 'owner'
        async def turn_input_artifacts(self,turn_id,owner_id):
            return [{'artifact':SimpleNamespace(id=artifact_id,sha256='1'*64,size=12,mime='image/png'),
                'project_file_id':'project-file','project_path':'assets/source.png','source_type':'upload'},
                {'artifact':SimpleNamespace(id='0'*32,sha256='2'*64,size=13,mime='text/plain'),
                'project_file_id':'private','project_path':'assets/private.txt','source_type':'upload'}]
    metadata={'operations':[{'id':'artifact.inspect'},{'id':'private.operation'}],
        'capabilities':[{'id':'allowed.capability'},{'id':'private.capability'}],
        'skills':[{'id':'pptx'},{'id':'private-skill'}],
        'child_execution_profiles':[{'id':'child-general'}]}
    async def snapshot(self):return {}
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,'Store',Store)
    monkeypatch.setattr(gateway.HarnessCatalog,'public_execution_metadata',
        lambda self,availability=None:metadata)
    monkeypatch.setattr(gateway.ExecutionReadiness,'snapshot',snapshot)
    monkeypatch.setattr(gateway.MediaCatalog,'public',lambda self: [{'id':'private-media'}])
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'catalog.db'))
    app=gateway.create_gateway(settings);codec=CapabilityCodec(settings.gateway_signing_key)
    token,_=codec.issue(thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/execution/catalog'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100,scope='job',group_id='c'*32,
        job_id='d'*32,generation=1,source_identity='sandbox-1',descriptor_id='delegate.harness',
        descriptor_version='1',descriptor_digest='e'*64,operations=['check'],
        artifact_ids=[artifact_id],skill_ids=['pptx'],capability_ids=['allowed.capability'],
        operation_ids=['artifact.inspect'],child_profile_id='child-general',
        runtime_profile_id='general-cpu',parameter_digest='3'*64)
    headers=[(b'authorization',('Bearer '+token).encode()),
        (b'x-updream-thread-id',b'a'*32),(b'x-updream-turn-id',b'b'*32),
        (b'x-updream-group-id',b'c'*32),(b'x-updream-job-id',b'd'*32),
        (b'x-updream-job-generation',b'1'),(b'x-updream-source-sandbox',b'sandbox-1'),
        (b'x-updream-descriptor-id',b'delegate.harness'),(b'x-updream-descriptor-version',b'1'),
        (b'x-updream-descriptor-digest',b'e'*64),(b'x-updream-operation',b'check'),
        (b'x-updream-artifact-ids',json.dumps([artifact_id]).encode()),
        (b'x-updream-skill-ids',json.dumps(['pptx']).encode()),
        (b'x-updream-child-profile-id',b'child-general'),
        (b'x-updream-runtime-profile-id',b'general-cpu')]
    endpoint=next(route.endpoint for route in app.routes
        if getattr(route,'path',None)=='/v1/execution/catalog')
    request=Request({'type':'http','method':'GET','path':'/v1/execution/catalog',
        'query_string':b'','headers':headers})
    value=await endpoint(request)
    assert value['job_descriptor']=={'id':'delegate.harness','version':'1','digest':'e'*64}
    assert [row['id'] for row in value['operations']]==['artifact.inspect']
    assert [row['id'] for row in value['capabilities']]==['allowed.capability']
    assert [row['id'] for row in value['skills']]==['pptx']
    assert [row['artifact_id'] for row in value['input_artifacts']]==[artifact_id]
    assert value['child_execution_profiles']==[] and value['media_models']==[]
    assert 'job_group_schema' not in value

    denied_token,_=codec.issue(thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/job-artifacts'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100,scope='job',group_id='c'*32,
        job_id='d'*32,generation=1,source_identity='sandbox-1',descriptor_id='delegate.harness',
        descriptor_version='1',descriptor_digest='e'*64,operations=['check'],parameter_digest='3'*64)
    denied_headers=[(key,(('Bearer '+denied_token).encode() if key==b'authorization' else val))
        for key,val in headers]
    denied=Request({'type':'http','method':'GET','path':'/v1/execution/catalog',
        'query_string':b'','headers':denied_headers})
    with pytest.raises(HTTPException) as error:await endpoint(denied)
    assert error.value.status_code==401


async def test_inline_artifact_inspect_is_fenced_by_job_operation_and_artifact_grants(tmp_path,monkeypatch):
    import json
    from types import SimpleNamespace
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    class Redis:
        async def eval(self,*args):return 1
    artifact_id='f'*32
    class Store(CapabilityStoreStub):
        def __init__(self,*args,**kwargs):pass
        async def get_artifact(self,identity):
            return (SimpleNamespace(id=identity,path='inputs/poster.png',sha256='1'*64,
                size=123,mime='image/png') if identity==artifact_id else None)
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,'Store',Store)
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'inspect.db'))
    app=gateway.create_gateway(settings);codec=CapabilityCodec(settings.gateway_signing_key)
    token,_=codec.issue(thread_id='a'*32,turn_id='b'*32,sandbox_id='sandbox-1',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/operations'],ttl_seconds=60,
        max_requests=10,max_public_requests=1,token_budget=100,scope='job',group_id='c'*32,
        job_id='d'*32,generation=1,source_identity='sandbox-1',descriptor_id='delegate.harness',
        descriptor_version='1',descriptor_digest='e'*64,operations=['check'],
        artifact_ids=[artifact_id],operation_ids=['artifact.inspect'],parameter_digest='2'*64)
    headers=[(b'authorization',('Bearer '+token).encode()),
        (b'x-updream-thread-id',b'a'*32),(b'x-updream-turn-id',b'b'*32),
        (b'x-updream-group-id',b'c'*32),(b'x-updream-job-id',b'd'*32),
        (b'x-updream-job-generation',b'1'),(b'x-updream-source-sandbox',b'sandbox-1'),
        (b'x-updream-descriptor-id',b'delegate.harness'),(b'x-updream-descriptor-version',b'1'),
        (b'x-updream-descriptor-digest',b'e'*64),(b'x-updream-operation',b'check'),
        (b'x-updream-operation-id',b'artifact.inspect'),
        (b'x-updream-artifact-ids',json.dumps([artifact_id]).encode()),
        (b'x-updream-skill-ids',b'[]')]
    endpoint=next(route.endpoint for route in app.routes
        if getattr(route,'path',None)=='/v1/operations/artifact.inspect')
    request=Request({'type':'http','method':'GET','path':'/v1/operations/artifact.inspect',
        'query_string':b'','headers':headers})
    value=await endpoint(artifact_id,request)
    assert value=={'artifact_id':artifact_id,'path':'inputs/poster.png','sha256':'1'*64,
        'size':123,'mime':'image/png'}
    with pytest.raises(HTTPException) as denied:
        await endpoint('0'*32,request)
    assert denied.value.status_code==403


async def test_gateway_site_prepare_is_capability_scoped_and_turn_bound(tmp_path, monkeypatch):
    import io
    import tarfile
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    calls = []
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        content = b"<!doctype html><h1>site</h1>"; info = tarfile.TarInfo("index.html")
        info.size = len(content); archive.addfile(info, io.BytesIO(content))
    body = output.getvalue()

    class Redis:
        async def eval(self, *args): return 1
    class Draft:
        def model_dump(self, mode=None): return {"id":"draft", "thread_id":"thread-1"}
    class Store(CapabilityStoreStub):
        def __init__(self, *args, **kwargs): pass
        async def owner_id_for_thread(self, thread_id): return "a" * 64
        async def create_site_draft(self, owner_id, thread_id, turn_id, **values):
            calls.append((owner_id, thread_id, turn_id, values)); return Draft(), True
    class Objects:
        def put_file(self, path, *, digest, size, **_):
            assert path.stat().st_size == size
            return "sha256:" + digest, digest
    monkeypatch.setattr(gateway.Redis, "from_url", lambda *args, **kwargs: Redis())
    monkeypatch.setattr(gateway, "Store", Store)
    monkeypatch.setattr(gateway, "object_store", lambda settings: Objects())
    settings = Settings(backend="fake", database_url="sqlite+aiosqlite:///" + str(tmp_path / "gateway.db"))
    app = gateway.create_gateway(settings)
    token, _ = CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread-1", turn_id="turn-1",
        sandbox_id="sandbox-1", harness=Harness.CODEX, model="gpt-5.6", routes=["/v1/sites"],
        ttl_seconds=60, max_requests=10, max_public_requests=1, token_budget=1000)
    sent = False
    async def receive():
        nonlocal sent
        if sent:return {"type":"http.request", "body":b"", "more_body":False}
        sent = True; return {"type":"http.request", "body":body, "more_body":False}
    request = Request({"type":"http", "method":"POST", "path":"/v1/sites/drafts",
        "query_string":b"", "headers":[(b"authorization", ("Bearer " + token).encode())]}, receive)
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/v1/sites/drafts")
    response = await endpoint(request, name="Demo", slug="demo-site", idempotency_key="site-key-123")
    assert response["requires_confirmation"] and calls[0][:3] == ("a" * 64, "thread-1", "turn-1")
    assert calls[0][3]["file_count"] == 1 and calls[0][3]["total_size"] == len(content)

    async def empty_receive():return {"type":"http.request","body":b"","more_body":False}
    declared_request=Request({"type":"http","method":"POST","path":"/v1/sites/drafts",
        "query_string":b"","headers":[(b"authorization",("Bearer "+token).encode()),
            (b"content-length",str(gateway.MAX_SITE_ARCHIVE_BYTES+1).encode())]},empty_receive)
    with pytest.raises(HTTPException) as declared_error:
        await endpoint(declared_request,name="Demo",slug="demo-site",idempotency_key="site-key-124")
    assert declared_error.value.status_code==413

    monkeypatch.setattr(gateway,"MAX_SITE_ARCHIVE_BYTES",32);sent=False
    async def oversized_receive():
        nonlocal sent
        if sent:return {"type":"http.request","body":b"","more_body":False}
        sent=True;return {"type":"http.request","body":b"x"*33,"more_body":False}
    chunked_request=Request({"type":"http","method":"POST","path":"/v1/sites/drafts",
        "query_string":b"","headers":[(b"authorization",("Bearer "+token).encode())]},oversized_receive)
    with pytest.raises(HTTPException) as chunked_error:
        await endpoint(chunked_request,name="Demo",slug="demo-site",idempotency_key="site-key-125")
    assert chunked_error.value.status_code==413
    assert not list((settings.data_root/"tmp"/"site-stages").glob("site-ingress-*"))


async def test_gateway_media_prepare_creates_turn_bound_draft(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    calls = []
    expirations = []
    class Redis:
        async def eval(self, *args): return 1
    class Store(CapabilityStoreStub):
        def __init__(self, *args, **kwargs): pass
        async def owner_id_for_thread(self, thread_id): return "owner-1"
        async def create_media_draft(self, owner_id, thread_id, value, **kwargs):
            expirations.append(kwargs["expires_at"])
            calls.append((owner_id, thread_id, value.turn_id, value.kind, value.model, value.parameters))
            return SimpleNamespace(model_dump=lambda **_: {"id": "draft-1", "status": "awaiting_confirmation"}), True
        async def seal_media_batch(self, turn_id, thread_id, owner_id):
            calls.append(("commit", owner_id, thread_id, turn_id))
            return {"turn_id": turn_id, "status": "sealed", "draft_count": 1}
    monkeypatch.setattr(gateway.Redis, "from_url", lambda *args, **kwargs: Redis())
    monkeypatch.setattr(gateway, "Store", Store)
    settings = Settings(backend="fake", database_url="sqlite+aiosqlite:///" + str(tmp_path / "gateway.db"))
    app = gateway.create_gateway(settings)
    turn_id = "a" * 32
    token, _ = CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread-1", turn_id=turn_id,
        sandbox_id="sandbox-1", harness=Harness.CODEX, model="gpt-5.6", routes=["/v1/media"],
        ttl_seconds=60, max_requests=10, max_public_requests=1, token_budget=1000)
    body = json.dumps({"kind": "image", "model": "seedream-5.0", "parameters": {"prompt": "test"},
        "idempotency_key": "prepare-key-1"}).encode()
    sent = False
    async def receive():
        nonlocal sent
        if sent: return {"type": "http.request", "body": b"", "more_body": False}
        sent = True; return {"type": "http.request", "body": body, "more_body": False}
    request = Request({"type": "http", "method": "POST", "path": "/v1/media/drafts",
        "query_string": b"", "headers": [(b"authorization", ("Bearer " + token).encode()),
        (b"content-type", b"application/json")]}, receive)
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/v1/media/drafts")
    result = await endpoint(request)
    assert result["id"] == "draft-1" and result["requires_confirmation"] is True
    assert calls == [("owner-1", "thread-1", turn_id, "image", "seedream-5.0", {"prompt": "test"})]
    assert 23 * 3600 < (expirations[0] - __import__('sandbox_harness.domain',fromlist=['now']).now()).total_seconds() <= 24 * 3600
    commit_endpoint = next(route.endpoint for route in app.routes
        if getattr(route, "path", None) == "/v1/media/batch/commit")
    async def empty_receive():return {"type":"http.request","body":b"","more_body":False}
    committed = await commit_endpoint(Request({"type": "http", "method": "POST",
        "path": "/v1/media/batch/commit", "query_string": b"", "headers": [
            (b"authorization", ("Bearer " + token).encode())]},empty_receive))
    assert committed["status"] == "sealed"
    assert calls[-1] == ("commit", "owner-1", "thread-1", turn_id)


async def test_gateway_media_candidate_content_and_promotion_are_thread_scoped(tmp_path, monkeypatch):
    import json
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    promoted = []
    class Redis:
        async def eval(self, *args): return 1
    class Store(CapabilityStoreStub):
        def __init__(self, *args, **kwargs): pass
        async def media_job_output_for_thread(self, job_id, thread_id):
            assert (job_id, thread_id) == ("job-1", "thread-1")
            return {"object_ref": "sha256:" + "1" * 64, "mime": "image/png",
                "size": 9, "filename": "job-1.png"}
        async def promote_media_job_for_thread(self, job_id, thread_id, role):
            promoted.append((job_id, thread_id, role))
            return {"job_id": job_id, "output_role": role, "project_path": "artifacts/media/job-1.png"}
    class Objects:
        def get(self, reference, *, maximum):
            assert reference == "sha256:" + "1" * 64 and maximum > 9
            return b"candidate"
    monkeypatch.setattr(gateway.Redis, "from_url", lambda *args, **kwargs: Redis())
    monkeypatch.setattr(gateway, "Store", Store)
    monkeypatch.setattr(gateway, "object_store", lambda settings: Objects())
    settings = Settings(backend="fake", database_url="sqlite+aiosqlite:///" + str(tmp_path / "gateway-output.db"))
    app = gateway.create_gateway(settings)
    token, _ = CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread-1", turn_id="a" * 32,
        sandbox_id="sandbox-1", harness=Harness.CODEX, model="gpt-5.6", routes=["/v1/media"],
        ttl_seconds=60, max_requests=10, max_public_requests=1, token_budget=1000)
    headers = [(b"authorization", ("Bearer " + token).encode())]
    content_endpoint = next(route.endpoint for route in app.routes
        if getattr(route, "path", None) == "/v1/media/jobs/{job_id}/content")
    response = await content_endpoint("job-1", Request({"type": "http", "method": "GET",
        "path": "/v1/media/jobs/job-1/content", "query_string": b"", "headers": headers}))
    assert response.body == b"candidate" and response.media_type == "image/png"
    body = json.dumps({"output_role": "delivery"}).encode()
    async def receive(): return {"type": "http.request", "body": body, "more_body": False}
    promote_endpoint = next(route.endpoint for route in app.routes
        if getattr(route, "path", None) == "/v1/media/jobs/{job_id}/promote")
    result = await promote_endpoint("job-1", Request({"type": "http", "method": "POST",
        "path": "/v1/media/jobs/job-1/promote", "query_string": b"", "headers": headers}, receive))
    assert result["output_role"] == "delivery"
    assert promoted == [("job-1", "thread-1", "delivery")]


async def test_terminal_model_step_is_saved_before_last_frame_is_forwarded(tmp_path, monkeypatch):
    import httpx,json
    from starlette.requests import Request
    import sandbox_harness.gateway as gateway
    from sandbox_harness.model_activity import OutputProbe
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    from sandbox_harness.capability import CapabilityCodec
    class Redis:
        async def eval(self,*args):return 1
        async def exists(self,*args):return 0
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *a,**k:Redis())
    monkeypatch.setattr(gateway,'Store',lambda *a,**k:CapabilityStoreStub())
    observations=[]
    class Activity:
        def __init__(self,*args):self.probe=OutputProbe();self.finished=False;self.request_id='model-request';observations.append(self)
        async def start(self):pass
        async def observe(self,data):self.probe.feed(data)
        async def finish(self,*args,**kwargs):self.finished=True
    monkeypatch.setattr(gateway,'ModelActivity',Activity)
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n'
            yield b'data: {"type":"response.completed"}\n\n'
    async def respond(*args):return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream())
    monkeypatch.setattr(gateway,'send_with_start_timeout',respond)
    settings=Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'gateway.db'),upstream_codex='http://upstream')
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id='thread',turn_id='turn',sandbox_id='sandbox',
        harness=Harness.CODEX,model='gpt-5.6',routes=['/v1/models/codex'],ttl_seconds=60,max_requests=10,max_public_requests=1,token_budget=1000)
    async def receive():return {'type':'http.request','body':b'{"model":"gpt-5.6","stream":true}','more_body':False}
    request=Request({'type':'http','method':'POST','path':'/v1/models/codex/responses','query_string':b'',
        'headers':[(b'authorization',('Bearer '+token).encode())]},receive)
    endpoint=next(r.endpoint for r in app.routes if getattr(r,'path',None)=='/{path:path}')
    response=await endpoint('v1/models/codex/responses',request)
    stream=response.body_iterator
    await anext(stream);assert not observations[0].finished
    last=await anext(stream);assert b'response.completed' in last and observations[0].finished
    await stream.aclose()
