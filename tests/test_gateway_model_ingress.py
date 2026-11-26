"""Model image history has a separate bounded ingress budget."""
import base64
import asyncio
import json
import struct
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from sandbox_harness.common.security.capability import (CapabilityCodec)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.models.model_activity import (OutputProbe)


@pytest.fixture
def ingress(tmp_path,monkeypatch):
    import sandbox_harness.services.sandbox_gateway.gateway as gateway
    state=SimpleNamespace(upstream=[],activities=[],reservations=[],reservation_error=None)
    class SecurityStore:
        async def authorize_capability(self,*args,**kwargs):
            return {'token_remaining':100_000_000}
        async def reserve_model_tokens(self,*args):
            if state.reservation_error is not None:raise state.reservation_error
            state.reservations.append(args)
        async def finalize_model_tokens(self,*args):return {'charged':1}
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *args,**kwargs:object())
    monkeypatch.setattr(gateway,'Store',lambda *args,**kwargs:SecurityStore())
    class Activity:
        def __init__(self,*args):
            self.request_id='request-'+str(len(state.activities));self.probe=OutputProbe()
            self.span=None;self.started=False;self.finished=False;self.finish_calls=[]
            state.activities.append(self)
        async def start(self):self.started=True
        async def observe(self,data):self.probe.feed(data)
        async def finish(self,*args,**kwargs):
            self.finished=True;self.finish_calls.append((args,kwargs));self.probe.finish()
    monkeypatch.setattr(gateway,'ModelActivity',Activity)
    async def respond(_client,request,*args):
        state.upstream.append(request)
        return httpx.Response(200,headers={'content-type':'application/json'},
            content=b'{"output":[],"usage":{"input_tokens":1,"output_tokens":1}}')
    monkeypatch.setattr(gateway,'send_with_start_timeout',respond)
    def build(**overrides):
        settings=Settings(_env_file=None,backend='fake',data_root=tmp_path,
            database_url='sqlite+aiosqlite:///'+str(tmp_path/'ingress.db'),
            upstream_codex='http://unused',upstream_claude='http://unused',
            upstream_deepseek='http://unused',upstream_image='http://unused',**overrides)
        app=gateway.create_gateway(settings)
        endpoint=next(r.endpoint for r in app.routes if getattr(r,'path',None)=='/{path:path}')
        async def invoke(body,*,harness=Harness.CODEX,declared=True,model=True,
                         raw_length=None,receive_error=None):
            prefix='/v1/models/'+harness.value if model else '/v1/providers/image'
            suffix='/responses' if harness==Harness.CODEX else '/messages'
            path=prefix+(suffix if model else '/generate')
            token,_=CapabilityCodec(settings.gateway_signing_key).issue(
                thread_id='thread',turn_id='turn',sandbox_id='sandbox',harness=harness,
                model='selected-model',routes=[prefix],ttl_seconds=60,max_requests=10,
                max_public_requests=1,token_budget=100_000_000)
            headers=[(b'authorization',('Bearer '+token).encode())]
            if declared:headers.append((b'content-length',
                str(len(body) if raw_length is None else raw_length).encode()))
            chunks=iter([body[:len(body)//2],body[len(body)//2:]])
            async def receive():
                if receive_error is not None:raise receive_error
                chunk=next(chunks,b'')
                return {'type':'http.request','body':chunk,'more_body':bool(chunk)}
            request=Request({'type':'http','method':'POST','path':path,
                'query_string':b'','headers':headers},receive)
            response=await endpoint(path.removeprefix('/'),request)
            return b''.join([chunk async for chunk in response.body_iterator])
        return settings,invoke
    return state,build


@pytest.mark.parametrize('harness',list(Harness))
@pytest.mark.parametrize('declared',[True,False])
async def test_model_history_uses_its_limit_without_relaxing_other_routes(ingress,harness,declared):
    state,build=ingress
    settings,invoke=build(gateway_request_max_bytes=1024,gateway_model_request_max_bytes=4096)
    body=json.dumps({'input':'x'*1500}).encode()
    await invoke(body,harness=harness,declared=declared)
    assert len(state.upstream)==1
    assert json.loads(state.upstream[0].content)['input']=='x'*1500
    assert state.activities[0].started and state.activities[0].finished
    with pytest.raises(HTTPException) as error:
        await invoke(body,model=False,declared=declared)
    assert error.value.status_code==413 and error.value.detail=='request_too_large'
    assert len(state.upstream)==1 and settings.public_proxy_request_max_bytes==16*1024*1024


async def test_default_model_budget_accepts_accumulated_inline_image_history(ingress):
    state,build=ingress;settings,invoke=build()
    png=b'\x89PNG\r\n\x1a\n'+b'\x00\x00\x00\rIHDR'+struct.pack('>II',1968,1023)+b'x'*(7*1024*1024)
    image='data:image/png;base64,'+base64.b64encode(png).decode()
    payload={'input':[{'role':'user','content':[{'type':'input_image','image_url':image}]}]*2}
    body=json.dumps(payload).encode()
    assert len(body)>16*1024*1024
    await invoke(body)
    forwarded=json.loads(state.upstream[0].content)
    assert forwarded['input']==payload['input']
    assert settings.gateway_model_request_max_bytes==64*1024*1024


@pytest.mark.parametrize('declared',[True,False])
async def test_model_ingress_rejection_is_observed_without_upstream_side_effects(ingress,declared):
    state,build=ingress
    _,invoke=build(gateway_request_max_bytes=1024,gateway_model_request_max_bytes=2048)
    with pytest.raises(HTTPException) as error:
        await invoke(b'x'*2049,declared=declared)
    assert error.value.status_code==413 and error.value.detail=='model_request_too_large'
    assert len(state.activities)==1 and state.activities[0].started
    assert state.activities[0].finish_calls==[((413,),{'error_code':'model_request_too_large'})]
    assert not state.upstream and not state.reservations


@pytest.mark.parametrize(('body','raw_length','receive_error','status'),[
    (b'{}','invalid',None,400),
    (b'{}',-1,None,400),
    (b'{}',3,None,400),
    (b'{}',None,TimeoutError(),408),
    (b'not json',None,None,422),
    (b'[]',None,None,422),
    (b'{}',None,asyncio.CancelledError(),None),
])
async def test_every_ingress_failure_finishes_started_model_activity(
        ingress,body,raw_length,receive_error,status):
    state,build=ingress;_,invoke=build()
    error_type=asyncio.CancelledError if status is None else HTTPException
    with pytest.raises(error_type):
        await invoke(body,raw_length=raw_length,receive_error=receive_error)
    assert len(state.activities)==1 and state.activities[0].started
    assert state.activities[0].finished and len(state.activities[0].finish_calls)==1
    assert state.activities[0].finish_calls[0][0]==(status,)
    assert state.activities[0].finish_calls[0][1]['error_code']
    assert not state.upstream and not state.reservations


async def test_normalization_failure_finishes_model_activity(ingress,monkeypatch):
    import sandbox_harness.services.sandbox_gateway.gateway as gateway
    state,build=ingress;_,invoke=build()
    def invalid(*args,**kwargs):raise TypeError('invalid native request shape')
    monkeypatch.setattr(gateway,'normalize_model_request',invalid)
    with pytest.raises(TypeError):await invoke(b'{}')
    assert state.activities[0].started and state.activities[0].finished
    assert state.activities[0].finish_calls==[((None,),{'error_code':'TypeError'})]
    assert not state.upstream and not state.reservations


@pytest.mark.parametrize(('error','status','code'),[
    (ValueError('capability_revoked'),409,'capability_revoked'),
    (ValueError('capability_expired'),409,'capability_expired'),
    (RuntimeError('private authority detail'),503,'capability_authority_unavailable'),
])
async def test_token_reservation_failure_finishes_activity(ingress,error,status,code):
    state,build=ingress;_,invoke=build();state.reservation_error=error
    with pytest.raises(HTTPException) as captured:await invoke(b'{}')
    assert captured.value.status_code==status and captured.value.detail==code
    assert state.activities[0].started and state.activities[0].finished
    assert state.activities[0].finish_calls==[((status,),{'error_code':code})]
    assert not state.upstream and not state.reservations
