import base64
import json
import struct

import pytest
from starlette.requests import Request

from sandbox_harness.capability import CapabilityCodec
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.gateway import (enforce_model_output_budget,
    estimate_model_input_tokens,model_endpoint_kind)
from sandbox_harness.store import Store


def claims(settings,*,token_budget=10,max_requests=3):
    return CapabilityCodec(settings.gateway_signing_key).issue(
        thread_id="thread",turn_id="turn",sandbox_id="sandbox",harness=Harness.CODEX,
        model="gpt-5.6",routes=["/v1/models/codex"],ttl_seconds=3600,
        max_requests=max_requests,max_public_requests=2,token_budget=token_budget)[1]


@pytest.mark.parametrize(("harness","suffix","kind"),[
    (Harness.CODEX,"/responses","generation"),
    (Harness.CODEX,"/responses/compact","compaction"),
    (Harness.CLAUDE,"/messages","generation"),
    (Harness.CLAUDE,"/messages/count_tokens","token_count"),
    (Harness.DEEPSEEK,"/messages","generation"),
    (Harness.DEEPSEEK,"/messages/count_tokens","token_count"),
])
def test_native_model_endpoint_policy_is_explicit(harness,suffix,kind):
    assert model_endpoint_kind(harness,suffix)==kind


@pytest.mark.parametrize(("harness","suffix"),[
    (Harness.CODEX,"/messages"),(Harness.CLAUDE,"/responses/compact"),
    (Harness.DEEPSEEK,"/responses"),(Harness.DEEPSEEK,"/chat/completions"),
])
def test_native_model_endpoint_policy_rejects_cross_protocol(harness,suffix):
    assert model_endpoint_kind(harness,suffix) is None


def test_inline_image_budget_uses_dimensions_instead_of_base64_size():
    png=(b'\x89PNG\r\n\x1a\n'+b'\x00\x00\x00\rIHDR'+struct.pack('>II',972,896)+
        b'\x08\x06\x00\x00\x00'+b'x'*(1450*1024))
    uri='data:image/png;base64,'+base64.b64encode(png).decode()
    payload={'input':[{'role':'user','content':[{'type':'input_text','text':'检查图片'},
        {'type':'input_image','image_url':uri}]}]}
    raw=json.dumps(payload,separators=(',',':')).encode()
    estimate=estimate_model_input_tokens(payload,len(raw))
    assert len(raw)>1_900_000 and 4_000<estimate<20_000
    assert enforce_model_output_budget(payload,'/responses',100_000,estimate)==8192


def test_malformed_image_falls_back_to_raw_request_ceiling():
    payload={'input':[{'type':'input_image','image_url':'data:image/png;base64,***'}]}
    size=len(json.dumps(payload).encode())
    assert estimate_model_input_tokens(payload,size)==size


async def test_capability_and_model_budget_survive_store_restart(tmp_path):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"capabilities.db"))
    value=claims(settings,token_budget=10,max_requests=1)
    store=Store(settings.database_url);await store.initialize()
    assert (await store.authorize_capability(value,"request"))["request_count"]==1
    with pytest.raises(ValueError,match="capability_request_limit"):
        await store.authorize_capability(value,"request")
    assert (await store.reserve_model_tokens(value,"request-one",6))["reserved"]==10
    with pytest.raises(ValueError,match="model_token_budget_exhausted"):
        await store.reserve_model_tokens(value,"request-two",1)
    assert (await store.finalize_model_tokens("request-one",4))["charged"]==4
    assert (await store.reserve_model_tokens(value,"request-two",6))["reserved"]==6
    assert (await store.finalize_model_tokens("request-two",None))["charged"]==6
    await store.revoke_capability(value.nonce,value.expires_at);await store.close()
    restarted=Store(settings.database_url);await restarted.initialize()
    try:
        with pytest.raises(ValueError,match="capability_revoked"):
            await restarted.authorize_capability(value,"check")
    finally:await restarted.close()


async def test_native_compaction_and_count_routes_keep_native_payload_semantics(tmp_path,monkeypatch):
    import httpx
    import sandbox_harness.gateway as gateway
    from sandbox_harness.model_activity import OutputProbe
    class Redis:pass
    class SecurityStore:
        def __init__(self,*args,**kwargs):self.reservations=[];self.finalized=[]
        async def authorize_capability(self,*args,**kwargs):
            return {"request_count":1,"public_request_count":0,"token_remaining":100_000}
        async def reserve_model_tokens(self,claims,request_id,requested_output):
            self.reservations.append((request_id,requested_output));return {"reserved":100_000}
        async def finalize_model_tokens(self,request_id,usage):
            self.finalized.append((request_id,usage));return {"charged":usage or 100_000}
    store=SecurityStore();requests=[];activities=[]
    monkeypatch.setattr(gateway.Redis,"from_url",lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,"Store",lambda *args,**kwargs:store)
    class Activity:
        def __init__(self,*args):
            self.request_id="request-"+str(len(activities));self.probe=OutputProbe()
            self.finished=False;self.header_ms=None;self.span=None;activities.append(self)
        async def start(self):pass
        async def observe(self,data):self.probe.feed(data)
        async def finish(self,*args,**kwargs):self.probe.finish();self.finished=True
    monkeypatch.setattr(gateway,"ModelActivity",Activity)
    async def respond(_client,request,*args):
        requests.append(request)
        body=(b'{"input_tokens":321}' if request.url.path.endswith("count_tokens") else
            b'{"output":[],"usage":{"input_tokens":12,"output_tokens":3}}')
        return httpx.Response(200,headers={"content-type":"application/json"},content=body)
    monkeypatch.setattr(gateway,"send_with_start_timeout",respond)
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"routes.db"),
        upstream_codex="http://codex",upstream_claude="http://claude")
    endpoint=next(route.endpoint for route in gateway.create_gateway(settings).routes
        if getattr(route,"path",None)=="/{path:path}")
    async def invoke(harness,suffix,payload,query=b""):
        token,_=CapabilityCodec(settings.gateway_signing_key).issue(
            thread_id="thread",turn_id="turn",sandbox_id="sandbox",harness=harness,
            model="selected-model",routes=["/v1/models/"+harness.value],ttl_seconds=60,
            max_requests=10,max_public_requests=1,token_budget=100_000)
        body=json.dumps(payload).encode();sent=False
        async def receive():
            nonlocal sent
            if sent:return {"type":"http.request","body":b"","more_body":False}
            sent=True;return {"type":"http.request","body":body,"more_body":False}
        path=f"/v1/models/{harness.value}{suffix}"
        response=await endpoint(path.removeprefix("/"),Request({"type":"http","method":"POST",
            "path":path,"query_string":query,"headers":[(b"authorization",("Bearer "+token).encode())]},receive))
        return json.loads(b"".join([chunk async for chunk in response.body_iterator]))
    counted=await invoke(Harness.CLAUDE,"/messages/count_tokens",
        {"model":"ignored","messages":[{"role":"user","content":"hello"}]},b"beta=true")
    assert counted=={"input_tokens":321}
    assert requests[0].url.path=="/messages/count_tokens" and requests[0].url.query==b"beta=true"
    assert store.reservations==[] and activities==[]
    compacted=await invoke(Harness.CODEX,"/responses/compact",
        {"model":"ignored","input":[{"role":"user","content":"keep this"}]})
    assert compacted["usage"]=={"input_tokens":12,"output_tokens":3}
    body=json.loads(requests[1].content)
    assert body["model"]=="selected-model" and "max_output_tokens" not in body
    assert store.finalized==[("request-0",15)]
