import asyncio
import json

import pytest
from starlette.requests import Request

from sandbox_harness.capability import CapabilityCodec
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.gateway import bounded_request_body, enforce_model_output_budget
from sandbox_harness.store import Store


def claims(settings, *, nonce="", token_budget=10, max_requests=3, parent_nonce="",
           parent_token_budget=0):
    _, value = CapabilityCodec(settings.gateway_signing_key).issue(
        thread_id="thread", turn_id="turn", sandbox_id="sandbox", harness=Harness.CODEX,
        model="gpt-5.6", routes=["/v1/models/codex"], ttl_seconds=3600,
        max_requests=max_requests, max_public_requests=2, token_budget=token_budget,
        parent_nonce=parent_nonce, parent_token_budget=parent_token_budget)
    return value.model_copy(update={"nonce":nonce}) if nonce else value


@pytest.mark.parametrize(("suffix","field"), [
    ("/responses","max_output_tokens"),
    ("/messages","max_tokens"),
    ("/chat/completions","max_tokens"),
])
def test_native_model_protocols_enforce_output_budget(suffix,field):
    with pytest.raises(Exception) as captured:
        enforce_model_output_budget({field:100_000},suffix,1)
    assert captured.value.status_code==429
    payload={};assert enforce_model_output_budget(payload,suffix,7)==7
    assert payload[field]==7


async def test_durable_capability_counts_revocation_and_token_reservations_survive_restart(tmp_path):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"capabilities.db"))
    value=claims(settings,token_budget=10,max_requests=1)
    store=Store(settings.database_url);await store.initialize()
    assert (await store.authorize_capability(value,"request"))["request_count"]==1
    first=await store.reserve_model_tokens(value,"request-one",6)
    assert first["reserved"]==10
    results=await asyncio.gather(
        store.reserve_model_tokens(value,"request-two",1),
        store.reserve_model_tokens(value,"request-three",1),return_exceptions=True)
    assert all(isinstance(item,ValueError) and str(item)=="model_token_budget_exhausted"
        for item in results)
    assert (await store.finalize_model_tokens("request-one",4))["charged"]==4
    assert (await store.finalize_model_tokens("request-one",9))=={"charged":4,"reused":True}
    second=await store.reserve_model_tokens(value,"request-two",6)
    assert second["reserved"]==6
    assert (await store.finalize_model_tokens("request-two",None))["charged"]==6
    await store.revoke_capability(value.nonce,value.expires_at);await store.close()

    restarted=Store(settings.database_url);await restarted.initialize()
    try:
        with pytest.raises(ValueError,match="capability_revoked"):
            await restarted.authorize_capability(value,"check")
        with pytest.raises(ValueError,match="capability_revoked"):
            await restarted.authorize_capability(value,"request")
    finally:await restarted.close()


async def test_request_limit_survives_gateway_store_restart_without_redis(tmp_path):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"request-count.db"))
    value=claims(settings,max_requests=1)
    public=claims(settings,max_requests=5)
    first=Store(settings.database_url);await first.initialize()
    await first.authorize_capability(value,"request")
    await first.authorize_capability(public,"public");await first.authorize_capability(public,"public")
    await first.close()
    restarted=Store(settings.database_url);await restarted.initialize()
    try:
        with pytest.raises(ValueError,match="capability_request_limit"):
            await restarted.authorize_capability(value,"request")
        with pytest.raises(ValueError,match="capability_public_request_limit"):
            await restarted.authorize_capability(public,"public")
    finally:await restarted.close()


async def test_gateway_revocation_survives_redis_reset(tmp_path,monkeypatch):
    import sandbox_harness.gateway as gateway
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"redis-reset.db"))
    store=Store(settings.database_url);await store.initialize()
    class Redis:
        def __init__(self):self.values={}
        async def set(self,key,value,**kwargs):self.values[key]=value
    redis=Redis()
    monkeypatch.setattr(gateway.Redis,"from_url",lambda *args,**kwargs:redis)
    monkeypatch.setattr(gateway,"Store",lambda *args,**kwargs:store)
    app=gateway.create_gateway(settings)
    token,value=CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread",turn_id="turn",
        sandbox_id="sandbox",harness=Harness.CODEX,model="gpt-5.6",
        routes=["/v1/not-configured"],ttl_seconds=60,max_requests=5,
        max_public_requests=2,token_budget=10)
    proxy=next(route.endpoint for route in app.routes if getattr(route,"path",None)=="/{path:path}")
    def request(path,extra=()):return Request({"type":"http","method":"GET","path":path,
        "query_string":b"","headers":[(b"authorization",("Bearer "+token).encode()),*extra]})
    try:
        with pytest.raises(Exception) as before:
            await proxy("v1/not-configured",request("/v1/not-configured"))
        assert before.value.status_code==404
        revoke=next(route.endpoint for route in app.routes
            if getattr(route,"path",None)=="/internal/revoke/{nonce}")
        await revoke(value.nonce,request("/internal/revoke/"+value.nonce,
            ((b"x-sandbox-harness-control",settings.gateway_control_key.encode()),)))
        redis.values.clear()
        with pytest.raises(Exception) as after:
            await proxy("v1/not-configured",request("/v1/not-configured"))
        assert after.value.status_code==401 and after.value.detail=="capability_revoked"
    finally:await store.close()


async def test_gateway_fails_closed_when_capability_authority_is_unavailable(tmp_path,monkeypatch):
    import sandbox_harness.gateway as gateway
    class Redis:pass
    class BrokenStore:
        def __init__(self,*args,**kwargs):pass
        async def authorize_capability(self,*args,**kwargs):raise RuntimeError("database unavailable")
    monkeypatch.setattr(gateway.Redis,"from_url",lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,"Store",BrokenStore)
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"authority-down.db"))
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread",turn_id="turn",
        sandbox_id="sandbox",harness=Harness.CODEX,model="gpt-5.6",routes=["/v1/down"],
        ttl_seconds=60,max_requests=2,max_public_requests=1,token_budget=10)
    request=Request({"type":"http","method":"GET","path":"/v1/down","query_string":b"",
        "headers":[(b"authorization",("Bearer "+token).encode())]})
    endpoint=next(route.endpoint for route in app.routes if getattr(route,"path",None)=="/{path:path}")
    with pytest.raises(Exception) as captured:await endpoint("v1/down",request)
    assert captured.value.status_code==503
    assert captured.value.detail=="capability_authority_unavailable"


async def test_child_grants_share_parent_token_budget(tmp_path):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"parent-budget.db"))
    store=Store(settings.database_url);await store.initialize()
    try:
        first=claims(settings,nonce="child-one",token_budget=8,
            parent_nonce="parent",parent_token_budget=10)
        second=claims(settings,nonce="child-two",token_budget=8,
            parent_nonce="parent",parent_token_budget=10)
        await store.authorize_capability(first,"check");await store.authorize_capability(second,"check")
        assert (await store.reserve_model_tokens(first,"child-request-one",8))["reserved"]==8
        with pytest.raises(ValueError,match="model_token_budget_exhausted"):
            await store.reserve_model_tokens(second,"child-request-two",3)
        await store.finalize_model_tokens("child-request-one",4)
        assert (await store.reserve_model_tokens(second,"child-request-two",3))["reserved"]==6
    finally:await store.close()


async def test_concurrent_durable_request_counter_has_one_winner(tmp_path):
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"request-race.db"))
    value=claims(settings,max_requests=1);stores=[Store(settings.database_url) for _ in range(8)]
    await stores[0].initialize();await stores[0].authorize_capability(value,"check")
    try:
        results=await asyncio.gather(*(store.authorize_capability(value,"request")
            for store in stores),return_exceptions=True)
        assert sum(isinstance(item,dict) for item in results)==1
        assert sum(isinstance(item,ValueError) and str(item)=="capability_request_limit"
            for item in results)==7
    finally:await asyncio.gather(*(store.close() for store in stores))


def test_output_probe_reads_non_stream_and_three_native_usage_shapes():
    from sandbox_harness.model_activity import OutputProbe
    rows=[
        b'data: {"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":4}}}\n\n',
        b'data: {"type":"message_start","message":{"usage":{"input_tokens":5}}}\n\n'
        b'data: {"type":"message_delta","usage":{"output_tokens":6}}\n\n',
        b'{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":7,"completion_tokens":8}}',
    ]
    expected=[(3,4),(5,6),(7,8)]
    for raw,pair in zip(rows,expected):
        probe=OutputProbe();probe.feed(raw);probe.finish()
        assert (probe.input_tokens,probe.output_tokens)==pair


async def test_gateway_request_body_limits_declared_chunked_and_stalled_inputs():
    calls=0
    async def unread():
        nonlocal calls;calls+=1
        return {"type":"http.request","body":b"ignored","more_body":False}
    declared=Request({"type":"http","method":"POST","path":"/","query_string":b"",
        "headers":[(b"content-length",b"100")]},unread)
    with pytest.raises(Exception) as captured:
        await bounded_request_body(declared,8,.1,1)
    assert captured.value.status_code==413 and calls==0

    chunks=[b"1234",b"56789",b"unread"];position=0
    async def overflow():
        nonlocal position
        value=chunks[position];position+=1
        return {"type":"http.request","body":value,"more_body":position<len(chunks)}
    chunked=Request({"type":"http","method":"POST","path":"/","query_string":b"",
        "headers":[]},overflow)
    with pytest.raises(Exception) as captured:
        await bounded_request_body(chunked,8,.1,1)
    assert captured.value.status_code==413 and position==2

    first=True
    async def stalled():
        nonlocal first
        if first:first=False;return {"type":"http.request","body":b"x","more_body":True}
        await asyncio.Event().wait()
    slow=Request({"type":"http","method":"POST","path":"/","query_string":b"",
        "headers":[]},stalled)
    with pytest.raises(Exception) as captured:
        await bounded_request_body(slow,8,.01,.1)
    assert captured.value.status_code==408 and captured.value.detail=="request_body_timeout"


@pytest.mark.parametrize(("name","harness","suffix","field"),[
    ("codex",Harness.CODEX,"responses","max_output_tokens"),
    ("claude",Harness.CLAUDE,"messages","max_tokens"),
    ("deepseek",Harness.DEEPSEEK,"chat/completions","max_tokens"),
])
async def test_excessive_native_model_request_never_reaches_upstream(
        tmp_path,monkeypatch,name,harness,suffix,field):
    import sandbox_harness.gateway as gateway
    class Redis:
        pass
    class SecurityStore:
        def __init__(self,*args,**kwargs):pass
        async def authorize_capability(self,claims,usage="request"):
            return {"request_count":1,"public_request_count":0,"token_remaining":1}
    monkeypatch.setattr(gateway.Redis,"from_url",lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,"Store",SecurityStore)
    calls=[]
    async def forwarded(*args,**kwargs):calls.append(True)
    monkeypatch.setattr(gateway,"send_with_start_timeout",forwarded)
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/(name+".db")),
        **{"upstream_"+name:"http://upstream"})
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread",turn_id="turn",
        sandbox_id="sandbox",harness=harness,model="test-model",routes=["/v1/models/"+name],
        ttl_seconds=60,max_requests=2,max_public_requests=1,token_budget=1)
    body=json.dumps({field:100_000,"stream":True}).encode();sent=False
    async def receive():
        nonlocal sent
        if sent:return {"type":"http.request","body":b"","more_body":False}
        sent=True;return {"type":"http.request","body":body,"more_body":False}
    request=Request({"type":"http","method":"POST",
        "path":f"/v1/models/{name}/{suffix}","query_string":b"",
        "headers":[(b"authorization",("Bearer "+token).encode())]},receive)
    endpoint=next(route.endpoint for route in app.routes if getattr(route,"path",None)=="/{path:path}")
    with pytest.raises(Exception) as captured:
        await endpoint(f"v1/models/{name}/{suffix}",request)
    assert captured.value.status_code==429 and calls==[]


async def model_gateway_fixture(tmp_path,monkeypatch,stream,content_type):
    import httpx
    import sandbox_harness.gateway as gateway
    from sandbox_harness.model_activity import OutputProbe
    class Redis:pass
    class SecurityStore:
        def __init__(self,*args,**kwargs):self.finalized=[]
        async def authorize_capability(self,claims,usage="request"):
            return {"request_count":1,"public_request_count":0,"token_remaining":100}
        async def reserve_model_tokens(self,claims,request_id,requested_output):
            return {"reserved":100,"remaining":90}
        async def finalize_model_tokens(self,request_id,usage):
            self.finalized.append((request_id,usage));return {"charged":usage or 100}
    store=SecurityStore()
    monkeypatch.setattr(gateway.Redis,"from_url",lambda *args,**kwargs:Redis())
    monkeypatch.setattr(gateway,"Store",lambda *args,**kwargs:store)
    class Activity:
        def __init__(self,*args):self.request_id="idle-request";self.probe=OutputProbe();self.finished=False
        async def start(self):pass
        async def observe(self,data):self.probe.feed(data)
        async def finish(self,*args,**kwargs):
            if not self.finished:self.probe.finish();self.finished=True
    monkeypatch.setattr(gateway,"ModelActivity",Activity)
    async def respond(*args):return httpx.Response(200,headers={"content-type":content_type},stream=stream)
    monkeypatch.setattr(gateway,"send_with_start_timeout",respond)
    settings=Settings(backend="fake",data_root=tmp_path,
        database_url="sqlite+aiosqlite:///"+str(tmp_path/"idle.db"),
        upstream_codex="http://upstream").model_copy(update={"gateway_model_idle_timeout_seconds":.01})
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id="thread",turn_id="turn",
        sandbox_id="sandbox",harness=Harness.CODEX,model="gpt-5.6",routes=["/v1/models/codex"],
        ttl_seconds=60,max_requests=2,max_public_requests=1,token_budget=100)
    body=b'{"max_output_tokens":10,"stream":true}';sent=False
    async def receive():
        nonlocal sent
        if sent:return {"type":"http.request","body":b"","more_body":False}
        sent=True;return {"type":"http.request","body":body,"more_body":False}
    request=Request({"type":"http","method":"POST","path":"/v1/models/codex/responses",
        "query_string":b"","headers":[(b"authorization",("Bearer "+token).encode())]},receive)
    endpoint=next(route.endpoint for route in app.routes if getattr(route,"path",None)=="/{path:path}")
    return endpoint,request,store


@pytest.mark.parametrize("content_type",["text/event-stream","application/json"])
async def test_model_idle_before_first_byte_is_504(tmp_path,monkeypatch,content_type):
    import httpx
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            await asyncio.Event().wait()
            yield b"never"
    endpoint,request,store=await model_gateway_fixture(tmp_path,monkeypatch,Stream(),content_type)
    with pytest.raises(Exception) as captured:
        await endpoint("v1/models/codex/responses",request)
    assert captured.value.status_code==504
    assert captured.value.detail=="model_response_idle_timeout"
    assert store.finalized==[("idle-request",None)]


@pytest.mark.parametrize("content_type",["text/event-stream","application/json"])
async def test_model_idle_after_first_byte_is_detectable_failure(tmp_path,monkeypatch,content_type):
    import httpx
    from sandbox_harness.gateway import ModelUpstreamIdleTimeout
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield (b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
                if content_type=="text/event-stream" else b'{"partial":')
            await asyncio.Event().wait()
    endpoint,request,store=await model_gateway_fixture(tmp_path,monkeypatch,Stream(),content_type)
    response=await endpoint("v1/models/codex/responses",request)
    iterator=response.body_iterator.__aiter__();assert await iterator.__anext__()
    with pytest.raises(ModelUpstreamIdleTimeout):await iterator.__anext__()
    await iterator.aclose()
    assert store.finalized==[("idle-request",None)]


async def test_terminal_frame_then_idle_remains_successful(tmp_path,monkeypatch):
    import httpx
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"type":"response.output_text.delta","delta":"ok"}\n\n'
            yield b'data: {"type":"response.completed","response":{"usage":{"input_tokens":2,"output_tokens":3}}}\n\n'
            await asyncio.Event().wait()
    endpoint,request,store=await model_gateway_fixture(tmp_path,monkeypatch,Stream(),"text/event-stream")
    response=await endpoint("v1/models/codex/responses",request)
    iterator=response.body_iterator.__aiter__()
    assert b'output_text' in await iterator.__anext__()
    assert b'response.completed' in await iterator.__anext__()
    with pytest.raises(StopAsyncIteration):await iterator.__anext__()
    assert store.finalized==[("idle-request",5)]
