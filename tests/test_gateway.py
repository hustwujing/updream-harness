import asyncio
import pytest
from fastapi import HTTPException

from sandbox_harness.gateway import bytes_with_idle_timeout, send_with_start_timeout, upstream_suffix


def test_gateway_normalizes_native_client_v1_paths_against_versioned_base_url():
    prefix = "/v1/models/claude"
    assert upstream_suffix(prefix + "/v1/messages", prefix) == "/messages"
    assert upstream_suffix(prefix + "/messages", prefix) == "/messages"


async def test_gateway_fails_model_request_when_upstream_never_starts_response():
    class Client:
        async def send(self, request, stream):
            await asyncio.sleep(1)
    with pytest.raises(HTTPException) as error:
        await send_with_start_timeout(Client(), object(), .01)
    assert error.value.status_code == 504
    assert "did not start" in error.value.detail


async def test_gateway_closes_stream_after_idle_timeout():
    class Response:
        async def aiter_bytes(self):
            yield b"first"
            await asyncio.sleep(1)
            yield b"late"
    rows = []
    async for chunk in bytes_with_idle_timeout(Response(), .01):
        rows.append(chunk)
    assert rows == [b"first"]


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
    observations=[]
    class Activity:
        def __init__(self,*args):self.probe=OutputProbe();self.finished=False;observations.append(self)
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
