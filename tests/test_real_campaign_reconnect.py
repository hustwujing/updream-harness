import httpx
import importlib.util
from pathlib import Path
import pytest

from test_real_campaign_runner import campaign_module


def protocol_module():
    path=Path(__file__).with_name('run_real_parallel_protocol.py')
    spec=importlib.util.spec_from_file_location('real_parallel_protocol_runner',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


@pytest.mark.asyncio
async def test_campaign_reconnects_bounded_read_only_polling(monkeypatch):
    campaign=campaign_module();calls=[]
    def handler(request):
        calls.append(request.method)
        if len(calls)<3:raise httpx.ConnectError('restart window',request=request)
        return httpx.Response(200,json={'ready':True})
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,'sleep',no_sleep)
    async with campaign.ResilientAsyncClient(base_url='http://local/',
            transport=httpx.MockTransport(handler),reconnect_seconds=5) as client:
        response=await client.get('/status')
    assert response.json()=={'ready':True} and calls==['GET','GET','GET']


@pytest.mark.asyncio
async def test_campaign_never_replays_mutating_request_after_disconnect():
    campaign=campaign_module();calls=[]
    def handler(request):
        calls.append(request.method);raise httpx.ConnectError('unknown outcome',request=request)
    async with campaign.ResilientAsyncClient(base_url='http://local/',
            transport=httpx.MockTransport(handler),reconnect_seconds=5) as client:
        with pytest.raises(httpx.ConnectError):await client.post('/confirm')
    assert calls==['POST']


@pytest.mark.asyncio
async def test_parallel_protocol_reconnects_read_only_polling(monkeypatch):
    protocol=protocol_module();calls=[]
    def handler(request):
        calls.append(request.method)
        if len(calls)==1:raise httpx.ReadError('restart window',request=request)
        return httpx.Response(200,json={'state':'COMPLETED'})
    async def no_sleep(_):pass
    monkeypatch.setattr(protocol.asyncio,'sleep',no_sleep)
    async with protocol.ResilientAsyncClient(base_url='http://local/',
            transport=httpx.MockTransport(handler),reconnect_seconds=5) as client:
        response=await client.get('/turn')
    assert response.json()['state']=='COMPLETED' and calls==['GET','GET']
