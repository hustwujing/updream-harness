import base64

import pytest

from sandbox_harness.public_proxy import proxy_token, public_address


def test_proxy_allows_public_and_rejects_internal_addresses():
    assert public_address("1.1.1.1")
    for address in ("127.0.0.1", "169.254.169.254", "10.0.0.1", "172.31.0.1", "192.168.1.1", "::1"):
        assert not public_address(address)


def test_proxy_requires_dynamic_capability_basic_auth():
    token = "signed.turn.capability"
    encoded = base64.b64encode((token + ":").encode()).decode()
    assert proxy_token({"proxy-authorization": "Basic " + encoded}) == token
    with pytest.raises(ValueError, match="required"):
        proxy_token({})


async def test_active_public_tunnel_is_registered_and_cancelled_on_revocation(monkeypatch):
    import asyncio,time
    from collections import defaultdict
    from types import SimpleNamespace
    from sandbox_harness.public_proxy import start_proxy
    registry=defaultdict(set)
    async def echo(reader,writer):
        try:
            await reader.read()
        finally:writer.close()
    server=await asyncio.start_server(echo,'127.0.0.1',0)
    port=server.sockets[0].getsockname()[1]
    async def resolve(host,port):return '127.0.0.1'
    monkeypatch.setattr('sandbox_harness.public_proxy.resolve_public',resolve)
    async def authorize(token):return SimpleNamespace(nonce='grant',expires_at=time.time()+60)
    proxy=await start_proxy('127.0.0.1',0,authorize,connections=registry)
    reader,writer=await asyncio.open_connection('127.0.0.1',proxy.sockets[0].getsockname()[1])
    try:
        auth=base64.b64encode(b'capability:').decode()
        writer.write(f'CONNECT public.test:{port} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n\r\n'.encode());await writer.drain()
        assert b'200' in await reader.readuntil(b'\r\n\r\n')
        assert len(registry['grant'])==1
        tasks=list(registry['grant'])
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        assert await asyncio.wait_for(reader.read(),1)==b''
        assert not registry
    finally:
        writer.close();await writer.wait_closed();proxy.close();server.close()
        await proxy.wait_closed();await server.wait_closed()
