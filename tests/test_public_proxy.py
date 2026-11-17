import base64

import pytest

from sandbox_harness.public_proxy import DestinationDenied, DestinationPolicy, proxy_token, public_address, reject
from sandbox_harness.public_proxy import authority_header, parse_authority


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


def test_destination_policy_supports_exact_wildcard_ip_and_cidr_rules():
    policy = DestinationPolicy(
        ["allowed.example", "*.assets.example", "1.1.1.0/24"],
        ["blocked.allowed.example", "*.private.assets.example", "1.1.1.9"],
    )
    policy.check("allowed.example", {"8.8.8.8"})
    policy.check("cdn.assets.example", {"8.8.4.4"})
    policy.check("resolver.example", {"1.1.1.8"})
    with pytest.raises(DestinationDenied):
        policy.check("other.example", {"8.8.8.8"})
    with pytest.raises(DestinationDenied):
        policy.check("assets.example", {"8.8.8.8"})
    with pytest.raises(DestinationDenied):
        policy.check("blocked.allowed.example", {"8.8.8.8"})
    with pytest.raises(DestinationDenied):
        policy.check("cdn.private.assets.example", {"8.8.8.8"})
    with pytest.raises(DestinationDenied):
        policy.check("allowed.example", {"1.1.1.9"})


def test_destination_policy_normalizes_domains_and_rejects_invalid_rules():
    DestinationPolicy(["EXAMPLE.COM."], []).check("example.com", {"8.8.8.8"})
    DestinationPolicy([], ["*"])
    for value in ("https://example.com", "example.com/path", "*.127.0.0.1", "10.0.0.0/not-a-prefix"):
        with pytest.raises(ValueError, match="Invalid"):
            DestinationPolicy([value], [])


def test_proxy_authority_normalizes_idna_ip_and_default_ports():
    assert parse_authority("EXAMPLE.COM:80",80)==("example.com",80)
    assert parse_authority("[2001:4860:4860::8888]:443",80)==("2001:4860:4860::8888",443)
    assert parse_authority("例子.测试",80)[0].startswith("xn--")
    assert authority_header("1.1.1.1",80,80)=="1.1.1.1"
    assert authority_header("2001:4860:4860::8888",443,80)=="[2001:4860:4860::8888]:443"


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
    async def resolve(host,port,policy=None):return '127.0.0.1'
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


async def test_proxy_returns_distinct_policy_denied_code(monkeypatch):
    import asyncio,time
    from types import SimpleNamespace
    from sandbox_harness.public_proxy import start_proxy
    async def denied(host,port,policy=None):
        raise DestinationDenied("denied")
    monkeypatch.setattr('sandbox_harness.public_proxy.resolve_public',denied)
    async def authorize(token):return SimpleNamespace(nonce='grant',expires_at=time.time()+60)
    proxy=await start_proxy('127.0.0.1',0,authorize,policy=DestinationPolicy([],['blocked.example']))
    reader,writer=await asyncio.open_connection('127.0.0.1',proxy.sockets[0].getsockname()[1])
    try:
        auth=base64.b64encode(b'capability:').decode()
        writer.write(f'CONNECT blocked.example:443 HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n\r\n'.encode())
        await writer.drain();response=await reader.read()
        assert b'HTTP/1.1 403 Forbidden' in response
        assert b'X-Sandbox-Proxy-Error: destination_denied' in response
    finally:
        writer.close();await writer.wait_closed();proxy.close();await proxy.wait_closed()


async def test_proxy_selects_destination_policy_from_capability(monkeypatch):
    import asyncio,time
    from types import SimpleNamespace
    from sandbox_harness.public_proxy import start_proxy
    restricted=DestinationPolicy(['bilibili.com','*.bilibili.com'],[])
    public=DestinationPolicy([],[])
    async def resolve(host,port,policy=None):
        policy.check(host,{'8.8.8.8'})
        return '127.0.0.1'
    monkeypatch.setattr('sandbox_harness.public_proxy.resolve_public',resolve)
    async def authorize(token):
        return SimpleNamespace(nonce=token,expires_at=time.time()+60,
            network_access='public' if token == 'public' else 'restricted')
    async def upstream(reader,writer):
        try:await reader.read()
        finally:writer.close()
    server=await asyncio.start_server(upstream,'127.0.0.1',0)
    upstream_port=server.sockets[0].getsockname()[1]
    proxy=await start_proxy('127.0.0.1',0,authorize,policy=restricted,
        policy_selector=lambda claims: public if claims.network_access == 'public' else restricted)
    port=proxy.sockets[0].getsockname()[1]
    async def request(token):
        reader,writer=await asyncio.open_connection('127.0.0.1',port)
        auth=base64.b64encode((token+':').encode()).decode()
        writer.write(f'CONNECT example.com:{upstream_port} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n\r\n'.encode())
        await writer.drain();response=await reader.readuntil(b'\r\n\r\n')
        writer.close();await writer.wait_closed()
        return response
    try:
        assert b'403 Forbidden' in await request('restricted')
        assert b'200 Connection Established' in await request('public')
    finally:
        proxy.close();server.close();await proxy.wait_closed();await server.wait_closed()


async def test_http_proxy_canonicalizes_authority_and_rejects_ambiguous_framing(monkeypatch):
    import asyncio,time
    from types import SimpleNamespace
    from sandbox_harness.public_proxy import start_proxy
    captured=[]
    async def upstream(reader,writer):
        try:
            captured.append(await reader.readuntil(b'\r\n\r\n'))
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK')
            await writer.drain()
        finally:writer.close()
    server=await asyncio.start_server(upstream,'127.0.0.1',0)
    upstream_port=server.sockets[0].getsockname()[1]
    async def resolve(host,port,policy=None):return '127.0.0.1'
    monkeypatch.setattr('sandbox_harness.public_proxy.resolve_public',resolve)
    async def authorize(token):return SimpleNamespace(nonce='grant',expires_at=time.time()+60)
    proxy=await start_proxy('127.0.0.1',0,authorize,maximum_body=4,
        body_idle_timeout=.02,body_total_timeout=.1)
    proxy_port=proxy.sockets[0].getsockname()[1]
    auth=base64.b64encode(b'capability:').decode()
    async def exchange(raw,read_timeout=1):
        reader,writer=await asyncio.open_connection('127.0.0.1',proxy_port)
        writer.write(raw);await writer.drain()
        try:return await asyncio.wait_for(reader.read(),read_timeout)
        finally:writer.close();await writer.wait_closed()
    target=f'http://allowed.example:{upstream_port}/resource'
    try:
        mismatch=await exchange((f'GET {target} HTTP/1.1\r\nHost: denied.example\r\n'
            f'Proxy-Authorization: Basic {auth}\r\n\r\n').encode())
        assert b'destination_denied' in mismatch and captured==[]
        duplicate=await exchange((f'GET {target} HTTP/1.1\r\nHost: allowed.example:{upstream_port}\r\n'
            f'Host: allowed.example:{upstream_port}\r\nProxy-Authorization: Basic {auth}\r\n\r\n').encode())
        assert b'destination_rejected' in duplicate and captured==[]
        response=await exchange((f'GET {target} HTTP/1.1\r\n'
            f'Proxy-Authorization: Basic {auth}\r\n\r\n').encode())
        assert b'200 OK' in response
        assert f'Host: allowed.example:{upstream_port}\r\n'.encode() in captured[0]
        too_large=await exchange((f'POST {target} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n'
            'Content-Length: 5\r\n\r\n').encode())
        assert b'413 Payload Too Large' in too_large and b'request_too_large' in too_large
        conflicting=await exchange((f'POST {target} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n'
            'Content-Length: 1\r\nContent-Length: 2\r\n\r\n').encode())
        assert b'destination_rejected' in conflicting
        stalled=await exchange((f'POST {target} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n'
            'Content-Length: 3\r\n\r\nx').encode())
        assert b'408 Request Timeout' in stalled and b'request_body_timeout' in stalled
    finally:
        proxy.close();server.close();await proxy.wait_closed();await server.wait_closed()


async def test_resolver_checks_all_answers_against_address_policy(monkeypatch):
    import asyncio,socket
    from sandbox_harness.public_proxy import resolve_public
    class Loop:
        async def getaddrinfo(self,host,port,type):
            assert host == 'allowed.example' and port == 443 and type == socket.SOCK_STREAM
            return [(socket.AF_INET,socket.SOCK_STREAM,6,'',('8.8.8.8',443)),
                (socket.AF_INET,socket.SOCK_STREAM,6,'',('1.1.1.9',443))]
    monkeypatch.setattr(asyncio,'get_running_loop',lambda:Loop())
    assert await resolve_public('ALLOWED.EXAMPLE.',443,DestinationPolicy(['allowed.example'],[])) == '1.1.1.9'
    with pytest.raises(DestinationDenied):
        await resolve_public('allowed.example',443,DestinationPolicy(['allowed.example'],['1.1.1.0/24']))


async def test_proxy_error_response_exposes_stable_actionable_code():
    import asyncio
    async def handler(reader, writer):
        await reject(writer, 502, "destination_unreachable")
        writer.close()
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    try:
        response = await reader.read()
        assert b"HTTP/1.1 502 Bad Gateway" in response
        assert b"X-Sandbox-Proxy-Error: destination_unreachable" in response
        assert response.endswith(b"destination_unreachable\n")
    finally:
        writer.close();await writer.wait_closed();server.close();await server.wait_closed()
