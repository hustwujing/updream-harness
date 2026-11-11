from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import ipaddress
import re
import socket
import time
from .observability import event,bind,operation,enrich,add_link,observed
import logging
from urllib.parse import urlsplit


MAX_HEADER = 64 * 1024
ALLOWED_PORTS = {80, 443}
DOMAIN_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


class DestinationDenied(ValueError):
    pass


def normalize_host(value: str) -> str:
    host = value.strip().strip("[]").rstrip(".").lower()
    if not host or "://" in host or any(character in host for character in "/?#@"):
        raise ValueError("Invalid proxy destination")
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("Invalid proxy destination") from exc
        if len(host) > 253 or not all(DOMAIN_LABEL.fullmatch(label) for label in host.split(".")):
            raise ValueError("Invalid proxy destination")
        return host


@dataclass(frozen=True)
class DestinationRule:
    domain: str = ""
    network: ipaddress.IPv4Network | ipaddress.IPv6Network | None = None
    wildcard: bool = False
    any_destination: bool = False

    @classmethod
    def parse(cls, raw: str) -> "DestinationRule":
        value = raw.strip().lower()
        if value == "*":
            return cls(any_destination=True)
        wildcard = value.startswith("*.")
        candidate = value[2:] if wildcard else value
        if not wildcard:
            try:
                return cls(network=ipaddress.ip_network(candidate, strict=False))
            except ValueError:
                pass
        if "/" in candidate or not candidate:
            raise ValueError(f"Invalid public proxy destination rule: {raw!r}")
        domain = normalize_host(candidate)
        try:
            ipaddress.ip_address(domain)
        except ValueError:
            return cls(domain=domain, wildcard=wildcard)
        raise ValueError(f"Invalid public proxy destination rule: {raw!r}")

    def matches_host(self, host: str) -> bool:
        if self.any_destination:
            return True
        if not self.domain:
            return False
        return host.endswith("." + self.domain) if self.wildcard else host == self.domain

    def matches_address(self, address: str) -> bool:
        return self.any_destination or bool(self.network and ipaddress.ip_address(address) in self.network)


class DestinationPolicy:
    def __init__(self, allowlist=(), denylist=()):
        self.allowlist = tuple(DestinationRule.parse(value) for value in allowlist)
        self.denylist = tuple(DestinationRule.parse(value) for value in denylist)

    def check(self, host: str, addresses: set[str]) -> None:
        if any(rule.matches_host(host) for rule in self.denylist):
            raise DestinationDenied("Proxy destination is denied")
        if any(rule.matches_address(address) for rule in self.denylist for address in addresses):
            raise DestinationDenied("Proxy destination address is denied")
        if not self.allowlist:
            return
        allowed_by_name = any(rule.matches_host(host) for rule in self.allowlist)
        allowed_by_address = bool(addresses) and all(
            any(rule.matches_address(address) for rule in self.allowlist) for address in addresses
        )
        if not allowed_by_name and not allowed_by_address:
            raise DestinationDenied("Proxy destination is outside the allowlist")


async def reject(writer: asyncio.StreamWriter, status: int, code: str) -> None:
    body = (code + "\n").encode()
    writer.write((f"HTTP/1.1 {status} {'Forbidden' if status == 403 else 'Bad Gateway'}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\n"
        f"X-Sandbox-Proxy-Error: {code}\r\nConnection: close\r\n\r\n").encode() + body)
    await writer.drain()


def public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return address.is_global and not address.is_multicast


async def resolve_public(host: str, port: int, policy: DestinationPolicy | None = None) -> str:
    if port not in ALLOWED_PORTS:
        raise ValueError("Proxy destination port is not allowed")
    host = normalize_host(host)
    rows = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses = {row[4][0] for row in rows}
    if not addresses or any(not public_address(address) for address in addresses):
        raise ValueError("Proxy destination is not public")
    (policy or DestinationPolicy()).check(host, addresses)
    return sorted(addresses)[0]


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


def proxy_token(headers: dict[str, str]) -> str:
    authorization = headers.get("proxy-authorization", "")
    if not authorization.lower().startswith("basic "):
        raise ValueError("Proxy capability is required")
    try:
        user_password = base64.b64decode(authorization[6:].strip(), validate=True).decode()
        token, separator, password = user_password.partition(":")
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Invalid proxy capability") from exc
    if not separator or password or not token:
        raise ValueError("Invalid proxy capability")
    return token


@observed("public_proxy.request",new_trace=True)
async def handle_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, authorize, *, connections=None,
        policy: DestinationPolicy | None = None, policy_selector=None) -> None:
    upstream_writer = None
    registered_nonce = None
    expiry = None
    started=time.monotonic();log_context={}
    try:
        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
        if len(header) > MAX_HEADER:
            raise ValueError("Proxy header too large")
        lines = header.decode("latin-1").split("\r\n")
        method, target, version = lines[0].split(" ", 2)
        if version not in ("HTTP/1.0", "HTTP/1.1"):
            raise ValueError("Unsupported HTTP version")
        headers = []
        header_map = {}
        content_length = 0
        for line in lines[1:]:
            if not line:
                continue
            name, value = line.split(":", 1)
            lowered = name.strip().lower()
            header_map[lowered] = value.strip()
            if lowered in {"proxy-authorization", "proxy-connection", "connection"}:
                continue
            if lowered == "transfer-encoding":
                raise ValueError("Chunked proxy requests are not supported")
            if lowered == "content-length":
                content_length = int(value.strip())
                if content_length < 0 or content_length > 256 * 1024 * 1024:
                    raise ValueError("Proxy request body is too large")
            headers.append((name.strip(), value.strip()))
        claims = await authorize(proxy_token(header_map))
        request_policy = policy_selector(claims) if policy_selector else policy
        log_context={key:getattr(claims,key,"") for key in ("turn_id","thread_id","sandbox_id")}
        enrich(**log_context);add_link(getattr(claims,"traceparent",""))
        event("public_proxy.authorized",**log_context)
        if connections is not None:
            registered_nonce = claims.nonce
            connections[registered_nonce].add(asyncio.current_task())
            expiry = asyncio.get_running_loop().call_later(
                max(0, claims.expires_at - time.time()), asyncio.current_task().cancel)

        if method.upper() == "CONNECT":
            host, separator, port_text = target.rpartition(":")
            if not separator or not host:
                raise ValueError("Invalid CONNECT target")
            port = int(port_text)
            address = await resolve_public(host, port, request_policy)
            upstream_reader, upstream_writer = await asyncio.open_connection(address, port)
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            await asyncio.gather(relay(reader, upstream_writer), relay(upstream_reader, writer))
            return
        parsed = urlsplit(target)
        if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Proxy requires an absolute HTTP URL")
        port = parsed.port or 80
        address = await resolve_public(parsed.hostname, port, request_policy)
        upstream_reader, upstream_writer = await asyncio.open_connection(address, port)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        outgoing = f"{method} {path} {version}\r\n".encode("latin-1")
        outgoing += b"".join(f"{name}: {value}\r\n".encode("latin-1") for name, value in headers)
        outgoing += b"Connection: close\r\n"
        upstream_writer.write(outgoing + b"\r\n")
        if content_length:
            upstream_writer.write(await reader.readexactly(content_length))
        await upstream_writer.drain()
        while data := await upstream_reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ValueError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError) as exc:
        event("public_proxy.rejected",level=logging.WARNING,exc=exc,**log_context)
        if not writer.is_closing():
            code = ("destination_timeout" if isinstance(exc, asyncio.TimeoutError) else
                "destination_denied" if isinstance(exc, DestinationDenied) else "destination_rejected")
            await reject(writer,403,code)
    except (ConnectionError, OSError) as exc:
        event("public_proxy.failed",level=logging.ERROR,exc=exc,**log_context)
        if not writer.is_closing():
            await reject(writer,502,"destination_unreachable")
    finally:
        event("public_proxy.closed",duration_ms=round((time.monotonic()-started)*1000),**log_context)
        if expiry:
            expiry.cancel()
        if registered_nonce:
            tasks = connections.get(registered_nonce)
            if tasks is not None:
                tasks.discard(asyncio.current_task())
                if not tasks:
                    connections.pop(registered_nonce, None)
        if upstream_writer:
            upstream_writer.close()
        writer.close()


async def start_proxy(host: str, port: int, authorize, *, connections=None,
        policy: DestinationPolicy | None = None, policy_selector=None) -> asyncio.Server:
    async def handle(reader, writer):
        await handle_proxy(reader, writer, authorize, connections=connections, policy=policy,
            policy_selector=policy_selector)

    return await asyncio.start_server(handle, host, port, limit=MAX_HEADER + 1)
