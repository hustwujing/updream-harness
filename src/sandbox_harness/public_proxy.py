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
HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


class DestinationDenied(ValueError):
    pass


class ProxyRequestTooLarge(ValueError):
    pass


class ProxyBodyTimeout(TimeoutError):
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
    phrase={403:"Forbidden",408:"Request Timeout",413:"Payload Too Large",
        503:"Service Unavailable"}.get(status,"Bad Gateway")
    writer.write((f"HTTP/1.1 {status} {phrase}\r\n"
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


def parse_authority(value: str, default_port: int) -> tuple[str,int]:
    parsed=urlsplit("//"+value)
    if (not parsed.hostname or parsed.username or parsed.password or parsed.path
            or parsed.query or parsed.fragment):
        raise ValueError("Invalid Host header")
    try:port=parsed.port or default_port
    except ValueError as exc:raise ValueError("Invalid Host header") from exc
    return normalize_host(parsed.hostname),port


def authority_header(host: str, port: int, default_port: int) -> str:
    normalized=normalize_host(host)
    try:ip=ipaddress.ip_address(normalized)
    except ValueError:value=normalized
    else:value=f"[{normalized}]" if ip.version==6 else normalized
    return value if port==default_port else f"{value}:{port}"


@observed("public_proxy.request",new_trace=True)
async def handle_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, authorize, *, connections=None,
        policy: DestinationPolicy | None = None, policy_selector=None,
        maximum_body: int = 16*1024*1024, body_idle_timeout: float = 15,
        body_total_timeout: float = 120) -> None:
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
        headers = [];header_values={}
        content_length = 0
        for line in lines[1:]:
            if not line:
                continue
            if line.startswith((" ","\t")) or ":" not in line:
                raise ValueError("Ambiguous proxy header framing")
            name, value = line.split(":", 1)
            lowered = name.strip().lower()
            if not HEADER_NAME.fullmatch(name.strip()):raise ValueError("Invalid proxy header name")
            header_values.setdefault(lowered,[]).append(value.strip())
        for duplicate in ("host","content-length","proxy-authorization"):
            if len(header_values.get(duplicate,[]))>1:
                raise ValueError("Duplicate proxy header: "+duplicate)
        header_map={name:values[0] for name,values in header_values.items()}
        for lowered, values in header_values.items():
            value=values[0]
            if lowered in {"proxy-authorization", "proxy-connection", "connection"}:
                continue
            if lowered == "transfer-encoding":
                raise ValueError("Chunked proxy requests are not supported")
            if lowered == "content-length":
                try:content_length = int(value)
                except ValueError as exc:raise ValueError("Invalid Content-Length") from exc
                if content_length < 0 or content_length > maximum_body:
                    raise ProxyRequestTooLarge("request_too_large")
                continue
            if lowered != "host":headers.append((lowered, value))
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
            if content_length:
                raise ValueError("CONNECT request body is not allowed")
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
        if (parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password
                or parsed.fragment):
            raise ValueError("Proxy requires an absolute HTTP URL")
        port = parsed.port or 80
        canonical_host=normalize_host(parsed.hostname)
        if "host" in header_map and parse_authority(header_map["host"],80)!=(canonical_host,port):
            raise DestinationDenied("Proxy Host conflicts with absolute URL")
        address = await resolve_public(parsed.hostname, port, request_policy)
        upstream_reader, upstream_writer = await asyncio.open_connection(address, port)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        outgoing = f"{method} {path} {version}\r\n".encode("latin-1")
        outgoing += b"".join(f"{name}: {value}\r\n".encode("latin-1") for name, value in headers)
        outgoing += f"Host: {authority_header(canonical_host,port,80)}\r\n".encode("latin-1")
        outgoing += b"Connection: close\r\n"
        upstream_writer.write(outgoing + b"\r\n")
        if content_length:
            remaining=content_length
            try:
                async with asyncio.timeout(body_total_timeout):
                    while remaining:
                        try:data=await asyncio.wait_for(reader.read(min(64*1024,remaining)),
                            timeout=body_idle_timeout)
                        except TimeoutError as exc:raise ProxyBodyTimeout("request_body_timeout") from exc
                        if not data:raise asyncio.IncompleteReadError(data,remaining)
                        remaining-=len(data);upstream_writer.write(data);await upstream_writer.drain()
            except TimeoutError as exc:
                if isinstance(exc,ProxyBodyTimeout):raise
                raise ProxyBodyTimeout("request_body_timeout") from exc
        await upstream_writer.drain()
        while data := await upstream_reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ValueError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError) as exc:
        event("public_proxy.rejected",level=logging.WARNING,exc=exc,**log_context)
        if not writer.is_closing():
            code = ("request_too_large" if isinstance(exc,ProxyRequestTooLarge) else
                "request_body_timeout" if isinstance(exc,ProxyBodyTimeout) else
                "capability_authority_unavailable" if str(exc)=="capability_authority_unavailable" else
                "destination_timeout" if isinstance(exc, asyncio.TimeoutError) else
                "destination_denied" if isinstance(exc, DestinationDenied) else "destination_rejected")
            status=(413 if isinstance(exc,ProxyRequestTooLarge) else 408 if isinstance(exc,ProxyBodyTimeout)
                else 503 if code=="capability_authority_unavailable" else 403)
            await reject(writer,status,code)
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
        policy: DestinationPolicy | None = None, policy_selector=None,
        maximum_body: int = 16*1024*1024, body_idle_timeout: float = 15,
        body_total_timeout: float = 120) -> asyncio.Server:
    async def handle(reader, writer):
        await handle_proxy(reader, writer, authorize, connections=connections, policy=policy,
            policy_selector=policy_selector,maximum_body=maximum_body,
            body_idle_timeout=body_idle_timeout,body_total_timeout=body_total_timeout)

    return await asyncio.start_server(handle, host, port, limit=MAX_HEADER + 1)
