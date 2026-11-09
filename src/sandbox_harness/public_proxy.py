from __future__ import annotations

import asyncio
import base64
import ipaddress
import socket
from urllib.parse import urlsplit


MAX_HEADER = 64 * 1024
ALLOWED_PORTS = {80, 443}


def public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return address.is_global and not address.is_multicast


async def resolve_public(host: str, port: int) -> str:
    if port not in ALLOWED_PORTS:
        raise ValueError("Proxy destination port is not allowed")
    rows = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses = {row[4][0] for row in rows}
    if not addresses or any(not public_address(address) for address in addresses):
        raise ValueError("Proxy destination is not public")
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


async def handle_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, authorize) -> None:
    upstream_writer = None
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
        await authorize(proxy_token(header_map))
        if method.upper() == "CONNECT":
            host, separator, port_text = target.rpartition(":")
            if not separator or not host:
                raise ValueError("Invalid CONNECT target")
            port = int(port_text)
            address = await resolve_public(host.strip("[]"), port)
            upstream_reader, upstream_writer = await asyncio.open_connection(address, port)
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            await asyncio.gather(relay(reader, upstream_writer), relay(upstream_reader, writer))
            return
        parsed = urlsplit(target)
        if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Proxy requires an absolute HTTP URL")
        port = parsed.port or 80
        address = await resolve_public(parsed.hostname, port)
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
    except (ValueError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
        if not writer.is_closing():
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
    except (ConnectionError, OSError):
        if not writer.is_closing():
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
    finally:
        if upstream_writer:
            upstream_writer.close()
        writer.close()


async def start_proxy(host: str, port: int, authorize) -> asyncio.Server:
    async def handle(reader, writer):
        await handle_proxy(reader, writer, authorize)

    return await asyncio.start_server(handle, host, port, limit=MAX_HEADER + 1)
