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
