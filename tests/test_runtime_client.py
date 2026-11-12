import json

import httpx
import pytest

from sandbox_harness.domain import Harness
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import Sandbox


async def test_runtime_client_rejects_outputs_from_another_turn_batch():
    runtime=RuntimeClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,
        json={"batch_id":"old-turn","items":[{"path":"stale.txt"}]})))
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",runtime_url="http://runtime",metadata={})
    with pytest.raises(ValueError,match="does not belong"):
        await runtime.artifacts(sandbox,"current-turn")


async def test_runtime_configuration_retries_transient_proxy_startup_errors():
    requests = []

    def respond(request: httpx.Request):
        requests.append(request)
        if len(requests) < 3:
            return httpx.Response(502, text="runtime is starting")
        return httpx.Response(200, json={"configured": True})

    runtime = RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox = Sandbox(id="sandbox", harness=Harness.CLAUDE, image="image", runtime_url="http://runtime", metadata={})
    await runtime.inject(sandbox, capability_token="x" * 32, gateway_url="http://gateway:28600",
        proxy_url="http://proxy:28601", harness="claude", model="claude-opus-5-5")

    assert len(requests) == 3
    assert json.loads(requests[-1].content)["model"] == "claude-opus-5-5"


async def test_runtime_client_ignores_transport_heartbeats():
    def respond(request: httpx.Request):
        return httpx.Response(200, text='{"type":"heartbeat"}\n{"type":"result","runtime_sequence":2,"result":{"status":"completed","native_thread_id":"native","input_ids":["initial"]}}\n')

    runtime = RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox = Sandbox(id="sandbox", harness=Harness.DEEPSEEK, image="image", runtime_url="http://runtime", metadata={})
    rows = [row async for row in runtime.run(sandbox, message="hello", attachments=[], native_thread_id="",
        reasoning_effort="high")]
    assert len(rows) == 1 and rows[0].status == "completed"


async def test_broken_runtime_stream_reattaches_without_resubmitting_input():
    requests = []
    def respond(request):
        requests.append(request)
        if request.method == 'POST':
            return httpx.Response(200, text='{"kind":"text_delta","payload":{"text":"hello"},"runtime_sequence":1}\n')
        if request.url.path.endswith('/events'):
            assert request.url.params['after'] == '1'
            return httpx.Response(200, text='{"kind":"text_delta","payload":{"text":"hello"},"runtime_sequence":1}\n{"type":"result","runtime_sequence":2,"result":{"status":"completed"}}\n')
        return httpx.Response(200, json={'state':'running','last_sequence':1,'result':None})
    runtime = RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox = Sandbox(id='s', harness=Harness.CODEX, image='i', runtime_url='http://runtime', metadata={})
    rows = [row async for row in runtime.run(sandbox, message='once', attachments=[],
        native_thread_id='', reasoning_effort='high', execution_id='execution-one')]
    assert len(rows) == 2 and rows[-1].status == 'completed'
    assert sum(r.method == 'POST' for r in requests) == 1


async def test_runtime_stream_recovery_is_bounded_and_never_replays_post(monkeypatch):
    import pytest
    from sandbox_harness.runtime import RuntimeStreamInterrupted
    calls = []
    waits = []
    async def no_wait(seconds): waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.runtime.asyncio.sleep', no_wait)
    def respond(request):
        calls.append(request.method)
        return httpx.Response(200, text='') if request.method == 'POST' else httpx.Response(404)
    runtime = RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox = Sandbox(id='s', harness=Harness.CODEX, image='i', runtime_url='http://runtime', metadata={})
    with pytest.raises(RuntimeStreamInterrupted):
        _ = [row async for row in runtime.run(sandbox, message='once', attachments=[],
            native_thread_id='', reasoning_effort='high', execution_id='missing')]
    assert calls.count('POST') == 1 and calls.count('GET') == 8
    assert waits == [.5, 1, 2, 4, 4, 4, 4]
