import json

import httpx
import pytest

from sandbox_harness.domain import Harness
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import Sandbox


def test_runtime_client_does_not_create_staging_until_a_transfer(tmp_path):
    staging=tmp_path/"runtime-downloads"
    RuntimeClient(staging_root=staging)
    assert not staging.exists()


async def test_runtime_client_rejects_outputs_from_another_turn_batch():
    runtime=RuntimeClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,
        json={"batch_id":"old-turn","items":[{"path":"stale.txt"}]})))
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",runtime_url="http://runtime",metadata={})
    with pytest.raises(ValueError,match="does not belong"):
        await runtime.artifacts(sandbox,"current-turn")


async def test_runtime_project_file_upload_streams_with_explicit_length():
    requests=[]
    def respond(request):requests.append(request);return httpx.Response(200,json={"ok":True})
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",
        runtime_url="http://runtime",metadata={})
    async def content():
        yield b"first-";yield b"second"
    await runtime.put_project_file_stream(sandbox,"assets/inputs/report",content(),
        size=12,mime="application/octet-stream")
    assert requests[0].headers["content-length"]=="12"
    assert requests[0].url.params["size"]=="12"
    assert requests[0].content==b"first-second"


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


async def test_runtime_configuration_installs_code_before_content_bundle(tmp_path):
    requests=[];code=b"code-bundle";content=b"content-bundle"
    code_digest=__import__('hashlib').sha256(code).hexdigest();content_digest=__import__('hashlib').sha256(content).hexdigest()
    code_dir=tmp_path/'code';content_dir=tmp_path/'content';code_dir.mkdir();content_dir.mkdir()
    (code_dir/(code_digest+'.tar')).write_bytes(code);(code_dir/(code_digest+'.sig')).write_bytes(b'code-signature')
    (content_dir/(content_digest+'.tar')).write_bytes(content);(content_dir/(content_digest+'.sig')).write_bytes(b'content-signature')
    def respond(request):
        requests.append((request.method,request.url.path))
        if request.method=='GET':return httpx.Response(200,json={'sha256':''})
        return httpx.Response(200,json={'sha256':code_digest if 'bootstrap' in request.url.path else content_digest})
    runtime=RuntimeClient(code_bundle_dir=code_dir,default_code_bundle_digest=code_digest,
        bundle_dir=content_dir,default_bundle_digest=content_digest,transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})
    await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
        proxy_url='http://proxy:28601',harness='codex',model='gpt')
    assert requests == [('GET','/bootstrap/code-bundle'),('PUT','/bootstrap/code-bundle'),
        ('GET','/v1/runtime-bundle'),('PUT','/v1/runtime-bundle'),('POST','/v1/configure')]


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
