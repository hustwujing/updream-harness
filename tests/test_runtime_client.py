import json

import httpx
import pytest

from sandbox_harness.domain import Harness
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import Sandbox
from sandbox_harness.sandbox_contracts import SandboxEndpoint


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


async def test_runtime_auth_failure_forces_endpoint_refresh_on_next_operation():
    class Backend:
        def __init__(self):self.refresh=[]
        async def endpoint(self,sandbox,port,*,force_refresh=False):
            self.refresh.append(force_refresh)
            return SandboxEndpoint(f"http://runtime-{len(self.refresh)}")
    calls=0
    def respond(request):
        nonlocal calls
        calls+=1
        return (httpx.Response(401) if calls==1 else
            httpx.Response(200,json={"state":"running","last_sequence":0,"result":None}))
    backend=Backend();runtime=RuntimeClient(backend=backend,transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",
        runtime_url="http://runtime",metadata={})

    with pytest.raises(httpx.HTTPStatusError):
        await runtime.execution_status(sandbox,"execution")
    assert await runtime.execution_status(sandbox,"execution")=={
        "state":"running","last_sequence":0,"result":None}
    assert backend.refresh==[False,True]


async def test_runtime_terminal_protocol_uses_runtime_endpoint():
    class Backend:
        def __init__(self): self.ports=[]
        async def endpoint(self,sandbox,port,*,force_refresh=False):
            self.ports.append(port)
            return SandboxEndpoint("https://runtime.example/base",
                {"OPEN-SANDBOX-API-KEY":"secret"})
    requests=[]
    def respond(request):
        requests.append(request)
        if request.method=="POST":
            return httpx.Response(200,json={"session_id":"pty-one"})
        if request.method=="GET":
            return httpx.Response(200,json={"session_id":"pty-one","running":True})
        return httpx.Response(200,json={"deleted":True})
    backend=Backend();runtime=RuntimeClient(backend=backend,
        transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",
        runtime_url="",metadata={})

    assert (await runtime.create_terminal(sandbox,cwd="/workspace/project",
        command="/bin/bash"))["session_id"]=="pty-one"
    assert (await runtime.terminal_status(sandbox,"pty-one"))["running"] is True
    assert await runtime.delete_terminal(sandbox,"pty-one") is True
    ws_url,headers=await runtime.terminal_endpoint(sandbox,"pty-one",since=17)

    assert backend.ports==[8080,8080,8080,8080]
    assert [request.url.path for request in requests]==[
        "/base/v1/pty","/base/v1/pty/pty-one","/base/v1/pty/pty-one"]
    assert ws_url=="wss://runtime.example/base/v1/pty/pty-one/ws?since=17"
    assert headers["OPEN-SANDBOX-API-KEY"]=="secret"


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


async def test_child_runtime_configuration_uses_logical_protocol_identity():
    requests=[]
    def respond(request):requests.append(request);return httpx.Response(200,json={'configured':True})
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CLAUDE,image='image',runtime_url='http://runtime',
        metadata={},thread_id='child-job-'+'c'*32,turn_id='c'*32)
    await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
        proxy_url='http://proxy:28601',harness='claude',model='claude-opus-5-5',
        child_mode=True,logical_thread_id='a'*32,logical_turn_id='b'*32,
        logical_job_id='c'*32)
    payload=json.loads(requests[-1].content)
    assert payload['thread_id']=='a'*32 and payload['turn_id']=='b'*32
    assert payload['job_id']=='c'*32


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


async def test_runtime_configuration_retries_transient_code_bundle_upload(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.runtime.asyncio.sleep',no_wait)
    code=b"code-bundle";digest=__import__('hashlib').sha256(code).hexdigest()
    root=tmp_path/'code';root.mkdir();(root/(digest+'.tar')).write_bytes(code)
    (root/(digest+'.sig')).write_bytes(b'signature')
    requests=[];puts=0
    def respond(request):
        nonlocal puts
        requests.append((request.method,request.url.path))
        if request.method=='GET':return httpx.Response(200,json={'sha256':''})
        if request.url.path=='/bootstrap/code-bundle':
            puts+=1
            if puts==1:return httpx.Response(503,text='runtime is starting')
            return httpx.Response(200,json={'sha256':digest})
        return httpx.Response(200,json={'configured':True})
    runtime=RuntimeClient(code_bundle_dir=root,default_code_bundle_digest=digest,
        transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})

    await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
        proxy_url='http://proxy:28601',harness='codex',model='gpt')

    assert puts==2 and waits==[.25]
    assert requests[-1]==('POST','/v1/configure')


async def test_runtime_configuration_does_not_retry_deterministic_bundle_rejection(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.runtime.asyncio.sleep',no_wait)
    code=b"code-bundle";digest=__import__('hashlib').sha256(code).hexdigest()
    root=tmp_path/'code';root.mkdir();(root/(digest+'.tar')).write_bytes(code)
    (root/(digest+'.sig')).write_bytes(b'signature')
    puts=0
    def respond(request):
        nonlocal puts
        if request.method=='GET':return httpx.Response(200,json={'sha256':''})
        puts+=1;return httpx.Response(400,text='signature invalid')
    runtime=RuntimeClient(code_bundle_dir=root,default_code_bundle_digest=digest,
        transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})

    with pytest.raises(httpx.HTTPStatusError):
        await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
            proxy_url='http://proxy:28601',harness='codex',model='gpt')

    assert puts==1 and waits==[]


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
