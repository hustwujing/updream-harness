import asyncio
import json

import httpx
import pytest

from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (Sandbox)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxEndpoint)
from sandbox_harness.common.storage.objects import (ObjectStore, object_chunks)


def test_runtime_client_does_not_create_staging_until_a_transfer(tmp_path):
    staging=tmp_path/"runtime-downloads"
    RuntimeClient(staging_root=staging)
    assert not staging.exists()


async def test_runtime_client_acknowledges_only_the_committed_execution_sequence():
    requests=[]
    async def respond(request):
        requests.append((request.method,request.url.path,json.loads(await request.aread())))
        return httpx.Response(200,json={'committed_sequence':17})
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})
    await runtime.acknowledge_execution(sandbox,'execution',17)
    await runtime.acknowledge_execution(sandbox,'execution',0)
    assert requests==[('POST','/v1/executions/execution/ack',{'committed_sequence':17})]


async def test_checkpoint_restore_replays_the_complete_object_after_read_error(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    store=ObjectStore(tmp_path/'objects');payload=b'a'*(1024*1024)+b'second-chunk'
    reference,_=store.put(payload);requests=[]
    async def respond(request):
        body=await request.aread();requests.append(body)
        if len(requests)==1:raise httpx.ReadError('checkpoint reply lost',request=request)
        return httpx.Response(200,json={'restored':True})
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})

    await runtime.restore(sandbox,object_chunks(store,reference,maximum=len(payload)),
        size=len(payload))

    assert requests==[payload,payload]
    assert waits==[1]


async def test_checkpoint_restore_refreshes_provider_endpoint_after_retry_exhaustion(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    store=ObjectStore(tmp_path/'objects');payload=b'checkpoint-'*200000
    reference,_=store.put(payload);requests=[]
    class Backend:
        def __init__(self):self.calls=[]
        async def endpoint(self,sandbox,port,*,force_refresh=False):
            self.calls.append(force_refresh)
            return SandboxEndpoint(url=('http://runtime-two' if force_refresh else 'http://runtime-one'))
    async def respond(request):
        body=await request.aread();requests.append((request.url.host,body))
        if request.url.host=='runtime-one':raise httpx.ReadError('cold endpoint reset',request=request)
        return httpx.Response(200,json={'restored':True})
    backend=Backend();runtime=RuntimeClient(backend=backend,transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='',metadata={})
    await runtime.restore(sandbox,object_chunks(store,reference,maximum=len(payload)),size=len(payload))
    assert backend.calls==[False,True]
    assert [host for host,_ in requests]==['runtime-one','runtime-two']
    assert all(body==payload for _,body in requests)
    assert waits==[1]


async def test_checkpoint_restore_does_not_retry_deterministic_rejection(tmp_path):
    store=ObjectStore(tmp_path/'objects');reference,_=store.put(b'invalid-checkpoint');calls=[]
    async def respond(request):
        calls.append(await request.aread())
        return httpx.Response(400,json={'detail':'checkpoint_archive_invalid'})
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})

    with pytest.raises(httpx.HTTPStatusError):
        await runtime.restore(sandbox,object_chunks(store,reference,maximum=1024),
            size=len(b'invalid-checkpoint'))

    assert calls==[b'invalid-checkpoint']


async def test_checkpoint_restore_uses_configured_bounded_backoff(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    store=ObjectStore(tmp_path/'objects');reference,_=store.put(b'checkpoint');calls=[]
    async def respond(request):
        calls.append(await request.aread())
        raise httpx.ReadError('controlled disconnect',request=request)
    runtime=RuntimeClient(transport=httpx.MockTransport(respond),request_retry_attempts=3,
        request_retry_initial_delay_seconds=2,request_retry_backoff_multiplier=3,
        request_retry_max_delay_seconds=5)
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})

    with pytest.raises(httpx.ReadError):
        await runtime.restore(sandbox,object_chunks(store,reference,maximum=1024),size=10)

    assert calls==[b'checkpoint']*3
    assert waits==[2,5]


async def test_runtime_idempotent_preparation_calls_retry_transport_failures(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    store=ObjectStore(tmp_path/'objects');payload=b'project-data';reference,_=store.put(payload)
    calls={}
    async def respond(request):
        body=await request.aread();key=(request.method,request.url.path)
        calls.setdefault(key,[]).append(body)
        if len(calls[key])==1:raise httpx.ReadError('controlled disconnect',request=request)
        values={
            ('PUT','/v1/attachments/file'):{'bytes':len(body)},
            ('PUT','/v1/project-files/file'):{'bytes':len(body),'sha256':'digest'},
            ('POST','/v1/project-files/reset'):{'reset':True},
            ('GET','/v1/project-files/manifest'):{'items':[]},
            ('POST','/v1/project-files/preserve'):{'items':[]},
        }
        return httpx.Response(200,json=values[key])
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',
        runtime_url='http://runtime',metadata={})

    await runtime.put_attachment(sandbox,'input.bin',b'attachment','application/octet-stream')
    result=await runtime.put_project_file_stream(sandbox,'assets/input.bin',
        object_chunks(store,reference,maximum=len(payload)),size=len(payload),
        mime='application/octet-stream')
    await runtime.reset_project_files(sandbox)
    assert await runtime.project_file_manifest(sandbox)=={'items':[]}
    assert await runtime.preserve_project_files(sandbox,['assets/input.bin'],'work/saved')=={
        'items':[]}

    assert result=={'bytes':len(payload),'sha256':'digest'}
    assert calls[('PUT','/v1/attachments/file')]==[b'attachment',b'attachment']
    assert calls[('PUT','/v1/project-files/file')]==[payload,payload]
    assert all(len(values)==2 for values in calls.values())
    assert waits==[1]*len(calls)


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


async def test_project_file_upload_recovers_lost_reply_with_identical_bytes(monkeypatch):
    requests=[]
    async def respond(request):
        body=await request.aread()
        requests.append((request.method,request.url.path,dict(request.url.params),body))
        if len(requests)==1:raise httpx.ReadError('reply lost after upload',request=request)
        return httpx.Response(200,json={'ok':True})
    original=httpx.AsyncClient
    transport=httpx.MockTransport(respond)
    # Exercise the current byte-upload path even before it honors the test transport.
    def client(**kwargs):
        kwargs['transport']=transport
        return original(**kwargs)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.httpx.AsyncClient',client)
    runtime=RuntimeClient(transport=transport)
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    payload=b'immutable-project-file'*1024
    await runtime.put_project_file(sandbox,'artifacts/video.bin',payload,'application/octet-stream')
    assert len(requests)==2
    assert requests[0]==requests[1]==('PUT','/v1/project-files/file',
        {'path':'artifacts/video.bin','size':str(len(payload))},payload)


async def test_project_file_upload_does_not_retry_deterministic_rejection(monkeypatch):
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(422,json={'detail':'invalid_project_path'})
    original=httpx.AsyncClient
    transport=httpx.MockTransport(respond)
    def client(**kwargs):
        kwargs['transport']=transport
        return original(**kwargs)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.httpx.AsyncClient',client)
    runtime=RuntimeClient(transport=transport)
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    with pytest.raises(httpx.HTTPStatusError):
        await runtime.put_project_file(sandbox,'artifacts/rejected.bin',b'content','application/octet-stream')
    assert len(requests)==1


async def test_project_file_upload_retry_exhaustion_keeps_identical_body():
    requests=[]
    def respond(request):
        requests.append(request)
        raise httpx.ReadError('controlled lost reply',request=request)
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    with pytest.raises(httpx.ReadError):
        await runtime.put_project_file(sandbox,'artifacts/file.bin',b'content','application/octet-stream')
    assert len(requests)==4
    assert all(r.method=='PUT' and r.url.path=='/v1/project-files/file' and
        r.content==b'content' for r in requests)


async def test_project_file_upload_cancellation_stops_retries():
    entered=asyncio.Event();requests=[]
    def respond(request):
        requests.append(request);entered.set()
        raise httpx.ReadError('controlled lost reply',request=request)
    runtime=RuntimeClient(transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='image',runtime_url='http://runtime',metadata={})
    task=asyncio.create_task(runtime.put_project_file(
        sandbox,'artifacts/file.bin',b'content','application/octet-stream'))
    await asyncio.wait_for(entered.wait(),2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert len(requests)==1


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
        proxy_url="http://proxy:28601", harness="claude", model="claude-opus-5-5",
        telemetry_metrics_enabled=True,stderr_log_max_bytes=-1)

    assert len(requests) == 3
    payload=json.loads(requests[-1].content)
    assert payload["model"] == "claude-opus-5-5"
    assert payload["telemetry_metrics_enabled"] is True
    assert payload["stderr_log_max_bytes"]==-1


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


@pytest.mark.parametrize('inspection',[False,True])
async def test_runtime_configuration_installs_code_before_content_bundle(tmp_path,inspection):
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
    if inspection:
        await runtime.prepare(sandbox)
    else:
        await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
            proxy_url='http://proxy:28601',harness='codex',model='gpt')
    assert requests == [('GET','/bootstrap/code-bundle'),('PUT','/bootstrap/code-bundle'),
        ('GET','/v1/runtime-bundle'),('PUT','/v1/runtime-bundle')]+([] if inspection else [('POST','/v1/configure')])


@pytest.mark.parametrize('failure',['transport','proxy'])
async def test_runtime_bundle_status_reads_retry_before_configuration(tmp_path,monkeypatch,failure):
    import hashlib
    from collections import Counter
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    digests={}
    for name in ['code','content']:
        directory=tmp_path/name;directory.mkdir();data=name.encode()
        digest=hashlib.sha256(data).hexdigest();digests[name]=digest
        (directory/(digest+'.tar')).write_bytes(data)
        (directory/(digest+'.sig')).write_bytes(b'signature')
    calls=Counter()
    def respond(request):
        key=(request.method,request.url.path);calls[key]+=1
        if request.method=='GET':
            if calls[key]==1:
                if failure=='transport':raise httpx.ConnectError('startup connection unavailable',request=request)
                return httpx.Response(503)
            return httpx.Response(200,json={'sha256':''})
        return httpx.Response(200,json={'configured':True})
    runtime=RuntimeClient(code_bundle_dir=tmp_path/'code',default_code_bundle_digest=digests['code'],
        bundle_dir=tmp_path/'content',default_bundle_digest=digests['content'],transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})
    await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway',
        proxy_url='http://proxy',harness='codex',model='gpt')
    assert calls=={('GET','/bootstrap/code-bundle'):2,('PUT','/bootstrap/code-bundle'):1,
        ('GET','/v1/runtime-bundle'):2,('PUT','/v1/runtime-bundle'):1,('POST','/v1/configure'):1}
    assert waits==[1,1]


@pytest.mark.parametrize('status,expected_calls',[(403,1),(503,4)])
async def test_runtime_bundle_status_retry_is_bounded_and_rejects_permanent_errors(tmp_path,monkeypatch,status,expected_calls):
    import hashlib
    waits=[];calls=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    data=b'code';digest=hashlib.sha256(data).hexdigest()
    (tmp_path/(digest+'.tar')).write_bytes(data);(tmp_path/(digest+'.sig')).write_bytes(b'signature')
    def respond(request):calls.append((request.method,request.url.path));return httpx.Response(status)
    runtime=RuntimeClient(code_bundle_dir=tmp_path,default_code_bundle_digest=digest,
        transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})
    with pytest.raises(httpx.HTTPStatusError):
        await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway',
            proxy_url='http://proxy',harness='codex',model='gpt')
    assert calls==[('GET','/bootstrap/code-bundle')]*expected_calls
    assert len(waits)==expected_calls-1


async def test_runtime_configuration_retries_transient_code_bundle_upload(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
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

    assert puts==2 and waits==[1]
    assert requests[-1]==('POST','/v1/configure')


async def test_runtime_configure_uses_bounded_configured_backoff(monkeypatch):
    waits=[];calls=0
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    def respond(request):
        nonlocal calls;calls+=1
        return (httpx.Response(503,text='runtime is starting') if calls<3 else
            httpx.Response(200,json={'configured':True}))
    runtime=RuntimeClient(request_retry_attempts=3,request_retry_initial_delay_seconds=2,
        request_retry_backoff_multiplier=3,request_retry_max_delay_seconds=5,
        transport=httpx.MockTransport(respond))
    sandbox=Sandbox(id='s',harness=Harness.CODEX,image='i',runtime_url='http://runtime',metadata={})

    await runtime.inject(sandbox,capability_token='x'*32,gateway_url='http://gateway:28600',
        proxy_url='http://proxy:28601',harness='codex',model='gpt')

    assert calls==3 and waits==[2,5]


async def test_runtime_configuration_does_not_retry_deterministic_bundle_rejection(tmp_path,monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
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
    from sandbox_harness.common.sandbox.runtime import (RuntimeStreamInterrupted)
    calls = []
    waits = []
    async def no_wait(seconds): waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep', no_wait)
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
