"""Real OpenSandbox + synthetic Harness failure and actual stream interruption.

Uses configured product images/services, an isolated SQLite DB/object directory,
and a separate sandbox metadata namespace. No real model is called by this fixture.
"""
import asyncio
import json
import os
import uuid
from pathlib import Path

import httpx
from dotenv import dotenv_values
from sandbox_harness.common.execution.turn_broker import (Broker)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (OpenSandboxBackend)
from test_support.legacy_store import (Store)

FIXTURE = """import sys,json,time
from pathlib import Path
from PIL import Image
sys.stdin.readline()
Path('/workspace/project/retained.txt').write_text('retained after failure')
Image.new('RGB',(16,16),'blue').save('/workspace/outputs/partial.png')
print(json.dumps({'type':'assistant_delta','text':'artifact created'}),flush=True)
time.sleep(.8)
print(json.dumps({'type':'result','is_error':True,'result':'unexpected status 503 Service Unavailable: Our servers are currently overloaded.'}),flush=True)
"""

class FixtureBackend(OpenSandboxBackend):
    async def create(self, harness, image, metadata, environment):
        command = ['python', '-c', FIXTURE]
        return await super().create(harness, image, metadata,
            {**environment, 'HARNESS_COMMAND_JSON': json.dumps({'claude': command})})

class TruncatedStream(httpx.AsyncByteStream):
    def __init__(self, original): self.original = original
    async def __aiter__(self):
        async for data in self.original:
            yield data.split(b'\n')[0] + b'\n'
            return
    async def aclose(self): await self.original.aclose()

class FaultTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.inner = httpx.AsyncHTTPTransport(); self.posts = 0; self.reattach = 0
    async def handle_async_request(self, request):
        response = await self.inner.handle_async_request(request)
        if request.method == 'POST' and request.url.path.endswith('/v1/turns'):
            self.posts += 1
            return httpx.Response(response.status_code, headers=response.headers,
                stream=TruncatedStream(response.stream), extensions=response.extensions)
        if '/executions/' in request.url.path and request.url.path.endswith('/events'):
            self.reattach += 1
        return response
    async def aclose(self): pass  # Shared across the short-lived RuntimeClient clients.

async def main():
    for key, value in dotenv_values(os.environ.get('SANDBOX_HARNESS_ENV_FILE','/data/wujing02/sandbox-harness-platform-parallel-ali/.data/config/platform.env')).items():
        if value is not None: os.environ[key] = value
    configured = Settings()
    root = configured.data_root / 'self-review' / ('real-' + uuid.uuid4().hex)
    root.mkdir(parents=True)
    settings = configured.model_copy(update={'data_root':root,
        'database_url':'sqlite+aiosqlite:///' + str(root/'test.db'), 'session_keepalive_seconds':0})
    store=Store(settings.database_url); await store.initialize()
    objects=ObjectStore(root/'objects'); catalog=HarnessCatalog()
    backend=FixtureBackend(settings.opensandbox_url,settings.opensandbox_api_key,
        300,'sandbox-harness-self-review',settings.gateway_url,settings.gateway_proxy_port)
    pool=SandboxPool(backend,{h:settings.image_for(h.value) for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    transport=FaultTransport(); runtime=RuntimeClient(transport=transport)
    broker=Broker(settings,store,objects,pool,runtime,catalog)
    try:
        profile=catalog.resolve('claude','claude-4.6-sonnet','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CLAUDE,model=profile.model,
            reasoning_level='standard',title='Self-review fault injection'),profile,'test-owner')
        turn=await broker.submit(thread.id,TurnCreate(message='run fixture',idempotency_key=uuid.uuid4().hex))
        for _ in range(240):
            current=await store.get_turn(turn.id)
            if current.state in {'COMPLETED','FAILED','CANCELLED'} and turn.id not in broker.tasks: break
            await asyncio.sleep(.5)
        artifacts=await store.artifacts(turn.id)
        items=await store.items_after(thread.id)
        assert current.state=='FAILED' and current.error_code=='model_service_overloaded', current
        assert transport.posts==1 and transport.reattach>=1
        assert len(artifacts)==1 and objects.get(artifacts[0].object_ref,maximum=4096).startswith(b'\x89PNG')
        assert next(item for item in items if item.type=='artifact').payload['partial'] is True
        assert (await store.get_thread(thread.id)).checkpoint_ref
        assert not await backend.list_namespace()
        result={'passed':True,'turn_id':turn.id,'state':current.state,'error_code':current.error_code,
            'real_opensandbox':True,'model':'synthetic failure fixture','initial_posts':transport.posts,
            'reattach_gets':transport.reattach,'saved_artifacts':len(artifacts),'checkpoint_saved':True,
            'sandbox_deleted':True,'evidence':str(root)}
        (root/'report.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
        print(json.dumps(result,ensure_ascii=False))
    finally:
        await broker.close();await pool.close();await store.close()
        await backend.client.aclose();await transport.inner.aclose()

if __name__=='__main__': asyncio.run(main())
