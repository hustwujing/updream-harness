import asyncio
from dataclasses import replace
import json
import time

import httpx
import pytest

from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend, OpenSandboxBackend, SandboxCapacityUnavailable, SandboxNotFound, SandboxProfileUnavailable)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxScope)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateRequest)


class SlowCreateBackend(FakeSandboxBackend):
    def __init__(self, delay: float = 0.15):
        super().__init__()
        self.delay = delay
        self.create_started = asyncio.Event()

    async def create(self, *args, **kwargs):
        self.create_started.set()
        await asyncio.sleep(self.delay)
        return await super().create(*args, **kwargs)


async def test_fake_backend_reuses_one_sandbox_per_allocation():
    backend=FakeSandboxBackend(namespace="test")
    profile=backend.legacy_profile(Harness.CODEX,"image-a")
    request=SandboxCreateRequest(harness=Harness.CODEX,profile=profile,environment={},
        ttl_seconds=300,allocation_id="allocation-1")
    first,second=await asyncio.gather(backend.create(request),backend.create(request))
    assert first.id==second.id
    assert len(await backend.list_owned(SandboxScope("test")))==1


async def test_fake_backend_rejects_profile_conflict_for_same_allocation():
    backend=FakeSandboxBackend(namespace="test")
    first=SandboxCreateRequest(harness=Harness.CODEX,
        profile=backend.legacy_profile(Harness.CODEX,"image-a"),environment={},
        ttl_seconds=300,allocation_id="allocation-1")
    conflict=SandboxCreateRequest(harness=Harness.CODEX,
        profile=backend.legacy_profile(Harness.CODEX,"image-b"),environment={},
        ttl_seconds=300,allocation_id="allocation-1")
    await backend.create(first)
    with pytest.raises(SandboxProfileUnavailable,match="allocation"):
        await backend.create(conflict)


async def test_fake_backend_get_rejects_mismatched_durable_handle():
    backend=FakeSandboxBackend(namespace="test")
    sandbox=await backend.create(SandboxCreateRequest(harness=Harness.CODEX,
        profile=backend.legacy_profile(Harness.CODEX,"image-a"),environment={},
        ttl_seconds=300,allocation_id="allocation-1"))
    with pytest.raises(SandboxProfileUnavailable,match="handle fence"):
        await backend.get(replace(sandbox.handle,allocation_id="different"))


async def test_fake_backend_scopes_turn_lookup_to_namespace():
    first = FakeSandboxBackend(namespace="first-product")
    second = FakeSandboxBackend(namespace="second-product")
    sandbox = await first.create(
        Harness.CODEX,
        "image@sha256:" + "0" * 64,
        {"sandbox-harness.io/turn": "same-turn", "sandbox-harness.io/harness": "codex"},
        {},
    )
    second.items[sandbox.id] = sandbox

    allocation=sandbox.handle.allocation_id
    assert await first.reconcile_create(SandboxScope("first-product"),allocation) == sandbox
    assert await second.reconcile_create(SandboxScope("second-product"),allocation) is None


async def test_zero_warm_targets_are_available_for_on_demand_creation():
    backend=FakeSandboxBackend()
    pool=SandboxPool(backend,{harness:"" for harness in Harness},
        {harness:0 for harness in Harness})
    try:assert await pool.execution_available()
    finally:await pool.close()


async def test_opensandbox_lookup_sends_namespace_and_rejects_unscoped_rows():
    requests = []

    def respond(request: httpx.Request):
        requests.append(request)
        return httpx.Response(200, json={"items": [{
            "id": "foreign",
            "image": {"uri": "image@sha256:" + "0" * 64},
            "endpoints": {"runtime": "http://runtime"},
            "metadata": {
                "sandbox-harness.io/namespace": "another-product",
                "sandbox-harness.io/idempotency-key": "same-turn",
                "sandbox-harness.io/harness": "codex",
            },
        }]})

    backend = OpenSandboxBackend("http://opensandbox", "", 3600, "sandbox-harness")
    await backend.client.aclose()
    backend.client = httpx.AsyncClient(
        base_url="http://opensandbox",
        transport=httpx.MockTransport(respond),
    )
    try:
        assert await backend.reconcile_create(
            SandboxScope("sandbox-harness"),"same-turn") is None
        assert set(requests[0].url.params["metadata"].split("&"))=={
            "sandbox-harness.io/namespace=sandbox-harness",
            "sandbox-harness.io/idempotency-key=same-turn"}
        assert await backend.list_owned(SandboxScope("another-product"))==[]
        assert len(requests)==1
    finally:
        await backend.client.aclose()


async def test_opensandbox_turn_inventory_classifies_failed_and_ready_matches_independently():
    metadata=lambda allocation:{'sandbox-harness.io/namespace':'sandbox-harness',
        'sandbox-harness.io/idempotency-key':allocation,'sandbox-harness.io/harness':'codex'}
    rows=[{'id':'failed','metadata':metadata('failed'),'image':{'uri':'image'}},
        {'id':'healthy','metadata':metadata('healthy'),'image':{'uri':'image'}}]
    def respond(request:httpx.Request):
        if request.url.path=='/v1/sandboxes':return httpx.Response(200,json={'items':rows})
        if request.url.path=='/v1/sandboxes/failed':
            return httpx.Response(200,json={**rows[0],'status':{'state':'Failed'}})
        if request.url.path=='/v1/sandboxes/healthy':
            return httpx.Response(200,json={**rows[1],'status':{'state':'Running'}})
        if request.url.path=='/v1/sandboxes/healthy/endpoints/8080':
            return httpx.Response(200,json={'endpoint':'http://runtime'})
        if request.url.host=='runtime' and request.url.path=='/health':
            return httpx.Response(200,json={'status':'ok'})
        return httpx.Response(404)
    backend=OpenSandboxBackend('http://opensandbox','',3600,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(
        base_url='http://opensandbox',transport=httpx.MockTransport(respond))
    try:
        matches=await backend.list_owned(SandboxScope('sandbox-harness'))
        assert [(value.id,value.health_state) for value in matches]==[
            ('failed','failed'),('healthy','ready')]
        rows.reverse()
        matches=await backend.list_owned(SandboxScope('sandbox-harness'))
        assert [(value.id,value.health_state) for value in matches]==[
            ('healthy','ready'),('failed','failed')]
    finally:await backend.client.aclose()


async def test_opensandbox_create_rate_limit_has_infrastructure_error_code():
    def respond(request:httpx.Request):return httpx.Response(429,json={'detail':'capacity'})
    backend=OpenSandboxBackend('http://opensandbox','',3600,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(
        base_url='http://opensandbox',transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(SandboxCapacityUnavailable) as failure:
            await backend.create(Harness.CODEX,'runtime@sha256:'+'1'*64,{
                'sandbox-harness.io/turn':'turn-1'}, {})
        assert failure.value.error_code=='infrastructure_capacity_wait'
    finally:await backend.client.aclose()


async def test_opensandbox_turn_inventory_keeps_health_timeout_and_other_ready_match():
    metadata=lambda allocation:{'sandbox-harness.io/namespace':'sandbox-harness',
        'sandbox-harness.io/idempotency-key':allocation,'sandbox-harness.io/harness':'codex'}
    rows=[{'id':'timeout','metadata':metadata('timeout'),'image':{'uri':'image'}},
        {'id':'ready','metadata':metadata('ready'),'image':{'uri':'image'}}]
    def respond(request:httpx.Request):
        if request.url.path=='/v1/sandboxes':return httpx.Response(200,json={'items':rows})
        if request.url.path in {'/v1/sandboxes/timeout','/v1/sandboxes/ready'}:
            row=rows[0] if request.url.path.endswith('timeout') else rows[1]
            return httpx.Response(200,json={**row,'status':{'state':'Running'}})
        if request.url.path.endswith('/endpoints/8080'):
            identity=request.url.path.split('/')[-3]
            return httpx.Response(200,json={'endpoint':'http://'+identity})
        if request.url.host=='timeout':raise httpx.ConnectError('health timeout',request=request)
        if request.url.host=='ready':return httpx.Response(200,json={'status':'ok'})
        return httpx.Response(404)
    backend=OpenSandboxBackend('http://opensandbox','',3600,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(
        base_url='http://opensandbox',transport=httpx.MockTransport(respond))
    try:
        matches=await backend.list_owned(SandboxScope('sandbox-harness'))
        assert [(value.id,value.health_state) for value in matches]==[
            ('timeout','unhealthy'),('ready','ready')]
    finally:await backend.client.aclose()


async def test_namespace_scan_skips_sandbox_deleted_between_list_and_detail():
    def respond(request: httpx.Request):
        if request.url.path == "/v1/sandboxes":
            return httpx.Response(200, json={"items":[{"id":"gone",
                "image":{"uri":"image@sha256:" + "0" * 64},
                "metadata":{"sandbox-harness.io/namespace":"sandbox-harness",
                    "sandbox-harness.io/turn":"gone-turn","sandbox-harness.io/harness":"claude"}}]})
        return httpx.Response(404)
    backend = OpenSandboxBackend("http://opensandbox", "", 3600, "sandbox-harness")
    await backend.client.aclose(); backend.client = httpx.AsyncClient(
        base_url="http://opensandbox", transport=httpx.MockTransport(respond))
    try:
        assert await backend.list_owned(SandboxScope("sandbox-harness")) == []
    finally:
        await backend.client.aclose()


async def test_opensandbox_ttl_renew_does_not_mutate_ownership_metadata():
    requests = []
    def respond(request: httpx.Request):
        requests.append(request)
        if request.url.path=="/v1/sandboxes/sandbox-one":
            return httpx.Response(200,json={"id":"sandbox-one","metadata":{
                "sandbox-harness.io/namespace":"sandbox-harness",
                "sandbox-harness.io/harness":"codex"},"status":{"state":"running"}})
        if request.url.path.endswith("/renew-expiration"):
            return httpx.Response(200, json={"expiresAt": json.loads(request.content)["expiresAt"]})
        if request.url.path.endswith("/endpoints/8080"):
            return httpx.Response(200,json={"endpoint":"http://runtime"})
        return httpx.Response(404)
    backend = OpenSandboxBackend("http://opensandbox", "", 3600, "sandbox-harness")
    await backend.client.aclose(); backend.client = httpx.AsyncClient(
        base_url="http://opensandbox", transport=httpx.MockTransport(respond))
    sandbox = __import__('sandbox_harness.common.sandbox.sandbox', fromlist=['Sandbox']).Sandbox(
        id="sandbox-one", harness=Harness.CODEX, image="image", runtime_url="http://runtime", metadata={})
    try:
        await backend.renew(sandbox,force=True)
        assert [request.url.path for request in requests] == [
            "/v1/sandboxes/sandbox-one",
            "/v1/sandboxes/sandbox-one/renew-expiration",
            "/v1/sandboxes/sandbox-one",
            "/v1/sandboxes/sandbox-one/endpoints/8080"]
        assert sandbox.metadata=={}
        assert sandbox.expires_at_monotonic > time.monotonic() + 3500
    finally:
        await backend.client.aclose()


async def test_opensandbox_refreshes_runtime_endpoint_before_proxy_url_expires():
    requests=[]
    def respond(request:httpx.Request):
        requests.append(request)
        if request.url.path=='/v1/sandboxes/sandbox-one':
            return httpx.Response(200,json={'id':'sandbox-one','metadata':{
                'sandbox-harness.io/namespace':'sandbox-harness',
                'sandbox-harness.io/harness':'codex'},'status':{'state':'running'}})
        if request.url.path.endswith('/endpoints/8080'):
            return httpx.Response(200,json={'endpoint':'http://runtime-new'})
        return httpx.Response(500)
    backend=OpenSandboxBackend('http://opensandbox','',3600,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(
        base_url='http://opensandbox',transport=httpx.MockTransport(respond))
    sandbox=__import__('sandbox_harness.common.sandbox.sandbox',fromlist=['Sandbox']).Sandbox(
        id='sandbox-one',harness=Harness.CODEX,image='image',runtime_url='http://runtime-old',
        metadata={},expires_at_monotonic=time.monotonic()+3000)
    try:
        await backend.renew(sandbox)
        assert [request.url.path for request in requests]==[
            '/v1/sandboxes/sandbox-one',
            '/v1/sandboxes/sandbox-one/endpoints/8080']
        assert sandbox.runtime_url=='http://runtime-new'
        assert sandbox.runtime_endpoint_refresh_at_monotonic>time.monotonic()+50
    finally:await backend.client.aclose()


async def test_create_scopes_sandbox_for_the_shared_internal_network():
    requests = []

    def respond(request: httpx.Request):
        requests.append(request)
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"status": "bootstrap"})
        if request.url.path.endswith("/endpoints/8080"):
            return httpx.Response(200, json={"endpoint": "127.0.0.1:28090/proxy/8080"})
        if request.method == "GET":
            return httpx.Response(200, json={"id": "sandbox-one",
                "metadata":{"sandbox-harness.io/namespace":"sandbox-harness",
                    "sandbox-harness.io/harness":"codex"},
                "status": {"state": "Running"}})
        return httpx.Response(202, json={
            "id": "sandbox-one",
        })

    backend = OpenSandboxBackend(
        "http://opensandbox", "", 3600, "sandbox-harness",
        "http://172.31.0.1:28600", 28601,
    )
    await backend.client.aclose()
    backend.client = httpx.AsyncClient(base_url="http://opensandbox", transport=httpx.MockTransport(respond))
    try:
        sandbox = await backend.create(
            Harness.CODEX,
            "image@sha256:" + "0" * 64,
            {"sandbox-harness.io/turn": "turn-one", "sandbox-harness.io/harness": "codex"},
            {},
        )
    finally:
        await backend.client.aclose()

    body = json.loads(requests[0].content)
    assert sandbox.metadata["sandbox-harness.io/namespace"] == "sandbox-harness"
    assert sandbox.runtime_url == "http://127.0.0.1:28090/proxy/8080"
    assert "networkPolicy" not in body


async def test_attached_sandbox_expires_and_is_deleted():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness})
    sandbox = await pool.acquire(thread_id="thread", turn_id="turn", harness=Harness.CLAUDE, environment={})
    await pool.release(sandbox, keep_attached=True, keepalive_seconds=0)
    for _ in range(100):
        if sandbox.id not in backend.items:
            break
        await __import__('asyncio').sleep(.01)
    assert sandbox.id not in backend.items
    assert "thread" not in pool._attached
    await pool.close()


async def test_reacquiring_attached_sandbox_cancels_expiration():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness})
    sandbox = await pool.acquire(thread_id="thread", turn_id="one", harness=Harness.CLAUDE, environment={})
    await pool.release(sandbox, keep_attached=True, keepalive_seconds=1)
    same = await pool.acquire(thread_id="thread", turn_id="two", harness=Harness.CLAUDE, environment={})
    assert same is sandbox
    assert same.reused is True
    await __import__('asyncio').sleep(.02)
    assert sandbox.id in backend.items
    await pool.release(sandbox, keep_attached=False)
    await pool.close()


async def test_attached_claim_update_failure_preserves_expiry_idle_and_capacity_for_retry():
    class Backend(FakeSandboxBackend):
        fail=True
        async def renew(self,sandbox,*,force=False):
            if self.fail:raise RuntimeError('temporary claim failure')
            return await super().renew(sandbox,force=force)
    import pytest
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1)
    sandbox=await pool.acquire(thread_id='thread',turn_id='one',harness=Harness.CLAUDE,environment={})
    await pool.release(sandbox,keep_attached=True,keepalive_seconds=60)
    expiry=pool._expiry['thread'];idle=pool._idle_since['thread']
    with pytest.raises(RuntimeError,match='temporary claim failure'):
        await pool.acquire(thread_id='thread',turn_id='two',harness=Harness.CLAUDE,environment={})
    assert pool._attached['thread'] is sandbox
    assert pool._expiry['thread'] is expiry and not expiry.cancelled()
    assert pool._idle_since['thread']==idle
    assert not await pool.execution_available()
    backend.fail=False
    assert await pool.acquire(thread_id='thread',turn_id='two',harness=Harness.CLAUDE,
        environment={}) is sandbox
    await pool.release(sandbox,keep_attached=True,keepalive_seconds=60)
    other=await asyncio.wait_for(pool.acquire(thread_id='other',turn_id='other',
        harness=Harness.CLAUDE,environment={}),1)
    assert sandbox.id not in backend.items and other.id in backend.items
    await pool.release(other,keep_attached=False);await pool.close()


async def test_cancellation_during_attached_claim_update_keeps_expiry_tracking():
    class Backend(FakeSandboxBackend):
        block=False
        entered=asyncio.Event()
        async def renew(self,sandbox,*,force=False):
            if self.block:
                self.entered.set();await asyncio.Event().wait()
            return await super().renew(sandbox,force=force)
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    sandbox=await pool.acquire(thread_id='thread',turn_id='one',harness=Harness.CLAUDE,environment={})
    await pool.release(sandbox,keep_attached=True,keepalive_seconds=60);backend.block=True
    task=asyncio.create_task(pool.acquire(thread_id='thread',turn_id='two',
        harness=Harness.CLAUDE,environment={}))
    await backend.entered.wait();task.cancel();await asyncio.gather(task,return_exceptions=True)
    assert pool._attached['thread'] is sandbox
    assert 'thread' in pool._expiry and 'thread' in pool._idle_since
    await pool.discard_thread('thread');await pool.close()


async def test_execution_availability_rejects_and_retires_stale_attached_and_warm_objects():
    backend=FakeSandboxBackend();targets={h:(1 if h==Harness.CODEX else 0) for h in Harness}
    pool=SandboxPool(backend,{h:'' for h in Harness},targets,max_sandboxes=2)
    await pool.fill();warm=pool._warm[Harness.CODEX][0]
    await backend.delete(warm)
    assert not await pool.execution_available()
    assert not pool._warm[Harness.CODEX]
    attached=await pool.acquire(thread_id='thread',turn_id='turn',harness=Harness.CODEX,environment={})
    await pool.release(attached,keep_attached=True,keepalive_seconds=60)
    await backend.delete(attached)
    assert not await pool.execution_available()
    assert 'thread' not in pool._attached
    replacement=await asyncio.wait_for(pool.acquire(thread_id='other',turn_id='other',
        harness=Harness.CODEX,environment={}),1)
    await pool.release(replacement,keep_attached=False);await pool.close()


async def test_concurrent_retire_merges_user_slot_and_releases_exactly_once():
    class Backend(FakeSandboxBackend):
        entered=asyncio.Event();release=asyncio.Event()
        async def delete(self,sandbox):
            self.entered.set();await self.release.wait();await super().delete(sandbox)
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1)
    sandbox=await pool.acquire(thread_id='thread',turn_id='turn',harness=Harness.CODEX,environment={})
    first=asyncio.create_task(pool.retire(sandbox));await backend.entered.wait()
    second=asyncio.create_task(pool.retire(sandbox));await asyncio.sleep(.01)
    assert pool._retiring[pool.sandbox_key(sandbox)][1] is True
    backend.release.set();await asyncio.gather(first,second)
    assert not await pool.managed_sandbox_ids()
    replacement=await asyncio.wait_for(pool.acquire(thread_id='other',turn_id='other',
        harness=Harness.CODEX,environment={}),1)
    await pool.release(replacement,keep_attached=False);await pool.close()


async def test_cancelled_retire_keeps_slot_bit_for_idempotent_retry():
    class Backend(FakeSandboxBackend):
        entered=asyncio.Event();block=True
        async def delete(self,sandbox):
            if self.block:self.entered.set();await asyncio.Event().wait()
            await super().delete(sandbox)
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1)
    sandbox=await pool.acquire(thread_id='thread',turn_id='turn',harness=Harness.CODEX,environment={})
    task=asyncio.create_task(pool.retire(sandbox));await backend.entered.wait();task.cancel()
    await asyncio.gather(task,return_exceptions=True)
    assert pool._retiring[pool.sandbox_key(sandbox)][1] is True
    backend.block=False;await pool.retry_deletions()
    replacement=await asyncio.wait_for(pool.acquire(thread_id='other',turn_id='other',
        harness=Harness.CODEX,environment={}),1)
    await pool.release(replacement,keep_attached=False);await pool.close()


async def test_concurrent_retire_after_external_delete_is_success_and_releases_slot_once():
    class Backend(FakeSandboxBackend):
        async def delete(self,sandbox):
            if sandbox.id not in self.items:
                raise SandboxNotFound('already absent')
            await super().delete(sandbox)
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1)
    sandbox=await pool.acquire(thread_id='thread',turn_id='turn',harness=Harness.CODEX,environment={})
    await backend.delete(sandbox)
    await asyncio.gather(pool.retire(sandbox),pool.retire(sandbox))
    assert not await pool.managed_sandbox_ids()
    replacement=await asyncio.wait_for(pool.acquire(thread_id='replacement',turn_id='replacement',
        harness=Harness.CODEX,environment={}),1)
    await pool.release(replacement,keep_attached=False)
    await pool.close()


async def test_frozen_image_does_not_reuse_attached_sandbox_from_another_release():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "image-a@sha256:" + "1" * 64 for harness in Harness},
        {harness: 0 for harness in Harness})
    first = await pool.acquire(thread_id="thread", turn_id="one", harness=Harness.CLAUDE,
        environment={}, image="image-a@sha256:" + "1" * 64)
    await pool.release(first, keep_attached=True, keepalive_seconds=60)
    second = await pool.acquire(thread_id="thread", turn_id="two", harness=Harness.CLAUDE,
        environment={}, image="image-b@sha256:" + "2" * 64)
    assert second.id != first.id
    assert second.image.endswith("2" * 64) and first.id not in backend.items
    await pool.close()


async def test_deleting_thread_discards_its_retained_sandbox():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness})
    sandbox = await pool.acquire(thread_id="delete-thread", turn_id="done", harness=Harness.CODEX, environment={})
    await pool.release(sandbox, keep_attached=True, keepalive_seconds=60)
    await pool.discard_thread("delete-thread")
    assert sandbox.id not in backend.items
    assert "delete-thread" not in pool._attached and "delete-thread" not in pool._expiry
    await pool.close()


async def test_warm_pool_refills_after_claim():
    backend = FakeSandboxBackend()
    targets = {harness: (2 if harness == Harness.CODEX else 0) for harness in Harness}
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, targets)
    await pool.fill()
    assert len(pool._warm[Harness.CODEX]) == 2
    claimed = await pool.acquire(thread_id="thread", turn_id="turn", harness=Harness.CODEX, environment={})
    for _ in range(100):
        if len(pool._warm[Harness.CODEX]) == 2:
            break
        await __import__('asyncio').sleep(.01)
    assert len(pool._warm[Harness.CODEX]) == 2
    assert claimed.id not in {sandbox.id for sandbox in pool._warm[Harness.CODEX]}
    await pool.release(claimed, keep_attached=False)
    await pool.close()


async def test_total_limit_applies_backpressure_until_user_slot_is_released():
    asyncio = __import__('asyncio')
    backend = FakeSandboxBackend()
    targets = {harness: (1 if harness == Harness.CLAUDE else 0) for harness in Harness}
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, targets, max_sandboxes=2)
    await pool.fill()
    first = await pool.acquire(thread_id="first", turn_id="one", harness=Harness.CLAUDE, environment={})
    waiting = asyncio.create_task(pool.acquire(thread_id="second", turn_id="two", harness=Harness.CLAUDE, environment={}))
    await asyncio.sleep(.02)
    assert not waiting.done()
    await pool.release(first, keep_attached=False)
    second = await asyncio.wait_for(waiting, 1)
    assert second.thread_id == "second"
    assert len(pool._attached) + sum(len(items) for items in pool._warm.values()) <= 2
    await pool.release(second, keep_attached=False)
    await pool.close()


async def test_pool_shutdown_can_preserve_an_active_sandbox_for_broker_recovery():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness})
    active = await pool.acquire(thread_id="thread", turn_id="turn", harness=Harness.CODEX, environment={})
    await pool.close(preserve_sandbox_ids={active.id})
    assert active.id in backend.items
    await backend.delete(active)


async def test_capacity_evicts_oldest_idle_attached_sandbox_before_blocking_new_turn():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness},
        max_sandboxes=2)
    first = await pool.acquire(thread_id="first", turn_id="one", harness=Harness.CLAUDE, environment={})
    await pool.release(first, keep_attached=True, keepalive_seconds=60)
    await __import__('asyncio').sleep(.01)
    second = await pool.acquire(thread_id="second", turn_id="two", harness=Harness.CLAUDE, environment={})
    await pool.release(second, keep_attached=True, keepalive_seconds=60)
    third = await __import__('asyncio').wait_for(pool.acquire(
        thread_id="third", turn_id="three", harness=Harness.CLAUDE, environment={}), timeout=1)
    assert first.id not in backend.items
    assert second.id in backend.items and third.id in backend.items
    await pool.release(third, keep_attached=False)
    await pool.close()


async def test_capacity_waiter_wakes_when_active_sandbox_becomes_idle():
    backend=FakeSandboxBackend()
    pool=SandboxPool(backend,{harness:"" for harness in Harness},
        {harness:0 for harness in Harness},max_sandboxes=1)
    first=await pool.acquire(thread_id="first",turn_id="one",harness=Harness.CLAUDE,
        environment={})
    waiting=asyncio.create_task(pool.acquire(thread_id="second",turn_id="two",
        harness=Harness.CLAUDE,environment={}))
    await asyncio.sleep(.01)
    assert not waiting.done()
    await pool.release(first,keep_attached=True,keepalive_seconds=60)
    second=await asyncio.wait_for(waiting,1)
    assert first.id not in backend.items and second.id in backend.items
    await pool.release(second,keep_attached=False)
    await pool.close()


async def test_concurrent_claims_atomically_evict_idle_sessions_at_capacity():
    asyncio = __import__('asyncio')
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness},
        max_sandboxes=2)
    for name in ("idle-one", "idle-two"):
        sandbox = await pool.acquire(thread_id=name, turn_id=name, harness=Harness.CLAUDE, environment={})
        await pool.release(sandbox, keep_attached=True, keepalive_seconds=60)
    first, second = await asyncio.wait_for(asyncio.gather(
        pool.acquire(thread_id="new-one", turn_id="new-one", harness=Harness.CLAUDE, environment={}),
        pool.acquire(thread_id="new-two", turn_id="new-two", harness=Harness.CLAUDE, environment={}),
    ), timeout=2)
    assert first.id in backend.items and second.id in backend.items
    await pool.release(first, keep_attached=False); await pool.release(second, keep_attached=False)
    await pool.close()


async def test_expired_warm_sandbox_is_replaced_during_claim():
    backend = FakeSandboxBackend()
    targets = {harness: (1 if harness == Harness.CLAUDE else 0) for harness in Harness}
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, targets, max_sandboxes=2)
    await pool.fill()
    expired = pool._warm[Harness.CLAUDE][0]
    await backend.delete(expired)
    claimed = await pool.acquire(thread_id="thread", turn_id="turn", harness=Harness.CLAUDE, environment={})
    assert claimed.id != expired.id
    assert claimed.id in backend.items
    await pool.release(claimed, keep_attached=False)
    await pool.close()


async def test_expired_attached_sandbox_is_replaced_during_reacquire():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness},
        max_sandboxes=2)
    expired = await pool.acquire(thread_id="thread", turn_id="one", harness=Harness.CLAUDE, environment={})
    await pool.release(expired, keep_attached=True, keepalive_seconds=60)
    await backend.delete(expired)
    claimed = await pool.acquire(thread_id="thread", turn_id="two", harness=Harness.CLAUDE, environment={})
    assert claimed.id != expired.id
    assert claimed.id in backend.items
    await pool.release(claimed, keep_attached=False)
    await pool.close()


async def test_sandbox_creates_for_different_threads_run_concurrently():
    backend = SlowCreateBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness},
        {harness: 0 for harness in Harness}, max_sandboxes=8)
    started = time.monotonic()
    sandboxes = await asyncio.gather(*(pool.acquire(
        thread_id=f"thread-{index}", turn_id=f"turn-{index}", harness=Harness.CLAUDE,
        environment={},
    ) for index in range(4)))
    elapsed = time.monotonic() - started
    assert elapsed < backend.delay * 2.5
    await asyncio.gather(*(pool.release(sandbox, keep_attached=False) for sandbox in sandboxes))
    await pool.close()


async def test_unrelated_sandbox_create_does_not_block_release():
    backend = SlowCreateBackend(delay=0.2)
    pool = SandboxPool(backend, {harness: "" for harness in Harness},
        {harness: 0 for harness in Harness}, max_sandboxes=4)
    first = await pool.acquire(thread_id="first", turn_id="first", harness=Harness.CLAUDE, environment={})
    backend.create_started = asyncio.Event()
    creating = asyncio.create_task(pool.acquire(
        thread_id="second", turn_id="second", harness=Harness.CLAUDE, environment={}))
    await backend.create_started.wait()
    started = time.monotonic()
    await pool.release(first, keep_attached=False)
    assert time.monotonic() - started < 0.1
    second = await creating
    await pool.release(second, keep_attached=False)
    await pool.close()


async def test_failed_delete_keeps_capacity_until_retry_confirms_absence():
    class Backend(FakeSandboxBackend):
        fail = True
        async def delete(self, sandbox):
            if self.fail:
                raise RuntimeError('temporary delete failure')
            await super().delete(sandbox)
    import pytest
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    first=await pool.acquire(thread_id='one',turn_id='one',harness=Harness.CODEX,environment={})
    with pytest.raises(RuntimeError,match='temporary delete failure'):
        await pool.release(first,keep_attached=False)
    assert first.id in await pool.managed_sandbox_ids()
    waiting=asyncio.create_task(pool.acquire(thread_id='two',turn_id='two',harness=Harness.CODEX,environment={}))
    await asyncio.sleep(.01);assert not waiting.done()
    backend.fail=False;await pool.retry_deletions()
    second=await asyncio.wait_for(waiting,1)
    assert first.id not in backend.items
    await pool.release(second,keep_attached=False);await pool.close()


async def test_namespace_sweep_includes_failed_sandboxes_and_exact_turn_lookup():
    def respond(request):
        metadata={'sandbox-harness.io/namespace':'sandbox-harness','sandbox-harness.io/harness':'codex','sandbox-harness.io/idempotency-key':'wrong-turn'}
        if request.url.path=='/v1/sandboxes':
            return httpx.Response(200,json={'items':[{'id':'broken','metadata':metadata,'image':{'uri':'i'}}]})
        return httpx.Response(200,json={'id':'broken','status':{'state':'Failed'}})
    backend=OpenSandboxBackend('http://sandbox','',60,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(base_url='http://sandbox',transport=httpx.MockTransport(respond))
    try:
        assert [s.id for s in await backend.list_owned(SandboxScope('sandbox-harness'))]==['broken']
        assert await backend.reconcile_create(
            SandboxScope('sandbox-harness'),'expected-turn') is None
    finally:await backend.client.aclose()


async def test_repeated_adoption_does_not_reserve_an_extra_slot():
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    external=await backend.create(Harness.CODEX,'',{'sandbox-harness.io/turn':'t'}, {})
    first=await pool.adopt(external,thread_id='thread',turn_id='t',expected_profile=external.profile)
    assert await asyncio.wait_for(pool.adopt(external,thread_id='thread',turn_id='t',
        expected_profile=external.profile),1) is first
    await pool.release(first,keep_attached=False)
    second=await asyncio.wait_for(pool.acquire(thread_id='new',turn_id='next',harness=Harness.CODEX,environment={}),1)
    await pool.release(second,keep_attached=False);await pool.close()


async def test_pool_replaces_attached_sandbox_when_default_profile_changes():
    first_image='registry/runtime@sha256:'+'1'*64
    second_image='registry/runtime@sha256:'+'2'*64
    backend=FakeSandboxBackend();images={h:first_image for h in Harness}
    pool=SandboxPool(backend,images,{h:0 for h in Harness},max_sandboxes=2)
    first=await pool.acquire(thread_id='thread',turn_id='one',harness=Harness.CODEX,environment={})
    await pool.release(first,keep_attached=True)
    images[Harness.CODEX]=second_image

    second=await pool.acquire(thread_id='thread',turn_id='two',harness=Harness.CODEX,environment={})

    assert second.id!=first.id and first.id not in backend.items
    assert second.profile.provider_ref==second_image
    await pool.release(second,keep_attached=False);await pool.close()


async def test_pool_skips_incompatible_warm_sandbox_and_rejects_incompatible_adoption():
    first_image='registry/runtime@sha256:'+'1'*64
    second_image='registry/runtime@sha256:'+'2'*64
    backend=FakeSandboxBackend();images={h:first_image for h in Harness}
    targets={h:(1 if h==Harness.CODEX else 0) for h in Harness}
    pool=SandboxPool(backend,images,targets,max_sandboxes=3)
    await pool.fill();warm_id=pool._warm[Harness.CODEX][0].id
    images[Harness.CODEX]=second_image
    acquired=await pool.acquire(thread_id='thread',turn_id='turn',harness=Harness.CODEX,environment={})
    assert acquired.id!=warm_id and acquired.profile.provider_ref==second_image
    await pool.release(acquired,keep_attached=False)
    external=await backend.create(Harness.CODEX,first_image,
        {'sandbox-harness.io/turn':'external'}, {})
    with pytest.raises(SandboxProfileUnavailable):
        await pool.adopt(external,thread_id='external-thread',turn_id='external')
    await backend.delete(external);await pool.close()


async def test_durable_allocation_keeps_one_slot_until_late_adoption():
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown)
    class Backend(FakeSandboxBackend):
        creates=[]
        async def create(self,request):
            self.creates.append(request.allocation_id)
            sandbox=await super().create(request)
            if request.allocation_id=='allocation':
                raise SandboxCreateOutcomeUnknown('response lost',provider=self.provider)
            return sandbox
    backend=Backend();pool=SandboxPool(backend,{Harness.CODEX:'image'},
        {Harness.CODEX:0},max_sandboxes=1)
    waiting=None
    try:
        with pytest.raises(SandboxCreateOutcomeUnknown):
            await pool.acquire(thread_id='owner',turn_id='allocation',harness=Harness.CODEX,
                environment={},retain_unresolved_allocation=True)
        assert await pool.owns_allocation('fake','allocation')
        with pytest.raises(SandboxCreateOutcomeUnknown):
            await pool.acquire(thread_id='owner',turn_id='allocation',harness=Harness.CODEX,
                environment={},retain_unresolved_allocation=True)
        assert backend.creates==['allocation']
        waiting=asyncio.create_task(pool.acquire(thread_id='other',turn_id='other',
            harness=Harness.CODEX,environment={}))
        await asyncio.sleep(.02);assert not waiting.done()
        sandbox=await pool.reconcile_create('allocation')
        adopted=await asyncio.wait_for(pool.adopt(sandbox,thread_id='owner',turn_id='allocation',
            expected_profile=sandbox.profile),.5)
        assert adopted.id==sandbox.id and not waiting.done()
        await pool.release(adopted,keep_attached=False)
        replacement=await asyncio.wait_for(waiting,.5)
        assert backend.creates==['allocation','other']
        await pool.release(replacement,keep_attached=False)
    finally:
        if waiting and not waiting.done():waiting.cancel();await asyncio.gather(waiting,return_exceptions=True)
        await pool.close()


async def test_cancelled_durable_create_retains_slot_until_confirmed_retirement():
    class Backend(FakeSandboxBackend):
        created=asyncio.Event();fail_delete=True
        async def create(self,request):
            sandbox=await super().create(request)
            self.created.set()
            await asyncio.Event().wait()
            return sandbox
        async def delete(self,sandbox):
            if self.fail_delete:raise RuntimeError('temporary deletion failure')
            await super().delete(sandbox)
    backend=Backend();pool=SandboxPool(backend,{Harness.CODEX:'image'},
        {Harness.CODEX:0},max_sandboxes=1)
    task=asyncio.create_task(pool.acquire(thread_id='owner',turn_id='allocation',
        harness=Harness.CODEX,environment={},retain_unresolved_allocation=True))
    try:
        await backend.created.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert await pool.owns_allocation('fake','allocation')
        sandbox=await pool.reconcile_create('allocation')
        with pytest.raises(RuntimeError,match='deletion failure'):await pool.retire(sandbox)
        assert pool._user_slots._value==0
        backend.fail_delete=False;await pool.retry_deletions()
        assert pool._user_slots._value==1 and not backend.items
        await pool.retire(sandbox)
        assert pool._user_slots._value==1
    finally:await pool.close()
