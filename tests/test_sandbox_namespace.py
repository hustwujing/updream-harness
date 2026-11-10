import asyncio
import json
import time

import httpx

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox import FakeSandboxBackend, OpenSandboxBackend
from sandbox_harness.pool import SandboxPool


class SlowCreateBackend(FakeSandboxBackend):
    def __init__(self, delay: float = 0.15):
        super().__init__()
        self.delay = delay
        self.create_started = asyncio.Event()

    async def create(self, *args, **kwargs):
        self.create_started.set()
        await asyncio.sleep(self.delay)
        return await super().create(*args, **kwargs)


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

    assert await first.find_by_turn("same-turn") == [sandbox]
    assert await second.find_by_turn("same-turn") == []


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
                "sandbox-harness.io/turn": "same-turn",
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
        assert await backend.find_by_turn("same-turn") == []
        assert requests[0].url.params["metadata"] == (
            "sandbox-harness.io/namespace=sandbox-harness&"
            "sandbox-harness.io/turn=same-turn"
        )
    finally:
        await backend.client.aclose()


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
        assert await backend.list_namespace() == []
    finally:
        await backend.client.aclose()


async def test_warm_sandbox_binding_persists_turn_metadata():
    backend = FakeSandboxBackend()
    sandbox = await backend.create(
        Harness.CLAUDE,
        "image@sha256:" + "0" * 64,
        {
            "sandbox-harness.io/pool": "warm",
            "sandbox-harness.io/turn": "warm-one",
            "sandbox-harness.io/harness": "claude",
        },
        {},
    )

    await backend.bind(sandbox, "thread-one", "turn-one")

    assert sandbox.metadata.get("sandbox-harness.io/pool") is None
    assert sandbox.metadata["sandbox-harness.io/thread"] == "thread-one"
    assert await backend.find_by_turn("turn-one") == [sandbox]


async def test_create_scopes_sandbox_for_the_shared_internal_network():
    requests = []

    def respond(request: httpx.Request):
        requests.append(request)
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"status": "ok"})
        if request.url.path.endswith("/endpoints/8080"):
            return httpx.Response(200, json={"endpoint": "127.0.0.1:28090/proxy/8080"})
        if request.method == "GET":
            return httpx.Response(200, json={"id": "sandbox-one", "status": {"state": "Running"}})
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


async def test_deleting_thread_discards_its_retained_sandbox():
    backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {harness: "" for harness in Harness}, {harness: 0 for harness in Harness})
    sandbox = await pool.acquire(thread_id="delete-thread", turn_id="done", harness=Harness.CODEX, environment={})
    await pool.release(sandbox, keep_attached=True, keepalive_seconds=60)
    await pool.discard_thread("delete-thread")
    assert sandbox.id not in backend.items
    assert "delete-thread" not in pool._attached and "delete-thread" not in pool._expiry
    orphan = await backend.create(Harness.CODEX, "", {
        "sandbox-harness.io/thread":"orphan-thread", "sandbox-harness.io/turn":"done",
        "sandbox-harness.io/harness":"codex",
    }, {})
    await pool.discard_thread("orphan-thread")
    assert orphan.id not in backend.items
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
        metadata={'sandbox-harness.io/namespace':'sandbox-harness','sandbox-harness.io/harness':'codex','sandbox-harness.io/turn':'wrong-turn'}
        if request.url.path=='/v1/sandboxes':
            return httpx.Response(200,json={'items':[{'id':'broken','metadata':metadata,'image':{'uri':'i'}}]})
        return httpx.Response(200,json={'id':'broken','status':{'state':'Failed'}})
    backend=OpenSandboxBackend('http://sandbox','',60,'sandbox-harness')
    await backend.client.aclose();backend.client=httpx.AsyncClient(base_url='http://sandbox',transport=httpx.MockTransport(respond))
    try:
        assert [s.id for s in await backend.list_namespace()]==['broken']
        assert await backend.find_by_turn('expected-turn')==[]
    finally:await backend.client.aclose()


async def test_repeated_adoption_does_not_reserve_an_extra_slot():
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    external=await backend.create(Harness.CODEX,'',{'sandbox-harness.io/turn':'t'}, {})
    first=await pool.adopt(external,thread_id='thread',turn_id='t')
    assert await asyncio.wait_for(pool.adopt(external,thread_id='thread',turn_id='t'),1) is first
    await pool.release(first,keep_attached=False)
    second=await asyncio.wait_for(pool.acquire(thread_id='new',turn_id='next',harness=Harness.CODEX,environment={}),1)
    await pool.release(second,keep_attached=False);await pool.close()
