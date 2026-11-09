import json

import httpx

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox import FakeSandboxBackend, OpenSandboxBackend
from sandbox_harness.pool import SandboxPool


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
    await __import__('asyncio').sleep(.02)
    assert sandbox.id in backend.items
    await pool.release(sandbox, keep_attached=False)
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
