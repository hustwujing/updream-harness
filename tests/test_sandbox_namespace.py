import json

import httpx

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox import FakeSandboxBackend, OpenSandboxBackend


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
