import json

import httpx

from sandbox_harness.domain import Harness
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import Sandbox


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
