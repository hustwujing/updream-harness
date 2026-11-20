import httpx

from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.pool import SandboxPool
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import FakeSandboxBackend,OpenSandboxBackend
from sandbox_harness.sandbox_factory import SandboxProfileResolver,build_sandbox_backend


def test_backend_factory_is_the_single_provider_construction_boundary(tmp_path):
    fake=build_sandbox_backend(Settings(_env_file=None,backend="fake",data_root=tmp_path))
    assert isinstance(fake,FakeSandboxBackend) and fake.provider=="fake"
    opened=build_sandbox_backend(Settings(_env_file=None,backend="opensandbox",data_root=tmp_path,
        opensandbox_url="http://opensandbox",opensandbox_api_key="key"))
    assert isinstance(opened,OpenSandboxBackend) and opened.provider=="opensandbox"


def test_profile_resolver_freezes_provider_ref_and_runtime_contract():
    resolver=SandboxProfileResolver(provider="opensandbox",runtime_contract="runtime/v1",
        runtime_bundle_digest="1"*64,runtime_code_bundle_digest="2"*64)
    first=resolver.resolve(Harness.CODEX,"registry/one@sha256:"+"3"*64)
    same=resolver.resolve(Harness.CODEX,"registry/one@sha256:"+"3"*64)
    changed=resolver.resolve(Harness.CODEX,"registry/two@sha256:"+"4"*64)
    assert first.compatibility_key==same.compatibility_key
    assert first.compatibility_key!=changed.compatibility_key


async def test_pool_claims_sandbox_through_generic_update_not_provider_bind():
    backend=FakeSandboxBackend()
    async def forbidden(*args,**kwargs):raise AssertionError("provider bind must not be called")
    backend.bind=forbidden
    resolver=SandboxProfileResolver(provider="fake",runtime_contract="runtime/v1",
        runtime_bundle_digest="1"*64,runtime_code_bundle_digest="2"*64)
    pool=SandboxPool(backend,{h:"image@sha256:"+"3"*64 for h in Harness},
        {h:0 for h in Harness},profile_resolver=resolver)
    sandbox=await pool.acquire(thread_id="thread",turn_id="turn",harness=Harness.CODEX,
        environment={})
    assert sandbox.metadata["sandbox-harness.io/thread"]=="thread"
    await pool.close()


async def test_runtime_resolves_provider_endpoint_before_request():
    backend=FakeSandboxBackend(runtime_url="http://runtime")
    sandbox=await backend.create(Harness.CODEX,"image@sha256:"+"3"*64,
        {"sandbox-harness.io/turn":"turn"},{})
    calls=[];original=backend.endpoint
    async def endpoint(*args,**kwargs):
        calls.append((args[1],kwargs));return await original(*args,**kwargs)
    backend.endpoint=endpoint
    def respond(request:httpx.Request):
        assert request.url==httpx.URL("http://runtime/ports/8080/v1/executions/execution")
        return httpx.Response(404)
    runtime=RuntimeClient(backend=backend,transport=httpx.MockTransport(respond))
    assert await runtime.execution_status(sandbox,"execution") is None
    assert calls and calls[0][0]==8080
