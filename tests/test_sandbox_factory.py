import httpx
import pytest

from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.pool import SandboxPool
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import AliyunSandboxBackend,FakeSandboxBackend,OpenSandboxBackend
from sandbox_harness.sandbox_contracts import SandboxCapabilities,SandboxProviderUnavailable
from sandbox_harness.sandbox_factory import SandboxProfileResolver,build_sandbox_backend


def test_backend_factory_is_the_single_provider_construction_boundary(tmp_path):
    fake=build_sandbox_backend(Settings(_env_file=None,backend="fake",data_root=tmp_path))
    assert isinstance(fake,FakeSandboxBackend) and fake.provider=="fake"
    opened=build_sandbox_backend(Settings(_env_file=None,backend="opensandbox",data_root=tmp_path,
        opensandbox_url="http://opensandbox",opensandbox_api_key="key"))
    assert isinstance(opened,OpenSandboxBackend) and opened.provider=="opensandbox"
    aliyun=build_sandbox_backend(Settings(_env_file=None,backend="aliyun",data_root=tmp_path,
        aliyun_sandbox_api_key="e2b_"+"1"*40))
    assert isinstance(aliyun,AliyunSandboxBackend) and aliyun.provider=="aliyun"
    assert not aliyun.capabilities.native_idempotency


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
    assert sandbox.thread_id=="thread" and sandbox.turn_id=="turn"
    assert "sandbox-harness.io/thread" not in sandbox.metadata
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


def immutable_backend():
    class ImmutableFakeBackend(FakeSandboxBackend):
        async def update(self,sandbox,value):
            assert not value.metadata
            return await super().update(sandbox,value)
    backend=ImmutableFakeBackend()
    backend.capabilities=SandboxCapabilities(native_idempotency=False,scoped_inventory=True,
        arbitrary_ports=True,ttl_renewal=True,authenticated_runtime_http=True,
        delete_convergence=True,pause_resume=False)
    return backend


async def test_pool_claims_warm_sandbox_without_provider_ownership_metadata():
    backend=immutable_backend()
    resolver=SandboxProfileResolver(provider="fake",runtime_contract="runtime/v1",
        runtime_bundle_digest="1"*64,runtime_code_bundle_digest="2"*64)
    pool=SandboxPool(backend,{h:"template" for h in Harness},
        {Harness.CODEX:1,Harness.CLAUDE:0,Harness.DEEPSEEK:0},profile_resolver=resolver)
    await pool.fill();warm=(await pool.managed_sandbox_ids()).pop()
    sandbox=await pool.acquire(thread_id="thread",turn_id="turn",harness=Harness.CODEX,
        environment={})
    assert sandbox.id==warm and sandbox.thread_id=="thread" and sandbox.turn_id=="turn"
    assert not any(key in sandbox.metadata for key in (
        "sandbox-harness.io/pool","sandbox-harness.io/thread","sandbox-harness.io/turn"))
    await pool.close()


async def test_pool_reuses_same_thread_without_provider_metadata_mutation():
    backend=immutable_backend()
    resolver=SandboxProfileResolver(provider="fake",runtime_contract="runtime/v1",
        runtime_bundle_digest="1"*64,runtime_code_bundle_digest="2"*64)
    pool=SandboxPool(backend,{h:"template" for h in Harness},{h:0 for h in Harness},
        profile_resolver=resolver)
    first=await pool.acquire(thread_id="thread",turn_id="turn-1",harness=Harness.CODEX,
        environment={})
    await pool.release(first,keep_attached=True,keepalive_seconds=900)
    second=await pool.acquire(thread_id="thread",turn_id="turn-2",harness=Harness.CODEX,
        environment={})
    assert second.id==first.id
    assert second.turn_id=="turn-2"
    await pool.close()


async def test_failed_warm_bind_retires_the_popped_sandbox():
    class FailingBindBackend(FakeSandboxBackend):
        async def renew(self,sandbox,*,force=False):
            raise SandboxProviderUnavailable("ttl failed",provider="fake")
    backend=FailingBindBackend();resolver=SandboxProfileResolver(provider="fake",
        runtime_contract="runtime/v1",runtime_bundle_digest="1"*64,
        runtime_code_bundle_digest="2"*64)
    pool=SandboxPool(backend,{h:"template" for h in Harness},
        {Harness.CODEX:1,Harness.CLAUDE:0,Harness.DEEPSEEK:0},profile_resolver=resolver)
    await pool.fill();warm=(await pool.managed_sandbox_ids()).pop()
    with pytest.raises(SandboxProviderUnavailable):
        await pool.acquire(thread_id="thread",turn_id="turn",harness=Harness.CODEX,
            environment={})
    assert warm not in backend.items and warm not in await pool.managed_sandbox_ids()
    await pool.close()
