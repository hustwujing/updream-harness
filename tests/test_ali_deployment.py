import importlib.util
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]
ALI_ROOT=Path("/data2/wujing03/sandbox-harness-platform-parallel-ali")


def load(name: str, path: Path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ali_identity_matrix_is_disjoint_from_parallel():
    identity=load("ali_identity",ROOT/"deploy/ali/identity.py")
    assert identity.ROOT==ALI_ROOT
    assert identity.BRANCH=="sandbox-harness-platform-parallel-ali"
    assert identity.DATA_ROOT==ALI_ROOT/".data"
    assert identity.PORTS=={"api":24767,"postgres":28433,"gateway":29882,"proxy":29883}
    assert identity.POSTGRES_IDENTITY=="sandbox_harness_parallel_ali"
    assert identity.NAMESPACE=="sandbox-harness-platform-parallel-ali"
    assert identity.REDIS_PREFIX=="sandbox-harness-platform-parallel-ali:"
    assert identity.OBJECT_PREFIX=="sandbox-harness-platform-parallel-ali/"
    assert identity.REGISTRY_PREFIX=="127.0.0.1:25001/sandbox-harness-platform-parallel-ali"
    assert not {24767,28433,29882,29883}&{24766,28432,29880,29881}


def test_ali_environment_regenerates_service_secrets_and_isolates_paths():
    configure=load("ali_configure",ROOT/"deploy/ali/configure.py")
    source={
        "SANDBOX_HARNESS_UPSTREAM_CODEX":"https://provider.example/v1",
        "SANDBOX_HARNESS_UPSTREAM_CODEX_KEY":"shared-provider-key",
        "SANDBOX_HARNESS_MODEL_ROUTES_JSON":'[{"model":"test"}]',
        "SANDBOX_HARNESS_OPENSANDBOX_API_KEY":"shared-opensandbox-key",
        "SANDBOX_HARNESS_SESSION_SIGNING_KEY":"source-session-secret",
        "SANDBOX_HARNESS_GATEWAY_SIGNING_KEY":"source-gateway-secret",
        "SANDBOX_HARNESS_DATABASE_URL":"postgresql+asyncpg://source:secret@127.0.0.1:28432/source",
        "SANDBOX_HARNESS_DATA_ROOT":"/data2/wujing03/sandbox-harness-platform-parallel/.data",
    }
    tokens=iter(("db-secret","gateway-secret","control-secret","session-secret"))
    values=configure.build_environment(source,token=lambda:next(tokens))
    assert values["SANDBOX_HARNESS_BACKEND"]=="opensandbox"
    assert values["SANDBOX_HARNESS_DATA_ROOT"]==str(ALI_ROOT/".data")
    assert values["SANDBOX_HARNESS_NAMESPACE"]=="sandbox-harness-platform-parallel-ali"
    assert values["SANDBOX_HARNESS_REDIS_PREFIX"]=="sandbox-harness-platform-parallel-ali:"
    assert values["SANDBOX_HARNESS_OBJECT_PREFIX"]=="sandbox-harness-platform-parallel-ali/"
    assert "@127.0.0.1:28433/sandbox_harness_parallel_ali" in values["SANDBOX_HARNESS_DATABASE_URL"]
    assert values["SANDBOX_HARNESS_SESSION_SIGNING_KEY"]=="session-secret"
    assert values["SANDBOX_HARNESS_GATEWAY_SIGNING_KEY"]=="gateway-secret"
    assert values["SANDBOX_HARNESS_UPSTREAM_CODEX_KEY"]=="shared-provider-key"
    assert values["SANDBOX_HARNESS_OPENSANDBOX_API_KEY"]=="shared-opensandbox-key"
    assert "sandbox-harness-platform-parallel/.data" not in "\n".join(values.values())


def test_ali_configurator_has_fail_closed_preflights():
    source=(ROOT/"deploy/ali/configure.py").read_text()
    for contract in ("unexpected_checkout","unexpected_branch","port_in_use",
                     "path_outside_data2","digest_mismatch","--check-only","--force"):
        assert contract in source
    assert "SHARED_KEYS" in source
    assert "source.items()" not in source
