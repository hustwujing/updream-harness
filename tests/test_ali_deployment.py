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
    assert values["SANDBOX_HARNESS_MODEL_CATALOG_SNAPSHOT_PATH"]==str(
        ALI_ROOT/".data/catalogs/model-catalog.json")
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
    assert 'DATA_ROOT/"logs"' in source
    assert 'DATA_ROOT/"image-decompose"' in source


def test_ali_postgresql_scripts_use_dedicated_cluster_under_data2():
    initializer=(ROOT/"deploy/ali/init-postgresql.sh").read_text()
    starter=(ROOT/"deploy/ali/postgresql-start.sh").read_text()
    combined=initializer+starter
    assert "127.0.0.1:28433" in initializer
    assert "sandbox_harness_parallel_ali" in initializer
    assert ".data/postgresql" in combined
    assert ".data/postgresql/run" in combined
    assert ".data/postgresql/postgresql.log" in starter
    assert "/usr/lib/postgresql/16/bin/initdb" in initializer
    assert "/usr/lib/postgresql/16/bin/postgres" in starter
    for forbidden in ("28432","sandbox_harness_parallel ","/var/lib/postgresql","/var/log","mktemp /tmp"):
        assert forbidden not in combined


def test_ali_postgresql_initializer_validates_exact_environment_identity():
    source=(ROOT/"deploy/ali/init-postgresql.sh").read_text()
    assert "SANDBOX_HARNESS_DATABASE_URL" in source
    assert "@127.0.0.1:28433/sandbox_harness_parallel_ali" in source
    assert "unexpected_project_root" in source
    assert "run as root" in source


def test_ali_profile_catalog_rewrite_preserves_digests_and_changes_registry():
    assets=load("ali_assets",ROOT/"deploy/ali/materialize-assets.py")
    original={
        "schema_version":1,
        "child_execution_profiles":[],
        "runtime_dependency_profiles":[{
            "id":"primary-full-cpu","supported_harnesses":["codex"],
            "image_digests":{"codex":"a"*64},
            "image_refs":{"codex":"127.0.0.1:25001/sandbox-harness-parallel/primary-full-cpu-codex@sha256:"+"a"*64},
        }],
    }
    rewritten=assets.rewrite_profile_catalog(original)
    profile=rewritten["runtime_dependency_profiles"][0]
    assert profile["image_digests"]==original["runtime_dependency_profiles"][0]["image_digests"]
    assert profile["image_refs"]["codex"].startswith(
        "127.0.0.1:25001/sandbox-harness-platform-parallel-ali/")
    assert "sandbox-harness-parallel/" not in profile["image_refs"]["codex"]


def test_ali_asset_materializer_owns_all_runtime_paths():
    source=(ROOT/"deploy/ali/materialize-assets.py").read_text()
    for contract in ("runtime-bundles","runtime-code-bundles","profile-catalog.json",
                     "job-catalog.json","controlled-runtime","provider-runtime",
                     "digest_mismatch","promote_registry_manifest","image","inspect"):
        assert contract in source
    assert "sandbox-harness-platform-parallel/.data" not in source
    assert '[docker,"push",target_tag]' in source
    assert "client.put" not in source
    assert "copy_function=os.link" in source


def test_ali_network_rule_is_disjoint_from_parallel_gateway():
    source=(ROOT/"deploy/ali/network-rule.sh").read_text()
    assert "29882:29883" in source
    assert "sandbox-harness-parallel-ali-gateway" in source
    assert "sandbox-harness-parallel-gateway" not in source
    assert "29880:29881" not in source
    assert "sandbox-agent-net" in source


def test_ali_transient_service_graph_is_complete_and_root_partition_free():
    start=(ROOT/"deploy/ali/start.sh").read_text()
    stop=(ROOT/"deploy/ali/stop.sh").read_text()
    status=(ROOT/"deploy/ali/status.sh").read_text()
    runner=(ROOT/"deploy/ali/run-service.sh").read_text()
    combined=start+stop+status+runner
    for service in ("postgresql","network","gateway","job-worker","controlled-worker",
                    "media-worker","site-worker","api"):
        assert f"sandbox-harness-parallel-ali-{service}" in combined
    assert "systemd-run" in start and "--collect" in start
    assert start.index("ali-postgresql")<start.index("ali-network")<start.index("ali-gateway")
    assert start.index("ali-gateway")<start.index("ali-job-worker")<start.index("ali-api")
    assert "/etc/systemd" not in combined
    assert "systemctl enable" not in combined
    assert "/data2/wujing03/sandbox-harness-platform-parallel-ali" in combined
    assert "stop_units" in stop and "network-rule.sh" in stop and " remove" in stop


def test_ali_service_runner_uses_only_ali_environment_and_logs():
    source=(ROOT/"deploy/ali/run-service.sh").read_text()
    assert ".data/config/platform.env" in source
    assert ".data/service-logs" in source
    assert "sandbox-harness-platform-parallel/.data" not in source
    assert "unexpected_project_root" in source


def test_ali_image_builder_uses_only_ali_checkout_and_namespaces():
    source=(ROOT/"runtime/build-images.sh").read_text()
    assert "[[ \"$root\" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]]" in source
    assert "127.0.0.1:25001/sandbox-harness-platform-parallel-ali" in source
    assert "SANDBOX_HARNESS_LOCAL_IMAGE_PREFIX:-sandbox-harness-parallel-ali" in source
    assert 'output_root="$root/.data/image-build/$tag"' in source


def test_ali_transient_runner_passes_service_argument_separately():
    source=(ROOT/"deploy/ali/start.sh").read_text()
    assert '"$root/deploy/ali/run-service.sh" gateway' in source
    assert '"$root/deploy/ali/run-service.sh" job-worker' in source
    assert '"$root/deploy/ali/run-service.sh gateway"' not in source
