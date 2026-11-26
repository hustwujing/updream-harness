import importlib.util
import pwd
import stat
import subprocess
import sys
from pathlib import Path
import json

import pytest


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
        "SANDBOX_HARNESS_ALIYUN_SANDBOX_API_KEY":"shared-aliyun-key",
        "SANDBOX_HARNESS_ALIYUN_SANDBOX_API_URL":"https://api.cn-shanghai.sandbox.aliyuncs.com",
        "SANDBOX_HARNESS_ALIYUN_SANDBOX_DOMAIN":"cn-shanghai.sandbox.aliyuncs.com",
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
    assert values["SANDBOX_HARNESS_ALIYUN_SANDBOX_API_KEY"]=="shared-aliyun-key"
    assert values["SANDBOX_HARNESS_ALIYUN_SANDBOX_API_URL"].startswith("https://api.cn-shanghai")
    assert values["SANDBOX_HARNESS_ALIYUN_SANDBOX_GATEWAY_URL"]=="http://10.194.49.8:29882"
    assert values["SANDBOX_HARNESS_ALIYUN_SANDBOX_PROXY_URL"]=="http://10.194.49.8:29883"
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


def test_ali_configurator_provisions_private_preview_cache(tmp_path,monkeypatch):
    configure=load("ali_configure_preview",ROOT/"deploy/ali/configure.py")
    source_env=tmp_path/"source.env";source_env.write_text("")
    monkeypatch.setattr(configure,"DATA_ROOT",tmp_path/"data")
    monkeypatch.setattr(configure,"ensure_checkout",lambda:None)
    monkeypatch.setattr(sys,"argv",["configure.py","--source-env",str(source_env),"--allow-running"])
    configure.main()
    preview=configure.DATA_ROOT/"preview-cache";metadata=preview.stat()
    assert preview.is_dir()
    assert metadata.st_uid==pwd.getpwnam("sandbox-harness").pw_uid
    assert stat.S_IMODE(metadata.st_mode)==0o700


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
        "schema_version":2,
        "child_execution_profiles":[],
        "runtime_dependency_profiles":[{
            "id":"primary-full-cpu","supported_harnesses":["codex"],
            "provider_releases":{"opensandbox":{"codex":{
                "ref":"127.0.0.1:25001/sandbox-harness-parallel/primary-full-cpu-codex@sha256:"+"a"*64,
                "revision":"sha256:"+"a"*64,"artifact_digest":"a"*64}}},
        }],
    }
    rewritten=assets.rewrite_profile_catalog(original)
    profile=rewritten["runtime_dependency_profiles"][0]
    release=profile["provider_releases"]["opensandbox"]["codex"]
    assert release["artifact_digest"]=="a"*64
    assert release["ref"].startswith(
        "127.0.0.1:25001/sandbox-harness-platform-parallel-ali/")
    assert "sandbox-harness-parallel/" not in release["ref"]
    assert assets.rewrite_profile_catalog(rewritten)==rewritten


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


def test_ali_bundle_materialization_is_idempotent_in_same_content_store(tmp_path):
    assets=load("ali_assets_same_store",ROOT/"deploy/ali/materialize-assets.py")
    data=b"runtime-bundle"
    digest=__import__("hashlib").sha256(data).hexdigest()
    (tmp_path/(digest+".tar")).write_bytes(data)
    (tmp_path/(digest+".sig")).write_bytes(b"signature")

    assets._copy_bundle(tmp_path,tmp_path,digest)

    assert (tmp_path/(digest+".tar")).read_bytes()==data
    assert (tmp_path/(digest+".sig")).read_bytes()==b"signature"


def test_ali_controlled_runtime_force_does_not_delete_same_store_source(tmp_path,monkeypatch):
    assets=load("ali_assets_same_controlled_store",ROOT/"deploy/ali/materialize-assets.py")
    digest="a"*64
    data_root=tmp_path/"data"
    source=data_root/"controlled-runtime"/digest
    source.mkdir(parents=True)
    payload=source/"runtime.bin"
    payload.write_bytes(b"runtime")
    link=data_root/"provider-runtime"
    link.symlink_to(Path("controlled-runtime")/digest)
    monkeypatch.setattr(assets,"DATA_ROOT",data_root)

    result=assets._materialize_controlled(link,digest,force=True)

    assert result==source
    assert payload.read_bytes()==b"runtime"
    assert link.resolve()==source


def test_ali_materializer_propagates_release_identity_into_independent_checkout():
    assets=load("ali_assets_release_identity",ROOT/"deploy/ali/materialize-assets.py")
    source={
        "SANDBOX_HARNESS_RELEASE":"abc123def456",
        "SANDBOX_HARNESS_WEB_DIST_DIGEST":"f"*64,
        "PYTHONPATH":"/data2/wujing03/sandbox-harness-platform-parallel/src",
    }

    identity=assets.release_identity(source)

    assert identity=={
        "SANDBOX_HARNESS_RELEASE":"abc123def456",
        "SANDBOX_HARNESS_WEB_DIST_DIGEST":"f"*64,
        "PYTHONPATH":str(ALI_ROOT/"src"),
    }


def test_ali_materializer_atomic_environment_replace_preserves_owner_and_mode(tmp_path):
    assets=load("ali_assets_environment_identity",ROOT/"deploy/ali/materialize-assets.py")
    target=tmp_path/"platform.env"
    target.write_text("OLD=value\n")
    target.chmod(0o640)
    before=target.stat()

    assets.atomic_write_environment(target,"NEW=value\n")

    after=target.stat()
    assert target.read_text()=="NEW=value\n"
    assert (after.st_uid,after.st_gid)==(before.st_uid,before.st_gid)
    assert stat.S_IMODE(after.st_mode)==0o640


def test_ali_materializer_signs_rewritten_profile_catalog(tmp_path):
    assets=load("ali_assets_catalog_signature",ROOT/"deploy/ali/materialize-assets.py")
    private=tmp_path/"signing.pem";public=tmp_path/"public.pem"
    subprocess.run(["openssl","genpkey","-algorithm","ED25519","-out",str(private)],check=True)
    with public.open("wb") as stream:
        subprocess.run(["openssl","pkey","-in",str(private),"-pubout"],
            stdout=stream,check=True)
    target=tmp_path/"profile-catalog.json";raw=b'{"schema_version":2}\n'

    assets.write_signed_profile_catalog(target,raw,private,public)

    assert target.read_bytes()==raw
    signature=target.with_suffix(".json.sig")
    assert signature.is_file() and signature.stat().st_size==64
    subprocess.run(["openssl","pkeyutl","-verify","-pubin","-inkey",str(public),
        "-rawin","-in",str(target),"-sigfile",str(signature)],check=True)
    assert not list(tmp_path.glob("*.tmp"))


def test_aliyun_provider_release_overlay_is_complete_and_immutable():
    value=json.loads((ROOT/"deploy/ali/provider-releases.json").read_text())
    assert value["schema_version"]==1 and value["provider"]=="aliyun"
    assert set(value["profiles"])=={
        "primary-full-cpu","general-cpu","office","web-browser","data-analysis","media-cpu"}
    for releases in value["profiles"].values():
        assert set(releases)=={"codex","claude","deepseek"}
        for release in releases.values():
            assert len(release["ref"])==20
            assert release["revision"].startswith("sha256:") and len(release["revision"])==71
            assert len(release["artifact_digest"])==64


def test_ali_materializer_merges_provider_releases_and_fences_artifacts():
    assets=load("ali_assets_merge",ROOT/"deploy/ali/materialize-assets.py")
    digests={"codex":"a"*64,"claude":"b"*64,"deepseek":"c"*64}
    catalog={"schema_version":2,"runtime_dependency_profiles":[{
        "id":"primary-full-cpu","supported_harnesses":list(digests),
        "provider_releases":{"opensandbox":{harness:{"ref":f"image-{harness}",
            "revision":"sha256:"+digest,"artifact_digest":digest}
            for harness,digest in digests.items()}}}]}
    overlay={"schema_version":1,"provider":"aliyun","profiles":{"primary-full-cpu":{
        harness:{"ref":"t"*20,"revision":"sha256:"+digest,"artifact_digest":digest}
        for harness,digest in digests.items()}}}
    merged=assets.merge_provider_releases(catalog,overlay)
    assert set(merged["runtime_dependency_profiles"][0]["provider_releases"])=={
        "opensandbox","aliyun"}
    overlay["profiles"]["primary-full-cpu"]["codex"]["artifact_digest"]="d"*64
    with pytest.raises(ValueError,match="artifact_digest_mismatch"):
        assets.merge_provider_releases(catalog,overlay)


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
    for service in ("postgresql","network","sandbox-gateway-opensandbox","sandbox-gateway-aliyun",
                    "image-inference","job-worker","controlled-worker",
                    "media-worker","site-worker","api"):
        assert f"sandbox-harness-parallel-ali-{service}" in combined
    assert "systemd-run" in start and "--collect" in start
    gateway_positions=(start.index("ali-sandbox-gateway-opensandbox"),start.index("ali-sandbox-gateway-aliyun"))
    assert start.index("ali-postgresql")<start.index("ali-network")<min(gateway_positions)
    assert max(gateway_positions)<start.index("ali-job-worker")<start.index("ali-api")
    assert "/etc/systemd" not in combined
    assert "systemctl enable" not in combined
    assert "/data2/wujing03/sandbox-harness-platform-parallel-ali" in combined
    assert "stop_units" in stop and "network-rule.sh" in stop and " remove" in stop


def test_ali_image_decompose_service_is_materialized_under_data2():
    source=(ROOT/"deploy/ali/materialize-image-decompose.sh").read_text()
    assert 'target="$root/.data/services/image-layered"' in source
    assert "/data2/wujing03/sandbox-harness-platform-parallel-ali" in source
    assert "source_root=/data/wujing03/image-layered" in source
    assert 'model_source="$source_root/models"' in source
    assert "ln -s" in source and "models" in source
    assert 'media_core_source=/data/wujing03/media-foundation-core/media_core' in source
    assert '"$media_core_source" "$target/"' in source
    assert "runtime" in source and "sandbox-harness" in source
    assert "unexpected_project_root" in source


def test_ali_service_runner_uses_only_ali_environment_and_logs():
    source=(ROOT/"deploy/ali/run-service.sh").read_text()
    assert ".data/config/platform.env" in source
    assert ".data/service-logs" in source
    assert "sandbox-harness-platform-parallel/.data" not in source
    assert "/data/wujing03/media-foundation-core" not in source
    assert 'export PYTHONPATH="$service_root:${PYTHONPATH:-}"' in source
    assert "unexpected_project_root" in source
    assert 'sandbox-gateway-opensandbox)' in source and 'SANDBOX_HARNESS_GATEWAY_BIND_HOST="172.31.0.1"' in source
    assert 'sandbox-gateway-aliyun)' in source and 'SANDBOX_HARNESS_GATEWAY_BIND_HOST="10.194.49.8"' in source
    assert 'opensandbox) provider_gateway_host="172.31.0.1"' in source
    assert 'aliyun) provider_gateway_host="10.194.49.8"' in source
    assert 'SANDBOX_HARNESS_GATEWAY_URL="http://${provider_gateway_host}:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"' in source
    assert "tools/service_release.py" in source and "service-current" not in source
    assert "SANDBOX_HARNESS_RELEASE_DIGEST" in source
    assert 'export PYTHONPATH=${release[1]}' in source
    assert '"$root/.venv/bin/sandbox-harness' not in source
    assert "0.0.0.0" not in source


def test_ali_image_builder_uses_only_ali_checkout_and_namespaces():
    source=(ROOT/"runtime/build-images.sh").read_text()
    assert "[[ \"$root\" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]]" in source
    assert "127.0.0.1:25001/sandbox-harness-platform-parallel-ali" in source
    assert "SANDBOX_HARNESS_LOCAL_IMAGE_PREFIX:-sandbox-harness-parallel-ali" in source
    assert 'output_root="$root/.data/image-build/$tag"' in source


def test_ali_transient_runner_passes_service_argument_separately():
    source=(ROOT/"deploy/ali/start.sh").read_text()
    assert '"$root/deploy/ali/run-service.sh" sandbox-gateway-opensandbox' in source
    assert '"$root/deploy/ali/run-service.sh" sandbox-gateway-aliyun' in source
    assert '"$root/deploy/ali/run-service.sh" job-worker' in source
    assert '"$root/deploy/ali/run-service.sh gateway"' not in source


def test_ali_gateway_lifecycle_uses_two_exact_provider_hosts():
    start=(ROOT/"deploy/ali/start.sh").read_text()
    stop=(ROOT/"deploy/ali/stop.sh").read_text()
    status=(ROOT/"deploy/ali/status.sh").read_text()
    for provider,host in (("opensandbox","172.31.0.1"),("aliyun","10.194.49.8")):
        unit=f"sandbox-harness-parallel-ali-sandbox-gateway-{provider}"
        assert unit in start and unit in stop and unit in status
        assert f"wait_port {host} 29882" in start
        assert f"wait_port {host} 29883" in start
        assert host in status
    assert "sandbox-harness-parallel-ali-gateway" not in start+stop+status
    assert 'case "${SANDBOX_HARNESS_BACKEND:-}" in' in start
    assert "provider switch requires deploy/ali/stop.sh first" in start
    assert "Aliyun sandbox routes are not explicitly configured" in start
    assert "selected sandbox backend" in status
    assert "0.0.0.0" not in start+stop+status
