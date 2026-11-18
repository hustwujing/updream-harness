from pathlib import Path
import importlib.util
import re
import tomllib


ROOT=Path(__file__).resolve().parents[1]
DATA_ROOT=ROOT/".data"


def load_configure_local():
    spec=importlib.util.spec_from_file_location("configure_local",ROOT/"deploy/configure-local.py")
    module=importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def deployment_text() -> str:
    paths=[ROOT/".env.example",ROOT/"deploy/configure-local.py",
        ROOT/"deploy/init-postgresql.sh",ROOT/"deploy/install-units.sh",
        ROOT/"deploy/record-release.py",*sorted((ROOT/"deploy/systemd").glob("*.service"))]
    return "\n".join(path.read_text() for path in paths)


def test_parallel_deployment_has_every_authoritative_service():
    expected={
        "sandbox-harness-parallel-api.service",
        "sandbox-harness-parallel-controlled-worker.service",
        "sandbox-harness-parallel-gateway.service",
        "sandbox-harness-parallel-job-worker.service",
        "sandbox-harness-parallel-media-worker.service",
        "sandbox-harness-parallel-network.service",
        "sandbox-harness-parallel-postgresql.service",
        "sandbox-harness-parallel-site-worker.service",
    }
    assert {path.name for path in (ROOT/"deploy/systemd").glob("*.service")}==expected
    scripts=tomllib.loads((ROOT/"pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["sandbox-harness-parallel-job-worker"]=="sandbox_harness.job_service:run"
    assert scripts["sandbox-harness-parallel-controlled-worker"]=="sandbox_harness.controlled_service:run"
    install=(ROOT/"deploy/install-units.sh").read_text()
    for name in expected:
        assert name in install


def test_deployment_writes_only_below_parallel_data_root():
    source=deployment_text()
    allowed=str(DATA_ROOT)
    assert allowed in source
    for forbidden in ("/root/","/var/lib/sandbox-harness","/opt/sandbox-harness",
            "/data/wujing03/miniforge","/data2/wujing03/sandbox-harness-platform/.data",
            "/data2/wujing03/sandbox-harness-platform-plus/.data"):
        assert forbidden not in source
    for unit in (ROOT/"deploy/systemd").glob("*.service"):
        text=unit.read_text()
        for value in re.findall(r"(?:ReadWritePaths=|StandardOutput=append:|StandardError=append:|PGDATA=)(\S+)",text):
            assert value.startswith(allowed),f"{unit.name} writes outside project data root: {value}"


def test_fresh_database_and_shared_services_are_isolated_not_reinstalled():
    environment=(ROOT/".env.example").read_text()
    initializer=(ROOT/"deploy/init-postgresql.sh").read_text()
    installer=(ROOT/"deploy/install-units.sh").read_text()
    assert "127.0.0.1:28432/sandbox_harness_parallel" in environment
    assert "sandbox_harness_parallel" in initializer and "-p 28432" in initializer
    assert "migrate" not in initializer.lower() and "alembic" not in initializer.lower()
    assert "redis-server.service" not in "\n".join(
        path.name for path in (ROOT/"deploy/systemd").glob("*.service"))
    for command in ("apt-get install","docker network create","redis-server --"):
        assert command not in installer


def test_configure_local_rejects_collision_and_non_data2_storage():
    source=(ROOT/"deploy/configure-local.py").read_text()
    assert "SANDBOX_HARNESS_DATA_ROOT" in source
    assert "port_in_use" in source and "identity_collision" in source
    assert "sandbox-harness-parallel:" in source
    assert "sandbox-harness-parallel/" in source
    assert "sandbox-harness-parallel" in source
    assert "/data2/wujing03/sandbox-harness-platform-parallel/.data" in source
    assert "os.path.commonpath" in source
    assert "controlled_runtime_source_required" in source
    assert "build_controlled_runtime_manifest.py" in source


def test_service_account_can_read_descriptors_and_schema_startup_is_serialized():
    installer=(ROOT/"deploy/install-units.sh").read_text()
    assert '"$root/runtime"' in installer
    assert 'u:sandbox-harness:rx "$root/deploy"' in installer
    assert '"$root/deploy/job-catalog.example.json"' in installer
    assert 'find "$root/.venv" -type d -exec setfacl -m d:u:sandbox-harness:rX' in installer
    assert 'import sandbox_harness.main; import defusedxml.ElementTree' in installer
    store=(ROOT/"src/sandbox_harness/store.py").read_text()
    assert "pg_advisory_xact_lock" in store
    assert "SCHEMA_INITIALIZATION_LOCK_ID" in store


def test_service_staging_is_owned_before_each_worker_starts():
    installer=(ROOT/"deploy/install-units.sh").read_text()
    assert '0750 "$root/.data/tmp/runtime-downloads"' in installer
    assert all(name in installer for name in ("child-job-artifacts","job-artifacts",
        "operation-job-artifacts","provider-job-artifacts","provider-outputs","site-stages",
        "controlled-job-artifacts","controlled-operations"))
    runtime=("ExecStartPre=+/usr/bin/install -d -o sandbox-harness -g sandbox-harness -m 0750 "
        "/data2/wujing03/sandbox-harness-platform-parallel/.data/tmp/runtime-downloads")
    for name in ("sandbox-harness-parallel-api.service","sandbox-harness-parallel-job-worker.service"):
        assert runtime in (ROOT/"deploy/systemd"/name).read_text()
    ownership={"sandbox-harness-parallel-job-worker.service":("child-job-artifacts",
        "operation-job-artifacts","provider-job-artifacts"),
        "sandbox-harness-parallel-gateway.service":("job-artifacts","site-stages"),
        "sandbox-harness-parallel-media-worker.service":("provider-outputs",),
        "sandbox-harness-parallel-site-worker.service":("site-stages",)}
    for name,directories in ownership.items():
        source=(ROOT/"deploy/systemd"/name).read_text()
        assert all(".data/tmp/"+directory in source for directory in directories)


def test_configure_local_inherits_provider_configuration_but_not_storage_identity():
    module=load_configure_local()
    route_key="CUSTOM_MODEL_ROUTE_KEY"
    source={
        "SANDBOX_HARNESS_UPSTREAM_IMAGE":"https://provider.example/v1",
        "SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY":"provider-secret",
        "SANDBOX_HARNESS_OBJECT_PUBLIC_ENDPOINT":"https://objects.example",
        "SANDBOX_HARNESS_OBJECT_PREFIX":"predecessor/",
        "SANDBOX_HARNESS_DATABASE_URL":"postgresql://predecessor",
        "SANDBOX_HARNESS_MODEL_ROUTES_JSON":(
            "'{\"custom/model\":{\"credential_env\":\""+route_key+"\"}}'"),
        route_key:"route-secret",
        "UNREFERENCED_SECRET":"must-not-be-copied",
    }
    values=dict(module.EXPECTED)
    module.copy_shared_configuration(values,source)
    assert values["SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY"]=="provider-secret"
    assert values["SANDBOX_HARNESS_OBJECT_PUBLIC_ENDPOINT"]=="https://objects.example"
    assert values[route_key]=="route-secret"
    assert values["SANDBOX_HARNESS_OBJECT_PREFIX"]=="sandbox-harness-parallel/"
    assert "SANDBOX_HARNESS_DATABASE_URL" not in values
    assert "UNREFERENCED_SECRET" not in values


def test_configure_local_rejects_unsafe_dynamic_credential_names():
    module=load_configure_local()
    unsafe='{\"route\":{\"credential_env\":\"BAD-NAME\"}}'
    try:
        module.model_route_credential_keys(unsafe)
    except ValueError as exc:
        assert str(exc)=="invalid_model_route_credential_env"
    else:
        raise AssertionError("unsafe credential env accepted")


def test_release_requires_complete_signed_profile_catalog_under_data2():
    source=(ROOT/"deploy/record-release.py").read_text()
    assert "publish_profile_catalog" in source
    assert "Profile Catalog image matrix is incomplete" in source
    assert "openssl','pkeyutl','-verify" in source
    assert "root/'.data'/'profile-catalogs'" in source
    assert "SANDBOX_HARNESS_PROFILE_CATALOG_PATH" in source
    assert "grant_service_release_read_access" in source
    assert "u:sandbox-harness:rX" in source


def test_units_are_hardened_and_recover_without_graceful_shutdown():
    for unit in (ROOT/"deploy/systemd").glob("sandbox-harness-parallel-*.service"):
        text=unit.read_text()
        assert "Restart=on-failure" in text or unit.name.endswith("network.service")
        if "Type=oneshot" not in text:
            if unit.name.endswith("controlled-worker.service"):
                assert "NoNewPrivileges=false" in text
                assert "RestrictSUIDSGID=false" in text
            else:
                assert "NoNewPrivileges=true" in text
            assert "ProtectSystem=strict" in text
            assert "UMask=0077" in text
    worker=(ROOT/"deploy/systemd/sandbox-harness-parallel-job-worker.service").read_text()
    assert "sandbox-harness-parallel-job-worker" in worker
    assert "KillMode=control-group" in worker and "TimeoutStopSec=" in worker
    controlled=(ROOT/"deploy/systemd/sandbox-harness-parallel-controlled-worker.service").read_text()
    assert "User=root" in controlled and "sandbox-harness-parallel-controlled-worker" in controlled
    assert ".data/provider-runtime" in controlled and ".data/controlled-operations" in controlled
    assert "ReadWritePaths=/data2/wujing03/sandbox-harness-platform-parallel/.data" in controlled
    assert "LockPersonality=true" not in controlled
    assert "DevicePolicy=closed" in controlled
    assert "DeviceAllow=char-nvidia rw" in controlled
    assert "DeviceAllow=char-nvidia-uvm rw" in controlled
    assert "/dev/nvidia0" not in controlled
