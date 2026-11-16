from pathlib import Path
import re
import tomllib


ROOT=Path(__file__).resolve().parents[1]
DATA_ROOT=ROOT/".data"


def deployment_text() -> str:
    paths=[ROOT/".env.example",ROOT/"deploy/configure-local.py",
        ROOT/"deploy/init-postgresql.sh",ROOT/"deploy/install-units.sh",
        ROOT/"deploy/record-release.py",*sorted((ROOT/"deploy/systemd").glob("*.service"))]
    return "\n".join(path.read_text() for path in paths)


def test_parallel_deployment_has_every_authoritative_service():
    expected={
        "sandbox-harness-parallel-api.service",
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


def test_release_requires_complete_signed_profile_catalog_under_data2():
    source=(ROOT/"deploy/record-release.py").read_text()
    assert "publish_profile_catalog" in source
    assert "Profile Catalog image matrix is incomplete" in source
    assert "openssl','pkeyutl','-verify" in source
    assert "root/'.data'/'profile-catalogs'" in source
    assert "SANDBOX_HARNESS_PROFILE_CATALOG_PATH" in source


def test_units_are_hardened_and_recover_without_graceful_shutdown():
    for unit in (ROOT/"deploy/systemd").glob("sandbox-harness-parallel-*.service"):
        text=unit.read_text()
        assert "Restart=on-failure" in text or unit.name.endswith("network.service")
        if "Type=oneshot" not in text:
            assert "NoNewPrivileges=true" in text
            assert "ProtectSystem=strict" in text
            assert "UMask=0077" in text
    worker=(ROOT/"deploy/systemd/sandbox-harness-parallel-job-worker.service").read_text()
    assert "sandbox-harness-parallel-job-worker" in worker
    assert "KillMode=control-group" in worker and "TimeoutStopSec=" in worker
