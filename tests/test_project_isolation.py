from pathlib import Path

import tomllib

from sandbox_harness.config import Settings


ROOT = Path(__file__).resolve().parents[1]
OLD_ROOT = "/data2/wujing03/sandbox-harness-platform/"


def settings_without_environment(monkeypatch) -> Settings:
    for field in Settings.model_fields:
        monkeypatch.delenv("SANDBOX_HARNESS_" + field.upper(), raising=False)
    return Settings(_env_file=None)


def test_parallel_project_defaults_are_isolated(monkeypatch):
    settings = settings_without_environment(monkeypatch)
    assert settings.api_bind_port == 24766
    assert settings.namespace == "sandbox-harness-parallel"
    assert settings.redis_prefix == "sandbox-harness-parallel:"
    assert settings.object_prefix == "sandbox-harness-parallel/"
    assert settings.gateway_bind_host == "172.31.0.1"
    assert settings.gateway_bind_port == 29880
    assert settings.gateway_proxy_port == 29881
    assert settings.site_public_base_path == "/sandbox-harness-platform-parallel/sites"


def test_parallel_distribution_and_commands_have_independent_identity():
    value = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert value["project"]["name"] == "sandbox-harness-platform-parallel"
    scripts = value["project"]["scripts"]
    assert "sandbox-harness-parallel-api" in scripts
    assert "sandbox-harness-parallel-gateway" in scripts
    assert "sandbox-harness-parallel-media-worker" in scripts
    assert "sandbox-harness-parallel-site-worker" in scripts


def test_parallel_deployment_files_cannot_target_predecessor_state():
    unit_root = ROOT / "deploy/systemd"
    expected = {
        "sandbox-harness-parallel-api.service",
        "sandbox-harness-parallel-gateway.service",
        "sandbox-harness-parallel-media-worker.service",
        "sandbox-harness-parallel-network.service",
        "sandbox-harness-parallel-postgresql.service",
        "sandbox-harness-parallel-site-worker.service",
    }
    assert {item.name for item in unit_root.glob("*.service")} == expected

    deployment_files = [
        *unit_root.glob("*.service"),
        ROOT / "deploy/install-units.sh",
        ROOT / "deploy/init-postgresql.sh",
        ROOT / "deploy/check-service-access.sh",
        ROOT / "deploy/network-rule.sh",
        ROOT / "deploy/nginx.conf",
        ROOT / "deploy/nginx-frpc.conf",
        ROOT / "deploy/record-release.py",
    ]
    combined = "\n".join(path.read_text() for path in deployment_files)
    assert OLD_ROOT not in combined
    assert "sandbox-harness-api.service" not in combined
    assert "sandbox-harness-gateway.service" not in combined
    assert "sandbox-harness-postgresql.service" not in combined
    assert "/data2/wujing03/sandbox-harness-platform-parallel/" in combined
    assert "-p 28432" in combined
    assert "sandbox-harness-platform-parallel" in combined


def test_parallel_nginx_uses_unique_prefix_and_api_port():
    direct = (ROOT / "deploy/nginx.conf").read_text()
    frpc = (ROOT / "deploy/nginx-frpc.conf").read_text()
    for value in (direct, frpc):
        assert "/sandbox-harness-platform-parallel" in value
        assert "proxy_pass http://127.0.0.1:24766/;" in value
        assert "/sandbox-harness/" not in value


def test_service_persistent_storage_stays_under_data2_project_root():
    allowed_root = "/data2/wujing03/sandbox-harness-platform-parallel/.data"
    deployment = "\n".join(
        path.read_text()
        for path in [
            ROOT / ".env.example",
            ROOT / "deploy/install-units.sh",
            ROOT / "deploy/init-postgresql.sh",
            *sorted((ROOT / "deploy/systemd").glob("*.service")),
        ]
    )
    assert allowed_root in deployment
    for forbidden in ("/root/", "/var/lib/sandbox-harness", "/opt/sandbox-harness"):
        assert forbidden not in deployment
    assert "root-partition storage is forbidden" in deployment
