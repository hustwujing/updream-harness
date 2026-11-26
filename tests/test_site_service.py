from pathlib import Path
import json

from sandbox_runtime import site_cli


ROOT = Path(__file__).resolve().parents[1]


class Response:
    is_success = True
    text = '{"id":"draft","requires_confirmation":true}'
    def json(self): return {"id":"draft","requires_confirmation":True}


def test_site_worker_is_independent_and_runtime_cli_is_installed():
    runner = (ROOT / "deploy/ali/run-service.sh").read_text()
    module = json.loads((ROOT / "modules/host/site-worker/module.json").read_text())
    project = (ROOT / "pyproject.toml").read_text()
    runtime = (ROOT / "src/sandbox_runtime/instructions.py").read_text()
    assert 'site-worker) command=("$release_python" -m sandbox_harness.services.site_worker.site_service)' in runner
    assert module["id"]=="site-worker" and module["location"]=="host"
    assert 'sandbox-harness-site-worker = "sandbox_harness.services.site_worker.site_service:run"' in project
    assert 'sandbox-site = "sandbox_runtime.site_cli:main"' in project
    assert "sandbox-site prepare" in runtime and "temporary localhost" in runtime


def test_site_cli_validates_packages_and_creates_confirmation_only(tmp_path, monkeypatch, capsys):
    source = tmp_path / "outputs" / "site"; source.mkdir(parents=True)
    (source / "index.html").write_text("<!doctype html><h1>demo</h1>")
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("SANDBOX_SITE_OUTPUT_ROOT", str(tmp_path / "outputs"))
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(site_cli.httpx, "post", lambda url, **kwargs:
        calls.append((url, kwargs)) or Response())
    assert site_cli.main(["prepare", "--source", str(source), "--name", "Demo",
        "--slug", "demo-site", "--idempotency-key", "site-prepare-key"]) == 0
    assert calls[0][0] == "http://gateway.test/v1/sites/drafts"
    assert calls[0][1]["params"] == {"name":"Demo", "slug":"demo-site",
        "idempotency_key":"site-prepare-key"}
    assert calls[0][1]["headers"]["Content-Type"] == "application/x-tar"
    assert "validated_files" in capsys.readouterr().out
