from pathlib import Path

from sandbox_runtime import media_cli


ROOT = Path(__file__).resolve().parents[1]


class Response:
    is_success = True
    text = '{"ok":true}'


def test_media_worker_is_a_separate_service():
    app = (ROOT / "src/sandbox_harness/app.py").read_text()
    unit = (ROOT / "deploy/systemd/sandbox-harness-media-worker.service").read_text()
    project = (ROOT / "pyproject.toml").read_text()
    install = (ROOT / "deploy/install-units.sh").read_text()
    assert "MediaWorker(" not in app
    assert "ExecStart=/data2/wujing03/sandbox-harness-platform/.venv/bin/sandbox-harness-media-worker" in unit
    assert 'sandbox-harness-media-worker = "sandbox_harness.media_service:run"' in project
    assert "sandbox-harness-media-worker.service" in install


def test_media_cli_prepare_has_no_network_side_effect(monkeypatch, capsys):
    monkeypatch.setattr(media_cli.httpx, "get", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(media_cli.httpx, "post", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    assert media_cli.main(["prepare", "image", "--model", "seedream-5.0",
        "--parameters", '{"prompt":"test"}']) == 0
    assert '"requires_confirmation": true' in capsys.readouterr().out


def test_media_cli_status_uses_get_and_cancel_uses_post(tmp_path, monkeypatch):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_MEDIA_API_URL", "http://media.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "get", lambda url, **kwargs: calls.append(("GET", url)) or Response())
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs: calls.append(("POST", url)) or Response())
    assert media_cli.main(["status", "--job-id", "job-1"]) == 0
    assert media_cli.main(["cancel", "--job-id", "job-1"]) == 0
    assert calls == [
        ("GET", "http://media.test/api/media/jobs/job-1"),
        ("POST", "http://media.test/api/media/jobs/job-1/cancel"),
    ]
