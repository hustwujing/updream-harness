from pathlib import Path

from sandbox_runtime import media_cli


ROOT = Path(__file__).resolve().parents[1]


class Response:
    is_success = True
    text = '{"ok":true}'


def test_media_worker_is_a_separate_service():
    app = (ROOT / "src/sandbox_harness/app.py").read_text()
    worker = (ROOT / "src/sandbox_harness/media_worker.py").read_text()
    unit = (ROOT / "deploy/systemd/sandbox-harness-media-worker.service").read_text()
    project = (ROOT / "pyproject.toml").read_text()
    base = (ROOT / "runtime/base.Dockerfile").read_text()
    install = (ROOT / "deploy/install-units.sh").read_text()
    assert "MediaWorker(" not in app
    assert "continuation_callback" not in worker
    assert "ExecStart=/data2/wujing03/sandbox-harness-platform/.venv/bin/sandbox-harness-media-worker" in unit
    assert 'sandbox-harness-media-worker = "sandbox_harness.media_service:run"' in project
    assert "sandbox-harness-media-worker.service" in install
    for command in ("sandbox-harness-api", "sandbox-harness-gateway", "sandbox-harness-media-worker"):
        assert f"/usr/local/bin/{command}" in base
    instructions = (ROOT / "src/sandbox_runtime/instructions.py").read_text()
    adapters = (ROOT / "src/sandbox_runtime/adapters.py").read_text()
    codex = (ROOT / "src/sandbox_runtime/codex_runner.py").read_text()
    deepseek = (ROOT / "src/sandbox_runtime/deepseek_runner.py").read_text()
    assert "sandbox-media prepare" in instructions and "sandbox-image" in instructions
    assert "--append-system-prompt" in adapters
    assert '"developerInstructions": RUNTIME_INSTRUCTIONS' in codex
    assert 'RUNTIME_INSTRUCTIONS + "\\nUser request:' in deepseek


def test_media_cli_prepare_creates_confirmation_draft_only(tmp_path, monkeypatch, capsys):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        calls.append((url, kwargs["json"])) or Response())
    assert media_cli.main(["prepare", "image", "--model", "seedream-5.0",
        "--parameters", '{"prompt":"test"}', "--idempotency-key", "prepare-key-1",
        "--output-role", "workspace"]) == 0
    assert calls == [("http://gateway.test/v1/media/drafts", {
        "kind": "image", "model": "seedream-5.0", "parameters": {"prompt": "test"},
        "idempotency_key": "prepare-key-1", "output_role": "workspace",
    })]
    assert '{"ok":true}' in capsys.readouterr().out


def test_media_cli_commit_seals_complete_batch(tmp_path, monkeypatch, capsys):
    from sandbox_runtime import media_cli
    token = tmp_path / "token"
    token.write_text("capability")
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    class Response:
        is_success = True
        text = '{"status":"sealed","draft_count":3}'
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        calls.append((url, kwargs)) or Response())
    assert media_cli.main(["commit"]) == 0
    assert calls[0][0] == "http://gateway.test/v1/media/batch/commit"
    assert calls[0][1]["headers"] == {"Authorization": "Bearer capability"}
    assert '"status":"sealed"' in capsys.readouterr().out


def test_media_cli_fetch_and_promote_candidate(tmp_path, monkeypatch, capsys):
    from sandbox_runtime import media_cli
    token = tmp_path / "token"
    token.write_text("capability")
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    class Response:
        def __init__(self, *, content=b"", text="{}"):
            self.is_success = True; self.content = content; self.text = text
    monkeypatch.setattr(media_cli.httpx, "get", lambda *args, **kwargs: Response(content=b"candidate"))
    posts = []
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        posts.append((url, kwargs)) or Response(text='{"output_role":"delivery"}'))
    output = tmp_path / "candidate.png"
    assert media_cli.main(["fetch", "--job-id", "job-1", "--output", str(output)]) == 0
    assert output.read_bytes() == b"candidate"
    assert media_cli.main(["promote", "--job-id", "job-1", "--output-role", "delivery"]) == 0
    assert posts == [("http://gateway.test/v1/media/jobs/job-1/promote", {
        "headers": {"Authorization": "Bearer capability"},
        "json": {"output_role": "delivery"}, "timeout": 20,
    })]
    assert '"output_role":"delivery"' in capsys.readouterr().out


def test_media_cli_status_uses_get_and_cancel_uses_post(tmp_path, monkeypatch):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "get", lambda url, **kwargs: calls.append(("GET", url)) or Response())
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs: calls.append(("POST", url)) or Response())
    assert media_cli.main(["catalog", "--kind", "image"]) == 0
    assert media_cli.main(["status", "--job-id", "job-1"]) == 0
    assert media_cli.main(["cancel", "--job-id", "job-1"]) == 0
    assert calls == [
        ("GET", "http://gateway.test/v1/media/catalog"),
        ("GET", "http://gateway.test/v1/media/jobs/job-1"),
        ("POST", "http://gateway.test/v1/media/jobs/job-1/cancel"),
    ]
