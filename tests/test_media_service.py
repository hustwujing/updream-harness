from pathlib import Path
import io

from fastapi.testclient import TestClient
from PIL import Image

from sandbox_harness.app import create_app
from sandbox_harness.config import Settings
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
    assert "Never copy a role name into usage" in instructions and "usage=composition" in instructions
    assert "never reuse a Job ID from another Thread" in instructions
    assert "Never use `sandbox-interact approval` as a replacement" in instructions
    assert "non-empty Job ID" in instructions
    assert "one short public progress sentence" in instructions
    assert "current files are authoritative" in instructions
    assert "compatibility location only" in instructions
    assert "Markdown link" in instructions
    assert "current-Turn publication" in instructions and "verify every input path exists" in instructions
    assert "setsar=1" in instructions
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
        "references": [], "idempotency_key": "prepare-key-1", "output_role": "workspace",
    })]
    assert '{"ok":true}' in capsys.readouterr().out


def test_media_cli_prepares_tripo_draft_without_provider_fields(tmp_path, monkeypatch):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        calls.append(kwargs["json"]) or Response())
    assert media_cli.main(["prepare", "model3d", "--model", "v3.1-20260211",
        "--generation-mode", "text_to_model", "--parameters",
        '{"prompt":"a museum-ready bronze dolphin","texture":true,"pbr":true}',
        "--output-role", "workspace"]) == 0
    assert calls[-1]["kind"] == "model3d" and calls[-1]["generation_mode"] == "text_to_model"
    assert set(calls[-1]["parameters"]) == {"prompt", "texture", "pbr"}


def test_public_media_catalog_exposes_tripo_availability(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///"+str(tmp_path/"catalog.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key="media-catalog-secret", tripo_api_key="configured")
    with TestClient(create_app(settings)) as client:
        response = client.get("/api/catalog/media", params={"kind": "model3d"})
        assert response.status_code == 200
        model = response.json()["models"][0]
        assert model["model"] == "v3.1-20260211" and model["available"] is True
        assert "credential_env" not in model


def test_media_cli_normalizes_durable_reference_path(tmp_path, monkeypatch):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        calls.append(kwargs["json"]) or Response())
    assert media_cli.main(["prepare", "video", "--model", "sed2",
        "--generation-mode", "first_frame", "--parameters", '{"prompt":"move","duration":5}',
        "--reference", '{"project_path":"/workspace/project/assets/start.png","role":"first_frame","usage":"composition"}']) == 0
    assert calls[0]["generation_mode"] == "first_frame"
    assert calls[0]["references"] == [{"project_path":"assets/start.png",
        "role":"first_frame", "usage":"composition"}]


def test_media_cli_rejects_invalid_usage_and_provider_native_parameters_locally(capsys):
    import pytest
    with pytest.raises(SystemExit):
        media_cli.main(["prepare","video","--model","sed2","--generation-mode","first_frame",
            "--parameters",'{"prompt":"move"}',"--reference",
            '{"project_path":"assets/start.png","role":"first_frame","usage":"first_frame"}'])
    assert "references.0.usage" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        media_cli.main(["prepare","audio","--model","speech-2.8-turbo","--parameters",
            '{"text":"hello","voice_setting":{"voice_id":"x"},"language_boost":"Chinese"}'])
    error=capsys.readouterr().err
    assert "voice_setting" in error and "voice_id" in error


def test_media_cli_canonicalizes_edit_and_extend_provider_constraints(tmp_path, monkeypatch):
    token = tmp_path / "token"; token.write_text("x" * 32)
    monkeypatch.setenv("HARNESS_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("HARNESS_CAPABILITY_TOKEN_FILE", str(token))
    calls = []
    monkeypatch.setattr(media_cli.httpx, "post", lambda url, **kwargs:
        calls.append(kwargs["json"]) or Response())
    common = ["prepare", "video", "--model", "sed2", "--output-role", "workspace",
        "--reference", '{"project_path":"artifacts/source.mp4","role":"reference_video","usage":"edit_source"}']
    assert media_cli.main([*common, "--generation-mode", "edit_video", "--parameters",
        '{"prompt":"fix motion","duration":10,"aspect_ratio":"16:9"}']) == 0
    assert calls[-1]["parameters"]["duration"] == -1
    assert calls[-1]["parameters"]["aspect_ratio"] == "adaptive"
    extend = ["prepare", "video", "--model", "sed2", "--output-role", "workspace",
        "--generation-mode", "extend_video",
        "--reference", '{"project_path":"artifacts/source.mp4","role":"reference_video","usage":"extend_source"}',
        "--parameters", '{"prompt":"continue","duration":4,"aspect_ratio":"16:9","extend_direction":"forward"}']
    assert media_cli.main(extend) == 0
    assert calls[-1]["parameters"]["aspect_ratio"] == "adaptive"


def test_media_reference_api_freezes_owned_project_file_snapshot(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///"+str(tmp_path/"api.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key="media-reference-api-secret",ark_access_key="test-ak",ark_secret_key="test-sk",
        ark_asset_group_id="group-test")
    image=Image.new("RGB",(640,480),"cyan");buffer=io.BytesIO();image.save(buffer,format="PNG")
    with TestClient(create_app(settings)) as client:
        first=client.post("/api/threads",json={"harness":"codex","model":"gpt-5.6",
            "reasoning_level":"standard","title":"Reference API"}).json()
        second=client.post("/api/threads",json={"harness":"codex","model":"gpt-5.6",
            "reasoning_level":"standard","title":"Other thread"}).json()
        project=client.post(f"/api/threads/{first['id']}/project-files?path=assets/start.png&mime=image/png",
            content=buffer.getvalue()).json()
        payload={"kind":"video","model":"sed2","generation_mode":"first_frame",
            "parameters":{"prompt":"water begins moving","duration":5,"aspect_ratio":"adaptive"},
            "references":[{"project_file_id":project["id"],"role":"first_frame","usage":"composition"}],
            "idempotency_key":"reference-api-draft","output_role":"workspace"}
        forbidden=client.post(f"/api/threads/{second['id']}/media/drafts",json=payload)
        assert forbidden.status_code==422 and "missing, stale" in forbidden.json()["detail"]
        draft=client.post(f"/api/threads/{first['id']}/media/drafts",json=payload)
        assert draft.status_code==200
        value=draft.json();assert value["generation_mode"]=="first_frame"
        expires=__import__('datetime').datetime.fromisoformat(value["expires_at"])
        assert 23*3600 < (expires-__import__('datetime').datetime.now(__import__('datetime').timezone.utc)).total_seconds() <= 24*3600
        assert value["references"][0]["project_file_id"]==project["id"]
        assert value["references"][0]["width"]==640 and "object_ref" not in value["references"][0]
        assert client.delete(f"/api/project-files/{project['id']}").status_code==200
        confirmed=client.post(f"/api/media/drafts/{value['id']}/confirm")
        assert confirmed.status_code==200 and confirmed.json()["references"][0]["sha256"]==project["sha256"]


def test_media_reference_presigned_upload_and_completion_are_owner_scoped(tmp_path, monkeypatch):
    image=Image.new("RGB",(640,480),"cyan");buffer=io.BytesIO();image.save(buffer,format="PNG")
    content=buffer.getvalue();digest=__import__('hashlib').sha256(content).hexdigest()
    class Objects:
        mismatch=False
        def prepare_media_upload(self, *, kind, digest, mime, size, expires_seconds):
            key=f"aniforge/{kind}/{digest[:2]}/{digest}.png"
            return {"object_key":key,"public_url":"https://boss-shjd.biliapi.net/updream/"+key,
                "put_url":"https://boss-shjd.biliapi.net/signed-put","headers":{"Content-Type":mime,
                    "x-amz-meta-sha256":digest,"x-amz-meta-size":str(size),"x-amz-meta-kind":kind}}
        def head_media_upload(self, key):
            return {"size":999 if self.mismatch else len(content),"mime":"image/png",
                "metadata":{"sha256":digest,"size":str(len(content)),"kind":"image"}}
        def verify_media_upload(self, key, *, digest, expected_size): return None
        def promote_media_upload(self, key, digest): return "sha256:"+digest
        def put(self,data): return "sha256:"+"0"*64,"0"*64
        def get(self,*args,**kwargs): return content
    objects=Objects();monkeypatch.setattr("sandbox_harness.app.object_store",lambda settings:objects)
    settings=Settings(data_root=tmp_path,database_url="sqlite+aiosqlite:///"+str(tmp_path/"upload.db"),
        redis_url="",backend="fake",object_backend="local",model_catalog_discovery=False,
        session_signing_key="media-upload-session-secret")
    with TestClient(create_app(settings)) as client:
        thread=client.post("/api/threads",json={"harness":"codex","model":"gpt-5.6",
            "reasoning_level":"standard","title":"Upload"}).json()
        ticket=client.post(f"/api/threads/{thread['id']}/media/reference-uploads",json={
            "name":"reference.png","kind":"image","mime":"image/png","size":len(content),"sha256":digest})
        assert ticket.status_code==200
        value=ticket.json();assert value["object_key"].startswith("aniforge/image/")
        assert value["public_url"].startswith("https://boss-shjd.biliapi.net/updream/aniforge/image/")
        assert value["upload"]["method"]=="PUT" and "shjd-boss.bilibili.co" not in str(value)
        saved_cookies=dict(client.cookies);client.cookies.clear()
        assert client.post(f"/api/media/reference-uploads/{value['id']}/complete").status_code==404
        client.cookies.update(saved_cookies)
        completed=client.post(f"/api/media/reference-uploads/{value['id']}/complete")
        assert completed.status_code==200 and completed.json()["status"]=="completed"
        assert completed.json()["project_file_id"]
        repeated=client.post(f"/api/media/reference-uploads/{value['id']}/complete")
        assert repeated.status_code==200 and repeated.json()["project_file_id"]==completed.json()["project_file_id"]


def test_media_reference_upload_rejects_boss_metadata_mismatch(tmp_path, monkeypatch):
    class Objects:
        def prepare_media_upload(self, *, kind, digest, mime, size, expires_seconds):
            return {"object_key":"aniforge/image/11/file.png",
                "public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/11/file.png",
                "put_url":"https://boss-shjd.biliapi.net/signed-put","headers":{}}
        def head_media_upload(self, key):
            return {"size":99,"mime":"image/png","metadata":{"sha256":"1"*64,"size":"100","kind":"image"}}
        def put(self,data): return "sha256:"+"0"*64,"0"*64
        def get(self,*args,**kwargs): return b""
    monkeypatch.setattr("sandbox_harness.app.object_store",lambda settings:Objects())
    settings=Settings(data_root=tmp_path,database_url="sqlite+aiosqlite:///"+str(tmp_path/"mismatch.db"),
        redis_url="",backend="fake",object_backend="local",model_catalog_discovery=False,
        session_signing_key="media-mismatch-session-secret")
    with TestClient(create_app(settings)) as client:
        thread=client.post("/api/threads",json={"harness":"codex","model":"gpt-5.6",
            "reasoning_level":"standard","title":"Mismatch"}).json()
        ticket=client.post(f"/api/threads/{thread['id']}/media/reference-uploads",json={
            "name":"reference.png","kind":"image","mime":"image/png","size":100,"sha256":"1"*64}).json()
        response=client.post(f"/api/media/reference-uploads/{ticket['id']}/complete")
        assert response.status_code==422 and "metadata does not match" in response.json()["detail"]




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
