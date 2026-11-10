from fastapi.testclient import TestClient
import httpx

from sandbox_harness.app import create_app
from sandbox_harness.config import Settings
from sandbox_harness.sessions import COOKIE, Sessions


def test_threads_and_uploads_are_scoped_to_signed_browser_owner(tmp_path):
    secret = "isolated-browser-session-secret"
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "isolation.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key=secret)
    sessions = Sessions(secret)
    _, token_a, _ = sessions.resolve(None)
    _, token_b, _ = sessions.resolve(None)
    with TestClient(create_app(settings)) as client:
        client.cookies.set(COOKIE, token_a)
        response = client.post("/api/threads", json={"harness":"claude", "model":"claude-4.6-sonnet",
            "reasoning_level":"standard", "title":"private-a"})
        assert response.status_code == 200
        thread_a = response.json()
        uploaded = client.post("/api/uploads?name=private.txt&mime=text/plain", content=b"private-a").json()

        client.cookies.set(COOKIE, token_b)
        assert client.get("/api/threads").json() == []
        assert client.get(f"/api/threads/{thread_a['id']}").status_code == 404
        assert client.delete(f"/api/threads/{thread_a['id']}").status_code == 404
        thread_b = client.post("/api/threads", json={"harness":"claude", "model":"claude-4.6-sonnet",
            "reasoning_level":"standard", "title":"private-b"}).json()
        response = client.post(f"/api/threads/{thread_b['id']}/turns", json={"message":"try foreign attachment",
            "attachments":[uploaded], "idempotency_key":"foreign-upload-key", "delivery_mode":"start"})
        assert response.status_code == 404

        client.cookies.set(COOKIE, token_a)
        assert [row["id"] for row in client.get("/api/threads").json()] == [thread_a["id"]]
        assert client.delete(f"/api/threads/{thread_a['id']}").status_code == 204
        assert client.get("/api/threads").json() == []


def test_project_file_mutations_are_owner_scoped_versioned_and_path_checked(tmp_path):
    secret = "project-file-owner-secret"
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "files.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key=secret)
    sessions = Sessions(secret); _, token_a, _ = sessions.resolve(None); _, token_b, _ = sessions.resolve(None)
    with TestClient(create_app(settings)) as client:
        client.cookies.set(COOKIE, token_a)
        thread = client.post("/api/threads", json={"harness":"claude", "model":"claude-4.6-sonnet",
            "reasoning_level":"standard", "title":"files"}).json()
        response = client.post(f"/api/threads/{thread['id']}/project-files",
            params={"path":"assets/reference.txt", "mime":"text/plain"}, content=b"one")
        assert response.status_code == 200; first = response.json()
        assert "object_ref" not in first
        preview = client.get(f"/api/project-files/{first['id']}?inline=true")
        assert preview.content == b"one" and preview.headers["content-disposition"].startswith("inline;")
        assert client.post(f"/api/threads/{thread['id']}/project-files",
            params={"path":"../escape"}, content=b"x").status_code == 422
        client.cookies.set(COOKIE, token_b)
        assert client.get(f"/api/threads/{thread['id']}/project-files").status_code == 404
        assert client.delete(f"/api/project-files/{first['id']}").status_code == 404
        client.cookies.set(COOKIE, token_a)
        renamed = client.post(f"/api/project-files/{first['id']}/rename",
            json={"path":"assets/renamed.txt"})
        assert renamed.status_code == 200
        history = client.get(f"/api/threads/{thread['id']}/project-files/history",
            params={"path":"assets/reference.txt"}).json()
        assert history[0]["deleted"] is True
        assert client.delete(f"/api/project-files/{renamed.json()['id']}").status_code == 200
        assert client.get(f"/api/threads/{thread['id']}/project-files").json() == []


def test_upload_names_dispositions_inline_policy_and_body_limits(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "downloads.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key="download-policy-secret", max_attachment_bytes=64)
    with TestClient(create_app(settings)) as client:
        for name in ("..", "x\ny.txt", 'a"b.txt', "a\\b.txt"):
            assert client.post("/api/uploads", params={"name":name,"mime":"text/plain"}, content=b"x").status_code == 400
        assert client.post("/api/uploads", params={"name":"large.bin"},
            headers={"Content-Length":"65"}, content=b"").status_code == 413
        thread = client.post("/api/threads", json={"harness":"codex", "model":"gpt-5.6",
            "reasoning_level":"standard", "title":"downloads"}).json()
        created = client.post(f"/api/threads/{thread['id']}/project-files",
            params={"path":"assets/报告.txt", "mime":"text/plain"}, content=b"hello").json()
        response = client.get(f"/api/project-files/{created['id']}?inline=true")
        assert response.status_code == 200 and "filename*=UTF-8''" in response.headers["content-disposition"]
        html = client.post(f"/api/threads/{thread['id']}/project-files",
            params={"path":"assets/page.html", "mime":"text/html"}, content=b"<script>alert(1)</script>").json()
        unsafe = client.get(f"/api/project-files/{html['id']}?inline=true")
        assert unsafe.headers["content-disposition"].startswith("attachment;")
        assert unsafe.headers["content-type"].startswith("application/octet-stream")
        assert unsafe.headers["content-security-policy"].startswith("sandbox;")


def test_append_input_delivery_error_is_controlled_and_visible(tmp_path):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "input-delivery.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key="input-delivery-secret")
    app = create_app(settings)

    async def failing_delivery(value):
        raise ValueError("attachment verification failed")

    app.state.broker.try_deliver_input = failing_delivery
    with TestClient(app, raise_server_exceptions=False) as client:
        thread = client.post("/api/threads", json={"harness":"claude", "model":"claude-4.6-sonnet",
            "reasoning_level":"standard", "title":"input failure"}).json()
        turn = client.post(f"/api/threads/{thread['id']}/turns", json={"message":"initial",
            "idempotency_key":"input-delivery-turn", "delivery_mode":"queue"}).json()
        response = client.post(f"/api/turns/{turn['id']}/inputs", json={"message":"follow up",
            "idempotency_key":"input-delivery-failure"})
        assert response.status_code == 409
        assert response.json()["detail"] == "input_delivery_failed"
        inputs = client.get(f"/api/turns/{turn['id']}/inputs").json()
        assert inputs[-1]["status"] == "failed"
        items = client.get(f"/api/threads/{thread['id']}/items").json()
        assert items[-1]["summary"] == "follow up" and items[-1]["status"] == "failed"


def test_slow_model_probe_does_not_block_api_startup(tmp_path, monkeypatch):
    import asyncio
    async def unavailable(self):
        await asyncio.Event().wait()
    monkeypatch.setattr('sandbox_harness.model_discovery.ModelDiscovery.refresh', unavailable)
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'startup.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=True,
        session_signing_key='startup-browser-session-secret')
    with TestClient(create_app(settings)) as client:
        assert client.get('/api/health').status_code==200
        assert client.get('/api/catalog/harnesses').status_code==200
