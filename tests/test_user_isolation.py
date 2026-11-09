from fastapi.testclient import TestClient

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
        thread_b = client.post("/api/threads", json={"harness":"claude", "model":"claude-4.6-sonnet",
            "reasoning_level":"standard", "title":"private-b"}).json()
        response = client.post(f"/api/threads/{thread_b['id']}/turns", json={"message":"try foreign attachment",
            "attachments":[uploaded], "idempotency_key":"foreign-upload-key", "delivery_mode":"start"})
        assert response.status_code == 404

        client.cookies.set(COOKIE, token_a)
        assert [row["id"] for row in client.get("/api/threads").json()] == [thread_a["id"]]


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
