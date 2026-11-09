from pathlib import Path

from fastapi.testclient import TestClient

import sandbox_runtime.app as runtime_app


def test_runtime_configuration_freezes_turn_harness_and_model(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    app = runtime_app.create_app()

    with TestClient(app) as client:
        response = client.post("/v1/configure", json={
            "capability_token": "x" * 32,
            "gateway_url": "http://gateway:28600",
            "harness": "deepseek",
            "model": "deepseek-v4.1",
            "proxy_url": "http://172.31.0.1:28601",
        })

    assert response.status_code == 200
    assert runtime_app.os.environ["HARNESS_KIND"] == "deepseek"
    assert runtime_app.os.environ["HARNESS_MODEL"] == "deepseek-v4.1"
    assert runtime_app.os.environ["HTTPS_PROXY"].startswith("http://" + "x" * 32 + ":@")
    assert "172.31.0.1" in runtime_app.os.environ["NO_PROXY"]
    assert (tmp_path / "run" / "capability-token").read_text() == "x" * 32
