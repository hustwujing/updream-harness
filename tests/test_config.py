from sandbox_harness.config import Settings


def test_opensandbox_environment_contract(monkeypatch):
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_URL", "http://opensandbox:8080")
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_API_KEY", "secret-key")

    settings = Settings(_env_file=None)

    assert settings.opensandbox_url == "http://opensandbox:8080"
    assert settings.opensandbox_api_key == "secret-key"
