from sandbox_harness.config import Settings, validate_service_settings
import pytest
from sandbox_harness.model_routes import ModelRoutes


def test_opensandbox_environment_contract(monkeypatch):
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_URL", "http://opensandbox:8080")
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_API_KEY", "secret-key")

    settings = Settings(_env_file=None)

    assert settings.opensandbox_url == "http://opensandbox:8080"
    assert settings.opensandbox_api_key == "secret-key"


def test_public_proxy_policy_lists_load_from_json_environment(monkeypatch):
    monkeypatch.setenv("SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST", '["example.com","*.example.net"]')
    monkeypatch.setenv("SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST", '["blocked.example.com","203.0.113.0/24"]')
    settings = Settings(_env_file=None)
    assert settings.public_proxy_allowlist == ["example.com", "*.example.net"]
    assert settings.public_proxy_denylist == ["blocked.example.com", "203.0.113.0/24"]


def test_model_routes_allow_distinct_upstreams_and_credential_references(monkeypatch):
    monkeypatch.setenv("CODEX_FAST_KEY", "fast-secret")
    settings = Settings(_env_file=None, upstream_codex="http://default.example",
        model_routes_json='{"codex/gpt-6-sol":{"base_url":"http://fast.example/v1","credential_env":"CODEX_FAST_KEY"}}')
    routes = ModelRoutes(settings)
    assert routes.resolve("codex", "gpt-6-sol").base_url == "http://fast.example/v1"
    assert routes.resolve("codex", "gpt-6-sol").credential == "fast-secret"
    assert routes.resolve("codex", "gpt-6-astra").base_url == "http://default.example"
    assert routes.configured_models("codex") == ["gpt-6-sol"]


def test_opensandbox_rejects_development_signing_secrets():
    with pytest.raises(ValueError, match="explicit signing"):
        validate_service_settings(Settings(_env_file=None, backend="opensandbox"))
    validate_service_settings(Settings(_env_file=None, backend="fake"))
