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


def test_origin_claude_uses_its_model_specific_credential(monkeypatch):
    monkeypatch.setenv("ORIGIN_CLAUDE_KEY","origin-secret")
    settings=Settings(_env_file=None,upstream_claude="http://default.example/v1",
        model_routes_json='{"claude/origin/claude-opus-5-5":{"base_url":'
            '"http://default.example/v1","credential_env":"ORIGIN_CLAUDE_KEY"}}')
    route=ModelRoutes(settings).resolve("claude","origin/claude-opus-5-5")
    assert route.base_url=="http://default.example/v1" and route.credential=="origin-secret"


@pytest.mark.parametrize("field",["upstream_codex","upstream_claude","upstream_deepseek"])
@pytest.mark.parametrize("value",[
    "not-a-url","ftp://models.example/v1","https:///missing-host",
    "https://user:secret@models.example/v1","https://models.example/v1?query=1",
    "https://models.example/v1#fragment",
])
def test_default_model_routes_reject_malformed_urls(field,value):
    with pytest.raises(ValueError,match="model route base URL"):
        ModelRoutes(Settings(_env_file=None,**{field:value}))


def test_default_model_routes_allow_empty_and_normalize_trailing_path():
    routes=ModelRoutes(Settings(_env_file=None,upstream_codex="https://models.example/api/v1///",
        upstream_claude="",upstream_deepseek=""))
    assert routes.default("codex").base_url=="https://models.example/api/v1"
    assert routes.default("claude").base_url==""


def test_opensandbox_rejects_development_signing_secrets():
    with pytest.raises(ValueError,match="Production sandbox deployment"):
        validate_service_settings(Settings(_env_file=None, backend="opensandbox"))
    validate_service_settings(Settings(_env_file=None, backend="fake"))
