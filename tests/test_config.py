from sandbox_harness.common.configuration.config import (Settings, validate_service_settings)
import pytest
from sandbox_harness.common.models.model_routes import (ModelRoutes)


def test_opensandbox_environment_contract(monkeypatch):
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_URL", "http://opensandbox:8080")
    monkeypatch.setenv("SANDBOX_HARNESS_OPENSANDBOX_API_KEY", "secret-key")

    settings = Settings(_env_file=None)

    assert settings.opensandbox_url == "http://opensandbox:8080"
    assert settings.opensandbox_api_key == "secret-key"


def test_aliyun_sandbox_environment_contract(monkeypatch):
    monkeypatch.setenv("SANDBOX_HARNESS_BACKEND", "aliyun")
    monkeypatch.setenv("SANDBOX_HARNESS_ALIYUN_SANDBOX_API_KEY", "e2b_test")
    monkeypatch.setenv("SANDBOX_HARNESS_ALIYUN_SANDBOX_API_URL", "https://api.sandbox.test")
    monkeypatch.setenv("SANDBOX_HARNESS_ALIYUN_SANDBOX_DOMAIN", "sandbox.test")

    settings=Settings(_env_file=None)

    assert settings.backend=="aliyun"
    assert settings.aliyun_sandbox_api_key=="e2b_test"
    assert settings.aliyun_sandbox_api_url=="https://api.sandbox.test"
    assert settings.aliyun_sandbox_domain=="sandbox.test"


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
def test_public_proxy_connect_timeout_is_bounded():
    from pydantic import ValidationError
    from sandbox_harness.common.configuration.config import (Settings)
    assert Settings(backend='fake').public_proxy_connect_timeout_seconds==10
    for value in (0,61):
        with pytest.raises(ValidationError):
            Settings(backend='fake',public_proxy_connect_timeout_seconds=value)


def test_model_body_budget_is_separately_configurable_and_bounded(monkeypatch):
    monkeypatch.setenv('SANDBOX_HARNESS_GATEWAY_MODEL_REQUEST_MAX_BYTES','33554432')
    settings=Settings(_env_file=None)
    assert settings.gateway_model_request_max_bytes==32*1024*1024
    assert settings.gateway_request_max_bytes==16*1024*1024
    for value in (1023,64*1024*1024+1):
        with pytest.raises(ValueError):Settings(_env_file=None,gateway_model_request_max_bytes=value)


def test_stream_flush_intervals_are_shared_configurable_and_bounded():
    settings=Settings(_env_file=None)
    assert settings.stream_live_publish_seconds==.3
    assert settings.stream_persist_seconds==5
    assert Settings(_env_file=None,stream_live_publish_seconds=.5,
        stream_persist_seconds=9).stream_persist_seconds==9
    for field,value in (('stream_live_publish_seconds',.01),('stream_persist_seconds',.1)):
        with pytest.raises(ValueError):Settings(_env_file=None,**{field:value})


def test_runtime_stderr_log_budget_allows_disable_and_negative_maximum():
    from pydantic import ValidationError
    assert Settings(_env_file=None).runtime_stderr_log_max_bytes==1024
    assert Settings(_env_file=None,runtime_stderr_log_max_bytes=0).runtime_stderr_log_max_bytes==0
    assert Settings(_env_file=None,runtime_stderr_log_max_bytes=-1).runtime_stderr_log_max_bytes==-1
    with pytest.raises(ValidationError):
        Settings(_env_file=None,runtime_stderr_log_max_bytes=1024*1024+1)


def test_handoff_retry_budget_is_configurable_and_bounded():
    settings = Settings(_env_file=None, handoff_delivery_max_attempts=3,
        handoff_delivery_max_age_seconds=45)
    assert settings.handoff_options == {
        "handoff_max_attempts": 3,
        "handoff_max_age_seconds": 45,
    }
    with pytest.raises(ValueError):
        Settings(_env_file=None, handoff_delivery_max_attempts=0)
    with pytest.raises(ValueError):
        Settings(_env_file=None, handoff_delivery_max_age_seconds=0)


def test_gateway_signing_keyring_requires_distinct_bounded_ids():
    current=Settings(_env_file=None,gateway_signing_key="n"*32,
        gateway_signing_key_id="next",gateway_previous_signing_key="o"*32,
        gateway_previous_signing_key_id="old")
    validate_service_settings(current.model_copy(update={"backend":"fake"}))
    assert current.gateway_previous_signing_keys=={"old":"o"*32}
    with pytest.raises(ValueError,match="distinct"):
        validate_service_settings(current.model_copy(update={
            "gateway_previous_signing_key_id":"next","backend":"fake"}))
    with pytest.raises(ValueError,match="too short"):
        validate_service_settings(current.model_copy(update={
            "gateway_previous_signing_key":"short","backend":"fake"}))


def test_production_real_sandbox_requires_https_gateway(tmp_path):
    value=Settings(_env_file=None,backend="opensandbox",deployment_environment="production",
        gateway_url="http://gateway.internal",gateway_signing_key="g"*32,
        gateway_control_key="c"*32,session_signing_key="s"*32,
        runtime_bundle_digest="a"*64,runtime_code_bundle_digest="b"*64,
        job_catalog_digest="c"*64,profile_catalog_digest="d"*64,
        runtime_bundle_dir=tmp_path,runtime_code_bundle_dir=tmp_path)
    (tmp_path/("a"*64+".tar")).write_bytes(b"runtime")
    (tmp_path/("b"*64+".tar")).write_bytes(b"code")
    with pytest.raises(ValueError,match="HTTPS"):
        validate_service_settings(value)
    validate_service_settings(value.model_copy(update={"gateway_url":"https://gateway.example"}))
