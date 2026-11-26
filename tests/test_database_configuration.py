import pytest
from pydantic import ValidationError


def test_database_settings_require_explicit_owned_connections():
    from sandbox_harness.common.configuration.databases import DatabaseSettings
    settings = DatabaseSettings(media_database_url="postgresql+asyncpg://media:secret@localhost:5432/media")
    assert set(settings.for_service("media-worker")) == {"media"}
    with pytest.raises(ValueError, match="database_connection_missing"):
        settings.for_service("api")
    with pytest.raises(ValueError, match="database_service_invalid"):
        settings.for_service("gateway")
    assert "secret" not in repr(settings)


def test_same_database_with_different_credentials_is_rejected():
    from sandbox_harness.common.configuration.databases import DatabaseSettings
    with pytest.raises(ValidationError) as error:
        DatabaseSettings(core_database_url="postgresql+asyncpg://core:very-private@127.0.0.1:5432/same",
            media_database_url="postgresql+asyncpg://media:very-private@localhost:5432/same")
    assert "database_authorities_overlap" in str(error.value)
    assert "very-private" not in str(error.value)


def test_database_deployment_requires_separate_roles_and_postgresql(tmp_path):
    from sandbox_harness.common.configuration.databases import DatabaseSettings
    settings = DatabaseSettings(core_database_url="postgresql+asyncpg://same:secret@localhost:5432/core",
        media_database_url="postgresql+asyncpg://same:secret@localhost:5432/media")
    with pytest.raises(ValueError, match="database_roles_overlap"):
        settings.validate_deployment()
    local = DatabaseSettings(core_database_url="sqlite+aiosqlite:///" + str(tmp_path / "core.db"))
    with pytest.raises(ValueError, match="database_postgresql_required"):
        local.validate_deployment()


def test_legacy_single_database_environment_is_not_an_alias(monkeypatch):
    from sandbox_harness.common.configuration.databases import DatabaseSettings
    for domain in ("ACCOUNTS", "CORE", "MEDIA", "SITES"):
        monkeypatch.delenv("SANDBOX_HARNESS_" + domain + "_DATABASE_URL", raising=False)
    monkeypatch.setenv("SANDBOX_HARNESS_DATABASE_URL", "postgresql+asyncpg://old:secret@localhost/old")
    with pytest.raises(ValueError, match="database_connection_missing"):
        DatabaseSettings().for_service("job-worker")
