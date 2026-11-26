"""Explicit database connections for independent service deployment."""
from pathlib import Path

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


SERVICE_DOMAINS = {
    "api": ("accounts", "core", "media", "sites"),
    "sandbox-gateway": ("core", "media", "sites"),
    "job-worker": ("core", "media"),
    "allocation-worker": ("core",),
    "controlled-worker": ("core",),
    "media-worker": ("media",),
    "site-worker": ("sites",),
    "account-admin": ("accounts",),
    "image-inference": (),
}


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SANDBOX_HARNESS_", env_file=None,
                                     extra="ignore", hide_input_in_errors=True)
    accounts_database_url: SecretStr | None = None
    core_database_url: SecretStr | None = None
    media_database_url: SecretStr | None = None
    sites_database_url: SecretStr | None = None

    def _configured(self):
        return {domain: value.get_secret_value() for domain in ("accounts", "core", "media", "sites")
                if (value := getattr(self, domain + "_database_url")) is not None}

    @model_validator(mode="after")
    def unique_authorities(self):
        identities = set()
        for value in self._configured().values():
            try:
                url = make_url(value)
                if url.drivername == "postgresql+asyncpg":
                    if not url.host or not url.database:
                        raise ValueError
                    host = "loopback" if url.host in {"localhost", "127.0.0.1", "::1"} else url.host.casefold()
                    identity = ("postgresql", host, url.port or 5432, url.database)
                elif url.drivername == "sqlite+aiosqlite":
                    if not url.database or url.database == ":memory:":
                        raise ValueError
                    identity = ("sqlite", str(Path(url.database).resolve()))
                else:
                    raise ValueError
            except (ValueError, TypeError, ArgumentError):
                raise ValueError("database_connection_invalid") from None
            if identity in identities:
                raise ValueError("database_authorities_overlap")
            identities.add(identity)
        return self

    def for_service(self, service):
        required = SERVICE_DOMAINS.get(service)
        if required is None:
            raise ValueError("database_service_invalid")
        configured = self._configured()
        if any(domain not in configured for domain in required):
            raise ValueError("database_connection_missing")
        return {domain: configured[domain] for domain in required}

    def validate_deployment(self):
        roles = set()
        for value in self._configured().values():
            url = make_url(value)
            if url.drivername != "postgresql+asyncpg":
                raise ValueError("database_postgresql_required")
            if not url.username or not url.password:
                raise ValueError("database_credentials_required")
            host = "loopback" if url.host in {"localhost", "127.0.0.1", "::1"} else url.host.casefold()
            role = (host, url.port or 5432, url.username)
            if role in roles:
                raise ValueError("database_roles_overlap")
            roles.add(role)
