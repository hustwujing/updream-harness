from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SANDBOX_HARNESS_", env_file=".env", extra="ignore")

    data_root: Path = Path(".data")
    api_bind_host: str = "127.0.0.1"
    api_bind_port: int = Field(default=22766, ge=1024, le=65535)
    database_url: str = "sqlite+aiosqlite:///.data/platform.db"
    namespace: str = Field(default="sandbox-harness", pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    backend: Literal["opensandbox", "fake"] = "opensandbox"
    opensandbox_url: str = "http://127.0.0.1:28090"
    opensandbox_api_key: str = ""
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_prefix: str = "sandbox-harness:"
    object_backend: Literal["local", "s3"] = "local"
    object_endpoint: str = ""
    object_bucket: str = ""
    object_prefix: str = "sandbox-harness/"
    gateway_url: str = "http://172.31.0.1:28600"
    gateway_bind_host: str = "172.31.0.1"
    gateway_bind_port: int = Field(default=28600, ge=1024, le=65535)
    gateway_proxy_port: int = Field(default=28601, ge=1024, le=65535)
    gateway_signing_key: str = Field(default="development-only-change-me", min_length=16)
    gateway_control_key: str = Field(default="development-control-change-me", min_length=16)
    session_signing_key: str = Field(default="development-session-change-me", min_length=16)
    gateway_model_start_timeout_seconds: float = Field(default=120, ge=5, le=600)
    gateway_model_idle_timeout_seconds: float = Field(default=300, ge=10, le=1800)
    runtime_model_start_timeout_seconds: float = Field(default=150, ge=10, le=900)
    runtime_event_idle_timeout_seconds: float = Field(default=900, ge=60, le=3600)

    @property
    def public_proxy_url(self) -> str:
        return f"http://{self.gateway_bind_host}:{self.gateway_proxy_port}"
    runtime_image_codex: str = ""
    runtime_image_claude: str = ""
    runtime_image_deepseek: str = ""
    warm_pool_codex: int = Field(default=2, ge=0, le=32)
    warm_pool_claude: int = Field(default=2, ge=0, le=32)
    warm_pool_deepseek: int = Field(default=1, ge=0, le=32)
    max_sandboxes: int = Field(default=12, ge=1, le=128)
    session_keepalive_seconds: int = Field(default=900, ge=0, le=3600)
    sandbox_timeout_seconds: int = Field(default=3600, ge=60, le=86400)
    turn_lease_seconds: int = Field(default=30, ge=10, le=300)
    turn_heartbeat_seconds: int = Field(default=10, ge=2, le=60)
    recovery_scan_seconds: int = Field(default=15, ge=5, le=300)
    orphan_reaper_seconds: int = Field(default=60, ge=15, le=3600)
    max_attachment_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_artifact_bytes: int = Field(default=2 * 1024 * 1024 * 1024, ge=1)
    max_checkpoint_bytes: int = Field(default=4 * 1024 * 1024 * 1024, ge=1)
    capability_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    capability_request_limit: int = Field(default=200, ge=1, le=10000)
    capability_public_request_limit: int = Field(default=5000, ge=1, le=100000)
    model_token_budget: int = Field(default=2_000_000, ge=1)
    upstream_codex: str = ""
    upstream_claude: str = ""
    upstream_deepseek: str = ""
    upstream_image: str = ""
    upstream_video: str = ""
    upstream_audio: str = ""
    model_routes_json: str = ""
    harness_catalog_json: str = ""
    model_catalog_discovery: bool = True
    model_catalog_refresh_seconds: int = Field(default=300, ge=30, le=86400)
    model_catalog_timeout_seconds: float = Field(default=10, ge=1, le=60)
    model_probe_enabled: bool = True
    model_probe_timeout_seconds: float = Field(default=20, ge=2, le=120)
    model_probe_concurrency: int = Field(default=6, ge=1, le=32)

    def image_for(self, harness: str) -> str:
        return {
            "codex": self.runtime_image_codex,
            "claude": self.runtime_image_claude,
            "deepseek": self.runtime_image_deepseek,
        }[harness]


def load_settings() -> Settings:
    value = Settings()
    value.data_root.mkdir(parents=True, exist_ok=True)
    return value
