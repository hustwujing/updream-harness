from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


DEVELOPMENT_SECRETS = {
    "development-only-change-me", "development-control-change-me", "development-session-change-me",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SANDBOX_HARNESS_", env_file=".env", extra="ignore")

    data_root: Path = Path(".data")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file_enabled: bool = True
    log_stdout_enabled: bool = False
    log_dir: Path = Path("logs")
    log_file_template: str = Field(default="{service}-{instance}.telemetry.jsonl", min_length=1, max_length=180)
    log_queue_size: int = Field(default=8192, ge=1, le=65536)
    log_ring_capacity: int = Field(default=8192, ge=128, le=65536)
    log_max_bytes: int = Field(default=2 * 1024 * 1024 * 1024, ge=1024)
    runtime_log_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024)
    log_segment_bytes: int = Field(default=8 * 1024 * 1024, ge=1024)
    log_segment_max_age_seconds: int = Field(default=86400, ge=60, le=7 * 86400)
    log_retention_days: int = Field(default=7, ge=1, le=3650)
    log_otlp_timeout_seconds: float = Field(default=2, ge=0.1, le=60)
    log_schema_strict: bool = False
    diagnostic_capture: Literal["off", "metadata", "payload"] = "off"
    diagnostic_dir: Path = Path("diagnostics")
    diagnostic_file_template: str = Field(default="{service}-{instance}-{session}.diagnostic.jsonl", min_length=1, max_length=180)
    diagnostic_capture_session: str = Field(default="", pattern=r"^$|^[A-Za-z0-9._-]{1,80}$")
    diagnostic_trace_id: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")
    diagnostic_max_body_bytes: int = Field(default=256 * 1024, ge=0, le=16 * 1024 * 1024)
    diagnostic_max_bytes: int = Field(default=256 * 1024 * 1024, ge=1024)
    diagnostic_ttl_hours: int = Field(default=24, ge=1, le=168)
    api_bind_host: str = "127.0.0.1"
    api_bind_port: int = Field(default=24767, ge=1024, le=65535)
    database_url: str = "sqlite+aiosqlite:///.data/platform.db"
    namespace: str = Field(default="sandbox-harness-platform-parallel-ali", pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    backend: Literal["opensandbox", "aliyun", "fake"] = "opensandbox"
    job_allocation_dispatcher_enabled: bool = False
    allocation_lease_seconds: float = Field(default=30,ge=3,le=300)
    allocation_recovery_seconds: float = Field(default=90,ge=10,le=900)
    allocation_provision_ttl_seconds: int = Field(default=300,ge=60,le=3600)
    opensandbox_url: str = "http://127.0.0.1:28090"
    opensandbox_api_key: str = ""
    aliyun_sandbox_api_url: str = "https://api.cn-shanghai.sandbox.aliyuncs.com"
    aliyun_sandbox_domain: str = "cn-shanghai.sandbox.aliyuncs.com"
    aliyun_sandbox_api_key: str = ""
    aliyun_sandbox_request_timeout_seconds: float = Field(default=60, ge=1, le=300)
    aliyun_sandbox_gateway_url:str=""
    aliyun_sandbox_proxy_url:str=""
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_prefix: str = "sandbox-harness-platform-parallel-ali:"
    job_catalog_path: Path = Path("deploy/job-catalog.example.json")
    job_catalog_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")
    profile_catalog_path: Path = Path("deploy/profile-catalog.example.json")
    profile_catalog_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")
    object_backend: Literal["local", "s3"] = "local"
    object_endpoint: str = ""
    object_bucket: str = ""
    object_prefix: str = "sandbox-harness-platform-parallel-ali/"
    object_region: str = "updream"
    object_gc_enabled: bool = True
    object_gc_dry_run: bool = True
    object_gc_grace_seconds: int = Field(default=7*86400,ge=60,le=365*86400)
    object_gc_interval_seconds: int = Field(default=3600,ge=60,le=86400)
    object_public_endpoint: str = "https://boss-shjd.biliapi.net"
    media_reference_prefix: str = "aniforge"
    ark_control_endpoint: str = "https://ark.cn-beijing.volcengineapi.com"
    ark_access_key: str = ""
    ark_secret_key: str = ""
    ark_project_name: str = "default"
    ark_asset_group_id: str = ""
    ark_asset_group_type: str = "AIGC"
    ark_asset_poll_seconds: float = Field(default=2, ge=0.1, le=30)
    ark_asset_max_wait_seconds: int = Field(default=900, ge=5, le=3600)
    gateway_url: str = "http://172.31.0.1:29882"
    gateway_bind_host: str = "172.31.0.1"
    gateway_bind_port: int = Field(default=29882, ge=1024, le=65535)
    gateway_proxy_port: int = Field(default=29883, ge=1024, le=65535)
    public_proxy_allowlist: list[str] = Field(default_factory=list)
    public_proxy_denylist: list[str] = Field(default_factory=list)
    gateway_signing_key: str = Field(default="development-only-change-me", min_length=16)
    gateway_signing_key_id: str = Field(default="current",
        pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    gateway_previous_signing_key: str = ""
    gateway_previous_signing_key_id: str = Field(default="previous",
        pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    gateway_control_key: str = Field(default="development-control-change-me", min_length=16)
    session_signing_key: str = Field(default="development-session-change-me", min_length=16)
    auth_session_ttl_seconds: int = Field(default=30 * 86400, ge=300, le=365 * 86400)
    gateway_model_start_timeout_seconds: float = Field(default=120, ge=5, le=600)
    gateway_model_idle_timeout_seconds: float = Field(default=300, ge=10, le=1800)
    runtime_model_start_timeout_seconds: float = Field(default=150, ge=10, le=900)
    runtime_event_idle_timeout_seconds: float = Field(default=900, ge=60, le=3600)
    runtime_metrics_enabled: bool = False
    runtime_stderr_log_max_bytes: int = Field(default=1024,le=1024*1024)
    runtime_stream_reattach_attempts: int = Field(default=8, ge=2, le=12)
    runtime_stream_reattach_max_delay_seconds: float = Field(default=4, ge=0.1, le=10)
    runtime_bundle_dir: Path = Path(".data/runtime-bundles")
    runtime_bundle_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")
    runtime_code_bundle_dir: Path = Path(".data/runtime-code-bundles")
    runtime_code_bundle_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")

    @property
    def public_proxy_url(self) -> str:
        return f"http://{self.gateway_bind_host}:{self.gateway_proxy_port}"
    runtime_image_codex: str = ""
    runtime_image_claude: str = ""
    runtime_image_deepseek: str = ""
    warm_pool_codex: int = Field(default=2, ge=0, le=32)
    warm_pool_claude: int = Field(default=2, ge=0, le=32)
    warm_pool_deepseek: int = Field(default=1, ge=0, le=32)
    max_sandboxes: int = Field(default=13, ge=1, le=128)
    session_keepalive_seconds: int = Field(default=900, ge=0, le=3600)
    sandbox_control_ttl_seconds: int = Field(default=3600, ge=300, le=86400)
    sandbox_port_grant_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    sandbox_terminal_ticket_seconds: int = Field(default=60, ge=10, le=300)
    sandbox_inspect_max_file_bytes: int = Field(default=2 * 1024 * 1024, ge=1024, le=16 * 1024 * 1024)
    sandbox_timeout_seconds: int = Field(default=3600, ge=60, le=86400)
    sandbox_username: str = Field(default="updream", pattern=r"^[a-z_][a-z0-9_-]{1,31}$")
    turn_lease_seconds: int = Field(default=30, ge=10, le=300)
    turn_heartbeat_seconds: int = Field(default=10, ge=2, le=60)
    recovery_scan_seconds: int = Field(default=15, ge=5, le=300)
    orphan_reaper_seconds: int = Field(default=60, ge=15, le=3600)
    platform_leader_lease_seconds: int = Field(default=30, ge=6, le=300)
    max_attachment_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_artifact_bytes: int = Field(default=2 * 1024 * 1024 * 1024, ge=1)
    max_checkpoint_bytes: int = Field(default=4 * 1024 * 1024 * 1024, ge=1)
    capability_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    capability_request_limit: int = Field(default=200, ge=1, le=10000)
    capability_public_request_limit: int = Field(default=5000, ge=1, le=100000)
    model_token_budget: int = Field(default=2_000_000, ge=1)
    model_cache_read_budget_percent: int = Field(default=10,ge=0,le=100)
    model_cache_write_budget_percent: int = Field(default=125,ge=0,le=500)
    gateway_request_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)
    gateway_model_request_max_bytes: int = Field(default=64 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)
    gateway_body_idle_timeout_seconds: float = Field(default=15, ge=1, le=120)
    gateway_body_total_timeout_seconds: float = Field(default=120, ge=5, le=600)
    public_proxy_request_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)
    public_proxy_connect_timeout_seconds: float = Field(default=10, ge=1, le=60)
    public_proxy_body_idle_timeout_seconds: float = Field(default=15, ge=1, le=120)
    public_proxy_body_total_timeout_seconds: float = Field(default=120, ge=5, le=600)
    job_node_id: str = Field(default="local-sandbox",pattern=r"^[A-Za-z0-9._-]{1,160}$")
    job_node_cpu_limit: int = Field(default=8,ge=1,le=1024)
    job_node_memory_mb_limit: int = Field(default=32768,ge=128)
    job_node_heartbeat_seconds: float = Field(default=10,ge=1,le=60)
    job_worker_concurrency: int = Field(default=8,ge=1,le=128)
    job_recovery_concurrency: int = Field(default=1,ge=1,le=128)
    job_lease_seconds: int = Field(default=60,ge=15,le=600)
    controlled_runtime_root: Path = Path(
        "/data2/wujing03/sandbox-harness-platform-plus/.data/provider-runtime")
    controlled_runtime_digest: str = Field(default="",pattern=r"^$|^[0-9a-f]{64}$")
    controlled_node_id: str = Field(default="local-controlled",pattern=r"^[A-Za-z0-9._-]{1,160}$")
    controlled_worker_concurrency: int = Field(default=2,ge=1,le=16)
    controlled_node_cpu_limit: int = Field(default=8,ge=1,le=128)
    controlled_node_memory_mb_limit: int = Field(default=65536,ge=4096)
    image_decompose_base_url: str = "http://127.0.0.1:8912"
    upstream_codex: str = ""
    upstream_claude: str = ""
    upstream_deepseek: str = ""
    upstream_image: str = ""
    upstream_video: str = ""
    upstream_audio: str = ""
    dashscope_video_create_url: str = "https://bashscope.bilibili.co/api/v1/services/aigc/video-generation/video-synthesis"
    dashscope_video_task_url: str = "https://bashscope.bilibili.co/api/v1/tasks"
    tripo_base_url: str = "https://openapi.tripo3d.com/v3"
    tripo_api_key: str = ""
    tripo_worker_concurrency: int = Field(default=2, ge=1, le=5)
    tripo_poll_seconds: float = Field(default=2, ge=1, le=30)
    tripo_submit_max_retries: int = Field(default=5, ge=0, le=10)
    tripo_task_timeout_seconds: int = Field(default=1800, ge=60, le=7200)
    tripo_max_output_bytes: int = Field(default=512 * 1024 * 1024, ge=1024 * 1024)
    media_catalog_json: str = ""
    media_job_lease_seconds: int = Field(default=60, ge=10, le=600)
    provider_output_allowlist: list[str] = Field(default_factory=lambda:[
        "*.biliapi.net","*.volces.com","*.volccdn.com","*.aliyuncs.com",
        "*.dashscope.aliyuncs.com","*.minimax.io","*.minimaxi.com","*.hailuoai.com",
        "*.tripo3d.ai","*.tripo3d.com"])
    media_draft_ttl_seconds: int = Field(default=24 * 3600, ge=600, le=7 * 86400)
    media_worker_concurrency: int = Field(default=6, ge=1, le=32)
    audio_worker_concurrency: int = Field(default=6, ge=1, le=16)
    image_task_timeout_seconds: int = Field(default=300, ge=60, le=3600)
    audio_task_timeout_seconds: int = Field(default=2 * 3600, ge=600, le=24 * 3600)
    video_task_timeout_seconds: int = Field(default=2 * 3600, ge=600, le=24 * 3600)
    site_worker_concurrency: int = Field(default=2, ge=1, le=16)
    site_job_lease_seconds: int = Field(default=60, ge=10, le=600)
    site_job_max_attempts: int = Field(default=4, ge=1, le=20)
    site_job_retry_base_seconds: int = Field(default=5, ge=1, le=300)
    supervisor_retry_initial_seconds: float = Field(default=.5, ge=.01, le=30)
    supervisor_retry_max_seconds: float = Field(default=30, ge=.02, le=300)
    handoff_delivery_max_attempts: int = Field(default=8, ge=1, le=100)
    handoff_delivery_max_age_seconds: int = Field(default=900, ge=1, le=86400)
    site_draft_ttl_seconds: int = Field(default=24 * 3600, ge=600, le=7 * 86400)
    site_public_base_path: str = Field(default="/sandbox-harness-platform-parallel-ali/sites",
        pattern=r"^/[A-Za-z0-9/_-]+$")
    web_dist_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")
    gateway_telemetry_fallback_capacity: int = Field(default=4096, ge=64, le=65536)
    reviewer_harness: str = Field(default="codex", pattern=r"^(codex|claude|deepseek)$")
    reviewer_model: str = Field(default="gpt-5.6", min_length=1, max_length=160)
    child_input_max_count: int = Field(default=100, ge=1, le=1000)
    child_input_max_total_bytes: int = Field(default=8 * 1024 * 1024 * 1024, ge=1)
    model_routes_json: str = ""
    harness_catalog_json: str = ""
    model_catalog_discovery: bool = True
    model_catalog_snapshot_path: Path | None = None
    model_catalog_refresh_seconds: int = Field(default=300, ge=30, le=86400)
    model_catalog_timeout_seconds: float = Field(default=10, ge=1, le=60)
    model_probe_enabled: bool = True
    model_probe_timeout_seconds: float = Field(default=20, ge=2, le=120)
    model_probe_failure_threshold: int = Field(default=3, ge=1, le=10)
    model_probe_recovery_threshold: int = Field(default=2, ge=1, le=10)
    model_probe_concurrency: int = Field(default=6, ge=1, le=32)
    model_catalog_max_models_per_harness: int = Field(default=30, ge=1, le=30)

    def image_for(self, harness: str) -> str:
        return {
            "codex": self.runtime_image_codex,
            "claude": self.runtime_image_claude,
            "deepseek": self.runtime_image_deepseek,
        }[harness]

    @property
    def handoff_options(self) -> dict[str, int]:
        return {
            "handoff_max_attempts": self.handoff_delivery_max_attempts,
            "handoff_max_age_seconds": self.handoff_delivery_max_age_seconds,
        }

    @property
    def gateway_previous_signing_keys(self) -> dict[str, str]:
        return ({self.gateway_previous_signing_key_id:self.gateway_previous_signing_key}
            if self.gateway_previous_signing_key else {})


def load_settings() -> Settings:
    value = Settings()
    validate_service_settings(value)
    value.data_root.mkdir(parents=True, exist_ok=True)
    return value


def validate_service_settings(value: Settings) -> None:
    if value.gateway_previous_signing_key and len(value.gateway_previous_signing_key)<16:
        raise ValueError("Previous gateway signing key is too short")
    if (value.gateway_previous_signing_key
            and value.gateway_previous_signing_key_id==value.gateway_signing_key_id):
        raise ValueError("Gateway signing key ids must be distinct")
    if value.backend != "fake":
        configured = (value.gateway_signing_key, value.gateway_control_key, value.session_signing_key)
        if any(secret in DEVELOPMENT_SECRETS for secret in configured):
            raise ValueError("Production sandbox deployment requires explicit signing and control secrets")
        if not value.runtime_bundle_digest:
            raise ValueError("Production sandbox deployment requires a frozen Runtime Bundle digest")
        if not value.runtime_code_bundle_digest:
            raise ValueError("Production sandbox deployment requires a frozen Runtime Code Bundle digest")
        if not value.job_catalog_digest or not value.profile_catalog_digest:
            raise ValueError("Production sandbox deployment requires frozen Job and Profile Catalog digests")
        bundle = value.runtime_bundle_dir / (value.runtime_bundle_digest + ".tar")
        code_bundle = value.runtime_code_bundle_dir / (value.runtime_code_bundle_digest + ".tar")
        if not bundle.is_file():
            raise ValueError("Configured Runtime Bundle is unavailable")
        if not code_bundle.is_file():
            raise ValueError("Configured Runtime Code Bundle is unavailable")
