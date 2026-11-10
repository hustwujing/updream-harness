import uvicorn

from .config import load_settings
from .observability import configure


def run() -> None:
    settings = load_settings()
    configure("api", settings.data_root / "logs")
    uvicorn.run("sandbox_harness.app:create_app", factory=True, host=settings.api_bind_host, port=settings.api_bind_port, log_config=None, access_log=False)


if __name__ == "__main__":
    run()
