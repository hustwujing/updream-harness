import uvicorn

from .config import load_settings


def run() -> None:
    settings = load_settings()
    uvicorn.run("sandbox_harness.app:create_app", factory=True, host=settings.api_bind_host, port=settings.api_bind_port)


if __name__ == "__main__":
    run()
