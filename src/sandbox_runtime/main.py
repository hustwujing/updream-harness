import uvicorn
from sandbox_harness.observability import configure


def run() -> None:
    configure("runtime", "/run/harness/observability")
    uvicorn.run("sandbox_runtime.app:create_app", factory=True, host="0.0.0.0", port=8080, log_config=None, access_log=False)


if __name__ == "__main__":
    run()

