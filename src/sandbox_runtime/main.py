import uvicorn
import os
from sandbox_harness.observability import configure


def run() -> None:
    configure("runtime", "/run/harness/observability")
    uvicorn.run("sandbox_runtime.app:create_app", factory=True, host="127.0.0.1",
        port=int(os.environ.get("SANDBOX_RUNTIME_PORT", "8080")), log_config=None, access_log=False)


if __name__ == "__main__":
    run()
