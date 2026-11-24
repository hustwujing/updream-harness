import uvicorn
import os
from sandbox_harness.observability import configure
from .state import runtime_state_dir


def run() -> None:
    configure("runtime", str(runtime_state_dir() / "observability"))
    uvicorn.run("sandbox_runtime.app:create_app", factory=True, host="127.0.0.1",
        port=int(os.environ.get("SANDBOX_RUNTIME_PORT", "8080")), log_config=None, access_log=False)


if __name__ == "__main__":
    run()
