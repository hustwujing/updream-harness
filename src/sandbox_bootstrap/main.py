import uvicorn

from sandbox_harness.observability import configure
from .supervisor import create_app
from .supervisor import runtime_state_dir


def run() -> None:
    configure("runtime-bootstrap",str(runtime_state_dir()/"observability"))
    uvicorn.run(create_app(), host="0.0.0.0", port=8080, log_config=None, access_log=False)


if __name__ == "__main__": run()
