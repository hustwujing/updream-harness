import uvicorn

from .supervisor import create_app


def run() -> None:
    uvicorn.run(create_app(), host="0.0.0.0", port=8080, log_config=None, access_log=False)


if __name__ == "__main__": run()
