import uvicorn


def run() -> None:
    uvicorn.run("sandbox_runtime.app:create_app", factory=True, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    run()

