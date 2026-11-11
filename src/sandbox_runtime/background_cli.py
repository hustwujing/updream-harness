from __future__ import annotations

import argparse
import json
import urllib.request


BASE = "http://127.0.0.1:8080/v1/background-jobs"


def call(method: str, path: str = "", value: dict | None = None):
    data = json.dumps(value).encode() if value is not None else None
    request = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read())


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage supervised background work in the current Sandbox")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--name", required=True)
    start.add_argument("--cwd", default="/workspace/project")
    start.add_argument("argv", nargs=argparse.REMAINDER)
    commands.add_parser("list")
    status = commands.add_parser("status")
    status.add_argument("job_id", nargs="?")
    status.add_argument("--name")
    cancel = commands.add_parser("cancel"); cancel.add_argument("job_id")
    args = parser.parse_args()
    if args.command == "start":
        argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
        if not argv:
            parser.error("start requires an argv after --")
        result = call("POST", value={"name": args.name, "argv": argv, "cwd": args.cwd})
    elif args.command == "cancel":
        result = call("POST", "/" + args.job_id + "/cancel")
    elif args.command == "status":
        if not args.job_id and not args.name:
            parser.error("status requires a job_id or --name")
        items = call("GET").get("items", [])
        matches = [item for item in items if item.get("id") == args.job_id] if args.job_id else [
            item for item in items if item.get("name") == args.name]
        if not matches:
            parser.error("background job not found")
        result = matches[-1]
    else:
        result = call("GET")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
