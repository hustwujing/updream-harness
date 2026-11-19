from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.error import HTTPError,URLError
from urllib.parse import urlencode
import urllib.request


BASE = "http://127.0.0.1:8080/v1/background-jobs"


class BackgroundCLIError(Exception):
    def __init__(self,value:dict):
        self.value=value


def api_error(exc:HTTPError)->BackgroundCLIError:
    try:value=json.loads(exc.read())
    except (ValueError,TypeError):value={}
    detail=value.get("detail") if isinstance(value,dict) else None
    if isinstance(detail,dict):result=dict(detail)
    else:result={"code":"background_request_failed",
        "message":str(detail or exc.reason or "Background request failed"),
        "retryable":exc.code>=500,"allowed_actions":[]}
    result["http_status"]=exc.code
    return BackgroundCLIError(result)


def call(method: str, path: str = "", value: dict | None = None):
    data = json.dumps(value).encode() if value is not None else None
    request = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read())
    except HTTPError as exc:raise api_error(exc) from None
    except URLError as exc:raise BackgroundCLIError({"code":"background_runtime_unavailable",
        "message":str(exc.reason),"retryable":True,"allowed_actions":["status"]}) from None


def stream(path:str):
    request=urllib.request.Request(BASE+path,method="GET")
    try:
        with urllib.request.urlopen(request,timeout=None) as response:
            for raw in response:
                if raw.strip():yield json.loads(raw)
    except HTTPError as exc:raise api_error(exc) from None
    except URLError as exc:raise BackgroundCLIError({"code":"background_runtime_unavailable",
        "message":str(exc.reason),"retryable":True,"allowed_actions":["status"]}) from None


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage supervised background work in the current Sandbox")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--name", required=True)
    start.add_argument("--cwd")
    start.add_argument("--replaces", default="")
    start.add_argument("--timeout",type=float)
    start.add_argument("--terminate-grace",type=float)
    start.add_argument("argv", nargs=argparse.REMAINDER)
    commands.add_parser("list")
    status = commands.add_parser("status")
    status.add_argument("job_id", nargs="?")
    status.add_argument("--name")
    cancel = commands.add_parser("cancel"); cancel.add_argument("job_id")
    wait=commands.add_parser("wait");wait.add_argument("job_id")
    wait.add_argument("--timeout",type=float,help="Observation timeout; does not cancel the job")
    args = parser.parse_args()
    try:
        if args.command == "start":
            argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
            if not argv:parser.error("start requires an argv after --")
            payload={"name":args.name,"argv":argv,"cwd":args.cwd or os.getcwd(),
                "replaces":args.replaces}
            if args.timeout is not None:payload["timeout_seconds"]=args.timeout
            if args.terminate_grace is not None:payload["terminate_grace_seconds"]=args.terminate_grace
            result=call("POST",value=payload)
        elif args.command == "cancel":
            result = call("POST", "/" + args.job_id + "/cancel")
        elif args.command == "wait":
            query=("?"+urlencode({"timeout_seconds":args.timeout}) if args.timeout is not None else "")
            result=None
            for row in stream("/"+args.job_id+"/wait"+query):
                if row.get("type")=="heartbeat":
                    print("background job "+args.job_id+" is "+str(row.get("job",{}).get("status","running")),
                        file=sys.stderr,flush=True)
                elif row.get("type")=="wait_timeout":
                    result={**row["job"],"wait_timed_out":True}
                elif row.get("type")=="terminal":result=row["job"]
            if result is None:raise BackgroundCLIError({"code":"background_wait_incomplete",
                "message":"Background wait ended without a terminal snapshot","retryable":True,
                "allowed_actions":["status","wait"]})
        elif args.command == "status":
            if not args.job_id and not args.name:parser.error("status requires a job_id or --name")
            items = call("GET").get("items", [])
            matches = [item for item in items if item.get("id") == args.job_id] if args.job_id else [
                item for item in items if item.get("name") == args.name]
            if not matches:parser.error("background job not found")
            result = matches[-1]
        else:result = call("GET")
        print(json.dumps(result, ensure_ascii=False))
    except BackgroundCLIError as exc:
        print(json.dumps(exc.value,ensure_ascii=False),file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
