#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import shutil
import socket
import stat
import subprocess
from urllib.parse import quote


ROOT=Path(__file__).resolve().parents[1]
EXPECTED_DATA_ROOT=Path("/data2/wujing03/sandbox-harness-platform-parallel/.data")
EXPECTED={
    "SANDBOX_HARNESS_DATA_ROOT":str(EXPECTED_DATA_ROOT),
    "SANDBOX_HARNESS_NAMESPACE":"sandbox-harness-parallel",
    "SANDBOX_HARNESS_REDIS_PREFIX":"sandbox-harness-parallel:",
    "SANDBOX_HARNESS_OBJECT_PREFIX":"sandbox-harness-parallel/",
    "SANDBOX_HARNESS_API_BIND_PORT":"24766",
    "SANDBOX_HARNESS_GATEWAY_BIND_PORT":"29880",
    "SANDBOX_HARNESS_GATEWAY_PROXY_PORT":"29881",
}
FORBIDDEN_IDENTITIES={"sandbox-harness","agent-plus","sandbox-harness-platform-plus"}
SHARED_CREDENTIAL_KEYS={
    "SANDBOX_HARNESS_OPENSANDBOX_API_KEY","SANDBOX_HARNESS_UPSTREAM_CODEX_KEY",
    "SANDBOX_HARNESS_UPSTREAM_CLAUDE_KEY","SANDBOX_HARNESS_UPSTREAM_DEEPSEEK_KEY",
    "SANDBOX_HARNESS_MODEL_ROUTES_JSON","SANDBOX_HARNESS_TRIPO_API_KEY",
    "SANDBOX_HARNESS_ARK_ACCESS_KEY","SANDBOX_HARNESS_ARK_SECRET_KEY",
    "SANDBOX_HARNESS_ARK_PROJECT_NAME","SANDBOX_HARNESS_ARK_ASSET_GROUP_ID",
    "SANDBOX_HARNESS_OBJECT_ENDPOINT","SANDBOX_HARNESS_OBJECT_BUCKET",
}


def parse_environment(path: Path) -> dict[str,str]:
    values={}
    for line in path.read_text().splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key,value=line.split("=",1);values[key]=value
    return values


def render_environment(values: dict[str,str]) -> str:
    return "".join(f"{key}={value}\n" for key,value in values.items())


def ensure_isolated(values: dict[str,str]) -> None:
    data_root=Path(values.get("SANDBOX_HARNESS_DATA_ROOT","")).resolve()
    if os.path.commonpath((str(data_root),str(EXPECTED_DATA_ROOT)))!=str(EXPECTED_DATA_ROOT):
        raise ValueError("data_root_outside_data2")
    for key,value in EXPECTED.items():
        if values.get(key)!=value:raise ValueError("identity_collision:"+key)
    identities={values.get("SANDBOX_HARNESS_NAMESPACE",""),
        values.get("SANDBOX_HARNESS_REDIS_PREFIX","").rstrip(":"),
        values.get("SANDBOX_HARNESS_OBJECT_PREFIX","").rstrip("/")}
    if identities & FORBIDDEN_IDENTITIES:raise ValueError("identity_collision:predecessor")
    database=values.get("SANDBOX_HARNESS_DATABASE_URL","")
    if "@127.0.0.1:28432/sandbox_harness_parallel" not in database:
        raise ValueError("identity_collision:database")


def check_port(port: int) -> None:
    with socket.socket() as probe:
        try:probe.bind(("127.0.0.1",port))
        except OSError as exc:raise RuntimeError("port_in_use:"+str(port)) from exc


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-env",type=Path)
    parser.add_argument("--force",action="store_true")
    parser.add_argument("--allow-running",action="store_true")
    parser.add_argument("--check-only",action="store_true")
    parser.add_argument("--runtime-signing-key",type=Path)
    args=parser.parse_args()
    if ROOT!=Path("/data2/wujing03/sandbox-harness-platform-parallel"):
        raise ValueError("identity_collision:project_root")
    target=EXPECTED_DATA_ROOT/"config/platform.env"
    values=parse_environment(ROOT/".env.example");values.update(EXPECTED)
    if args.source_env:
        source=parse_environment(args.source_env)
        for key in SHARED_CREDENTIAL_KEYS:
            if source.get(key):values[key]=source[key]
    password=secrets.token_urlsafe(36)
    values["SANDBOX_HARNESS_DATABASE_URL"]=("postgresql+asyncpg://sandbox_harness_parallel:"
        +quote(password,safe="")+"@127.0.0.1:28432/sandbox_harness_parallel")
    values["SANDBOX_HARNESS_GATEWAY_SIGNING_KEY"]=secrets.token_urlsafe(48)
    values["SANDBOX_HARNESS_GATEWAY_CONTROL_KEY"]=secrets.token_urlsafe(48)
    values["SANDBOX_HARNESS_SESSION_SIGNING_KEY"]=secrets.token_urlsafe(48)
    ensure_isolated(values)
    if not args.allow_running:
        for port in (24766,28432,29880,29881):check_port(port)
    if args.check_only:return
    if not args.runtime_signing_key:raise ValueError("runtime_signing_key_required")
    public=subprocess.check_output(["openssl","pkey","-in",str(args.runtime_signing_key),
        "-pubout"])
    if public.strip()!=(ROOT/"runtime/bundle_public_key.pem").read_bytes().strip():
        raise ValueError("identity_collision:runtime_signing_key")
    if target.exists() and not args.force:raise FileExistsError(str(target))
    target.parent.mkdir(parents=True,exist_ok=True);target.parent.chmod(0o750)
    temporary=target.with_suffix(".tmp");temporary.write_text(render_environment(values));temporary.chmod(0o640)
    temporary.replace(target);os.chmod(target,stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP)
    key_target=target.parent/"runtime-bundle-signing.pem"
    shutil.copyfile(args.runtime_signing_key,key_target);key_target.chmod(0o600)
    print("Configured isolated environment at "+str(target))


if __name__=="__main__":main()
