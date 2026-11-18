#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shlex
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
    "AWS_ACCESS_KEY_ID","AWS_DEFAULT_REGION","AWS_SECRET_ACCESS_KEY",
    "SANDBOX_HARNESS_OPENSANDBOX_API_KEY","SANDBOX_HARNESS_UPSTREAM_CODEX_KEY",
    "SANDBOX_HARNESS_UPSTREAM_CLAUDE_KEY","SANDBOX_HARNESS_UPSTREAM_DEEPSEEK_KEY",
    "SANDBOX_HARNESS_UPSTREAM_CODEX","SANDBOX_HARNESS_UPSTREAM_CLAUDE",
    "SANDBOX_HARNESS_UPSTREAM_DEEPSEEK","SANDBOX_HARNESS_UPSTREAM_IMAGE",
    "SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY","SANDBOX_HARNESS_UPSTREAM_VIDEO",
    "SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY","SANDBOX_HARNESS_UPSTREAM_AUDIO",
    "SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY",
    "SANDBOX_HARNESS_MODEL_ROUTES_JSON","SANDBOX_HARNESS_TRIPO_API_KEY",
    "SANDBOX_HARNESS_TRIPO_BASE_URL","SANDBOX_HARNESS_TRIPO_POLL_SECONDS",
    "SANDBOX_HARNESS_TRIPO_SUBMIT_MAX_RETRIES","SANDBOX_HARNESS_TRIPO_TASK_TIMEOUT_SECONDS",
    "SANDBOX_HARNESS_TRIPO_WORKER_CONCURRENCY",
    "SANDBOX_HARNESS_ARK_ACCESS_KEY","SANDBOX_HARNESS_ARK_SECRET_KEY",
    "SANDBOX_HARNESS_ARK_PROJECT_NAME","SANDBOX_HARNESS_ARK_ASSET_GROUP_ID",
    "SANDBOX_HARNESS_ARK_IMAGE_MODEL_SEEDREAM_5_0","SANDBOX_HARNESS_ARK_VIDEO_MODEL_SED2",
    "SANDBOX_HARNESS_OBJECT_BACKEND","SANDBOX_HARNESS_OBJECT_ENDPOINT",
    "SANDBOX_HARNESS_OBJECT_PUBLIC_ENDPOINT","SANDBOX_HARNESS_OBJECT_BUCKET",
    "SANDBOX_HARNESS_OBJECT_REGION","SANDBOX_HARNESS_MEDIA_CATALOG_JSON",
    "SANDBOX_HARNESS_MEDIA_REFERENCE_PREFIX",
}
ENVIRONMENT_KEY=re.compile(r"^[A-Z_][A-Z0-9_]*$")


def parse_environment(path: Path) -> dict[str,str]:
    values={}
    for line in path.read_text().splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key,value=line.split("=",1);values[key]=value
    return values


def render_environment(values: dict[str,str]) -> str:
    return "".join(f"{key}={value}\n" for key,value in values.items())


def decode_environment_value(value: str) -> str:
    if len(value)>=2 and value[0] in {"'",'"'} and value[-1]==value[0]:
        decoded=shlex.split(value)
        if len(decoded)!=1:raise ValueError("invalid_quoted_environment_value")
        return decoded[0]
    return value


def model_route_credential_keys(value: str) -> set[str]:
    routes=json.loads(decode_environment_value(value))
    keys=set()
    pending=[routes]
    while pending:
        item=pending.pop()
        if isinstance(item,dict):
            credential=item.get("credential_env")
            if credential is not None:
                if not isinstance(credential,str) or not ENVIRONMENT_KEY.fullmatch(credential):
                    raise ValueError("invalid_model_route_credential_env")
                keys.add(credential)
            pending.extend(item.values())
        elif isinstance(item,list):pending.extend(item)
    return keys


def copy_shared_configuration(values: dict[str,str],source: dict[str,str]) -> None:
    keys=set(SHARED_CREDENTIAL_KEYS)
    routes=source.get("SANDBOX_HARNESS_MODEL_ROUTES_JSON")
    if routes:keys.update(model_route_credential_keys(routes))
    for key in keys:
        if source.get(key):values[key]=source[key]


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


def controlled_runtime_value(source:Path)->tuple[object,dict]:
    script=ROOT/'scripts/build_controlled_runtime_manifest.py'
    spec=importlib.util.spec_from_file_location('controlled_runtime_manifest',script)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    value=module.build_manifest(source);return module,value


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-env",type=Path)
    parser.add_argument("--force",action="store_true")
    parser.add_argument("--allow-running",action="store_true")
    parser.add_argument("--check-only",action="store_true")
    parser.add_argument("--runtime-signing-key",type=Path)
    parser.add_argument("--controlled-runtime-source",type=Path)
    args=parser.parse_args()
    if ROOT!=Path("/data2/wujing03/sandbox-harness-platform-parallel"):
        raise ValueError("identity_collision:project_root")
    target=EXPECTED_DATA_ROOT/"config/platform.env"
    values=parse_environment(ROOT/".env.example");values.update(EXPECTED)
    if args.source_env:
        source=parse_environment(args.source_env)
        copy_shared_configuration(values,source)
    password=secrets.token_urlsafe(36)
    values["SANDBOX_HARNESS_DATABASE_URL"]=("postgresql+asyncpg://sandbox_harness_parallel:"
        +quote(password,safe="")+"@127.0.0.1:28432/sandbox_harness_parallel")
    values["SANDBOX_HARNESS_GATEWAY_SIGNING_KEY"]=secrets.token_urlsafe(48)
    values["SANDBOX_HARNESS_GATEWAY_CONTROL_KEY"]=secrets.token_urlsafe(48)
    values["SANDBOX_HARNESS_SESSION_SIGNING_KEY"]=secrets.token_urlsafe(48)
    controlled=None
    if args.controlled_runtime_source:
        source=args.controlled_runtime_source.resolve()
        if not source.is_relative_to(Path('/data2/wujing03')):
            raise ValueError('controlled_runtime_source_outside_data2')
        module,manifest=controlled_runtime_value(source);controlled=(module,source,manifest)
        values['SANDBOX_HARNESS_CONTROLLED_RUNTIME_ROOT']=str(EXPECTED_DATA_ROOT/'provider-runtime')
        values['SANDBOX_HARNESS_CONTROLLED_RUNTIME_DIGEST']=manifest['digest']
    elif not args.check_only:
        raise ValueError('controlled_runtime_source_required')
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
    module,source,manifest=controlled
    module.publish_entry(source,EXPECTED_DATA_ROOT/'provider-runtime')
    manifest_path=target.parent/'controlled-runtime-manifest.json'
    manifest_path.write_text(json.dumps(manifest,ensure_ascii=True,indent=2,sort_keys=True)+'\n')
    manifest_path.chmod(0o444)
    temporary=target.with_suffix(".tmp");temporary.write_text(render_environment(values));temporary.chmod(0o640)
    temporary.replace(target);os.chmod(target,stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP)
    key_target=target.parent/"runtime-bundle-signing.pem"
    shutil.copyfile(args.runtime_signing_key,key_target);key_target.chmod(0o600)
    print("Configured isolated environment at "+str(target))


if __name__=="__main__":main()
