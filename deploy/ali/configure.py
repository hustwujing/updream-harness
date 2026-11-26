#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import pwd
import grp
import secrets
import socket
import stat
import subprocess
from urllib.parse import quote


_identity_spec=importlib.util.spec_from_file_location(
    "sandbox_harness_ali_identity",Path(__file__).with_name("identity.py"))
identity=importlib.util.module_from_spec(_identity_spec)
_identity_spec.loader.exec_module(identity)

ROOT=identity.ROOT
DATA_ROOT=identity.DATA_ROOT

SHARED_KEYS={
    "AWS_ACCESS_KEY_ID","AWS_DEFAULT_REGION","AWS_SECRET_ACCESS_KEY",
    "SANDBOX_HARNESS_OPENSANDBOX_API_KEY","SANDBOX_HARNESS_OPENSANDBOX_URL",
    "SANDBOX_HARNESS_ALIYUN_SANDBOX_API_KEY","SANDBOX_HARNESS_ALIYUN_SANDBOX_API_URL",
    "SANDBOX_HARNESS_ALIYUN_SANDBOX_DOMAIN","SANDBOX_HARNESS_ALIYUN_SANDBOX_REQUEST_TIMEOUT_SECONDS",
    "SANDBOX_HARNESS_UPSTREAM_CODEX","SANDBOX_HARNESS_UPSTREAM_CODEX_KEY",
    "SANDBOX_HARNESS_UPSTREAM_CLAUDE","SANDBOX_HARNESS_UPSTREAM_CLAUDE_KEY",
    "SANDBOX_HARNESS_UPSTREAM_DEEPSEEK","SANDBOX_HARNESS_UPSTREAM_DEEPSEEK_KEY",
    "SANDBOX_HARNESS_UPSTREAM_IMAGE","SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY",
    "SANDBOX_HARNESS_UPSTREAM_VIDEO","SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY",
    "SANDBOX_HARNESS_UPSTREAM_AUDIO","SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY",
    "SANDBOX_HARNESS_MODEL_ROUTES_JSON","SANDBOX_HARNESS_MEDIA_CATALOG_JSON",
    "SANDBOX_HARNESS_ARK_ACCESS_KEY","SANDBOX_HARNESS_ARK_SECRET_KEY",
    "SANDBOX_HARNESS_ARK_PROJECT_NAME","SANDBOX_HARNESS_ARK_ASSET_GROUP_ID",
    "SANDBOX_HARNESS_ARK_IMAGE_MODEL_SEEDREAM_5_0","SANDBOX_HARNESS_ARK_VIDEO_MODEL_SED2",
    "SANDBOX_HARNESS_TRIPO_API_KEY","SANDBOX_HARNESS_TRIPO_BASE_URL",
    "SANDBOX_HARNESS_TRIPO_POLL_SECONDS","SANDBOX_HARNESS_TRIPO_SUBMIT_MAX_RETRIES",
    "SANDBOX_HARNESS_TRIPO_TASK_TIMEOUT_SECONDS","SANDBOX_HARNESS_TRIPO_WORKER_CONCURRENCY",
    "SANDBOX_HARNESS_OBJECT_BACKEND","SANDBOX_HARNESS_OBJECT_ENDPOINT",
    "SANDBOX_HARNESS_OBJECT_PUBLIC_ENDPOINT","SANDBOX_HARNESS_OBJECT_BUCKET",
    "SANDBOX_HARNESS_OBJECT_REGION","SANDBOX_HARNESS_MEDIA_REFERENCE_PREFIX",
    "SANDBOX_HARNESS_DASHSCOPE_VIDEO_CREATE_URL","SANDBOX_HARNESS_DASHSCOPE_VIDEO_TASK_URL",
    "SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST","SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST",
    "SANDBOX_HARNESS_MODEL_CATALOG_DISCOVERY","SANDBOX_HARNESS_MODEL_CATALOG_REFRESH_SECONDS",
    "SANDBOX_HARNESS_MODEL_CATALOG_TIMEOUT_SECONDS","SANDBOX_HARNESS_MODEL_PROBE_ENABLED",
    "SANDBOX_HARNESS_MODEL_PROBE_TIMEOUT_SECONDS","SANDBOX_HARNESS_MODEL_PROBE_CONCURRENCY",
    "SANDBOX_HARNESS_ORIGIN_CLAUDE_OPUS_5_5_KEY",
    "SANDBOX_HARNESS_WARM_POOL_CODEX","SANDBOX_HARNESS_WARM_POOL_CLAUDE",
    "SANDBOX_HARNESS_WARM_POOL_DEEPSEEK","SANDBOX_HARNESS_MAX_SANDBOXES",
    "SANDBOX_HARNESS_AUDIO_TASK_TIMEOUT_SECONDS","SANDBOX_HARNESS_AUDIO_WORKER_CONCURRENCY",
    "SANDBOX_HARNESS_VIDEO_TASK_TIMEOUT_SECONDS","SANDBOX_HARNESS_SITE_WORKER_CONCURRENCY",
}


def parse_environment(path:Path)->dict[str,str]:
    values={}
    for line in path.read_text().splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key,value=line.split("=",1);values[key]=value
    return values


def render_environment(values:dict[str,str])->str:
    return "".join(f"{key}={value}\n" for key,value in sorted(values.items()))


def build_environment(source:dict[str,str],*,token=lambda:secrets.token_urlsafe(48))->dict[str,str]:
    values={key:source[key] for key in SHARED_KEYS if source.get(key)}
    database_secret=token();gateway_secret=token();control_secret=token();session_secret=token()
    values.update({
        "SANDBOX_HARNESS_DATA_ROOT":str(DATA_ROOT),
        "SANDBOX_HARNESS_API_BIND_HOST":"127.0.0.1",
        "SANDBOX_HARNESS_API_BIND_PORT":str(identity.PORTS["api"]),
        "SANDBOX_HARNESS_ADMIN_DATABASE_URL":(
            f"postgresql+asyncpg://{identity.POSTGRES_IDENTITY}:"
            f"{quote(database_secret,safe='')}@127.0.0.1:{identity.PORTS['postgres']}/"
            "postgres"),
        "SANDBOX_HARNESS_NAMESPACE":identity.NAMESPACE,
        "SANDBOX_HARNESS_BACKEND":"opensandbox",
        "SANDBOX_HARNESS_REDIS_URL":source.get("SANDBOX_HARNESS_REDIS_URL","redis://127.0.0.1:6379/0"),
        "SANDBOX_HARNESS_REDIS_PREFIX":identity.REDIS_PREFIX,
        "SANDBOX_HARNESS_OBJECT_PREFIX":identity.OBJECT_PREFIX,
        "SANDBOX_HARNESS_GATEWAY_URL":f"http://172.31.0.1:{identity.PORTS['gateway']}",
        "SANDBOX_HARNESS_GATEWAY_BIND_HOST":"172.31.0.1",
        "SANDBOX_HARNESS_GATEWAY_BIND_PORT":str(identity.PORTS["gateway"]),
        "SANDBOX_HARNESS_GATEWAY_PROXY_PORT":str(identity.PORTS["proxy"]),
        "SANDBOX_HARNESS_ALIYUN_SANDBOX_GATEWAY_URL":f"http://10.194.49.8:{identity.PORTS['gateway']}",
        "SANDBOX_HARNESS_ALIYUN_SANDBOX_PROXY_URL":f"http://10.194.49.8:{identity.PORTS['proxy']}",
        "SANDBOX_HARNESS_GATEWAY_SIGNING_KEY":gateway_secret,
        "SANDBOX_HARNESS_GATEWAY_CONTROL_KEY":control_secret,
        "SANDBOX_HARNESS_SESSION_SIGNING_KEY":session_secret,
        "SANDBOX_HARNESS_JOB_NODE_ID":identity.JOB_NODE_ID,
        "SANDBOX_HARNESS_CONTROLLED_NODE_ID":identity.CONTROLLED_NODE_ID,
        "SANDBOX_HARNESS_JOB_CATALOG_PATH":str(DATA_ROOT/"catalogs/job-catalog.json"),
        "SANDBOX_HARNESS_PROFILE_CATALOG_PATH":str(DATA_ROOT/"catalogs/profile-catalog.json"),
        "SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR":str(DATA_ROOT/"runtime-bundles"),
        "SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR":str(DATA_ROOT/"runtime-code-bundles"),
        "SANDBOX_HARNESS_CONTROLLED_RUNTIME_ROOT":str(DATA_ROOT/"provider-runtime"),
        "SANDBOX_HARNESS_MODEL_CATALOG_SNAPSHOT_PATH":str(DATA_ROOT/"catalogs/model-catalog.json"),
        "SANDBOX_HARNESS_SITE_PUBLIC_BASE_PATH":"/sandbox-harness-platform-parallel-ali/sites",
    })
    return values


def ensure_checkout(root:Path=ROOT)->None:
    if root.resolve()!=ROOT:raise ValueError("unexpected_checkout")
    if not root.resolve().is_relative_to(Path("/data2/wujing03")):
        raise ValueError("path_outside_data2")
    branch=subprocess.check_output(["git","-C",str(root),"branch","--show-current"],text=True).strip()
    if branch!=identity.BRANCH:raise ValueError("unexpected_branch")


def check_port(port:int)->None:
    with socket.socket() as probe:
        try:probe.bind(("127.0.0.1",port))
        except OSError as exc:raise RuntimeError(f"port_in_use:{port}") from exc


def verify_digest(path:Path,digest:str)->None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
        raise ValueError("digest_mismatch")


def main()->None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-env",type=Path,required=True)
    parser.add_argument("--runtime-signing-key",type=Path)
    parser.add_argument("--controlled-runtime-source",type=Path)
    parser.add_argument("--check-only",action="store_true")
    parser.add_argument("--force",action="store_true")
    parser.add_argument("--allow-running",action="store_true")
    args=parser.parse_args()
    ensure_checkout()
    source=parse_environment(args.source_env)
    values=build_environment(source)
    if not args.allow_running:
        for port in identity.PORTS.values():check_port(port)
    if args.check_only:return
    target=DATA_ROOT/"config/platform.env"
    if target.exists() and not args.force:raise FileExistsError(target)
    service_paths=(DATA_ROOT/"logs",DATA_ROOT/"cache",DATA_ROOT/"pip-cache",
        DATA_ROOT/"npm-cache",DATA_ROOT/"playwright-browsers",DATA_ROOT/"objects",
        DATA_ROOT/"service-logs",DATA_ROOT/"tmp",DATA_ROOT/"image-decompose",
        DATA_ROOT/"preview-cache",DATA_ROOT/"services",DATA_ROOT/"catalogs",
        DATA_ROOT/"runtime-bundles",DATA_ROOT/"runtime-code-bundles")
    for path in (DATA_ROOT/"config",*service_paths,DATA_ROOT/"postgresql",DATA_ROOT/"postgresql/run"):
        path.mkdir(parents=True,exist_ok=True)
    service_uid=pwd.getpwnam("sandbox-harness").pw_uid
    service_gid=grp.getgrnam("sandbox-harness").gr_gid
    for path in service_paths:
        os.chown(path,service_uid,service_gid);path.chmod(0o700)
    temporary=target.with_suffix(".tmp")
    temporary.write_text(render_environment(values));temporary.chmod(0o600)
    temporary.replace(target);os.chown(target,service_uid,service_gid)
    os.chmod(target,stat.S_IRUSR|stat.S_IWUSR)
    print(f"configured {target}")


if __name__=="__main__":main()
