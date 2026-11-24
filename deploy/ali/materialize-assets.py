#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

import httpx


def _load(name:str,path:Path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


identity=_load("sandbox_harness_ali_identity",Path(__file__).with_name("identity.py"))
configure=_load("sandbox_harness_ali_configure",Path(__file__).with_name("configure.py"))
ROOT=identity.ROOT
DATA_ROOT=identity.DATA_ROOT


def canonical_json(value)->str:
    return json.dumps(value,ensure_ascii=True,sort_keys=True,separators=(",",":"))


def digest_bytes(data:bytes)->str:
    return hashlib.sha256(data).hexdigest()


def verified_copy(source:Path,target:Path,digest:str)->None:
    if not source.is_file() or digest_bytes(source.read_bytes())!=digest:
        raise ValueError(f"digest_mismatch:{source}")
    target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_suffix(target.suffix+".tmp")
    shutil.copyfile(source,temporary);temporary.chmod(0o644);temporary.replace(target)


def rewrite_profile_catalog(value:dict)->dict:
    rewritten=json.loads(json.dumps(value))
    old_prefix="127.0.0.1:25001/sandbox-harness-parallel/"
    new_prefix=identity.REGISTRY_PREFIX+"/"
    for profile in rewritten.get("runtime_dependency_profiles",[]):
        releases=profile.get("provider_releases",{}).get("opensandbox",{})
        for harness,release in list(releases.items()):
            reference=release["ref"]
            if not reference.startswith(old_prefix):
                raise ValueError("unexpected_registry_reference")
            digest=release["artifact_digest"]
            if not reference.endswith("@sha256:"+digest):
                raise ValueError("digest_mismatch:image_reference")
            release["ref"]=new_prefix+reference.removeprefix(old_prefix)
    return rewritten


def merge_provider_releases(value:dict,overlay:dict)->dict:
    rewritten=json.loads(json.dumps(value))
    if overlay.get("schema_version")!=1 or overlay.get("provider")!="aliyun":
        raise ValueError("invalid_provider_release_overlay")
    profiles={row["id"]:row for row in rewritten.get("runtime_dependency_profiles",[])}
    overlay_profiles=overlay.get("profiles",{})
    if not profiles or set(overlay_profiles)!=set(profiles):
        raise ValueError("provider_release_profile_matrix_incomplete")
    for identity,profile in profiles.items():
        harnesses=set(profile.get("supported_harnesses",[]))
        releases=overlay_profiles[identity]
        if not harnesses or set(releases)!=harnesses:
            raise ValueError("provider_release_harness_matrix_incomplete")
        opensandbox=profile.get("provider_releases",{}).get("opensandbox",{})
        if set(opensandbox)!=harnesses:
            raise ValueError("opensandbox_release_matrix_incomplete")
        for harness,release in releases.items():
            if (not release.get("ref") or not release.get("revision")
                    or len(release.get("artifact_digest",""))!=64):
                raise ValueError("provider_release_identity_invalid")
            if release["artifact_digest"]!=opensandbox[harness].get("artifact_digest"):
                raise ValueError("provider_release_artifact_digest_mismatch")
        profile["provider_releases"]["aliyun"]=json.loads(json.dumps(releases))
    return rewritten


def promote_registry_manifest(source_ref:str,target_ref:str)->None:
    source_host_repo,source_digest=source_ref.split("@sha256:",1)
    target_host_repo,target_digest=target_ref.split("@sha256:",1)
    if source_digest!=target_digest:raise ValueError("digest_mismatch:registry_target")
    source_host,_=source_host_repo.split("/",1)
    target_host,target_repo=target_host_repo.split("/",1)
    if source_host!=target_host:raise ValueError("registry_host_mismatch")
    headers={"Accept":",".join((
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json"))}
    docker="/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker"
    image_id=subprocess.check_output([docker,"image","inspect",source_ref,
        "--format","{{.Id}}"],text=True).strip()
    target_tag=f"{target_host_repo}:ali-{source_digest[:16]}"
    subprocess.run([docker,"tag",image_id,target_tag],check=True)
    subprocess.run([docker,"push",target_tag],check=True)
    with httpx.Client(base_url=f"http://{source_host}",timeout=60,trust_env=False) as client:
        checked=client.head(f"/v2/{target_repo}/manifests/sha256:{target_digest}",headers=headers)
        checked.raise_for_status()


def _copy_bundle(source_dir:Path,target_dir:Path,digest:str)->None:
    verified_copy(source_dir/(digest+".tar"),target_dir/(digest+".tar"),digest)
    signature=source_dir/(digest+".sig")
    if not signature.is_file():raise ValueError("missing_bundle_signature")
    target=(target_dir/(digest+".sig"));target.parent.mkdir(parents=True,exist_ok=True)
    if signature.resolve()!=target.resolve():shutil.copyfile(signature,target)
    target.chmod(0o644)


def _materialize_controlled(source:Path,digest:str,force:bool)->Path:
    resolved=source.resolve()
    if resolved.name!=digest:raise ValueError("digest_mismatch:controlled_runtime")
    target=DATA_ROOT/"controlled-runtime"/digest
    if target.exists() and force:shutil.rmtree(target)
    if not target.exists():
        shutil.copytree(resolved,target,symlinks=True,copy_function=os.link)
    link=DATA_ROOT/"provider-runtime"
    temporary=DATA_ROOT/"provider-runtime.tmp"
    temporary.unlink(missing_ok=True);temporary.symlink_to(Path("controlled-runtime")/digest)
    temporary.replace(link)
    return target


def materialize(source_env:Path,target_env:Path,*,promote_images:bool=False,force:bool=False)->dict[str,str]:
    source=configure.parse_environment(source_env);target=configure.parse_environment(target_env)
    provider=target.get("SANDBOX_HARNESS_BACKEND","opensandbox")
    job_digest=source["SANDBOX_HARNESS_JOB_CATALOG_DIGEST"]
    profile_digest=source["SANDBOX_HARNESS_PROFILE_CATALOG_DIGEST"]
    runtime_digest=source["SANDBOX_HARNESS_RUNTIME_BUNDLE_DIGEST"]
    code_digest=source["SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIGEST"]
    controlled_digest=source["SANDBOX_HARNESS_CONTROLLED_RUNTIME_DIGEST"]
    job_target=DATA_ROOT/"catalogs/job-catalog.json"
    verified_copy(Path(source["SANDBOX_HARNESS_JOB_CATALOG_PATH"]),job_target,job_digest)
    profile_source=Path(source["SANDBOX_HARNESS_PROFILE_CATALOG_PATH"])
    if digest_bytes(profile_source.read_bytes())!=profile_digest:raise ValueError("digest_mismatch:profile_catalog")
    original=json.loads(profile_source.read_text());rewritten=rewrite_profile_catalog(original)
    overlay=json.loads((ROOT/"deploy/ali/provider-releases.json").read_text())
    rewritten=merge_provider_releases(rewritten,overlay)
    if promote_images and provider=="opensandbox":
        original_profiles={row["id"]:row for row in original["runtime_dependency_profiles"]}
        for profile in rewritten["runtime_dependency_profiles"]:
            source_profile=original_profiles[profile["id"]]
            target_releases=profile["provider_releases"]["opensandbox"]
            source_releases=source_profile["provider_releases"]["opensandbox"]
            for harness,target in target_releases.items():
                promote_registry_manifest(source_releases[harness]["ref"],target["ref"])
    profile_raw=canonical_json(rewritten).encode();new_profile_digest=digest_bytes(profile_raw)
    profile_target=DATA_ROOT/"catalogs/profile-catalog.json"
    profile_target.parent.mkdir(parents=True,exist_ok=True);profile_target.write_bytes(profile_raw);profile_target.chmod(0o644)
    _copy_bundle(Path(source["SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR"]),DATA_ROOT/"runtime-bundles",runtime_digest)
    _copy_bundle(Path(source["SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR"]),DATA_ROOT/"runtime-code-bundles",code_digest)
    _materialize_controlled(Path(source["SANDBOX_HARNESS_CONTROLLED_RUNTIME_ROOT"]),controlled_digest,force)
    primary=next(row for row in rewritten["runtime_dependency_profiles"] if row["id"]=="primary-full-cpu")
    primary_releases=primary["provider_releases"].get(provider,{})
    if set(primary_releases)!={"codex","claude","deepseek"}:
        raise ValueError("primary_provider_release_matrix_incomplete")
    target.update({
        "SANDBOX_HARNESS_JOB_CATALOG_PATH":str(job_target),
        "SANDBOX_HARNESS_JOB_CATALOG_DIGEST":job_digest,
        "SANDBOX_HARNESS_PROFILE_CATALOG_PATH":str(profile_target),
        "SANDBOX_HARNESS_PROFILE_CATALOG_DIGEST":new_profile_digest,
        "SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR":str(DATA_ROOT/"runtime-bundles"),
        "SANDBOX_HARNESS_RUNTIME_BUNDLE_DIGEST":runtime_digest,
        "SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR":str(DATA_ROOT/"runtime-code-bundles"),
        "SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIGEST":code_digest,
        "SANDBOX_HARNESS_CONTROLLED_RUNTIME_ROOT":str(DATA_ROOT/"provider-runtime"),
        "SANDBOX_HARNESS_CONTROLLED_RUNTIME_DIGEST":controlled_digest,
        "SANDBOX_HARNESS_RUNTIME_IMAGE_CODEX":primary_releases["codex"]["ref"],
        "SANDBOX_HARNESS_RUNTIME_IMAGE_CLAUDE":primary_releases["claude"]["ref"],
        "SANDBOX_HARNESS_RUNTIME_IMAGE_DEEPSEEK":primary_releases["deepseek"]["ref"],
    })
    raw=configure.render_environment(target)
    source_mutable=str(ROOT.with_name("sandbox-harness-platform-parallel")/".data")
    if source_mutable in raw:raise ValueError("mutable_source_path_leak")
    temporary=target_env.with_suffix(".tmp");temporary.write_text(raw);temporary.chmod(0o600)
    temporary.replace(target_env);os.chmod(target_env,0o600)
    return target


def main()->None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-env",type=Path,required=True)
    parser.add_argument("--target-env",type=Path,default=DATA_ROOT/"config/platform.env")
    parser.add_argument("--promote-images",action="store_true")
    parser.add_argument("--force",action="store_true")
    args=parser.parse_args();configure.ensure_checkout()
    materialize(args.source_env,args.target_env,promote_images=args.promote_images,force=args.force)
    print("materialized independent Ali runtime assets")


if __name__=="__main__":main()
