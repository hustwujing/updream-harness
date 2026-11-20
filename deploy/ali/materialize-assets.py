#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil

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
        refs=profile.get("image_refs",{})
        for harness,reference in list(refs.items()):
            if not reference.startswith(old_prefix):
                raise ValueError("unexpected_registry_reference")
            digest=profile["image_digests"][harness]
            if not reference.endswith("@sha256:"+digest):
                raise ValueError("digest_mismatch:image_reference")
            refs[harness]=new_prefix+reference.removeprefix(old_prefix)
    return rewritten


def promote_registry_manifest(source_ref:str,target_ref:str)->None:
    source_host_repo,source_digest=source_ref.split("@sha256:",1)
    target_host_repo,target_digest=target_ref.split("@sha256:",1)
    if source_digest!=target_digest:raise ValueError("digest_mismatch:registry_target")
    source_host,source_repo=source_host_repo.split("/",1)
    target_host,target_repo=target_host_repo.split("/",1)
    if source_host!=target_host:raise ValueError("registry_host_mismatch")
    headers={"Accept":",".join((
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json"))}
    with httpx.Client(base_url=f"http://{source_host}",timeout=60,trust_env=False) as client:
        response=client.get(f"/v2/{source_repo}/manifests/sha256:{source_digest}",headers=headers)
        response.raise_for_status()
        if response.headers.get("docker-content-digest") not in (None,"sha256:"+source_digest):
            raise ValueError("digest_mismatch:registry_source")
        media=response.headers.get("content-type","application/vnd.docker.distribution.manifest.v2+json")
        tag="ali-"+source_digest[:16]
        uploaded=client.put(f"/v2/{target_repo}/manifests/{tag}",content=response.content,
            headers={"Content-Type":media})
        uploaded.raise_for_status()
        checked=client.head(f"/v2/{target_repo}/manifests/sha256:{target_digest}",headers=headers)
        checked.raise_for_status()


def _copy_bundle(source_dir:Path,target_dir:Path,digest:str)->None:
    verified_copy(source_dir/(digest+".tar"),target_dir/(digest+".tar"),digest)
    signature=source_dir/(digest+".sig")
    if not signature.is_file():raise ValueError("missing_bundle_signature")
    target=(target_dir/(digest+".sig"));target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(signature,target);target.chmod(0o644)


def _materialize_controlled(source:Path,digest:str,force:bool)->Path:
    resolved=source.resolve()
    if resolved.name!=digest:raise ValueError("digest_mismatch:controlled_runtime")
    target=DATA_ROOT/"controlled-runtime"/digest
    if target.exists() and force:shutil.rmtree(target)
    if not target.exists():shutil.copytree(resolved,target,symlinks=True)
    link=DATA_ROOT/"provider-runtime"
    temporary=DATA_ROOT/"provider-runtime.tmp"
    temporary.unlink(missing_ok=True);temporary.symlink_to(Path("controlled-runtime")/digest)
    temporary.replace(link)
    return target


def materialize(source_env:Path,target_env:Path,*,promote_images:bool=False,force:bool=False)->dict[str,str]:
    source=configure.parse_environment(source_env);target=configure.parse_environment(target_env)
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
    if promote_images:
        original_profiles={row["id"]:row for row in original["runtime_dependency_profiles"]}
        for profile in rewritten["runtime_dependency_profiles"]:
            source_profile=original_profiles[profile["id"]]
            for harness,target_ref in profile["image_refs"].items():
                promote_registry_manifest(source_profile["image_refs"][harness],target_ref)
    profile_raw=canonical_json(rewritten).encode();new_profile_digest=digest_bytes(profile_raw)
    profile_target=DATA_ROOT/"catalogs/profile-catalog.json"
    profile_target.parent.mkdir(parents=True,exist_ok=True);profile_target.write_bytes(profile_raw);profile_target.chmod(0o644)
    _copy_bundle(Path(source["SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR"]),DATA_ROOT/"runtime-bundles",runtime_digest)
    _copy_bundle(Path(source["SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR"]),DATA_ROOT/"runtime-code-bundles",code_digest)
    _materialize_controlled(Path(source["SANDBOX_HARNESS_CONTROLLED_RUNTIME_ROOT"]),controlled_digest,force)
    primary=next(row for row in rewritten["runtime_dependency_profiles"] if row["id"]=="primary-full-cpu")
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
        "SANDBOX_HARNESS_RUNTIME_IMAGE_CODEX":primary["image_refs"]["codex"],
        "SANDBOX_HARNESS_RUNTIME_IMAGE_CLAUDE":primary["image_refs"]["claude"],
        "SANDBOX_HARNESS_RUNTIME_IMAGE_DEEPSEEK":primary["image_refs"]["deepseek"],
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
