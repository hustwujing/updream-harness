"""Build and resolve the external image-inference release without copying weights."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


MODEL_PATHS=(
    "models/diffusion_models/qwen_image_layered_fp8mixed.safetensors",
    "models/text_encoders/qwen_2.5_vl_7b_fp8_scaled.safetensors",
    "models/vae/qwen_image_layered_vae.safetensors",
)


def _encode(value):
    return (json.dumps(value,sort_keys=True,separators=(",",":"))+"\n").encode()


def _sha(path):
    digest=hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b""):
            digest.update(chunk)
    return digest.hexdigest()


def _code_paths(root):
    values=[]
    for directory in ("service","media_core","web","python-packages"):
        if not (root/directory).is_dir():
            continue
        values.extend(path for path in (root/directory).rglob("*")
                      if path.is_file() and "__pycache__" not in path.parts
                      and path.suffix!=".pyc")
    values.extend(root/name for name in ("extra_model_paths.yaml","requirements.txt")
                  if (root/name).is_file())
    return sorted(values)


def _row(root,path,kind):
    return {"path":path.relative_to(root).as_posix(),"kind":kind,
            "size":path.stat().st_size,"sha256":_sha(path)}


def build(service_root:Path,python:Path,data_root:Path):
    service_root=service_root.resolve();python=python.resolve();data_root=data_root.resolve()
    if not (service_root/"service/app.py").is_file() or not python.is_file():
        raise ValueError("image_inference_input_missing")
    code=[_row(service_root,path,"code") for path in _code_paths(service_root)]
    models=[]
    for name in MODEL_PATHS:
        path=(service_root/name).resolve()
        if not path.is_file():raise ValueError("image_inference_model_missing")
        models.append({"path":name,"resolved":str(path),"kind":"model",
                       "size":path.stat().st_size,"sha256":_sha(path)})
    conda=python.parents[1]/"conda-meta"
    environment=[{"path":path.name,"size":path.stat().st_size,"sha256":_sha(path)}
                 for path in sorted(conda.glob("*.json"))]
    payload={"schema_version":1,"service":"image-inference","service_root":str(service_root),
        "python":str(python),"python_sha256":_sha(python),"code":code,"models":models,
        "environment":environment}
    digest=hashlib.sha256(_encode(payload)).hexdigest();payload["digest"]=digest
    release=data_root/"external-service-releases/image-inference"/digest
    release.mkdir(parents=True,exist_ok=True);release.chmod(0o555)
    manifest=release/"manifest.json";manifest.write_bytes(_encode(payload));manifest.chmod(0o444)
    current=data_root/"service-current";current.mkdir(parents=True,exist_ok=True);current.chmod(0o755)
    descriptor={"schema_version":1,"service":"image-inference","digest":digest,
                "manifest":str(manifest.resolve())}
    staging=current/"image-inference.json.tmp";staging.write_bytes(_encode(descriptor));staging.chmod(0o444)
    os.replace(staging,current/"image-inference.json")
    return manifest


def resolve(data_root:Path,full=False):
    data_root=data_root.resolve();descriptor=json.loads((data_root/"service-current/image-inference.json").read_text())
    if descriptor.get("service")!="image-inference":raise ValueError("image_inference_descriptor_invalid")
    manifest=Path(descriptor["manifest"]).resolve()
    if not manifest.is_relative_to(data_root/"external-service-releases/image-inference"):
        raise ValueError("image_inference_release_escape")
    value=json.loads(manifest.read_text());payload={key:item for key,item in value.items() if key!="digest"}
    digest=hashlib.sha256(_encode(payload)).hexdigest()
    if digest!=descriptor.get("digest") or digest!=value.get("digest") or manifest.parent.name!=digest:
        raise ValueError("image_inference_release_invalid")
    root=Path(value["service_root"]);python=Path(value["python"])
    if _sha(python)!=value["python_sha256"]:raise ValueError("image_inference_environment_changed")
    for row in value["code"]:
        path=root/row["path"]
        if not path.is_file() or path.stat().st_size!=row["size"] or _sha(path)!=row["sha256"]:
            raise ValueError("image_inference_code_changed")
    for row in value["models"]:
        path=Path(row["resolved"])
        if not path.is_file() or path.stat().st_size!=row["size"]:
            raise ValueError("image_inference_model_changed")
        if full and _sha(path)!=row["sha256"]:raise ValueError("image_inference_model_changed")
    return {"python":str(python),"service_root":str(root),"digest":digest}


def main():
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest="action",required=True)
    build_parser=sub.add_parser("build");build_parser.add_argument("--service-root",type=Path,required=True)
    build_parser.add_argument("--python",type=Path,required=True);build_parser.add_argument("--data-root",type=Path,required=True)
    resolve_parser=sub.add_parser("resolve");resolve_parser.add_argument("--data-root",type=Path,required=True)
    resolve_parser.add_argument("--full",action="store_true");args=parser.parse_args()
    if args.action=="build":print(build(args.service_root,args.python,args.data_root))
    else:
        value=resolve(args.data_root,args.full);print(value["python"]);print(value["service_root"]);print(value["digest"])


if __name__=="__main__":main()
