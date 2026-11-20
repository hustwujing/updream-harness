#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import uuid


CRITICAL_FILES=(
    'models/sam3/sam3.pt',
    'models/ZIM/zim-anything-vitl/zim_vit_l_2092/decoder.onnx',
    'models/ZIM/zim-anything-vitl/zim_vit_l_2092/encoder.onnx',
    'models/media-foundation/faster-whisper-small/config.json',
    'models/media-foundation/faster-whisper-small/model.bin',
    'models/media-foundation/faster-whisper-small/tokenizer.json',
    'models/media-foundation/faster-whisper-small/vocabulary.txt',
)
ENVIRONMENT_MARKERS=(
    'envs/sam3/conda-meta/history',
    'envs/face-makeup/conda-meta/history',
    'envs/media-foundation/conda-meta/history',
)
RUNTIME_TREES=(
    'models/sam3/sam3',
    'models/ZIM/source/zim_anything',
    'models/ZIM/source/zim_config',
    'lib-director-vF/src/libdirector_vf',
)


def digest(path:Path)->str:
    value=hashlib.sha256()
    with path.open('rb') as stream:
        while chunk:=stream.read(8*1024*1024):value.update(chunk)
    return value.hexdigest()


def build_manifest(source:Path)->dict:
    source=source.resolve();relative=[*CRITICAL_FILES,*ENVIRONMENT_MARKERS]
    for name in RUNTIME_TREES:
        tree=source/name
        if not tree.is_dir():raise FileNotFoundError(name)
        files=[path for path in tree.rglob('*') if path.is_file()
            and '.git' not in path.relative_to(tree).parts
            and '__pycache__' not in path.relative_to(tree).parts
            and not path.name.startswith('._') and path.suffix!='.pyc']
        if not files:raise FileNotFoundError(name+':empty')
        relative.extend(str(path.relative_to(source)) for path in sorted(files))
    rows=[]
    for name in dict.fromkeys(relative):
        path=source/name
        if not path.is_file():raise FileNotFoundError(name)
        rows.append({'path':name,'size':path.stat().st_size,'sha256':digest(path)})
    payload={'schema_version':1,'source':str(source),'files':rows}
    encoded=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
    return {**payload,'digest':hashlib.sha256(encoded).hexdigest()}


def _copy_tree(source:Path,destination:Path)->None:
    destination.mkdir(parents=True)
    subprocess.run(('cp','-a','--reflink=auto','--',str(source)+'/.',str(destination)),check=True)


def _copy_external_target(source:Path,destination:Path)->None:
    subprocess.run(('cp','-aL','--reflink=auto','--',str(source),str(destination)),check=True)


def _materialize_external_links(snapshot:Path)->None:
    root=snapshot.resolve()
    for path in sorted(snapshot.rglob('*')):
        if not path.is_symlink():continue
        target=path.resolve(strict=True)
        if target.is_relative_to(root):continue
        path.unlink();_copy_external_target(target,path)


def _assert_no_external_links(snapshot:Path)->None:
    root=snapshot.resolve()
    for path in snapshot.rglob('*'):
        if path.is_symlink() and not path.resolve(strict=True).is_relative_to(root):
            raise ValueError('controlled_runtime_external_symlink:'+str(path.relative_to(snapshot)))


def _make_read_only(snapshot:Path)->None:
    for root,directories,files in os.walk(snapshot,topdown=False,followlinks=False):
        for name in files:
            path=Path(root)/name
            if not path.is_symlink():
                executable=bool(path.stat().st_mode & (stat.S_IXUSR|stat.S_IXGRP|stat.S_IXOTH))
                path.chmod(0o555 if executable else 0o444)
        for name in directories:
            path=Path(root)/name
            if not path.is_symlink():path.chmod(0o555)
    snapshot.chmod(0o555)


def _verify_snapshot(snapshot:Path,manifest:dict)->None:
    rebuilt=build_manifest(snapshot)
    if rebuilt['files']!=manifest['files']:
        raise ValueError('controlled_runtime_snapshot_corrupt')
    _assert_no_external_links(snapshot)


def _publish_entry(entry:Path,snapshot:Path)->None:
    entry=entry.absolute();entry.parent.mkdir(parents=True,exist_ok=True)
    if entry.exists() and not entry.is_symlink():raise FileExistsError(str(entry))
    if entry.is_symlink() and entry.resolve()==snapshot.resolve():return
    temporary=entry.parent/('.'+entry.name+'.'+uuid.uuid4().hex+'.tmp')
    temporary.symlink_to(os.path.relpath(snapshot,entry.parent),target_is_directory=True)
    try:os.replace(temporary,entry)
    finally:
        if temporary.is_symlink():temporary.unlink()


def materialize_entry(source:Path,entry:Path,manifest:dict)->Path:
    source=source.resolve();entry=entry.absolute()
    digest=str(manifest.get('digest',''))
    if len(digest)!=64 or any(value not in '0123456789abcdef' for value in digest):
        raise ValueError('controlled_runtime_manifest_digest_invalid')
    snapshot_root=entry.parent/'controlled-runtime';snapshot=snapshot_root/digest
    snapshot_root.mkdir(parents=True,exist_ok=True)
    if snapshot.exists():
        _verify_snapshot(snapshot,manifest);_publish_entry(entry,snapshot);return snapshot
    staging=snapshot_root/('.'+digest+'.'+uuid.uuid4().hex+'.tmp')
    try:
        _copy_tree(source,staging)
        _materialize_external_links(staging)
        _verify_snapshot(staging,manifest)
        _make_read_only(staging)
        os.replace(staging,snapshot)
    finally:
        if staging.exists():shutil.rmtree(staging)
    _publish_entry(entry,snapshot);return snapshot


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--entry',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();value=build_manifest(args.source);materialize_entry(args.source,args.entry,value)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(value,ensure_ascii=True,indent=2,sort_keys=True)+'\n')
    args.output.chmod(0o444);print(json.dumps({'digest':value['digest'],'files':len(value['files']),
        'entry':str(args.entry),'manifest':str(args.output)},sort_keys=True))


if __name__=='__main__':main()
