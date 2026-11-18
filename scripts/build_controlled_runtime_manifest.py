#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


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


def digest(path:Path)->str:
    value=hashlib.sha256()
    with path.open('rb') as stream:
        while chunk:=stream.read(8*1024*1024):value.update(chunk)
    return value.hexdigest()


def build_manifest(source:Path)->dict:
    source=source.resolve();relative=[*CRITICAL_FILES,*ENVIRONMENT_MARKERS]
    relative.extend(str(path.relative_to(source)) for path in sorted(
        (source/'lib-director-vF/src').rglob('*.py')))
    rows=[]
    for name in dict.fromkeys(relative):
        path=source/name
        if not path.is_file():raise FileNotFoundError(name)
        rows.append({'path':name,'size':path.stat().st_size,'sha256':digest(path)})
    payload={'schema_version':1,'source':str(source),'files':rows}
    encoded=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
    return {**payload,'digest':hashlib.sha256(encoded).hexdigest()}


def publish_entry(source:Path,entry:Path)->None:
    source=source.resolve();entry=entry.absolute();entry.parent.mkdir(parents=True,exist_ok=True)
    if entry.is_symlink():
        if entry.resolve()!=source:raise FileExistsError(str(entry))
        return
    if entry.exists():raise FileExistsError(str(entry))
    os.symlink(source,entry,target_is_directory=True)


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--entry',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();value=build_manifest(args.source);publish_entry(args.source,args.entry)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(value,ensure_ascii=True,indent=2,sort_keys=True)+'\n')
    args.output.chmod(0o444);print(json.dumps({'digest':value['digest'],'files':len(value['files']),
        'entry':str(args.entry),'manifest':str(args.output)},sort_keys=True))


if __name__=='__main__':main()
