from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import uuid

import httpx

from .state import capability_token_file


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(prog='sandbox-artifact')
    commands=parser.add_subparsers(dest='command',required=True)
    emit=commands.add_parser('emit')
    emit.add_argument('--job-id',required=True);emit.add_argument('--generation',type=int,required=True)
    emit.add_argument('--path',required=True);emit.add_argument('--role',required=True)
    emit.add_argument('--item-key',required=True);emit.add_argument('--position',type=int,required=True)
    emit.add_argument('--filename',required=True)
    emit.add_argument('--disposition',choices=('candidate','workspace','delivery'),required=True)
    emit.add_argument('--mime',default='application/octet-stream')
    fetch=commands.add_parser('fetch')
    fetch.add_argument('--artifact-id',required=True)
    fetch.add_argument('--output',required=True)
    args=parser.parse_args(argv)
    if args.command=='fetch':
        output=Path(args.output)
        if output.is_dir() or not output.parent.is_dir():
            print(json.dumps({'error':'Artifact output must have an existing parent directory'}))
            return 2
        gateway=os.environ.get('HARNESS_GATEWAY_URL','').rstrip('/')
        token_file=capability_token_file()
        if not gateway or not token_file.is_file():
            print(json.dumps({'error':'Harness Gateway capability is not configured'}))
            return 2
        temporary=output.parent/('.'+output.name+'.'+uuid.uuid4().hex+'.partial')
        try:
            with httpx.stream('GET',gateway+f'/v1/job-artifacts/{args.artifact_id}/content',
                    headers={'Authorization':'Bearer '+token_file.read_text().strip()},timeout=3600) as response:
                if not response.is_success:
                    response.read()
                    print(response.text[:2000]);return 1
                raw_size=response.headers.get('content-length','')
                digest=response.headers.get('x-updream-artifact-sha256','')
                if not raw_size.isdigit() or len(digest)!=64:
                    print(json.dumps({'error':'Artifact identity headers are missing'}));return 1
                expected_size=int(raw_size);hasher=hashlib.sha256();size=0
                with temporary.open('xb') as stream:
                    for chunk in response.iter_bytes():
                        size+=len(chunk)
                        if size>expected_size:raise ValueError('artifact_size_mismatch')
                        hasher.update(chunk);stream.write(chunk)
                    stream.flush();os.fsync(stream.fileno())
                if size!=expected_size:raise ValueError('artifact_size_mismatch')
                if hasher.hexdigest()!=digest:raise ValueError('artifact_digest_mismatch')
            os.replace(temporary,output)
            print(json.dumps({'artifact_id':args.artifact_id,'path':str(output),
                'size':size,'sha256':digest}))
            return 0
        except (OSError,ValueError,httpx.HTTPError) as exc:
            print(json.dumps({'error':str(exc) or type(exc).__name__}));return 1
        finally:temporary.unlink(missing_ok=True)
    runtime=os.environ.get('SANDBOX_RUNTIME_URL','http://127.0.0.1:8080').rstrip('/')
    response=httpx.post(runtime+'/v1/job-artifacts/stage',json={
        'job_id':args.job_id,'generation':args.generation,'path':args.path,
        'artifact_role':args.role,'item_key':args.item_key,'position':args.position,
        'filename':args.filename,'output_disposition':args.disposition,'mime':args.mime,
    },timeout=3600)
    print(response.text)
    return 0 if response.is_success else 1


if __name__=='__main__':raise SystemExit(main())
