from __future__ import annotations

import argparse
import json
import os

import httpx


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(prog='sandbox-artifact')
    emit=parser.add_subparsers(dest='command',required=True).add_parser('emit')
    emit.add_argument('--job-id',required=True);emit.add_argument('--generation',type=int,required=True)
    emit.add_argument('--path',required=True);emit.add_argument('--role',required=True)
    emit.add_argument('--item-key',required=True);emit.add_argument('--position',type=int,required=True)
    emit.add_argument('--filename',required=True)
    emit.add_argument('--disposition',choices=('candidate','workspace','delivery'),required=True)
    emit.add_argument('--mime',default='application/octet-stream')
    args=parser.parse_args(argv)
    runtime=os.environ.get('SANDBOX_RUNTIME_URL','http://127.0.0.1:8080').rstrip('/')
    response=httpx.post(runtime+'/v1/job-artifacts/stage',json={
        'job_id':args.job_id,'generation':args.generation,'path':args.path,
        'artifact_role':args.role,'item_key':args.item_key,'position':args.position,
        'filename':args.filename,'output_disposition':args.disposition,'mime':args.mime,
    },timeout=3600)
    print(response.text)
    return 0 if response.is_success else 1


if __name__=='__main__':raise SystemExit(main())
