#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json,re,subprocess
from pathlib import Path

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--matrix',type=Path,required=True)
    parser.add_argument('--template',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--signing-key',type=Path,required=True);args=parser.parse_args()
    value=json.loads(args.template.read_text());matrix={}
    for line in args.matrix.read_text().splitlines():
        profile,harness,reference=line.split('\t')
        match=re.search(r'@sha256:([0-9a-f]{64})$',reference)
        if not match:raise ValueError('Image reference is not digest pinned')
        matrix[(profile,harness)]=(reference,match.group(1))
    for profile in value['runtime_dependency_profiles']:
        identities=[(profile['id'],harness) for harness in profile['supported_harnesses']]
        if any(identity not in matrix for identity in identities):
            raise ValueError('Image matrix is incomplete for '+profile['id'])
        profile['provider_releases']={'opensandbox':{harness:{
            'ref':matrix[(profile['id'],harness)][0],
            'revision':'sha256:'+matrix[(profile['id'],harness)][1],
            'artifact_digest':matrix[(profile['id'],harness)][1]}
            for harness in profile['supported_harnesses']}}
    payload=(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode()
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_bytes(payload);args.output.chmod(0o444)
    signature=subprocess.run(['openssl','pkeyutl','-sign','-rawin','-inkey',str(args.signing_key),
        '-in',str(args.output)],stdout=subprocess.PIPE,check=True).stdout
    signature_path=args.output.with_suffix(args.output.suffix+'.sig');signature_path.write_bytes(signature);signature_path.chmod(0o444)
    print(json.dumps({'path':str(args.output),'signature_path':str(signature_path),
        'sha256':hashlib.sha256(payload).hexdigest(),'profiles':len(value['runtime_dependency_profiles'])},sort_keys=True))
if __name__=='__main__':main()
