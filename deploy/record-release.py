#!/usr/bin/env python3
"""Pin service version without granting the service account access to .git or secrets."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--env-file',default='/data2/wujing03/sandbox-harness-platform-parallel/.data/config/platform.env')
    parser.add_argument('--code-root',type=Path,help='Clean checkout to deploy; defaults to this repository')
    parser.add_argument('--record-file',type=Path,help='Override release manifest path')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    code_root=(args.code_root or root).resolve()
    if not (code_root/'src/sandbox_harness/main.py').is_file():raise ValueError('Invalid code root')
    subprocess.run(['git','diff','--quiet'],cwd=code_root,check=True)
    subprocess.run(['git','diff','--cached','--quiet'],cwd=code_root,check=True)
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=code_root,text=True).strip()
    if not re.fullmatch('[0-9a-f]{40}',revision):raise ValueError('Invalid Git revision')
    bundle_dir=root/'.data'/'runtime-bundles'
    bundle_dir.mkdir(parents=True,exist_ok=True)
    os.chmod(bundle_dir,0o755)
    bundle=json.loads(subprocess.check_output([os.sys.executable,
        str(code_root/'runtime'/'build_runtime_bundle.py'),'--output-dir',str(bundle_dir),
        '--signing-key',str(root/'.data'/'config'/'runtime-bundle-signing.pem')],text=True))
    code_bundle_dir=root/'.data'/'runtime-code-bundles'
    code_bundle_dir.mkdir(parents=True,exist_ok=True);os.chmod(code_bundle_dir,0o755)
    code_bundle=json.loads(subprocess.check_output([os.sys.executable,
        str(code_root/'runtime'/'build_runtime_code_bundle.py'),'--output-dir',str(code_bundle_dir),
        '--signing-key',str(root/'.data'/'config'/'runtime-bundle-signing.pem')],text=True))
    job_catalog_digest=hashlib.sha256(
        (code_root/'deploy'/'job-catalog.example.json').read_bytes()).hexdigest()
    profile_catalog_digest=hashlib.sha256(
        (code_root/'deploy'/'profile-catalog.example.json').read_bytes()).hexdigest()
    target=Path(args.env_file);info=target.stat();source=target.read_text()
    for key,value in {'SANDBOX_HARNESS_RELEASE':revision[:12],'PYTHONPATH':str(code_root/'src'),
            'SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR':str(bundle_dir),
            'SANDBOX_HARNESS_RUNTIME_BUNDLE_DIGEST':bundle['sha256'],
            'SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR':str(code_bundle_dir),
            'SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIGEST':code_bundle['sha256'],
            'SANDBOX_HARNESS_JOB_CATALOG_DIGEST':job_catalog_digest,
            'SANDBOX_HARNESS_PROFILE_CATALOG_DIGEST':profile_catalog_digest}.items():
        line=key+'='+value
        source=re.sub('^'+key+'=.*$',lambda match:line,source,flags=re.M) if re.search('^'+key+'=',source,re.M) else source.rstrip()+'\n'+line+'\n'
    with tempfile.NamedTemporaryFile(mode='w',dir=target.parent,delete=False) as out:
        temporary=Path(out.name)
        try:
            out.write(source);out.flush();os.fsync(out.fileno())
            os.fchmod(out.fileno(),stat.S_IMODE(info.st_mode));os.fchown(out.fileno(),info.st_uid,info.st_gid)
            temporary.replace(target)
        finally:temporary.unlink(missing_ok=True)
    record=args.record_file or root/'.data'/'release.json'
    record.parent.mkdir(parents=True,exist_ok=True)
    record.write_text(json.dumps({'revision':revision,'service_version':revision[:12],'code_root':str(code_root),
        'runtime_bundle_digest':bundle['sha256'],
        'runtime_code_bundle_digest':code_bundle['sha256'],
        'job_catalog_digest':job_catalog_digest,
        'profile_catalog_digest':profile_catalog_digest,
        'recorded_at':datetime.now(timezone.utc).isoformat()},indent=2))
    print('Service version pinned to '+revision[:12]+' runtime_code_bundle='+code_bundle['sha256']
        +' runtime_bundle='+bundle['sha256'])

if __name__=='__main__':main()
