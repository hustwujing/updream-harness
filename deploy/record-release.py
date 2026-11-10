#!/usr/bin/env python3
"""Pin service version without granting the service account access to .git or secrets."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--env-file',default='/etc/sandbox-harness/platform.env')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    subprocess.run(['git','diff','--quiet'],cwd=root,check=True)
    subprocess.run(['git','diff','--cached','--quiet'],cwd=root,check=True)
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    if not re.fullmatch('[0-9a-f]{40}',revision):raise ValueError('Invalid Git revision')
    target=Path(args.env_file);info=target.stat();source=target.read_text()
    key='SANDBOX_HARNESS_RELEASE';line=key+'='+revision[:12]
    source=re.sub('^'+key+'=.*$',line,source,flags=re.M) if re.search('^'+key+'=',source,re.M) else source.rstrip()+'\n'+line+'\n'
    with tempfile.NamedTemporaryFile(mode='w',dir=target.parent,delete=False) as out:
        temporary=Path(out.name)
        try:
            out.write(source);out.flush();os.fsync(out.fileno())
            os.fchmod(out.fileno(),stat.S_IMODE(info.st_mode));os.fchown(out.fileno(),info.st_uid,info.st_gid)
            temporary.replace(target)
        finally:temporary.unlink(missing_ok=True)
    record=root/'.data'/'release.json'
    record.write_text(json.dumps({'revision':revision,'service_version':revision[:12],
        'recorded_at':datetime.now(timezone.utc).isoformat()},indent=2))
    print('Service version pinned to '+revision[:12])

if __name__=='__main__':main()
