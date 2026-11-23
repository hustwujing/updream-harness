#!/usr/bin/env python3
"""Pin service version without granting the service account access to .git or secrets."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile


def publish_immutable(source: Path, target: Path, digest: str) -> None:
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:
            raise ValueError('Content-addressed release artifact is corrupted')
    elif source.resolve()!=target.resolve():
        shutil.copyfile(source,target)
    target.chmod(0o444)


def publish_profile_catalog(source: Path, destination_root: Path, public_key: Path) -> tuple[Path,str,str]:
    raw=source.read_bytes();digest=hashlib.sha256(raw).hexdigest();value=json.loads(raw)
    profiles=value.get('runtime_dependency_profiles',[])
    if value.get('schema_version')!=2:raise ValueError('Profile Catalog schema must be v2')
    if not profiles:raise ValueError('Profile Catalog has no Runtime Profiles')
    for profile in profiles:
        harnesses=set(profile.get('supported_harnesses',[]))
        providers=profile.get('provider_releases',{})
        if not harnesses or not providers:raise ValueError('Profile Catalog release matrix is incomplete')
        for provider,releases in providers.items():
            if set(releases)!=harnesses:
                raise ValueError('Profile Catalog release matrix is incomplete')
            if provider=='opensandbox' and any(not releases[harness]['ref'].endswith(
                    '@sha256:'+releases[harness]['artifact_digest']) for harness in harnesses):
                raise ValueError('Profile Catalog image matrix is not digest-pinned')
    signature=source.with_suffix(source.suffix+'.sig')
    if not signature.is_file():raise ValueError('Profile Catalog signature is unavailable')
    subprocess.run(['openssl','pkeyutl','-verify','-pubin','-inkey',str(public_key),'-rawin',
        '-in',str(source),'-sigfile',str(signature)],check=True,stdout=subprocess.DEVNULL)
    destination_root.mkdir(parents=True,exist_ok=True);destination_root.chmod(0o755)
    target=destination_root/(digest+'.json');target_signature=target.with_suffix('.json.sig')
    signature_digest=hashlib.sha256(signature.read_bytes()).hexdigest()
    publish_immutable(source,target,digest)
    publish_immutable(signature,target_signature,signature_digest)
    return target,digest,signature_digest


def primary_runtime_images(catalog: Path) -> dict[str,str]:
    value=json.loads(catalog.read_bytes())
    profiles={profile.get('id'):profile for profile in value.get('runtime_dependency_profiles',[])}
    primary=profiles.get('primary-full-cpu')
    if not primary:raise ValueError('Profile Catalog has no primary-full-cpu Runtime Profile')
    references={key:value['ref'] for key,value in
        primary.get('provider_releases',{}).get('opensandbox',{}).items()}
    required={'codex','claude','deepseek'}
    if set(references)!=required or any('@sha256:' not in references[harness] for harness in required):
        raise ValueError('Primary Runtime image matrix is incomplete')
    return references


def ensure_release_clean(code_root: Path) -> None:
    subprocess.run(['git','diff','--quiet'],cwd=code_root,check=True)
    subprocess.run(['git','diff','--cached','--quiet'],cwd=code_root,check=True)
    untracked=subprocess.check_output(['git','ls-files','--others','--exclude-standard','--',
        'src','runtime','deploy','web'],cwd=code_root,text=True).splitlines()
    if untracked:raise ValueError('Untracked deployable files: '+', '.join(sorted(untracked)))


def directory_digest(root: Path) -> str:
    files=sorted(path for path in root.rglob('*') if path.is_file())
    if not files:raise ValueError('Frontend distribution is empty')
    digest=hashlib.sha256()
    for path in files:
        relative=path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4,'big'));digest.update(relative)
        size=path.stat().st_size;digest.update(size.to_bytes(8,'big'))
        with path.open('rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def build_frontend(code_root: Path) -> str:
    subprocess.run(['npm','--prefix',str(code_root/'web'),'run','build'],cwd=code_root,check=True)
    output=code_root/'web'/'dist'
    if not (output/'index.html').is_file():raise ValueError('Frontend build did not produce index.html')
    return directory_digest(output)


def grant_service_release_read_access(code_root: Path) -> None:
    subprocess.run(['setfacl','-m','u:sandbox-harness:r-x',str(code_root)],check=True)
    subprocess.run(['setfacl','-m','u:sandbox-harness:r-x',str(code_root/'web')],check=True)
    subprocess.run(['setfacl','-R','-m','u:sandbox-harness:rX',str(code_root/'src'),
        str(code_root/'runtime'),str(code_root/'web'/'dist')],check=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--env-file',default='/data2/wujing03/sandbox-harness-platform-parallel/.data/config/platform.env')
    parser.add_argument('--code-root',type=Path,help='Clean checkout to deploy; defaults to this repository')
    parser.add_argument('--record-file',type=Path,help='Override release manifest path')
    parser.add_argument('--profile-catalog',type=Path,
        help='Complete signed Profile Catalog generated by runtime/build-images.sh')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    code_root=(args.code_root or root).resolve()
    if not (code_root/'src/sandbox_harness/main.py').is_file():raise ValueError('Invalid code root')
    ensure_release_clean(code_root)
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=code_root,text=True).strip()
    if not re.fullmatch('[0-9a-f]{40}',revision):raise ValueError('Invalid Git revision')
    web_dist_digest=build_frontend(code_root)
    grant_service_release_read_access(code_root)
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
    target=Path(args.env_file);info=target.stat();source=target.read_text()
    configured=dict(line.split('=',1) for line in source.splitlines() if '=' in line)
    profile_source=args.profile_catalog or Path(configured.get('SANDBOX_HARNESS_PROFILE_CATALOG_PATH',''))
    profile_catalog_path,profile_catalog_digest,profile_signature_digest=publish_profile_catalog(
        profile_source,root/'.data'/'profile-catalogs',code_root/'runtime'/'bundle_public_key.pem')
    images=primary_runtime_images(profile_catalog_path)
    for key,value in {'SANDBOX_HARNESS_RELEASE':revision[:12],
            'SANDBOX_HARNESS_WEB_DIST_DIGEST':web_dist_digest,'PYTHONPATH':str(code_root/'src'),
            'SANDBOX_HARNESS_RUNTIME_BUNDLE_DIR':str(bundle_dir),
            'SANDBOX_HARNESS_RUNTIME_BUNDLE_DIGEST':bundle['sha256'],
            'SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIR':str(code_bundle_dir),
            'SANDBOX_HARNESS_RUNTIME_CODE_BUNDLE_DIGEST':code_bundle['sha256'],
            'SANDBOX_HARNESS_JOB_CATALOG_DIGEST':job_catalog_digest,
            'SANDBOX_HARNESS_PROFILE_CATALOG_PATH':str(profile_catalog_path),
            'SANDBOX_HARNESS_PROFILE_CATALOG_DIGEST':profile_catalog_digest,
            'SANDBOX_HARNESS_RUNTIME_IMAGE_CODEX':images['codex'],
            'SANDBOX_HARNESS_RUNTIME_IMAGE_CLAUDE':images['claude'],
            'SANDBOX_HARNESS_RUNTIME_IMAGE_DEEPSEEK':images['deepseek']}.items():
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
        'profile_catalog_signature_digest':profile_signature_digest,
        'web_dist_digest':web_dist_digest,
        'recorded_at':datetime.now(timezone.utc).isoformat()},indent=2))
    print('Service version pinned to '+revision[:12]+' runtime_code_bundle='+code_bundle['sha256']
        +' runtime_bundle='+bundle['sha256'])

if __name__=='__main__':main()
