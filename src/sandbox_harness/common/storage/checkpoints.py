"""Local verification for a staged immutable workspace checkpoint."""
from __future__ import annotations

import hashlib
from pathlib import Path,PurePosixPath
import sqlite3
import stat
import tarfile
from tempfile import TemporaryDirectory


def verify_checkpoint_archive(path:Path,*,expected_size:int,expected_sha256:str,
                              harness:str,native_thread_id:str)->dict:
    info=path.stat(follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
            or info.st_size!=expected_size):
        raise ValueError('checkpoint_archive_invalid')
    with path.open('rb') as stream:actual=hashlib.file_digest(stream,'sha256').hexdigest()
    if actual!=expected_sha256:raise ValueError('checkpoint_archive_invalid')
    members={};roots=set();native_present=None;integrity=None
    try:
        with tarfile.open(path,'r:*') as archive:
            for member in archive.getmembers():
                pure=PurePosixPath(member.name)
                if (pure.is_absolute() or not pure.parts or any(
                        part in {'','..'} for part in pure.parts)
                        or pure.parts[0] not in {'project','state'}
                        or member.issym() or member.islnk() or member.isdev()):
                    raise ValueError('checkpoint_archive_invalid')
                roots.add(pure.parts[0]);members[pure.as_posix()]=member
            if roots!={'project','state'}:raise ValueError('checkpoint_archive_invalid')
            if harness=='codex' and native_thread_id:
                sqlite_name='state/codex/state_5.sqlite'
                if sqlite_name not in members:raise ValueError('checkpoint_native_state_invalid')
                with TemporaryDirectory(prefix='checkpoint-verify-',dir=path.parent) as temporary:
                    root=Path(temporary)
                    for name in (sqlite_name,sqlite_name+'-wal',sqlite_name+'-shm'):
                        member=members.get(name)
                        if member is None:continue
                        source=archive.extractfile(member)
                        if source is None:raise ValueError('checkpoint_native_state_invalid')
                        target=root/Path(name).name
                        with source,target.open('wb') as output:
                            while chunk:=source.read(1024*1024):output.write(chunk)
                    connection=sqlite3.connect('file:'+str(root/'state_5.sqlite')+'?mode=ro',uri=True)
                    try:
                        integrity=connection.execute('pragma integrity_check').fetchone()[0]
                        rows={str(row[0]) for row in connection.execute('select id from threads')}
                    finally:connection.close()
                    native_present=native_thread_id in rows
                if integrity!='ok' or not native_present:
                    raise ValueError('checkpoint_native_state_invalid')
    except (OSError,sqlite3.Error,tarfile.TarError) as exc:
        raise ValueError('checkpoint_archive_invalid') from exc
    return {'archive_sha256':actual,'archive_size':expected_size,
        'member_count':len(members),'roots':sorted(roots),
        'native_integrity':integrity or 'not_applicable',
        'native_thread_id_present':native_present}
