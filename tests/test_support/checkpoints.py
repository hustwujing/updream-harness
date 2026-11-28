import io
import os
from pathlib import Path
import sqlite3
import tarfile
import tempfile


NATIVE_IDS=('native','native-1','native-appended','native-good','native-interaction',
    'native-pause','native-recovered','native_stream')


def checkpoint_bytes(native_ids=NATIVE_IDS):
    descriptor,path=tempfile.mkstemp(prefix='test-checkpoint-state-',suffix='.sqlite')
    os.close(descriptor);state=Path(path)
    try:
        connection=sqlite3.connect(state)
        connection.execute('create table threads (id text primary key)')
        connection.executemany('insert into threads(id) values (?)',[(value,) for value in native_ids])
        connection.commit();connection.close();payload=io.BytesIO()
        with tarfile.open(fileobj=payload,mode='w') as archive:
            for name in ('project','state','state/codex'):
                member=tarfile.TarInfo(name);member.type=tarfile.DIRTYPE;member.mode=0o755
                archive.addfile(member)
            data=state.read_bytes();member=tarfile.TarInfo('state/codex/state_5.sqlite')
            member.size=len(data);member.mode=0o600;archive.addfile(member,io.BytesIO(data))
        return payload.getvalue()
    finally:state.unlink(missing_ok=True)
