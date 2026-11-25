"""Bounded, replayable staging for managed files; never stages business actions."""
from __future__ import annotations

from collections import OrderedDict
import hashlib
import os
from pathlib import Path
import re
import stat
import time
import uuid

from fastapi import HTTPException

CHUNK_BYTES = 4 * 1024 * 1024
MAX_BYTES = 2 * 1024 * 1024 * 1024
MAX_ACTIVE = 4
MAX_RECEIPTS = 128
IDLE_SECONDS = 600


class ProjectUploads:
    def __init__(self, root: Path, target):
        self.root = root
        self.target = target
        self.epoch = uuid.uuid4().hex
        self.records: OrderedDict[str, dict] = OrderedDict()
        self.writers: dict[str, str] = {}
        if root.is_symlink() or root.parent.is_symlink():
            raise ValueError('project_upload_staging_link')
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Runtime startup invalidates unfinished transport work. These files
        # belong only to this component and never enter a checkpoint.
        for path in root.glob('*.part'):
            path.unlink()

    def _discard(self, value: dict, status: str) -> None:
        value['temporary'].unlink(missing_ok=True)
        value.update(status=status, touched=time.monotonic())
        if self.writers.get(value['path']) == value['id']:
            self.writers.pop(value['path'], None)

    def _reap(self) -> None:
        now = time.monotonic()
        for identity, value in list(self.records.items()):
            if now - value['touched'] < IDLE_SECONDS:
                continue
            if value['status'] == 'open':
                self._discard(value, 'expired')
            elif time.time() > value['issued_at'] + IDLE_SECONDS:
                del self.records[identity]
    def _make_room(self) -> None:
        # A superseded begin whose reply was lost may still be retrying. Do not
        # forget its identity while the replay window is open: that would let
        # an old writer become a new writer and overwrite its successor.
        if len(self.records) >= MAX_RECEIPTS:
            raise HTTPException(429, 'project_upload_capacity')

    def fence(self, path: str, identity: str) -> None:
        previous = self.writers.get(path)
        if previous in self.records:
            self._discard(self.records[previous], 'superseded')
        self.writers[path] = identity

    def require_current(self, path: str, identity: str) -> None:
        if self.writers.get(path) != identity:
            raise HTTPException(409, 'project_upload_superseded')

    def finish_write(self, path: str, identity: str) -> None:
        if self.writers.get(path) == identity:
            self.writers.pop(path, None)

    def begin(self, identity: str, path: str, size: int, issued_at: int | None = None,
              epoch: str | None = None) -> dict:
        if epoch is not None and epoch != self.epoch:
            raise HTTPException(409, 'project_upload_runtime_replaced')
        self._reap()
        issued_at = int(time.time()) if issued_at is None else issued_at
        # A delayed initialization cannot resurrect a forgotten writer after
        # receipt expiry. The client freezes this timestamp across retries.
        age = time.time() - issued_at
        if not -60 <= age <= IDLE_SECONDS:
            raise HTTPException(410, 'project_upload_initialization_expired')
        if not re.fullmatch(r'[0-9a-f]{32}', identity):
            raise HTTPException(400, 'project_upload_id_invalid')
        if not 0 <= size <= MAX_BYTES:
            raise HTTPException(413, 'project_upload_size_invalid')
        try:
            self.target(path)
        except ValueError:
            raise HTTPException(400, 'project_upload_path_invalid')
        path = Path(path).as_posix()
        if identity in self.records:
            value = self.records[identity]
            if (value['path'], value['size'], value['issued_at']) != (path, size, issued_at):
                raise HTTPException(409, 'project_upload_identity_conflict')
            if value['status'] not in {'open', 'complete'}:
                raise HTTPException(409, 'project_upload_inactive')
            value['touched'] = time.monotonic()
            return {'upload_id': identity, 'chunk_bytes': CHUNK_BYTES}
        self._make_room()
        active = [v for v in self.records.values()
                  if v['status'] == 'open' and v['path'] != path]
        if len(active) >= MAX_ACTIVE or sum(v['size'] for v in active) + size > MAX_BYTES:
            raise HTTPException(429, 'project_upload_capacity')
        temporary = self.root / (uuid.uuid4().hex + '.part')
        with temporary.open('xb'):
            pass
        os.chmod(temporary, 0o600)
        self.fence(path, identity)
        self.records[identity] = {'id': identity, 'path': path, 'size': size,
            'issued_at': issued_at,
            'written': 0, 'temporary': temporary, 'status': 'open',
            'touched': time.monotonic()}
        return {'upload_id': identity, 'chunk_bytes': CHUNK_BYTES}

    def _open(self, identity: str) -> dict:
        self._reap()
        value = self.records.get(identity)
        if value is None:
            raise HTTPException(404, 'project_upload_unknown')
        if value['status'] != 'open':
            raise HTTPException(409, 'project_upload_inactive')
        self.require_current(value['path'], identity)
        value['touched'] = time.monotonic()
        return value

    def _file(self, value: dict, writable: bool = False):
        if self.root.is_symlink() or self.root.parent.is_symlink():
            raise HTTPException(400, 'project_upload_staging_link')
        descriptor = os.open(value['temporary'],
                             (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            os.close(descriptor)
            raise HTTPException(400, 'project_upload_staging_invalid')
        return os.fdopen(descriptor, 'r+b' if writable else 'rb')

    def chunk(self, identity: str, offset: int, body: bytes) -> dict:
        value = self._open(identity)
        if not body or len(body) > CHUNK_BYTES or offset < 0 or offset + len(body) > value['size']:
            raise HTTPException(400, 'project_upload_chunk_invalid')
        if offset > value['written']:
            raise HTTPException(409, 'project_upload_offset_mismatch')
        with self._file(value, writable=True) as stream:
            stream.seek(offset)
            if offset < value['written']:
                if offset + len(body) > value['written'] or stream.read(len(body)) != body:
                    raise HTTPException(409, 'project_upload_chunk_conflict')
            else:
                stream.write(body)
                stream.flush()
                value['written'] += len(body)
        return {'bytes': len(body), 'offset': offset}

    def commit(self, identity: str, digest: str) -> dict:
        self._reap()
        value = self.records.get(identity)
        if value and value['status'] == 'complete':
            if digest != value['receipt']['sha256']:
                raise HTTPException(409, 'project_upload_digest_conflict')
            return value['receipt']
        value = self._open(identity)
        if value['written'] != value['size']:
            raise HTTPException(409, 'project_upload_incomplete')
        with self._file(value) as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            os.fsync(stream.fileno())
        if actual != digest:
            raise HTTPException(400, 'project_upload_digest_mismatch')
        try:
            target = self.target(value['path'])
            target.parent.mkdir(parents=True, exist_ok=True)
            target = self.target(value['path'])
        except ValueError:
            raise HTTPException(400, 'project_upload_path_invalid')
        self.require_current(value['path'], identity)
        value['temporary'].replace(target)
        receipt = {'path': '/workspace/project/' + value['path'],
                   'bytes': value['size'], 'sha256': actual}
        value.update(status='complete', receipt=receipt, touched=time.monotonic())
        self.finish_write(value['path'], identity)
        return receipt

    def abort(self, identity: str) -> None:
        value = self.records.get(identity)
        if value and value['status'] == 'open':
            self._discard(value, 'aborted')

    def reset(self) -> None:
        self.epoch = uuid.uuid4().hex
        for value in self.records.values():
            if value['status'] == 'open':
                self._discard(value, 'superseded')
        self.writers.clear()
