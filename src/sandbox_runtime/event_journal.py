from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile
import time


SEGMENT_BYTES=4*1024*1024
JOURNAL_BYTES=256*1024*1024
TERMINAL_RETENTION_SECONDS=600


def _atomic_json(path:Path,value:dict)->None:
    path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
    with NamedTemporaryFile('w',dir=path.parent,prefix='.',delete=False,
            encoding='utf-8') as stream:
        temporary=Path(stream.name)
        json.dump(value,stream,ensure_ascii=False,separators=(',',':'))
        stream.flush();os.fsync(stream.fileno())
    os.chmod(temporary,0o600);temporary.replace(path)


class ExecutionEventJournal:
    def __init__(self,root:Path,execution_id:str,*,segment_bytes:int=SEGMENT_BYTES,
                 maximum_bytes:int=JOURNAL_BYTES):
        digest=hashlib.sha256(execution_id.encode()).hexdigest()
        self.root=root/digest;self.execution_id=execution_id
        self.segment_bytes=segment_bytes;self.maximum_bytes=maximum_bytes
        self.root.mkdir(parents=True,mode=0o700,exist_ok=True);self.root.chmod(0o700)
        identity=self.root/'identity.json'
        if identity.exists():
            value=json.loads(identity.read_text())
            if value.get('execution_id')!=execution_id:raise RuntimeError('runtime_journal_identity_conflict')
        else:_atomic_json(identity,{'execution_id':execution_id})
        self.acked_sequence=self._read_ack()
        self._repair_trailing_record()
        self._last_sequence=self._scan_last_sequence()

    @classmethod
    def discover(cls,root:Path)->dict[str,'ExecutionEventJournal']:
        result={}
        if not root.is_dir():return result
        for directory in root.iterdir():
            try:
                value=json.loads((directory/'identity.json').read_text())
                execution_id=str(value['execution_id'])
                journal=cls(root,execution_id)
                result[execution_id]=journal
            except (OSError,ValueError,TypeError,KeyError,RuntimeError):continue
        return result

    def _read_ack(self)->int:
        try:
            value=json.loads((self.root/'ack.json').read_text()).get('committed_sequence',0)
            return value if isinstance(value,int) and value>=0 else 0
        except (OSError,ValueError,TypeError,AttributeError):return 0

    def _segments(self)->list[Path]:
        return sorted(self.root.glob('events-*.ndjson'))

    def _repair_trailing_record(self)->None:
        segments=self._segments()
        if not segments:return
        target=segments[-1];data=target.read_bytes()
        if not data or data.endswith(b'\n'):return
        boundary=data.rfind(b'\n')+1
        with target.open('r+b') as stream:
            stream.truncate(boundary);stream.flush();os.fsync(stream.fileno())
        if boundary==0:target.unlink(missing_ok=True)

    def rows_after(self,sequence:int)->list[dict]:
        rows=[]
        for path in self._segments():
            for value in self._rows(path,strict=True):
                current=value.get('runtime_sequence')
                if isinstance(current,int) and current>sequence:rows.append(value)
        rows.sort(key=lambda value:value['runtime_sequence'])
        if any(right['runtime_sequence']<=left['runtime_sequence']
                for left,right in zip(rows,rows[1:])):
            raise RuntimeError('runtime_journal_sequence_invalid')
        return rows

    @property
    def last_sequence(self)->int:
        return max(self.acked_sequence,self._last_sequence)

    def _scan_last_sequence(self)->int:
        for path in reversed(self._segments()):
            rows=self._rows(path,strict=True)
            if rows:return int(rows[-1]['runtime_sequence'])
        return self.acked_sequence

    @property
    def result(self)->dict|None:
        rows=self.rows_after(0)
        return next((value.get('result') for value in reversed(rows)
            if value.get('type')=='result' and isinstance(value.get('result'),dict)),None)

    def append(self,row:dict,*,terminal:bool=False)->None:
        sequence=row.get('runtime_sequence')
        if not isinstance(sequence,int) or sequence<=self.last_sequence:
            raise RuntimeError('runtime_journal_sequence_invalid')
        encoded=(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n').encode()
        pending=sum(path.stat().st_size for path in self._segments())
        if pending+len(encoded)>self.maximum_bytes and not terminal:
            raise RuntimeError('runtime_event_journal_limit_exceeded')
        segments=self._segments();target=segments[-1] if segments else None
        if target is None or target.stat().st_size+len(encoded)>self.segment_bytes:
            target=self.root/f'events-{sequence:020d}.ndjson'
        descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
        try:
            pending=memoryview(encoded)
            while pending:
                written=os.write(descriptor,pending)
                if written<=0:raise OSError('runtime_journal_write_failed')
                pending=pending[written:]
            if terminal:os.fsync(descriptor)
        finally:os.close(descriptor)
        self._last_sequence=sequence

    def acknowledge(self,sequence:int)->int:
        if sequence<self.acked_sequence:return self.acked_sequence
        if sequence>self.last_sequence:raise ValueError('runtime_ack_sequence_invalid')
        self.acked_sequence=sequence
        _atomic_json(self.root/'ack.json',{'committed_sequence':sequence})
        for path in self._segments():
            rows=self._rows(path,strict=True)
            if (rows and int(rows[-1]['runtime_sequence'])<=sequence
                    and not any(row.get('type')=='result' for row in rows)):
                path.unlink(missing_ok=True)
        result=self.result
        if result is not None and sequence>=self.last_sequence:
            _atomic_json(self.root/'cleanup.json',{
                'delete_after':time.time()+TERMINAL_RETENTION_SECONDS})
        return self.acked_sequence

    @property
    def cleanup_after(self)->float|None:
        try:
            value=json.loads((self.root/'cleanup.json').read_text()).get('delete_after')
            return float(value) if isinstance(value,(int,float)) else None
        except (OSError,ValueError,TypeError,AttributeError):return None

    def remove(self)->None:
        shutil.rmtree(self.root,ignore_errors=True)

    @staticmethod
    def _rows(path:Path,*,strict:bool=False)->list[dict]:
        result=[]
        try:
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    try:value=json.loads(line)
                    except (ValueError,TypeError):
                        if strict:raise RuntimeError('runtime_journal_record_invalid')
                        continue
                    if not isinstance(value,dict):
                        if strict:raise RuntimeError('runtime_journal_record_invalid')
                        continue
                    sequence=value.get('runtime_sequence')
                    if not isinstance(sequence,int) or sequence<=0:
                        if strict:raise RuntimeError('runtime_journal_record_invalid')
                        continue
                    result.append(value)
        except OSError:
            if strict:raise
        return result

    def mark_interrupted(self)->dict|None:
        if self.result is not None:return None
        sequence=self.last_sequence+1
        result={'status':'failed','error_code':'runtime_process_restarted',
            'error_detail':'Sandbox Runtime restarted before the Harness execution completed',
            'input_ids':[]}
        self.append({'type':'result','result':result,'runtime_sequence':sequence},terminal=True)
        return result
