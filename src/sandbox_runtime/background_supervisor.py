from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


STOP=False


def atomic_json(path:Path,value:dict)->None:
    temporary=path.with_suffix(path.suffix+'.tmp')
    with temporary.open('w') as target:
        target.write(json.dumps(value,separators=(',',':')))
        target.flush();os.fsync(target.fileno())
    temporary.replace(path)
    descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def boot_id()->str:
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def start_time(pid:int)->str:
    value=Path(f'/proc/{pid}/stat').read_text()
    return value[value.rfind(') ')+2:].split()[19]


def children(root:int)->set[int]:
    parents={}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():continue
        try:
            value=(entry/'stat').read_text();parts=value[value.rfind(') ')+2:].split()
            parents[int(entry.name)]=int(parts[1])
        except (OSError,ValueError,IndexError):continue
    result=set();pending=[root]
    while pending:
        parent=pending.pop()
        found=[pid for pid,ppid in parents.items() if ppid==parent and pid not in result]
        result.update(found);pending.extend(found)
    return result


def stop_children(root:int)->None:
    for sig,seconds in ((signal.SIGTERM,.6),(signal.SIGKILL,.8)):
        targets=children(root)
        if not targets:return
        for pid in targets:
            try:os.kill(pid,sig)
            except ProcessLookupError:pass
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            if not children(root):return
            reap();time.sleep(.02)


def reap()->dict[int,int]:
    result={}
    while True:
        try:pid,status=os.waitpid(-1,os.WNOHANG)
        except ChildProcessError:return result
        if pid==0:return result
        result[pid]=os.waitstatus_to_exitcode(status)


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument('--receipt',type=Path,required=True)
    parser.add_argument('argv',nargs=argparse.REMAINDER);args=parser.parse_args()
    argv=args.argv[1:] if args.argv[:1]==['--'] else args.argv
    if not argv:raise SystemExit(127)
    libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(36,1,0,0,0)!=0:
        raise OSError(ctypes.get_errno(),'PR_SET_CHILD_SUBREAPER failed')
    global STOP
    def stopping(signum,frame):
        global STOP;STOP=True
    signal.signal(signal.SIGTERM,stopping);signal.signal(signal.SIGINT,stopping)
    args.receipt.parent.mkdir(parents=True,exist_ok=True)
    atomic_json(args.receipt,{'pid':os.getpid(),'pid_start_time':start_time(os.getpid()),
        'boot_id':boot_id(),'created_at':time.time()})
    child=subprocess.Popen(argv,start_new_session=True)
    main_code=None
    while True:
        statuses=reap()
        if child.pid in statuses:main_code=statuses[child.pid]
        elif main_code is None:
            value=child.poll()
            if value is not None:main_code=value
        if STOP:
            stop_children(os.getpid());statuses.update(reap())
            if child.pid in statuses:main_code=statuses[child.pid]
        remaining=children(os.getpid())
        if main_code is not None and not remaining:
            raise SystemExit(main_code)
        time.sleep(.03)


if __name__=='__main__':main()
