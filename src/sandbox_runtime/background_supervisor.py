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


STOP_REASON=""


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


def stop_children(root:int,grace:float)->tuple[bool,bool]:
    sent_term=sent_kill=False
    for sig,seconds in ((signal.SIGTERM,grace),(signal.SIGKILL,.8)):
        targets=children(root)
        if not targets:return sent_term,sent_kill
        if sig==signal.SIGTERM:sent_term=True
        else:sent_kill=True
        for pid in targets:
            try:os.kill(pid,sig)
            except ProcessLookupError:pass
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            if not children(root):return sent_term,sent_kill
            reap();time.sleep(.02)
    return sent_term,sent_kill


def reap()->dict[int,int]:
    result={}
    while True:
        try:pid,status=os.waitpid(-1,os.WNOHANG)
        except ChildProcessError:return result
        if pid==0:return result
        result[pid]=os.waitstatus_to_exitcode(status)


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument('--receipt',type=Path,required=True)
    parser.add_argument('--deadline-at',type=float,default=0)
    parser.add_argument('--terminate-grace',type=float,default=2)
    parser.add_argument('argv',nargs=argparse.REMAINDER);args=parser.parse_args()
    argv=args.argv[1:] if args.argv[:1]==['--'] else args.argv
    if not argv:raise SystemExit(127)
    libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(36,1,0,0,0)!=0:
        raise OSError(ctypes.get_errno(),'PR_SET_CHILD_SUBREAPER failed')
    global STOP_REASON
    def stopping(signum,frame):
        global STOP_REASON
        if not STOP_REASON:STOP_REASON='cancelled'
    signal.signal(signal.SIGTERM,stopping);signal.signal(signal.SIGINT,stopping)
    args.receipt.parent.mkdir(parents=True,exist_ok=True)
    if args.deadline_at<=0:args.deadline_at=time.time()+1200
    args.terminate_grace=min(max(args.terminate_grace,.01),10)
    identity={'pid':os.getpid(),'pid_start_time':start_time(os.getpid()),
        'boot_id':boot_id(),'created_at':time.time(),'deadline_at':args.deadline_at,
        'terminate_grace_seconds':args.terminate_grace,'status':'running'}
    atomic_json(args.receipt,identity)
    child=subprocess.Popen(argv,start_new_session=True)
    main_code=None;sent_term=sent_kill=False
    deadline=time.monotonic()+max(0,args.deadline_at-time.time())
    while True:
        statuses=reap()
        if child.pid in statuses:main_code=statuses[child.pid]
        elif main_code is None:
            value=child.poll()
            if value is not None:main_code=value
        if not STOP_REASON and time.monotonic()>=deadline:
            STOP_REASON='timed_out'
        if STOP_REASON:
            term,kill=stop_children(os.getpid(),max(.01,args.terminate_grace))
            sent_term|=term;sent_kill|=kill;statuses.update(reap())
            if child.pid in statuses:main_code=statuses[child.pid]
        remaining=children(os.getpid())
        if main_code is not None and not remaining:
            if STOP_REASON=='timed_out':status='timed_out';code=124
            elif STOP_REASON:status='cancelled';code=130
            else:status='completed' if main_code==0 else 'failed';code=main_code
            atomic_json(args.receipt,{**identity,'status':status,'returncode':code,
                'command_returncode':main_code,'reason':STOP_REASON,
                'sigterm_sent':sent_term,'sigkill_sent':sent_kill,
                'completed_at':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())})
            raise SystemExit(code)
        time.sleep(.03)


if __name__=='__main__':main()
