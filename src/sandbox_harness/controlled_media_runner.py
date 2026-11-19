from __future__ import annotations

import grp
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import re

from PIL import Image


THREAD_ENVIRONMENT={
    'OPENBLAS_NUM_THREADS':'4','OMP_NUM_THREADS':'4','MKL_NUM_THREADS':'4',
    'NUMEXPR_NUM_THREADS':'4','VECLIB_MAXIMUM_THREADS':'4',
}


class ControlledOperationError(RuntimeError):
    def __init__(self,message:str,*,error_code:str,failed_phase:str,
                 retry_class:str='none',retryable:bool=False,infrastructure:bool=False):
        super().__init__(message);self.error_code=error_code;self.failed_phase=failed_phase
        self.retry_class=retry_class;self.retryable=retryable;self.infrastructure=infrastructure


def _thread_environment_args()->list[str]:
    return [value for key,item in THREAD_ENVIRONMENT.items()
        for value in ('--setenv',key,item)]


def _base_args(work:Path,runtime:Path)->list[str]:
    project_root=runtime.parent.parent
    return ['/usr/bin/bwrap','--die-with-parent','--new-session','--unshare-pid','--unshare-net',
        '--unshare-ipc','--unshare-uts','--proc','/proc','--dev','/dev','--ro-bind','/usr','/usr',
        '--ro-bind','/bin','/bin','--ro-bind','/lib','/lib','--ro-bind','/lib64','/lib64',
        '--ro-bind','/etc','/etc','--ro-bind','/sys','/sys','--dir','/data2',
        '--dir','/data2/wujing03','--dir',str(project_root),
        '--dir',str(project_root/'.data'),'--ro-bind',str(runtime),str(runtime),'--bind',str(work),'/work',
        '--tmpfs','/tmp','--chmod','1777','/tmp']


def _run(args:list[str],work:Path,timeout:int)->None:
    result=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    (work/'bwrap.log').write_text(result.stdout+'\n'+result.stderr)
    if result.returncode:raise RuntimeError('controlled_bwrap_failed:'+result.stderr[-1600:])


def _gpu_uuid(index:str)->str:
    if not re.fullmatch(r'0|[1-9][0-9]*',index):raise RuntimeError('segment_gpu_id_invalid')
    rows=subprocess.check_output(['/usr/bin/nvidia-smi','--query-gpu=index,uuid',
        '--format=csv,noheader,nounits'],text=True,timeout=10).splitlines()
    values={key.strip():value.strip() for key,value in
        (row.split(',',1) for row in rows if ',' in row)}
    uuid=values.get(index,'')
    if not re.fullmatch(r'GPU-[0-9a-fA-F-]+',uuid):raise RuntimeError('segment_gpu_uuid_missing')
    return uuid


def _gpu_isolation_args(work:Path,runtime:Path,index:str)->list[str]:
    account=pwd.getpwnam('agent-plus')
    groups=','.join(str(grp.getgrnam(name).gr_gid) for name in (
        'sandbox-agent-runtime','video-use-runtime'))
    gpu_uuid=_gpu_uuid(index);args=_base_args(work,runtime)
    if Path('/proc/driver/nvidia').exists():args += ['--ro-bind','/proc/driver/nvidia','/proc/driver/nvidia']
    for name in (f'nvidia{index}','nvidiactl','nvidia-modeset','nvidia-uvm','nvidia-uvm-tools'):
        path='/dev/'+name
        if Path(path).exists():args += ['--dev-bind',path,path]
    for pattern in ('libnvidia*.so*','libcuda.so*'):
        for path in Path('/usr/lib/x86_64-linux-gnu').glob(pattern):
            if path.is_file():args += ['--ro-bind',str(path.resolve()),str(path)]
    return [*args,'--clearenv',*_thread_environment_args(),
        '--setenv','PATH','/usr/bin:/bin','--setenv','HOME','/work',
        '--setenv','TMPDIR','/tmp','--setenv','CUDA_VISIBLE_DEVICES',gpu_uuid,
        '--setenv','CONTROLLED_MEDIA_RUNTIME_ROOT',str(runtime),'--setenv','HF_HUB_OFFLINE','1',
        '--setenv','TRANSFORMERS_OFFLINE','1','--chdir','/work','/usr/bin/setpriv',
        '--reuid='+str(account.pw_uid),'--regid='+str(account.pw_gid),'--groups='+groups,
        '--no-new-privs','--bounding-set=-all']


def probe_controlled_gpu(index:str,runtime:Path,work_root:Path)->bool:
    account=pwd.getpwnam('agent-plus');work=work_root/('gpu-'+index)
    if work.exists():shutil.rmtree(work)
    work.mkdir(parents=True,mode=0o700);os.chown(work,account.pw_uid,account.pw_gid)
    code=("import json,torch; assert torch.cuda.is_available(); "
        "value=torch.ones((1,),device='cuda'); torch.cuda.synchronize(); "
        "print(json.dumps({'cuda':True,'device':torch.cuda.current_device(),'sum':value.item()}))")
    args=_gpu_isolation_args(work,runtime,index)+[
        str(runtime/'envs/sam3/bin/python'),'-c',code]
    try:_run(args,work,90);return True
    except (OSError,RuntimeError,subprocess.SubprocessError):return False


def _segment(work:Path,source:Path,parameters:dict,gpu_ids:tuple[str,...],runtime:Path)->dict:
    if os.geteuid()!=0:raise RuntimeError('controlled_worker_requires_root')
    if len(gpu_ids)!=1:raise RuntimeError('segment_gpu_reservation_invalid')
    account=pwd.getpwnam('agent-plus');script=work/'sam3_job.py'
    os.chown(work,account.pw_uid,account.pw_gid)
    shutil.copyfile(Path(__file__).with_name('sam3_job.py'),script)
    request={**parameters,'source':'/work/'+source.name};(work/'request.json').write_text(json.dumps(request))
    for path in work.iterdir():os.chown(path,account.pw_uid,account.pw_gid)
    index=gpu_ids[0].removeprefix('gpu-');args=_gpu_isolation_args(work,runtime,index)
    args += ['/usr/bin/python3','/work/sam3_job.py','/work/request.json']
    try:_run(args,work,900)
    except RuntimeError as exc:
        outcome=work/'model-outcome.json'
        code=(json.loads(outcome.read_text()).get('code') if outcome.is_file() else '')
        if code=='no_selection':raise RuntimeError('no_selection') from exc
        if code=='gpu_runtime_unavailable':
            raise ControlledOperationError('Controlled CUDA runtime is unavailable',
                error_code='gpu_runtime_unavailable',failed_phase='operation_runtime',
                retry_class='node_health_change',retryable=True,infrastructure=True) from exc
        raise
    with Image.open(work/'foreground.png') as foreground:
        size=foreground.size
        if foreground.mode!='RGBA':raise RuntimeError('segment_foreground_alpha_missing')
    with Image.open(work/'mask.png') as mask:
        values=mask.convert('L').getextrema()
        if mask.size!=size or values[0]==values[1]:raise RuntimeError('segment_mask_invalid')
    receipts=[{'validator':'image.dimensions_exact','status':'pass','evidence':{'size':size}}]
    return {'outputs':[
        {'role':'foreground','path':'foreground.png','mime':'image/png','validation_receipts':[
            *receipts,{'validator':'image.alpha','status':'pass'}]},
        {'role':'mask','path':'mask.png','mime':'image/png','validation_receipts':[
            *receipts,{'validator':'mask.nontrivial','status':'pass','evidence':{'extrema':values}}]},
        {'role':'segmentation_report','path':'segmentation.json','mime':'application/json'}]}


def _require_audio_stream(source:Path)->None:
    result=subprocess.run(['/usr/bin/ffprobe','-v','error','-select_streams','a:0',
        '-show_entries','stream=index','-of','json',str(source)],capture_output=True,text=True,timeout=30)
    if result.returncode:
        raise ControlledOperationError('Source media could not be inspected for audio',
            error_code='media_probe_failed',failed_phase='input_validation')
    try:streams=json.loads(result.stdout).get('streams',[])
    except (AttributeError,json.JSONDecodeError) as exc:
        raise ControlledOperationError('Source media audio probe returned invalid data',
            error_code='media_probe_failed',failed_phase='input_validation') from exc
    if not streams:
        raise ControlledOperationError('Source media has no audio stream',
            error_code='audio_stream_missing',failed_phase='input_validation')


def _transcribe(work:Path,source:Path,parameters:dict,runtime:Path)->dict:
    if os.geteuid()!=0:raise RuntimeError('controlled_worker_requires_root')
    _require_audio_stream(source)
    script=work/'transcribe_job.py';shutil.copyfile(Path(__file__).with_name('transcribe_job.py'),script)
    request={**parameters,'source':'/work/'+source.name,
        'model':str(runtime/'models/media-foundation/faster-whisper-small'),
        'threads':max(1,min(8,int(parameters.get('threads',4)))),
        'beam_size':max(1,min(10,int(parameters.get('beam_size',5))))}
    (work/'request.json').write_text(json.dumps(request));args=_base_args(work,runtime)
    args += ['--clearenv',*_thread_environment_args(),
        '--setenv','HOME','/work','--setenv','TMPDIR','/tmp',
        '--setenv','HF_HUB_OFFLINE','1','--setenv','TRANSFORMERS_OFFLINE','1','--chdir','/work',
        '/usr/bin/setpriv','--reuid=0','--regid=0','--clear-groups','--no-new-privs',
        '--bounding-set=-all',str(runtime/'envs/media-foundation/bin/python'),'/work/transcribe_job.py',
        '/work/request.json']
    _run(args,work,1800)
    report=json.loads((work/'transcript.json').read_text())
    receipt={'validator':'transcript.timeline','status':'pass','evidence':{
        'segments':len(report['segments']),'language':report['language']}}
    return {'outputs':[
        {'role':'transcript_json','path':'transcript.json','mime':'application/json',
            'validation_receipts':[receipt]},
        {'role':'transcript_text','path':'transcript.txt','mime':'text/plain'},
        {'role':'transcript_srt','path':'transcript.srt','mime':'text/plain'}]}


def run_controlled_operation(operation:str,work:Path,source:Path,parameters:dict,
                             gpu_ids:tuple[str,...],runtime_root:Path)->dict:
    runtime=runtime_root.resolve()
    if not runtime.is_relative_to(Path('/data2/wujing03')) or not runtime.is_dir():
        raise RuntimeError('controlled_runtime_root_invalid')
    if operation=='image.segment':return _segment(work,source,parameters,gpu_ids,runtime)
    if operation=='audio.transcribe':return _transcribe(work,source,parameters,runtime)
    raise RuntimeError('controlled_operation_invalid')
