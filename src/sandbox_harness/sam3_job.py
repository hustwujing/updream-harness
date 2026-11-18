"""Fixed SAM3/ZIM operation executed only by the root controlled worker in bwrap."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request,urlopen

RUNTIME=Path(os.environ['CONTROLLED_MEDIA_RUNTIME_ROOT'])
SAM3_PYTHON=str(RUNTIME/'envs/sam3/bin/python')
ZIM_PYTHON=str(RUNTIME/'envs/face-makeup/bin/python')
PROJECT=RUNTIME/'lib-director-vF';SOURCE=PROJECT/'src'
SAM3_CHECKPOINT=RUNTIME/'models/sam3/sam3.pt'
ZIM_CHECKPOINT=RUNTIME/'models/ZIM/zim-anything-vitl/zim_vit_l_2092'


class NoSelectionError(RuntimeError):pass


def request_json(url,payload,timeout=300):
    request=Request(url,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'},
        method='POST')
    try:
        with urlopen(request,timeout=timeout) as response:return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"provider HTTP {error.code}: {error.read().decode('utf-8','replace')}") from error


def wait_health(endpoint,process):
    deadline=time.monotonic()+45
    while time.monotonic()<deadline:
        if process.poll() is not None:raise RuntimeError('SAM3 worker exited during startup')
        try:
            with urlopen(endpoint+'/health',timeout=1) as response:
                if json.load(response).get('status')=='ok':return
        except Exception:time.sleep(.25)
    raise RuntimeError('SAM3 worker startup timeout')


def sam3(request,work):
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    endpoint=f'http://127.0.0.1:{port}';environment={**os.environ,
        'PYTHONPATH':str(SOURCE)+':'+str(RUNTIME/'models/sam3'),'HF_HUB_OFFLINE':'1',
        'TRANSFORMERS_OFFLINE':'1'}
    for key in ('PYTHONHOME','LD_LIBRARY_PATH','PYTHONPYCACHEPREFIX'):environment.pop(key,None)
    log=(work/'sam3.log').open('w');process=subprocess.Popen([SAM3_PYTHON,'-u','-m',
        'libdirector_vf.sam3_worker','--bind','127.0.0.1','--port',str(port),'--checkpoint',
        str(SAM3_CHECKPOINT)],cwd=PROJECT,env=environment,stdout=log,stderr=subprocess.STDOUT)
    try:
        wait_health(endpoint,process);output=work/'sam3';output.mkdir()
        payload={'image':request['source'],'output_dir':str(output),'draft_id':'selection'}
        if request.get('prompt'):
            records=[];confidence=float(request.get('confidence',.35))
            for threshold in dict.fromkeys((confidence,max(.1,confidence/2))):
                result=request_json(endpoint+'/segment/text',{**payload,'labels':[request['prompt']],
                    'build_material_groups':False,'confidence':threshold})
                records=json.loads(Path(result['detections']).read_text()).get('detections') or []
                recommended=[item for item in records if item.get('recommended')]
                if recommended:records=recommended
                if records:break
            if not records:raise NoSelectionError('SAM3 did not find the described object')
            record=max(records,key=lambda item:(float(item.get('score',0)),int(item.get('mask_pixels',0))))
            mask=output/record['mask'];preview=output/record['preview']
        else:
            box=request.get('box');points=request.get('points') or []
            payload['box']=[box[0],box[1],box[0]+box[2],box[1]+box[3]] if box else None
            if points:payload['groups']=[{'points':[{'x':p[0],'y':p[1],'label':1} for p in points]}]
            result=request_json(endpoint+'/segment/points',payload);mask=output/result['mask'];preview=output/result['preview']
        shutil.copyfile(mask,work/'raw-mask.png');shutil.copyfile(preview,work/'sam3-preview.jpg')
    finally:
        try:request_json(endpoint+'/shutdown',{},10)
        except Exception:pass
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        log.close()


def compose_foreground(source,alpha,output):
    script=("from PIL import Image; import sys; source=Image.open(sys.argv[1]).convert('RGBA'); "
        "alpha=Image.open(sys.argv[2]).convert('L'); source.putalpha(alpha); source.save(sys.argv[3])")
    result=subprocess.run([ZIM_PYTHON,'-c',script,str(source),str(alpha),str(output)],
        capture_output=True,text=True,timeout=60)
    if result.returncode:raise RuntimeError('Foreground composition failed: '+result.stderr[-1600:])


def zim(request,work):
    output=work/'zim';output.mkdir();record=output/'result.json';environment={**os.environ,
        'PYTHONPATH':str(SOURCE)+':'+str(RUNTIME/'models/ZIM/source')}
    for key in ('PYTHONHOME','LD_LIBRARY_PATH','PYTHONPYCACHEPREFIX'):environment.pop(key,None)
    command=[ZIM_PYTHON,'-u','-m','libdirector_vf.zim_refine','--checkpoint',str(ZIM_CHECKPOINT),
        '--image',request['source'],'--output-dir',str(output),'--raw-mask',str(work/'raw-mask.png'),
        '--label',request.get('prompt') or 'object','--id','selection','--source','parallel-platform',
        '--output',str(record)]
    result=subprocess.run(command,cwd=PROJECT,env=environment,capture_output=True,text=True,timeout=300)
    (work/'zim.log').write_text(result.stdout+'\n'+result.stderr)
    if result.returncode:raise RuntimeError('ZIM refinement failed: '+result.stderr[-1600:])
    value=json.loads(record.read_text());compose_foreground(request['source'],
        output/value['alpha'],work/'foreground.png')
    shutil.copyfile(work/'raw-mask.png',work/'mask.png')
    (work/'segmentation.json').write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')


def main():
    request=json.loads(Path(sys.argv[1]).read_text());work=Path(sys.argv[1]).parent
    try:
        sam3(request,work)
        if request.get('refine',True):zim(request,work)
        else:
            shutil.copyfile(work/'raw-mask.png',work/'mask.png')
            compose_foreground(request['source'],work/'raw-mask.png',work/'foreground.png')
            (work/'segmentation.json').write_text('{"segmenter":"sam3","matting":"none"}\n')
    except NoSelectionError:
        (work/'model-outcome.json').write_text('{"schema":1,"code":"no_selection"}\n');raise
    except RuntimeError as exc:
        if 'requires a CUDA GPU' in str(exc) or 'CUDA' in str(exc) and 'unavailable' in str(exc):
            (work/'model-outcome.json').write_text(
                '{"schema":1,"code":"gpu_runtime_unavailable"}\n')
        raise


if __name__=='__main__':main()
