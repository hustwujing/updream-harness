#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
from pathlib import Path
import re
import subprocess

from PIL import Image


DEFAULT_SOURCE=Path('/data2/wujing03/distributed-agent-system/workspace/demos/catalog.json')
DEFAULT_OUTPUT=Path(__file__).resolve().parents[1]/'tests/example_library/cases.json'
OPERATIONS={
    'transcribe':('audio.transcribe',),
    'image-layers':('image.decompose',),
    'image-cutout':('image.segment',),
    'image-select':('image.segment',),
    'image-sam3-zim':('image.segment',),
    'image-erase':('image.erase',),
    'image-ocr':('image.ocr',),
    'image-local':('image.edit',),
    'image-color-match':('image.edit',),
    'image-replace-text':('image.edit',),
}


def declared_source_files(entry:dict)->list[str]:
    files=[]
    for source in entry.get('sources',[]):
        files.append(str(source['file']))
        transcripts=source.get('transcripts') or {}
        if isinstance(transcripts,dict):files.extend(map(str,transcripts.values()))
    return list(dict.fromkeys(files))


def media_probe(path:Path)->dict:
    value=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_streams',
        '-show_format','-of','json',str(path)],text=True))
    streams=value.get('streams',[]);video=next((row for row in streams
        if row.get('codec_type')=='video'),None);audio=next((row for row in streams
        if row.get('codec_type')=='audio'),None)
    result={'duration':round(float(value.get('format',{}).get('duration') or
        (video or {}).get('duration') or (audio or {}).get('duration') or 0),3)}
    if video:result.update(width=int(video['width']),height=int(video['height']),audio=bool(audio))
    return result


def has_audio_stream(path:Path)->bool:
    result=subprocess.run(['ffprobe','-v','error','-select_streams','a',
        '-show_entries','stream=index','-of','csv=p=0',str(path)],capture_output=True,text=True)
    return result.returncode==0 and bool(result.stdout.strip())


def declared_sidecar(entry:dict,result_suffix:str)->str:
    requirement=' '.join([str(entry.get('prompt','')),*map(str,entry.get('options',[]))]).lower()
    for suffix,markers in (('.srt',('srt','字幕文件')),('.ora',('ora','可编辑图层')),
            ('.json',('json','结构化报告'))):
        if suffix!=result_suffix and any(marker in requirement for marker in markers):return suffix
    return ''


def result_probe(entry:dict,demo_root:Path)->dict:
    result=demo_root/entry['id']/entry['result'];mime=entry.get('result_type') or (
        mimetypes.guess_type(result.name)[0] or 'application/octet-stream')
    if not result.is_file():raise ValueError('missing_demo_result:'+str(result))
    if mime.startswith('image/'):
        with Image.open(result) as image:value={'kind':'image','width':image.width,'height':image.height}
    elif mime.startswith('video/'):
        value={'kind':'video',**media_probe(result)}
    elif mime.startswith('audio/'):
        value={'kind':'audio',**media_probe(result)}
    else:value={'kind':'text'}
    return value


def expectation(entry:dict,demo_root:Path)->dict:
    probe=result_probe(entry,demo_root);value={'kind':probe['kind']}
    requirement='；'.join([str(entry.get('prompt','')),*map(str,entry.get('options',[]))])
    if value['kind']=='image':
        match=re.search(r'(?:输出|制作成|导出为)[^。；]{0,24}?(\d{2,5})\s*[×xX]\s*(\d{2,5})',requirement)
        if not match:
            match=re.search(r'(\d{2,5})\s*[×xX]\s*(\d{2,5})[^。；]{0,16}?画布',requirement)
        if match:value.update(width=int(match.group(1)),height=int(match.group(2)))
    if value['kind']=='video':
        match=re.search(r'(?:输出|制作成)[^。；]{0,24}?(\d{2,5})\s*[×xX]\s*(\d{2,5})',requirement)
        if match:value.update(width=int(match.group(1)),height=int(match.group(2)))
        elif re.search(r'\b720p\b',requirement,re.I):value.update(width=1280,height=720)
        segments=(entry.get('preset') or {}).get('segments') or []
        if segments:value['duration']=round(sum(float(row['end'])-float(row['start']) for row in segments),3)
        elif re.search(r'\d+(?:\.\d+)?\s*秒',requirement):value['duration']=probe.get('duration',0)
        sources=[demo_root/'sources'/str(row['file']) for row in entry.get('sources',[])]
        if any(marker in requirement for marker in ('声音','音量','响度','音轨','音频','配乐','对白')) and any(
                has_audio_stream(path) for path in sources):
            value['audio']=True
    if value['kind']=='audio' and 'duration' in probe:value['duration']=probe['duration']
    result=demo_root/entry['id']/entry['result']
    sidecar=declared_sidecar(entry,result.suffix.lower())
    if sidecar:value['extra_extension']=sidecar
    return value


def build(source:Path)->dict:
    raw=source.read_bytes();entries=json.loads(raw);demo_root=source.parent;asset_root=demo_root/'sources'
    if not isinstance(entries,list) or len(entries)!=44:raise ValueError('expected_exactly_44_examples')
    cases=[]
    for index,entry in enumerate(entries,1):
        assets=declared_source_files(entry)
        missing=[name for name in assets if not (asset_root/name).is_file()]
        if missing:raise ValueError('missing_demo_sources:'+','.join(missing))
        expected=expectation(entry,demo_root);options='；'.join(map(str,entry.get('options',[])))
        sidecar=('，并同时交付 '+expected['extra_extension']+' 伴随文件'
            if expected.get('extra_extension') else '')
        prompt=('已确认本次执行范围；请直接执行并完成机器验收，不必先停下来等待方案确认。'+
            str(entry['prompt'])+' 原示例验收要求：'+options+'。主交付必须包含 '+
            str(entry['result_type'])+' 类型成品（参考文件名 '+str(entry['result'])+'）'+sidecar+'。')
        cases.append({'id':f'E{index:02d}','family':'distributed-agent-system-example-library-v1',
            'source_demo_id':entry['id'],'title':entry['title'],'category':entry.get('category',''),
            'complexity':'example_library','assets':assets,'prompt':prompt,
            'source_prompt':entry['prompt'],'expect':expected,
            'content_checks':list(entry.get('options',[])),'workflow_min_steps':0,'hook':None,
            'status':'not_run','attempts':[],'allowed_external_operations':list(
                OPERATIONS.get(entry['id'],())), 'entry_mode':'deployed_browser',
            'executor':'primary_harness','provenance':'distributed-agent-system:'+entry['id'],
            'source_result':entry['result'],'source_result_probe':result_probe(entry,demo_root)})
    return {'schema_version':1,'family':'distributed-agent-system-example-library-v1',
        'source_catalog':str(source),'source_catalog_sha256':hashlib.sha256(raw).hexdigest(),
        'asset_root':str(asset_root),'runnable_case_count':len(cases),'cases':cases}


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,default=DEFAULT_SOURCE)
    parser.add_argument('--output',type=Path,default=DEFAULT_OUTPUT);args=parser.parse_args()
    value=build(args.source.resolve());args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'output':str(args.output),'cases':len(value['cases']),
        'source_catalog_sha256':value['source_catalog_sha256']}))


if __name__=='__main__':main()
