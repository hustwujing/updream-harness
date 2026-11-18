#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
from pathlib import Path
import subprocess

from PIL import Image


DEFAULT_SOURCE=Path('/data2/wujing03/distributed-agent-system/workspace/demos/catalog.json')
DEFAULT_OUTPUT=Path(__file__).resolve().parents[1]/'tests/example_library/cases.json'
OPERATIONS={
    'transcribe':('audio.transcribe',),
    'image-layers':('image.decompose',),
    'image-select':('image.segment',),
    'image-sam3-zim':('image.segment',),
    'image-erase':('image.erase',),
    'image-ocr':('image.ocr',),
    'image-local':('image.edit',),
    'image-color-match':('image.edit',),
    'image-replace-text':('image.edit',),
}


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


def expectation(entry:dict,demo_root:Path)->dict:
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
    result_suffix=result.suffix.lower();files=[Path(name).suffix.lower() for name in entry.get('files',[])]
    for suffix in ('.ora','.psd','.srt','.json'):
        if suffix in files and suffix!=result_suffix:
            value['extra_extension']=suffix;break
    return value


def build(source:Path)->dict:
    raw=source.read_bytes();entries=json.loads(raw);demo_root=source.parent;asset_root=demo_root/'sources'
    if not isinstance(entries,list) or len(entries)!=44:raise ValueError('expected_exactly_44_examples')
    cases=[]
    for index,entry in enumerate(entries,1):
        assets=[str(row['file']) for row in entry.get('sources',[])]
        missing=[name for name in assets if not (asset_root/name).is_file()]
        if missing:raise ValueError('missing_demo_sources:'+','.join(missing))
        prompt=('已确认本次执行范围；请直接执行并完成机器验收，不必先停下来等待方案确认。'+
            str(entry['prompt']))
        cases.append({'id':f'E{index:02d}','family':'distributed-agent-system-example-library-v1',
            'source_demo_id':entry['id'],'title':entry['title'],'category':entry.get('category',''),
            'complexity':'example_library','assets':assets,'prompt':prompt,
            'source_prompt':entry['prompt'],'expect':expectation(entry,demo_root),
            'content_checks':list(entry.get('options',[])),'workflow_min_steps':0,'hook':None,
            'status':'not_run','attempts':[],'allowed_external_operations':list(
                OPERATIONS.get(entry['id'],())), 'entry_mode':'deployed_browser',
            'executor':'primary_harness','provenance':'distributed-agent-system:'+entry['id'],
            'source_result':entry['result']})
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
