"""Fixed offline faster-whisper operation executed inside root-controlled bwrap."""
from __future__ import annotations

import json
from pathlib import Path
import sys

from faster_whisper import WhisperModel


def timestamp(value):
    milliseconds=max(0,round(float(value)*1000));hours,milliseconds=divmod(milliseconds,3_600_000)
    minutes,milliseconds=divmod(milliseconds,60_000);seconds,milliseconds=divmod(milliseconds,1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"


def main():
    request=json.loads(Path(sys.argv[1]).read_text());model=WhisperModel(request['model'],
        device='cpu',compute_type='int8',cpu_threads=request['threads'],num_workers=1)
    segments,info=model.transcribe(request['source'],language=request.get('language') or None,
        task='translate' if request.get('translate') else 'transcribe',beam_size=request['beam_size'],
        vad_filter=True,condition_on_previous_text=True)
    rows=[]
    for item in segments:
        text=' '.join(str(item.text or '').split())
        if text:rows.append({'id':len(rows)+1,'start':round(float(item.start),3),
            'end':round(float(item.end),3),'text':text})
    if not rows:raise RuntimeError('transcription_empty')
    value={'schema':1,'language':info.language,'language_probability':info.language_probability,
        'duration':info.duration,'duration_after_vad':info.duration_after_vad,
        'task':'translate' if request.get('translate') else 'transcribe','segments':rows}
    Path('/work/transcript.json').write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    Path('/work/transcript.txt').write_text('\n'.join(row['text'] for row in rows)+'\n',encoding='utf-8')
    Path('/work/transcript.srt').write_text('\n'.join(
        f"{row['id']}\n{timestamp(row['start'])} --> {timestamp(row['end'])}\n{row['text']}\n"
        for row in rows),encoding='utf-8')


if __name__=='__main__':main()
