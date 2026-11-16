#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def timestamp(value:float)->str:
    milliseconds=max(0,round(value*1000));hours,milliseconds=divmod(milliseconds,3600000)
    minutes,milliseconds=divmod(milliseconds,60000);seconds,milliseconds=divmod(milliseconds,1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"


def transcribe(source:Path,model_path:Path,*,language:str|None,beam_size:int,threads:int)->dict:
    from faster_whisper import WhisperModel
    model=WhisperModel(str(model_path),device="cpu",compute_type="int8",cpu_threads=threads)
    segments,info=model.transcribe(str(source),language=language,beam_size=beam_size,
        vad_filter=True,word_timestamps=True)
    rows=[]
    for segment in segments:
        text=segment.text.strip()
        if text:rows.append({"start":round(segment.start,3),"end":round(segment.end,3),"text":text})
    return {"language":info.language,"language_probability":round(info.language_probability,6),
        "duration":round(info.duration,3),"segments":rows,"text":" ".join(x["text"] for x in rows)}


def main()->None:
    parser=argparse.ArgumentParser(prog="sandbox-transcribe")
    parser.add_argument("source",type=Path);parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--model",type=Path,default=Path(os.environ.get("SANDBOX_WHISPER_MODEL",
        "/opt/models/faster-whisper-small")));parser.add_argument("--language",default="")
    parser.add_argument("--beam-size",type=int,default=5,choices=range(1,9))
    parser.add_argument("--threads",type=int,default=min(4,os.cpu_count() or 1),choices=range(1,9))
    args=parser.parse_args()
    if not args.source.is_file():raise SystemExit("source file not found")
    if not args.model.joinpath("model.bin").is_file():raise SystemExit("ASR model is unavailable")
    result=transcribe(args.source,args.model,language=args.language or None,
        beam_size=args.beam_size,threads=args.threads)
    args.output_dir.mkdir(parents=True,exist_ok=True)
    args.output_dir.joinpath("transcript.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
    args.output_dir.joinpath("transcript.txt").write_text(result["text"]+"\n")
    srt="\n\n".join(f"{index}\n{timestamp(row['start'])} --> {timestamp(row['end'])}\n{row['text']}"
        for index,row in enumerate(result["segments"],1))
    args.output_dir.joinpath("transcript.srt").write_text(srt+("\n" if srt else ""))
    print(json.dumps({"language":result["language"],"duration":result["duration"],
        "segments":len(result["segments"]),"output_dir":str(args.output_dir)},ensure_ascii=False))


if __name__=="__main__":main()
