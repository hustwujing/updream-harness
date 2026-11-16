#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from asr_cli import transcribe


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--model",type=Path,required=True)
    parser.add_argument("--audio",type=Path,required=True);parser.add_argument("--expect",required=True)
    args=parser.parse_args();result=transcribe(args.audio,args.model,language="en",beam_size=3,threads=2)
    if args.expect.lower() not in result["text"].lower():
        raise SystemExit("ASR smoke transcript mismatch: "+result["text"][:300])
    print("ASR smoke ok: "+result["text"][:300])


if __name__=="__main__":main()
