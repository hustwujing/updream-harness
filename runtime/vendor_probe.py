#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "vendor" / "npm-prefix" / "lib" / "node_modules"
MODEL_ROOT=Path(__file__).resolve().parent/"vendor"/"faster-whisper-small"
EXPECTED = {
    "docx": "9.15.3",
    "pnpm": "12.18.0",
    "pptxgenjs": "4.0.1",
    "react": "19.3.1",
    "react-dom": "19.3.1",
    "react-icons": "5.7.0",
    "sharp": "0.35.5",
    "typescript": "7.0.2",
}
MODEL_FILES={
    "config.json":(2370,"b55496ac7940a7ae47d2c01eab40edfd8701feec1229d9cce3b40014383fb828"),
    "model.bin":(483546902,"3e305921506d8872816023e4c273e75d2419fb89b24da97b4fe7bce14170d671"),
    "tokenizer.json":(2203239,"fb7b63191e9bb045082c79fd742a3106a12c99513ab30df4a0d47fa6cb6fd0ab"),
    "vocabulary.txt":(459861,"34ce3fe1c5041027b3f8d42912270993f986dbc4bb34cf27f951e34a1e453913"),
}


def main() -> None:
    actual = {}
    for name, expected in EXPECTED.items():
        package = ROOT / name / "package.json"
        if not package.is_file():
            raise SystemExit("Missing vendored Node package: " + name)
        actual[name] = json.loads(package.read_text())["version"]
        if actual[name] != expected:
            raise SystemExit(f"Vendored Node package mismatch: {name}={actual[name]} expected {expected}")
    models={}
    for name,(size,digest) in MODEL_FILES.items():
        path=MODEL_ROOT/name
        if not path.is_file() or path.stat().st_size!=size:
            raise SystemExit("Missing or invalid faster-whisper model file: "+name)
        actual_digest=hashlib.file_digest(path.open("rb"),"sha256").hexdigest()
        if actual_digest!=digest:raise SystemExit("Faster-whisper model digest mismatch: "+name)
        models[name]={"size":size,"sha256":digest}
    print(json.dumps({"node":actual,"faster_whisper_small":models},sort_keys=True))


if __name__ == "__main__":
    main()
