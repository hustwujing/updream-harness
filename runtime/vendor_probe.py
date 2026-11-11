#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "vendor" / "npm-prefix" / "lib" / "node_modules"
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


def main() -> None:
    actual = {}
    for name, expected in EXPECTED.items():
        package = ROOT / name / "package.json"
        if not package.is_file():
            raise SystemExit("Missing vendored Node package: " + name)
        actual[name] = json.loads(package.read_text())["version"]
        if actual[name] != expected:
            raise SystemExit(f"Vendored Node package mismatch: {name}={actual[name]} expected {expected}")
    print(json.dumps(actual, sort_keys=True))


if __name__ == "__main__":
    main()
