#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


PACKAGES = (
    "build-essential", "ca-certificates", "cmake", "curl", "ffmpeg", "file",
    "fonts-wqy-zenhei", "git", "jq", "nodejs", "npm", "pkg-config", "procps",
    "ripgrep", "unzip", "xz-utils",
)

COMMANDS = {
    "cmake": ["cmake", "--version"],
    "curl": ["curl", "--version"],
    "ffmpeg": ["ffmpeg", "-version"],
    "ffprobe": ["ffprobe", "-version"],
    "file": ["file", "--version"],
    "git": ["git", "--version"],
    "g++": ["g++", "--version"],
    "gcc": ["gcc", "--version"],
    "jq": ["jq", "--version"],
    "make": ["make", "--version"],
    "node": ["node", "--version"],
    "npm": ["npm", "--version"],
    "pkg-config": ["pkg-config", "--version"],
    "python": ["python", "--version"],
    "rg": ["rg", "--version"],
    "unzip": ["unzip", "-v"],
    "xz": ["xz", "--version"],
}


def first_line(command: list[str]) -> str:
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    output = (result.stdout + result.stderr).strip().splitlines()
    if not output:
        raise RuntimeError(f"{command[0]} returned no version")
    return output[0]


def snapshot() -> dict:
    package_rows = subprocess.check_output(
        ["dpkg-query", "-W", "-f=${Package}\t${Version}\n", *PACKAGES], text=True,
    ).splitlines()
    packages = dict(row.split("\t", 1) for row in package_rows)
    return {
        "schema_version": 1,
        "commands": {name: first_line(command) for name, command in COMMANDS.items()},
        "packages": dict(sorted(packages.items())),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    actual = snapshot()
    if args.verify:
        expected = json.loads(args.verify.read_text())
        if actual != expected:
            raise SystemExit("runtime toolchain does not match toolchain.lock.json")
    else:
        print(json.dumps(actual, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
