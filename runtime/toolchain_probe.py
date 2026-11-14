#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


PACKAGES = (
    "build-essential", "ca-certificates", "chromium", "cmake", "curl", "draw.io", "dvisvgm", "ffmpeg", "file",
    "fonts-liberation2", "fonts-noto-color-emoji", "fonts-wqy-zenhei", "git", "graphviz",
    "imagemagick", "jq", "libcairo2", "libgbm1", "libgl1-mesa-dri", "libglx-mesa0", "libpango-1.0-0", "libpangocairo-1.0-0", "libreoffice-calc",
    "libreoffice-core", "libreoffice-impress", "libreoffice-writer", "lmodern", "mesa-utils", "pandoc", "pkg-config",
    "poppler-utils", "procps", "qpdf", "ripgrep", "tesseract-ocr", "tesseract-ocr-chi-sim", "texlive-fonts-recommended", "texlive-latex-base", "texlive-latex-extra", "unzip",
    "xauth", "xvfb", "xz-utils", "zip",
)

COMMANDS = {
    "cmake": ["cmake", "--version"],
    "chromium": ["chromium", "--version"],
    "curl": ["curl", "--version"],
    "dot": ["dot", "-V"],
    "drawio": ["xvfb-run", "-a", "/usr/bin/drawio", "--no-sandbox", "--version"],
    "dvisvgm": ["dvisvgm", "--version"],
    "ffmpeg": ["ffmpeg", "-version"],
    "ffprobe": ["ffprobe", "-version"],
    "file": ["file", "--version"],
    "git": ["git", "--version"],
    "identify": ["identify", "--version"],
    "imagemagick": ["convert", "--version"],
    "g++": ["g++", "--version"],
    "gcc": ["gcc", "--version"],
    "jq": ["jq", "--version"],
    "latex": ["latex", "--version"],
    "make": ["make", "--version"],
    "manim": ["manim", "--version"],
    "libreoffice": ["libreoffice", "--version"],
    "node": ["node", "--version"],
    "npm": ["npm", "--version"],
    "pandoc": ["pandoc", "--version"],
    "pdftoppm": ["pdftoppm", "-v"],
    "pkg-config": ["pkg-config", "--version"],
    "playwright": ["playwright", "--version"],
    "python": ["python", "--version"],
    "qpdf": ["qpdf", "--version"],
    "rg": ["rg", "--version"],
    "unzip": ["unzip", "-v"],
    "tesseract": ["tesseract", "--version"],
    "xz": ["xz", "--version"],
    "zip": ["zip", "-v"],
}

NPM_PACKAGES = ("docx", "esbuild", "playwright", "pnpm", "pptxgenjs", "react", "react-dom",
    "react-icons", "sharp", "three", "typescript")


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
    npm_root = Path(subprocess.check_output(["npm", "root", "-g"], text=True).strip())
    browser = Path(subprocess.check_output(["node", "-e",
        "const {chromium}=require('playwright');process.stdout.write(chromium.executablePath())"], text=True))
    if not browser.is_file() or not browser.stat().st_mode & 0o111:
        raise RuntimeError("Playwright Chromium executable is missing")
    return {
        "schema_version": 1,
        "commands": {name: first_line(command) for name, command in COMMANDS.items()},
        "npm_packages": {name: json.loads((npm_root / name / "package.json").read_text())["version"] for name in NPM_PACKAGES},
        "packages": dict(sorted(packages.items())),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.verify and args.output:
        parser.error("--verify and --output are mutually exclusive")
    actual = snapshot()
    if args.verify:
        expected = json.loads(args.verify.read_text())
        if actual != expected:
            raise SystemExit("runtime toolchain does not match toolchain.lock.json")
    elif args.output:
        args.output.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
        print(args.output)
    else:
        print(json.dumps(actual, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
