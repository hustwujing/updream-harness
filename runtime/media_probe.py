#!/usr/bin/env python3
from __future__ import annotations

import importlib


MODULES = (
    "anthropic", "av", "bs4", "cairosvg", "cv2", "defusedxml", "ffmpeg", "filelock",
    "imagehash", "imageio", "jsonschema", "librosa", "lxml", "markitdown", "matplotlib", "mcp",
    "manim", "manimpango", "moviepy", "mutagen", "networkx", "numpy", "onnxruntime", "openai", "openpyxl", "pandas",
    "pdf2image", "pdfplumber", "piexif", "PIL", "playwright", "pptx", "psd_tools", "psutil",
    "pydub", "pymupdf", "pypdf", "pytesseract", "rembg", "reportlab", "requests", "scipy",
    "shapely", "skimage", "sklearn", "soundfile", "svgwrite", "sympy", "trimesh", "xmltodict", "yaml",
)


def main() -> None:
    failures = []
    for name in MODULES:
        try:
            importlib.import_module(name)
        except Exception as exc:
            failures.append(f"{name}: {type(exc).__name__}: {exc}")
    if failures:
        raise SystemExit("Runtime media imports failed:\n" + "\n".join(failures))
    print("runtime media imports ok: " + ", ".join(MODULES))


if __name__ == "__main__":
    main()
