#!/usr/bin/env python3
from __future__ import annotations

import importlib


MODULES = (
    "av", "cairosvg", "cv2", "ffmpeg", "imagehash", "imageio", "jsonschema",
    "librosa", "matplotlib", "moviepy", "mutagen", "networkx", "numpy",
    "onnxruntime", "piexif", "PIL", "psd_tools", "pydub", "pymupdf", "rembg",
    "reportlab", "scipy", "shapely", "skimage", "sklearn", "soundfile",
    "svgwrite", "sympy", "trimesh", "xmltodict", "yaml",
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
