#!/usr/bin/env python3
"""Render one deterministic Manim scene and validate the resulting MP4."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def main() -> None:
    root = Path(os.environ.get("MANIM_SMOKE_DIR", "/workspace/tmp/manim-smoke"))
    root.mkdir(parents=True, exist_ok=True)
    script = root / "smoke.py"
    media = root / "media"
    script.write_text(
        "from manim import MathTex, Scene, Write\n"
        "class SmokeScene(Scene):\n"
        "    def construct(self):\n"
        "        formula = MathTex(r'e^{i\\pi}+1=0')\n"
        "        self.play(Write(formula), run_time=0.2)\n"
        "        self.wait(0.1)\n",
        encoding="utf-8",
    )
    subprocess.run(
        ["manim", "-ql", "--disable_caching", "--media_dir", str(media), str(script), "SmokeScene"],
        check=True,
        timeout=180,
    )
    videos = sorted(media.glob("videos/smoke/480p15/SmokeScene.mp4"))
    if len(videos) != 1 or videos[0].stat().st_size == 0:
        raise SystemExit("Manim smoke did not produce a non-empty MP4")
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration:stream=codec_name,width,height",
         "-of", "json", str(videos[0])],
        check=True,
        capture_output=True,
        text=True,
    )
    probe = json.loads(result.stdout)
    duration = float(probe["format"]["duration"])
    streams = probe.get("streams", [])
    if duration <= 0 or not any(stream.get("codec_name") == "h264" for stream in streams):
        raise SystemExit("Manim smoke MP4 failed codec or duration validation")
    print(json.dumps({"passed": True, "path": str(videos[0]), "bytes": videos[0].stat().st_size,
                      "duration": duration}, sort_keys=True))


if __name__ == "__main__":
    main()
