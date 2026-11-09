from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import uuid

import httpx


CASES = {
    "canvas-2d": {
        "harness": "codex", "model": "gpt-5.6",
        "prompt": """Work directly in the sandbox. Create /workspace/project/canvas.html as a self-contained 640x360 Canvas 2D animation. It must visibly render the Chinese title “创意动画实验室”, use at least four distinct non-white colors, and expose window.renderFrame(timeSeconds) that moves at least two shapes differently over time. Set window.__RENDER_READY__=true only after drawing the first frame. Run sandbox-browser-render to produce exactly 60 frames at 30 fps as /workspace/outputs/canvas.mp4, plus /workspace/outputs/canvas.png and /workspace/outputs/canvas-diagnostics.json. Inspect the diagnostics, use ffprobe to verify H.264 yuv420p 640x360, 60 frames and about 2 seconds, and use framemd5 to verify frames change. If anything fails, read the actual error and repair it before replying.""",
        "artifacts": ["canvas.mp4", "canvas.png", "canvas-diagnostics.json"],
    },
    "three-webgl": {
        "harness": "claude", "model": "claude-sonnet-5-5",
        "prompt": """Work directly in the sandbox without downloading packages. Create /workspace/project/three-entry.js that imports Three.js from the globally installed `three` package and builds a lit 640x360 WebGL scene with a floor, shadows, at least three colored 3D meshes, perspective camera motion, and window.renderFrame(timeSeconds). Bundle it with the globally installed esbuild into /workspace/project/three-bundle.js and create /workspace/project/three.html that loads the bundle and sets __RENDER_READY__ only after the first render. Run sandbox-browser-render for exactly 60 frames at 30 fps, producing /workspace/outputs/three.mp4, /workspace/outputs/three.png and /workspace/outputs/three-diagnostics.json. Diagnostics must report webgl=true, a non-empty renderer, no page errors, and animation_supported=true. Verify video dimensions, codec, frame count, duration, and changing framemd5 values before replying. Repair real errors if needed.""",
        "artifacts": ["three.mp4", "three.png", "three-diagnostics.json"],
    },
    "browser-repair": {
        "harness": "deepseek", "model": "deepseek-v4-pro",
        "prompt": """Work directly in the sandbox and execute the following steps immediately. Do not inspect the renderer source, help text, installed tools or fonts. First create /workspace/project/broken.html containing `throw new Error('intentional-browser-failure')` before __RENDER_READY__, then run `sandbox-browser-render --html /workspace/project/broken.html --output /workspace/project/broken.png --diagnostics /workspace/project/broken-diagnostics.json` exactly once, redirecting its complete stderr to /workspace/outputs/initial-browser-error.txt. The command must fail. Then create /workspace/project/repaired.html as a self-contained 640x360 HTML/Canvas animation with the Chinese title “浏览器自修复完成”, window.renderFrame(timeSeconds), and __RENDER_READY__=true. Run `sandbox-browser-render` with explicit width 640, height 360, frames 30, fps 30, video /workspace/outputs/repaired.mp4, output /workspace/outputs/repaired.png and diagnostics /workspace/outputs/repaired-diagnostics.json. Verify no page errors, changing frames, H.264 yuv420p, 30 frames and about one second. Write /workspace/outputs/repair-report.json as exactly {"initial_failure_observed":true,"repaired_render_valid":true}. Repair unexpected errors before replying.""",
        "artifacts": ["initial-browser-error.txt", "repaired.mp4", "repaired.png", "repaired-diagnostics.json", "repair-report.json"],
    },
}


def command(*argv: str) -> bytes:
    return subprocess.run(argv, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def video_probe(path: Path) -> dict:
    return json.loads(command("ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)))


def changing_frames(path: Path) -> int:
    output = command("ffmpeg", "-v", "error", "-i", str(path), "-f", "framemd5", "-").decode()
    hashes = {line.rsplit(",", 1)[-1].strip() for line in output.splitlines() if line and not line.startswith("#")}
    return len(hashes)


async def wait(client: httpx.AsyncClient, turn_id: str) -> dict:
    for _ in range(1800):
        response = await client.get(f"api/turns/{turn_id}")
        response.raise_for_status()
        value = response.json()
        if value["state"] in {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}:
            return value
        await asyncio.sleep(1)
    raise TimeoutError(turn_id)


async def execute(client: httpx.AsyncClient, root: Path, name: str, spec: dict) -> dict:
    response = await client.post("api/threads", json={"harness": spec["harness"], "model": spec["model"],
        "reasoning_level": "standard", "title": "Browser render validation " + name})
    response.raise_for_status(); thread = response.json()
    response = await client.post(f"api/threads/{thread['id']}/turns", json={"message": spec["prompt"], "attachments": [],
        "idempotency_key": "browser-render-" + uuid.uuid4().hex, "delivery_mode": "start"})
    response.raise_for_status(); started = datetime.now(timezone.utc); final = await wait(client, response.json()["id"])
    if final["state"] != "COMPLETED":
        raise AssertionError(f"{name}: {final['state']} {final.get('error_code')} {final.get('error_detail')}")
    artifacts = {item["path"]: item for item in final["artifacts"]}
    missing = set(spec["artifacts"]) - set(artifacts)
    if missing:
        raise AssertionError(f"{name} missing {sorted(missing)}")
    paths = {}
    for filename in spec["artifacts"]:
        response = await client.get(f"api/artifacts/{artifacts[filename]['id']}")
        response.raise_for_status(); target = root / f"{name}-{Path(filename).name}"; target.write_bytes(response.content); paths[filename] = target
    details = validate(name, paths)
    response = await client.get(f"api/threads/{thread['id']}/items"); response.raise_for_status(); items = response.json()
    return {"case": name, "state": final["state"], "thread_id": thread["id"], "turn_id": final["id"],
        "harness": spec["harness"], "model": spec["model"], "sandbox_id": final["sandbox_id"],
        "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
        "tool_items": sum(item["type"] == "tool" for item in items), "validation": details,
        "artifacts": {key: {"bytes": value.stat().st_size, "sha256": hashlib.sha256(value.read_bytes()).hexdigest()} for key, value in paths.items()}}


def validate(name: str, paths: dict[str, Path]) -> dict:
    if name == "canvas-2d": video, expected_frames, expected_duration, diagnostics = paths["canvas.mp4"], 60, 2, paths["canvas-diagnostics.json"]
    elif name == "three-webgl": video, expected_frames, expected_duration, diagnostics = paths["three.mp4"], 60, 2, paths["three-diagnostics.json"]
    else: video, expected_frames, expected_duration, diagnostics = paths["repaired.mp4"], 30, 1, paths["repaired-diagnostics.json"]
    probe = video_probe(video); stream = next(item for item in probe["streams"] if item["codec_type"] == "video")
    assert (stream["codec_name"], stream["pix_fmt"], stream["width"], stream["height"]) == ("h264", "yuv420p", 640, 360)
    assert int(stream["nb_frames"]) == expected_frames
    assert abs(float(probe["format"]["duration"]) - expected_duration) <= 0.05
    unique = changing_frames(video); assert unique >= min(10, expected_frames)
    diagnostic = json.loads(diagnostics.read_text()); assert diagnostic["page_errors"] == [] and diagnostic["graphics"]["animation_supported"] is True
    if name == "three-webgl":
        assert diagnostic["graphics"]["webgl"] is True and diagnostic["graphics"]["renderer"]
    if name == "browser-repair":
        assert paths["initial-browser-error.txt"].stat().st_size > 0
        assert json.loads(paths["repair-report.json"].read_text()) == {"initial_failure_observed": True, "repaired_render_valid": True}
    return {"codec": stream["codec_name"], "pix_fmt": stream["pix_fmt"], "width": 640, "height": 360,
        "frames": expected_frames, "duration": float(probe["format"]["duration"]), "unique_frames": unique,
        "webgl": diagnostic["graphics"]["webgl"], "renderer": diagnostic["graphics"]["renderer"]}


async def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--base-url", default="http://127.0.0.1:22766/")
    parser.add_argument("--report", default=".data/browser-render-report.json"); args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sandbox-render-", dir="/data2/wujing03") as directory:
        async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
            response = await client.get("api/session"); response.raise_for_status()
            results = await asyncio.gather(*(execute(client, Path(directory), name, spec) for name, spec in CASES.items()), return_exceptions=True)
        report = {"results": []}; failed = False
        for name, result in zip(CASES, results, strict=True):
            if isinstance(result, Exception):
                failed = True; report["results"].append({"case": name, "state": "FAILED", "error": type(result).__name__ + ": " + str(result)})
            else: report["results"].append(result)
        target = Path(args.report); target.parent.mkdir(parents=True, exist_ok=True); target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if failed: raise SystemExit(1)


if __name__ == "__main__": asyncio.run(main())
