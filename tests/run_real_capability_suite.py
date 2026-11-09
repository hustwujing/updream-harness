from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

import httpx


CASES = {
    "codex-media": {
        "harness": "codex",
        "model": "gpt-5.6",
        "reasoning_level": "standard",
        "attachments": ["source.mp4"],
        "prompt": """Work directly in the sandbox and complete the task; do not only explain it.
Inspect /workspace/inbox/source.mp4. Write a reusable Python program at /workspace/project/process_video.py that invokes ffmpeg and ffprobe with subprocess. Run it to create /workspace/outputs/codex-edited.mp4 with all of these properties: 256x144, H.264 yuv420p video, AAC audio, duration between 1.8 and 2.2 seconds, and audio volume reduced to approximately half. Also write /workspace/outputs/codex-report.json containing width, height, duration, video_codec and audio_codec derived from ffprobe output. If a command fails, inspect its actual stderr, fix the code, and retry. Verify both output files before replying.""",
        "artifacts": ["codex-edited.mp4", "codex-report.json"],
    },
    "claude-images": {
        "harness": "claude",
        "model": "claude-4.6-sonnet",
        "reasoning_level": "standard",
        "attachments": ["red.png", "green.png", "blue.png"],
        "prompt": """Work directly in the sandbox and complete the task; do not only explain it.
Inspect the three PNG files in /workspace/inbox. Write a reusable program at /workspace/project/batch_images.py that uses ffmpeg through subprocess to resize every input image to exactly 160x90 and then assemble them left-to-right into /workspace/outputs/contact-sheet.png at exactly 480x90. Keep the three resized files in /workspace/outputs as thumb-red.png, thumb-green.png and thumb-blue.png. Write /workspace/outputs/claude-report.json with the sorted input names, output dimensions and SHA-256 of the contact sheet. Execute the program and verify dimensions and files. If anything fails, inspect the real error, repair the program and retry before replying.""",
        "artifacts": ["contact-sheet.png", "thumb-red.png", "thumb-green.png", "thumb-blue.png", "claude-report.json"],
    },
    "deepseek-repair": {
        "harness": "deepseek",
        "model": "deepseek-v4.1-flash",
        "reasoning_level": "standard",
        "attachments": ["tone.wav", "buggy_audio.py"],
        "prompt": """Work directly in the sandbox and complete the task; do not only explain it.
First execute /workspace/inbox/buggy_audio.py exactly once against /workspace/inbox/tone.wav and save its stderr to /workspace/outputs/initial-error.txt; the supplied program is expected to fail. Diagnose that observed failure. Copy and repair the implementation as /workspace/project/process_audio.py, then run it to create /workspace/outputs/processed.wav using only Python's standard library. The output must remain 48000 Hz mono 16-bit PCM, be between 1.9 and 2.1 seconds, and have approximately half the input peak amplitude. Separately use curl through the sandbox network to fetch https://example.com/ and save the HTTP response body to /workspace/outputs/network.html. Write /workspace/outputs/deepseek-report.json with sample_rate, channels, sample_width, frames, initial_error_detected and network_fetch_ok. Verify all outputs before replying.""",
        "artifacts": ["initial-error.txt", "processed.wav", "network.html", "deepseek-report.json"],
    },
}


def run(*argv: str) -> bytes:
    return subprocess.run(argv, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def make_inputs(root: Path) -> None:
    run("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=24:duration=4",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=4", "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest", str(root / "source.mp4"))
    for name, color in (("red", "red"), ("green", "green"), ("blue", "blue")):
        run("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", f"color=c={color}:s=320x180:d=0.1",
            "-frames:v", "1", str(root / f"{name}.png"))
    run("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000:duration=2",
        "-ac", "1", "-c:a", "pcm_s16le", str(root / "tone.wav"))
    (root / "buggy_audio.py").write_text("""import sys, wave
source, target = sys.argv[1:]
with wave.open(source, 'rb') as reader:
    frames = reader.readframes(reader.getnframes())
with wave.open(target, 'wb') as writer:
    writer.setnchannels(1)
    writer.setsampwidth(2)
    writer.setframerate(sample_rate)
    writer.writeframes(frames)
""")


async def upload(client: httpx.AsyncClient, path: Path) -> dict:
    mime = {".mp4": "video/mp4", ".png": "image/png", ".wav": "audio/wav", ".py": "text/x-python"}.get(path.suffix, "application/octet-stream")
    response = await client.post("api/uploads", params={"name": path.name, "mime": mime}, content=path.read_bytes())
    response.raise_for_status()
    return response.json()


async def collect_events(client: httpx.AsyncClient, turn_id: str) -> list[str]:
    kinds = []
    async with client.stream("GET", f"api/turns/{turn_id}/events") as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line.startswith("event: "):
                kinds.append(line.removeprefix("event: "))
    return kinds


async def execute_case(client: httpx.AsyncClient, root: Path, name: str, spec: dict) -> dict:
    attachments = [await upload(client, root / filename) for filename in spec["attachments"]]
    response = await client.post("api/threads", json={
        "harness": spec["harness"], "model": spec["model"], "reasoning_level": spec["reasoning_level"],
        "title": "Capability validation " + name,
    })
    response.raise_for_status()
    thread = response.json()
    response = await client.post(f"api/threads/{thread['id']}/turns", json={
        "message": spec["prompt"], "attachments": attachments, "idempotency_key": "capability-" + uuid.uuid4().hex,
        "harness": spec["harness"], "model": spec["model"], "reasoning_level": spec["reasoning_level"], "delivery_mode": "start",
    })
    response.raise_for_status()
    turn = response.json()
    started = datetime.now(timezone.utc)
    events = await collect_events(client, turn["id"])
    response = await client.get(f"api/turns/{turn['id']}")
    response.raise_for_status()
    final = response.json()
    if final["state"] != "COMPLETED":
        raise AssertionError(f"{name} ended as {final['state']}: {final.get('error_code')} {final.get('error_detail')}")
    response = await client.get(f"api/threads/{thread['id']}")
    response.raise_for_status()
    frozen = response.json()
    if not frozen.get("checkpoint_ref"):
        raise AssertionError(name + " did not persist a checkpoint")
    response = await client.get(f"api/threads/{thread['id']}/items")
    response.raise_for_status()
    items = response.json()
    names = {item["summary"] for item in items if item["type"] == "artifact"}
    missing = set(spec["artifacts"]) - names
    if missing:
        raise AssertionError(f"{name} missing artifacts: {sorted(missing)}")
    downloaded = {}
    for artifact in final["artifacts"]:
        if artifact["path"] not in spec["artifacts"]:
            continue
        response = await client.get(f"api/artifacts/{artifact['id']}")
        response.raise_for_status()
        target = root / (name + "-" + Path(artifact["path"]).name)
        target.write_bytes(response.content)
        downloaded[artifact["path"]] = target
    validate_outputs(name, downloaded)
    return {
        "case": name,
        "thread_id": thread["id"],
        "turn_id": turn["id"],
        "harness": spec["harness"],
        "model": spec["model"],
        "state": final["state"],
        "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
        "event_kinds": {kind: events.count(kind) for kind in sorted(set(events))},
        "item_types": {kind: sum(item["type"] == kind for item in items) for kind in sorted({item["type"] for item in items})},
        "checkpoint_ref": frozen["checkpoint_ref"],
        "artifacts": {key: {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for key, path in downloaded.items()},
    }


def probe(path: Path) -> dict:
    return json.loads(run("ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)))


def validate_outputs(name: str, outputs: dict[str, Path]) -> None:
    if name == "codex-media":
        value = probe(outputs["codex-edited.mp4"])
        video = next(stream for stream in value["streams"] if stream["codec_type"] == "video")
        audio = next(stream for stream in value["streams"] if stream["codec_type"] == "audio")
        assert (video["width"], video["height"], video["codec_name"], video["pix_fmt"]) == (256, 144, "h264", "yuv420p")
        assert audio["codec_name"] == "aac" and 1.8 <= float(value["format"]["duration"]) <= 2.2
    elif name == "claude-images":
        sheet = next(stream for stream in probe(outputs["contact-sheet.png"])["streams"] if stream["codec_type"] == "video")
        assert (sheet["width"], sheet["height"]) == (480, 90)
        for filename in ("thumb-red.png", "thumb-green.png", "thumb-blue.png"):
            image = next(stream for stream in probe(outputs[filename])["streams"] if stream["codec_type"] == "video")
            assert (image["width"], image["height"]) == (160, 90)
    else:
        value = probe(outputs["processed.wav"])
        audio = value["streams"][0]
        assert audio["sample_rate"] == "48000" and audio["channels"] == 1 and audio["codec_name"] == "pcm_s16le"
        assert 1.9 <= float(value["format"]["duration"]) <= 2.1
        assert outputs["initial-error.txt"].stat().st_size > 0
        assert b"Example Domain" in outputs["network.html"].read_bytes()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:22766/")
    parser.add_argument("--case", choices=[*CASES, "all"], default="all")
    parser.add_argument("--report", default=".data/first-capability-report.json")
    args = parser.parse_args()
    selected = CASES if args.case == "all" else {args.case: CASES[args.case]}
    with tempfile.TemporaryDirectory(prefix="sandbox-harness-capability-", dir="/data2/wujing03") as directory:
        root = Path(directory)
        make_inputs(root)
        async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
            results = await asyncio.gather(*(execute_case(client, root, name, spec) for name, spec in selected.items()), return_exceptions=True)
            session_token = client.cookies.get("sandbox_harness_session")
        if session_token:
            session_file = Path(args.report).with_suffix(".session")
            session_file.parent.mkdir(parents=True, exist_ok=True)
            session_file.write_text(session_token)
            os.chmod(session_file, 0o600)
        report = {"started_at": datetime.now(timezone.utc).isoformat(), "base_url": args.base_url, "results": []}
        failed = False
        for name, result in zip(selected, results, strict=True):
            if isinstance(result, Exception):
                failed = True
                report["results"].append({"case": name, "state": "FAILED", "error": type(result).__name__ + ": " + str(result)})
            else:
                report["results"].append(result)
        target = Path(args.report)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if failed:
            raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
