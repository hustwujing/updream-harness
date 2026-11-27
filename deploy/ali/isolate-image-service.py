#!/usr/bin/env python3
"""Constrain the already materialized inference dependency's writable paths.

Shared GPU lock inodes remain the coordination authority, but flock works with
read-only descriptors. Never copy/recreate those locks in a private namespace.
Unexpected dependency source shapes fail closed rather than silently patching.
"""
from pathlib import Path


def replace_verified(source: str, before: str, after: str, count: int) -> str:
    if source.count(before)==count:return source.replace(before,after)
    if source.count(before)==0 and source.count(after)==count:return source
    raise RuntimeError('image_service_dependency_shape_changed')


def isolate(service_root: Path) -> None:
    gpu=service_root/'media_core'/'gpu.py';source=gpu.read_text()
    source=replace_verified(source,
        '        self.directory.mkdir(parents=True, exist_ok=True)',
        '        if not self.directory.is_dir():\n            return None',2)
    source=replace_verified(source,
        '            handle = (self.directory / f"gpu-{gpu.index}.lock").open("a+")',
        '            try:\n                handle = (self.directory / f"gpu-{gpu.index}.lock").open("r")\n'
        '            except FileNotFoundError:\n                continue',2)
    for body in ('f"{os.getpid()}\\n"','f"{os.getpid()} group={self.count}\\n"'):
        source=replace_verified(source,
            '            handle.seek(0)\n            handle.truncate()\n'
            f'            handle.write({body})\n            handle.flush()',
            '            # Shared coordination is read-only; no PID file mutation. '+body,1)
    compile(source,str(gpu),'exec')
    if gpu.read_text()!=source:gpu.write_text(source)
    worker=service_root/'service'/'worker.py';source=worker.read_text()
    source=replace_verified(source,
        '    for directory in (input_dir, comfy_output, temp_dir):',
        '    user_dir = ctx.job_dir / "comfy-user"\n'
        '    for directory in (input_dir, comfy_output, temp_dir, user_dir):',1)
    source=replace_verified(source,
        '        "--extra-model-paths-config", str(EXTRA_MODELS),',
        '        "--user-directory", str(user_dir), "--extra-model-paths-config", str(EXTRA_MODELS),',1)
    compile(source,str(worker),'exec')
    if worker.read_text()!=source:worker.write_text(source)


if __name__=='__main__':
    root=Path(__file__).resolve().parents[2]
    if root!=Path('/data/wujing02/sandbox-harness-platform-parallel-ali'):
        raise RuntimeError('unexpected_project_root')
    isolate(root/'.data/services/image-layered')
