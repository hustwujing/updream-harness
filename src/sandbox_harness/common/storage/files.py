"""Bounded immutable file streams shared by execution adapters."""
from __future__ import annotations

import asyncio
from pathlib import Path

async def staged_file_chunks(path: Path, chunk_size: int = 1024 * 1024):
    with path.open("rb") as stream:
        while chunk := await asyncio.to_thread(stream.read, chunk_size):
            yield chunk

