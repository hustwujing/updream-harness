"""Provider transports share these receipts; MediaWorker owns their lifecycle."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass
class ProviderResult:
    status: str
    url: str = ""
    provider_job_id: str = ""
    progress: int | None = None
    error: str = ""
    error_code: str = ""
    outputs: list[dict[str, str]] | None = None
    credits_consumed: float | None = None


@dataclass
class DownloadedMedia:
    path: Path
    size: int
    sha256: str
    mime: str

    def remove(self):
        self.path.unlink(missing_ok=True)


class MediaCancellationRequested(RuntimeError):
    pass


class ExternalMediaProvider(Protocol):
    async def prepare(self, job: dict) -> ProviderResult | None: ...
    async def submit(self, job: dict) -> ProviderResult: ...
    async def poll(self, job: dict) -> ProviderResult: ...
    async def cancel(self, job: dict) -> ProviderResult: ...
    async def download(self, job: dict, output: dict, *, staging_root: Path,
                       prefix: str, maximum_bytes: int) -> DownloadedMedia: ...
