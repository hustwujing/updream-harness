from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import mimetypes
from pathlib import Path, PurePosixPath
import re
import tarfile


MAX_SITE_FILES = 512
MAX_SITE_FILE_BYTES = 32 * 1024 * 1024
MAX_SITE_TOTAL_BYTES = 128 * 1024 * 1024
MAX_SITE_ARCHIVE_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class SiteFile:
    path: str
    data: bytes
    sha256: str
    size: int
    mime: str


def safe_site_path(value: str) -> str:
    if not value or len(value) > 300 or "\\" in value or "\x00" in value:
        raise ValueError("invalid_site_path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("invalid_site_path")
    if any(ord(character) < 32 for character in value):
        raise ValueError("invalid_site_path")
    return path.as_posix()


def site_mime(path: str) -> str:
    if path.endswith(".wasm"):
        return "application/wasm"
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def validate_site_archive(data: bytes) -> list[SiteFile]:
    if not data or len(data) > MAX_SITE_ARCHIVE_BYTES:
        raise ValueError("site_archive_size_invalid")
    files: list[SiteFile] = []
    paths: set[str] = set()
    total = 0
    try:
        archive = tarfile.open(fileobj=io.BytesIO(data), mode="r:*")
    except tarfile.TarError as exc:
        raise ValueError("site_archive_invalid") from exc
    with archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            if not member.isfile() or member.issym() or member.islnk():
                raise ValueError("site_archive_special_file")
            path = safe_site_path(member.name)
            if path in paths:
                raise ValueError("site_archive_duplicate_path")
            if member.size < 0 or member.size > MAX_SITE_FILE_BYTES:
                raise ValueError("site_file_size_invalid")
            total += member.size
            if total > MAX_SITE_TOTAL_BYTES:
                raise ValueError("site_total_size_invalid")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("site_archive_invalid")
            content = stream.read(member.size + 1)
            if len(content) != member.size:
                raise ValueError("site_archive_truncated")
            files.append(SiteFile(path=path, data=content,
                sha256=hashlib.sha256(content).hexdigest(), size=len(content), mime=site_mime(path)))
            paths.add(path)
            if len(files) > MAX_SITE_FILES:
                raise ValueError("site_file_count_invalid")
    if "index.html" not in paths:
        raise ValueError("site_index_missing")
    if next(value for value in files if value.path == "index.html").size == 0:
        raise ValueError("site_index_invalid")
    if site_mime("index.html") != "text/html":
        raise ValueError("site_index_invalid")
    return sorted(files, key=lambda value: value.path)


def normalize_site_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", value.strip().lower()).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)
    if not 3 <= len(slug) <= 48:
        raise ValueError("invalid_site_slug")
    return slug


def build_site_archive(source: Path) -> tuple[bytes, list[SiteFile]]:
    source = source.resolve()
    if not source.is_dir():
        raise ValueError("site_source_not_directory")
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
        count = 0
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                raise ValueError("site_source_symlink")
            if path.is_dir():
                continue
            if not path.is_file():
                raise ValueError("site_source_special_file")
            relative = safe_site_path(path.relative_to(source).as_posix())
            stat = path.stat()
            if stat.st_size > MAX_SITE_FILE_BYTES:
                raise ValueError("site_file_size_invalid")
            info = tarfile.TarInfo(relative)
            info.size = stat.st_size
            info.mode = 0o644
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            with path.open("rb") as stream:
                archive.addfile(info, stream)
            count += 1
            if count > MAX_SITE_FILES:
                raise ValueError("site_file_count_invalid")
    data = output.getvalue()
    return data, validate_site_archive(data)
