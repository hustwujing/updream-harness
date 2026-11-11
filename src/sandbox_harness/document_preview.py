from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory


PRESENTATION_MIMES = {
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}


def is_presentation(filename: str, mime: str) -> bool:
    return Path(filename).suffix.lower() in {".ppt", ".pptx"} or mime.lower() in PRESENTATION_MIMES


def presentation_pdf(data: bytes, *, filename: str, mime: str, digest: str,
                     cache_root: Path, maximum: int) -> bytes:
    if not is_presentation(filename, mime):
        raise ValueError("document_preview_unsupported")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("document_preview_digest_invalid")
    cache_root.mkdir(parents=True, exist_ok=True)
    cached = cache_root / f"{digest}.pdf"
    if cached.exists():
        result = cached.read_bytes()
        if result.startswith(b"%PDF-") and len(result) <= maximum:
            return result
        cached.unlink(missing_ok=True)
    suffix = Path(filename).suffix.lower()
    if suffix not in {".ppt", ".pptx"}:
        suffix = ".pptx" if "openxmlformats" in mime else ".ppt"
    with TemporaryDirectory(prefix="preview-", dir=cache_root) as temporary:
        root = Path(temporary)
        source = root / ("presentation" + suffix)
        source.write_bytes(data)
        source.chmod(0o600)
        profile = root / "libreoffice-profile"
        profile.mkdir()
        environment = {**os.environ, "HOME": str(root), "TMPDIR": str(root)}
        try:
            result = subprocess.run([
                "/usr/bin/libreoffice", "--headless", "--nologo", "--nodefault", "--nolockcheck",
                "--nofirststartwizard", f"-env:UserInstallation={profile.resolve().as_uri()}",
                "--convert-to", "pdf", "--outdir", str(root), str(source),
            ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=90, check=False, env=environment)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("document_preview_conversion_failed") from exc
        converted = root / "presentation.pdf"
        if result.returncode or not converted.exists():
            raise RuntimeError("document_preview_conversion_failed")
        payload = converted.read_bytes()
        if not payload.startswith(b"%PDF-"):
            raise RuntimeError("document_preview_conversion_invalid")
        if len(payload) > maximum:
            raise ValueError("document_preview_too_large")
        with NamedTemporaryFile(dir=cache_root, prefix=f".{digest}.", suffix=".tmp", delete=False) as stream:
            stream.write(payload)
            staging = Path(stream.name)
        try:
            staging.chmod(0o600)
            os.replace(staging, cached)
        finally:
            staging.unlink(missing_ok=True)
        return payload
