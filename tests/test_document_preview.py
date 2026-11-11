from hashlib import sha256
from io import BytesIO

import pytest
from pptx import Presentation

from sandbox_harness.document_preview import is_presentation, presentation_pdf
from sandbox_harness.app import SAFE_INLINE_MIMES


def test_presentation_preview_converts_and_reuses_content_cache(tmp_path, monkeypatch):
    presentation = Presentation()
    presentation.slides.add_slide(presentation.slide_layouts[5]).shapes.title.text = "Preview"
    stream = BytesIO()
    presentation.save(stream)
    data = stream.getvalue()
    digest = sha256(data).hexdigest()
    first = presentation_pdf(data, filename="deck.pptx",
        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        digest=digest, cache_root=tmp_path, maximum=10_000_000)
    assert first.startswith(b"%PDF-")
    monkeypatch.setattr("sandbox_harness.document_preview.subprocess.run",
        lambda *args, **kwargs: pytest.fail("cached preview should not run LibreOffice"))
    assert presentation_pdf(data, filename="deck.pptx",
        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        digest=digest, cache_root=tmp_path, maximum=10_000_000) == first


def test_presentation_preview_rejects_other_formats(tmp_path):
    assert "image/svg+xml" in SAFE_INLINE_MIMES
    assert is_presentation("slides.ppt", "application/octet-stream")
    assert not is_presentation("report.pdf", "application/pdf")
    with pytest.raises(ValueError, match="unsupported"):
        presentation_pdf(b"not a document", filename="report.pdf", mime="application/pdf",
            digest="1" * 64, cache_root=tmp_path, maximum=1000)
