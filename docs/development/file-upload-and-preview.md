# File Upload and Preview

## Scope

This guide applies to Composer attachments, Artifact and ProjectFile cards,
inline previews and document conversion. The source entries are
`web/src/main.jsx`, `web/src/DocumentPreview.jsx`,
`src/sandbox_harness/services/api/app.py` and `src/sandbox_harness/services/api/document_preview.py`.

## Contracts

- File picker and clipboard paste both call the owner-scoped `/api/uploads`
  API. A Turn references only the returned immutable Attachment record.
- Preview detection uses both MIME and filename extension. Download remains
  available for every format and is the fallback after a preview error.
- Image, video, PDF, PPT/PPTX, DOCX and XLS/XLSX artifacts use the thumbnail
  grid. Project files stay compact in the Workspace inspector.
- PDF uses PDF.js continuous pages. PPT/PPTX is content-hash cached after
  Host LibreOffice Impress converts it to PDF. DOCX and spreadsheets are
  parsed in lazy browser chunks; spreadsheet values render as React text,
  never injected HTML.
- SVG is served as `image/svg+xml` only under `Content-Security-Policy:
  sandbox; default-src 'none'` so image rendering works without script or
  external-resource execution.
- GLB uses a lazy Three.js viewer whose chunk is prefetched while the browser is
  idle. The modal reports viewer initialization, byte download, parsing and
  scene preparation separately. Browsers without WebGL get an actionable
  download fallback instead of a blank canvas.
- A successful Artifact whose basename is `index.html` represents a static
  interactive site. Its card uses the site treatment and opens the immutable
  sibling Artifact directory through `/api/artifacts/{id}/site-preview/` in an
  iframe sandbox without `allow-same-origin`. Never fetch it into a text `<pre>`
  and never render generated HTML in the main document origin.
- Artifact and ProjectFile responses stream from object storage, support one
  HTTP byte range, and use an owner-scoped immutable ETag cache. Do not buffer a
  complete large video or GLB in API memory before sending the first byte.

## Host dependency

The API Host needs `/usr/bin/libreoffice` and
`/etc/libreoffice/registry/impress.xcd`. On Ubuntu install
`libreoffice-impress-nogui`. Verify the selected API service account can execute
it before selecting a release; `tests/test_document_preview.py` verifies the
conversion and project-owned cache contract.

## Completion checks

Run the frontend production build, `tests/test_document_preview.py`,
`tests/browser_document_previews.cjs` and `tests/browser_media_gallery.cjs`.
The browser cases cover pasted files, document thumbnails, PPT paging, PDF and
DOCX scrolling, spreadsheet sheet switching, SVG rendering and page errors.
Run `tests/browser_model3d_preview.cjs` for GLB prefetch, loading/rendering and
the no-WebGL fallback, plus object-store and owner tests for Range and ETag
behavior.
Run `tests/browser_site_deployment.cjs` and `tests/test_site_deployment.py` when
HTML detection, directory resolution, iframe isolation or deployment cards
change.
