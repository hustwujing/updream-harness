# Frozen artifact format selection

The first full Ali gate exposed a collection-boundary defect: a successful
multi-format source returned PNG, PSD, ORA, TIFF, ZIP and JSON, while its frozen
collection allowed only PNG, ORA, ZIP and JSON. Emitting every optional source
file correctly hit Store's `artifact_mime_not_allowed` fence. Repeating
collection cannot change that immutable mismatch.

## Approach

Keep strict `ArtifactGateway.emit` as the default. Add an explicit
`emit_matching` entry for optional source-format selection. It shares the same
staging, size bounds, content detection and declared/content validation, then
excludes nonmatching formats before object persistence. Selected artifacts
still pass the unchanged Store role, disposition, generation and MIME checks.
The caller uses the frozen collection's MIME patterns, never a widened grant.

Widening the frozen contract would change authorization. Provider-specific or
case-specific filename exclusions would not generalize. Content-based selection
enforces the existing contract and lets any multi-format adapter use the same
gateway behavior. Empty MIME patterns preserve unrestricted selection.

The collection numbers selected outputs densely while preserving source order
and stable item identity across retries. It must not silently truncate to the
maximum item count. Store still rejects insufficient or excessive selected
items and requires successful validation/review before publication.

MIME detection recognizes standard PSD/TIFF headers and OpenRaster archives.
OpenRaster detection requires safe member paths, `stack.xml`, and a bounded
`mimetype` entry read. Declared/content mismatches remain errors even when a
candidate would otherwise be excluded.

## Verification

- Reproduce the failed multi-format collection before the implementation.
- Verify allowed files only, dense positions, stable retry Artifact IDs, and
  no persisted object for excluded content.
- Verify MIME spoofing rejection, strict default behavior, unchanged cardinality
  bounds, unrestricted empty patterns, and bounded archive metadata handling.
- Run artifact, adapter, Store, coordinator and worker tests, then C09 and
  affected artifact-producing real cases. Run a new clean 169-case gate after
  all defects are resolved.

This is an implementation choice within the user's authorization to resolve
generic execution defects continuously. No Ali, case-ID or operation-name
conditional is introduced.
