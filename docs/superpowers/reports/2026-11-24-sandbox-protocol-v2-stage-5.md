# Sandbox Protocol v2 - Stage 5 Report

## Scope And Design

Stage 5 replaces Catalog schema v1 image matrices with provider releases. A release records provider ref, provider revision, and artifact digest separately, allowing image, template, snapshot, or future provider-native release types.

Implementation commit: `98639890734b8e55fbf2728c7e6910dd5ab43f85`

## Development

- Added immutable `ProviderRelease(ref, revision, artifact_digest)` and Catalog schema version 2.
- Replaced serialized `image_refs/image_digests` with `provider_releases[provider][harness]`.
- Materialized all current profiles as OpenSandbox releases backed by the existing digest-pinned images.
- Updated catalog builder, signed release validation, primary image extraction, build scripts, and isolated Ali asset rewriting.
- Changed recovered child compatibility to compare Handle revision with the frozen release revision instead of parsing the sandbox image string.
- Added a durable provisional Handle receipt for media operation sandboxes before Runtime injection.
- Kept internal OpenSandbox image projections for the current scheduler API; these are derived from provider releases and are not part of the serialized schema.

## Migration And Rollback

- Catalog schema v1 is rejected; there is intentionally no legacy parsing fallback.
- Existing OpenSandbox image identities and artifact digests are preserved exactly in schema v2.
- Ali provider releases are not invented before template validation; attempting to select an absent provider release fails closed.
- Rollback restores the previous signed Catalog and implementation commit.

## Tests

- Profile resolver, Job Service, coordinator, catalog builder, signed release, Ali materialization, child recovery, and media operation suites: `78 passed`.
- Coverage verifies schema v2 has no serialized legacy keys, complete provider matrices, digest pinning, signature verification, isolated registry rewriting, and Handle revision recovery.
- `git diff --check`: passed.

## Review Findings

- Recovery compatibility no longer depends on `image.endswith(@sha256:...)`; Handle revision is authoritative.
- Start-time image digest checks remain as provisioning integrity checks for the OpenSandbox scheduler and are not used for recovery identity.
- No deployment, catalog publication, image promotion, or cloud mutation was performed.

## Residual Risks

- Only OpenSandbox provider releases exist until Ali templates are validated and deliberately published.
- Runtime route injection still uses global Settings and is resolved in Stage 6.
