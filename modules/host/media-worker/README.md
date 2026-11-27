# Media Worker

Owns confirmed image, video, audio, 3D and image-decomposition provider execution,
including submit, poll, cancel, collection, staged outputs and terminal receipts.

- Entry: `sandbox_harness.services.media_worker.media_service:run`
- Database: media only
- External inputs: configured providers and independent image-inference service
- Verification: media repository/worker/provider tests plus K/DM real cases

Provider contracts are registered by Adapter under
`src/sandbox_harness/common/providers/`. They own public parameter metadata,
local preflight and request normalization. `MediaCatalog` only resolves generic
model descriptors and rejects any unregistered Adapter.
