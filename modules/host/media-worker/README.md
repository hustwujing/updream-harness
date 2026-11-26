# Media Worker

Owns confirmed image, video, audio, 3D and image-decomposition provider execution,
including submit, poll, cancel, collection, staged outputs and terminal receipts.

- Entry: `sandbox_harness.services.media_worker.media_service:run`
- Database: media only
- External inputs: configured providers and independent image-inference service
- Verification: media repository/worker/provider tests plus K/DM real cases
