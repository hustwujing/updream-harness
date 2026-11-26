# Controlled Worker

Runs fixed host Operations such as image segmentation and transcription inside a
validated bwrap environment with explicit CPU, memory and GPU reservations.

- Entry: `sandbox_harness.services.controlled_worker.controlled_service:run`
- Database: core only
- External input: manifest-addressed controlled runtime/models/interpreters
- Verification: controlled adapter/runtime manifest and real Operation cases
