# Image Inference

Independent GPU service for layered image decomposition. MediaWorker is its only
platform execution owner; this module owns model loading and inference runtime.

- Entry: `service.app:app` on `127.0.0.1:8912`
- External input: materialized `image-layered` code, environment and model weights
- Verification: health, real create/poll/cancel/artifact and DM recovery cases
