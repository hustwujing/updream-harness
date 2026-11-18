---
name: media-processing
description: Process, inspect, convert, compose, or validate local audio, video, and raster media with FFmpeg/FFprobe and the installed Python media stack. Use for delegated media work that requires those tools; do not use for external generation providers or GPU-only operations.
---

# Media Processing

Use the frozen input artifacts as the only source of truth. Inspect streams with `ffprobe` before editing and prefer one vectorized FFmpeg filter graph for ordinary trimming, scaling, compositing, timing, audio mixing, and format conversion.

Keep scripts, decoded buffers, diagnostic frames, and intermediate renders under `/workspace/project/work`. Put only the requested deliverables and explicitly requested sidecars in `/workspace/outputs`, then verify their declared duration, dimensions, streams, codecs, and hashes before returning.

Do not call external generation providers, install packages, assume GPU access, or replace a registered host Operation. If the objective requires a capability absent from this Profile, report the missing dependency instead of approximating it.
