# Audio Process Operation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: implement this plan task-by-task with tests before product edits.

**Goal:** Add a registered, deterministic `audio.process` Operation that runs against frozen Artifacts in an authorized OpenSandbox media profile and atomically returns processed audio plus an objective report.

**Architecture:** Extend the existing `media.render` controlled Operation adapter rather than adding a host bwrap worker. The signed Job Catalog freezes the public schema and execution policy; the Runtime performs networkless FFmpeg processing; Artifact Gateway commits both declared outputs before continuation.

**Tech Stack:** Python 3.13, FastAPI/Pydantic, FFmpeg/FFprobe, OpenSandbox, PostgreSQL durable Jobs.

**Spec:** `docs/superpowers/specs/2026-11-16-primary-harness-durable-job-platform-design.md`

## Global Constraints

- No database migration or backward compatibility path.
- Input bytes come only from frozen Job Artifacts.
- Runtime profile must be `media-cpu` or `primary-full-cpu`; no network or Provider credential.
- Retry is bounded and deterministic; output publication is atomic at the JobGroup boundary.
- Persistent state remains under `/data2/wujing03/sandbox-harness-platform-parallel/.data`.

## Review Focus

- Reject missing/non-audio frozen inputs before processing.
- Preserve source duration, sample rate and channel count unless the schema explicitly asks otherwise.
- Two-pass LUFS must fail closed when FFmpeg measurement JSON is unavailable.
- Collect audio and report under distinct declared roles; no partial publication.
- Cancellation/restart must use the existing deterministic Operation lease and cleanup path.

### Task 1: Freeze the catalog contract

**Files:** `deploy/job-catalog.example.json`, `src/sandbox_harness/catalog.py`, `src/sandbox_harness/gateway.py`, `src/sandbox_harness/store.py`, `tests/test_job_catalog.py`, `tests/test_store.py`

- [ ] Add `audio.process@1` with exact parameter schema, `media.render` adapter policy, media CPU resources, two output roles and bounded deterministic retry.
- [ ] Return Descriptor parameter schemas, the JobGroup schema and frozen input Artifact identities from `sandbox-job catalog`; input aliases remain hidden from user delivery Items.
- [ ] Test valid requests and reject unknown fields/ranges.
- [ ] Commit.

### Task 2: Execute deterministic audio processing

**Files:** `src/sandbox_runtime/app.py`, `tests/test_runtime_configuration.py`

- [ ] Add a failing Runtime test using a generated WAV.
- [ ] Extend `MediaRenderOperation` and `/v1/operations/media-render` for gain, denoise, high-pass, fades and optional two-pass LUFS.
- [ ] Produce `processed.{wav|mp3|m4a}` and `audio-process-report.json`; assert duration and stream metadata.
- [ ] Run targeted tests and commit.

### Task 3: Atomically collect multiple Operation outputs

**Files:** `src/sandbox_harness/media_render_adapter.py`, `tests/test_media_render_adapter.py`

- [ ] Add a failing adapter test with `processed_audio` and `measurement_report` output contracts.
- [ ] Generalize collection from the legacy single path to role-addressed outputs while keeping one-output render operations valid.
- [ ] Run Job/adapter/runtime tests and commit.
- [ ] Publish Runtime Code Bundle, restart isolated services and rerun `C16` only.
