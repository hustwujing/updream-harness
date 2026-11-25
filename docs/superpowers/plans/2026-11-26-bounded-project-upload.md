# Bounded Managed-file Upload Implementation Plan

> **For agentic workers:** Implement in the existing native session with an independent review; preserve the authorized affected-only real campaign.

**Goal:** Materialize large managed files through bounded idempotent requests.

**Architecture:** Runtime owns staged uploads and atomic commit; RuntimeClient
splits byte/stream sources. Existing owner, sandbox, frozen input and Turn lease
boundaries remain authoritative.

**Tech Stack:** Python 3.13, FastAPI, httpx, pytest.

**Spec:** `docs/superpowers/specs/2026-11-26-bounded-project-upload.md`

## Global Constraints

- Edits and generated files stay in the isolated Ali worktree on /data2/wujing03.
- No provider/case/operation conditionals; no model/business execution retry.
- Chunk limit 4 MiB; file/staging limit 2 GiB; active uploads 4; receipts 128.

## Review Focus

- Lost replies must not duplicate chunks or republish an old file.
- Uneven streams and size mismatches cannot expose partial files.
- Reset/new writers invalidate stale requests before target replacement.
- Cancellation, restart and expiry reclaim only owned staging files.
- Managed path links and resource limits remain protected.

## Task 1: Bounded client and Runtime protocol

Files: create `src/sandbox_runtime/project_uploads.py` and
`tests/test_project_uploads.py`; modify `src/sandbox_runtime/app.py`,
`src/sandbox_harness/runtime.py` and the managed transfer development guidance.

- [x] Add an ASGI transport test rejecting requests over 4 MiB, with byte and
  uneven-stream input, plus lost chunk/commit replies. Confirm current failure.
- [x] Implement `ProjectUploads` staging, offset validation, final digest,
  atomic publication, bounded expiry/abort and write fencing.
- [x] Add PUT upload initialization, PUT numbered-offset chunks, POST commit,
  DELETE abort; connect managed reset and legacy single-file write fences.
- [x] Implement shared bounded client transfer and validate final receipt.
- [x] Test negative transport/path/concurrency/resource boundaries, then run
  Runtime/Broker/Inspector focused checks and independent review.
- [x] Commit this independent fix, run full Python and release build, record
  isolated deployment and verify real large upload, C29 and Inspector restore.

Verified by commits `707bf58` and `136bcb1`: full suite, real Ali/OpenSandbox
large transfers with lost replies, original C29 Matroska replay, fresh C29,
large Inspector restore and affected D18/D19/P18/D42 reruns.
