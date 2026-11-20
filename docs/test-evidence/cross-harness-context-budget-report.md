# Cross-Harness Context And Budget Report

Date: 2026-11-20

## Root Cause

Thread `ec24e432d8864697930a0a6709a427fd` exhausted the local 2M Turn token
budget after fourteen successful Codex calls. Input grew from about 113k to
138k logical tokens per call. The final Gateway response was the 41-byte local
`model_token_budget_exhausted` body and never reached upstream, but the native
client retained only HTTP 429 and the platform mislabeled it as model rate
limiting.

## Cross-Harness Probe

A real Gateway probe established the native usage shapes:

| Harness | Ordinary input | Cache read | Cache write | Output | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Codex GPT-5.6 | 167 | 4,224 | 0 | 5 | HTTP 200 |
| Claude Opus 5.5 | 2 | 967 | 846 | 4 | HTTP 200 |
| DeepSeek 4.1 Flash | 35 | 0 | 0 | 11 | HTTP 200 |

The configured Codex upstream returned HTTP 502 for `/responses/compact`, so
the release selects Codex local compaction instead of advertising remote v2.
DeepSeek documentation likewise states Responses `context_management` is not
supported; its native Session remains authoritative.

## Implemented Contract

- All three protocol usage shapes normalize into four non-overlapping fields.
- Budget settlement applies configurable read/write cache weights; logical
  input remains separate telemetry.
- Unknown cache outcome is not precharged as ordinary input before submission.
- Local exhaustion is a durable model failure and survives native error loss.
- Codex auto-compacts locally at the explicit threshold. Claude and DeepSeek
  retain native context management.
- Failed Turns show a dismissible sanitized tip without raw diagnostics.

Final current-revision gate and deployed real-Harness evidence are appended at
release completion.

## Deployed Two-Turn Sessions

Candidate `8c7b213307e3` ran two native Turns for each Harness. All six Turns
completed and the second Turn resumed the original native Session. Durable
model activity and reservation settlement produced:

| Harness / Turn | Logical input | Ordinary | Cache read | Cache write | Output | Budget charge |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Codex 1 | 21,200 | 17,488 | 3,712 | 0 | 10 | 17,870 |
| Codex 2 | 23,733 | 2,741 | 20,992 | 0 | 10 | 4,851 |
| Claude 1 | 44,773 | 2 | 0 | 44,771 | 15 | 55,981 |
| Claude 2 | 48,291 | 2 | 35,132 | 13,157 | 15 | 19,978 |
| DeepSeek 1 | 16,322 | 10,306 | 6,016 | 0 | 7 | 10,915 |
| DeepSeek 2 | 18,969 | 13,593 | 5,376 | 0 | 7 | 14,138 |

The test Threads and their retained Sandboxes were deleted after evidence was
captured. The temporary probe script was removed.
