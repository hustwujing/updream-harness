# Cross-Sandbox Project Files Report

Date: 2026-11-09

Scope: versioned Thread project-file manifest, object-store retention, Runtime materialization, owner-scoped list/download API and Workspace browser UI.

## Deployment

The project-file implementation is commit `4d9a7e0`; the final deployed source also includes checkpoint deduplication from `e47d1d9`. Runtime image digests are:

| Harness | Immutable image digest |
| --- | --- |
| Codex | `sha256:9a6b2484a2f082a1b6675db32995187ff33e27d39efbd678c22bed8b76aba814` |
| Claude | `sha256:7c47d03b74c9afd529c7750ff2ef5e7ce6d743927323d2ba6c2831e894f2b0aa` |
| DeepSeek | `sha256:576332b0a69c1cc0cf98bb419c021afea38e9d4f2bc8dc071ed26d50a9543cc8` |

## Automated Verification

The final Python suite passed 58 tests and the Vite production build completed. Tests cover manifest versioning and idempotency, owner isolation, artifact registration, managed path restrictions, Sandbox materialization and exclusion of object-backed media from checkpoint tar files.

After the final image update, real model smokes passed for Codex `gpt-6-sol`, Claude `claude-sonnet-5-5` and DeepSeek `deepseek-v4-pro`; each final digest executed, checkpointed and exported a validated JSON artifact.

The browser test used a signed isolated test session and rendered the Workspace panel at 1440x900 and 390x844. It verified separate project asset and project artifact sections, three download rows, stable panel bounds and no page errors. Screenshots are retained under `.data/project-files-desktop.png` and `.data/project-files-mobile.png`.

## Real Cross-Sandbox Case

Thread `e673804d0d374c0794fe2a8bd6e783f2` uploaded `reference.txt` and ran two Claude Turns.

The first Turn used Sandbox `251a76a8-539a-4afd-8f3e-818be0554c0e`, read the current-message inbox attachment and produced `first.json`. The API was then restarted, deleting the completed attached Sandbox. The second Turn used new Sandbox `98946143-fe20-49e0-adce-b04d921ff745`, received no attachments and was explicitly prohibited from reading inbox. It read:

- `/workspace/project/assets/reference.txt`
- `/workspace/project/artifacts/first.json`

It then produced and independently downloaded `artifacts/second.json` with:

```json
{"asset":"persistent-reference-v1","previous_stage":1,"stage":2}
```

The final current manifest contained three version-1 logical files with distinct content hashes: `assets/reference.txt`, `artifacts/first.json` and `artifacts/second.json`. This proves the second Sandbox obtained both prior input and prior output from the object-backed project manifest rather than container-local inbox/output state.
