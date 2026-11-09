# Cross-Sandbox Project Files Report

Date: 2026-11-09

Scope: versioned Thread project-file manifest, object-store retention, Runtime materialization, owner-scoped list/download API and Workspace browser UI.

## Deployment

The deployed source includes commit `4d9a7e0`. Runtime image digests are:

| Harness | Immutable image digest |
| --- | --- |
| Codex | `sha256:279cc36892c82953f97d82270073a070c55f1b23de646d6f03a7584370ee47b8` |
| Claude | `sha256:f751cee7a8defc18449e0378ac0d8329e5856234a9dd8110e8488ef27675b89a` |
| DeepSeek | `sha256:c9c474d5436f3853bef7b0e8683b7a1e0c4efbd93bb7dafe4619da8ef9cb62c6` |

## Automated Verification

The Python suite passed 47 tests and the Vite production build completed. Tests cover manifest versioning and idempotency, owner isolation, artifact registration, managed path restrictions and Sandbox materialization.

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
