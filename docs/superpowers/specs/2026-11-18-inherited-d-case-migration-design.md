# Inherited D-Case Migration Design

Date: 2026-11-18

Status: approved by the user's requirement to migrate and execute every inherited case.

## Intent

Restore the `D01-D51` real-machine cases omitted when the Parallel catalog copied only Plus
`S/C/R`. The complete catalog becomes 149 cases: 129 inherited Plus cases and 20 Parallel cases.
Every D case retains its original ID, prompt, expected artifact contract, checks and source digest.

## Mapping Boundary

Plus-specific implementation names are mapped to the Parallel architecture without weakening the
risk being tested:

| Plus term | Parallel boundary |
| --- | --- |
| Manager / Orchestrator | Primary Harness Turn and Broker lease |
| WorkUnit / Task | Durable Job or Primary Harness execution stage |
| DAG expansion / join | dependency-free JobGroup plus continuation JobGroup |
| Agent revision | immutable replacement Job linked by `replaces_job_id` |
| CapabilityCall | Provider/Operation Job through the Gateway |
| Final Review | deterministic Validator plus optional/required Reviewer Job |
| Run input barrier | ordered Turn input or successor Turn |

The new platform must not reintroduce a mandatory Manager, predicted DAG, Delivery entity, or Plus
database schema merely to reproduce a test. A D case may use a deterministic protocol driver when
its invariant is independent of model behavior. Artifact-producing cases still use the deployed
Primary/Child Harness or real Provider path and validate the final bytes.

## Evidence Contract

- Catalog fields include `source_case_id`, `source_project`, `source_definition_sha256`, original
  prompt/checks and a non-empty `parallel_adaptation`.
- Every D handler emits concrete identities and observed facts, not a synthetic pass marker.
- Unsupported or missing mappings are program failures; no case may be silently skipped or marked
  not applicable.
- The campaign stops dispatch on program or optimizable prompt failures, checkpoints evidence after
  each completed case and resumes only untested/affected cases.
- Fault-injection cases use only the isolated Parallel services and state under `/data2/wujing03`.

## Case Groups

- D01-D08: staged execution, join, cancellation, pause and worker recovery.
- D09-D20: authorization, budget, tenant isolation, uncertain submission and scoped Skills.
- D21-D41: immutable replacement, ordered input, Artifact lineage, review and delivery selection.
- D42-D46: Primary/Reviewer/Child/Provider hard-kill takeover.
- D47-D51: multi-leaf delivery, attachment rejection, successor authorization and usage approval.

## Completion

Completion requires all 51 D cases to have a latest real-machine result, no program failures and no
clearly optimizable prompt failures. Model/provider failures may remain only when the program exposes
the precise bounded reason and publishes no invalid result. The eight existing rate-limit/timeout
cases are rerun in the same campaign. A fresh complete gate and release follow convergence.
