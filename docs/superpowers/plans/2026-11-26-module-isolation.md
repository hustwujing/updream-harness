# Module Isolation Implementation Plan

> **For agentic workers:** Implement natively in this session with focused independent
> review and a final fresh review. The user explicitly authorized execution after
> the complete inventory and subsequently required a fresh full real-case gate;
> do not ask again to approve this plan or execution method.

**Goal:** Isolate every service/execution artifact and package shared capabilities
as explicit common libraries, supporting independent service releases/rollback.

**Architecture:** Keep one repository; the later user instruction requires fresh
independent accounts/core/media/sites databases with explicit durable handoffs.
Seven host platform services consume declared common closures; sandbox Runtime,
Bootstrap, content/images, controlled execution, inference and browser artifacts
have separate manifests and immutable versions. Preserve transactions/protocols.

**Tech Stack:** Python 3.13 host, existing FastAPI/SQLAlchemy/PostgreSQL/systemd,
existing signed sandbox bundles/images and controlled interpreter/model snapshots.

**Spec:** `docs/superpowers/specs/2026-11-26-module-isolation-design.md` and the
superseding database design `docs/superpowers/specs/2026-11-26-independent-databases.md`.

## Global Constraints

- Work only in `/data2/wujing03/sandbox-harness-platform-parallel-ali`, branch
  `sandbox-harness-platform-parallel-ali`; original sibling remains read-only.
- Generated code snapshots, virtualenvs, logs, caches and fixtures stay under
  this checkout's `.data` or another explicit `/data2/wujing03` location.
- Canonical host service name `sandbox-gateway`; Python package `sandbox_gateway`.
- 14 product build/release modules, 8 long-running host application services.
- No service→other-service implementation imports, common→service imports, or
  host-only dependencies/credentials in sandbox bundles.
- Preserve 7 historical uncertain capacity obligations and paused Job identity;
  unknown external execution never permits blind resubmission or capacity release.
- New user requirement supersedes affected-only final coverage: after refactoring,
  run all 169 product + RC7 + AR4 + DM4 real cases against the final release.
  Supplemental sandbox-provider/browser/terminal gates are separate evidence.
- Seedream is currently closed; recheck, and retain external failures as failures.

## Review Focus

- Lazy imports/data paths after live source moves: freeze baseline, clean artifact
  imports with repository paths absent, then check actual process source paths.
- Model/Row/exception identity and transaction boundaries across Store splitting:
  metadata and facade identity plus populated PostgreSQL concurrency tests.
- Runtime pure-common closure, frozen bundle compatibility and CLI path visibility:
  signed bundle activation and real Ali/OpenSandbox commands/restore.
- Native binaries, GPU/IPC permissions and external source image/model inputs:
  manifest validation plus actual document conversion/controlled execution.
- Mixed host releases and one-service rollback: old/new compatibility, unchanged
  other service descriptors, exact execution identities and retained cleanup debt.

## Task 1: Baseline protection and executable module inventory

**Files:** `modules/**/module.json`, `tools/module_manifest.py`,
`tests/test_module_manifests.py`, `.data/evidence/module-isolation/`.
JSON is chosen for machine-readable manifests (same schema/purpose as the spec's
illustrative `module.toml`); no new TOML writer dependency is required.

**Interfaces:** `load_manifests(root: Path) -> dict[str, dict]`,
`validate_manifest(value: dict) -> None`, `module_sources(root, manifest) -> list[Path]`.
Manifests declare id, kind/location, entry point, sources, common closure, resources,
Python/native requirements, external inputs and verification commands.

- [x] Capture service/process/version/resource state and freeze the deployed source
  and web assets under `.data/releases`; repoint only owned services to that
  baseline before changing paths. Verify health and source file paths.
- [x] Add red tests: exactly 14 IDs, supported kinds, safe explicit source roots,
  executable entry points, native/external inputs, no unknown source ownership.
- [x] Implement manifest loader and all source ownership declarations from the
  complete inventory; retain an explicit staging map while source moves land.
- [x] Run `tests/test_module_manifests.py`; commit the independently useful inventory.

## Task 2: Remove accidental cross-owner dependencies

**Files:** shared file/process/network helpers, allocation client contracts,
review coordinator/contracts, stable media error/receipt contracts; consumers and
focused helper tests. Source locations follow common directories in the spec.

**Interfaces:** Preserve existing callable signatures and object identity for
`staged_file_chunks`, process identity, network policy helpers, allocation request/
client protocol, reviewer coordinator and adapter contracts.

- [x] Add boundary regression tests covering Controlled→Harness and Media→Runtime/
  Gateway imports and allocation client importing a service loop; observe failures.
- [x] Extract the existing helpers and shared contracts without behavior changes;
  keep implementation transport/service logic with its owner.
- [x] Run existing controlled/media/Job/allocation/reviewer/client tests plus the
  boundary tests; commit the coherent shared-library extraction.

## Task 3: Service/common packages and Runtime boundaries

**Files:** `src/sandbox_harness/services/**`, `src/sandbox_harness/common/**`,
`src/sandbox_harness/tools/**`, `src/controlled_runtime/**`, `pyproject.toml`,
`runtime/build_runtime_code_bundle.py`, import consumers in tests/tools/deploy.

**Interfaces:** Existing public functions/classes preserved under mapped packages;
new canonical CLI `sandbox-harness-parallel-sandbox-gateway`. Entry-point and
module manifest identities are authoritative; existing operator aliases may
forward during migration, without duplicate implementations.

- [x] Add failing service import-boundary and Runtime bundle closure tests.
- [x] Move all mapped modules, rewrite source and test imports/patch targets,
  split controlled fixed scripts from host launcher and update resource lookup.
- [x] Update signed Code Bundle builder to include only declared portable common;
  keep allowed package roots, wire protocol and old retained bundle identities.
- [x] Replace `__file__.parents` checkout assumptions with explicit packaged
  resource resolution; add tests with absent repository cwd and relocated assets.
- [x] Run focused API/worker/Runtime/CLI tests and full collection/import checks;
  fix source-reference tests to check new authoritative locations, then commit.

## Task 4: Database separation — scope revised by user

**Files:** `src/sandbox_harness/common/db/{models,base,upgrades,store}.py`,
domain transaction modules, Store tests and PostgreSQL tests.

**Revision:** The user now requires independent databases and no backward
compatibility. The mechanical shared-Store source split may prepare clear domain method
groups but is not the final physical-database design. Complete the cross-domain transaction audit and define new owned
schemas, commands/outbox/inbox, idempotency and recovery after the database
partition boundary is settled. Existing red Store-boundary tests must follow
that final design; old compatibility is no longer an acceptance requirement.

- [ ] Add regression for metadata/Row identity, facade method coverage and atomic
  cross-domain handoff/publication before splitting source ownership.
- [ ] Extract models/upgrade helpers and domain method groups in reviewable steps;
  preserve all original transaction bodies and generation/owner predicates.
- [ ] Run relevant Store/component tests and isolated PostgreSQL populated-upgrade,
  ownership, allocation, publication and concurrency tests; commit the separation.

## Task 5: Isolated artifacts and compatibility metadata

**Files:** `tools/build_module.py`, module manifests, resource utilities,
`tests/test_module_packaging.py`, deployment build documentation.

**Interfaces:** `build_module(root: Path, module_id: str, output_root: Path) -> Path`
returns an immutable artifact manifest; records source/common/dependency/resource
hashes, schema/protocol compatibility and all external input identities.

- [ ] Add red tests for source closure, excluded other services, unsafe sources,
  hash mismatch, missing native/external inputs and cwd-independent resources.
- [ ] Build host service packages with exact common closure and immutable dependency
  environments; split native/runtime/content/image/inference/web build contracts.
- [ ] Test imports and entry points in clean isolated environments without editable
  checkout; validate Runtime bundle payload and controlled program independence.
- [ ] Verify frontend PDF-worker assets, document conversion, and current external
  image/model references. Independent native/build inputs may remain referenced
  snapshots; do not duplicate weights or rebuild unchanged provider images.
- [ ] Commit build tooling and manifests after focused packaging tests pass.

## Task 6: Per-service launch, release and rollback

**Files:** `deploy/ali/run-service.sh`, `start.sh`, `stop.sh`, `status.sh`,
release/build commands, `tests/test_module_deployment.py`, isolation tests.

**Interfaces:** `.data/service-releases/<service>/<digest>/manifest.json` immutable;
`.data/service-current/<service>.json` atomic non-secret descriptor selects it.
Shared `platform.env` supplies environment/credentials, descriptor supplies only
allowlisted release paths/versions/catalogs. Never shell-evaluate manifest values.

- [ ] Add red launch/selection tests: path escape/hash/identity mismatch rejected,
  only selected descriptor changes, old/new names never start duplicate Gateway.
- [ ] Implement artifact launch with exact source/dependency path and resource
  selection, atomic descriptor promotion and tested compatible rollback.
- [ ] Migrate sandbox-gateway CLI/unit/status/test controls while preserving ports,
  authentication and provider routes; legacy lookup supports rollback only.
- [ ] Verify one-service changes leave other process versions untouched and no
  released service imports mutable checkout paths; commit deployment tooling.

## Task 7: Release verification and complete real gate

**Files:** development guides/README, module manifests and release evidence;
existing real-case/fault runners updated for canonical entry points and units.

- [ ] Run full Python suite, required isolated PostgreSQL tests, frontend tests/build,
  package isolation, all module entry checks and independent whole-branch review.
- [ ] Commit the release, freeze artifact/bundle/catalog descriptors, check active
  ownership, deploy isolated Ali and verify health, browser, terminal, recovery,
  controlled and image-inference contracts plus both sandbox-provider protocols.
- [ ] Recheck Seedream and execute all 169 product + RC7 + AR4 + DM4 cases at the
  final release. Report every two minutes; investigate while cases run. Record
  platform revision separately from test-driver revision; no retained successes
  count as this fresh full gate. Fix failures and rerun affected cases; if code
  changes invalidate the full-release claim, complete a full final-version gate.
- [ ] Confirm no internal platform exceptions hidden by successful top-level cases.
  Any external blocked case remains failed/unrun; no unsupported passing claim.
- [ ] Record all 184 results, per-module versions, cleanup and rollout receipts;
  preserve referenced old releases/unknown allocations, scan secrets and push.

Task 2 evidence: five red boundary tests, 157 focused passes and 40 additional
allocation/recovery passes. Independent review confirmed 19 extracted definitions
retain identical AST bodies and caught a relocated client-clock monkeypatch;
that regression was reproduced and its mock target corrected.

Task 3 evidence: 12 red package/boundary tests; 28 boundary and 58 focused passes.
Full Python verification after correcting the sandbox-gateway Skill resource
lookup and current design traceability paths: 1412 passed, 14 skipped. Old
production remains pinned to the verified 8b79ea5 read-only snapshot.
