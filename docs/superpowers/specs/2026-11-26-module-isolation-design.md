# Service modules and common libraries

Status: proposed design, awaiting user approval. No product source, database or
running deployment changes are part of this design commit.

Independent builder check completed with no material unanswered design questions.

## Intent and scope

The user requested module isolation with shared code in `common`, to simplify
management and independent deployment. Keep one repository and the existing
seven platform services, separate image-layering inference process, sandbox
Runtime and Bootstrap. Give each service its own source boundary, entry point,
configuration validation, dependency set, immutable release and rollback target.
Common is a library bundled with each consumer, not a new network service.

This is one staged architecture migration. Success requires enforceable import
boundaries and independently installable/releasable artifacts, not just moving
files into folders. Preserve public APIs, database schema, serialized contracts,
job outcomes and provider-neutral recovery behavior.

Implementation choices below are the author's recommendations, not decisions
already approved by the user. The existing native implementation workflow is
suitable; focused independent reviews follow transaction and deployment stages.

## Approaches

1. Directory grouping inside the current all-in-one deployment: smallest move,
   but it leaves one dependency closure and mutable shared release path. It does
   not satisfy independent rollout and rollback.
2. **Recommended: one repository, service modules and common libraries, separate
   immutable service distributions.** Shared contracts can change atomically in
   source, while each deployed service pins its own common snapshot. This fits
   the current transactional architecture and existing systemd deployment.
3. Separate repository and database per service: stronger organizational
   separation, but introduces cross-service transactions and protocol/version
   coordination. The current request does not justify replacing the database
   consistency model with distributed transactions.

Keep Python 3.13, FastAPI, SQLAlchemy/PostgreSQL, current sandbox-provider
configuration and systemd. Do not introduce Kubernetes, a message broker or a
new common RPC service for this refactor. Container publication can use the same
service manifest, but all seven application services need not move to containers
as a condition of finishing this work.

## Inspected baseline

Source: `3438c46`; running application release: `8b79ea5`.

- `src/sandbox_harness` contains 76 top-level Python modules, 28,537 lines.
- `store.py`: 7,695 lines, 55 ORM Row classes, 339 Store methods.
- `pyproject.toml` builds one distribution containing host, Runtime and Bootstrap;
  all host console scripts share dependencies.
- `runtime/service.Dockerfile` copies the whole checkout and installs that one
  distribution. Ali units use the same mutable worktree and `.venv`.
- `deploy/record-release.py` rebuilds web and both signed bundles and writes a
  single global release/PYTHONPATH into `platform.env`.
- `controlled_media_adapter.py` imports `staged_file_chunks` from `harness_job`.
  `media_worker.py` imports `_process_start` from the Runtime HTTP client and
  address validation from Gateway's proxy implementation.
- API and Gateway legitimately share Job preflight/coordinator and review
  contracts. They must share domain libraries, not import another service.
- Runtime Code Bundle explicitly copies six host modules; Runtime imports
  observability, domain/Job contracts, media catalog and site packaging helpers.
  Bootstrap allows the `sandbox_runtime`, `sandbox_harness` and `editppt` package
  roots. Changing host module paths changes bundle contents even without wire
  protocol changes.
- No `.codegraph/` exists. Static AST inventory, including deferred function
  imports, is retained under `.data/evidence/module-isolation/dependency-inventory.json`.
  This inventory is a starting point; packaging checks must cover dynamic and
  path-based loads as well.

## Target source structure

```text
src/
  sandbox_harness/
    services/
      api/                  # HTTP, Broker, inspector, browser serving
      gateway/              # capability routes, streaming, public proxy server
      job_worker/           # sandbox Child/Reviewer/Operation adapters
      allocation_worker/    # provider-create and reconciliation runner
      media_worker/         # external media lifecycle and provider transports
      controlled_worker/    # bwrap Operation adapters and fixed programs
      site_worker/          # static site deployment runner
    common/
      contracts/            # serialized domain types, error/receipt contracts
      catalog/              # profiles, readiness, bounded preflight policy
      execution/            # generic JobWorker engine and adapter interfaces
      jobs/                 # coordination, review policy, durable media handoff
      db/                   # shared models, schema upgrades, transaction APIs
      sandbox/              # provider protocol, clients, pool, Runtime client
      scheduling/           # NodeAgent, inventory and resource reservations
      artifacts/            # artifact validation and bounded collection
      storage/              # object store and bounded file helpers
      security/             # capabilities, grants and network address policy
      observability/        # logging, tracing, Runtime ingestion
      configuration/        # common settings and service-specific validation
      events/               # Redis transport and activity projection
      models/               # route/catalog health and activity contracts
      sites/                # portable static-site packaging helpers
      process/              # process identity helpers
    tools/                  # administrative/log CLI consumers
  sandbox_runtime/          # in-sandbox app, CLIs and Harness adapters
  sandbox_bootstrap/        # image-owned signature verification and activation
services/
  api/service.toml
  gateway/service.toml
  job-worker/service.toml
  allocation-worker/service.toml
  media-worker/service.toml
  controlled-worker/service.toml
  site-worker/service.toml
  image-inference/service.toml
  runtime/service.toml
  bootstrap/service.toml
web/                        # frontend built only for API release
runtime/                    # signed bundles, profiles, Skills and image inputs
```

Use a shared `sandbox_harness` namespace with a side-effect-free initializer.
Every host artifact contains its own copy of only its service plus its required
common modules. Host services do not install `sandbox_runtime` or
`sandbox_bootstrap` merely to import common. Runtime includes an explicit safe
common subset in its signed Code Bundle. This keeps Bootstrap's existing allowed
package roots; a new package-root allowlist or image rebuild is unnecessary for
source moves alone. Actual dependency or Bootstrap changes still require image
rebuild and image gates under existing rules.

Each `service.toml` defines identity, console/module entry, allowed source files,
common dependencies, third-party dependencies, non-secret required config names,
packaged data, runtime assets and verification commands. Inference's manifest
pins the existing materialized implementation/environment; do not copy model
weights into application wheels or import source from predecessor projects.

## Source ownership and extractions

Preserve recognizable filenames within their new owner to keep history and
review readable. The complete 76-module assignment is checked in the appendix below; the
following fixes the ownership and extraction decisions.

| Owner | Existing implementation / required extraction |
| --- | --- |
| API | `main`, `app`, `broker`, `sandbox_inspector`, `document_preview`, `execution_graph`, `job_continuation`, `auth`, `sessions`, `model_discovery` |
| Gateway | `gateway`, proxy listener/CONNECT logic from `public_proxy`; network validation primitives move to common/security |
| JobWorker | `job_service`, `harness_job`, `media_render_adapter`, `media_job_adapter`, `image_decompose_adapter`, execution adapter part of `reviewer` |
| AllocationWorker | `allocation_service`, worker/service half of `allocation_dispatch` |
| MediaWorker | `media_service`, `media_worker`, `image_decompose_provider`, `media_reference_publisher`, `ark_assets`, `minimax_voice`, provider-specific response classifier from `media_errors` |
| ControlledWorker | `controlled_service`, `controlled_media_adapter`, `controlled_media_runner`, `sam3_job`, `transcribe_job` kept together for relative script materialization |
| SiteWorker | `site_service`, `site_worker` |
| Common contracts | `domain`, `job_contracts`, `sandbox_contracts`, `job_errors`, stable media-error guidance and pure receipts/protocols from `media_provider` |
| Common execution / jobs | `job_worker`, `job_adapters`, `job_coordinator`, shared review coordinator/types; transactional media handoff interfaces if extracted from adapters |
| Common sandbox | `sandbox_factory`, provider adapters, `pool`, `runtime`, `job_allocation`, client and request/view contracts from `allocation_dispatch` |
| Common catalog / models | `catalog`, `profiles`, `job_catalog`, `media_catalog`, `execution_readiness`, `provider_preflight`, `media_references`, `model_routes`, `model_health`, `model_activity` |
| Common infrastructure | `scheduler`, `node_agent`, `objects`, `artifact_gateway`, `capability`, `grants`, `events`, `activity_projection`, `observability`, `telemetry_ingest`, `site_bundle`, `config`, generic `errors` |
| Tools | `auth_cli`, `log_query` with exact required common/API authentication dependency; extract shared authentication primitives if needed instead of importing API startup |

`staged_file_chunks` moves to common/storage; process identity to common/process;
public address normalization/validation to common/security. These extractions
remove the demonstrated Controlled→Harness and Media→Runtime/Gateway imports
without duplicating behavior. Allocation clients must not import the allocation
service loop. Provider result structures used by Store must not import a media
transport or its service startup.

Keep provider-neutral lifecycle decisions in common. Provider adapters may map
provider wire responses; no case, Operation or vendor names enter shared recovery
state branches. MediaWorker retains image.decompose transport ownership; inference
remains a separate process. ControlledWorker still uses the generic JobWorker
engine and bwrap, not an LLM Harness or a new sandbox allocation.

## Common dependency rules

- Services can import their own implementation and declared common modules.
  They cannot import another service implementation or startup module.
- Common cannot import any service, Runtime application or Bootstrap.
- Contracts, portable site helpers and base telemetry are the bottom layer;
  they must not load Store, settings/secrets, provider SDKs or service globals.
- DB depends on common contracts and explicitly extracted pure helpers. Domain
  workflows and workers depend on DB APIs. Keep imports acyclic; an existing
  workflow/DB helper cycle must be resolved by extracting its pure contract,
  not by deferred imports that conceal the dependency.
- Host-only sandbox SDKs, PostgreSQL/Redis clients and credentials never enter
  the signed Runtime common subset. A manifest includes its transitive imports,
  tested in an environment without the checkout or editable installation.
- Configuration validation applies to the service's needs. SiteWorker does not
  require model/sandbox/GPU credentials merely because another service does.
  Validation must not weaken required fields for consumers that need them.

CI/local gates enforce imports by AST and actual packaged import tests. Dynamic
imports and copied script/data paths are declared in manifests. Reject unknown
unowned source files and wildcard inclusion of the whole repository. A temporary
migration exception must name an exact edge and removal stage; the completed
migration has none between service implementations.

## Database module boundaries

The services continue to use this product's authoritative PostgreSQL database.
Module isolation does not mean separate databases for each service. It remains
fully isolated from the original product's database and namespaces.

Split `store.py` into common/db modules by responsibility: connection/base model
registry, additive upgrades, auth/ownership, Thread/Turn/input transactions,
Job/attempt/group/continuation transactions, allocation/cleanup, reservations,
artifacts/project files, media and sites. Preserve one SQLAlchemy metadata/Row
identity and the current `Store` public facade during mechanical migration.

Preserve each existing atomic transaction as one unit. For example, Job terminal
publication, budget release and continuation outbox remain in the same commit;
image.decompose handoff remains atomic. Cross-domain atomic workflows belong in
an explicit transaction module with one session; repositories must not secretly
open independent sessions/commits when called inside that workflow. No schema,
column, serialized enum or receipt format changes are required by the split.

Move models/upgrades first, then coherent method groups with owner/generation
fence and PostgreSQL race tests. Do not split 339 methods in one opaque move or
copy Store into every service. Retain one source implementation; deployments
bundle immutable copies of the common version they were built against.

Existing startup initialization remains idempotent for this schema-preserving
migration. The release manifest records schema compatibility; old/new service
versions must coexist against the same database. A future schema change needs
an additive expand/contract rollout and a tested compatibility range; independent
release never means arbitrary incompatible common versions may run together.

## Independent artifact and rollout contract

The first implementation delivers immutable filesystem releases under this
checkout's `.data`, using the current systemd service model. A service release
contains only its source/common closure, pinned installed dependencies, static
resources and a machine-readable manifest. Shared binary/package caches are
allowed; no deployed service imports mutable code from the checkout or from
another service's active release. Exact lock/constraints hashes are recorded.

Each manifest records service name, Git revision, own/common content digests,
entry point, dependency lock digest, schema/protocol compatibility, profile and
Runtime bundle references, static asset digest where applicable, and test receipts.
Common changes produce new artifacts for affected consumers. A MediaWorker-only
change must not rebuild frontend assets or restart API/JobWorker. A common
contract change lists all affected consumers and requires their compatibility
checks before selective or coordinated rollout. The initial mixed-version gate
compares the deployed `8b79ea5` release against the modular candidate; there is
no promise that arbitrary older revisions interoperate. Later releases must
record and test their explicitly supported previous release.

Keep `platform.env` as the ignored environment/credential source. Publish a
non-secret per-service release descriptor containing artifact location and frozen
release/catalog/bundle values. Ali's launcher reads the shared environment, then
the selected service descriptor; it must not overwrite shared global version or
PYTHONPATH during a one-service release. Schema/backend/namespace changes are
coordinated environment changes, not hidden service overrides. Keep descriptor
paths and launcher allowlists within the Ali checkout's `.data` tree.

Build and verify first, then atomically switch only that service's descriptor and
restart its existing unit. Verify loaded module paths and manifest digests, not
just process env version strings. Rollback switches that descriptor to the last
validated compatible release. A failed health/startup check rolls back only the
changed service and preserves current work's frozen identities. Systemd unit
names, original-project ports and shared infrastructure remain unchanged.

The existing global release command needs an explicit compatibility migration:
old CLI aliases remain thin launchers for supported operator/test commands;
new per-service build/release commands are authoritative for modular releases.
Remove import shims once repository callers/tests have migrated. Do not retain a
second implementation or a silent fallback to the mutable checkout.

Package data paths use explicit release resources/settings, not `__file__.parents`
assumptions about the old checkout depth. Include profile/job catalogs, Skill
manifest, public verification key, controlled scripts and web assets only where
needed. Preserve the existing external model/environment manifests and scratch
paths; do not duplicate the 38 GiB controlled environment per release.

## Runtime and old executions

Update Runtime imports and the signed Code Bundle builder together. Keep wire
contracts and Bootstrap protocol stable. Verify bundle construction, signature,
activation, CLI discovery and artifacts without the host checkout on sys.path.
Preserve old Code/Content Bundles and provider image/template references for
frozen executions and rollback. Active executions stay on their recorded bundle;
new execution uses the newly selected compatible bundle. Never hot-activate new
code inside an already executing sandbox merely because a host service moved.

Seven historical unknown allocation obligations and the named paused Job remain
owned and unchanged. Current code does not offer arbitrary multi-revision profile
rescheduling; module isolation must not pretend otherwise or opportunistically
change frozen profile identity. Provider selection remains configuration-driven.

## Migration stages and acceptance

1. Freeze the source/service/import inventory, complete module ownership map,
   add failing import/package/entrypoint boundary tests, and define manifests.
2. Extract true shared helpers/contracts, including the portable Runtime subset;
   remove the concrete cross-owner imports identified above. Validate signed
   bundle compatibility and no leaked host dependency/credential paths.
3. Move each service into its owner package in small independently testable
   commits, updating imports, tests, CLI entry points and data-resource lookups.
4. Split Store models/upgrades and coherent transaction groups, preserving the
   public facade and schema. Run isolated populated PostgreSQL upgrades and
   lease/ownership/atomicity/race gates before dependent moves ship.
5. Build isolated service artifacts and install each in a clean environment with
   no repository cwd/PYTHONPATH. Verify process entry, data inclusion, service
   config validation, dependency closure and cross-service import rejection.
6. Introduce per-service release descriptors and tested rollback. Deploy on the
   Ali instance, record existing work first, restart the selected service and
   verify takeover with no extra external execution. User already authorized
   test restarts; do not demand a drain just to avoid testing recovery.
7. Remove migration shims/legacy build paths that remain unneeded; refresh README,
   development guidance, per-service instructions and release evidence. Scan
   and push small commits after verification under existing authorization.

No new paid model tests are necessary just to review this design. During actual
implementation run focused tests per move, full Python/build checks at release,
then affected real cases based on code and packaging reachability. Common contract
or Runtime moves have a broad blast radius: explicitly reassess all 169 product
cases and RC7/AR4/DM4, rather than labeling them unaffected because behavior was
intended to remain the same. Existing unaffected receipts retain their revisions.

Required release proofs include: isolated service imports; ABI/schema identity;
old/new mixed-version service operation; allocation recovery and capacity;
Controlled Operation; MediaWorker handoff; site deployment; API/Gateway/browser;
real Ali and OpenSandbox Runtime/CLI/restore; one-service restart and rollback
while other services' loaded versions stay unchanged. Use the existing suite and
appropriate real gates; add tests for newly exposed package/release boundaries.

The current Seedream endpoint is still closed. Its D02 failure and ten deferred
cases remain external blockers. Passing mocks or module-import tests cannot turn
that gate green; do not silently change the provider model for this migration.

## Completion and protected constraints

- Seven platform services remain seven independent units; inference remains one
  separate provider process. Runtime/Bootstrap are sandbox artifacts, not new
  always-running host service counts. Frontend remains served by API.
- Every product source module has an owner; no service implementation cross-imports,
  no common→service edges, no hidden checkout fallback in packaged deployment.
- Every service can build, start, health-check, upgrade and roll back with a pinned
  common/dependency version. Compatibility constrains simultaneous versions.
- Original project untouched; edits, logs, caches, build artifacts and test data
  stay below `/data2/wujing03`, specifically the Ali checkout for this work.
- Leases, fences, same-execution recovery, cleanup-before-capacity-release,
  short-lived sandbox capabilities and output validation remain unchanged.
- Preserve all failed/affected case evidence and current environment ownership.
  Report actual test scope, provider, loaded versions and remaining blockers.

## Complete current-module assignment

This table assigns all 76 current top-level host modules exactly once. Split rows
name both required owners; new pure helpers are extracted before implementation
modules move. `sandbox.py` remains a provider compatibility/fake adapter facade
in common/sandbox during migration, with fake-only implementation excluded from
production artifacts if no production import requires it.

| Owner | Existing module stems |
| --- | --- |
| API | `main`, `app`, `broker`, `sandbox_inspector`, `document_preview`, `execution_graph`, `job_continuation`, `auth`, `sessions`, `model_discovery` |
| Gateway | `gateway` |
| JobWorker | `job_service`, `harness_job`, `media_render_adapter`, `media_job_adapter`, `image_decompose_adapter` |
| AllocationWorker | `allocation_service` |
| MediaWorker | `media_service`, `media_worker`, `image_decompose_provider`, `media_reference_publisher`, `ark_assets`, `minimax_voice` |
| ControlledWorker | `controlled_service`, `controlled_media_adapter`, `controlled_media_runner`, `sam3_job`, `transcribe_job` |
| SiteWorker | `site_service`, `site_worker` |
| common/contracts | `domain`, `job_contracts`, `sandbox_contracts`, `job_errors`, `media_provider`, `errors` |
| common/execution | `job_worker`, `job_adapters` |
| common/jobs | `job_coordinator` |
| common/sandbox | `sandbox`, `sandbox_factory`, `sandbox_aliyun`, `sandbox_opensandbox`, `pool`, `runtime`, `job_allocation` |
| common/catalog | `catalog`, `profiles`, `job_catalog`, `media_catalog`, `execution_readiness`, `provider_preflight`, `media_references` |
| common/models | `model_routes`, `model_health`, `model_activity` |
| common/configuration | `config` |
| common/db | `store` |
| common/scheduling | `scheduler`, `node_agent` |
| common/storage | `objects` |
| common/artifacts | `artifact_gateway` |
| common/security | `capability`, `grants` |
| common/observability | `observability`, `telemetry_ingest` |
| common/events | `events`, `activity_projection` |
| common/sites | `site_bundle` |
| tools | `auth_cli`, `log_query` |
| split: Gateway + common/security | `public_proxy` |
| split: JobWorker + common/jobs | `reviewer` |
| split: AllocationWorker + common/sandbox | `allocation_dispatch` |
| split: MediaWorker + common/contracts | `media_errors` |
| namespace initializer | `__init__` |
