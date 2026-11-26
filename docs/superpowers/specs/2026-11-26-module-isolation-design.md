# Service modules and common libraries

Status: approved for implementation by the user after the completeness inventory
("列完之后就动手吧"). Subsequent user steering requires a fresh complete real-case
gate after implementation. Earlier design commits changed documentation only.

This revision incorporates the user's explicit `sandbox-gateway` naming and
full deployment-location isolation requirements. The independent builder check
was repeated for the revised design with no material unanswered questions.

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

The user explicitly selected the name `sandbox-gateway` and required code that
runs inside sandboxes or other execution environments to be independently owned
and released. The remaining implementation choices below are the author's
recommendations now covered by the user's instruction to proceed.
The existing native implementation workflow is suitable; focused independent reviews follow transaction and deployment stages.

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
  address validation from sandbox-gateway's proxy implementation.
- API and sandbox-gateway legitimately share Job preflight/coordinator and review
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
      sandbox_gateway/      # host capability routes and sandbox egress proxy
      job_worker/           # sandbox Child/Reviewer/Operation adapters
      allocation_worker/    # provider-create and reconciliation runner
      media_worker/         # external media lifecycle and provider transports
      controlled_worker/    # host adapters and bwrap launcher; programs are separate
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
  controlled_runtime/       # fixed programs launched inside bwrap, no host service imports
modules/                     # explicit build/release manifests, grouped by location
  host/
    api/module.json
    sandbox-gateway/module.json
    job-worker/module.json
    allocation-worker/module.json
    media-worker/module.json
    controlled-worker/module.json
    site-worker/module.json
  sandbox/
    runtime/module.json
    bootstrap/module.json
    content/module.json
    images/module.json
  execution/
    controlled-runtime/module.json
    image-inference/module.json
  browser/
    web-ui/module.json
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

Each `module.json` defines identity, execution location, artifact type, applicable
console/module entry, allowed source files, common dependencies, third-party dependencies, non-secret required config names,
packaged data, runtime assets, native OS/permission requirements, external build
inputs, compatible consumers and verification commands.
A static bundle/image manifest has no invented daemon entry point. Inference's manifest
pins the existing materialized implementation/environment; do not copy model
weights into application wheels or import source from predecessor projects.

## Complete deployment-location inventory

The inventory contains **14 independently owned build/release modules**, plus
common libraries. It still represents **eight long-running host application
services** (seven platform services and inference), plus the database dependency.
Bundles, images, browser assets and per-sandbox/per-Job processes are deployment
artifacts, not additional always-running host services.

| Module ID | Runs or is consumed where | Source and deployment artifact | Activation / version boundary |
| --- | --- | --- | --- |
| `api` | Host | API/Broker package and pinned web-ui artifact | API unit's immutable release descriptor |
| `sandbox-gateway` | Host, accessed by sandboxes | Capability/streaming/egress package | Dedicated host unit; provider route selected by config |
| `job-worker` | Host | Sandbox Job adapter package | JobWorker descriptor and frozen common/contracts |
| `allocation-worker` | Host | Provider-create/reconciliation package | AllocationWorker descriptor |
| `media-worker` | Host | Media lifecycle and HTTP Provider adapters | MediaWorker descriptor; inference is an external endpoint |
| `controlled-worker` | Host | Job engine integration, bwrap launcher, resource/lease management | Worker descriptor pins the controlled-runtime execution manifest |
| `site-worker` | Host | Site publication lifecycle package | SiteWorker descriptor |
| `sandbox-runtime` | Inside each Ali/OpenSandbox instance | `src/sandbox_runtime`, Harness bridges, Runtime CLIs, portable common closure; signed Code Bundle | Bootstrap verifies and activates a digest; running executions remain on their frozen code |
| `sandbox-bootstrap` | Inside each sandbox, image-owned | `src/sandbox_bootstrap`, entrypoint, signature key, CLI dispatcher; Bootstrap image layer | Image/template rollout; compatibility with Code Bundles verified first |
| `sandbox-content` | Materialized inside sandboxes and consumed by Runtime/Harnesses | Skills, prompts, instruction assets and Skill metadata; signed Content Bundle | Content digest frozen per execution independently of host release |
| `sandbox-images` | Provider registry/templates; layers execute inside sandboxes | Foundation/common/profile/Harness Dockerfiles, native helpers, OS/Python/Node dependencies and Codex/Claude/DeepSeek client pins | Immutable image digests, Ali template revisions and complete signed profile catalog |
| `controlled-runtime` | Per-Job bwrap namespace on host | Fixed SAM3/ZIM/transcribe programs plus references to existing immutable model/interpreter snapshots | Separate program/runtime manifest pinned by Worker; no importing worker modules inside bwrap |
| `image-inference` | Separate model-serving process | Existing materialized image-layering implementation, dependency/model manifest and integration launcher | Independent provider release and health check; MediaWorker uses HTTP |
| `web-ui` | JavaScript in browser; assets served by API | Existing `web/` sources and deterministic static build | Versioned artifact pinned by API; changing other workers does not rebuild it |

PostgreSQL, Redis, registry, ingress/network setup and upstream sandbox/model
providers are infrastructure or external dependencies, not new application
modules. Shared infrastructure is not redeployed as part of module isolation.
Build scripts, administrative CLIs and test runners have explicit tooling
ownership and consume the same module manifests; they do not become services.

### Sandbox-owned code and content

Runtime and Bootstrap remain different source and release boundaries. Python
Harness bridges and dynamic Runtime CLIs belong to sandbox-runtime. Native
Codex/Claude clients, DeepSeek SDK, browser/proxy/drawio helpers and other
image-installed programs belong to the declared sandbox-image layer/profile.
The image's CLI dispatcher is Bootstrap-owned and resolves the activated Code
Bundle; it does not embed a second mutable Runtime implementation.

Skills may contain executable tools. Their manifest must identify which files
are content-delivered scripts and which are Code Bundle payloads. Today `editppt`
is copied into the Code Bundle from its Skill source and is also present in the
Skill tree in the Content Bundle. Preserve one canonical source and explicitly
record both consumers/digests during migration; do not silently drop either copy
or permit divergent implementations. A change to that shared source invalidates
both affected artifacts, while unrelated content changes need no image rebuild.
Instruction-generation functions/assets currently imported by the content builder
from Runtime must also have declared build dependencies; content builds must
not boot a Runtime server or require a host service installation.

A sandbox-image release pins Bootstrap/native dependency/Harness-client versions;
a Runtime release pins its portable common closure and required image ABI/lock;
a Content release pins its Skills and compatibility requirements. A release set
records and verifies this tuple instead of forcing all artifacts to share one
version or assuming any independently built versions can be combined.

### Controlled execution and model serving

ControlledWorker stays responsible for database leases, resource ownership,
launch, cancellation and result validation. Its executed fixed programs become
a separate `controlled-runtime` module, with entry-point paths, input/result
schema versions and content hashes. Resolve programs from that immutable
manifest and copy only the declared program into the Job's isolated work area.
Keep the current bwrap mounts, network isolation, GPU ownership and output
validation behavior.

The host's CPython 3.13 requirement does not impose that ABI on the existing
SAM3/ZIM/Whisper or inference environments. Their manifests retain the actual
interpreter/model versions and executable paths. Large model/environment
snapshots remain referenced and shared read-only under the established ownership
rules; service packaging must not duplicate them or introduce writes into their
external source trees. Existing materialized inference code must be versioned
from its current managed snapshot; do not re-import predecessor application
source or fetch unpinned upstream code to fill the new module directory.

Tests must launch controlled programs using only the declared program/runtime
artifacts, without the host checkout or `sandbox_harness.services` on sys.path.
Inference is independently smoke-tested through its HTTP contract, not imported
as a Python implementation by MediaWorker.

### Portable common versus host-only common

Common has one source tree but several declared packaging closures. Runtime's
allowlist includes only pure contracts, needed catalog/site helpers and bounded
telemetry. Host-only DB, sandbox-provider SDKs, credentials, scheduling and
service-startup configuration remain outside every sandbox bundle and image.
Controlled fixed programs keep their existing minimal imports and receive only
the specific portable helpers they actually need, compatible with their own
interpreter. They do not install the host common distribution wholesale.

## Completeness audit and non-obvious dependencies

The follow-up audit checked console entry points, subprocess launch sites,
service lifespans, image COPY/build inputs and browser worker loading. The 14
module definitions cover the inspected implementation; no fifteenth product
module or additional host service was identified. This is a source/design audit,
not proof that the future isolated packages already work.

A complete proposed ownership inventory covers **1,344 tracked deployment-input
files** under `src`, `runtime`, `web`, `deploy`, `scripts` and workspace build
metadata, with no unassigned paths. Test/documentation trees are outside that
count except colocated web tests. The machine-readable inventory is
`.data/evidence/module-isolation/full-source-coverage.json`. Ignored vendor files,
materialized inference code, model environments and provider images require the
external-input manifests below; tracked-file coverage cannot certify those bytes.

| Execution or input easy to overlook | Explicit owner and publication rule |
| --- | --- |
| API's Broker, inspector, Job continuation, Media outbox, draft expiry, catalog refresh and warm-pool loops | `api`; its lifespan starts/stops these tasks and their lease/recovery behavior is included in API upgrade/rollback checks |
| Document conversion subprocess | `api`; `document_preview.py` and its LibreOffice install, config/resources, writable cache and IPC requirements are packaged or declared together |
| Sandbox `media.render`, `audio.process`, `media.concat_sequence` implementation | `sandbox-runtime`; preserve request/response/receipt compatibility with JobWorker's host adapter, including execution and cleanup; FFmpeg/ffprobe binaries are `sandbox-images` inputs |
| `background_supervisor` and PTY/Harness child processes | `sandbox-runtime`; same signed Code Bundle, manifest entry paths and bounded shutdown/recovery checks; no extra host service |
| Browser rendering, proxy bridge, Playwright patch, drawio and sandbox ASR helpers | `sandbox-images` with profile-specific native dependencies; sandbox ASR CLI and bwrap transcription program remain different owned entry points |
| Browser PDF worker and lazy-loaded preview assets | `web-ui`; Vite output must include the PDF worker resolved by `DocumentPreview.jsx`, and production preview tests use packaged assets |
| Registry publication, template publication, signature generation, catalog generation and module release/rollback commands | build/release tooling under `tools/build` and `tools/release`; signing/provider credentials stay on the host, outside artifacts |
| Database initialization/upgrades and operational CLI commands | migration implementation in `common/db`; operator entry points in `tools/admin`; preserve idempotent SQLite/PostgreSQL upgrades and owner isolation |
| Nginx/network/systemd/database setup and health/status scripts | infrastructure/release tooling with declared target environment; no new application-service count |
| Test runners, fixtures, fault injection and evidence generation | `tools/test` and module-linked tests; excluded from production artifacts except explicitly declared shipped image probes |

### Host-native requirements

Python import/installation success is not a sufficient service startup gate.
Host manifests also declare executable paths and compatible versions/capabilities,
OS resources, execution identity, device/driver access, directory permissions,
and writable data/cache locations. Required capabilities are checked by the
service's own gate, without granting additional privilege merely to make a
refactored package start.

In particular, API declares `/usr/bin/libreoffice` and its conversion/IPC
requirements and must actually convert a fixture. ControlledWorker declares
`/usr/bin/bwrap`, `setpriv`, `ffprobe`, `nvidia-smi`, the system/helper interpreter
and relevant device/driver capabilities; its isolated execution proof checks the
actual runtime manifest and GPU ownership when the operation requires GPU.
The exact paths/version constraints come from the validated deployment, not
from whatever executable happens to be first on a release builder's PATH.

### External and non-tracked build inputs

`sandbox-images` must declare more than Dockerfiles. The present ASR overlay
copies Whisper weights and smoke audio from an existing `ASR_SOURCE_IMAGE`.
Codex/Claude layers copy pinned native clients from source images, and Draw.io
uses an external vendor binary. Record immutable source image digests/content
identities, binary/model hashes, verification probes, provenance/acquisition
location and retention/reference ownership. Resolve a mutable source tag before
building; a tag alone is not a reproducible release input.

Existing mounted model/interpreter snapshots and inference implementation have
the same explicit-reference requirement. Reuse validated local/registry artifacts
without silently pulling a newer version or copying another project's mutable
application tree. Keep current, rollback, active and unresolved-frozen references
before deleting old images or source artifacts. Evidence for a new clean build
must verify the full declared input closure even when caches were absent.

These are publication requirements of existing modules and tooling, so the
product-module count remains 14 and the host application-service count remains 8.

## sandbox-gateway naming and migration

The user-selected module/service name is `sandbox-gateway`. Its Python package
is `sandbox_harness.services.sandbox_gateway` (Python identifier uses underscore),
its manifest is `modules/host/sandbox-gateway/module.json`, and its canonical
entry point is `sandbox-harness-parallel-sandbox-gateway`. It runs on the host;
"sandbox" describes the clients and controlled access boundary.

For Ali deployment, migrate the owned unit prefix from
`sandbox-harness-parallel-ali-gateway-{aliyun,opensandbox}` to
`sandbox-harness-parallel-ali-sandbox-gateway-{aliyun,opensandbox}`. These are
config-selected listener variants of one service role, not two modules. Preserve
ports, origin URLs, capability routes, protocol payloads and namespace identities.
Update launch/start/stop/status scripts, release selectors, observability service
labels, test controls and network-rule ownership checks consistently.

During the controlled rename, record the old unit/version, stop only that owned
listener and start its renamed replacement on the same ports. Verify health,
sandbox model/capability access and terminal proxy/recovery; rollback starts the
old verified unit if the replacement fails. Never leave both old/new units
competing for the port or rename another project's service. Keep old CLI/unit
names in an explicit migration lookup for status/cleanup and rollback only;
new releases use the new canonical identity. Historical logs keep their original
service names, and no durable Job/Turn identity is rewritten to match a display
name. Ports and provider route configuration are not inferred from the new name.

## Source ownership and extractions

Preserve recognizable filenames within their new owner to keep history and
review readable. The complete 76-module assignment is checked in the appendix below; the
following fixes the ownership and extraction decisions.

| Owner | Existing implementation / required extraction |
| --- | --- |
| API | `main`, `app`, `broker`, `sandbox_inspector`, `document_preview`, `execution_graph`, `job_continuation`, `auth`, `sessions`, `model_discovery` |
| sandbox-gateway | `gateway`, proxy listener/CONNECT logic from `public_proxy`; network validation primitives move to common/security |
| JobWorker | `job_service`, `harness_job`, `media_render_adapter`, `media_job_adapter`, `image_decompose_adapter`, execution adapter part of `reviewer` |
| AllocationWorker | `allocation_service`, worker/service half of `allocation_dispatch` |
| MediaWorker | `media_service`, `media_worker`, `image_decompose_provider`, `media_reference_publisher`, `ark_assets`, `minimax_voice`, provider-specific response classifier from `media_errors` |
| ControlledWorker | `controlled_service`, `controlled_media_adapter`, host launch/validation logic from `controlled_media_runner`; consumes the pinned controlled-runtime manifest |
| Controlled Runtime | `sam3_job`, `transcribe_job` move out of the host service package; fixed execution entry points and model/environment compatibility belong to `src/controlled_runtime` and its execution manifest |
| SiteWorker | `site_service`, `site_worker` |
| Common contracts | `domain`, `job_contracts`, `sandbox_contracts`, `job_errors`, stable media-error guidance and pure receipts/protocols from `media_provider` |
| Common execution / jobs | `job_worker`, `job_adapters`, `job_coordinator`, shared review coordinator/types; transactional media handoff interfaces if extracted from adapters |
| Common sandbox | `sandbox_factory`, provider adapters, `pool`, `runtime`, `job_allocation`, client and request/view contracts from `allocation_dispatch` |
| Common catalog / models | `catalog`, `profiles`, `job_catalog`, `media_catalog`, `execution_readiness`, `provider_preflight`, `media_references`, `model_routes`, `model_health`, `model_activity` |
| Common infrastructure | `scheduler`, `node_agent`, `objects`, `artifact_gateway`, `capability`, `grants`, `events`, `activity_projection`, `observability`, `telemetry_ingest`, `site_bundle`, `config`, generic `errors` |
| Tools | `auth_cli`, `log_query` with exact required common/API authentication dependency; extract shared authentication primitives if needed instead of importing API startup |

`staged_file_chunks` moves to common/storage; process identity to common/process;
public address normalization/validation to common/security. These extractions
remove the demonstrated Controlled→Harness and Media→Runtime/sandbox-gateway imports
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
changed service and preserves current work's frozen identities. The
sandbox-gateway rename has the explicit unit migration described above;
other systemd unit names, original-project ports and shared infrastructure remain
unchanged.

The existing global release command needs an explicit compatibility migration:
old CLI aliases remain thin launchers for supported operator/test commands;
new per-service build/release commands are authoritative for modular releases.
Remove import shims once repository callers/tests have migrated. Do not retain a
second implementation or a silent fallback to the mutable checkout.

Package data paths use explicit release resources/settings, not `__file__.parents`
assumptions about the old checkout depth. Include profile/job catalogs, Skill
manifest, public verification key, controlled-runtime manifest references and web assets
only where needed. ControlledWorker resolves fixed programs through the pinned
execution manifest instead of `Path(__file__).with_name(...)`; script copying
into each owned bwrap work directory uses those declared immutable files.
Preserve the existing external model/environment manifests and scratch paths; do not duplicate the 38 GiB controlled environment per release.

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

1. Freeze the full host/sandbox/execution/browser inventory, complete source and
   build-input ownership maps, add failing import/package/entrypoint boundary
   tests, and define all 14 module manifests, including sandbox-gateway naming.
2. Extract true shared helpers/contracts, including the portable Runtime subset;
   remove the concrete cross-owner imports identified above. Validate signed
   bundle compatibility and no leaked host dependency/credential paths.
3. Move each service into its owner package in small independently testable
   commits. Separate fixed controlled-runtime programs from host launchers;
   update imports, tests, CLI entry points, manifest-based program lookup and
   data-resource lookups. Include sandbox Runtime/Bootstrap/content/image inputs
   and web/inference modules in the ownership and independent-build gates.
4. Split Store models/upgrades and coherent transaction groups, preserving the
   public facade and schema. Run isolated populated PostgreSQL upgrades and
   lease/ownership/atomicity/race gates before dependent moves ship.
5. Build isolated service artifacts and install each in a clean environment with
   no repository cwd/PYTHONPATH. Verify process entry, data inclusion, service
   config validation, Python/native/external-input closure, browser worker assets
   and cross-service import rejection. Real conversion/controlled execution
   supplements import checks; image builds verify pinned non-tracked inputs.
6. Introduce per-service release descriptors, artifact compatibility selection,
   sandbox-gateway unit migration and tested rollback. Deploy on the
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
intended to remain the same. Existing unaffected receipts retain their revisions as history. The subsequent
explicit user requirement is stronger for final acceptance: run all 169 product
plus RC7/AR4/DM4 real cases anew on the final implementation release; historical
successes do not satisfy this fresh gate.

Required release proofs include: isolated service imports; ABI/schema identity;
old/new mixed-version service operation; allocation recovery and capacity;
Controlled Operation; MediaWorker handoff; site deployment; API/sandbox-gateway/browser;
real Ali and OpenSandbox Runtime/Bootstrap/CLI/content/restore; isolated
controlled-runtime program launch; independently selected web assets; old/new
sandbox-gateway identity migration; one-service restart and rollback
while other services' loaded versions stay unchanged. Use the existing suite and
appropriate real gates; add tests for newly exposed package/release boundaries.

The current Seedream endpoint is still closed. Its D02 failure and ten deferred
cases remain external blockers. Passing mocks or module-import tests cannot turn
that gate green; do not silently change the provider model for this migration.

## Completion and protected constraints

- Fourteen deployable module definitions cover host services, sandbox code/content/
  images, controlled execution, inference and browser assets. Seven platform
  services remain seven independent units; inference remains one separate provider
  process. Artifact modules do not inflate the host service count. Frontend remains
  served by API and sandbox-gateway remains a host service.
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
| sandbox-gateway | `gateway` |
| JobWorker | `job_service`, `harness_job`, `media_render_adapter`, `media_job_adapter`, `image_decompose_adapter` |
| AllocationWorker | `allocation_service` |
| MediaWorker | `media_service`, `media_worker`, `image_decompose_provider`, `media_reference_publisher`, `ark_assets`, `minimax_voice` |
| ControlledWorker | `controlled_service`, `controlled_media_adapter`, `controlled_media_runner` |
| Controlled Runtime | `sam3_job`, `transcribe_job` |
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
| split: sandbox-gateway + common/security | `public_proxy` |
| split: JobWorker + common/jobs | `reviewer` |
| split: AllocationWorker + common/sandbox | `allocation_dispatch` |
| split: MediaWorker + common/contracts | `media_errors` |
| namespace initializer | `__init__` |
