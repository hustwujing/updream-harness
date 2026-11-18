# sandbox-harness-platform-parallel

A thin, stateful OpenSandbox platform that sends every user message to a Codex, Claude or DeepSeek Harness. The platform restores the Harness workspace, streams its public output and steps, exports files, checkpoints the environment and removes the sandbox. It contains no DAG, planner or reviewer; media generation and static-site deployment use durable Jobs with independent Workers.

## Services

```text
Web → Session API → Leased Turn Worker → Warm Sandbox Pool → OpenSandbox/gVisor
                                      │                     │
                                      └──── Capability Gateway ◄── Harness

Media confirmation → Session API → PostgreSQL Job → Media Worker → Provider
                                      ├── reference → public BOSS → Ark Asset
                                      └── terminal Outbox → Broker continuation

Site confirmation  → Session API → PostgreSQL Job → Site Worker → Object storage
                                                        └── immutable version → public route

PostgreSQL: Session, Turn, event and artifact metadata
Redis: live event fan-out and short-lived capability revocation
Object storage: content-addressed checkpoints and artifacts
```

The parallel project uses a dedicated PostgreSQL database on `127.0.0.1:28432` and reuses only
shared executables and infrastructure daemons. PostgreSQL data, service logs, caches, browser
dependencies, test temporaries, and all other writable state stay under
`/data2/wujing03/sandbox-harness-platform-parallel/.data`. Database identity, Redis key prefix,
image repository, OpenSandbox namespace, object prefix, ports, service units, and Nginx ingress are
isolated from both predecessor deployments.

The Capability Gateway is an infrastructure proxy. It injects model and Provider credentials after validating a Turn-scoped token. It does not understand prompts, plans or media quality.

The Media Worker is a separate systemd service. API creates and queries Jobs;
the Worker alone submits external Provider requests, persists external IDs,
resumes polling after crashes and commits the Artifact, terminal state and
Outbox transaction. API and Worker can restart independently.

The Site Worker is another independent service. A Harness packages a complete
static site with `sandbox-site prepare`; user confirmation creates a durable
deployment Job. The Worker validates and uploads content-addressed files, then
atomically activates an immutable version. Published pages survive Sandbox,
API, Worker and browser restarts and support rollback and downline.

Deployment remains discoverable if a Harness omits the CLI call: after a
successful Turn, Broker detects each exported directory containing
`index.html`, builds the same content-addressed draft and deduplicates it by
bundle digest. This is still only a confirmation draft; no public version is
activated until the user chooses **部署站点**. The HTML Artifact opens as an
interactive owner-scoped preview in a CSP-sandboxed iframe instead of source
text. Published versions use `/sandbox-harness-platform-parallel/sites/<slug>/`, and Workspace
provides the stable link, immutable history, rollback and downline controls.

Seedream supports immutable single/multi-image references through stable public
BOSS URLs. Seedance supports first frame, first/last frame, multimodal reference,
video editing and extension; image/video/audio inputs are registered as Ark
Assets and submitted only after they become `Active`. Project files, BOSS
publication and Ark Asset IDs are cached by content digest and survive Worker
restart without duplicate external submissions.

Tripo v3 supports confirmed text-to-3D, image-to-3D, multiview-to-3D, texture,
decimation and conversion Jobs. The Worker persists the Tripo task ID before
polling, resumes that task after restart and atomically publishes the model and
preview. GLB files open in an interactive Three.js preview; every output remains
available for download and later Harness work.

Direct browser access keeps the existing signed anonymous identity. The FRPC
entry is identified through PROXY Protocol/nginx and requires an account; an
account Cookie resolves to the same stable owner from either entry. Users can
change username/password from the UI, while account creation/reset/disable uses
the `sandbox-harness-user` host CLI.

## Turn Lifecycle

```text
QUEUED → RESTORING → STARTING → RUNNING
                                ├─→ WAITING_USER result
                                ├─→ CHECKPOINTING → COMPLETED result
                                ├─→ FAILED result
                                └─→ CANCELLED result
result → CLEANING → terminal state
```

Only one Turn may run for a Thread. A used sandbox may stay attached to that
same Thread for a short TTL after its workspace checkpoint commits; after
expiry, the sandbox is deleted and its capacity is released.

Turn ownership is a renewable PostgreSQL lease. The Sandbox Runtime consumes Harness stdout in a background task and keeps a sequenced replay journal independently of the Broker HTTP connection. A replacement Broker can reattach the same namespaced Sandbox, replay only missing Runtime events, checkpoint the result and finish cleanup. If the Runtime or Sandbox itself is lost after Harness execution began, the broker reports an interrupted Turn instead of replaying potentially non-idempotent public side effects.

All three native Harnesses publish public assistant output through the same
durable `text_delta` event contract. Codex maps app-server agent-message deltas,
Claude maps stream-json text deltas, and DeepSeek maps committed SDK
`assistant/message` notifications. A terminal full response is retained only as
a fallback snapshot and is suppressed when that input already streamed text;
the deduplication boundary resets for every appended input.

Harnesses can request user decisions with `sandbox-interact`; Codex native user-input and approval requests map to the same durable Interaction protocol. Long commands can be delegated to `sandbox-background`, whose process group, bounded log, cancellation and exit status are supervised by the Runtime and remain part of the Turn result.

Every Turn receives one pinned 32-Skill bundle shared by Codex, Claude and
DeepSeek. It includes diagram, documentation, editable/image-based PowerPoint,
four focused Superpowers workflows, three data-quality/SQL workflows and the
complete pinned `anthropics/skills` collection with declared runtime
dependencies. See [Runtime Skill Bundle](docs/skills.md).

## Workspace

```text
/workspace/project   persistent user workspace
/workspace/inbox     current message and attachments
/workspace/outputs   exported artifacts
/workspace/state     native Harness and Provider state
/workspace/tmp       disposable Turn data
/run/harness         ephemeral token and runtime configuration
```

Runtime UID/GID remain fixed at `10001`; the passwd username is configurable at
image build time with `SANDBOX_HARNESS_SANDBOX_USERNAME` and defaults to
`updream`. Changing the display name does not change checkpoint ownership.

Uploaded Turn attachments are versioned as managed project assets and exported outputs are versioned as managed project artifacts. Their immutable objects remain in object storage and are materialized under `/workspace/project/assets` and `/workspace/project/artifacts` after every checkpoint restore. They therefore survive a new Sandbox or a different OpenSandbox worker without retaining historical `inbox` or `outputs` directories.

Each Turn freezes an immutable ProjectFile snapshot before materialization. A
replacement uploaded while that Turn runs is available to the next Turn but
cannot change the current execution's inputs. Object size and SHA-256 are
verified before every materialization. `/workspace/outputs` carries a hidden
Turn batch identity; the Broker rejects stale output from another Turn.

The Workspace panel supports direct project uploads, rename, deletion, version
history, text-version comparison and restore-as-new-version. Deletion creates an
auditable tombstone; each Turn resets managed directories before materializing
its frozen snapshot, so stale files cannot survive in an attached Sandbox.

The Sandbox workbench adds owner-scoped live overview, files, processes,
metrics, Execd PTY terminal, private port preview and audit history. Inspection
does not expose OpenSandbox credentials. Writable terminal and file editing use
an exclusive takeover state; release checkpoints the workspace and revokes its
short-lived network capability before Agent execution is re-enabled.
If terminal work changed `project/assets` or `project/artifacts`, release
requires an explicit choice: discard and rematerialize, preserve the changed
files under `project/work`, or register them as new managed versions.

The browser keeps the selected Thread in `?thread=<id>`, restores it after refresh and supports history navigation. Users may delete terminal Threads; deletion first removes any retained Session Sandbox, then removes the owner's conversation records. Images, videos, text files and interactive HTML sites open in ownership-checked previews, while every file retains an explicit download action. A Thread with an active published site must first downline that site before deletion.

## Development

```bash
UV_CACHE_DIR=.data/uv-cache uv sync --extra test --extra runtime-media
.venv/bin/pytest
.venv/bin/uvicorn sandbox_harness.app:create_app --factory --reload --port 24766
```

Set `SANDBOX_HARNESS_BACKEND=fake` only for local UI development and tests. Production accepts only `opensandbox` and never falls back to a host Harness.

Before changing code, read [AGENTS.md](AGENTS.md) and use the
[development guide index](docs/development/README.md) to load the rules for the
affected lifecycle, Gateway, Runtime, image, Skill, API or deployment boundary.

See [architecture.md](docs/architecture.md), [client API](docs/api.md), [data model](docs/data-model.md), [product interface](docs/product-interface.md), [gateway.md](docs/gateway.md), [Runtime Skill Bundle](docs/skills.md) and [deployment.md](docs/deployment.md).

Real deployment evidence for code execution, media processing, autonomous repair, checkpoint recovery, discovered models and browser isolation is recorded in [first-capability-report.md](docs/test-evidence/first-capability-report.md).

Real Chromium, Canvas, Three.js, deterministic animation recording and native multi-input evidence is recorded in [browser-rendering-report.md](docs/test-evidence/browser-rendering-report.md).

The 25-Skill image matrix, real image/video/audio generation and API, Worker,
Sandbox and reconnect recovery evidence are recorded in
[media-generation-and-skill-report.md](docs/test-evidence/media-generation-and-skill-report.md).

Tripo 3D generation, multi-output publication, Three.js preview and non-graceful
Media Worker recovery evidence are recorded in
[tripo-3d-report.md](docs/test-evidence/tripo-3d-report.md).

Non-graceful API restart, Runtime event replay, Turn lease fencing and cleanup recovery evidence is recorded in [broker-recovery-report.md](docs/test-evidence/broker-recovery-report.md).

Versioned uploads and outputs restored into a different Sandbox, plus desktop/mobile Workspace UI evidence, are recorded in [project-files-report.md](docs/test-evidence/project-files-report.md).

The final 21-case capability, interaction, Workspace, background execution, recovery and browser matrix is recorded in [full-product-report.md](docs/test-evidence/full-product-report.md).

## Runtime Toolchain

Every Harness image inherits the same immutable foundation with Python, Node.js, ffmpeg/ffprobe, Chromium, Playwright with its pinned Chromium browser, ImageMagick, Three.js, Git, curl, jq, ripgrep, file inspection, GCC/G++, Make, CMake, pkg-config, archive tools and CJK fonts. The common Runtime layer additionally carries NumPy, Pillow, OpenCV, Beautiful Soup, psutil, SciPy, scikit-image, librosa, SoundFile, Matplotlib, MoviePy, PyAV, ImageIO, CairoSVG, PSD Tools, rembg/ONNX Runtime, Trimesh and related media libraries, plus the workspace-scoped `apply_patch` helper. `runtime/media_probe.py` imports the declared modules during every image build. [toolchain.lock.json](runtime/toolchain.lock.json) records the observed foundation command and package versions; image builds fail when the foundation no longer matches it. The deployed Harness images are referenced by registry digest rather than mutable tag.

Runtime Python and CLI ship as a signed Code Bundle; instructions and Skills
ship as a signed Content Bundle. A minimal image-resident Bootstrap verifies,
activates and supervises the Runtime child. A Turn freezes image and both
Bundle digests, while dependency, Bootstrap and native Harness changes still
use immutable image releases.

## Public Network Policy

Sandboxes have no direct public route. The Runtime injects a short-lived Turn capability into standard `HTTP_PROXY` and `HTTPS_PROXY` variables, so libraries such as Requests use the authenticated Gateway automatically. `sandbox-browser-screenshot URL OUTPUT` uses the same Gateway for the page, redirects and subresources; raw Chromium cannot bypass it.

The Gateway permits only public addresses on ports 80 and 443, rejects private, loopback, link-local, reserved and cloud-metadata addresses, and applies configurable destination rules after DNS resolution:

```dotenv
SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST='["bilibili.com","*.bilibili.com"]'
SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST='[]'
```

Rules may be exact domains, wildcard subdomains, individual IPs or CIDRs. The
allowlist restricts anonymous Turns. Turn submissions from any authenticated
account may reach every otherwise-valid public HTTP/S destination; the
denylist, private-address rejection, 80/443 port limit, request budget and
capability revocation still apply. curl, Requests, Chromium and Playwright use
the same authenticated proxy. See [gateway.md](docs/gateway.md) for the complete
contract.

Runtime boundary handling: execution creation and stdin submission are serialized. An input ID is bound to its original body; a replay returns the existing execution and conflicting content is rejected before attachment metadata changes. Streaming Harnesses must acknowledge their pending inputs: EOF alone is not success. Protocol failures and cancellation quiesce the Harness process group and supervised background jobs before publishing a terminal result; jobs ignoring SIGTERM are killed after a bounded grace period.

Stop requests are persisted on the Turn. The lease owner observes them through its heartbeat, and a replacement Broker honors them without replaying the Harness. Stop and final completion are serialized in the database; a stop accepted before completion prevents a later success from overriding it. The UI displays “正在停止” and disables further input while cleanup finishes. Targeted regression cases are in `tests/test_runtime_boundaries.py` and `tests/test_cancel_boundaries.py`.

Native interaction IDs exposed outside the sandbox are namespaced by execution; late answers cannot reach a later Harness process that reused the same native ID. Ending an execution also releases unanswered helper requests. Model discovery retains the last inventory if the upstream listing endpoint fails and still probes explicitly configured model routes. Redis connection and command stalls are bounded, so event persistence can fall back to database replay instead of waiting indefinitely.

The sidebar green dot means a completed Turn has not been viewed. Once the completed transcript is loaded in the focused, visible conversation, the client acknowledges that exact Turn through `POST /api/threads/{thread_id}/read`. The owner-scoped acknowledgement is stored on the Thread, survives reloads, and cannot pre-read a running Turn or acknowledge a newer completion through a stale request. Running Turns retain their spinner.

Structured lifecycle logs and OpenTelemetry traces cover API, Broker, Gateway, storage and Runtime. Logs are stored under `.data/logs` with bounded queues, redaction and retention; Runtime records are exported to the existing Gateway and collected again before deletion. Use `.venv/bin/sandbox-harness-logs --turn TURN_ID` to assemble a timeline. See [logging, IDs, retention, export configuration and troubleshooting](docs/observability.md).
