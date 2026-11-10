# updream-harness

A thin, stateful OpenSandbox platform that sends every user message to a Codex, Claude or DeepSeek Harness. The platform restores the Harness workspace, streams its public output and steps, exports files, checkpoints the environment and removes the sandbox. It contains no DAG, planner, reviewer or media workflow engine.

## Services

```text
Web → Session API → Leased Turn Worker → Warm Sandbox Pool → OpenSandbox/gVisor
                                      │                     │
                                      └──── Capability Gateway ◄── Harness

PostgreSQL: Session, Turn, event and artifact metadata
Redis: live event fan-out and short-lived capability revocation
Object storage: content-addressed checkpoints and artifacts
```

Production runs a dedicated PostgreSQL cluster on `127.0.0.1:25432` and reuses the host Redis, Docker/runsc, OpenSandbox and BOSS/S3 services. PostgreSQL data, service logs, caches, browser dependencies, test temporaries and other local writable state stay under `/data2/wujing03/sandbox-harness-platform/.data`; the root filesystem is not used for project-generated data. Shared services remain isolated by Redis key prefix, image repository, OpenSandbox metadata namespace and object prefix.

The Capability Gateway is an infrastructure proxy. It injects model and Provider credentials after validating a Turn-scoped token. It does not understand prompts, plans or media quality.

## Turn Lifecycle

```text
QUEUED → RESTORING → STARTING → RUNNING
                                ├─→ WAITING_USER result
                                ├─→ CHECKPOINTING → COMPLETED result
                                ├─→ FAILED result
                                └─→ CANCELLED result
result → CLEANING → terminal state
```

Only one Turn may run for a Session. A warm sandbox may stay attached to that Session for a short TTL; after expiry, its workspace is checkpointed and the sandbox is deleted.

Turn ownership is a renewable PostgreSQL lease. The Sandbox Runtime consumes Harness stdout in a background task and keeps a sequenced replay journal independently of the Broker HTTP connection. A replacement Broker can reattach the same namespaced Sandbox, replay only missing Runtime events, checkpoint the result and finish cleanup. If the Runtime or Sandbox itself is lost after Harness execution began, the broker reports an interrupted Turn instead of replaying potentially non-idempotent public side effects.

Harnesses can request user decisions with `sandbox-interact`; Codex native user-input and approval requests map to the same durable Interaction protocol. Long commands can be delegated to `sandbox-background`, whose process group, bounded log, cancellation and exit status are supervised by the Runtime and remain part of the Turn result.

## Workspace

```text
/workspace/project   persistent user workspace
/workspace/inbox     current message and attachments
/workspace/outputs   exported artifacts
/workspace/state     native Harness and Provider state
/workspace/tmp       disposable Turn data
/run/harness         ephemeral token and runtime configuration
```

Uploaded Turn attachments are versioned as managed project assets and exported outputs are versioned as managed project artifacts. Their immutable objects remain in object storage and are materialized under `/workspace/project/assets` and `/workspace/project/artifacts` after every checkpoint restore. They therefore survive a new Sandbox or a different OpenSandbox worker without retaining historical `inbox` or `outputs` directories.

The Workspace panel also supports direct project uploads, rename, deletion and version history. Deletion creates an auditable tombstone; each Turn resets managed directories before materializing the current manifest, so stale files cannot survive in an attached Sandbox.

The browser keeps the selected Thread in `?thread=<id>`, restores it after refresh and supports history navigation. Users may delete terminal Threads; deletion first removes any retained Session Sandbox, then removes the owner's conversation records. Images, videos and text files open in an ownership-checked inline preview, while every file retains an explicit download action.

## Development

```bash
UV_CACHE_DIR=.data/uv-cache uv sync --extra test
.venv/bin/pytest
.venv/bin/uvicorn sandbox_harness.app:create_app --factory --reload --port 22766
```

Set `SANDBOX_HARNESS_BACKEND=fake` only for local UI development and tests. Production accepts only `opensandbox` and never falls back to a host Harness.

See [architecture.md](docs/architecture.md), [client API](docs/api.md), [data model](docs/data-model.md), [product interface](docs/product-interface.md), [gateway.md](docs/gateway.md) and [deployment.md](docs/deployment.md).

Real deployment evidence for code execution, media processing, autonomous repair, checkpoint recovery, discovered models and browser isolation is recorded in [first-capability-report.md](docs/test-evidence/first-capability-report.md).

Real Chromium, Canvas, Three.js, deterministic animation recording and native multi-input evidence is recorded in [browser-rendering-report.md](docs/test-evidence/browser-rendering-report.md).

Non-graceful API restart, Runtime event replay, Turn lease fencing and cleanup recovery evidence is recorded in [broker-recovery-report.md](docs/test-evidence/broker-recovery-report.md).

Versioned uploads and outputs restored into a different Sandbox, plus desktop/mobile Workspace UI evidence, are recorded in [project-files-report.md](docs/test-evidence/project-files-report.md).

The final 21-case capability, interaction, Workspace, background execution, recovery and browser matrix is recorded in [full-product-report.md](docs/test-evidence/full-product-report.md).

## Runtime Toolchain

Every Harness image inherits the same immutable foundation with Python, Node.js, ffmpeg/ffprobe, Chromium, Playwright with its pinned Chromium browser, ImageMagick, Three.js, Git, curl, jq, ripgrep, file inspection, GCC/G++, Make, CMake, pkg-config, archive tools and CJK fonts. The common Runtime layer additionally carries NumPy, Pillow, OpenCV, SciPy, scikit-image, librosa, SoundFile, Matplotlib, MoviePy, PyAV, ImageIO, CairoSVG, PSD Tools, rembg/ONNX Runtime, Trimesh and related media libraries. `runtime/media_probe.py` imports the declared modules during every image build. [toolchain.lock.json](runtime/toolchain.lock.json) records the observed foundation command and package versions; image builds fail when the foundation no longer matches it. The deployed Harness images are referenced by registry digest rather than mutable tag.

Sandbox public traffic is best-effort and passes through the capability-checked HTTP/S proxy. Foreign sites may be unavailable because of host routing, DNS interception or the destination's own anti-bot policy. Agents are instructed to stop repeated retries, prefer accessible domestic/public sources or user-provided assets, and report the limitation. For browser access to public pages, use `sandbox-browser-screenshot URL OUTPUT`; it launches Chromium with the Turn's authenticated proxy. Raw Chromium has no direct Internet route.

Runtime boundary handling: execution creation and stdin submission are serialized. An input ID is bound to its original body; a replay returns the existing execution and conflicting content is rejected before attachment metadata changes. Streaming Harnesses must acknowledge their pending inputs: EOF alone is not success. Protocol failures and cancellation quiesce the Harness process group and supervised background jobs before publishing a terminal result; jobs ignoring SIGTERM are killed after a bounded grace period.

Stop requests are persisted on the Turn. The lease owner observes them through its heartbeat, and a replacement Broker honors them without replaying the Harness. Stop and final completion are serialized in the database; a stop accepted before completion prevents a later success from overriding it. The UI displays “正在停止” and disables further input while cleanup finishes. Targeted regression cases are in `tests/test_runtime_boundaries.py` and `tests/test_cancel_boundaries.py`.

Native interaction IDs exposed outside the sandbox are namespaced by execution; late answers cannot reach a later Harness process that reused the same native ID. Ending an execution also releases unanswered helper requests. Model discovery retains the last inventory if the upstream listing endpoint fails and still probes explicitly configured model routes. Redis connection and command stalls are bounded, so event persistence can fall back to database replay instead of waiting indefinitely.

The sidebar green dot means a completed Turn has not been viewed. Once the completed transcript is loaded in the focused, visible conversation, the client acknowledges that exact Turn through `POST /api/threads/{thread_id}/read`. The owner-scoped acknowledgement is stored on the Thread, survives reloads, and cannot pre-read a running Turn or acknowledge a newer completion through a stale request. Running Turns retain their spinner.

Structured lifecycle logs and OpenTelemetry traces cover API, Broker, Gateway, storage and Runtime. Logs are stored under `.data/logs` with bounded queues, redaction and retention; Runtime records are exported to the existing Gateway and collected again before deletion. Use `.venv/bin/sandbox-harness-logs --turn TURN_ID` to assemble a timeline. See [logging, IDs, retention, export configuration and troubleshooting](docs/observability.md).
