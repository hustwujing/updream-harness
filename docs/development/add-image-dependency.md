# Adding a Runtime Image Dependency

## Applies to

Required when adding or upgrading an OS package, Python module, global Node
package, browser, renderer, codec, font, native library or Harness client in a
Runtime image.

For the end-to-end Skill/content/image workflow, also read [Skill and Runtime
Image Lifecycle](skill-and-image-lifecycle.md).

## Choose the correct layer

- `bootstrap.Dockerfile`: signature verification, supervision and generic CLI dispatch.
- `common.Dockerfile` / `requirements-common.txt`: shared lightweight dependencies.
- `profiles/<profile>.Dockerfile` and the matching requirements file: the
  narrowest dependency closure needed by the declared Skills.
- `{codex,claude,deepseek}.Dockerfile`: native Harness client only.

`runtime/build-images.sh` is authoritative for these layers. The older
`foundation.Dockerfile` and `base.Dockerfile` remain compatibility references;
their complete toolchain is not a guarantee for every smaller profile.

A required signed CLI needs a global dispatcher entrypoint in its profile:
login shells can discard the Code Bundle bin directory from inherited `PATH`.
Test both inherited and login shells, then execute the activated CLI and verify
its output. Keep independent entrypoint changes in a separate layer when they
do not change heavy dependencies.

Do not add a shared dependency independently to three Harness layers. Rebuild
and probe every affected Profile/Harness variant, then merge unchanged digest
rows into a complete signed catalog before deployment.

## Reproducibility and mirrors

The foundation Python image is digest-pinned. Node, Playwright, Three.js,
esbuild and Harness clients have explicit versions. Debian, Node/npm,
Playwright and Python downloads use the configured domestic mirrors. New
downloads must preserve configurable mirror arguments and fail the build on
checksum/version mismatch where the upstream publishes one.

`toolchain_probe.py` verifies the older full foundation against
`toolchain.lock.json`; apply that lock only to the image it describes. Current
profile probes must check the profile catalog and Skill dependency declarations. An intentional foundation
change requires generating and reviewing a new snapshot from the built image;
do not hand-edit a version string merely to make verification pass.
Use `python runtime/toolchain_probe.py --output runtime/toolchain.lock.json`
inside the newly built foundation image to write that snapshot.

Python media modules are imported by `media_probe.py`. Add the distribution to
the `runtime-media` extra and its import name to the probe. A successful pip
install without a successful import is not evidence.

When a wheel needs native headers, install those headers only for the build
step in the Base layer and remove them in the same Docker layer. Keep the
runtime shared libraries in Foundation and regenerate the toolchain lock from
the resulting final image.

## Runtime constraints

Images run as UID/GID 10001 without root. Verify executable/read permissions,
writable paths and gVisor compatibility. Browser/3D work uses software rendering
unless a separately designed GPU path exists. Cache, build context and registry
storage stay on `/data2`.

Review image size and cold-start effect. A dependency used by one optional tool
may belong in a Harness/tool-specific layer rather than the common base.

For heavy renderers such as Manim, keep the Skill instructions and references
in the Content Bundle, but install the renderer's Python package in the Runtime
dependency layer and its native toolchain (LaTeX, fonts, codecs and build
headers) in the image layer. The image must expose a deterministic capability
probe and a low-quality render smoke; a Skill entry alone must never advertise
an unavailable renderer. Use the configured domestic Debian/PyPI mirrors for
these build inputs and record the resulting toolchain snapshot.

## Completion

1. Build the affected foundation/base/final images from a clean revision.
2. Run toolchain, media import and image contract probes.
3. Run the dependency's real command/import as UID 10001 in the final image.
4. Run one real OpenSandbox case that uses the capability, including output
   validation rather than only `command -v`.
5. Push and deploy immutable image digests; record old/new digest and build
   revision.
6. For browser/media changes, validate representative output pixels, duration,
   codec or file structure as applicable.
