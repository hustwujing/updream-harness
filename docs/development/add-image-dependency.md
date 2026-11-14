# Adding a Runtime Image Dependency

## Applies to

Required when adding or upgrading an OS package, Python module, global Node
package, browser, renderer, codec, font, native library or Harness client in a
Runtime image.

For the end-to-end Skill/content/image workflow, also read [Skill and Runtime
Image Lifecycle](skill-and-image-lifecycle.md).

## Choose the correct layer

- `foundation.Dockerfile`: OS packages, compiler/toolchain, Node, global browser
  and rendering packages shared by every Harness.
- `base.Dockerfile` and `pyproject.toml[runtime-media]`: project Runtime plus
  shared Python/media libraries and project-owned helper commands.
- `{codex,claude,deepseek}.Dockerfile`: native Harness client only.

Do not add a shared media library three times in Harness images. Do not add a
Harness client to the common base.

## Reproducibility and mirrors

The foundation Python image is digest-pinned. Node, Playwright, Three.js,
esbuild and Harness clients have explicit versions. Debian, Node/npm,
Playwright and Python downloads use the configured domestic mirrors. New
downloads must preserve configurable mirror arguments and fail the build on
checksum/version mismatch where the upstream publishes one.

`toolchain_probe.py` verifies OS packages, commands, global npm packages and the
Playwright browser against `toolchain.lock.json`. An intentional foundation
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
