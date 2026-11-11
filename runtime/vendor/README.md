# Runtime binary cache

Place `drawio-amd64-32.6.10.deb` here before building Runtime images. The file
is intentionally Git-ignored because it is 122104752 bytes. The foundation
build verifies the upstream SHA-256 declared in `foundation.Dockerfile` before
installing it. The source is the official `jgraph/drawio-desktop` v32.6.10
release.

Install the pinned shared Node packages into `npm-prefix/` with the command in
the development handoff. The directory is Git-ignored and copied over the
foundation's existing global Node tree. `runtime/vendor_probe.py` verifies all
top-level package versions before a build starts.
