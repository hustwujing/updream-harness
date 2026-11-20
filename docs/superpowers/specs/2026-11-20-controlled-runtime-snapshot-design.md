# Controlled Runtime Snapshot Design

## Intent

The Parallel controlled CPU/GPU worker must survive deletion or replacement of
the project used as its installation source. Every byte needed at service start
must remain below the Parallel `.data` root.

## Approaches

- A stable symlink to the source is rejected: deleting the source broke the
  deployed worker.
- A mutable copy at `.data/provider-runtime` is rejected: partial updates cannot
  be distinguished from a valid release and rollback is ambiguous.
- A manifest-addressed immutable snapshot is selected. Installation materializes
  and validates the complete source under `.data/controlled-runtime/<digest>`,
  then atomically changes the stable `.data/provider-runtime` symlink.

## Contract

- Copy into a same-filesystem staging directory with reflink support when
  available; never write runtime bytes to the root filesystem.
- Materialize any symlink that resolves outside the snapshot. Relative links
  resolving within the snapshot remain links.
- Conda interpreters retain their build-time prefixes. The bwrap namespace
  exposes each project-owned environment read-only at that virtual prefix; it
  never bind-mounts the host `/data` tree.
- The complete snapshot is mounted at `/runtime` inside bwrap. Operation code
  never traverses the host checkout or `.data` path after privilege reduction.
- Rebuild the manifest from the snapshot and require its file rows to equal the
  source manifest before publication.
- The manifest includes every runtime file in the SAM3 package, ZIM package and
  config, and libdirector package. Git metadata, caches, examples and unrelated
  duplicate model weights are not part of the execution closure.
- Publish the stable symlink atomically. Repeating the same installation is
  idempotent; a corrupt existing digest directory is rejected.
- The controlled worker continues to validate the frozen manifest digest and
  required entry points at every start.

## Non-goals

- Repackaging Conda environments or changing their dependencies.
- Rebuilding Runtime images.
- Sharing mutable state with a predecessor project.
