# Runtime CLI Image Repair Plan

**Goal:** Make an already signed Runtime CLI available in login shells in the
profile that declares its Skill, without installing packages during user work.

**Evidence:** `runtime-cli-visibility.json` proves the current real Ali primary
profile resolves `editppt` in an inherited shell and runs its help successfully,
but a login shell cannot resolve it. The global image shim is absent. K02's
model-driven installation workaround is retained as failure history.

**Scope:** Add the missing image entrypoint to primary-full-cpu as a separate
cached layer. Runtime code/content and other profiles require no changes.
Rebuild all three Harness variants of the changed profile. Preserve frozen
active releases and use a complete signed catalog when publishing the partial
matrix update. Registry credentials stay on the host; do not reuse legacy
relay scripts that copy them into a sandbox.

- [ ] Add a profile/CLI contract regression and confirm current failure.
- [ ] Add the entrypoint, run image contracts, and commit the independent fix.
- [ ] Build/probe Codex, Claude and DeepSeek images as a non-root user.
- [ ] Verify login-shell discovery, signed CLI execution and a real artifact.
- [ ] Publish immutable registry images/templates and a signed merged catalog.
- [ ] Deploy and rerun K02 plus relevant provider/restore/CLI smoke checks.
- [ ] Preserve failure evidence, remove owned obsolete resources only after
  reference checks, update documentation and push the final small commits.
