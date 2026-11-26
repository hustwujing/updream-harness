# Module inventory

The 14 `module.json` files declare product ownership, runtime location and build
inputs. Each host service builds and deploys independently through its immutable
descriptor. See [architecture and ownership](../docs/development/architecture-and-ownership.md).

`ownership.json` maps every Python source module to its current owner. Shared
implementation has one source owner; a release includes its declared
common closure. The host, sandbox, controlled-runtime and browser boundaries must
remain explicit. Native dependencies and external model/image inputs are part of
the release contract, not Python dependency aliases.

Validate without launching services:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tools/module_manifest.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest tests/test_module_manifests.py
```

Service artifacts, dependency environments, generated manifests and logs belong
under `.data`. No credentials are stored in these manifests.

The source/resource artifact builder follows actual local Python imports and
includes parent package initializers. It rejects cross-service implementation
imports, external symlink resources and altered artifacts. Its output is a
content-addressed `source-only` intermediate; `tools/service_release.py` binds
that source to the frozen dependency environment and produces the selected
deployable release:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tools/build_module.py \
  --module site-worker --output .data/module-artifacts
```
