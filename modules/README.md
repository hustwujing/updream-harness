# Module inventory

The 14 `module.json` files declare product ownership, runtime location and build
inputs. `migration_stage: inventory` describes the existing source layout; these
manifests are not yet a claim that each service installs or deploys independently.
The approved migration plan is in
`docs/superpowers/plans/2026-11-26-module-isolation.md`.

`ownership.json` maps every Python source module to its current owner and planned
path. Shared implementation has one source owner; a release includes its declared
common closure. The host, sandbox, controlled-runtime and browser boundaries must
remain explicit. Native dependencies and external model/image inputs are part of
the release contract, not Python dependency aliases.

Validate without launching services:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tools/module_manifest.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest tests/test_module_manifests.py
```

Service artifacts, dependency environments, generated manifests and logs belong
under `.data`. No credentials are stored in these source manifests.
