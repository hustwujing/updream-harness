"""Check implementation import ownership, including deferred imports."""
from __future__ import annotations

import argparse
import ast
import importlib.util
import json
from pathlib import Path


def module_name(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root / "src").with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def imports(root: Path, path: Path) -> set[str]:
    name = module_name(root, path)
    package = name if path.name == "__init__.py" else name.rpartition(".")[0]
    result = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                module = importlib.util.resolve_name("." * node.level + module, package)
            result.add(module)
            for alias in node.names:
                candidate = module + "." + alias.name
                source = root / "src" / candidate.replace(".", "/")
                if source.with_suffix(".py").is_file() or (source / "__init__.py").is_file():
                    result.add(candidate)
        elif isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
            function = node.func
            if ((isinstance(function, ast.Name) and function.id == "__import__")
                    or (isinstance(function, ast.Attribute) and function.attr == "import_module")):
                if isinstance(node.args[0].value, str):
                    result.add(node.args[0].value)
    return result


def violations(root: Path) -> list[dict]:
    result = []
    for path in sorted((root / "src").rglob("*.py")):
        name = module_name(root, path)
        service = name.split(".")[2] if name.startswith("sandbox_harness.services.") else None
        for dependency in sorted(imports(root, path)):
            rule = None
            if name.startswith("sandbox_harness.common.") and dependency.startswith("sandbox_harness.services."):
                rule = "common_imports_service"
            elif service and dependency.startswith("sandbox_harness.services.") and dependency.split(".")[2] != service:
                rule = "cross_service_import"
            elif name.startswith("sandbox_harness.common.") and dependency.startswith(("sandbox_runtime", "sandbox_bootstrap")):
                rule = "common_imports_execution_application"
            elif name.startswith("controlled_runtime") and dependency.startswith("sandbox_harness.services"):
                rule = "controlled_program_imports_host_service"
            elif name.startswith("sandbox_runtime") and dependency.startswith((
                    "sandbox_harness.services", "sandbox_harness.common.db",
                    "sandbox_harness.common.configuration", "sandbox_harness.common.sandbox")):
                rule = "sandbox_imports_host_implementation"
            if rule:
                result.append({"source": name, "dependency": dependency, "rule": rule})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    findings = violations(args.root)
    print(json.dumps({"violations": findings}, sort_keys=True))
    if findings:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
