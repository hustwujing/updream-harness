#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "skills"
SOURCES = ROOT / "sources.json"
NODE_BUILTINS = {
    "assert", "buffer", "child_process", "cluster", "console", "constants", "crypto",
    "dgram", "diagnostics_channel", "dns", "domain", "events", "fs", "http", "http2",
    "https", "module", "net", "os", "path", "perf_hooks", "process", "punycode",
    "querystring", "readline", "repl", "stream", "string_decoder", "test", "timers",
    "tls", "trace_events", "tty", "url", "util", "v8", "vm", "wasi", "worker_threads",
    "zlib",
}
NODE_IMPORT = re.compile(
    r"(?:require\s*\(|import\s*\(|(?:^|[;\n])\s*import(?:\s+[^;\n]*?\s+from)?\s*)"
    r"[\"']([^\"']+)[\"']",
    re.MULTILINE,
)
SUBPROCESS_METHODS = {"run", "Popen", "call", "check_call", "check_output"}


def _local_python_names(skill_root: Path) -> set[str]:
    return ({path.stem for path in skill_root.rglob("*.py")}
        | {path.name for path in skill_root.rglob("*") if path.is_dir()})


def _python_dependencies(skill_root: Path) -> tuple[set[str], set[str]]:
    imports: set[str] = set()
    commands: set[str] = set()
    local_names = _local_python_names(skill_root)
    for path in skill_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(errors="strict"), filename=str(path))
        except SyntaxError as exc:
            raise ValueError(f"Cannot audit Python dependency syntax in {path}: {exc}") from exc
        command_variables: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imports.add(node.module.split(".", 1)[0])
            elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in SUBPROCESS_METHODS and node.args):
                argument = node.args[0]
                if isinstance(argument, ast.Name):
                    command_variables.add(argument.id)
                command = _literal_command(argument)
                if command:
                    commands.add(command)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if not any(isinstance(target, ast.Name) and target.id in command_variables
                       for target in targets):
                continue
            value = node.value
            command = _literal_command(value)
            if command:
                commands.add(command)
            if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add):
                command = _literal_command(value.left)
                if command:
                    commands.add(command)
    external = imports - set(sys.stdlib_module_names) - local_names - {"__future__"}
    return external, commands


def _literal_command(value: ast.AST | None) -> str | None:
    if isinstance(value, (ast.List, ast.Tuple)) and value.elts:
        first = value.elts[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value
    return None


def _node_dependencies(skill_root: Path) -> set[str]:
    modules: set[str] = set()
    for path in skill_root.rglob("*"):
        if not path.is_file() or path.suffix not in {".js", ".cjs", ".mjs", ".ts", ".tsx"}:
            continue
        for raw in NODE_IMPORT.findall(path.read_text(errors="strict")):
            if raw.startswith((".", "/", "~")):
                continue
            name = raw.removeprefix("node:")
            if name.split("/", 1)[0] in NODE_BUILTINS:
                continue
            modules.add("/".join(name.split("/")[:2]) if name.startswith("@")
                else name.split("/", 1)[0])
    return modules


def audit(source_file: Path = SOURCES, skill_root: Path = ROOT) -> dict[str, dict[str, list[str]]]:
    source = json.loads(source_file.read_text())
    profiles = source["dependency_profiles"]
    profile_by_skill = {row["id"]: row["profile"]
        for upstream in source["sources"] for row in upstream["skills"]}
    findings: dict[str, dict[str, list[str]]] = {}
    for skill_id, profile_id in sorted(profile_by_skill.items()):
        profile = profiles[profile_id]
        python_modules, commands = _python_dependencies(skill_root / skill_id)
        node_modules = _node_dependencies(skill_root / skill_id)
        declared_commands = set(profile["commands"]) | set(profile.get("optional_commands", ()))
        missing = {
            "commands": sorted(commands - declared_commands),
            "python_modules": sorted(python_modules - set(profile["python_modules"])),
            "node_modules": sorted(node_modules - set(profile["node_modules"])),
        }
        if any(missing.values()):
            findings[skill_id] = missing
    return findings


def verify(source_file: Path = SOURCES, skill_root: Path = ROOT) -> None:
    findings = audit(source_file, skill_root)
    if findings:
        raise SystemExit("Runtime Skill dependency declarations are incomplete:\n"
            + json.dumps(findings, indent=2, sort_keys=True))
    print("runtime skill dependency audit ok")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=Path, default=SOURCES)
    parser.add_argument("--skill-root", type=Path, default=ROOT)
    args = parser.parse_args()
    verify(args.sources, args.skill_root)


if __name__ == "__main__":
    main()
