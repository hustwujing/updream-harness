#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat
import sys
import tempfile


@dataclass
class Operation:
    kind: str
    path: str
    lines: list[str]
    move_to: str = ""


def parse_patch(source: str) -> list[Operation]:
    lines = source.splitlines()
    if not lines or lines[0] != "*** Begin Patch" or lines[-1] != "*** End Patch":
        raise ValueError("patch must start with '*** Begin Patch' and end with '*** End Patch'")
    operations: list[Operation] = []
    index = 1
    while index < len(lines) - 1:
        header = lines[index]
        prefixes = {
            "*** Add File: ": "add",
            "*** Update File: ": "update",
            "*** Delete File: ": "delete",
        }
        matched = next(((prefix, kind) for prefix, kind in prefixes.items() if header.startswith(prefix)), None)
        if not matched:
            raise ValueError(f"unexpected patch line: {header}")
        prefix, kind = matched
        path = header[len(prefix):].strip()
        if not path:
            raise ValueError("empty patch path")
        index += 1
        move_to = ""
        if index < len(lines) - 1 and lines[index].startswith("*** Move to: "):
            if kind != "update":
                raise ValueError("only an updated file can be moved")
            move_to = lines[index][len("*** Move to: "):].strip()
            index += 1
        body: list[str] = []
        while index < len(lines) - 1 and not lines[index].startswith((
            "*** Add File: ", "*** Update File: ", "*** Delete File: ",
        )):
            body.append(lines[index])
            index += 1
        operations.append(Operation(kind, path, body, move_to))
    if not operations:
        raise ValueError("patch contains no operations")
    return operations


def safe_path(root: Path, raw: str) -> Path:
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"path is outside the working directory: {raw}")
    if candidate.is_symlink():
        raise ValueError(f"symbolic links are not supported: {raw}")
    return resolved


def find_block(lines: list[str], block: list[str], start: int) -> int:
    if not block:
        raise ValueError("update hunk has no context or removed lines")
    matches = [index for index in range(start, len(lines) - len(block) + 1)
        if lines[index:index + len(block)] == block]
    if not matches:
        raise ValueError("update hunk does not match the target file")
    if len(matches) > 1:
        raise ValueError("update hunk is ambiguous")
    return matches[0]


def update_content(content: str, body: list[str]) -> str:
    original = content.splitlines()
    final_newline = content.endswith("\n")
    hunks: list[list[str]] = []
    current: list[str] = []
    for line in body:
        if line.startswith("@@"):
            if current:
                hunks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        hunks.append(current)
    if not hunks:
        raise ValueError("update contains no hunks")
    cursor = 0
    for hunk in hunks:
        old: list[str] = []
        new: list[str] = []
        for line in hunk:
            if not line:
                marker, value = " ", ""
            else:
                marker, value = line[0], line[1:]
            if marker not in {" ", "+", "-"}:
                raise ValueError(f"invalid update line: {line}")
            if marker != "+":
                old.append(value)
            if marker != "-":
                new.append(value)
        position = find_block(original, old, cursor)
        original[position:position + len(old)] = new
        cursor = position + len(new)
    result = "\n".join(original)
    if final_newline:
        result += "\n"
    return result


def apply(source: str, root: Path) -> None:
    root = root.resolve()
    operations = parse_patch(source)
    staged: dict[Path, tuple[str | None, int]] = {}

    def read_target(path: Path) -> tuple[str, int]:
        if path in staged:
            content, mode = staged[path]
            if content is None:
                raise ValueError(f"file was already deleted: {path}")
            return content, mode
        if not path.exists() or not path.is_file():
            raise ValueError(f"target is not a regular file: {path}")
        return path.read_text(), stat.S_IMODE(path.stat().st_mode)

    for operation in operations:
        path = safe_path(root, operation.path)
        if operation.kind == "add":
            if path.exists() or path in staged:
                raise ValueError(f"file already exists: {operation.path}")
            if any(not line.startswith("+") for line in operation.lines):
                raise ValueError("every added-file line must start with '+'")
            staged[path] = ("\n".join(line[1:] for line in operation.lines) + "\n", 0o644)
        elif operation.kind == "delete":
            content, mode = read_target(path)
            if operation.lines:
                raise ValueError("delete operation must not contain patch lines")
            staged[path] = (None, mode)
        else:
            content, mode = read_target(path)
            destination = safe_path(root, operation.move_to) if operation.move_to else path
            if destination != path and (destination.exists() or destination in staged):
                raise ValueError(f"move destination already exists: {operation.move_to}")
            staged[destination] = (update_content(content, operation.lines), mode)
            if destination != path:
                staged[path] = (None, mode)

    for path, (content, mode) in staged.items():
        if content is None:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=".apply-patch-", dir=path.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, mode)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    for path, (content, _) in staged.items():
        if content is None:
            path.unlink()


def main() -> None:
    try:
        apply(sys.stdin.read(), Path.cwd())
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"apply_patch: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print("Done!")


if __name__ == "__main__":
    main()
