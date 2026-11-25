# Execution Graph Readability Design

## Intent

Make the execution graph useful for diagnosing real work. A user expanding an activity group must see what actually ran instead of only `N 项操作`, and a desktop viewport should fit more graph ranks without wasting horizontal space.

## Goals

- Preserve grouped semantic stages such as `读取资料` and `执行处理命令`.
- Show the concrete command, file path, external tool, query, or plan description for every child activity.
- Keep long and multiline values readable through a bounded preview plus an expandable full value.
- Reduce desktop nodes from `190x92` to `170x82`, rank gap from `76` to `52`, node gap from `28` to `22`, and wrapped-rank gap from `76` to `52`.
- Keep mobile nodes large enough to read and prevent text overflow.

## Non-goals

- No activity protocol, database, or provider-specific changes.
- Do not display model stream text or command output in the activity summary.
- Do not change graph topology, grouping, drag behavior, or execution semantics.

## Approach

Use the structured activity payload already returned by the API. A shared frontend formatter will derive a short preview and optional full detail from `description`, `command`, `commandActions`, file paths, tool names, and structured queries. Both the Harness timeline and generic node detail panel will consume the same formatter.

Backend-preformatted Chinese copy was rejected because it couples the protocol to one client. Raw event rendering was rejected because it is noisy and can expose large command output. Layout changes stay in the existing Dagre projection and CSS constraints.

## Constraints

- Commands remain factual and are not parsed to infer behavior.
- The preview is single-line and bounded; the full command remains available on explicit expansion.
- Missing structured detail falls back to status text.
- Desktop density must improve without changing the mobile minimum canvas behavior.

## Verification

- Unit tests for command, file action, image, external tool, query, fallback, and multiline truncation.
- Layout tests assert exact node dimensions and at least four wrapped ranks at the representative desktop width.
- Browser execution-graph regression verifies command text is visible after expanding the command group and no overlap occurs.

