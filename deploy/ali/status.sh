#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]] || {
  echo "unexpected_project_root:$root" >&2; exit 1; }
for unit in postgresql network gateway job-worker controlled-worker media-worker site-worker api; do
  name="sandbox-harness-parallel-ali-$unit.service"
  printf '%-58s %s\n' "$name" "$(systemctl is-active "$name" 2>/dev/null || true)"
done
for port in 24767 28433 29882 29883; do
  if ss -lnt | rg -q ":${port}\\b"; then state=listening; else state=closed; fi
  printf 'port %-5s %s\n' "$port" "$state"
done
curl -sS --max-time 5 http://127.0.0.1:24767/api/health 2>/dev/null || true
printf '\n'
