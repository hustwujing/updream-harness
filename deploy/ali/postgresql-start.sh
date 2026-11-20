#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
if [[ "$root" != /data2/wujing03/sandbox-harness-platform-parallel-ali ]]; then
  echo "unexpected_project_root:$root" >&2
  exit 1
fi
data_root="$root/.data/postgresql"
socket_root="$root/.data/postgresql/run"
log_file="$root/.data/postgresql/postgresql.log"
exec /usr/lib/postgresql/16/bin/postgres -D "$data_root/data" \
  -h 127.0.0.1 -p 28433 -k "$socket_root" \
  1>>"$log_file" 2>&1
