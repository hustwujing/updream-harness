#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
if [[ "$root" != /data2/wujing03/sandbox-harness-platform-parallel-ali ]]; then
  echo "unexpected_project_root:$root" >&2
  exit 1
fi
component=${1:-}
env_file="$root/.data/config/platform.env"
log_root="$root/.data/service-logs"
[[ -f "$env_file" ]] || { echo "missing $env_file" >&2; exit 1; }
mkdir -p "$log_root" "$root/.data/tmp"
set -a
source "$env_file"
set +a
export TMPDIR="$root/.data/tmp"
export XDG_CACHE_HOME="$root/.data/cache"
export PIP_CACHE_DIR="$root/.data/pip-cache"
export UV_CACHE_DIR="$root/.data/uv-cache"
export npm_config_cache="$root/.data/npm-cache"
export PLAYWRIGHT_BROWSERS_PATH="$root/.data/playwright-browsers"

case "$component" in
  api) command=("$root/.venv/bin/sandbox-harness-parallel-api") ;;
  gateway) command=("$root/.venv/bin/sandbox-harness-parallel-gateway") ;;
  image-decompose)
    service_root="$root/.data/services/image-layered"
    [[ -f "$service_root/service/app.py" ]] || { echo "image decompose service is not materialized" >&2; exit 1; }
    export PYTHONPATH="$service_root:${PYTHONPATH:-}"
    command=(/data/wujing03/miniforge3/envs/media-foundation/bin/python -m uvicorn
      service.app:app --host 127.0.0.1 --port 8912 --no-access-log)
    ;;
  job-worker) command=("$root/.venv/bin/sandbox-harness-parallel-job-worker") ;;
  controlled-worker) command=("$root/.venv/bin/sandbox-harness-parallel-controlled-worker") ;;
  media-worker) command=("$root/.venv/bin/sandbox-harness-parallel-media-worker") ;;
  site-worker) command=("$root/.venv/bin/sandbox-harness-parallel-site-worker") ;;
  *) echo "unknown Ali service: $component" >&2; exit 2 ;;
esac
exec "${command[@]}" >>"$log_root/$component.log" 2>&1
