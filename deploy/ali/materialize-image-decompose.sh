#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$root" == /data/wujing02/sandbox-harness-platform-parallel-ali ]] || {
  echo "unexpected_project_root:$root" >&2; exit 1; }
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

source_root=/data/wujing03/image-layered
target="$root/.data/services/image-layered"
model_source="$source_root/models"
media_core_source=/data/wujing03/media-foundation-core/media_core
overlay_requirements="$root/deploy/ali/image-inference-overlay-requirements.txt"
[[ -f "$source_root/service/app.py" && -d "$model_source" ]] || {
  echo "image decompose source is unavailable" >&2; exit 1; }
[[ -f "$media_core_source/__init__.py" ]] || {
  echo "media core source is unavailable" >&2; exit 1; }
[[ -f "$overlay_requirements" ]] || {
  echo "image inference overlay requirements are unavailable" >&2; exit 1; }

install -d -o sandbox-harness -g sandbox-harness -m 700 "$target"
cp -a "$source_root/service" "$source_root/web" "$source_root/examples" "$target/"
cp -a "$media_core_source" "$target/"
cp -a "$source_root/requirements.txt" "$source_root/extra_model_paths.yaml" "$target/"
if [[ -L "$target/models" ]]; then
  [[ "$(readlink "$target/models")" == "$model_source" ]] || {
    echo "unexpected shared model link" >&2; exit 1; }
elif [[ -e "$target/models" ]]; then
  echo "model target must be a symlink" >&2; exit 1
else
  ln -s "$model_source" "$target/models"
fi
install -d -o sandbox-harness -g sandbox-harness -m 700 "$target/runtime" "$target/.run"
rm -rf "$target/python-packages"
install -d -o sandbox-harness -g sandbox-harness -m 700 "$target/python-packages"
UV_CACHE_DIR="$root/.data/uv-cache" /data/wujing03/miniforge3/bin/uv pip install \
  --python /data/wujing03/miniforge3/envs/face-makeup/bin/python3.10 \
  --target "$target/python-packages" --no-deps --requirement "$overlay_requirements"
chown -R sandbox-harness:sandbox-harness "$target/service" "$target/web" "$target/examples" \
  "$target/media_core" "$target/python-packages"
chown sandbox-harness:sandbox-harness "$target/requirements.txt" "$target/extra_model_paths.yaml"
echo "materialized $target"
