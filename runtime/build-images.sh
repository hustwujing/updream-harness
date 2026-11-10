#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
docker=/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker
export DOCKER_HOST=unix:///run/docker.sock
registry=127.0.0.1:25001/sandbox-harness
tag=$(git -C "$root" rev-parse --short=12 HEAD)
base="sandbox-harness-runtime-base:$tag"
foundation_hash=$(sha256sum "$root/runtime/foundation.Dockerfile" | cut -c1-12)
foundation="sandbox-harness-runtime-foundation:$foundation_hash"

if ! "$docker" image inspect "$foundation" >/dev/null 2>&1; then
  "$docker" build -f "$root/runtime/foundation.Dockerfile" -t "$foundation" "$root"
fi
"$docker" build -f "$root/runtime/base.Dockerfile" --build-arg "FOUNDATION_IMAGE=$foundation" --build-arg "RELEASE=$tag" -t "$base" "$root"
for harness in codex claude deepseek; do
  image="$registry/$harness:$tag"
  "$docker" build -f "$root/runtime/$harness.Dockerfile" --build-arg "BASE_IMAGE=$base" -t "$image" "$root"
  "$docker" push "$image"
done

"$docker" image inspect \
  "$registry/codex:$tag" "$registry/claude:$tag" "$registry/deepseek:$tag" \
  --format '{{index .RepoDigests 0}}'
