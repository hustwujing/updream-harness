#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
docker=/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker
export DOCKER_HOST=unix:///run/docker.sock
drawio="$root/runtime/vendor/drawio-amd64-32.6.10.deb"
drawio_sha=fc96faafe68cb4e06c0b7116779891592b20fce2cde7907adec504f23c5af83c
test -f "$drawio"
test "$(stat -c %s "$drawio")" = 122104752
printf '%s  %s\n' "$drawio_sha" "$drawio" | sha256sum -c -
git -C "$root" diff --quiet
git -C "$root" diff --cached --quiet
python3 "$root/runtime/skill_bundle.py" verify
python3 "$root/runtime/vendor_probe.py"
registry=127.0.0.1:25001/sandbox-harness
tag=$(git -C "$root" rev-parse --short=12 HEAD)
base="sandbox-harness-runtime-base:$tag"
foundation_hash=$(sha256sum "$root/runtime/foundation.Dockerfile" | cut -c1-12)
foundation="sandbox-harness-runtime-foundation:$foundation_hash"

if ! "$docker" image inspect "$foundation" >/dev/null 2>&1; then
  "$docker" build --network host -f "$root/runtime/foundation.Dockerfile" -t "$foundation" "$root"
fi
"$docker" build --network host -f "$root/runtime/base.Dockerfile" --build-arg "FOUNDATION_IMAGE=$foundation" --build-arg "RELEASE=$tag" -t "$base" "$root"
for harness in codex claude deepseek; do
  image="$registry/$harness:$tag"
  "$docker" build --network host -f "$root/runtime/$harness.Dockerfile" --build-arg "BASE_IMAGE=$base" -t "$image" "$root"
  "$docker" push "$image"
done

"$docker" image inspect \
  "$registry/codex:$tag" "$registry/claude:$tag" "$registry/deepseek:$tag" \
  --format '{{index .RepoDigests 0}}'
