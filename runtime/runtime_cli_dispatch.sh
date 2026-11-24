#!/bin/sh
name=$(basename "$0")
state_root=${SANDBOX_RUNTIME_STATE_DIR:-${TMPDIR:-/tmp}/sandbox-harness-runtime-$(id -u)}
release="$state_root/code-releases/current"
target="$release/bin/$name"
if [ ! -x "$target" ]; then
  echo "Runtime Code Bundle is not active" >&2
  exit 75
fi
export PYTHONPATH="$release/site-packages"
export PATH="$release/bin:$PATH"
exec "$target" "$@"
