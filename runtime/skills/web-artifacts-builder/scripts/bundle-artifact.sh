#!/bin/bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test -f package.json || { echo "package.json is required" >&2; exit 2; }
test -f index.html || { echo "index.html is required" >&2; exit 2; }
node "$script_dir/bundle-artifact.mjs" "$PWD"
