#!/bin/bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$ ]]; then
  echo "usage: init-artifact.sh PROJECT_NAME" >&2
  exit 2
fi
project="$1"
if [[ -e "$project" ]]; then echo "project already exists: $project" >&2; exit 2; fi
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
skill_root="$(cd "$script_dir/.." && pwd)"
global_root="$(npm root -g)"
test -d "$global_root"
mkdir -p "$project"
cp -R "$skill_root/template/." "$project/"
chmod -R u+rwX "$project"
tar -xzf "$script_dir/shadcn-components.tar.gz" -C "$project/src"

node - "$project" "$global_root" <<'NODE'
const fs=require('fs');
const path=require('path');
const project=path.resolve(process.argv[2]);
const globalRoot=path.resolve(process.argv[3]);
const manifest=JSON.parse(fs.readFileSync(path.join(project,'package.json'),'utf8'));
const dependencies={...(manifest.dependencies||{}),...(manifest.devDependencies||{})};
for(const name of Object.keys(dependencies)){
  const source=path.join(globalRoot,name);
  if(!fs.existsSync(source))throw new Error(`offline dependency unavailable: ${name}`);
  const target=path.join(project,'node_modules',name);
  fs.mkdirSync(path.dirname(target),{recursive:true});
  fs.symlinkSync(source,target,'dir');
}
NODE
echo "offline web artifact initialized: $project"
