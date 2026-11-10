#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
data_root="$root/.data/postgresql"
env_file="$root/.data/config/platform.env"
if [[ $EUID -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi
if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file" >&2
  exit 1
fi
set -a
source "$env_file"
set +a
if [[ "$SANDBOX_HARNESS_DATABASE_URL" != *"@127.0.0.1:25432/sandbox_harness" ]]; then
  echo "database URL must target the dedicated cluster on 127.0.0.1:25432" >&2
  exit 1
fi

credentials=${SANDBOX_HARNESS_DATABASE_URL#*//}
credentials=${credentials%%@*}
password=${credentials#*:}
install -d -o sandbox-harness-db -g sandbox-harness-db -m 0700 "$data_root" "$data_root/run"
if [[ ! -f "$data_root/data/PG_VERSION" ]]; then
  password_file=$(mktemp "$data_root/.init-password.XXXXXX")
  trap 'shred -u "$password_file"' EXIT
  chown sandbox-harness-db:sandbox-harness-db "$password_file"
  chmod 0600 "$password_file"
  printf '%s\n' "$password" >"$password_file"
  runuser -u sandbox-harness-db -- /usr/lib/postgresql/16/bin/initdb -D "$data_root/data" \
    --username=sandbox_harness --pwfile="$password_file" --auth-host=scram-sha-256 \
    --auth-local=trust --encoding=UTF8 --locale=C.UTF-8
  shred -u "$password_file"
  trap - EXIT
fi

systemctl start sandbox-harness-postgresql.service
export PGPASSWORD="$password"
if ! /usr/lib/postgresql/16/bin/psql -h 127.0.0.1 -p 25432 -U sandbox_harness -d postgres \
    -Atc "select 1 from pg_database where datname='sandbox_harness'" | grep -qx 1; then
  /usr/lib/postgresql/16/bin/createdb -h 127.0.0.1 -p 25432 -U sandbox_harness sandbox_harness
fi
unset PGPASSWORD
