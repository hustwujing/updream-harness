#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
expected_root=/data2/wujing03/sandbox-harness-platform-parallel-ali
if [[ "$root" != "$expected_root" ]]; then
  echo "unexpected_project_root:$root" >&2
  exit 1
fi
if [[ $EUID -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

env_file="$root/.data/config/platform.env"
data_root="$root/.data/postgresql"
socket_root="$root/.data/postgresql/run"
log_file="$root/.data/postgresql/postgresql.log"
if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file" >&2
  exit 1
fi
set -a
source "$env_file"
set +a
if [[ "$SANDBOX_HARNESS_ADMIN_DATABASE_URL" != *"@127.0.0.1:28433/postgres" ]]; then
  echo "admin database identity must be sandbox_harness_parallel_ali@127.0.0.1:28433/postgres" >&2
  exit 1
fi

credentials=${SANDBOX_HARNESS_ADMIN_DATABASE_URL#*//}
credentials=${credentials%%@*}
password=${credentials#*:}
install -d -o sandbox-harness-db -g sandbox-harness-db -m 0700 \
  "$data_root" "$socket_root"
if [[ ! -f "$data_root/data/PG_VERSION" ]]; then
  password_file=$(mktemp "$data_root/.init-password.XXXXXX")
  trap 'shred -u "$password_file"' EXIT
  chown sandbox-harness-db:sandbox-harness-db "$password_file"
  chmod 0600 "$password_file"
  printf '%s\n' "$password" > "$password_file"
  runuser -u sandbox-harness-db -- /usr/lib/postgresql/16/bin/initdb \
    -D "$data_root/data" --username=sandbox_harness_parallel_ali \
    --pwfile="$password_file" --auth-host=scram-sha-256 --auth-local=trust \
    --encoding=UTF8 --locale=C.UTF-8
  shred -u "$password_file"
  trap - EXIT
fi

runuser -u sandbox-harness-db -- /usr/lib/postgresql/16/bin/pg_ctl \
  -D "$data_root/data" -l "$log_file" \
  -o "-h 127.0.0.1 -p 28433 -k $socket_root" -w start
trap 'runuser -u sandbox-harness-db -- /usr/lib/postgresql/16/bin/pg_ctl -D "$data_root/data" -m fast -w stop >/dev/null 2>&1 || true' EXIT
runuser -u sandbox-harness-db -- /usr/lib/postgresql/16/bin/pg_ctl \
  -D "$data_root/data" -m fast -w stop
trap - EXIT
echo "initialized Ali PostgreSQL cluster at $data_root"
