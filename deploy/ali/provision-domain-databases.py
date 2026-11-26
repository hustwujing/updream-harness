"""Provision isolated PostgreSQL authorities and atomically update platform.env."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import pwd
import re
import secrets
from urllib.parse import quote, unquote, urlsplit

import asyncpg
from dotenv import dotenv_values


ROOT=Path(__file__).resolve().parents[2]
ENV=ROOT/".data/config/platform.env"
DOMAINS=("accounts","core","media","sites")


def _identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,62}",value):
        raise ValueError("database_identifier_invalid")
    return '"'+value+'"'


def _connection(value):
    parsed=urlsplit(value.replace("postgresql+asyncpg://","postgresql://",1))
    if (parsed.scheme != "postgresql" or not parsed.hostname or not parsed.username
            or parsed.password is None or not parsed.path.strip("/")):
        raise ValueError("database_connection_invalid")
    return {"host":parsed.hostname,"port":parsed.port or 5432,"user":unquote(parsed.username),
            "password":unquote(parsed.password),"database":parsed.path.strip("/")}


def _domain_url(admin, domain, password):
    role=f"sandbox_harness_ali_{domain}"
    database=f"sandbox_harness_ali_{domain}"
    return ("postgresql+asyncpg://"+quote(role,safe="")+":"+quote(password,safe="")+"@"
            +admin["host"]+":"+str(admin["port"])+"/"+database)


def _updated_environment(source, values):
    keys={f"SANDBOX_HARNESS_{domain.upper()}_DATABASE_URL" for domain in DOMAINS}
    keys.add("SANDBOX_HARNESS_DATABASE_URL")
    lines=[line for line in source.splitlines() if line.split("=",1)[0] not in keys]
    lines.extend(f"SANDBOX_HARNESS_{domain.upper()}_DATABASE_URL={values[domain]}"
                 for domain in DOMAINS)
    return "\n".join(lines)+"\n"


async def provision(env_file=ENV):
    if ROOT != Path("/data2/wujing03/sandbox-harness-platform-parallel-ali"):
        raise RuntimeError("unexpected_project_root")
    source=env_file.read_text();configured=dotenv_values(env_file)
    admin=_connection(configured.get("SANDBOX_HARNESS_ADMIN_DATABASE_URL", ""))
    passwords={}
    for domain in DOMAINS:
        existing=configured.get(f"SANDBOX_HARNESS_{domain.upper()}_DATABASE_URL")
        passwords[domain]=(_connection(existing)["password"] if existing
                           else secrets.token_urlsafe(32))
    connection=await asyncpg.connect(**admin)
    try:
        for domain in DOMAINS:
            role=f"sandbox_harness_ali_{domain}";database=f"sandbox_harness_ali_{domain}"
            password=passwords[domain].replace("'","''")
            exists=await connection.fetchval("select 1 from pg_roles where rolname=$1",role)
            if not exists:
                await connection.execute(f"CREATE ROLE {_identifier(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}'")
            else:
                await connection.execute(f"ALTER ROLE {_identifier(role)} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}'")
            exists=await connection.fetchval("select 1 from pg_database where datname=$1",database)
            if not exists:
                await connection.execute(f"CREATE DATABASE {_identifier(database)} OWNER {_identifier(role)}")
            else:
                await connection.execute(f"ALTER DATABASE {_identifier(database)} OWNER TO {_identifier(role)}")
            await connection.execute(f"REVOKE ALL ON DATABASE {_identifier(database)} FROM PUBLIC")
            await connection.execute(f"GRANT CONNECT, TEMPORARY ON DATABASE {_identifier(database)} TO {_identifier(role)}")
    finally:
        await connection.close()
    urls={domain:_domain_url(admin,domain,passwords[domain]) for domain in DOMAINS}
    staging=env_file.with_suffix(".env.domain.tmp")
    staging.write_text(_updated_environment(source,urls));staging.chmod(0o600)
    account=pwd.getpwnam("sandbox-harness")
    os.chown(staging,account.pw_uid,account.pw_gid)
    os.replace(staging,env_file)
    for domain in DOMAINS:
        test=await asyncpg.connect(**_connection(urls[domain]))
        try:
            assert await test.fetchval("select current_user") == f"sandbox_harness_ali_{domain}"
        finally:
            await test.close()


def main():
    asyncio.run(provision())
    print("provisioned independent accounts/core/media/sites databases")


if __name__=="__main__":
    main()
