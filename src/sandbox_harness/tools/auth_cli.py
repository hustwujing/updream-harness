from __future__ import annotations

import argparse
import asyncio
from getpass import getpass
import os
from pathlib import Path

from dotenv import dotenv_values

from sandbox_harness.common.security.auth import (AccountAuth, normalize_username)
from sandbox_harness.common.configuration.config import (load_settings)
from sandbox_harness.common.db.store import (Store)


def password_value(path: str) -> str:
    if path:
        target=Path(path)
        if target.stat().st_mode & 0o077:raise ValueError("Password file must not be group/world accessible")
        value = target.read_text().rstrip("\r\n")
    else:
        value = getpass("Password: ")
        if value != getpass("Repeat password: "):raise ValueError("Passwords do not match")
    return value


async def run(args) -> None:
    env_file = Path(args.env_file) if args.env_file else Path.cwd()/'.data/config/platform.env'
    if env_file.is_file():
        for key,value in dotenv_values(env_file).items():
            if value is not None:os.environ[key]=value
    settings = load_settings();store = Store(settings.database_url);await store.initialize()
    auth = AccountAuth(store, settings.auth_session_ttl_seconds)
    try:
        if args.action == "list":
            for user in await store.list_users():print(f"{user.id}\t{user.username}\t{'disabled' if user.disabled else 'enabled'}")
            return
        username = normalize_username(args.username)
        if args.action == "create":
            user = await store.create_user(username,await auth.hash_password(password_value(args.password_file)))
        else:
            row = await store.get_user_by_username(username)
            if not row:raise KeyError("user_not_found")
            user = row[0]
            if args.action == "reset-password":await store.update_password(user.id,await auth.hash_password(password_value(args.password_file)))
            elif args.action == "rename":user = await store.update_username(user.id,normalize_username(args.new_username))
            elif args.action in {"disable","enable"}:user = await store.set_user_disabled(user.id,args.action=="disable")
        print(f"{user.id}\t{user.username}\t{'disabled' if user.disabled else 'enabled'}")
    finally:
        await store.close()


def main() -> None:
    parser=argparse.ArgumentParser(description="Manage updream-harness accounts")
    parser.add_argument('--env-file',default=os.getenv('SANDBOX_HARNESS_ENV_FILE',''))
    sub=parser.add_subparsers(dest="action",required=True)
    sub.add_parser("list")
    for action in ("create","reset-password","disable","enable"):
        command=sub.add_parser(action);command.add_argument("username")
        if action in {"create","reset-password"}:command.add_argument("--password-file",default="")
    rename=sub.add_parser("rename");rename.add_argument("username");rename.add_argument("new_username")
    args=parser.parse_args()
    try:asyncio.run(run(args))
    except (KeyError,RuntimeError,ValueError) as exc:parser.error(str(exc))


if __name__ == "__main__":main()
