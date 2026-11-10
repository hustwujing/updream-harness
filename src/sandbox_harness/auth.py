from __future__ import annotations

import asyncio
from datetime import timedelta
import hashlib
import re
import secrets
import unicodedata

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from .domain import AccountView, now


AUTH_COOKIE = "sandbox_harness_auth"
USERNAME = re.compile(r"^[a-z0-9][a-z0-9_.-]{2,63}$")


def normalize_username(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip().casefold()
    if not USERNAME.fullmatch(normalized):
        raise ValueError("Username must be 3-64 characters using letters, numbers, dot, underscore or hyphen")
    return normalized


def validate_password(value: str) -> str:
    if len(value) < 8 or len(value) > 256:
        raise ValueError("Password must be 8-256 characters")
    return value


def token_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class AccountAuth:
    def __init__(self, store, ttl_seconds: int):
        self.store = store
        self.ttl_seconds = ttl_seconds
        self.hasher = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=2)
        self.dummy_hash = self.hasher.hash("not-a-real-account-password")

    async def hash_password(self, password: str) -> str:
        validate_password(password)
        return await asyncio.to_thread(self.hasher.hash, password)

    async def verify_hash(self, password_hash: str, password: str) -> bool:
        try:
            return await asyncio.to_thread(self.hasher.verify, password_hash, password)
        except (VerifyMismatchError, InvalidHashError):
            return False

    async def authenticate(self, username: str, password: str) -> AccountView | None:
        try: normalized = normalize_username(username)
        except ValueError: normalized = "invalid-user"
        row = await self.store.get_user_by_username(normalized)
        password_hash = row[1] if row else self.dummy_hash
        valid = await self.verify_hash(password_hash, password)
        if not row or not valid or row[0].disabled:return None
        if self.hasher.check_needs_rehash(password_hash):
            await self.store.update_password(row[0].id, await self.hash_password(password))
        return row[0]

    async def verify_account(self, account: AccountView, password: str) -> bool:
        row = await self.store.get_user_by_username(account.username)
        return bool(row and row[0].id == account.id and await self.verify_hash(row[1], password))

    async def create_session(self, user_id: str) -> tuple[str, str]:
        token = secrets.token_urlsafe(32);digest = token_hash(token)
        await self.store.create_auth_session(digest, user_id, now() + timedelta(seconds=self.ttl_seconds))
        return token, digest

    async def resolve(self, token: str | None) -> tuple[AccountView | None, str]:
        if not token:return None,""
        digest = token_hash(token)
        return await self.store.resolve_auth_session(digest), digest
