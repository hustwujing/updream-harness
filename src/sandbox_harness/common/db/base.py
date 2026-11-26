"""Base persistence; transaction boundaries are preserved."""
from __future__ import annotations
import asyncio
from collections import (defaultdict)
from contextlib import (asynccontextmanager)
from sqlalchemy import (text)
from sqlalchemy.ext.asyncio import (async_sessionmaker, create_async_engine)
from sandbox_harness.common.events.events import (RedisEventBus)


class BaseConnections:
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:"):
        self.engine = create_async_engine(database_url)
        self.connections = async_sessionmaker(self.engine, expire_on_commit=False)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None
        self._initialize_locks()

    def _initialize_locks(self):
        self.event_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.event_lock_users: dict[str, int] = defaultdict(int)
        self.event_lock_guard = asyncio.Lock()
        self.job_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.job_lock_users: dict[str, int] = defaultdict(int)
        self.job_lock_guard = asyncio.Lock()

    async def _admit_new_work(self, db, owner_id, thread_id):
        """The old single-database facade has no distributed lifecycle barrier."""

    async def _admit_job_draft(self, db, owner_id, draft_id):
        """Distributed repositories fence draft confirmation before row locks."""

    @asynccontextmanager
    async def transaction(self):
        async with self.connections() as db:
            async with db.begin():
                if self.engine.dialect.name=="sqlite":
                    connection=await db.connection()
                    await connection.exec_driver_sql("BEGIN IMMEDIATE")
                yield db


    @asynccontextmanager
    async def event_lock(self, turn_id: str):
        async with self.event_lock_guard:
            lock = self.event_locks[turn_id]
            self.event_lock_users[turn_id] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.event_lock_guard:
                self.event_lock_users[turn_id] -= 1
                if self.event_lock_users[turn_id] == 0:
                    self.event_lock_users.pop(turn_id, None)
                    self.event_locks.pop(turn_id, None)


    @asynccontextmanager
    async def job_lock(self, identity: str):
        """Serialize one identity locally; database row locks fence other processes."""
        async with self.job_lock_guard:
            lock = self.job_locks[identity]
            self.job_lock_users[identity] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.job_lock_guard:
                self.job_lock_users[identity] -= 1
                if self.job_lock_users[identity] == 0:
                    self.job_lock_users.pop(identity, None)
                    self.job_locks.pop(identity, None)


    async def close(self) -> None:
        if self.events:
            await self.events.close()
        await self.engine.dispose()


    async def healthcheck(self) -> bool:
        async with self.connections() as db:
            await db.execute(text("SELECT 1"))
        return True
