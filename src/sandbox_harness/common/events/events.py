from __future__ import annotations

import asyncio
import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from sandbox_harness.common.observability.observability import (event as log_event)
from sandbox_harness.common.contracts.domain import (Event)

logger = logging.getLogger(__name__)
REDIS_IO_TIMEOUT = 2.0


class RedisEventBus:
    """Redis wake-up and short replay buffer; PostgreSQL remains authoritative."""

    def __init__(self, url: str, prefix: str):
        self.redis = Redis.from_url(url, decode_responses=True, socket_connect_timeout=REDIS_IO_TIMEOUT, socket_timeout=17)
        self.prefix = prefix
        self.available = True

    def key(self, turn_id: str) -> str:
        return self.prefix + "events:" + turn_id

    async def initialize(self) -> None:
        try:
            async with asyncio.timeout(REDIS_IO_TIMEOUT):
                await self.redis.ping()
        except (RedisError, TimeoutError) as exc:
            self.available = False
            log_event("redis.connection.failed",level=logging.WARNING,exc=exc,reason="database_replay_fallback")

    async def close(self) -> None:
        await self.redis.aclose()

    async def publish(self, event: Event) -> None:
        key = self.key(event.turn_id)
        payload = json.dumps(event.model_dump(mode="json"), ensure_ascii=False)
        try:
            async with asyncio.timeout(REDIS_IO_TIMEOUT), self.redis.pipeline(transaction=True) as pipeline:
                pipeline.xadd(key, {"event": payload}, id=f"{event.sequence}-0", maxlen=2000, approximate=True)
                pipeline.expire(key, 86400)
                await pipeline.execute()
            self.available = True
        except (RedisError, TimeoutError) as exc:
            self.available = False
            log_event("redis.publish.failed",level=logging.WARNING,exc=exc,turn_id=event.turn_id,reason="database_replay_fallback")

    async def wait(self, turn_id: str, after: int, block_ms: int = 15000) -> None:
        try:
            block_ms = max(1, min(block_ms, 15000))
            async with asyncio.timeout(block_ms / 1000 + REDIS_IO_TIMEOUT):
                await self.redis.xread({self.key(turn_id): f"{after}-0"}, count=1, block=block_ms)
            self.available = True
        except (RedisError, TimeoutError) as exc:
            self.available = False
            log_event("redis.wait.failed",level=logging.WARNING,exc=exc,turn_id=turn_id,reason="database_replay_fallback")

    async def delete(self, turn_ids: list[str]) -> None:
        if not turn_ids:
            return
        try:
            async with asyncio.timeout(REDIS_IO_TIMEOUT):
                await self.redis.delete(*(self.key(turn_id) for turn_id in turn_ids))
        except (RedisError, TimeoutError) as exc:
            self.available = False
            log_event("redis.cleanup.failed",level=logging.WARNING,exc=exc,count=len(turn_ids))
