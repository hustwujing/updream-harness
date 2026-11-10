from __future__ import annotations

import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .domain import Event

logger = logging.getLogger(__name__)


class RedisEventBus:
    """Redis wake-up and short replay buffer; PostgreSQL remains authoritative."""

    def __init__(self, url: str, prefix: str):
        self.redis = Redis.from_url(url, decode_responses=True)
        self.prefix = prefix
        self.available = True

    def key(self, turn_id: str) -> str:
        return self.prefix + "events:" + turn_id

    async def initialize(self) -> None:
        try:
            await self.redis.ping()
        except RedisError:
            self.available = False
            logger.warning("Redis event bus is unavailable; using database replay polling", exc_info=True)

    async def close(self) -> None:
        await self.redis.aclose()

    async def publish(self, event: Event) -> None:
        key = self.key(event.turn_id)
        payload = json.dumps(event.model_dump(mode="json"), ensure_ascii=False)
        try:
            async with self.redis.pipeline(transaction=True) as pipeline:
                pipeline.xadd(key, {"event": payload}, id=f"{event.sequence}-0", maxlen=2000, approximate=True)
                pipeline.expire(key, 86400)
                await pipeline.execute()
            self.available = True
        except RedisError:
            self.available = False
            logger.warning("Redis event publish failed; event remains durable in the database", exc_info=True)

    async def wait(self, turn_id: str, after: int, block_ms: int = 15000) -> None:
        try:
            await self.redis.xread({self.key(turn_id): f"{after}-0"}, count=1, block=block_ms)
            self.available = True
        except RedisError:
            self.available = False
            logger.warning("Redis event wait failed; falling back to database polling", exc_info=True)

    async def delete(self, turn_ids: list[str]) -> None:
        if not turn_ids:
            return
        try:
            await self.redis.delete(*(self.key(turn_id) for turn_id in turn_ids))
        except RedisError:
            self.available = False
            logger.warning("Redis event cleanup failed; keys will expire automatically", exc_info=True)
