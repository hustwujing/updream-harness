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
LIVE_EVENT_MAX_BYTES=1024*1024


class RedisEventBus:
    """Redis wake-up and short replay buffer; PostgreSQL remains authoritative."""

    def __init__(self, url: str, prefix: str):
        self.redis = Redis.from_url(url, decode_responses=True, socket_connect_timeout=REDIS_IO_TIMEOUT, socket_timeout=17)
        self.prefix = prefix
        self.available = True

    def key(self, turn_id: str) -> str:
        return self.prefix + "events:" + turn_id

    def live_key(self,scope:str,identity:str)->str:
        if scope not in {'turn','job'}:raise ValueError('live_stream_scope_invalid')
        return self.prefix+"live:"+scope+":"+identity

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

    async def publish_live(self,scope:str,identity:str,value:dict)->None:
        key=self.live_key(scope,identity)
        payload=json.dumps(value,ensure_ascii=False,separators=(',',':'))
        if len(payload.encode())>LIVE_EVENT_MAX_BYTES:
            log_event('redis.live.payload.skipped',level=logging.WARNING,
                reason='live_payload_limit',scope=scope)
            return
        try:
            async with asyncio.timeout(REDIS_IO_TIMEOUT), self.redis.pipeline(transaction=True) as pipeline:
                pipeline.xadd(key,{"event":payload},maxlen=512,approximate=True)
                pipeline.expire(key,3600)
                await pipeline.execute()
            self.available=True
        except (RedisError,TimeoutError) as exc:
            self.available=False
            log_event('redis.live.publish.failed',level=logging.WARNING,exc=exc,
                reason='database_progress_fallback')

    async def read_live(self,scope:str,identity:str,after:str='0-0',
                        block_ms:int=1000)->tuple[str,list[dict]]:
        key=self.live_key(scope,identity);cursor=after;values=[]
        try:
            block_ms=max(1,min(block_ms,15000))
            async with asyncio.timeout(block_ms/1000+REDIS_IO_TIMEOUT):
                rows=await self.redis.xread({key:after},count=100,block=block_ms)
            for _,entries in rows:
                for entry_id,fields in entries:
                    cursor=entry_id
                    try:value=json.loads(fields.get('event','{}'))
                    except (TypeError,ValueError):continue
                    if isinstance(value,dict):values.append(value)
            self.available=True
        except (RedisError,TimeoutError) as exc:
            self.available=False
            log_event('redis.live.read.failed',level=logging.WARNING,exc=exc,
                reason='database_progress_fallback')
        return cursor,values

    async def wait_turn_activity(self,turn_id:str,durable_after:int,live_after:str='0-0',
                                 block_ms:int=15000)->tuple[bool,str,list[dict]]:
        live_key=self.live_key('turn',turn_id);cursor=live_after;values=[];durable=False
        try:
            block_ms=max(1,min(block_ms,15000))
            async with asyncio.timeout(block_ms/1000+REDIS_IO_TIMEOUT):
                rows=await self.redis.xread({self.key(turn_id):f'{durable_after}-0',
                    live_key:live_after},count=100,block=block_ms)
            for key,entries in rows:
                if key==self.key(turn_id):durable=True;continue
                for entry_id,fields in entries:
                    cursor=entry_id
                    try:value=json.loads(fields.get('event','{}'))
                    except (TypeError,ValueError):continue
                    if isinstance(value,dict):values.append(value)
            self.available=True
        except (RedisError,TimeoutError) as exc:
            self.available=False
            log_event('redis.activity.wait.failed',level=logging.WARNING,exc=exc,
                turn_id=turn_id,reason='database_replay_fallback')
        return durable,cursor,values

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
