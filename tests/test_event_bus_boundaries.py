import asyncio
import time
import sandbox_harness.common.events.events as events
from sandbox_harness.common.contracts.domain import (Event, EventKind)


async def test_redis_connected_but_silent_has_bounded_database_fallback(monkeypatch):
    monkeypatch.setattr(events,'REDIS_IO_TIMEOUT',.05)
    handlers=set()
    async def silent(reader,writer):
        task=asyncio.current_task();handlers.add(task)
        try:
            while await reader.read(65536):pass
        finally:
            writer.close();await writer.wait_closed();handlers.discard(task)
    server=await asyncio.start_server(silent,'127.0.0.1',0)
    bus=events.RedisEventBus(f'redis://127.0.0.1:{server.sockets[0].getsockname()[1]}','isolated:')
    started=time.monotonic()
    try:
        await bus.initialize();assert not bus.available
        await bus.publish(Event(turn_id='test',sequence=1,kind=EventKind.STATE,payload={'state':'RUNNING'}));assert not bus.available
        await bus.publish_live('turn','test',{'execution_id':'one','parts':[]});assert not bus.available
        _,rows=await bus.read_live('turn','test',block_ms=1);assert rows==[] and not bus.available
        assert await bus.latest_live('turn',{'test'})=={} and not bus.available
        ready,_,rows=await bus.wait_turn_activity('test',0,block_ms=1)
        assert not ready and rows==[] and not bus.available
        await bus.wait('test',0,block_ms=1);assert not bus.available
        await bus.delete(['test']);assert not bus.available
        assert time.monotonic()-started<1.5
    finally:
        await bus.close();server.close();await server.wait_closed()
        for task in list(handlers):task.cancel()
        await asyncio.gather(*list(handlers),return_exceptions=True)
