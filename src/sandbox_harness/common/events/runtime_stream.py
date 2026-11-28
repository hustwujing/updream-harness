from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from sandbox_harness.common.contracts.domain import EventKind, RuntimeEvent


logger = logging.getLogger(__name__)
PersistEvent = Callable[[RuntimeEvent], Awaitable[object]]
PublishLive = Callable[[dict], Awaitable[None]]
AckSequence = Callable[[int], Awaitable[None]]


class RuntimeStreamBuffer:
    """Coalesce public text while preserving ordered control-event commits."""

    def __init__(self, *, execution_id:str, persist_event:PersistEvent,
                 persist_seconds:float=5, live_seconds:float=.3,
                 publish_live:PublishLive|None=None,
                 acknowledge:AckSequence|None=None):
        self.execution_id=execution_id
        self.persist_event=persist_event
        self.persist_seconds=persist_seconds
        self.live_seconds=live_seconds
        self.publish_live=publish_live
        self.acknowledge=acknowledge
        self._lock=asyncio.Lock()
        self._commit_lock=asyncio.Lock()
        self._text:list[str]=[]
        self._text_start=0
        self._text_end=0
        self._tool_payload:dict|None=None
        self._tool_text:list[str]=[]
        self._tool_start=0
        self._tool_end=0
        self._live_parts:list[tuple[int,str]]=[]
        self._live_item_id=''
        self._persist_task:asyncio.Task|None=None
        self._live_task:asyncio.Task|None=None
        self._first_text_committed=False
        self._failure:BaseException|None=None

    async def add(self,value:RuntimeEvent)->None:
        self._raise_failure()
        if value.kind==EventKind.TOOL_OUTPUT:
            await self._flush_persisted()
            async with self._lock:
                identity=str(value.payload.get('tool_use_id') or '')
                current=str((self._tool_payload or {}).get('tool_use_id') or '')
            if self._tool_payload is not None and identity!=current:
                await self._flush_tool_output()
            async with self._lock:
                if self._tool_payload is None:self._tool_start=value.runtime_sequence
                self._tool_payload=dict(value.payload)
                self._tool_text.append(str(value.payload.get('text') or ''))
                self._tool_end=value.runtime_sequence
            self._ensure_persist_timer();return
        if value.kind!=EventKind.TEXT_DELTA:
            await self.flush()
            await self._persist(value)
            if (value.kind in {EventKind.TOOL_STARTED,EventKind.STEP_STARTED}
                    and not value.payload.get('model_request_id')):
                self._first_text_committed=False
                self._live_item_id=''
            return
        await self._flush_tool_output()
        text=str(value.payload.get('text') or '')
        if not text:
            await self._persist(value)
            return
        async with self._lock:
            self._append_text(text,value.runtime_sequence)
            self._append_live(text,value.runtime_sequence)
            first=not self._first_text_committed
        if first:
            await self._flush_persisted()
        else:
            self._ensure_persist_timer()
        self._ensure_live_timer()

    async def flush(self)->None:
        self._raise_failure()
        await self._cancel_timer('_live_task')
        await self._flush_live()
        await self._cancel_timer('_persist_task')
        await self._flush_persisted()
        await self._flush_tool_output()
        self._raise_failure()

    async def close(self)->None:
        await self.flush()

    async def abandon(self)->None:
        await self._cancel_timer('_live_task')
        await self._cancel_timer('_persist_task')

    def _append_text(self,text:str,sequence:int)->None:
        if not self._text:self._text_start=sequence
        self._text.append(text);self._text_end=sequence

    def _append_live(self,text:str,sequence:int)->None:
        self._live_parts.append((sequence,text))

    def _ensure_persist_timer(self)->None:
        if self._persist_task is None or self._persist_task.done():
            self._persist_task=asyncio.create_task(
                self._delayed_persist(),name='runtime-text-persist-'+self.execution_id)

    def _ensure_live_timer(self)->None:
        if self.publish_live and (self._live_task is None or self._live_task.done()):
            self._live_task=asyncio.create_task(
                self._delayed_live(),name='runtime-text-live-'+self.execution_id)

    async def _delayed_persist(self)->None:
        try:
            await asyncio.sleep(self.persist_seconds)
            await self._flush_persisted()
            await self._flush_tool_output()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._failure=exc

    async def _delayed_live(self)->None:
        try:
            await asyncio.sleep(self.live_seconds)
            await self._flush_live()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            logger.warning('Live Runtime stream publication unavailable execution=%s error=%s',
                self.execution_id,type(exc).__name__)

    async def _flush_persisted(self)->None:
        async with self._commit_lock:
            async with self._lock:
                if not self._text:return
                text=''.join(self._text);start=self._text_start;end=self._text_end
                self._text=[];self._text_start=0;self._text_end=0
            try:
                receipt=await self._persist_unlocked(RuntimeEvent(kind=EventKind.TEXT_DELTA,
                    payload={'text':text,'sequence_start':start,'sequence_end':end},
                    runtime_sequence=end))
                item_id=getattr(receipt,'item_id','')
                if not item_id and isinstance(receipt,dict):item_id=str(receipt.get('item_id') or '')
                if item_id:self._live_item_id=str(item_id)
                self._first_text_committed=True
            except BaseException:
                async with self._lock:
                    self._text=[text,*self._text]
                    self._text_start=start
                    if not self._text_end:self._text_end=end
                raise

    async def _flush_live(self)->None:
        if not self.publish_live:return
        async with self._lock:
            if not self._live_parts:return
            parts=self._live_parts;self._live_parts=[]
            value={'execution_id':self.execution_id,'item_id':self._live_item_id,
                'sequence_start':parts[0][0],
                'sequence_end':parts[-1][0],'text':''.join(text for _,text in parts),
                'parts':[{'sequence':sequence,'text':text} for sequence,text in parts]}
        await self.publish_live(value)

    async def _flush_tool_output(self)->None:
        async with self._commit_lock:
            async with self._lock:
                if self._tool_payload is None:return
                payload={**self._tool_payload,'text':''.join(self._tool_text),
                    'sequence_start':self._tool_start,'sequence_end':self._tool_end}
                start=self._tool_start;end=self._tool_end
                self._tool_payload=None;self._tool_text=[];self._tool_start=0;self._tool_end=0
            try:
                await self._persist_unlocked(RuntimeEvent(kind=EventKind.TOOL_OUTPUT,
                    payload=payload,runtime_sequence=end))
            except BaseException:
                async with self._lock:
                    self._tool_payload=payload;self._tool_text=[str(payload.get('text') or '')]
                    self._tool_start=start;self._tool_end=end
                raise

    async def _persist(self,value:RuntimeEvent)->None:
        async with self._commit_lock:await self._persist_unlocked(value)

    async def _persist_unlocked(self,value:RuntimeEvent):
        result=await self.persist_event(value)
        if self.acknowledge and value.runtime_sequence:
            try:await self.acknowledge(value.runtime_sequence)
            except Exception as exc:
                logger.warning('Runtime stream acknowledgement unavailable execution=%s sequence=%s error=%s',
                    self.execution_id,value.runtime_sequence,type(exc).__name__)
        return result

    async def _cancel_timer(self,name:str)->None:
        task=getattr(self,name)
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        if task is not asyncio.current_task():setattr(self,name,None)

    def _raise_failure(self)->None:
        if self._failure is not None:raise self._failure
