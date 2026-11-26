from __future__ import annotations

import asyncio
from datetime import datetime,timezone
import time
from typing import Awaitable,Callable
from urllib.parse import urlsplit


Probe=Callable[[],Awaitable[tuple[bool,str]]]


class ExecutionReadiness:
    def __init__(self,probes:dict[str,Probe]|None=None,*,ttl_seconds:float=5):
        self.probes=dict(probes or {});self.ttl_seconds=max(0,ttl_seconds)
        self.states:dict[str,dict]={};self.locks={key:asyncio.Lock() for key in self.probes}

    @staticmethod
    def tcp_probe(url:str,*,timeout_seconds:float=.5)->Probe:
        parsed=urlsplit(url)
        if parsed.scheme!='http' or not parsed.hostname or not parsed.port:
            raise ValueError('readiness_probe_url_invalid')
        async def probe()->tuple[bool,str]:
            try:
                reader,writer=await asyncio.wait_for(asyncio.open_connection(
                    parsed.hostname,parsed.port),timeout_seconds)
                writer.close();await writer.wait_closed();return True,''
            except (OSError,asyncio.TimeoutError):return False,'connection_unavailable'
        return probe

    @classmethod
    def from_settings(cls,settings,catalog,*,ttl_seconds:float=5):
        probes={}
        for descriptor in catalog.payload.operations:
            if descriptor.availability_probe=='image_decompose':
                probes[descriptor.id]=cls.tcp_probe(settings.image_decompose_base_url)
        return cls(probes,ttl_seconds=ttl_seconds)

    @staticmethod
    def _public(value:dict)->dict:
        return {key:value[key] for key in ('available','reason','generation','checked_at')}

    async def _state(self,descriptor_id:str)->dict:
        probe=self.probes.get(descriptor_id)
        if not probe:return {'available':True,'reason':'','generation':0,
            'checked_at':'','checked_monotonic':time.monotonic()}
        lock=self.locks[descriptor_id]
        async with lock:
            current=self.states.get(descriptor_id)
            if current and time.monotonic()-current['checked_monotonic']<self.ttl_seconds:
                return current
            available,reason=await probe();generation=int(current.get('generation',0) if current else 0)
            if current is None or bool(current['available'])!=bool(available):generation+=1
            value={'available':bool(available),'reason':str(reason or ''),'generation':generation,
                'checked_at':datetime.now(timezone.utc).isoformat(),
                'checked_monotonic':time.monotonic()}
            self.states[descriptor_id]=value;return value

    async def snapshot(self)->dict[str,dict]:
        values=await asyncio.gather(*(self._state(identity) for identity in self.probes))
        return {identity:self._public(value) for identity,value in zip(self.probes,values)}

    async def require_available(self,descriptor_id:str)->dict:
        value=self._public(await self._state(descriptor_id))
        if not value['available']:
            raise ValueError('descriptor_unavailable:'+descriptor_id+':'+(
                value['reason'] or 'unavailable'))
        return value
