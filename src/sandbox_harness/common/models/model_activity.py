"""Payload-free Gateway timing observations for the conversation transcript."""
import asyncio
from dataclasses import dataclass
import json
import logging
import math
import time
import uuid
import sandbox_harness.common.observability.observability as obs
from opentelemetry import trace
from opentelemetry.trace import SpanKind,Status,StatusCode
from sandbox_harness.common.contracts.domain import (EventKind, RuntimeEvent)

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ModelTokenUsage:
    ordinary_input_tokens:int=0
    cache_read_input_tokens:int=0
    cache_write_input_tokens:int=0
    output_tokens:int=0

    @property
    def input_tokens(self)->int:
        return self.ordinary_input_tokens+self.cache_read_input_tokens+self.cache_write_input_tokens

    def budget_charge(self,cache_read_percent:int,cache_write_percent:int)->int:
        return (self.ordinary_input_tokens+self.output_tokens
            +math.ceil(self.cache_read_input_tokens*cache_read_percent/100)
            +math.ceil(self.cache_write_input_tokens*cache_write_percent/100))


class OutputProbe:
    def __init__(self):
        self.buffer = b''
        self.failed = False
        self.completed = False
        self.input_tokens = None
        self.output_tokens = None
        self.token_usage=ModelTokenUsage()

    @staticmethod
    def _count(value):
        return value if isinstance(value,int) and not isinstance(value,bool) and 0<=value<2**63 else None

    def _usage(self,usage:dict)->None:
        output=self._count(usage.get('output_tokens',usage.get('completion_tokens')))
        if output is not None:
            self.output_tokens=output
            current=self.token_usage
            self.token_usage=ModelTokenUsage(current.ordinary_input_tokens,
                current.cache_read_input_tokens,current.cache_write_input_tokens,output)
        total=self._count(usage.get('input_tokens',usage.get('prompt_tokens')))
        if total is None:return
        read=self._count(usage.get('cache_read_input_tokens'))
        write=self._count(usage.get('cache_creation_input_tokens'))
        if read is not None or write is not None:
            ordinary=total;read=read or 0;write=write or 0;logical=ordinary+read+write
        else:
            details=(usage.get('input_tokens_details') or usage.get('prompt_tokens_details') or {})
            if not isinstance(details,dict):details={}
            read=self._count(details.get('cached_tokens'))
            if read is None:read=self._count(usage.get('prompt_cache_hit_tokens'))
            write=self._count(details.get('cache_write_tokens')) or 0
            miss=self._count(usage.get('prompt_cache_miss_tokens'))
            read=read or 0
            ordinary=(miss if miss is not None else total-read-write)
            if ordinary<0 or ordinary+read+write>total:
                ordinary=total;read=0;write=0
            logical=total
        self.input_tokens=logical
        self.token_usage=ModelTokenUsage(ordinary,read,write,self.output_tokens or 0)

    def feed(self, chunk: bytes) -> bool:
        self.buffer += chunk
        lines = self.buffer.split(b'\n')
        self.buffer = lines.pop()[-262144:]
        output = False
        for line in lines:
            if not line.startswith(b'data:'):
                continue
            try:
                value = json.loads(line[5:])
            except (ValueError, UnicodeError):
                continue
            if not isinstance(value, dict):
                continue
            for candidate in (value,value.get('response'),value.get('message')):
                usage=candidate.get('usage') if isinstance(candidate,dict) else None
                if not isinstance(usage,dict):continue
                self._usage(usage)
            kind = value.get('type', '')
            if not isinstance(kind, str):
                continue
            self.failed |= kind in {'error', 'response.failed', 'response.incomplete'}
            self.completed |= kind in {'response.completed', 'message_stop'}
            if kind in {'response.output_text.delta', 'response.reasoning_summary_text.delta',
                        'response.reasoning_text.delta', 'response.function_call_arguments.delta'}:
                output |= bool(value.get('delta'))
            elif kind == 'content_block_delta':
                delta = value.get('delta') if isinstance(value.get('delta'), dict) else {}
                output |= any(bool(delta.get(key)) for key in ('text', 'thinking', 'partial_json'))
            elif kind == 'response.output_item.added':
                item = value.get('item') if isinstance(value.get('item'), dict) else {}
                output |= item.get('type') == 'function_call'
            elif isinstance(value.get('choices'), list):
                output |= any(bool((choice.get('delta') or {}).get('content') or
                                   (choice.get('delta') or {}).get('tool_calls')) for choice in value['choices'] if isinstance(choice, dict) and isinstance(choice.get('delta'), dict))
        return output

    def finish(self) -> None:
        raw = self.buffer.strip()
        if not raw:
            return
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            return
        if not isinstance(value, dict):
            return
        for candidate in (value,value.get('response'),value.get('message')):
            usage=candidate.get('usage') if isinstance(candidate,dict) else None
            if not isinstance(usage,dict):continue
            self._usage(usage)

class ModelActivity:
    def __init__(self, store, claims):
        self.store = store
        self.claims = claims
        self.request_id = uuid.uuid4().hex
        self.item_id = None
        self.started = time.monotonic()
        self.first_output_ms = None
        self.header_ms = None
        self.probe = OutputProbe()
        self.finished = False
        self.span = None

    async def _observe(self, awaitable):
        try:
            return await asyncio.wait_for(awaitable, timeout=2)
        except Exception:
            logger.warning('Model timing persistence unavailable turn=%s request=%s',
                self.claims.turn_id, self.request_id, exc_info=True)
            return None

    async def start(self):
        if not getattr(self.claims,'job_id',''):
            event = await self._observe(self.store.persist_runtime_event(
                self.claims.thread_id, self.claims.turn_id,
                RuntimeEvent(kind=EventKind.STEP_STARTED, payload={
                    'name': f'等待 {self.claims.model} 响应', 'model': self.claims.model,
                    'model_request_id': self.request_id, 'phase': 'waiting', 'job_id': '',
                }), 'gateway:' + self.request_id))
            if event:
                self.item_id = event.item_id
        self.started = time.monotonic()
        self.span=obs._PROVIDER.get_tracer('updream-harness').start_span('model.request',kind=SpanKind.CLIENT,
            attributes=obs.sanitize({'turn_id':self.claims.turn_id,'thread_id':self.claims.thread_id,
                'sandbox_id':getattr(self.claims,'sandbox_id',''),'model':self.claims.model,'model_request_id':self.request_id}))
        with trace.use_span(self.span,end_on_exit=False):
            obs.event('model.request.started',turn_id=self.claims.turn_id,model=self.claims.model,model_request_id=self.request_id)

    async def observe(self, chunk):
        has_output = self.probe.feed(chunk)
        if self.first_output_ms is None and has_output:
            self.first_output_ms = round((time.monotonic() - self.started) * 1000)
            if self.span:
                with trace.use_span(self.span,end_on_exit=False):
                    obs.event('model.first_output',turn_id=self.claims.turn_id,model=self.claims.model,model_request_id=self.request_id,first_output_ms=self.first_output_ms)
                    harness=getattr(self.claims,'harness','unknown')
                    obs.business_metric('updream.turn.first_output.duration',self.first_output_ms,
                        harness=getattr(harness,'value',harness) or 'unknown')
            await self.update('streaming', 'in_progress')

    async def update(self, phase, status, **extra):
        if not self.item_id:
            return
        elapsed = round((time.monotonic() - self.started) * 1000)
        await self._observe(self.store.update_model_activity(self.item_id, phase=phase, status=status,
            payload={'model': self.claims.model, 'model_request_id': self.request_id,
                     'job_id':getattr(self.claims,'job_id',''),
                     'first_output_ms': self.first_output_ms, 'header_ms': self.header_ms,
                     'elapsed_ms': elapsed, **extra}))

    async def finish(self, status_code=None, error_code=''):
        if self.finished:
            return
        self.finished = True
        self.probe.finish()
        usage=self.probe.token_usage
        failed = bool(error_code or self.probe.failed or status_code is None or status_code >= 400)
        if self.span:
            self.span.set_attributes(obs.sanitize({'duration_ms':round((time.monotonic()-self.started)*1000,3),'header_ms':self.header_ms,'first_output_ms':self.first_output_ms,'outcome':'failed' if failed else 'success'}))
            if failed:self.span.set_status(Status(StatusCode.ERROR))
            with trace.use_span(self.span,end_on_exit=False):
                obs.event('model.request.finished',level=logging.ERROR if failed else logging.INFO,turn_id=self.claims.turn_id,model=self.claims.model,model_request_id=self.request_id,input_tokens=self.probe.input_tokens,ordinary_input_tokens=usage.ordinary_input_tokens,cache_read_input_tokens=usage.cache_read_input_tokens,cache_write_input_tokens=usage.cache_write_input_tokens,output_tokens=self.probe.output_tokens,usage_source='provider_reported' if self.probe.input_tokens is not None or self.probe.output_tokens is not None else 'unavailable',header_ms=self.header_ms,first_output_ms=self.first_output_ms,duration_ms=round((time.monotonic()-self.started)*1000),outcome='failed' if failed else 'success',error_code=error_code,failure_origin=obs.failure_origin(error_code or ('model_response_failed' if self.probe.failed else ''),status_code) if failed else None,**{'http.response.status_code':status_code})
            self.span.end()
        await self.update('failed' if failed else 'completed', 'failed' if failed else 'completed',
                          http_status=status_code,error_code=error_code or ('model_response_failed' if self.probe.failed else ''),
                          input_tokens=self.probe.input_tokens,ordinary_input_tokens=usage.ordinary_input_tokens,
                          cache_read_input_tokens=usage.cache_read_input_tokens,
                          cache_write_input_tokens=usage.cache_write_input_tokens,
                          output_tokens=self.probe.output_tokens)
