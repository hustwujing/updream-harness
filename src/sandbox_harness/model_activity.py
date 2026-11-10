"""Payload-free Gateway timing observations for the conversation transcript."""
import asyncio
import json
import logging
import time
import uuid
from .domain import EventKind, RuntimeEvent

logger = logging.getLogger(__name__)

class OutputProbe:
    def __init__(self):
        self.buffer = b''
        self.failed = False
        self.completed = False

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

    async def _observe(self, awaitable):
        try:
            return await asyncio.wait_for(awaitable, timeout=2)
        except Exception:
            logger.warning('Model timing persistence unavailable turn=%s request=%s',
                self.claims.turn_id, self.request_id, exc_info=True)
            return None

    async def start(self):
        event = await self._observe(self.store.persist_runtime_event(
            self.claims.thread_id, self.claims.turn_id,
            RuntimeEvent(kind=EventKind.STEP_STARTED, payload={
                'name': f'等待 {self.claims.model} 响应', 'model': self.claims.model,
                'model_request_id': self.request_id, 'phase': 'waiting',
            }), 'gateway:' + self.request_id))
        if event:
            self.item_id = event.item_id
        self.started = time.monotonic()

    async def observe(self, chunk):
        has_output = self.probe.feed(chunk)
        if self.first_output_ms is None and has_output:
            self.first_output_ms = round((time.monotonic() - self.started) * 1000)
            await self.update('streaming', 'in_progress')

    async def update(self, phase, status, **extra):
        if not self.item_id:
            return
        elapsed = round((time.monotonic() - self.started) * 1000)
        await self._observe(self.store.update_model_activity(self.item_id, phase=phase, status=status,
            payload={'model': self.claims.model, 'model_request_id': self.request_id,
                     'first_output_ms': self.first_output_ms, 'header_ms': self.header_ms,
                     'elapsed_ms': elapsed, **extra}))

    async def finish(self, status_code=None, error_code=''):
        if self.finished:
            return
        self.finished = True
        failed = bool(error_code or self.probe.failed or status_code is None or status_code >= 400)
        await self.update('failed' if failed else 'completed', 'failed' if failed else 'completed',
                          http_status=status_code, error_code=error_code or ('model_response_failed' if self.probe.failed else ''))
