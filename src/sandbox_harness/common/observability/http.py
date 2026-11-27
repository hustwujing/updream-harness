"""HTTPX trace propagation, lifecycle events and optional diagnostic capture."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

import sandbox_harness.common.observability.observability as obs
from sandbox_harness.common.observability.diagnostics import BodyAccumulator

_BASE_ASYNC_CLIENT=httpx.AsyncClient


class _TeeStream(httpx.AsyncByteStream):
    def __init__(self,stream,accumulator: BodyAccumulator,finish=None):
        self.stream=stream;self.accumulator=accumulator;self.finish=finish;self.finished=False

    async def __aiter__(self):
        try:
            async for chunk in self.stream:
                self.accumulator.update(chunk);yield chunk
        except BaseException as exc:
            await self._finish(exc);raise
        else:await self._finish(None)

    async def _finish(self,exc):
        if self.finished:return
        self.finished=True
        if self.finish is not None:await self.finish(exc)

    async def aclose(self):
        try:
            close=getattr(self.stream,'aclose',None)
            if close is not None:await close()
        finally:await self._finish(None)


def httpx_event_hooks(peer_service: str, *, route: str = "external") -> dict[str,list]:
    """Return hooks that do not read ahead or change request/response byte streams."""
    async def request_hook(request: httpx.Request) -> None:
        diagnostic=obs.diagnostics();limit=diagnostic.max_body_bytes if diagnostic else 0
        request_body=BodyAccumulator(limit)
        try:request_body.update(request.content)
        except httpx.RequestNotRead:request.stream=_TeeStream(request.stream,request_body)
        span=obs._PROVIDER.get_tracer('updream-harness').start_span('HTTP '+request.method,
            kind=SpanKind.CLIENT,attributes=obs.sanitize({'peer_service':peer_service,
                'server.address':request.url.host or 'unknown','http.request.method':request.method,
                'http.route':route}))
        for key,value in obs.carrier(span).items():request.headers[key]=value
        state={'started':time.monotonic(),'span':span,'request_body':request_body,
            'response_body':BodyAccumulator(limit),'diagnostic':diagnostic,'finished':False}
        async def finish(response,stream_error):
            if state['finished']:return
            state['finished']=True;duration=(time.monotonic()-state['started'])*1000
            response_body=state['response_body'];status=response.status_code if response is not None else 0
            attrs={'peer_service':peer_service,'server.address':request.url.host or 'unknown',
                'http.request.method':request.method,'http.route':route,
                'request_bytes':request_body.size,'response_bytes':response_body.size,
                'duration_ms':round(duration,3)}
            if response is not None:attrs['http.response.status_code']=status
            span.set_attributes(obs.sanitize(attrs));failed=stream_error is not None or status>=500
            span.set_attribute('updream.outcome','failed' if failed else 'success')
            if failed:span.set_status(Status(StatusCode.ERROR))
            with trace.use_span(span,end_on_exit=False):
                if diagnostic is not None:
                    try:
                        captured=await asyncio.to_thread(diagnostic.record_http,direction='egress',
                            method=request.method,url=str(request.url),request_headers=request.headers,
                            request_body=request_body,response_headers=response.headers if response else {},
                            response_body=response_body,status_code=status,duration_ms=duration)
                        if captured:obs.event('diagnostic.http.captured',level=logging.DEBUG,
                            capture_mode=diagnostic.mode,capture_session=diagnostic.session,
                            request_bytes=request_body.size,response_bytes=response_body.size,
                            duration_ms=round(duration,3))
                    except Exception as capture_error:
                        obs.event('diagnostic.capture.failed',level=logging.WARNING,exc=capture_error,
                            capture_mode=diagnostic.mode,capture_session=diagnostic.session)
                obs.event('http.client.request.failed' if stream_error is not None else
                    'http.client.request.completed',level=logging.ERROR if stream_error is not None else
                    logging.WARNING if status>=400 else logging.INFO,exc=stream_error,**attrs)
            span.end()
        state['finish']=finish;request.extensions['updream.observation']=state
        with trace.use_span(span,end_on_exit=False):
            obs.event('http.client.request.started',peer_service=peer_service,
                **{'server.address':request.url.host or 'unknown','http.request.method':request.method,
                    'http.route':route})

    async def response_hook(response: httpx.Response) -> None:
        state=response.request.extensions.get('updream.observation')
        if not state:return
        response.stream=_TeeStream(response.stream,state['response_body'],
            lambda stream_error:state['finish'](response,None
                if state.get('terminal_success') and isinstance(stream_error,asyncio.CancelledError)
                else stream_error))

    return {'request':[request_hook],'response':[response_hook]}


class _ObservedAsyncClient(httpx.AsyncClient):
    async def send(self,request,*args,**kwargs):
        try:return await super().send(request,*args,**kwargs)
        except BaseException as exc:
            state=request.extensions.get('updream.observation')
            if state is not None:await state['finish'](None,exc)
            raise


def observed_async_client(peer_service: str, *, telemetry_route: str = "external",
        event_hooks: dict[str,list] | None = None, **kwargs) -> httpx.AsyncClient:
    hooks={key:list(values) for key,values in (event_hooks or {}).items()}
    for key,values in httpx_event_hooks(peer_service,route=telemetry_route).items():
        hooks.setdefault(key,[]).extend(values)
    if httpx.AsyncClient is not _BASE_ASYNC_CLIENT:
        return httpx.AsyncClient(event_hooks=hooks,**kwargs)
    return _ObservedAsyncClient(event_hooks=hooks,**kwargs)
