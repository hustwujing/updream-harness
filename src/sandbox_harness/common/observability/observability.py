"""Payload-free structured logs, OTel context and bounded asynchronous persistence."""
from __future__ import annotations
import asyncio
import atexit
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import functools
import inspect
import json
import logging
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
import traceback
import uuid
from string import Formatter

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.trace import TracerProvider, SpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk._logs import LoggerProvider, LogRecordProcessor, ReadableLogRecord
from opentelemetry._logs import LogRecord, SeverityNumber

from sandbox_harness.common.observability.schema import (
    BUSINESS_FIELDS, STANDARD_FIELDS, missing_required_fields, reportable_unknown_fields,
    sanitize_fields, unknown_fields,
)
from sandbox_harness.common.observability.diagnostics import DiagnosticCapture, resolve_diagnostic_root

_CONTEXT = ContextVar('updream_log_context', default={})
_PROPAGATOR = TraceContextTextMapPropagator()
_PROVIDER = TracerProvider(shutdown_on_exit=False)
_TELEMETRY = None
_DIAGNOSTICS = None
_SCHEMA_STRICT = False
# Compatibility exports remain the source for decorators and authenticated ingest.
BUSINESS = BUSINESS_FIELDS
STANDARD = STANDARD_FIELDS
EVENT = re.compile(r'^[a-z][a-z0-9_.-]{0,119}$')

DESCRIPTIONS = {
    'turn.accepted':'Turn accepted; duplicate submissions reuse the existing Turn',
    'turn.lease.claimed':'Broker acquired the fenced processing lease',
    'turn.lease.lost':'Broker no longer owns the processing lease',
    'turn.state.changed':'Committed Turn state transition',
    'turn.runtime.reattached':'Recovery reattached the existing Runtime without replaying input',
    'input.delivery.unknown':'Input receipt was lost; automatic resubmission is suppressed',
    'model.first_output':'First effective model content received',
    'model.request.finished':'Model stream ended; outcome is recorded separately from HTTP status',
    'workspace.checkpoint.persisted':'Checkpoint bytes and database reference persisted',
    'artifact.persisted':'Artifact object and metadata persisted',
    'runtime.logs.collected':'Sandbox diagnostics collected before cleanup',
    'runtime.logs.collection_failed':'Sandbox diagnostics could not be collected within the bounded deadline',
    'capability.revoked':'Capability revoked and active connections closed',
}

def describe(name):return DESCRIPTIONS.get(name,name.replace('.',' '))

def sanitize(fields, *, strict=False):
    return sanitize_fields(fields,strict=strict)

@contextmanager
def bind(**fields):
    token = _CONTEXT.set({**_CONTEXT.get(), **sanitize(fields)})
    try: yield
    finally: _CONTEXT.reset(token)

def enrich(**fields):
    _CONTEXT.set({**_CONTEXT.get(),**sanitize(fields)})
    trace.get_current_span().set_attributes(sanitize(fields))

def carrier(span=None):
    headers = {}; _PROPAGATOR.inject(headers,context=trace.set_span_in_context(span) if span else None); return headers

def remote_context(headers):
    # The W3C propagator validates IDs/flags. Baggage is deliberately not extracted.
    return _PROPAGATOR.extract(headers)

def link_to(header):
    value = trace.get_current_span(remote_context({'traceparent': header or ''})).get_span_context()
    return Link(value) if value.is_valid else None

def add_link(header):
    link = link_to(header)
    span=trace.get_current_span()
    if link and not any(item.context==link.context for item in getattr(span,"links",[])):span.add_link(link.context)

def failure_origin(code, status=None):
    if code in {"cancelled","CancelledError"}:return "user"
    if code in {"model_request_too_large","model_request_body_timeout"}:return "infrastructure"
    if code=="model_request_invalid":return "program"
    if status and status>=400 or code in {"model_service_overloaded","model_rate_limited"}:return "provider"
    if code=="model_response_failed":return "model"
    if code in {"AttributeError","NameError","AssertionError","TypeError"}:return "program"
    if code in {"RuntimeStreamInterrupted","ConnectError","ReadTimeout","ConnectTimeout","OSError","ConnectionError"}:return "infrastructure"
    return "unknown"

def safe_exception(exc):
    frames = traceback.extract_tb(exc.__traceback__)[-20:]
    return {'exception.type': type(exc).__name__, 'exception.stacktrace': ' | '.join(
        f'{Path(f.filename).name}:{f.lineno}:{f.name}' for f in frames)}

def event(name, *, level=logging.INFO, exc=None, **fields):
    if not EVENT.fullmatch(name): name = 'telemetry.invalid_event_name'
    unknown=unknown_fields(fields)
    missing=missing_required_fields(name,{**_CONTEXT.get(),**fields})
    if unknown or missing:
        if _SCHEMA_STRICT or os.getenv('SANDBOX_HARNESS_LOG_SCHEMA_STRICT','').lower() in {'1','true','yes','on'}:
            if unknown:sanitize(fields,strict=True)
            raise ValueError('missing_telemetry_fields:'+name+':'+','.join(missing))
        if _TELEMETRY is not None:_TELEMETRY.increment('schema_violations')
        violation=sanitize({'schema_event_name':name,'violation_kind':
            'unknown_and_missing' if unknown and missing else 'unknown' if unknown else 'missing',
            'unknown_fields':reportable_unknown_fields(unknown),'unknown_field_count':len(unknown),
            'missing_fields':','.join(missing),'missing_field_count':len(missing)})
        logging.getLogger('updream.events').log(logging.WARNING,'telemetry.schema_violation',
            extra={'event_name':'telemetry.schema_violation','event_attributes':violation},stacklevel=2)
    attrs = {**_CONTEXT.get(), **sanitize(fields)}
    if exc is not None:
        attrs.update(safe_exception(exc));attrs.setdefault("updream.failure_origin",failure_origin(type(exc).__name__))
    logging.getLogger('updream.events').log(level, name, extra={'event_name':name,'event_attributes':attrs},stacklevel=2)

@contextmanager
def operation(name, *, parent=None, new_trace=False, kind=SpanKind.INTERNAL, **fields):
    links = []
    if new_trace:
        previous = trace.get_current_span().get_span_context()
        if previous.is_valid: links.append(Link(previous))
        parent = Context()
    with bind(**fields), _PROVIDER.get_tracer('updream-harness').start_as_current_span(
        name, context=parent, links=links, kind=kind, attributes=sanitize({**_CONTEXT.get(),**fields}),
        record_exception=False, set_status_on_exception=False) as span:
        start = time.monotonic()
        event(name+'.started')
        try:
            yield span
        except BaseException as exc:
            cancelled = isinstance(exc, asyncio.CancelledError)
            if not cancelled: span.set_status(Status(StatusCode.ERROR))
            span.set_attribute('updream.outcome', 'cancelled' if cancelled else 'failed')
            span.set_attribute('updream.duration_ms',round((time.monotonic()-start)*1000,3))
            event(name+'.cancelled' if cancelled else name+'.failed',
                level=logging.INFO if cancelled else logging.ERROR, exc=exc,
                duration_ms=round((time.monotonic()-start)*1000,3))
            raise
        else:
            span.set_attribute('updream.duration_ms',round((time.monotonic()-start)*1000,3))
            event(name+'.completed', duration_ms=round((time.monotonic()-start)*1000,3))

def observed(name, *, new_trace=False):
    def decorate(fn):
        signature = inspect.signature(fn)
        @functools.wraps(fn)
        async def wrapped(*args, **kwargs):
            values = signature.bind(*args, **kwargs).arguments
            fields = {k:v for k,v in values.items() if k in BUSINESS}
            obj = values.get('sandbox') or values.get('turn') or values.get('execution')
            value=values.get('value')
            if getattr(value,'turn_id',None):fields.update(turn_id=value.turn_id,input_id=getattr(value,'id',''))
            if obj is not None:
                for key in ('thread_id','turn_id','harness','model'):
                    if getattr(obj,key,None): fields[key]=getattr(obj,key)
                if hasattr(obj,'runtime_url'): fields['sandbox_id']=obj.id
                elif hasattr(obj,'thread_id'): fields['turn_id']=obj.id
                elif hasattr(obj,'process'):fields['execution_id']=obj.id
            metadata = values.get('metadata') or {}
            for key in ('thread','turn'):
                if metadata.get('sandbox-harness.io/'+key): fields[key+'_id']=metadata['sandbox-harness.io/'+key]
            if getattr(values.get('self'),'instance_id',None): fields['broker_id']=values['self'].instance_id
            with operation(name,new_trace=new_trace,**fields): return await fn(*args,**kwargs)
        wrapped.__signature__=inspect.signature(fn,eval_str=True)
        return wrapped
    return decorate

class ToolObservations:
    """Source-side timing survives stream replay; holds only bounded active spans."""
    def __init__(self):self.active={}
    def observe(self,kind,payload,execution_id):
        tool_id=payload.get('tool_use_id')
        if not isinstance(tool_id,str) or not tool_id:
            if kind in {'tool_started','tool_finished'}:event('runtime.tool.unidentified',execution_id=execution_id,reason='missing_native_tool_id',state=kind)
            return
        if kind=='tool_started':
            if tool_id in self.active:self._finish(tool_id,'interrupted')
            if len(self.active)>=512:
                event('runtime.tool.tracking_limit',level=logging.WARNING,execution_id=execution_id);return
            name=payload.get('name','tool')
            if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}',name):name='tool'
            attrs={**_CONTEXT.get(),**sanitize({'execution_id':execution_id,'tool_id':tool_id,'tool_name':name})}
            span=_PROVIDER.get_tracer('updream-harness').start_span('harness.tool',attributes=attrs)
            self.active[tool_id]=(span,time.monotonic(),execution_id,name)
            with trace.use_span(span,end_on_exit=False):event('runtime.tool.started',execution_id=execution_id,tool_id=tool_id,tool_name=name)
        elif kind=='tool_finished':self._finish(tool_id,'failed' if payload.get('is_error') else 'success')
    def _finish(self,tool_id,outcome):
        value=self.active.pop(tool_id,None)
        if not value:return
        span,start,execution_id,name=value
        span.set_attribute('updream.outcome',outcome)
        span.set_attribute('updream.duration_ms',round((time.monotonic()-start)*1000,3))
        if outcome=='failed':span.set_status(Status(StatusCode.ERROR))
        with trace.use_span(span,end_on_exit=False):event('runtime.tool.finished',execution_id=execution_id,
            tool_id=tool_id,tool_name=name,outcome=outcome,duration_ms=round((time.monotonic()-start)*1000,3))
        span.end()
    def close(self):
        for tool_id in list(self.active):self._finish(tool_id,'interrupted')

def observed_sync(name):
    def decorate(fn):
        signature=inspect.signature(fn)
        @functools.wraps(fn)
        def wrapped(*args,**kwargs):
            values=signature.bind(*args,**kwargs).arguments
            fields={}
            if isinstance(values.get('data'),bytes):fields['bytes']=len(values['data'])
            reference=values.get('reference','')
            if re.fullmatch(r'sha256:[0-9a-f]{64}',reference):fields['object_digest']=reference[7:]
            with operation(name,**fields):return fn(*args,**kwargs)
        return wrapped
    return decorate

def resolve_log_storage(default_root, *, data_root=None, configured_dir=None,
        filename_template='{service}-{instance}.telemetry.jsonl'):
    base=Path(data_root or Path(default_root).parent).resolve()
    configured=Path(configured_dir) if configured_dir else Path(default_root)
    root=(configured if configured.is_absolute() else base/configured).resolve()
    try:root.relative_to(base)
    except ValueError as exc:raise ValueError('log_directory_outside_data_root') from exc
    template=filename_template or '{service}-{instance}.telemetry.jsonl'
    fields=[]
    try:
        for _,field,_,_ in Formatter().parse(template):
            if field:fields.append(field)
        rendered=template.format(service='service',instance='instance')
    except (KeyError,ValueError,IndexError) as exc:
        raise ValueError('invalid_log_filename_template') from exc
    if (set(fields)-{'service','instance'} or '/' in template or '\\' in template
            or '..' in template or not re.fullmatch(r'[A-Za-z0-9_.{}-]{1,180}',template)
            or not re.fullmatch(r'[A-Za-z0-9_.-]{1,220}\.jsonl',rendered)):
        raise ValueError('invalid_log_filename_template')
    return root,template


class JsonSink:
    """One namespace-wide cap, protected across API/Gateway processes with flock."""
    def __init__(self, root, service, instance, *, max_bytes=2*1024**3, days=7,
            segment_bytes=8*1024**2,segment_max_age_seconds=86400,
            filename_template='{service}-{instance}.telemetry.jsonl'):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.path=self.root/filename_template.format(service=service,instance=instance)
        self.max_bytes=max_bytes;self.days=days;self.segment_bytes=min(segment_bytes,max_bytes)
        self.segment_max_age_seconds=segment_max_age_seconds
        self.started=time.time()
    def write(self, rows):
        import fcntl
        missing=not self.root.exists()
        self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        if self.root.is_symlink() or not self.root.is_dir():raise ValueError('invalid_telemetry_directory')
        data=''.join(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n' for row in rows).encode()
        if len(data)>self.max_bytes: raise ValueError('telemetry_batch_exceeds_disk_budget')
        with (self.root/'.retention.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if self.path.exists() and (self.path.stat().st_size+len(data)>self.segment_bytes or time.time()-self.started>self.segment_max_age_seconds):
                self.path.rename(self.root/f'{self.path.stem}-{time.time_ns()}.telemetry.jsonl');self.started=time.time()
            files=sorted(self.root.glob('*.telemetry.jsonl'),key=lambda p:p.stat().st_mtime)
            total=sum(p.stat().st_size for p in files)
            for path in files:
                info=path.stat()
                if info.st_mtime<time.time()-self.days*86400 or total+len(data)>self.max_bytes:
                    total-=info.st_size;path.unlink(missing_ok=True)
            fd=os.open(self.path,os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'ab') as stream: stream.write(data)
        return missing

class Processor(SpanProcessor,LogRecordProcessor):
    def __init__(self, telemetry): self.telemetry=telemetry
    def on_start(self, span, parent_context=None): pass
    def on_end(self, span): self.telemetry.put('span',span)
    def on_emit(self, record):
        self.telemetry.put('log',ReadableLogRecord(record.log_record,record.resource,record.instrumentation_scope))
    def shutdown(self): pass
    def force_flush(self,timeout_millis=30000):return self.telemetry.flush(timeout_millis/1000)

class StructuredHandler(logging.Handler):
    def __init__(self, telemetry):super().__init__();self.telemetry=telemetry
    def emit(self,record):
        # Never format unknown library messages or exception strings: these often contain URLs/credentials.
        if record.name.startswith('opentelemetry') or record.name.startswith(('urllib3','httpx','httpcore')):
            if record.levelno>=logging.WARNING:self.telemetry.increment('library_warnings')
            return
        name=getattr(record,'event_name','diagnostic.'+record.name.replace('_','-').lower())
        if not EVENT.fullmatch(name):name='diagnostic.library'
        attributes=sanitize(getattr(record,'event_attributes',_CONTEXT.get()))
        attributes.update({'code.function.name':record.funcName,'code.file.path':Path(record.pathname).name,'code.line.number':record.lineno})
        if record.exc_info and record.exc_info[1]:attributes.update(safe_exception(record.exc_info[1]))
        severity=SeverityNumber.ERROR if record.levelno>=40 else SeverityNumber.WARN if record.levelno>=30 else SeverityNumber.INFO if record.levelno>=20 else SeverityNumber.DEBUG
        self.telemetry.logger.emit(LogRecord(timestamp=int(record.created*1e9),severity_text=record.levelname,
            severity_number=severity,body=describe(name),event_name=name,attributes=attributes))


class ConsoleJsonHandler(logging.Handler):
    """Provider-neutral stdout events for sandbox platform log collectors."""
    def __init__(self,service,stream=None):
        super().__init__();self.service=service;self.stream=stream or sys.stdout
    def emit(self,record):
        try:
            name=getattr(record,'event_name','diagnostic.'+record.name.replace('_','-').lower())
            if not EVENT.fullmatch(name):name='diagnostic.library'
            attributes=sanitize(getattr(record,'event_attributes',_CONTEXT.get()))
            identities={key:attributes.get('updream.'+key,'')
                for key in ('sandbox_id','thread_id','turn_id','job_id','execution_id')}
            row={'schema_version':1,
                'timestamp':datetime.fromtimestamp(record.created,timezone.utc).isoformat(),
                'level':record.levelname,'event':name,'service':self.service,**identities}
            reserved=set(row)
            row.update({key.removeprefix('updream.'):value for key,value in attributes.items()
                if key.removeprefix('updream.') not in reserved})
            self.stream.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
            self.stream.flush()
        except Exception:
            # Observability must never make the Runtime unavailable.
            return


def runtime_console_handler(service,stream=None):
    return ConsoleJsonHandler(service,stream) if service in {'runtime','runtime-bootstrap'} else None

class Telemetry:
    def __init__(self,service,root,*,queue_size=8192,max_bytes=2*1024**3,days=7,release='unknown',
            segment_bytes=8*1024**2,segment_max_age_seconds=86400,ring_capacity=8192,
            filename_template='{service}-{instance}.telemetry.jsonl',otlp_timeout_seconds=2):
        if not 1<=queue_size<=65536 or max_bytes<1024 or not 1<=days<=3650:raise ValueError('invalid_telemetry_limits')
        self.resource=Resource.create({'service.namespace':'updream-harness','service.name':service,
            'service.instance.id':uuid.uuid4().hex,'service.version':release,
            'deployment.environment.name':os.getenv('SANDBOX_HARNESS_ENVIRONMENT','local')})
        self.instance=self.resource.attributes['service.instance.id'];self.service=service
        self.sink=JsonSink(root,service,self.instance,max_bytes=max_bytes,days=days,
            segment_bytes=segment_bytes,segment_max_age_seconds=segment_max_age_seconds,
            filename_template=filename_template)
        self.queue=queue.Queue(queue_size);self.stats={'dropped_records':0,'disk_errors':0,'export_failures':0,'library_warnings':0,'schema_violations':0,'storage_recoveries':0}
        self.guard=threading.Lock();self.ring=deque(maxlen=ring_capacity);self.sequence=0;self.stopping=False;self.last_prune=time.monotonic()
        self.otlp_timeout_seconds=otlp_timeout_seconds
        self.remote=None;self.remote_guard=threading.Lock()
        self.provider=TracerProvider(resource=self.resource,shutdown_on_exit=False)
        self.logs=LoggerProvider(resource=self.resource,shutdown_on_exit=False)
        processor=Processor(self);self.provider.add_span_processor(processor);self.logs.add_log_record_processor(processor)
        self.logger=self.logs.get_logger('updream-harness');self.handler=StructuredHandler(self)
        self.console_handler=None
        self.worker=threading.Thread(target=self._work,name='telemetry-writer',daemon=True);self.worker.start()
    def increment(self,key,count=1):
        with self.guard:self.stats[key]=self.stats.get(key,0)+count
    def put(self,kind,value):
        if self.stopping:self.increment('dropped_records');return False
        try:self.queue.put_nowait((kind,value));return True
        except queue.Full:self.increment('dropped_records');return False
    def status(self):
        with self.guard:return {**self.stats,'service':self.service,'instance_id':self.instance,'version':self.resource.attributes['service.version'],'queue_depth':self.queue.qsize(),'queue_capacity':self.queue.maxsize,'writer_alive':self.worker.is_alive()}
    def set_remote(self,endpoint,headers=None):
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        self.flush(3)
        import requests
        def session():
            value=requests.Session();value.trust_env=False
            def status(response,*args,**kwargs):
                with self.guard:self.stats["last_export_http_status"]=response.status_code
            value.hooks["response"].append(status)
            return value
        with self.remote_guard:
            if self.remote:
                for exporter in self.remote:exporter.shutdown()
            self.remote=(OTLPLogExporter(endpoint=endpoint.rstrip('/')+'/logs',headers=headers or {},timeout=self.otlp_timeout_seconds,session=session(),compression=Compression.NoCompression),
                OTLPSpanExporter(endpoint=endpoint.rstrip('/')+'/traces',headers=headers or {},timeout=self.otlp_timeout_seconds,session=session(),compression=Compression.NoCompression)) if endpoint else None
    def _row(self,kind,value):
        if kind=='import':return value
        resource={k:v for k,v in dict(value.resource.attributes).items() if k.startswith(('service.','deployment.','telemetry.sdk.'))}
        if kind=='log':
            log=value.log_record;attrs=sanitize(dict(log.attributes or {}))
            return {'schema_version':1,'signal':'log','timestamp':datetime.fromtimestamp((log.timestamp or time.time_ns())/1e9,timezone.utc).isoformat(),
                'severity_text':log.severity_text,'severity_number':log.severity_number.value if log.severity_number else 0,'observed_timestamp':datetime.fromtimestamp((log.observed_timestamp or time.time_ns())/1e9,timezone.utc).isoformat(),'event_name':log.event_name,'body':log.body,
                'trace_flags':f'{int(log.trace_flags or 0):02x}','trace_id':f'{log.trace_id:032x}' if log.trace_id else None,'span_id':f'{log.span_id:016x}' if log.span_id else None,
                'resource':resource,'attributes':attrs}
        span=value;context=span.get_span_context()
        return {'schema_version':1,'signal':'span','timestamp':datetime.fromtimestamp(span.end_time/1e9,timezone.utc).isoformat(),
            'event_name':span.name,'trace_id':f'{context.trace_id:032x}','span_id':f'{context.span_id:016x}',
            'parent_span_id':f'{span.parent.span_id:016x}' if span.parent else None,
            'links':[{'trace_id':f'{link.context.trace_id:032x}','span_id':f'{link.context.span_id:016x}'} for link in span.links],
            'duration_ms':(span.attributes or {}).get('updream.duration_ms',round((span.end_time-span.start_time)/1e6,3)),'status':span.status.status_code.name,
            'resource':resource,'attributes':sanitize(dict(span.attributes or {}))}
    def _work(self):
        last_dropped=0
        while not self.stopping or not self.queue.empty():
            try:batch=[self.queue.get(timeout=.2)]
            except queue.Empty:
                if time.monotonic()-self.last_prune>=60:
                    try:self.sink.write([])
                    except Exception:self.increment("disk_errors")
                    self.last_prune=time.monotonic()
                continue
            deadline=time.monotonic()+.2
            while len(batch)<128:
                try:batch.append(self.queue.get(timeout=max(0,deadline-time.monotonic())))
                except queue.Empty:break
            rows=[]
            try:
                for kind,value in batch:
                    with self.guard:
                        self.sequence+=1;sequence=self.sequence
                    identity=self.instance+':'+str(sequence)
                    if kind=='log':
                        value.log_record.attributes={**dict(value.log_record.attributes or {}),
                            'updream.source_event_id':(value.log_record.attributes or {}).get('updream.source_event_id',identity)}
                    row={**self._row(kind,value),'event_id':identity,'sequence':sequence}
                    with self.guard:self.ring.append(row)
                    rows.append(row)
                dropped=self.status()['dropped_records']
                if dropped>last_dropped:
                    rows.append({'schema_version':1,'signal':'log','timestamp':datetime.now(timezone.utc).isoformat(),
                        'event_name':'telemetry.records.dropped','severity_text':'WARNING','resource':dict(self.resource.attributes),
                        'attributes':{'updream.dropped_count':dropped-last_dropped}});last_dropped=dropped
                try:
                    if self.sink.write(rows):self.increment('storage_recoveries')
                except Exception:self.increment('disk_errors')
                with self.remote_guard:
                    if self.remote:
                        for kind,exporter in zip(('log','span'),self.remote):
                            values=[value for tag,value in batch if tag==kind]
                            if values:
                                try:
                                    if exporter.export(values).value!=0:self.increment('export_failures')
                                except Exception:self.increment('export_failures')
            except Exception:self.increment('disk_errors')
            finally:
                for _ in batch:self.queue.task_done()
    def snapshot(self,after=0,limit=500):
        with self.guard:
            first=self.ring[0]['sequence'] if self.ring else self.sequence+1
            return {'instance_id':self.instance,'retained_from':first,'last_sequence':self.sequence,
                'records':[dict(row) for row in self.ring if row['sequence']>after][:limit],
                'stats':dict(self.stats)}
    def flush(self,timeout=3):
        end=time.monotonic()+timeout
        while self.queue.unfinished_tasks and time.monotonic()<end:time.sleep(.01)
        return not self.queue.unfinished_tasks
    def close(self):
        if self.stopping:return
        self.flush(5);self.stopping=True;self.worker.join(5)
        logging.getLogger().removeHandler(self.handler)
        if self.console_handler:logging.getLogger().removeHandler(self.console_handler)
        self.logs.shutdown();self.provider.shutdown()


def export_headers():
    from opentelemetry.util.re import parse_env_headers
    return parse_env_headers(os.getenv("OTEL_EXPORTER_OTLP_HEADERS",""),liberal=True)

def telemetry_configuration(service,root,settings=None):
    data_root=Path(getattr(settings,'data_root',Path(root).parent))
    configured_dir=getattr(settings,'log_dir',os.getenv('SANDBOX_HARNESS_LOG_DIR',''))
    filename_template=getattr(settings,'log_file_template',os.getenv(
        'SANDBOX_HARNESS_LOG_FILE_TEMPLATE','{service}-{instance}.telemetry.jsonl'))
    log_root,filename_template=resolve_log_storage(root,data_root=data_root,
        configured_dir=configured_dir,filename_template=filename_template)
    get=lambda name,default:getattr(settings,name,default)
    return {
        'root':log_root,'filename_template':filename_template,
        'queue_size':get('log_queue_size',int(os.getenv('SANDBOX_HARNESS_LOG_QUEUE_SIZE','8192'))),
        'ring_capacity':get('log_ring_capacity',8192),
        'max_bytes':(get('runtime_log_max_bytes',16*1024**2) if service=='runtime' else
            get('log_max_bytes',int(os.getenv('SANDBOX_HARNESS_LOG_MAX_BYTES',str(2*1024**3))))),
        'segment_bytes':get('log_segment_bytes',8*1024**2),
        'segment_max_age_seconds':get('log_segment_max_age_seconds',86400),
        'days':get('log_retention_days',int(os.getenv('SANDBOX_HARNESS_LOG_RETENTION_DAYS','7'))),
        'otlp_timeout_seconds':get('log_otlp_timeout_seconds',2),
        'level':getattr(settings,'log_level',os.getenv('SANDBOX_HARNESS_LOG_LEVEL','INFO')).upper(),
        'schema_strict':get('log_schema_strict',False),
    }

def configure(service,root,settings=None):
    global _PROVIDER,_TELEMETRY,_DIAGNOSTICS,_SCHEMA_STRICT
    if _TELEMETRY:return _TELEMETRY
    release=os.getenv('SANDBOX_HARNESS_RELEASE','unknown')
    if release=='unknown':
        import subprocess
        try:release=subprocess.check_output(['git','rev-parse','--short=12','HEAD'],stderr=subprocess.DEVNULL,timeout=2,text=True).strip()
        except Exception:pass
    config=telemetry_configuration(service,root,settings);_SCHEMA_STRICT=config['schema_strict']
    _TELEMETRY=Telemetry(service,config['root'],queue_size=config['queue_size'],
        max_bytes=config['max_bytes'],days=config['days'],release=release,
        segment_bytes=config['segment_bytes'],segment_max_age_seconds=config['segment_max_age_seconds'],
        ring_capacity=config['ring_capacity'],filename_template=config['filename_template'],
        otlp_timeout_seconds=config['otlp_timeout_seconds'])
    if settings is not None:
        _DIAGNOSTICS=DiagnosticCapture.from_settings(settings,service,_TELEMETRY.instance)
    else:
        mode=os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE','off')
        diagnostic_root=resolve_diagnostic_root(Path(root).parent,
            os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_DIR','diagnostics'))
        _DIAGNOSTICS=DiagnosticCapture(diagnostic_root,service,_TELEMETRY.instance,mode=mode,
            log_level=config['level'],session=os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE_SESSION',''),
            trace_id=os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_TRACE_ID',''),
            filename_template=os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_FILE_TEMPLATE',
                '{service}-{instance}-{session}.diagnostic.jsonl'),
            max_body_bytes=int(os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_MAX_BODY_BYTES',str(256*1024))),
            max_bytes=int(os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_MAX_BYTES',str(256*1024**2))),
            ttl_hours=int(os.getenv('SANDBOX_HARNESS_DIAGNOSTIC_TTL_HOURS','24')))
    _PROVIDER=_TELEMETRY.provider
    root_logger=logging.getLogger();handlers=[_TELEMETRY.handler]
    _TELEMETRY.console_handler=runtime_console_handler(service)
    if _TELEMETRY.console_handler:handlers.append(_TELEMETRY.console_handler)
    root_logger.handlers=handlers
    root_logger.setLevel(config['level'])
    for name in ('uvicorn','uvicorn.error','uvicorn.access'):
        log=logging.getLogger(name);log.handlers=[];log.propagate=True
    endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT','')
    if endpoint:
        headers=export_headers()
        _TELEMETRY.set_remote(endpoint.rstrip('/')+'/v1',headers)
    import sys
    def exception_hook(kind,exc,tb):
        event('service.crashed',level=logging.CRITICAL,exc=exc);_TELEMETRY.flush(3)
    sys.excepthook=exception_hook
    logging.captureWarnings(True)
    atexit.register(_TELEMETRY.close);event('service.started')
    if _DIAGNOSTICS.mode!='off':event('diagnostic.capture.configured',level=logging.WARNING,
        capture_mode=_DIAGNOSTICS.mode,capture_session=_DIAGNOSTICS.session)
    return _TELEMETRY

def telemetry():return _TELEMETRY
def diagnostics():
    return _DIAGNOSTICS if _DIAGNOSTICS is not None and _DIAGNOSTICS.mode!='off' else None

class RequestLoggingMiddleware:
    def __init__(self,app,service):self.app=app;self.service=service
    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or scope.get('path','').startswith('/v1/telemetry/') or scope.get('path')=='/v1/diagnostics':return await self.app(scope,receive,send)
        headers={k.decode().lower():v.decode() for k,v in scope.get('headers',[]) if k.lower() in {b'traceparent',b'tracestate',b'x-updream-ingress'}}
        request_id=uuid.uuid4().hex;status=0;response_bytes=0;started=time.monotonic();response_headers=[]
        diagnostic=diagnostics();request_body=diagnostic.accumulator() if diagnostic else None
        response_body=diagnostic.accumulator() if diagnostic else None
        quiet=scope['method']=='GET' and not scope['path'].endswith('/events')
        ctx=scope.setdefault('state',{}).setdefault('log_context',{})
        if headers.get('x-updream-ingress') in {'direct','frpc'}:ctx['ingress']=headers['x-updream-ingress']
        if self.service=='runtime':ctx.update(getattr(getattr(scope.get('app'),'state',None),'log_context',{}))
        with bind(request_id=request_id,**ctx),_PROVIDER.get_tracer('updream-harness').start_as_current_span(
            'HTTP '+scope['method'],context=remote_context(headers),kind=SpanKind.SERVER,
            record_exception=False,set_status_on_exception=False) as span:
            async def logged_receive():
                message=await receive()
                if request_body is not None and message.get('type')=='http.request':request_body.update(message.get('body',b''))
                return message
            async def logged_send(message):
                nonlocal status,response_bytes,response_headers
                if message["type"]=="http.response.body":
                    body=message.get("body",b"");response_bytes+=len(body)
                    if response_body is not None:response_body.update(body)
                if message['type']=='http.response.start':
                    status=message['status'];response_headers=list(message.get('headers',[]));message={**message,'headers':[(key,value) for key,value in response_headers if key.lower() not in {b'x-request-id',b'x-trace-id'}]+[
                        (b'x-request-id',request_id.encode()),(b'x-trace-id',f'{span.get_span_context().trace_id:032x}'.encode())]}
                await send(message)
            try:await self.app(scope,logged_receive,logged_send)
            except BaseException as exc:
                cancelled=isinstance(exc,asyncio.CancelledError)
                if not cancelled:span.set_status(Status(StatusCode.ERROR))
                event('http.request.disconnected' if cancelled else 'http.request.failed',level=logging.INFO if cancelled else logging.ERROR,exc=exc,**ctx);raise
            finally:
                route=getattr(scope.get('route'),'path','unmatched')
                ctx.update({key:value for key,value in scope.get('path_params',{}).items() if key in BUSINESS and isinstance(value,str) and re.fullmatch(r'(?:[0-9a-f]{32})(?::[0-9a-f]{32})?|runtime-[0-9a-f]{64}|[0-9a-f-]{36}',value)})
                attrs={**ctx,'response_bytes':response_bytes,'http.request.method':scope['method'],'http.route':route,'http.response.status_code':status or None,
                    'duration_ms':round((time.monotonic()-started)*1000,3)}
                span.update_name(scope['method']+' '+route);span.set_attributes(sanitize(attrs))
                if status>=500:span.set_status(Status(StatusCode.ERROR))
                event('http.request.completed',level=logging.ERROR if status>=500 else logging.WARNING if status>=400 else logging.DEBUG if quiet else logging.INFO,**attrs)
                if diagnostic is not None and request_body is not None and response_body is not None:
                    try:
                        raw_headers=scope.get('headers',[]);host=next((value.decode('latin1') for key,value in raw_headers if key.lower()==b'host'),'unknown')
                        captured=await asyncio.to_thread(diagnostic.record_http,direction='ingress',
                            method=scope['method'],url=f"{scope.get('scheme','http')}://{host}{route}",
                            request_headers=raw_headers,request_body=request_body,response_headers=response_headers,
                            response_body=response_body,status_code=status,request_id=request_id,
                            duration_ms=(time.monotonic()-started)*1000)
                        if captured:event('diagnostic.http.captured',level=logging.DEBUG,
                            capture_mode=diagnostic.mode,capture_session=diagnostic.session,
                            request_bytes=request_body.size,response_bytes=response_body.size,
                            duration_ms=round((time.monotonic()-started)*1000,3))
                    except Exception as capture_error:
                        event('diagnostic.capture.failed',level=logging.WARNING,exc=capture_error,
                            capture_mode=diagnostic.mode,capture_session=diagnostic.session)
