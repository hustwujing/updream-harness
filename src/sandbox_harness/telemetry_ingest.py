"""Restricted OTLP receiver for authenticated sandbox telemetry, never business state."""
import time
from datetime import datetime,timezone
from opentelemetry import trace
from opentelemetry._logs import LogRecord,SeverityNumber
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import SpanContext,TraceFlags,Status,StatusCode,Link,SpanKind
from .observability import sanitize,EVENT,describe


def scalar(value):
    kind=value.WhichOneof('value')
    return getattr(value,kind) if kind in {'string_value','int_value','double_value','bool_value'} else None


def attributes(values):return {v.key:scalar(v.value) for v in values[:64]}


def context(trace_id,span_id):
    if len(trace_id)!=16 or len(span_id)!=8:raise ValueError('invalid_context_length')
    value=SpanContext(int.from_bytes(trace_id,'big'),int.from_bytes(span_id,'big'),False,TraceFlags(1))
    if not value.is_valid:raise ValueError('invalid_context')
    return value


def claim_attributes(claims,attrs):
    attrs=sanitize(attrs)
    expected={'turn_id':claims.turn_id,'thread_id':claims.thread_id,'sandbox_id':claims.sandbox_id}
    for key,value in expected.items():
        supplied=attrs.get('updream.'+key)
        if supplied and supplied!=value:raise ValueError('telemetry_scope_mismatch')
        if value:attrs['updream.'+key]=value
    attrs['updream.origin']='sandbox_reported'
    return attrs


def resource(values):
    data=attributes(values)
    # Sandbox identity is separately authenticated by the capability; never let it impersonate API/Gateway.
    return Resource({'service.namespace':'updream-harness','service.name':'runtime',
        'service.instance.id':str(data.get('service.instance.id','unknown'))[:64],
        'service.version':str(data.get('service.version','unknown'))[:64]})


def decode_otlp(signal,data,claims):
    if signal=='logs':
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
        payload=ExportLogsServiceRequest();payload.ParseFromString(data)
        rows=[]
        for group in payload.resource_logs:
            res=resource(group.resource.attributes)
            for scope in group.scope_logs:
                for record in scope.log_records:
                    if len(rows)>=512:raise ValueError("too_many_records")
                    name=record.event_name or scalar(record.body)
                    if not isinstance(name,str) or not EVENT.fullmatch(name):raise ValueError('invalid_event_name')
                    ctx=context(record.trace_id,record.span_id) if record.trace_id or record.span_id else None
                    attrs=claim_attributes(claims,attributes(record.attributes))
                    number=record.severity_number if record.severity_number in range(1,25) else 9
                    rows.append(('log',ReadableLogRecord(LogRecord(timestamp=record.time_unix_nano or time.time_ns(),
                        trace_id=ctx.trace_id if ctx else 0,span_id=ctx.span_id if ctx else 0,trace_flags=TraceFlags(record.flags&1),
                        body=describe(name),event_name=name,severity_text=record.severity_text[:16],severity_number=SeverityNumber(number),attributes=attrs),res)))
    elif signal=='traces':
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        payload=ExportTraceServiceRequest();payload.ParseFromString(data);rows=[]
        for group in payload.resource_spans:
            res=resource(group.resource.attributes)
            for scope in group.scope_spans:
                for span in scope.spans:
                    if len(rows)>=512:raise ValueError("too_many_records")
                    if not EVENT.fullmatch(span.name) and not span.name.startswith(('GET ','POST ','PUT ','DELETE ','HTTP ')):
                        raise ValueError('invalid_span_name')
                    # HTTP span names can contain dynamic paths from malformed clients; retain only the route attribute.
                    name=span.name if EVENT.fullmatch(span.name) else 'runtime.http'
                    ctx=context(span.trace_id,span.span_id)
                    parent=context(span.trace_id,span.parent_span_id) if span.parent_span_id else None
                    links=[Link(context(link.trace_id,link.span_id)) for link in span.links[:16]]
                    attrs=claim_attributes(claims,attributes(span.attributes))
                    rows.append(('span',ReadableSpan(name,context=ctx,parent=parent,resource=res,attributes=attrs,
                        links=links,kind=SpanKind(max(0,min(span.kind-1,4))),
                        status=Status(StatusCode.ERROR if span.status.code==2 else StatusCode.UNSET),
                        start_time=span.start_time_unix_nano,end_time=max(span.end_time_unix_nano,span.start_time_unix_nano))))
    else:raise ValueError('unsupported_signal')
    if len(rows)>512:raise ValueError('too_many_records')
    return rows


def import_snapshot(telemetry,snapshot,sandbox):
    """Fallback before deletion; incoming data is still sandbox-reported/untrusted."""
    from types import SimpleNamespace
    claims=SimpleNamespace(turn_id=sandbox.turn_id,thread_id=sandbox.thread_id,
        sandbox_id=sandbox.id)
    count=0
    for row in snapshot.get('records',[])[:500]:
        try:
            name=row.get('event_name','')
            if not EVENT.fullmatch(name):continue
            attrs=claim_attributes(claims,row.get('attributes',{}))
            attrs['updream.source_event_id']=str(row.get('attributes',{}).get('updream.source_event_id') or row.get('event_id',''))[:256]
            res=Resource({'service.namespace':'updream-harness','service.name':'runtime',
                'service.instance.id':str(row.get('resource',{}).get('service.instance.id','unknown'))[:64],
                'service.version':str(row.get('resource',{}).get('service.version','unknown'))[:64]})
            ctx=context(bytes.fromhex(row['trace_id']),bytes.fromhex(row['span_id'])) if row.get('trace_id') else None
            timestamp=int(datetime.fromisoformat(row['timestamp']).timestamp()*1e9)
            if row.get('signal')=='log':
                severity={'DEBUG':SeverityNumber.DEBUG,'INFO':SeverityNumber.INFO,'WARNING':SeverityNumber.WARN,'ERROR':SeverityNumber.ERROR}.get(row.get('severity_text'),SeverityNumber.INFO)
                telemetry.put('log',ReadableLogRecord(LogRecord(timestamp=timestamp,body=describe(name),event_name=name,
                    severity_text=row.get('severity_text','INFO'),severity_number=severity,
                    trace_id=ctx.trace_id if ctx else 0,span_id=ctx.span_id if ctx else 0,attributes=attrs),res))
                count+=1
            elif row.get('signal')=='span' and ctx:
                duration=max(0,min(float(row.get('duration_ms',0)),86400000))
                parent=context(bytes.fromhex(row['trace_id']),bytes.fromhex(row['parent_span_id'])) if row.get('parent_span_id') else None
                links=[Link(context(bytes.fromhex(link['trace_id']),bytes.fromhex(link['span_id']))) for link in row.get('links',[])[:16]]
                telemetry.put('span',ReadableSpan(name,context=ctx,parent=parent,resource=res,attributes=attrs,links=links,
                    start_time=timestamp-int(duration*1e6),end_time=timestamp,
                    status=Status(StatusCode.ERROR if row.get('status')=='ERROR' else StatusCode.UNSET)))
                count+=1
        except (KeyError,ValueError,TypeError,OverflowError):continue
    return count
