"""Explicit, bounded HTTP payload capture kept outside operational telemetry."""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from string import Formatter
import time
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit
import uuid

from opentelemetry import trace

from sandbox_harness.common.observability.schema import SECRET


SAFE_HEADERS = frozenset({
    "accept", "content-encoding", "content-length", "content-type", "user-agent",
    "x-request-id", "x-trace-id",
})
CREDENTIAL_FIELD = re.compile(
    r"(?i)(?:authorization|cookie|password|passwd|secret|token|api[_-]?key|credential)"
)
TEXT_TYPES = ("application/json", "application/problem+json", "application/x-www-form-urlencoded",
    "application/xml", "application/javascript", "text/")


def _safe_filename(template: str, *, service: str, instance: str, session: str) -> str:
    fields=[]
    try:
        for _,field,_,_ in Formatter().parse(template):
            if field:fields.append(field)
        rendered=template.format(service=service,instance=instance,session=session)
    except (KeyError,ValueError,IndexError) as exc:
        raise ValueError("invalid_diagnostic_filename_template") from exc
    if (set(fields)-{"service","instance","session"} or "/" in template or "\\" in template
            or ".." in template or not re.fullmatch(r"[A-Za-z0-9_.{}-]{1,180}",template)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,240}\.jsonl",rendered)):
        raise ValueError("invalid_diagnostic_filename_template")
    return rendered


def resolve_diagnostic_root(data_root: Path, configured: Path | str) -> Path:
    base=Path(data_root).resolve();value=Path(configured)
    root=(value if value.is_absolute() else base/value).resolve()
    try:root.relative_to(base)
    except ValueError as exc:raise ValueError("diagnostic_directory_outside_data_root") from exc
    return root


def _redact(value, depth=0):
    if depth>20:return "[MAX_DEPTH]"
    if isinstance(value,dict):
        return {str(key)[:256]:("[REDACTED]" if CREDENTIAL_FIELD.search(str(key)) else
            _redact(item,depth+1)) for key,item in list(value.items())[:1024]}
    if isinstance(value,list):return [_redact(item,depth+1) for item in value[:1024]]
    if isinstance(value,str):return SECRET.sub("[REDACTED]",value)
    if value is None or isinstance(value,(bool,int,float)):return value
    return str(value)[:256]


def _headers(values: Mapping[str, str] | list[tuple[bytes,bytes]]) -> dict[str,str]:
    if isinstance(values,Mapping):items=values.items()
    else:items=((key.decode("latin1"),value.decode("latin1")) for key,value in values)
    return {str(key).lower():SECRET.sub("[REDACTED]",str(value))[:1024]
        for key,value in items if str(key).lower() in SAFE_HEADERS}


def _normalized_url(value: str) -> str:
    parsed=urlsplit(value)
    host=parsed.hostname or "unknown"
    try:port=parsed.port
    except ValueError:port=None
    if port:host=f"{host}:{port}"
    return urlunsplit((parsed.scheme or "http",host,parsed.path or "/","",""))


class BodyAccumulator:
    def __init__(self,limit: int):
        self.limit=max(0,limit);self.data=bytearray();self.size=0;self.digest=sha256()

    def update(self,value: bytes) -> None:
        if not value:return
        self.size+=len(value);self.digest.update(value)
        remaining=self.limit-len(self.data)
        if remaining>0:self.data.extend(value[:remaining])

    @classmethod
    def from_bytes(cls,value: bytes,limit: int):
        result=cls(limit);result.update(value);return result


class DiagnosticCapture:
    def __init__(self,root,service,instance,*,mode="off",log_level="INFO",session="",
            trace_id="",filename_template="{service}-{instance}-{session}.diagnostic.jsonl",
            max_body_bytes=256*1024,max_bytes=256*1024**2,ttl_hours=24):
        if mode not in {"off","metadata","payload"}:raise ValueError("invalid_diagnostic_capture_mode")
        if mode=="payload" and str(log_level).upper()!="DEBUG":raise ValueError("diagnostic_payload_requires_debug")
        if max_body_bytes<0 or max_bytes<1024 or not 1<=ttl_hours<=168:raise ValueError("invalid_diagnostic_limits")
        self.root=Path(root);self.service=service;self.instance=instance;self.mode=mode
        self.session=session or uuid.uuid4().hex;self.trace_id=trace_id
        self.max_body_bytes=max_body_bytes;self.max_bytes=max_bytes;self.ttl_seconds=ttl_hours*3600
        filename=_safe_filename(filename_template,service=service,instance=instance,session=self.session)
        self.path=self.root/filename

    @classmethod
    def from_settings(cls,settings,service,instance):
        root=resolve_diagnostic_root(settings.data_root,settings.diagnostic_dir)
        return cls(root,service,instance,mode=settings.diagnostic_capture,log_level=settings.log_level,
            session=settings.diagnostic_capture_session,trace_id=settings.diagnostic_trace_id,
            filename_template=settings.diagnostic_file_template,
            max_body_bytes=settings.diagnostic_max_body_bytes,max_bytes=settings.diagnostic_max_bytes,
            ttl_hours=settings.diagnostic_ttl_hours)

    def accumulator(self) -> BodyAccumulator:
        return BodyAccumulator(self.max_body_bytes if self.mode=="payload" else 0)

    def _body(self,value: BodyAccumulator,content_type: str) -> dict:
        result={"bytes":value.size,"sha256":value.digest.hexdigest(),
            "captured_bytes":len(value.data),"truncated":value.size>len(value.data)}
        normalized=content_type.split(";",1)[0].strip().lower()
        if self.mode!="payload" or not any(normalized.startswith(prefix) for prefix in TEXT_TYPES):return result
        text=bytes(value.data).decode("utf-8","replace")
        if normalized.endswith("json") and not result["truncated"]:
            try:result["json"]=_redact(json.loads(text));return result
            except (ValueError,TypeError):pass
        result["text"]=SECRET.sub("[REDACTED]",text)
        return result

    def _side(self,headers,body: BodyAccumulator) -> dict:
        safe=_headers(headers);content_type=safe.get("content-type","")
        return {"headers":safe,"body":self._body(body,content_type)}

    def _write(self,row: dict) -> bool:
        import fcntl
        encoded=(json.dumps(row,ensure_ascii=False,separators=(",",":"))+"\n").encode()
        if len(encoded)>self.max_bytes:return False
        self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        with (self.root/".retention.lock").open("a") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);now=time.time();total=0
            files=sorted(self.root.glob("*.diagnostic.jsonl"),key=lambda path:path.stat().st_mtime)
            for path in files:
                info=path.stat()
                if info.st_mtime<now-self.ttl_seconds:
                    path.unlink(missing_ok=True)
                else:total+=info.st_size
            for path in files:
                if total+len(encoded)<=self.max_bytes:break
                if path.exists() and path!=self.path:
                    size=path.stat().st_size;path.unlink(missing_ok=True);total-=size
            if total+len(encoded)>self.max_bytes and self.path.exists():return False
            fd=os.open(self.path,os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,"ab") as stream:stream.write(encoded)
        return True

    def record_http(self,*,direction: str,method: str,url: str,request_headers,
            request_body: bytes | BodyAccumulator,response_headers,response_body: bytes | BodyAccumulator,
            status_code: int,request_id: str="",duration_ms: float | None=None) -> bool:
        if self.mode=="off":return False
        context=trace.get_current_span().get_span_context()
        current_trace=f"{context.trace_id:032x}" if context.is_valid else ""
        if self.trace_id and current_trace!=self.trace_id:return False
        request=request_body if isinstance(request_body,BodyAccumulator) else BodyAccumulator.from_bytes(request_body,self.max_body_bytes)
        response=response_body if isinstance(response_body,BodyAccumulator) else BodyAccumulator.from_bytes(response_body,self.max_body_bytes)
        row={"schema_version":1,"timestamp":datetime.now(timezone.utc).isoformat(),
            "service":self.service,"instance":self.instance,"session":self.session,
            "trace_id":current_trace,"span_id":f"{context.span_id:016x}" if context.is_valid else "",
            "request_id":request_id,"direction":direction,"method":method[:16],
            "url":_normalized_url(url),"status_code":status_code,
            "request":self._side(request_headers,request),"response":self._side(response_headers,response)}
        if duration_ms is not None:row["duration_ms"]=round(duration_ms,3)
        return self._write(row)
