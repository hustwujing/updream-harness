import asyncio
import json
import logging
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
import httpx
from fastapi import FastAPI,Request
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from sandbox_harness import observability as obs
from sandbox_harness.log_query import query
from sandbox_harness.telemetry_ingest import decode_otlp

@pytest.fixture
def telemetry(tmp_path,monkeypatch):
    value=obs.Telemetry('test',tmp_path/'logs')
    monkeypatch.setattr(obs,'_PROVIDER',value.provider);monkeypatch.setattr(obs,'_TELEMETRY',value)
    root=logging.getLogger();old=root.level;root.setLevel(logging.INFO);root.addHandler(value.handler)
    yield value
    value.close();root.setLevel(old)


def rows(value):
    assert value.flush(5)
    return [json.loads(line) for path in value.sink.root.glob('*.telemetry.jsonl') for line in path.read_text().splitlines()]


async def test_concurrent_contexts_are_isolated_and_recovery_uses_link(telemetry):
    async def run(identity):
        with obs.operation('test.turn',new_trace=True,turn_id=identity):
            original=obs.carrier()['traceparent'];await asyncio.sleep(.01)
            obs.event('test.midpoint')
            with obs.operation('test.child',parent=obs.remote_context({'traceparent':original})):pass
        with obs.operation('test.recovery',new_trace=True,turn_id=identity):
            obs.add_link(original);obs.event('test.recovered')
    await asyncio.gather(run('turn-a'),run('turn-b'))
    records=rows(telemetry)
    for identity in ('turn-a','turn-b'):
        selected=[r for r in records if r['attributes'].get('updream.turn_id')==identity]
        root=next(r for r in selected if r['signal']=='span' and r['event_name']=='test.turn')
        child=next(r for r in selected if r['signal']=='span' and r['event_name']=='test.child')
        recovery=next(r for r in selected if r['signal']=='span' and r['event_name']=='test.recovery')
        assert child['trace_id']==root['trace_id'] and child['parent_span_id']==root['span_id']
        assert recovery['trace_id']!=root['trace_id'] and recovery['links'][0]['trace_id']==root['trace_id']
    assert not obs._CONTEXT.get()


def test_sensitive_fields_exception_messages_and_library_payloads_are_not_logged(telemetry):
    secret='sk-SUPER_PRIVATE_CREDENTIAL_12345'
    try:
        with obs.operation('test.failure',turn_id='safe'):
            obs.event('test.event',password=secret,prompt='private user content',reason='token='+secret)
            logging.getLogger('httpx').warning('https://user:'+secret+'@host/path?token='+secret)
            raise RuntimeError(secret+' private user content')
    except RuntimeError:pass
    serialized=json.dumps(rows(telemetry))
    assert secret not in serialized and 'private user content' not in serialized and 'password' not in serialized
    failed=next(r for r in rows(telemetry) if r['event_name']=='test.failure.failed')
    assert failed['attributes']['exception.type']=='RuntimeError'
    assert 'test_observability.py' in failed['attributes']['exception.stacktrace']


async def test_http_correlation_headers_and_template_path_do_not_leak_query(telemetry):
    app=FastAPI();app.add_middleware(obs.RequestLoggingMiddleware,service='api')
    @app.post('/items/{item_id}')
    async def handler(item_id:str,request:Request):
        request.state.log_context.update({'turn_id':'turn-http'});obs.event('test.handler')
        from fastapi.responses import JSONResponse
        return JSONResponse({'ok':True},headers={'X-Request-ID':'upstream-id','X-Trace-ID':'untrusted'})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
        response=await client.post('/items/private-file?token=VERY_PRIVATE',headers={'traceparent':'malformed','Authorization':'Bearer VERY_PRIVATE'})
    assert response.status_code==200 and len(response.headers['x-trace-id'])==32 and len(response.headers['x-request-id'])==32
    records=rows(telemetry);serialized=json.dumps(records)
    assert 'VERY_PRIVATE' not in serialized and 'private-file' not in serialized
    log=next(r for r in records if r['event_name']=='http.request.completed')
    assert log['attributes']['http.route']=='/items/{item_id}' and log['attributes']['updream.turn_id']=='turn-http'
    assert log['trace_id']==response.headers['x-trace-id']


def test_queue_overload_and_disk_failure_do_not_block_business(tmp_path,monkeypatch):
    value=obs.Telemetry('overload',tmp_path,queue_size=2)
    entered=threading.Event();release=threading.Event();original=value.sink.write
    def blocked(records):entered.set();release.wait(2);original(records)
    monkeypatch.setattr(value.sink,'write',blocked)
    try:
        record={'signal':'log','event_name':'test.record','attributes':{}}
        value.put('import',record);assert entered.wait(1)
        start=time.monotonic()
        for _ in range(100):value.put('import',record)
        assert time.monotonic()-start<.1 and value.status()['dropped_records']>0
        release.set();assert value.flush()
        def fail(records):raise OSError('disk full')
        monkeypatch.setattr(value.sink,'write',fail);value.put('import',record);assert value.flush()
        assert value.status()['disk_errors'] and value.worker.is_alive()
        monkeypatch.setattr(value.sink,'write',original);value.put('import',record);assert value.flush()
    finally:release.set();value.close()


def test_retention_is_bounded_and_does_not_remove_other_files(tmp_path):
    sink=obs.JsonSink(tmp_path,'test','one',max_bytes=2048,days=7,segment_bytes=512)
    other=tmp_path/'user.txt';other.write_text('keep')
    old=tmp_path/'old.telemetry.jsonl';old.write_text('{}\n');os.utime(old,(1,1))
    for i in range(20):sink.write([{'event_name':'test.record','body':'x'*300,'index':i}])
    assert sum(p.stat().st_size for p in tmp_path.glob('*.telemetry.jsonl'))<=2048
    assert not old.exists() and other.read_text()=='keep'


def test_otlp_exports_both_signals_and_ingest_checks_business_scope(telemetry,monkeypatch):
    captured=[];authorizations=[]
    class Handler(BaseHTTPRequestHandler):
        status=200
        def do_POST(self):
            authorizations.append(self.headers.get('Authorization'))
            captured.append((self.path,self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(self.status);self.end_headers()
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    try:
        monkeypatch.setenv('OTEL_EXPORTER_OTLP_HEADERS','Authorization=Bearer%20test-observability')
        telemetry.set_remote('http://127.0.0.1:'+str(server.server_port)+'/v1',obs.export_headers())
        with obs.operation('runtime.execution',turn_id='turn-1',thread_id='thread-1',sandbox_id='box-1'):
            obs.event('runtime.finished')
        assert telemetry.flush(5)
        assert {p for p,_ in captured}=={'/v1/logs','/v1/traces'}
        assert set(authorizations)=={'Bearer test-observability'}
        claims=SimpleNamespace(turn_id='turn-1',thread_id='thread-1',sandbox_id='box-1')
        for path,data in captured:
            decoded=decode_otlp(path.rsplit('/',1)[-1],data,claims)
            assert decoded
            with pytest.raises(ValueError,match='scope_mismatch'):
                decode_otlp(path.rsplit('/',1)[-1],data,SimpleNamespace(turn_id='other',thread_id='thread-1',sandbox_id='box-1'))
        payload=next(data for path,data in captured if path.endswith('/logs'))
        parsed=ExportLogsServiceRequest();parsed.ParseFromString(payload)
        assert parsed.resource_logs[0].scope_logs[0].log_records[0].trace_id
        Handler.status=400
        obs.event('test.export_unavailable');assert telemetry.flush(5)
        assert telemetry.status()['export_failures']>0 and telemetry.worker.is_alive()
        Handler.status=200;obs.event('test.export_recovered');assert telemetry.flush(5)
    finally:server.shutdown();server.server_close()


def test_timeline_deduplicates_imported_logs_and_spans(telemetry):
    with obs.operation('test.query',turn_id='turn-query'):obs.event('test.record')
    original=rows(telemetry)
    for record in original:telemetry.put('import',record)
    assert telemetry.flush()
    selected,malformed=query(telemetry.sink.root,{'turn_id':'turn-query'})
    assert len(selected)==len(original) and malformed==0


async def test_gateway_receiver_requires_scope_and_does_not_consume_model_budget(telemetry,tmp_path,monkeypatch):
    import sandbox_harness.gateway as gateway
    from sandbox_harness.capability import CapabilityCodec
    from sandbox_harness.config import Settings
    from sandbox_harness.domain import Harness
    keys=[]
    class Redis:
        async def exists(self,key):return 0
        async def eval(self,script,count,*values):keys.extend(values[:count]);return 1
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *a,**kw:Redis())
    settings=Settings(backend='fake',data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'db'),gateway_signing_key='test-key'*8)
    app=gateway.create_gateway(settings)
    token,_=CapabilityCodec(settings.gateway_signing_key).issue(thread_id='thread',turn_id='turn',sandbox_id='box',
        harness=Harness.CODEX,model='model',routes=['/v1/telemetry'],ttl_seconds=60,max_requests=1,max_public_requests=1,token_budget=1)
    message=ExportLogsServiceRequest();record=message.resource_logs.add().scope_logs.add().log_records.add()
    record.event_name='runtime.test';record.body.string_value='runtime.test'
    for key,value in {'updream.turn_id':'turn','updream.thread_id':'thread','updream.sandbox_id':'box'}.items():
        attribute=record.attributes.add();attribute.key=key;attribute.value.string_value=value
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
        assert (await client.post('/v1/telemetry/logs',content=message.SerializeToString())).status_code==401
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/x-protobuf'}
        assert (await client.post('/v1/telemetry/logs',headers=headers,content=message.SerializeToString())).status_code==200
        record.attributes[0].value.string_value='other-turn'
        assert (await client.post('/v1/telemetry/logs',headers=headers,content=message.SerializeToString())).status_code==422
        record.attributes[0].value.string_value='turn'
        with monkeypatch.context() as patch:
            patch.setattr(telemetry,'put',lambda *args:False)
            overloaded=await client.post('/v1/telemetry/logs',headers=headers,content=message.SerializeToString())
            assert overloaded.status_code==503
            from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceResponse
            receipt=ExportLogsServiceResponse();receipt.ParseFromString(overloaded.content)
            assert receipt.partial_success.rejected_log_records==1
    assert keys and all('telemetry:rate:' in key for key in keys)
    imported=next(row for row in rows(telemetry) if row['event_name']=='runtime.test')
    assert imported['attributes']['updream.origin']=='sandbox_reported'


async def test_trace_context_is_durable_and_stale_owner_cannot_replace_it(tmp_path,telemetry):
    from sandbox_harness.store import Store
    from test_second_review import seed
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'trace.db'));await store.initialize()
    try:
        _,turn,_=await seed(store)
        old=await store.claim_turn(turn.id,'old',30)
        with obs.operation('test.owner.old',turn_id=turn.id):
            previous=obs.carrier()['traceparent']
            await store.record_turn_trace(turn.id,previous,'old',old.recovery_generation)
        await store.release_turn_lease(turn.id,'old',old.recovery_generation)
        new=await store.claim_turn(turn.id,'new',30)
        assert new.last_traceparent==previous
        with pytest.raises(RuntimeError,match='stale_turn_lease'):
            await store.record_turn_trace(turn.id,'invalid','old',old.recovery_generation)
        assert (await store.get_turn(turn.id)).last_traceparent==previous
    finally:await store.close()


def test_request_timeline_expands_to_turn_and_skips_corrupt_lines(tmp_path,monkeypatch,capsys):
    from sandbox_harness.log_query import main
    records=[{'signal':'log','timestamp':'2026-11-10T00:00:00+00:00','event_name':'turn.accepted','event_id':'a','resource':{},'attributes':{'updream.request_id':'request-a','updream.turn_id':'turn-a'}},
        {'signal':'log','timestamp':'2026-11-10T00:00:01+00:00','event_name':'runtime.finished','event_id':'b','resource':{},'attributes':{'updream.request_id':'different','updream.turn_id':'turn-a'}}]
    (tmp_path/'test.telemetry.jsonl').write_text('\n'.join(json.dumps(row) for row in records)+'\n[]\npartial-json\n')
    monkeypatch.setattr('sys.argv',['logs','--log-dir',str(tmp_path),'--request','request-a','--json'])
    main();result=json.loads(capsys.readouterr().out)
    assert len(result['records'])==2 and result['malformed_lines']==2


async def test_shutdown_does_not_start_queued_successor(tmp_path,monkeypatch):
    from sandbox_harness.broker import Broker
    from sandbox_harness.store import Store
    from sandbox_harness.config import Settings
    from sandbox_harness.objects import ObjectStore
    from sandbox_harness.pool import SandboxPool
    from sandbox_harness.sandbox import FakeSandboxBackend
    from sandbox_harness.catalog import HarnessCatalog
    from sandbox_harness.domain import Harness,TurnState
    from test_second_review import seed
    from test_broker import Runtime
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'close.db'));await store.initialize()
    pool=SandboxPool(FakeSandboxBackend(),{h:'' for h in Harness},{h:0 for h in Harness})
    broker=Broker(Settings(backend='fake',data_root=tmp_path,gateway_url='http://127.0.0.1:9'),store,ObjectStore(tmp_path/'objects'),pool,Runtime(),HarnessCatalog())
    try:
        _,first,second=await seed(store)
        original=broker._state
        async def state(turn_id,target,**kwargs):
            result=await original(turn_id,target,**kwargs)
            if turn_id==first.id and target==TurnState.COMPLETED:broker.closing=True
            return result
        monkeypatch.setattr(broker,'_state',state)
        broker._schedule(first.id)
        async with asyncio.timeout(5):
            while first.id in broker.tasks:await asyncio.sleep(.01)
        assert (await store.get_turn(second.id)).state==TurnState.QUEUED
        assert second.id not in broker.tasks
        await broker.close()
        assert not broker.heartbeats
        assert second.id in {turn.id for turn in await store.recoverable_turns()}
    finally:await broker.close();await pool.close();await store.close()


def test_source_tool_timing_pairs_ids_and_interrupts_unfinished_tools(telemetry):
    tools=obs.ToolObservations()
    with obs.operation('runtime.execution',turn_id='turn-tool'):
        tools.observe('tool_started',{'tool_use_id':'tool-1','name':'Bash','detail':{'command':'PRIVATE_COMMAND'}},'execution-1')
        time.sleep(.02)
        tools.observe('tool_finished',{'tool_use_id':'tool-1','is_error':False},'execution-1')
        tools.observe('tool_started',{'tool_use_id':'tool-2','name':'Read'},'execution-1')
        tools.close()
    records=rows(telemetry)
    first=next(row for row in records if row['event_name']=='runtime.tool.finished' and row['attributes']['updream.tool_id']=='tool-1')
    second=next(row for row in records if row['event_name']=='runtime.tool.finished' and row['attributes']['updream.tool_id']=='tool-2')
    assert first['attributes']['updream.duration_ms']>=20 and first['attributes']['updream.outcome']=='success'
    assert second['attributes']['updream.outcome']=='interrupted'
    assert 'PRIVATE_COMMAND' not in json.dumps(records)


async def test_revoke_audit_uses_authenticated_business_ids_and_propagated_trace(telemetry,tmp_path,monkeypatch):
    import sandbox_harness.gateway as gateway
    from sandbox_harness.config import Settings
    class Redis:
        async def set(self,*args,**kwargs):pass
    monkeypatch.setattr(gateway.Redis,'from_url',lambda *a,**kw:Redis())
    settings=Settings(backend='fake',data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'revoke.db'))
    app=gateway.create_gateway(settings);turn_id='a'*32
    with obs.operation('test.cleanup',turn_id=turn_id):
        headers={**obs.carrier(),'X-Sandbox-Harness-Control':settings.gateway_control_key,
            'X-Updream-Turn-ID':turn_id,'X-Updream-Thread-ID':'b'*32,'X-Updream-Sandbox-ID':'c'*32}
        expected_trace=headers['traceparent'].split('-')[1]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
            assert (await client.post('/internal/revoke/private-nonce',headers=headers)).status_code==200
            assert (await client.post('/internal/revoke/private-nonce',headers={'X-Updream-Turn-ID':'d'*32})).status_code==403
    records=rows(telemetry)
    revoke=next(row for row in records if row['event_name']=='capability.revoked')
    assert revoke['trace_id']==expected_trace and revoke['attributes']['updream.turn_id']==turn_id
    assert 'private-nonce' not in json.dumps(records) and settings.gateway_control_key not in json.dumps(records)
