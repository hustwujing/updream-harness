"""Isolated real OpenSandbox + Broker SIGKILL recovery and telemetry assertions.

Only test worker processes are killed. Production services and user Turns are never
stopped by this case. The Harness is a deterministic fixture, not a model call.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from dotenv import dotenv_values
from sandbox_harness.services.api.broker import (Broker)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, TurnState)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (OpenSandboxBackend)
from test_support.legacy_store import (Store)
from sandbox_harness.common.observability.observability import (configure, operation, telemetry)
from sandbox_harness.tools.log_query import (query)

FIXTURE='''import sys,json,time
from pathlib import Path
sys.stdin.readline()
with Path('/workspace/project/observability-once.marker').open('x') as f: f.write('once')
print(json.dumps({'type':'assistant_delta','text':'OBSERVABILITY_STARTED'}),flush=True)
time.sleep(18)
Path('/workspace/outputs/observability-result.json').write_text(json.dumps({'executions':1,'result':'recovered'}))
print(json.dumps({'type':'result','result':'OBSERVABILITY_COMPLETED'}),flush=True)
'''

class Backend(OpenSandboxBackend):
    async def create(self,harness,image,metadata,environment):
        return await super().create(harness,image,metadata,{**environment,'HARNESS_COMMAND_JSON':json.dumps({'claude':['python','-c',FIXTURE]})})


def environment(path):
    for key,value in dotenv_values(path).items():
        if value is not None:os.environ[key]=value
    return Settings()


async def worker(mode,root,envfile):
    configured=environment(envfile)
    configure('test-broker',root/'logs')
    settings=configured.model_copy(update={'data_root':root,'database_url':'sqlite+aiosqlite:///'+str(root/'test.db'),
        'session_keepalive_seconds':0,'turn_lease_seconds':10,'turn_heartbeat_seconds':2,'recovery_scan_seconds':5})
    namespace=(root/'namespace').read_text()
    store=Store(settings.database_url);await store.initialize()
    backend=Backend(settings.opensandbox_url,settings.opensandbox_api_key,300,namespace,settings.gateway_url,settings.gateway_proxy_port)
    pool=SandboxPool(backend,{h:settings.image_for(h.value) for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    broker=Broker(settings,store,ObjectStore(root/'objects'),pool,RuntimeClient(),HarnessCatalog())
    try:
        if mode=='initial':
            profile=HarnessCatalog().resolve('claude','claude-4.6-sonnet','standard')
            thread=await store.create_thread(ThreadCreate(harness=Harness.CLAUDE,model=profile.model,title='Isolated telemetry recovery'),profile,'test-observability-owner')
            with operation('test.submit'):
                turn=await broker.submit(thread.id,TurnCreate(message='run deterministic fixture',idempotency_key=uuid.uuid4().hex))
            async with asyncio.timeout(90):
                while True:
                    current=await store.get_turn(turn.id)
                    if current.state==TurnState.RUNNING and current.runtime_sequence>=1 and current.last_traceparent:
                        telemetry().flush(2)
                        (root/'ready.json').write_text(json.dumps({'thread_id':thread.id,'turn_id':turn.id,
                            'sandbox_id':current.sandbox_id,'generation':current.recovery_generation,
                            'traceparent':current.last_traceparent,'instance_id':telemetry().instance}))
                        break
                    if current.state in {TurnState.FAILED,TurnState.CANCELLED}:raise AssertionError(current.error_code)
                    await asyncio.sleep(.1)
            await asyncio.Event().wait()
        else:
            control=json.loads((root/'ready.json').read_text());await broker.start()
            async with asyncio.timeout(120):
                while True:
                    current=await store.get_turn(control['turn_id'])
                    if current.state in {TurnState.COMPLETED,TurnState.FAILED,TurnState.CANCELLED} and current.id not in broker.tasks:break
                    await asyncio.sleep(.2)
            assert current.state==TurnState.COMPLETED,current.error_code
            artifacts=await store.artifacts(current.id)
            artifact=next(a for a in artifacts if a.path=='observability-result.json')
            assert json.loads(broker.objects.get(artifact.object_ref,maximum=4096))=={'executions':1,'result':'recovered'}
            assert current.sandbox_id==control['sandbox_id'] and current.recovery_generation>control['generation']
            assert not await backend.list_namespace()
            assert telemetry().flush(5)
            records,_=query(root/'logs',{'turn_id':current.id})
            old_trace=control['traceparent'].split('-')[1]
            new_trace=current.last_traceparent.split('-')[1]
            assert old_trace!=new_trace
            new_span=next(row for row in records if row.get('signal')=='span' and row.get('trace_id')==new_trace and row.get('event_name')=='turn.process')
            assert old_trace in {link['trace_id'] for link in new_span['links']}
            assert any(row.get('resource',{}).get('service.name')=='runtime' for row in records),'Runtime fallback collection missing'
            assert any(row.get('event_name')=='workspace.checkpoint.persisted' for row in records)
            assert any(row.get('event_name')=='runtime.logs.collected' for row in records)
            assert not any(row.get('event_name')=='turn.process.completed' and row.get('trace_id')==old_trace for row in records),'Old crashed attempt was incorrectly marked completed'
            report={'passed':True,'real_opensandbox':True,'harness':'deterministic fixture; no Provider call',
                'turn_id':current.id,'sandbox_id':current.sandbox_id,'generation_before':control['generation'],
                'generation_after':current.recovery_generation,'old_trace_id':old_trace,'new_trace_id':new_trace,
                'old_instance_id':control['instance_id'],'new_instance_id':telemetry().instance,
                'same_sandbox':True,'executions':1,'recovery_link':True,'runtime_logs_preserved':True,
                'checkpoint_saved':True,'artifact_verified':True,'sandbox_deleted':True,'records':len(records)}
            (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
    finally:
        await broker.close();await pool.close();await store.close();await backend.client.aclose()
        telemetry().close()


async def supervise(envfile,root):
    configured=environment(envfile)
    root.mkdir(parents=True,exist_ok=True)
    (root/'namespace').write_text('sandbox-harness-observe-'+uuid.uuid4().hex)
    started=time.monotonic();child=None
    try:
        with (root/'initial-process.log').open('w') as log:
            child=subprocess.Popen([sys.executable,__file__,'--worker','initial','--env',envfile,'--root',str(root)],stdout=log,stderr=log)
        async with asyncio.timeout(100):
            while not (root/'ready.json').exists():
                assert child.poll() is None,'Initial test worker exited before readiness'
                await asyncio.sleep(.1)
        child.kill();await asyncio.to_thread(child.wait,5)
        assert child.returncode==-signal.SIGKILL
        print('Isolated test Broker killed with SIGKILL; starting recovery worker',flush=True)
        with (root/'recovery-process.log').open('w') as log:
            child=subprocess.Popen([sys.executable,__file__,'--worker','recover','--env',envfile,'--root',str(root)],stdout=log,stderr=log)
        async with asyncio.timeout(130):
            while child.poll() is None:await asyncio.sleep(.2)
        assert child.returncode==0,'Recovery worker failed; inspect its local log'
        report=json.loads((root/'report.json').read_text());report['elapsed_seconds']=round(time.monotonic()-started,2)
        (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
    finally:
        if child and child.poll() is None:child.kill();await asyncio.to_thread(child.wait,5)
        backend=Backend(configured.opensandbox_url,configured.opensandbox_api_key,300,(root/'namespace').read_text(),configured.gateway_url,configured.gateway_proxy_port)
        try:
            for sandbox in await backend.list_namespace():await backend.delete(sandbox)
        finally:await backend.client.aclose()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--worker',choices=['initial','recover'])
    parser.add_argument('--env',default='/data2/wujing03/sandbox-harness-platform-parallel-ali/.data/config/platform.env');parser.add_argument('--root',required=True)
    args=parser.parse_args();root=Path(args.root).resolve()
    asyncio.run(worker(args.worker,root,args.env) if args.worker else supervise(args.env,root))
