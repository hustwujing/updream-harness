"""Opt-in real OpenSandbox fault injection, with no model calls and isolated metadata.

SANDBOX_HARNESS_ENV_FILE can point to a candidate image configuration.
"""
import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import httpx
from dotenv import dotenv_values
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (OpenSandboxBackend)

FIXTURE = '''import sys,json,time
from pathlib import Path
value=json.loads(sys.stdin.readline())
text=value['message']['content'][0]['text']
with Path('/workspace/project/receipts.txt').open('a') as f: f.write('received\\n')
if 'EOF_CASE' in text:
 print('[]',flush=True)
 sys.exit(0)
if 'HOLD_CASE' in text:
 time.sleep(60)
time.sleep(.3)
print(json.dumps({'type':'result','result':'done'}),flush=True)
'''

async def main():
    for key,value in dotenv_values(os.environ.get('SANDBOX_HARNESS_ENV_FILE','/data/wujing02/sandbox-harness-platform-parallel-ali/.data/config/platform.env')).items():
        if value is not None: os.environ[key]=value
    settings=Settings();identity=uuid.uuid4().hex
    namespace='sandbox-harness-boundary-'+identity
    backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,300,namespace,
        settings.gateway_url,settings.gateway_proxy_port)
    sandbox=None;started=time.monotonic()
    try:
        sandbox=await backend.create(Harness.CLAUDE,settings.image_for('claude'),{
            'sandbox-harness.io/idempotency-key':identity},
            {'HARNESS_COMMAND_JSON':json.dumps({'claude':['python','-c',FIXTURE]})})
        sandbox.thread_id='runtime-boundaries';sandbox.turn_id=identity
        await RuntimeClient().inject(sandbox,capability_token='synthetic-fixture-no-provider-access',
            gateway_url=settings.gateway_url,proxy_url=settings.gateway_url.rsplit(':',1)[0]+':'+str(settings.gateway_proxy_port),
            harness='claude',model='synthetic')
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,headers=sandbox.runtime_headers,trust_env=False,timeout=30) as client:
            body={'message':'one invocation','execution_id':'concurrent','input_id':'first'}
            responses=await asyncio.gather(*(client.post('/v1/turns',json=body) for _ in range(12)))
            for response in responses:
                response.raise_for_status()
                assert json.loads(response.text.splitlines()[-1])['result']['status']=='completed'
            conflict=await client.post('/v1/turns',json={**body,'message':'different'})
            assert conflict.status_code==409
            response=await client.post('/v1/turns',json={'message':'EOF_CASE','execution_id':'eof'})
            assert json.loads(response.text.splitlines()[-1])['result']['error_code']=='harness_result_missing'
            held=asyncio.create_task(client.post('/v1/turns',json={'message':'HOLD_CASE','execution_id':'hold'}))
            for _ in range(100):
                r=await client.get('/v1/executions/hold')
                if r.status_code==200:break
                await asyncio.sleep(.05)
            else:raise AssertionError('hold execution did not start')
            job=await client.post('/v1/background-jobs',json={'name':'ignore-term','argv':['python','-c',
                'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print("ready",flush=True);time.sleep(60)']})
            job.raise_for_status();job_id=job.json()['id']
            for _ in range(100):
                jobs=(await client.get('/v1/background-jobs')).json()['items']
                if any(j['id']==job_id and 'ready' in j['log_tail'] for j in jobs):break
                await asyncio.sleep(.05)
            else:raise AssertionError('background job did not become ready')
            cancelled_at=time.monotonic()
            response=await client.post('/v1/cancel');response.raise_for_status()
            cancel_elapsed=time.monotonic()-cancelled_at
            response=await held
            assert json.loads(response.text.splitlines()[-1])['result']['status']=='cancelled'
            jobs=(await client.get('/v1/background-jobs')).json()['items']
            assert next(j for j in jobs if j['id']==job_id)['status']=='cancelled'
            async def ask(execution):
                pending=asyncio.create_task(client.post('/v1/native-interactions',json={'kind':'question','prompt':'Pick?'}))
                async with client.stream('GET',f'/v1/executions/{execution}/events') as stream:
                    async for line in stream.aiter_lines():
                        event=json.loads(line)
                        if event.get('kind')=='waiting_user':
                            return event['payload']['interaction_id'],pending
                raise AssertionError('question event missing')
            old_id=None
            for number in range(2):
                execution=f'question-{number}'
                running=asyncio.create_task(client.post('/v1/turns',json={'message':'HOLD_CASE','execution_id':execution}))
                for _ in range(100):
                    if (await client.get(f'/v1/executions/{execution}')).status_code==200:break
                    await asyncio.sleep(.05)
                alias,pending=await ask(execution)
                if old_id:
                    stale=await client.post('/v1/interactions/'+old_id,json={'action':'answer','answers':{'choice':'old'}})
                    assert stale.status_code==409 and alias!=old_id
                answered=await client.post('/v1/interactions/'+alias,json={'action':'answer','answers':{'choice':'new'}})
                answered.raise_for_status();(await pending).raise_for_status()
                (await client.post('/v1/cancel')).raise_for_status()
                await running
                old_id=alias
            archive=await client.get('/v1/checkpoint');archive.raise_for_status()
            import io,tarfile
            with tarfile.open(fileobj=io.BytesIO(archive.content)) as tar:
                receipts=tar.extractfile('project/receipts.txt').read().decode().splitlines()
            assert len(receipts)==5, receipts
        await backend.delete(sandbox);sandbox=None
        assert not await backend.list_namespace()
        report={'passed':True,'real_opensandbox':True,'model':'synthetic fixture, no model request',
            'parallel_start_requests':12,'actual_executions':len(receipts),
            'stale_interaction_rejected':True,'eof_rejected':True,'conflicting_replay_rejected':True,'background_cancelled':True,
            'cancel_seconds':round(cancel_elapsed,2),'elapsed_seconds':round(time.monotonic()-started,2),
            'sandbox_deleted':True,'image':settings.image_for('claude')}
        target=settings.data_root/'self-review3'/'real-runtime-result.json';target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
    finally:
        if sandbox:await backend.delete(sandbox)
        await backend.client.aclose()

if __name__=='__main__':asyncio.run(main())
