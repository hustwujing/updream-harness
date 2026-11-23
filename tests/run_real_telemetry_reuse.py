"""Real OpenSandbox reuse across two Turns; synthetic Harness, no Provider call."""
import asyncio,json,os,time,uuid
from pathlib import Path
from dotenv import dotenv_values
import httpx
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,RuntimeResult
from sandbox_harness.capability import CapabilityCodec
from sandbox_harness.sandbox import OpenSandboxBackend
from sandbox_harness.sandbox_contracts import SandboxUpdate
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.observability import configure,operation,telemetry,carrier

FIXTURE="import sys,json\nsys.stdin.readline()\nprint(json.dumps({'type':'result','result':'fixture completed'}),flush=True)\n"

async def main():
    for key,value in dotenv_values(os.environ.get('SANDBOX_HARNESS_ENV_FILE','/data2/wujing03/sandbox-harness-platform-parallel/.data/config/platform.env')).items():
        if value is not None:os.environ[key]=value
    settings=Settings();identity=uuid.uuid4().hex;root=settings.data_root/'observability'/('reuse-'+identity)
    root.mkdir(parents=True);configure('test-reuse',root/'logs')
    namespace='sandbox-harness-logreuse-'+identity
    backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,300,namespace,settings.gateway_url,settings.gateway_proxy_port)
    sandbox=None;started=time.monotonic();turns=[uuid.uuid4().hex,uuid.uuid4().hex];thread_id=uuid.uuid4().hex
    try:
        sandbox=await backend.create(Harness.CLAUDE,settings.image_for('claude'),{
            'sandbox-harness.io/idempotency-key':identity},
            {'HARNESS_COMMAND_JSON':json.dumps({'claude':['python','-c',FIXTURE]})})
        client=RuntimeClient()
        for turn_id in turns:
            await backend.update(sandbox,SandboxUpdate(ttl_seconds=300))
            sandbox.thread_id=thread_id;sandbox.turn_id=turn_id;sandbox.warm=False
            with operation('test.reused_turn',thread_id=thread_id,turn_id=turn_id,sandbox_id=sandbox.id):
                token,claims=CapabilityCodec(settings.gateway_signing_key).issue(thread_id=thread_id,turn_id=turn_id,
                    sandbox_id=sandbox.id,harness=Harness.CLAUDE,model='synthetic',routes=['/v1/telemetry'],
                    ttl_seconds=120,max_requests=1,max_public_requests=1,token_budget=1)
                await client.inject(sandbox,capability_token=token,gateway_url=settings.gateway_url,proxy_url=settings.public_proxy_url,harness='claude',model='synthetic')
                final=None
                async for value in client.run(sandbox,message='fixture',attachments=[],native_thread_id='',reasoning_effort='high',execution_id=turn_id,input_id=turn_id):
                    if isinstance(value,RuntimeResult):final=value
                assert final and final.status=='completed'
                snapshot=await client.diagnostics(sandbox)
                config_logs=[row for row in snapshot['records'] if row.get('event_name')=='http.request.completed' and row.get('attributes',{}).get('http.route')=='/v1/configure']
                assert config_logs[-1]['attributes']['updream.turn_id']==turn_id
                async with httpx.AsyncClient(trust_env=False,timeout=10) as control:
                    response=await control.post(settings.gateway_url+'/internal/revoke/'+claims.nonce,headers={**carrier(),
                        'X-Sandbox-Harness-Control':settings.gateway_control_key,'X-Updream-Turn-ID':turn_id,
                        'X-Updream-Thread-ID':thread_id,'X-Updream-Sandbox-ID':sandbox.id})
                    response.raise_for_status()
        assert [row['attributes']['updream.turn_id'] for row in config_logs]==turns
        report={'passed':True,'real_opensandbox':True,'harness':'synthetic; no model calls','same_sandbox':sandbox.id,
            'turns':turns,'configuration_ids_correct':True,'source_version':snapshot['records'][-1]['resource']['service.version'],
            'elapsed_seconds':round(time.monotonic()-started,2),'runtime_stats':snapshot.get('stats',{})}
        await backend.delete(sandbox);sandbox=None
        assert not await backend.list_namespace();report['sandbox_deleted']=True
        (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps({**report,'evidence':str(root)}),flush=True)
    finally:
        if sandbox:await backend.delete(sandbox)
        await backend.client.aclose();telemetry().close()

if __name__=='__main__':asyncio.run(main())
