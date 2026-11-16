from __future__ import annotations
import asyncio,os,subprocess,uuid,json
from pathlib import Path
import httpx
ROOT=Path(__file__).parents[1]
async def main():
 async with httpx.AsyncClient(base_url='http://127.0.0.1:24766/',timeout=None,trust_env=False) as client:
  title='LIVE-U-BG-'+uuid.uuid4().hex[:8];thread=(await client.post('api/threads',json={'harness':'claude','model':'claude-sonnet-5-5','reasoning_level':'standard','title':title})).json()
  response=await client.post(f"api/threads/{thread['id']}/turns",json={'message':"Run exactly: sandbox-background start --name browser-cancel -- python -c 'import time;time.sleep(60)'. Then reply briefly.",'attachments':[],'idempotency_key':'browser-bg-'+uuid.uuid4().hex,'delivery_mode':'start'});response.raise_for_status();turn=response.json()
  for _ in range(240):
   jobs=(await client.get(f"api/turns/{turn['id']}/background-jobs")).json()
   if any(j['status']=='running' for j in jobs):break
   await asyncio.sleep(.25)
  else:raise TimeoutError('background job')
  env={**os.environ,'NODE_PATH':str(ROOT/'web/node_modules'),
      'PLAYWRIGHT_BROWSERS_PATH':str(ROOT/'.data/playwright-browsers'),
      'TMPDIR':str(ROOT/'.data/tmp'),'UI_SESSION_TOKEN':client.cookies.get('sandbox_harness_session'),'UI_THREAD_TITLE':title}
  subprocess.run(['node','tests/browser_live_background.cjs'],env=env,check=True)
  final=None
  for _ in range(240):
   final=(await client.get(f"api/turns/{turn['id']}")).json()
   if final['state'] in {'COMPLETED','FAILED','CANCELLED','WAITING_USER'}:break
   await asyncio.sleep(.25)
  if final['state']!='FAILED' or final['error_code']!='background_job_failed':raise AssertionError(json.dumps(final))
  print(json.dumps({'id':'U-BG','status':'passed','turn_id':turn['id'],'error_code':final['error_code']}))
if __name__=='__main__':asyncio.run(main())
