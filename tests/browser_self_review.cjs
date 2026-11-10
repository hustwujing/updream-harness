const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox']});
 try {
  const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.addInitScript(()=>{window.streams=[];window.EventSource=class{constructor(url){this.url=url;this.events={};this.closed=false;window.streams.push(this)}addEventListener(name,fn){this.events[name]=fn}close(){this.closed=true}}});
  let terminal=false,created=0,inputs=0,itemReads=0;const started=Date.now()-5000;const ended=started+12000;
  const thread={id:'review-thread',title:'Recovery test',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:new Date().toISOString()};
  const item={id:'i',thread_id:thread.id,turn_id:'turn',sequence:1,type:'user_message',status:'failed',summary:'failed append',payload:{input_id:'input'}};
  await page.route('**/api/**',async route=>{
   const path=new URL(route.request().url()).pathname;let value=[];let status=200;
   if(path.endsWith('/session'))value={ready:true};
   else if(path.endsWith('/catalog/harnesses'))value=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];
   else if(path.endsWith('/threads'))value=[thread];
   else if(path.endsWith('/turns')){if(route.request().method()==='POST')created++;value=[{id:'turn',harness:'codex',model:'gpt-5.6',state:terminal?'FAILED':'RUNNING',updated_at:new Date(ended).toISOString()}]}
   else if(path.endsWith('/items')){itemReads++;value=[item,{id:'timed-tool',thread_id:thread.id,turn_id:'turn',sequence:2,type:'tool',summary:'Bash',status:terminal?'completed':'in_progress',created_at:new Date(started).toISOString(),updated_at:new Date(terminal?ended:started).toISOString(),payload:{detail:{command:'python script.py'}}},{id:'model-step',thread_id:thread.id,turn_id:'turn',sequence:3,type:'plan',summary:terminal?'gpt-5.6-sol 响应完成':'等待 gpt-5.6-sol 响应',status:terminal?'completed':'in_progress',created_at:new Date(started).toISOString(),updated_at:new Date(terminal?ended:started).toISOString(),payload:{model_request_id:'observed-request',model:'gpt-5.6-sol',phase:terminal?'completed':'waiting',first_output_ms:terminal?7000:null,http_status:terminal?200:null}}]}
   else if(path.endsWith('/inputs')){inputs++;status=409;value={detail:'input_delivery_failed'}}
   else if(path.endsWith('/interactions'))value=[{id:'question',turn_id:'turn',kind:'approval',status:'responding',request:{prompt:'Allow?'},response:{action:'approve',answers:{}}}];
   await route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
  });
  await page.goto('http://127.0.0.1:22892/?thread=review-thread');
  await page.waitForSelector('.user-message');
  assert.match(await page.locator('.input-status').innerText(),/未能确认/);
  assert(await page.getByRole('button',{name:'允许',exact:true}).isVisible());
  assert(await page.getByText('等待 gpt-5.6-sol 响应',{exact:true}).isVisible());const firstTime=await page.locator('.activity-duration').first().innerText();await page.waitForTimeout(2400);assert.notEqual(await page.locator('.activity-duration').first().innerText(),firstTime,'running duration must advance');
  assert.equal(await page.evaluate(()=>streams.length),1,'refresh must not reconnect SSE');
  await page.locator('textarea').fill('follow up');await page.getByRole('button',{name:'追加消息',exact:true}).click();
  await page.waitForSelector('[role=alert]');assert.equal(inputs,1);assert.equal(created,0,'delivery failure must not create a duplicate queued Turn');
  terminal=true;await page.evaluate(()=>streams[0].events.state({lastEventId:'5'}));
  await page.waitForFunction(()=>streams[0].closed);if(!await page.locator('.activity').getAttribute('open').then(value=>value!==null))await page.locator('.activity > summary').click();
  assert.equal(await page.locator('.activity-duration').first().innerText(),'耗时 12 秒');const stable=itemReads;await page.waitForTimeout(2200);assert.equal(itemReads,stable,'terminal transcript must stop refreshing');
  assert.equal(await page.locator('.activity-duration').first().innerText(),'耗时 12 秒','completed duration must freeze');await page.reload();await page.waitForSelector('.activity > summary');if(!await page.locator('.activity').getAttribute('open').then(value=>value!==null))await page.locator('.activity > summary').click();await page.waitForSelector('.activity-duration');assert.equal(await page.locator('.activity-duration').first().innerText(),'耗时 12 秒','duration survives reload');await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/self-review2/step-timings.png'});assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,stable_sse:true,terminal_stops:true,no_duplicate_turn:true,input_status_visible:true,responding_retry_visible:true}));
 } finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
