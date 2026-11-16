const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:820}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
 const now=new Date().toISOString(),thread={id:'job-thread',title:'并行生成素材',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'origin-turn',latest_turn_state:'COMPLETED',unread_completion:false},turns=[{id:'origin-turn',thread_id:thread.id,harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now}],jobs=[{local_job_id:'free-analysis',kind:'operation',adapter:'free.compute',descriptor_id:'free.compute',descriptor_version:'1',descriptor_digest:'a'.repeat(64),idempotency_key:'free-analysis-key',parameters:{}},{local_job_id:'paid-image',kind:'provider',adapter:'image.generate',descriptor_id:'image.generate',descriptor_version:'1',descriptor_digest:'c'.repeat(64),idempotency_key:'paid-image-key',parameters:{prompt:'生成一张图片'}}];
 let draft={id:'draft-group',thread_id:thread.id,origin_turn_id:'origin-turn',idempotency_key:'group-key',request_digest:'1'.repeat(64),state:'AWAITING_CONFIRMATION',requires_confirmation:true,authorization_gaps:['confirmation_required:paid-image'],expires_at:new Date(Date.now()+600000).toISOString(),confirmed_group_id:'',created_at:now,updated_at:now,jobs},patches=0,confirms=0,confirmedJobs=[];
 await page.route('**/api/**',async route=>{const request=route.request(),path=new URL(request.url()).pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/job-thread/turns'))return json(turns);
  if(path.includes('/api/threads/job-thread/items')||path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs')||path.includes('/media/')||path.includes('/sites'))return json([]);
  if(path.endsWith('/api/threads/job-thread/job-group-drafts'))return json(draft.state==='AWAITING_CONFIRMATION'?[draft]:[]);
  if(path.endsWith('/api/job-group-drafts/draft-group')&&request.method()==='PATCH'){patches++;const value=request.postDataJSON();assert.equal(value.expected_digest,draft.request_digest);assert.deepEqual(value.jobs.map(item=>item.local_job_id),['paid-image']);draft={...draft,jobs:value.jobs,request_digest:'2'.repeat(64)};return json(draft)}
  if(path.endsWith('/api/job-group-drafts/draft-group/confirm')){confirms++;confirmedJobs=draft.jobs.map(item=>item.local_job_id);draft={...draft,state:'CONFIRMED'};return json({group:{id:'group',member_count:confirmedJobs.length},jobs:draft.jobs})}
  return json([]);
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24767/');await page.waitForSelector('.new-thread');await page.getByText('并行生成素材',{exact:true}).click();await page.waitForSelector('.job-group-confirmations');
 assert.equal(await page.locator('.job-group-member').count(),2);assert.match(await page.locator('.job-group-confirmations').textContent(),/整组确认后同时启动/);
 await page.getByTitle('移除 free-analysis').click();await page.waitForFunction(()=>document.querySelectorAll('.job-group-member').length===1);assert.equal(patches,1);assert.match(await page.locator('.job-group-confirm').textContent(),/确认 1 项并启动/);
 await page.setViewportSize({width:390,height:844});assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
 await page.locator('.job-group-confirm').click();await page.waitForFunction(()=>!document.querySelector('.job-group-confirmations'));assert.equal(confirms,1);assert.deepEqual(confirmedJobs,['paid-image']);assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,edited:true,whole_group_confirmed_once:true,accepted_jobs:confirmedJobs,mobile:true,page_errors:0}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
