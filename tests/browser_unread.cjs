const {chromium}=require('playwright');const assert=require('node:assert/strict');
(async()=>{const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});try{
 const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.addInitScript(()=>{window.polls=[];const interval=setInterval;window.setInterval=(fn,ms,...args)=>{if(ms===2000)polls.push(fn);return interval(fn,ms,...args)};window.EventSource=class{addEventListener(){}close(){}}});
 let generation=1,state='COMPLETED',read='',reads=0,itemDelay=0;
 const thread=()=>({id:'thread',title:'Unread test',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:new Date().toISOString(),latest_turn_id:'turn-'+generation,latest_turn_state:state,unread_completion:state==='COMPLETED'&&read!=='turn-'+generation});
 await page.route('**/api/**',async route=>{const req=route.request(),path=new URL(req.url()).pathname;let body=[];
  if(path.endsWith('/session'))body={ready:true};
  else if(path.endsWith('/catalog/harnesses'))body=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];
  else if(path.endsWith('/threads'))body=[thread()];
  else if(path.endsWith('/turns'))body=[{id:'turn-'+generation,thread_id:'thread',harness:'codex',model:'gpt-5.6',state}];
  else if(path.endsWith('/read')){reads++;if(req.postDataJSON().turn_id==='turn-'+generation&&state==='COMPLETED')read='turn-'+generation;body={read_turn_id:read}}
  else if(path.includes('/items')){if(itemDelay)await new Promise(r=>setTimeout(r,itemDelay));body=[{id:'item',turn_id:'turn-'+generation,type:'agent_message',status:'completed',summary:'Finished',payload:{}}]}
  await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 });
 await page.goto('http://127.0.0.1:22892/');await page.bringToFront();
 await page.waitForSelector('.thread-status.done');
 await page.getByRole('button',{name:/Unread test/}).click();await page.waitForFunction(()=>!document.querySelector('.thread-status.done'));
 assert.equal(read,'turn-1');assert.equal(reads,1);
 await page.reload();await page.waitForSelector('.agent-message');assert.equal(await page.locator('.thread-status.done').count(),0);
 await page.locator('.new-thread').click();generation=2;state='RUNNING';await page.evaluate(()=>polls.forEach(fn=>fn()));await page.waitForSelector('.thread-status.running');
 const before=reads;await page.getByRole('button',{name:/Unread test/}).click();await page.waitForTimeout(100);assert.equal(reads,before);
 state='COMPLETED';await page.evaluate(()=>polls.forEach(fn=>fn()));await page.waitForFunction(()=>!document.querySelector('.thread-status.running'));await page.waitForTimeout(150);
 assert.equal(read,'turn-2');assert.equal(await page.locator('.thread-status.done').count(),0);
 await page.locator('.new-thread').click();generation=3;await page.evaluate(()=>polls.forEach(fn=>fn()));await page.waitForSelector('.thread-status.done');
 itemDelay=250;const previous=reads;await page.getByRole('button',{name:/Unread test/}).click();await page.locator('.new-thread').click();await page.waitForTimeout(400);
 assert.equal(reads,previous);assert.equal(await page.locator('.thread-status.done').count(),1);
 await page.getByRole('button',{name:/Unread test/}).click();await page.waitForFunction(()=>!document.querySelector('.thread-status.done'));assert.equal(read,'turn-3');
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,open_marks_read:true,reload_preserves:true,running_not_preread:true,visible_completion_read:true,new_completion_unread:true,late_fetch_does_not_mark_read:true}));
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exitCode=1});
