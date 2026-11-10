const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox']});
 const page=await browser.newPage({viewport:{width:1280,height:800}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 const now=new Date().toISOString(),catalog=[{id:'claude',label:'Claude',available:false,default_model:'claude-4.6-sonnet',models:[{id:'claude-4.6-sonnet',label:'Claude 4.6 Sonnet',available:false,error_code:'http_502',reasoning:[{id:'standard',label:'标准'},{id:'deep',label:'深入'}]}]},{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,latency_ms:2500,reasoning:[{id:'standard',label:'标准'}]}]},{id:'deepseek',label:'DeepSeek',available:true,default_model:'deepseek-v4.1-flash',models:[{id:'deepseek-v4.1-flash',label:'DeepSeek 4.1 Flash',available:true,latency_ms:420,reasoning:[{id:'standard',label:'标准'}]},{id:'deepseek-r1',label:'DeepSeek R1',available:false,error_code:'http_500',reasoning:[{id:'standard',label:'标准'}]}]}];
 const threads=[{id:'running',title:'Running thread',harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',updated_at:now,latest_turn_state:'RUNNING'},{id:'done',title:'Done thread',harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',updated_at:now,latest_turn_state:'COMPLETED'}];
 await page.route('**/api/**',async route=>{const url=new URL(route.request().url()),path=url.pathname;
  const json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true});if(path.endsWith('/api/catalog/harnesses'))return json(catalog);if(path.endsWith('/api/threads'))return json(threads);
  if(path.endsWith('/api/threads/running/turns'))return json([{id:'turn-running',thread_id:'running',harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',state:'RUNNING'}]);
  if(path.endsWith('/api/threads/done/turns'))return json([{id:'turn-done',thread_id:'done',harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',state:'COMPLETED'}]);
  if(path.includes('/api/threads/running/items')){await new Promise(r=>setTimeout(r,300));return json([{id:'old-item',thread_id:'running',turn_id:'turn-running',sequence:1,type:'user_message',status:'completed',summary:'OLD CONTENT',payload:{},created_at:now,updated_at:now}])}
  if(path.includes('/items')||path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs'))return json([]);
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});return json({});
 });
 await page.goto('http://127.0.0.1:22766/');await page.waitForSelector('.new-thread');
 assert.equal(await page.locator('.thread-status.running').count(),1);assert.equal(await page.locator('.thread-status.done').count(),1);
 await page.locator('.thread-list button').filter({hasText:'Running thread'}).click();await page.waitForTimeout(20);await page.locator('.new-thread').click();await page.waitForTimeout(500);
 assert.ok(await page.getByLabel('Harness').isVisible());assert.equal(await page.getByText('OLD CONTENT',{exact:true}).count(),0);assert.ok(await page.getByText('今天想做什么？',{exact:true}).isVisible());
 assert.equal(await page.getByLabel('Harness').inputValue(),'deepseek');assert.equal(await page.getByLabel('模型').inputValue(),'deepseek-v4.1-flash');assert.equal(await page.getByLabel('模型').locator('option[value="deepseek-r1"]').evaluate(option=>option.disabled),true);
 await page.locator('.thread-list button').filter({hasText:'Done thread'}).click();await page.waitForTimeout(100);
 assert.equal(await page.getByLabel('Harness').count(),0);assert.ok(await page.getByLabel('模型').isVisible());assert.ok(await page.getByLabel('推理强度').isVisible());assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,late_refresh_isolated:true,new_thread_harness:true,thread_model_reasoning_only:true,status_indicators:true,unavailable_models_disabled:true}));
}finally{if(browser)await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
