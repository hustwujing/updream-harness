const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:820}}),errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 await page.addInitScript(()=>{window.EventSource=class{addEventListener(){}close(){}}});
 const now=new Date().toISOString(),thread={id:'thread',title:'确认锁测试',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn',latest_turn_state:'AWAITING_USER'},turn={id:'turn',thread_id:'thread',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'AWAITING_USER',updated_at:now};
 let interaction={id:'interaction',turn_id:'turn',kind:'question',status:'pending',request:{prompt:'请选择输出格式',questions:[{id:'format',question:'输出格式',options:['mp4','webm']}]},response:null},respondCalls=0,releaseRespond,failNext=true;
 const json=(route,value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
 await page.route('**/api/**',async route=>{const request=route.request(),path=new URL(request.url()).pathname;
  if(path.endsWith('/api/session'))return json(route,{ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json(route,[{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json(route,[thread]);
  if(path.endsWith('/api/threads/thread/turns'))return json(route,[turn]);
  if(path.endsWith('/api/threads/thread/items'))return json(route,[{id:'user',thread_id:'thread',turn_id:'turn',sequence:1,type:'user_message',status:'completed',summary:'开始任务',payload:{},created_at:now,updated_at:now}]);
  if(path.endsWith('/api/threads/thread/interactions'))return json(route,[interaction]);
  if(path.endsWith('/api/interactions/interaction/respond')){
   respondCalls+=1;const submitted=request.postDataJSON();
   await new Promise(resolve=>{releaseRespond=resolve});
   if(failNext){failNext=false;return json(route,{detail:'interaction_delivery_failed'},503)}
   interaction={...interaction,status:'responding',response:{action:submitted.action,answers:submitted.answers}};
   return json(route,interaction);
  }
  if(path.endsWith('/api/threads/thread/pause'))return json(route,{thread_id:'thread',state:'RUNNING',generation:0});
  return json(route,[]);
 });
 await page.goto(new URL('?thread=thread',process.env.TEST_BASE_URL||'http://127.0.0.1:22892/').toString());
 const card=page.locator('.interaction-card');await card.waitFor();await card.locator('select').selectOption('mp4');
 const submit=card.getByRole('button',{name:'提交回答'});
 await submit.evaluate(node=>{node.click();node.click()});
 await card.locator('.interaction-receipt').waitFor();
 assert.equal(respondCalls,1);assert.equal(await card.locator('select').count(),0);assert.equal(await card.locator('.interaction-actions').count(),0);
 assert.match(await card.textContent(),/正在确认送达.*mp4/s);
 releaseRespond();await card.getByRole('alert').waitFor();
 assert.equal(await card.locator('select').count(),1);assert.equal(await card.locator('select').inputValue(),'mp4');

 await card.getByRole('button',{name:'提交回答'}).evaluate(node=>{node.click();node.click()});
 await card.locator('.interaction-receipt').waitFor();assert.equal(respondCalls,2);
 releaseRespond();await page.waitForFunction(()=>document.querySelector('.interaction-card')?.textContent.includes('等待确认送达'));
 assert.equal(await card.locator('select').count(),0);assert.equal(await card.locator('.interaction-actions').count(),0);
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,immediate_lock:true,double_click_bounded:true,failure_restored:true,durable_lock:true}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
