const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1440,height:900}});const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  page.on('requestfailed',request=>errors.push(`request failed: ${request.url()} ${request.failure()?.errorText}`));
  await page.addInitScript(()=>{window.streams=[];window.EventSource=class{constructor(){this.events={};window.streams.push(this)}addEventListener(name,fn){this.events[name]=fn}close(){}}});
  let itemStatus='in_progress',inputStatus='pending',blockItems=false,releaseItems;
  const timestamp=new Date().toISOString();
  const thread={id:'thread',title:'Input status',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:timestamp};
  const turn={id:'turn',thread_id:'thread',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'RUNNING',created_at:timestamp,updated_at:timestamp};
  const longName='QQ_2026-11-10_17-07-50-with-dolphin-updream.png';
  await page.route('**/api/**',async route=>{
   const path=new URL(route.request().url()).pathname;let body=[];
   if(path.endsWith('/session'))body={ready:true};
   else if(path.endsWith('/catalog/harnesses'))body=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];
   else if(path.endsWith('/threads'))body=[thread];
   else if(path.endsWith('/turns'))body=[turn];
   else if(path.endsWith('/items')){
    const snapshot=itemStatus,snapshotInput=inputStatus;
    if(blockItems)await new Promise(resolve=>{releaseItems=resolve});
    body=[{id:'message',thread_id:'thread',turn_id:'turn',sequence:1,type:'user_message',status:snapshot,summary:'继续处理',payload:{input_id:'input',input_status:snapshotInput,delivery_mode:snapshotInput==='delivered'?'native_stream':'pending'}}];
   }else if(path.endsWith('/project-files'))body=[{id:'file',path:`assets/${longName}`,mime:'image/png',version:1,size:1024}];
   await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
  });
  const base=process.env.TEST_BASE_URL||'http://127.0.0.1:22892/';await page.goto(new URL('?thread=thread',base).toString());
  await page.waitForSelector('.input-status',{timeout:5000}).catch(async error=>{
   console.error({errors,url:page.url(),text:(await page.locator('body').innerText()).slice(0,1000)});throw error;
  });
  inputStatus='delivered';await page.evaluate(()=>streams[0].events['item.updated']({}));await page.waitForFunction(()=>!document.querySelector('.input-status'));
  blockItems=true;await page.evaluate(()=>streams[0].events['item.updated']({}));
  for(let attempt=0;attempt<100&&!releaseItems;attempt++)await page.waitForTimeout(10);
  assert.ok(releaseItems,'first refresh did not reach the blocked item request');
  itemStatus='completed';inputStatus='applied';
  await page.evaluate(()=>streams[0].events['item.updated']({}));
  await page.waitForTimeout(150);blockItems=false;releaseItems();
  await page.waitForFunction(()=>!document.querySelector('.input-status'));
  await page.getByTitle('Workspace 与 Sandbox').click();await page.getByRole('button',{name:'Workspace',exact:true}).click();
  const name=page.getByText(longName,{exact:true}),metrics=await name.evaluate(node=>({clientWidth:node.clientWidth,scrollWidth:node.scrollWidth,parentWidth:node.parentElement.clientWidth,parentOverflow:getComputedStyle(node.parentElement).overflow}));
  assert.ok(metrics.clientWidth<=metrics.parentWidth&&metrics.parentOverflow==='hidden',`long filename must stay within its clipped row: ${JSON.stringify(metrics)}`);
  assert.equal(await name.getAttribute('title'),longName);
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({passed:true,delivered_hint_hidden:true,status_refreshed:true,long_filename_clipped:true}));
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
