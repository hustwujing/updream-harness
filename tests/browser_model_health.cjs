const {chromium}=require('playwright');const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.addInitScript(()=>{const original=setInterval;window.catalogTicks=[];window.setInterval=(fn,ms,...args)=>{if(ms===60000)window.catalogTicks.push(fn);return original(fn,ms,...args)};window.EventSource=class{addEventListener(){}close(){}}});
  const catalogSuccessAt='2026-11-17T08:00:00Z';let phase='available',thread=null,turnState='COMPLETED',submitted=null,appends=0,cancelling=false,pauseState='RUNNING',catalogFailure=false,inventoryStale=false;
  const levels=[{id:'quick',label:'快速'},{id:'standard',label:'标准'},{id:'maximum',label:'最大'}];
  await page.route('**/api/**',async route=>{
   const req=route.request(),path=new URL(req.url()).pathname;let body=[];
   if(path.endsWith('/session'))body={ready:true};
   else if(path.endsWith('/catalog/harnesses')){if(catalogFailure)return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({detail:'catalog_offline'})});body=[{id:'codex',label:'Codex',default_model:'gpt-5.6',available:true,inventory:{status:inventoryStale?'stale':'fresh',stale:inventoryStale,error:inventoryStale?'inventory_offline':'',last_success_at:catalogSuccessAt},models:[
    {id:'gpt-5.6',label:'GPT 5.6',available:true,status:'available',reasoning:levels},
    {id:'gpt-6-astra',label:'GPT 6 Astra',available:phase!=='unavailable',status:phase,blocking_reason:phase==='unavailable'?'model_not_supported':null,reasoning:levels}]}]}
   else if(path.endsWith('/pause')){if(req.method()==='POST')pauseState='PAUSING';body={thread_id:'health-thread',state:pauseState,generation:1}}
   else if(path.endsWith('/threads')){
    if(req.method()==='POST'){thread={...req.postDataJSON(),id:'health-thread',updated_at:new Date().toISOString()};body=thread}else body=thread?[thread]:[];
   }else if(path.endsWith('/turns')){
    if(req.method()==='POST'){submitted=req.postDataJSON();body={id:'turn',state:'QUEUED',...submitted}}
    else body=[{id:'turn',state:turnState,cancel_requested:cancelling,harness:'codex',model:submitted.model}];
   }else if(path.endsWith('/cancel')){cancelling=true;body={accepted:true}}else if(path.endsWith('/inputs')){appends++;body={id:'input',status:'delivered'}}
   await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
  });
  const refreshCatalog=()=>page.evaluate(()=>catalogTicks.forEach(fn=>fn()));
  await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:22892/');await page.getByLabel('模型',{exact:true}).selectOption('gpt-6-astra');await page.getByLabel('推理强度').selectOption('maximum');
  phase='busy';await refreshCatalog();await page.waitForSelector('.model-health-note');
  assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');
  assert.equal(await page.getByLabel('推理强度').inputValue(),'maximum');
  assert.match(await page.locator('.model-health-note').innerText(),/可尝试/);
  catalogFailure=true;await refreshCatalog();await page.waitForSelector('.catalog-stale');assert.match(await page.locator('.catalog-stale').textContent(),/服务暂时不可用，正在恢复/);assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');catalogFailure=false;inventoryStale=true;await page.locator('.catalog-stale button').click();await page.waitForFunction(()=>document.querySelector('.catalog-stale')?.textContent.includes('Codex'));inventoryStale=false;await page.locator('.catalog-stale button').click();await page.locator('.catalog-stale').waitFor({state:'detached'});assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');
  inventoryStale=true;await page.reload();await page.waitForSelector('.catalog-stale');assert.match(await page.locator('.catalog-stale').textContent(),/上次成功/);assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');inventoryStale=false;await page.locator('.catalog-stale button').click();await page.locator('.catalog-stale').waitFor({state:'detached'});
  await page.reload();await page.waitForSelector('.model-health-note');assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');
  await page.locator('textarea').fill('hello');await page.getByRole('button',{name:'发送',exact:true}).click();
  await page.waitForURL('**/?thread=health-thread');await page.waitForTimeout(200);
  assert.equal(submitted.model,'gpt-6-astra');assert.equal(submitted.reasoning_level,'maximum');
  await page.getByLabel('模型',{exact:true}).selectOption('gpt-5.6');await page.getByLabel('推理强度').selectOption('quick');
  await page.reload();await page.waitForSelector('.thread-config');assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-5.6');assert.equal(await page.getByLabel('推理强度').inputValue(),'quick');
  await page.getByLabel('模型',{exact:true}).selectOption('gpt-6-astra');phase='unavailable';await refreshCatalog();await page.waitForFunction(()=>document.querySelector('.model-health-note')?.textContent.includes('不支持'));
  assert.equal(await page.getByLabel('模型',{exact:true}).inputValue(),'gpt-6-astra');await page.locator('textarea').fill('hello');assert(await page.getByRole('button',{name:'发送',exact:true}).isDisabled());
  turnState='RUNNING';await page.reload();await page.waitForSelector('.thread-config');await page.locator('textarea').fill('append');await page.getByRole('button',{name:'追加消息',exact:true}).click();await page.waitForTimeout(150);assert.equal(appends,1);
  await page.locator('.composer .send.stop').click();await page.waitForFunction(()=>document.querySelector('.topbar')?.textContent.includes('正在暂停'));
  assert(await page.locator('.composer .send.stop').isDisabled());assert(await page.locator('textarea').isDisabled());
  assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,busy_selectable:true,no_auto_switch:true,draft_reload:true,thread_reload:true,hard_failure_blocks_new_turn:true,active_turn_accepts_input:true,catalog_failure_visible:true,inventory_stale_visible:true,catalog_recovery_without_reload:true}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
