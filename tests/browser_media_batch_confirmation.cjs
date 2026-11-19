const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:820}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.addInitScript(()=>{window.EventSource=class{addEventListener(){}close(){}}});
 const now=new Date().toISOString(),thread={id:'batch-thread',title:'媒体批次',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',latest_turn_id:'batch-turn',latest_turn_state:'COMPLETED',updated_at:now},turn={id:'batch-turn',thread_id:'batch-thread',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now};
 let confirms=0,drafts=[1,2].map(index=>({id:`draft-${index}`,thread_id:thread.id,turn_id:turn.id,kind:'image',model:'seedream-5.0',generation_mode:'text_to_image',parameters:{prompt:`slide ${index}`,size:'2560x1440'},references:[],status:'awaiting_confirmation',output_role:'workspace',created_at:now}));
 const items=[{id:'user',thread_id:thread.id,turn_id:turn.id,sequence:1,type:'user_message',status:'completed',summary:'生成两张图',payload:{},created_at:now,updated_at:now},{id:'generic',thread_id:thread.id,turn_id:turn.id,sequence:2,type:'artifact',status:'completed',summary:'foreground.png',payload:{artifact_id:'generic-artifact',mime:'image/png',size:100,local_job_id:'segment-dolphin'},created_at:now,updated_at:now},{id:'explicit',thread_id:thread.id,turn_id:turn.id,sequence:3,type:'artifact',status:'completed',summary:'final.pptx',payload:{artifact_id:'explicit-artifact',mime:'application/vnd.openxmlformats-officedocument.presentationml.presentation',size:100},created_at:now,updated_at:now}];
 const json=(route,value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
 await page.route('**/api/**',async route=>{const request=route.request(),path=new URL(request.url()).pathname;
  if(path.endsWith('/api/session'))return json(route,{ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json(route,[{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json(route,[thread]);
  if(path.endsWith('/api/threads/batch-thread/turns'))return json(route,[turn]);
  if(path.endsWith('/api/threads/batch-thread/items'))return json(route,items);
  if(path.endsWith('/api/threads/batch-thread/media/drafts'))return json(route,drafts);
  if(path.endsWith('/api/turns/batch-turn/media/drafts/confirm')){confirms++;drafts=drafts.map(value=>({...value,status:'confirmed'}));return json(route,[])}
  if(path.endsWith('/api/threads/batch-thread/pause'))return json(route,{thread_id:thread.id,state:'RUNNING',generation:0});
  return json(route,[])
 });
 await page.goto(new URL('?thread=batch-thread',process.env.TEST_BASE_URL||'http://127.0.0.1:24766/').toString());
 await page.waitForSelector('.media-confirmations');
 assert.match(await page.locator('.topbar').textContent(),/等待确认 2 项媒体生成/);
 assert.match(await page.locator('.artifact-gallery').textContent(),/segment-dolphin-foreground\.png/);
 assert.match(await page.locator('.artifact-gallery').textContent(),/final\.pptx/);
 const button=page.getByRole('button',{name:'确认 2 项并启动'});await button.evaluate(node=>{node.click();node.click()});
 await page.waitForFunction(()=>!document.querySelector('.media-confirmations'));assert.equal(confirms,1);assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,batch_confirm:true,waiting_status:true,semantic_artifact:true}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
