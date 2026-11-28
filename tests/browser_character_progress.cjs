const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 const now=new Date().toISOString(),started=new Date(Date.now()-10000).toISOString();
 const thread={id:'character-thread',title:'字符进度测试',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',
  updated_at:now,latest_turn_id:'turn',latest_turn_state:'RUNNING',unread_completion:false};
 const turn={id:'turn',thread_id:thread.id,harness:'codex',model:'gpt-5.6',reasoning_level:'standard',
  state:'RUNNING',updated_at:now};
 const initialText='开场',liveText='模型正在持续返回可见文字，用于验证主聊天框的累计字符数字会平滑增长。';
 let graphCharacters=0,sseSent=false;
 await page.route('**/api/**',async route=>{const url=new URL(route.request().url()),path=url.pathname,
  json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/character-thread/turns'))return json([turn]);
  if(path.endsWith('/api/threads/character-thread/items'))return json([
   {id:'user',thread_id:thread.id,turn_id:turn.id,sequence:1,type:'user_message',status:'completed',summary:'继续生成',payload:{},created_at:started,updated_at:started},
   {id:'agent',thread_id:thread.id,turn_id:turn.id,sequence:2,type:'agent_message',status:'in_progress',summary:initialText,payload:{_execution_id:'execution',_runtime_sequence:1},created_at:started,updated_at:now},
  ]);
  if(path.endsWith('/api/turns/turn/events')){
   if(!sseSent){sseSent=true;await new Promise(resolve=>setTimeout(resolve,700));
    const body=`event: live_text_delta\ndata: ${JSON.stringify({execution_id:'execution',item_id:'agent',parts:[{sequence:2,text:liveText}]})}\n\n`;
    return route.fulfill({status:200,contentType:'text/event-stream',body});}
   return route.fulfill({status:200,contentType:'text/event-stream',body:''});
  }
  if(path.endsWith('/api/threads/character-thread/execution-graph')){
   const node={id:'turn:primary',type:'primary_harness',role:'主任务执行员',title:'codex · gpt-5.6',state:'RUNNING',
    visual_role:'primary_harness',location:'sandbox',location_label:'Cloud Sandbox',started_at:started,ended_at:null,
    duration_ms:0,retries:0,recovery_count:0,activity_count:0,turn_id:'turn',harness_view:true,objective:'继续生成',
    harness:'codex',model:'gpt-5.6',token_usage:{ordinary_input_tokens:0,cache_read_input_tokens:0,cache_write_input_tokens:0,output_tokens:0},
    model_rounds:[{index:1,request_id:'request',model:'gpt-5.6',phase:'streaming',status:'in_progress',started_at:started,
     ended_at:null,duration_ms:0,first_output_ms:400,ordinary_input_tokens:0,cache_read_input_tokens:0,
     cache_write_input_tokens:0,output_tokens:0,output_characters:graphCharacters,error_code:''}]};
   const cursor=`characters-${graphCharacters}`;
   if(url.searchParams.get('cursor')===cursor)return json({cursor,reset:false,nodes:[],edges:[]});
   return json({cursor,reset:true,nodes:[node],edges:[],metadata:{},historical_available:false});
  }
  return json([]);
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24777/');
 await page.locator('.topbar .mobile-only').click();
 await page.locator('.thread-select').filter({hasText:'字符进度测试'}).click();
 const chat=page.locator('.agent-character-progress');
 await chat.waitFor();
 assert.equal(Number(await chat.getAttribute('data-character-count')),[...initialText].length);
 const finalChat=[...(initialText+liveText)].length;
 await page.waitForFunction(({start,end})=>{const value=Number(document.querySelector('.agent-character-progress')?.dataset.characterCount);return value>start&&value<end},{start:[...initialText].length,end:finalChat});
 await page.waitForFunction(end=>Number(document.querySelector('.agent-character-progress')?.dataset.characterCount)===end,finalChat);
 assert.match(await chat.textContent(),new RegExp(`本轮已生成.*${finalChat}.*字符`));
 const evidenceRoot=process.env.TEST_EVIDENCE_ROOT||'/data/wujing02/sandbox-harness-platform-parallel-ali/.data/evidence/browser';
 fs.mkdirSync(evidenceRoot,{recursive:true});
 await page.screenshot({path:`${evidenceRoot}/character-progress-chat-mobile.png`,fullPage:true});

 await page.locator('.execution-graph-trigger').click();
 await page.locator('[data-node-id="turn:primary"]').click();
 await page.locator('.harness-model-rounds > summary').click();
 const graphCount=page.locator('[data-token-kind="characters"]');
 await graphCount.waitFor();assert.equal(Number(await graphCount.getAttribute('data-token-value')),0);
 graphCharacters=160;
 await page.waitForFunction(()=>{const value=Number(document.querySelector('[data-token-kind="characters"]')?.dataset.tokenValue);return value>0&&value<160});
 await page.waitForFunction(()=>Number(document.querySelector('[data-token-kind="characters"]')?.dataset.tokenValue)===160);
 await page.locator('.execution-details').evaluate(node=>{node.scrollTop=node.scrollHeight});
 await page.screenshot({path:`${evidenceRoot}/character-progress-mobile.png`,fullPage:true});
 await page.getByRole('button',{name:'关闭运行图'}).click();
 await page.setViewportSize({width:1280,height:900});
 await page.screenshot({path:`${evidenceRoot}/character-progress-chat-desktop.png`,fullPage:true});
 await page.locator('.execution-graph-trigger').click();
 await page.locator('[data-node-id="turn:primary"]').click();
 await page.locator('.harness-model-rounds > summary').click();
 await page.locator('.execution-details').evaluate(node=>{node.scrollTop=node.scrollHeight});
 await page.screenshot({path:`${evidenceRoot}/character-progress-desktop.png`,fullPage:true});
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,chat_start:[...initialText].length,chat_final:finalChat,graph_start:0,graph_final:160,animated_intermediate:true,page_errors:0}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
