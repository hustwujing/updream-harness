const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1360,height:900}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.addInitScript(()=>{class FakeWebSocket{static OPEN=1;constructor(){this.readyState=0;setTimeout(()=>{this.readyState=1;this.onopen?.();this.onmessage?.({data:JSON.stringify({type:'connected',offset:0})});const text=new TextEncoder().encode('sandbox$ '),frame=new Uint8Array(text.length+1);frame[0]=1;frame.set(text,1);this.onmessage?.({data:frame.buffer})},10)}send(value){if(value instanceof ArrayBuffer||ArrayBuffer.isView(value)){const bytes=new Uint8Array(value.buffer||value);if(bytes[0]===0){const text=new TextDecoder().decode(bytes.slice(1));const output=new TextEncoder().encode(text),frame=new Uint8Array(output.length+1);frame[0]=1;frame.set(output,1);setTimeout(()=>this.onmessage?.({data:frame.buffer}),5)}}}close(){this.readyState=3;this.onclose?.({code:1000})}}window.WebSocket=FakeWebSocket});
 const now=new Date().toISOString(),thread={id:'sandbox-thread',title:'沙箱工作台测试',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn-done',latest_turn_state:'COMPLETED',unread_completion:false};let controlled=false,fileText='hello sandbox';
 const status=()=>({state:'live',sandbox_id:'sandbox-test',harness:'codex',image:'sha256:test',turn_state:'idle',control_state:controlled?'user_controlled':'observer',checkpoint_available:true,persistent_file_count:2,overview:{execution:'idle',cpu_percent:7.5,cpu_count:4,memory_used:104857600,memory_total:1073741824,disk_used:209715200,disk_total:2147483648,process_count:5},capabilities:{files:true,processes:true,metrics:true,ports:true,terminal:controlled}});
 await page.route('**/api/**',async route=>{const req=route.request(),url=new URL(req.url()),path=url.pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.includes('/api/sandbox-preview/'))return route.fulfill({status:200,contentType:'text/html',body:'<html><body><h1>Preview OK</h1></body></html>'});
  if(path.endsWith('/api/session'))return json({ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/sandbox-thread/turns'))return json([{id:'turn-done',thread_id:thread.id,harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now}]);
  if(path.endsWith('/api/threads/sandbox-thread/items'))return json([{id:'u',thread_id:thread.id,turn_id:'turn-done',sequence:1,type:'user_message',status:'completed',summary:'inspect sandbox',payload:{},created_at:now,updated_at:now}]);
  if(path.endsWith('/api/threads/sandbox-thread/sandbox'))return json(status());
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/takeover')){controlled=true;return json({...status(),state:'user_controlled'})}
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/release')){controlled=false;return json({released:true,checkpointed:true})}
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/terminal-ticket'))return json({ticket:'fake-ticket',session_id:'pty',expires_in:60});
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/files'))return json({items:[{path:'project/readme.txt',name:'readme.txt',type:'file',size:fileText.length,mime:'text/plain',modified_at:now},{path:'project/src',name:'src',type:'directory',size:0,mime:'application/octet-stream',modified_at:now}]});
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/file')){if(req.method()==='PUT'){fileText=await req.postData()||'';return json({path:'project/readme.txt',size:fileText.length,updated:true})}return route.fulfill({status:200,contentType:'text/plain',body:fileText})}
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/processes'))return json({items:[{pid:42,ppid:1,name:'python3',status:'sleeping',memory_bytes:10485760,cpu_percent:1,started_at:now}]});
  if(path.includes('/sandbox/processes/42/stop'))return json({pid:42,stopped:true});
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/ports'))return json({items:[{port:3000,address:'0.0.0.0',pid:42,process:'python3',protocol:'http'}]});
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/ports/3000/grant'))return json({id:'grant',port:3000,preview_url:'/api/sandbox-preview/grant/',expires_at:now});
  if(path.endsWith('/api/threads/sandbox-thread/sandbox/audit'))return json({items:[{id:'a1',action:'sandbox.restored',detail:{},created_at:now}]});
  if(path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs')||path.includes('/media/drafts')||path.includes('/media/jobs'))return json([]);
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:22766/');await page.waitForSelector('.new-thread');await page.getByText('沙箱工作台测试').click();await page.getByTitle('Workspace 与 Sandbox').click();await page.getByRole('button',{name:'Sandbox',exact:true}).click();
 await page.waitForSelector('.sandbox-workbench');await page.waitForFunction(()=>document.querySelector('.sandbox-overview')?.innerText.includes('7.5%'));assert.match(await page.locator('.sandbox-overview').textContent(),/CPU.*7.5%.*内存/s);await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/sandbox-workbench-overview.png',fullPage:true});
 await page.getByTitle('文件').click();await page.getByText('readme.txt',{exact:true}).click();await page.waitForSelector('.sandbox-editor');assert.equal(await page.locator('.sandbox-editor textarea').inputValue(),'hello sandbox');assert.equal(await page.locator('.sandbox-editor textarea').getAttribute('readonly'),'');await page.locator('.sandbox-editor header button').click();
 await page.getByRole('button',{name:'接管'}).click();await page.waitForFunction(()=>document.body.innerText.includes('用户接管'));await page.getByTitle('终端').click();await page.waitForSelector('.xterm-screen');await page.waitForFunction(()=>document.querySelector('.xterm-rows')?.innerText.includes('sandbox$'));
 await page.getByTitle('进程').click();await page.waitForFunction(()=>document.body.innerText.includes('python3'));assert.equal(await page.getByTitle('停止进程').count(),1);
 await page.getByTitle('端口').click();await page.getByText('localhost:3000').click();await page.waitForSelector('.sandbox-port-preview iframe');const frame=page.frames().find(value=>value!==page.mainFrame());await frame.waitForSelector('h1');assert.equal(await frame.locator('h1').textContent(),'Preview OK');await page.locator('.sandbox-port-preview header button').click();
 await page.getByTitle('审计').click();await page.waitForFunction(()=>document.body.innerText.includes('sandbox.restored'));
 await page.setViewportSize({width:390,height:844});await page.waitForTimeout(150);assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/sandbox-workbench-mobile.png',fullPage:true});
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,overview:true,files:true,terminal:true,processes:true,ports:true,audit:true,mobile:true,page_errors:0}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
