const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:800}});
 const now=new Date().toISOString(),catalog=[{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}];
 let submissions=0;
 await page.route('**/api/**',async route=>{const request=route.request(),path=new URL(request.url()).pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true});
  if(path.endsWith('/api/catalog/harnesses'))return json(catalog);
  if(path.endsWith('/api/threads')&&request.method()==='GET')return json([]);
  if(path.endsWith('/api/threads')&&request.method()==='POST'){submissions++;return json({id:'thread-ime',title:'updream',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now})}
  if(path.endsWith('/api/threads/thread-ime/turns')&&request.method()==='POST'){submissions++;return json({id:'turn-ime',thread_id:'thread-ime',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'QUEUED'})}
  if(path.includes('/items')||path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs')||path.endsWith('/turns'))return json([]);
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});
  return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24767/');await page.waitForSelector('.composer textarea');
 const textarea=page.locator('.composer textarea'),send=page.getByRole('button',{name:'发送',exact:true});
 await textarea.evaluate(element=>element.dispatchEvent(new CompositionEvent('compositionstart',{bubbles:true,data:'updream'})));
 await textarea.fill('updream');
 assert.equal(await send.isDisabled(),true);
 await textarea.evaluate(element=>{const event=new KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true});Object.defineProperty(event,'isComposing',{value:true});element.dispatchEvent(event)});
 assert.equal(submissions,0);assert.equal(await textarea.inputValue(),'updream');
 await send.evaluate(element=>element.click());assert.equal(submissions,0);
 await textarea.evaluate(element=>element.dispatchEvent(new CompositionEvent('compositionend',{bubbles:true,data:'updream'})));
 await textarea.press('Enter');assert.equal(submissions,0);assert.equal(await textarea.inputValue(),'updream');
 await page.waitForTimeout(100);await textarea.press('Enter');await page.waitForTimeout(50);
 assert.equal(submissions,2);
 console.log(JSON.stringify({passed:true,composition_enter_blocked:true,composition_click_blocked:true,post_composition_enter_guarded:true,normal_enter_sent:true}));
}finally{if(browser)await browser.close()}})().catch(error=>{console.error(error);process.exit(1)});
