const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});try{
 const page=await browser.newPage({viewport:{width:1280,height:800}}),now=new Date().toISOString();
 await page.addInitScript(()=>{window.EventSource=class{addEventListener(){}close(){}}});
 const thread={id:'groups',title:'Activity groups',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now};
 const turn={id:'turn',thread_id:'groups',harness:'codex',model:'gpt-5.6',state:'RUNNING',created_at:now,updated_at:now};
 const make=(prefix,count,status,offset)=>Array.from({length:count},(_,index)=>({id:`${prefix}-${index}`,thread_id:'groups',turn_id:'turn',sequence:offset+index,type:'tool',status,summary:'bash',payload:{detail:{description:`${prefix} ${index}`}},created_at:now,updated_at:now}));
 const items=[{id:'message',thread_id:'groups',turn_id:'turn',sequence:1,type:'user_message',status:'completed',summary:'test',payload:{}},...make('running',8,'in_progress',10),...make('failed',7,'failed',30),...make('completed',9,'completed',50)];
 await page.route('**/api/**',route=>{const path=new URL(route.request().url()).pathname;let body=[];
  if(path.endsWith('/session'))body={ready:true};else if(path.endsWith('/catalog/harnesses'))body=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];else if(path.endsWith('/threads'))body=[thread];else if(path.endsWith('/turns'))body=[turn];else if(path.endsWith('/items'))body=items;
  return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 });
 await page.goto('http://127.0.0.1:22892/?thread=groups');await page.waitForSelector('.activity-group.running');
 assert.ok(await page.getByText('正在执行 8 · 已记录 16',{exact:true}).isVisible());
 assert.equal(await page.locator('.activity-group.running > .activity-row:visible').count(),5);
 assert.ok(await page.getByText('另有 3 个正在执行',{exact:true}).isVisible());
 assert.equal(await page.locator('.activity-group.history > .activity-row:visible').count(),8);
 assert.ok(await page.getByText('较早的 8 个步骤',{exact:true}).isVisible());
 assert.equal(await page.getByText('失败或中断',{exact:true}).count(),0);
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/activity-groups.png'});
 console.log(JSON.stringify({passed:true,running_visible:5,history_visible:8,no_separate_failure_group:true,counts_visible:true}));
 }finally{await browser.close()}})().catch(error=>{console.error(error);process.exit(1)});
