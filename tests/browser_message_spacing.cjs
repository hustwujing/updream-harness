const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});try{
 const page=await browser.newPage({viewport:{width:1280,height:800}}),now=new Date().toISOString();
 await page.addInitScript(()=>{window.EventSource=class{addEventListener(){}close(){}}});
 const thread={id:'spacing',title:'Message spacing',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now};
 const item=(id,turnId,sequence,type,summary)=>({id,thread_id:'spacing',turn_id:turnId,sequence,type,status:'completed',summary,payload:{},created_at:now,updated_at:now});
 const items=[item('u1','turn-1',1,'user_message','第一条用户消息'),item('u1b','turn-1',2,'user_message','同轮追加消息'),item('a1','turn-1',3,'agent_message','第一条回复'),item('a1b','turn-1',4,'agent_message','同轮补充回复'),item('u2','turn-2',5,'user_message','下一轮用户消息'),item('a2','turn-2',6,'agent_message','下一轮回复')];
 await page.route('**/api/**',route=>{const path=new URL(route.request().url()).pathname;let body=[];
  if(path.endsWith('/session'))body={ready:true};else if(path.endsWith('/catalog/harnesses'))body=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];else if(path.endsWith('/threads'))body=[thread];else if(path.endsWith('/turns'))body=[{id:'turn-1',thread_id:'spacing',harness:'codex',model:'gpt-5.6',state:'COMPLETED',updated_at:now},{id:'turn-2',thread_id:'spacing',harness:'codex',model:'gpt-5.6',state:'COMPLETED',updated_at:now}];else if(path.endsWith('/items'))body=items;
  return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 });
 await page.goto((process.env.TEST_BASE_URL||'http://127.0.0.1:22892/')+'?thread=spacing');await page.waitForSelector('.turn:nth-of-type(2)');
 const boxes=await page.locator('.turn').evaluateAll(turns=>turns.map(turn=>({users:[...turn.querySelectorAll('.user-message')].map(node=>node.getBoundingClientRect().toJSON()),agents:[...turn.querySelectorAll('.agent-message')].map(node=>node.getBoundingClientRect().toJSON())})));
 const userGap=boxes[0].users[1].y-(boxes[0].users[0].y+boxes[0].users[0].height),userAgentGap=boxes[0].agents[0].y-(boxes[0].users[1].y+boxes[0].users[1].height),agentGap=boxes[0].agents[1].y-(boxes[0].agents[0].y+boxes[0].agents[0].height),turnGap=boxes[1].users[0].y-(boxes[0].agents[1].y+boxes[0].agents[1].height);
 assert.ok(Math.abs(userGap-agentGap)<1&&Math.abs(userAgentGap-agentGap)<1,JSON.stringify({userGap,userAgentGap,agentGap}));
 assert.ok(turnGap>=agentGap+20,JSON.stringify({agentGap,turnGap}));
 assert.equal(await page.locator('.agent-avatar').count(),2);
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform-parallel/.data/message-spacing.png'});
 console.log(JSON.stringify({passed:true,user_gap:userGap,user_agent_gap:userAgentGap,agent_gap:agentGap,turn_gap:turnGap}));
 }finally{await browser.close()}})().catch(error=>{console.error(error);process.exit(1)});
