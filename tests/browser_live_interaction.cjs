const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{
 const token=process.env.UI_SESSION_TOKEN,title=process.env.UI_THREAD_TITLE;
 assert.ok(token&&title);let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
  const desktop=await browser.newPage({viewport:{width:1440,height:900}});const errors=[];
  desktop.on('pageerror',error=>errors.push(error.message));
  await desktop.context().addCookies([{name:'sandbox_harness_session',value:token,url:'http://127.0.0.1:24767/'}]);
  await desktop.goto('http://127.0.0.1:24767/');await desktop.waitForSelector('.new-thread');
  await desktop.locator('.thread-list button').filter({hasText:title}).click();
  const card=desktop.locator('.interaction-card.pending');await card.waitFor({timeout:30000});
  await card.locator('select').selectOption('mp4');
  await desktop.screenshot({path:'/data/wujing02/sandbox-harness-platform-parallel-ali/.data/live-interaction-pending-desktop.png',fullPage:true});
  await card.getByRole('button',{name:'提交回答'}).click();
  await desktop.locator('.interaction-card.resolved').waitFor({timeout:30000});
  assert.deepEqual(errors,[]);await desktop.close();
  const mobile=await browser.newPage({viewport:{width:390,height:844}});const mobileErrors=[];
  mobile.on('pageerror',error=>mobileErrors.push(error.message));
  await mobile.context().addCookies([{name:'sandbox_harness_session',value:token,url:'http://127.0.0.1:24767/'}]);
  await mobile.goto('http://127.0.0.1:24767/');await mobile.waitForSelector('.new-thread');
  await mobile.locator('.topbar .mobile-only').click();
  await mobile.locator('.thread-list button').filter({hasText:title}).click();
  await mobile.locator('.interaction-card.resolved').waitFor({timeout:30000});
  const box=await mobile.locator('.interaction-card.resolved').boundingBox();
  assert.ok(box&&box.x>=0&&box.x+box.width<=390);assert.deepEqual(mobileErrors,[]);
  await mobile.screenshot({path:'/data/wujing02/sandbox-harness-platform-parallel-ali/.data/live-interaction-resolved-mobile.png',fullPage:true});
  console.log(JSON.stringify({passed:true,desktop_pending:true,mobile_reentry:true,page_errors:[]}));
 }finally{if(browser)await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
