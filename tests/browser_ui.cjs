const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{
 let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
  for(const viewport of [{name:'desktop',width:1440,height:960},{name:'short-desktop',width:1280,height:600},{name:'mobile',width:390,height:844}]){
   const page=await browser.newPage({viewport});const errors=[];page.on('pageerror',error=>errors.push(error.message));
   await page.goto('http://127.0.0.1:24767/');await page.waitForSelector('.new-thread');
   await page.waitForTimeout(100);
   assert.equal(await page.locator('.thread-list section button').count(),0);
   if(viewport.name==='mobile'){
    await page.locator('.topbar .mobile-only').click();await page.waitForSelector('.sidebar.open');
    await page.locator('.new-thread').click();await page.waitForSelector('.sidebar.open',{state:'hidden'});await page.waitForTimeout(220);
   }
   if(viewport.name==='mobile')await page.screenshot({path:'/data/wujing02/sandbox-harness-platform-parallel-ali/.data/ui-mobile.png',fullPage:true});
   await page.getByTitle('Workspace 与 Sandbox').click();await page.waitForSelector('.inspector.open');
   if(viewport.name==='mobile'){assert.equal(await page.locator('.sidebar.open').count(),0);await page.waitForTimeout(220);}
   if(viewport.name==='desktop')assert.ok((await page.locator('.transcript').boundingBox()).width>500);
   assert.ok(await page.locator('.composer textarea').isVisible());
   const composerBox=await page.locator('.composer textarea').boundingBox();
   assert.ok(composerBox,'composer textarea must have a layout box');
   assert.ok(composerBox.y>=0 && composerBox.y+composerBox.height<=viewport.height,
    `composer escaped viewport: ${JSON.stringify({viewport,composerBox})}`);
   const mainBox=await page.locator('.app > main').boundingBox();
   assert.ok(mainBox,'main must have a layout box');
   assert.ok(Math.abs(mainBox.height-viewport.height)<=1,
    `main must stay viewport-height: ${JSON.stringify({viewport,mainBox})}`);
   assert.ok(await page.getByRole('button',{name:/添加文件/}).isVisible());
   await page.getByLabel('Harness').selectOption('claude');
   assert.ok(await page.getByLabel('模型').locator('option[value="claude-4.6-sonnet"]').count()>0);
   await page.getByLabel('Harness').selectOption('codex');
   assert.ok(await page.getByLabel('模型').locator('option').count()>=1);
   assert.equal(await page.getByLabel('推理强度').locator('option').count(),4);
   await page.getByLabel('Harness').selectOption('deepseek');
   assert.ok(await page.getByLabel('模型').locator('option').count()>=1);
   assert.equal(await page.getByLabel('推理强度').locator('option').count(),2);
   await page.locator('.inspector header .icon-button').click();
   await page.waitForSelector('.inspector.open',{state:'hidden'});
   assert.equal(await page.getByTitle('更多').count(),0);
   assert.ok(await page.getByTitle('Workspace 与 Sandbox').isVisible());
   assert.deepEqual(errors,[]);
   await page.screenshot({path:`/data/wujing02/sandbox-harness-platform-parallel-ali/.data/ui-${viewport.name}-workspace.png`,fullPage:true});
   await page.close();
  }
  console.log(JSON.stringify({passed:true,viewports:['desktop','short-desktop','mobile'],catalog_mapping:true,new_session_empty:true,workspace:true,composer_in_viewport:true}));
 }finally{if(browser)await browser.close();}
})().catch(error=>{console.error(error);process.exit(1)});
