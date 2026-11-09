const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{
 let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox']});
  for(const viewport of [{name:'desktop',width:1440,height:960},{name:'mobile',width:390,height:844}]){
   const page=await browser.newPage({viewport});const errors=[];page.on('pageerror',error=>errors.push(error.message));
   await page.goto('http://127.0.0.1:22766/');await page.waitForSelector('.new-thread');
   if(viewport.name==='mobile'){await page.locator('.topbar .mobile-only').click();await page.waitForSelector('.sidebar.open');}
   const thread=page.getByRole('button',{name:/制作一支产品介绍短片/});await thread.click();
   if(viewport.name==='mobile'){await page.waitForSelector('.sidebar:not(.open)');await page.waitForTimeout(220);}
   await page.waitForSelector('.agent-message');assert.match(await page.locator('.agent-copy').innerText(),/30 秒版本/);
   assert.equal(await page.locator('.activity-row').count(),2);assert.equal(await page.locator('.artifact-row').count(),1);
   if(viewport.name==='mobile')await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/ui-mobile.png',fullPage:true});
   await page.getByTitle('Workspace').click();await page.waitForSelector('.inspector.open');
   if(viewport.name==='mobile'){assert.equal(await page.locator('.sidebar.open').count(),0);await page.waitForTimeout(220);}
   if(viewport.name==='desktop')assert.ok((await page.locator('.transcript').boundingBox()).width>500);
   await page.locator('.composer-bar select').nth(0).selectOption('codex');
   assert.equal(await page.locator('.composer-bar select').nth(2).locator('option').count(),4);
   await page.locator('.composer-bar select').nth(0).selectOption('deepseek');
   assert.equal(await page.locator('.composer-bar select').nth(2).locator('option').count(),2);
   assert.deepEqual(errors,[]);
   await page.screenshot({path:`/data2/wujing03/sandbox-harness-platform/.data/ui-${viewport.name}-workspace.png`,fullPage:true});
   await page.close();
  }
  console.log(JSON.stringify({passed:true,viewports:['desktop','mobile'],catalog_mapping:true,transcript:true,workspace:true}));
 }finally{if(browser)await browser.close();}
})().catch(error=>{console.error(error);process.exit(1)});
