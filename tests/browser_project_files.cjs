const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');

(async()=>{
 const token=process.env.UI_SESSION_TOKEN,threadId=process.env.UI_THREAD_ID;
 assert.ok(token&&threadId,'UI_SESSION_TOKEN and UI_THREAD_ID are required');
 let browser;
 try{
  browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
  for(const viewport of [{name:'desktop',width:1440,height:900},{name:'mobile',width:390,height:844}]){
   const page=await browser.newPage({viewport});const errors=[];page.on('pageerror',error=>errors.push(error.message));
   await page.context().addCookies([{name:'sandbox_harness_session',value:token,url:'http://127.0.0.1:24766/'}]);
   await page.goto('http://127.0.0.1:24766/');await page.waitForSelector('.new-thread');
   if(viewport.name==='mobile'){await page.locator('.topbar .mobile-only').click();await page.waitForSelector('.sidebar.open');}
   const target=page.locator('.thread-list button').filter({hasText:'Workspace visual fixture'});
   await target.click();await page.waitForTimeout(150);
   await page.getByTitle('Workspace 与 Sandbox').click();await page.waitForSelector('.inspector.open');
   const files=page.locator('.project-file');await files.first().waitFor();
   assert.equal(await files.count(),3);
   assert.ok(await page.getByText('项目素材',{exact:true}).isVisible());
   assert.ok(await page.getByText('项目成果',{exact:true}).isVisible());
   const expectedAsset=viewport.name==='desktop'?'reference.txt':'renamed-reference.txt';
   assert.ok(await page.getByText(expectedAsset,{exact:true}).isVisible());
   assert.ok(await page.getByText('first.json',{exact:true}).isVisible());
   assert.ok(await page.getByText('second.json',{exact:true}).isVisible());
   const panel=await page.locator('.inspector.open').boundingBox();
   assert.ok(panel&&panel.x>=0&&panel.x+panel.width<=viewport.width);
   if(viewport.name==='desktop'){
    const asset=page.locator('.project-file-wrap').filter({hasText:'reference.txt'});
    await asset.getByTitle('版本记录').click();await asset.locator('.project-file-history').waitFor();
    await asset.getByTitle('重命名').click();await asset.locator('input').fill('renamed-reference.txt');await asset.getByRole('button',{name:'保存'}).click();
    await page.getByText('renamed-reference.txt',{exact:true}).waitFor();
    const upload='/data2/wujing03/sandbox-harness-platform-parallel/.data/browser-upload.txt';fs.writeFileSync(upload,'browser-upload');
    await page.locator('.inspector input[type=file]').setInputFiles(upload);await page.getByText('browser-upload.txt',{exact:true}).waitFor();
    const uploaded=page.locator('.project-file-wrap').filter({hasText:'browser-upload.txt'});await uploaded.getByTitle('删除').click();
    await page.getByText('browser-upload.txt',{exact:true}).waitFor({state:'detached'});
   }
   assert.deepEqual(errors,[]);
   await page.screenshot({path:`/data2/wujing03/sandbox-harness-platform-parallel/.data/project-files-${viewport.name}.png`,fullPage:true});
   await page.close();
  }
  console.log(JSON.stringify({passed:true,project_files:3,viewports:['desktop','mobile']}));
 }finally{if(browser)await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
