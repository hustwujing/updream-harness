const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});const page=await browser.newPage({viewport:{width:1280,height:800}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.context().addCookies([{name:'sandbox_harness_session',value:process.env.UI_SESSION_TOKEN,url:'http://127.0.0.1:24766/'}]);
 await page.goto('http://127.0.0.1:24766/');await page.waitForSelector('.new-thread');await page.locator('.thread-list button').filter({hasText:process.env.UI_THREAD_TITLE}).click();
 await page.getByRole('button',{name:'Workspace',exact:true}).click();const job=page.locator('.background-job').filter({hasText:'browser-cancel'});await job.waitFor({timeout:30000});
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform-parallel/.data/background-job-running.png',fullPage:true});await job.getByRole('button',{name:/停止/}).click();
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,job_visible:true,cancel_clicked:true}));
}finally{if(browser)await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
