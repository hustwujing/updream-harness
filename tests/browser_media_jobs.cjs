const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:820}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
 const now=new Date().toISOString(),started=new Date(Date.now()-8000).toISOString(),finished=new Date(Date.now()-3000).toISOString(),thread={id:'media-thread',title:'媒体任务检查',harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn-media',latest_turn_state:'COMPLETED',unread_completion:false};
 const jobs=[
  {id:'job-image',thread_id:thread.id,turn_id:'turn-media',kind:'image',model:'seedream-5.0',provider:'ark',adapter:'ark_image',execution_mode:'sync',cancellation_mode:'unsupported',status:'running',phase:'requesting',progress:null,cancelable:false,artifact_ids:[],output_role:'workspace',created_at:started,finished_at:null},
  {id:'job-video',thread_id:thread.id,turn_id:'turn-media',kind:'video',model:'sed2',provider:'ark',adapter:'ark_video_v2',execution_mode:'async',cancellation_mode:'supported',status:'submitted',phase:'provider_queue',progress:15,cancelable:true,artifact_ids:[],output_role:'delivery',created_at:started,finished_at:null},
 {id:'job-audio',thread_id:thread.id,turn_id:'turn-media',kind:'audio',model:'speech-2.8-turbo',provider:'minimax',adapter:'minimax_t2a',execution_mode:'async',cancellation_mode:'unsupported',status:'succeeded',phase:'committed',progress:100,cancelable:false,artifact_ids:['artifact-audio'],output_role:'candidate',created_at:started,finished_at:finished},
 ];
 const visibleJobs=[];let cancelAttempts=0,refreshAttempts=0;
 const drafts=[{id:'draft-image',thread_id:thread.id,turn_id:'turn-media',kind:'image',model:'seedream-5.0',parameters:{prompt:'生成一张蓝色圆形海报',size:'2048x2048'},status:'awaiting_confirmation',expires_at:new Date(Date.now()+600000).toISOString(),job_id:'',output_role:'workspace'}];
 await page.route('**/api/**',async route=>{const path=new URL(route.request().url()).pathname,json=(value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true,authentication_required:false,authenticated:false,user:null});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'claude',label:'Claude',available:true,default_model:'claude-4.6-sonnet',models:[{id:'claude-4.6-sonnet',label:'Claude 4.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/media-thread/turns'))return json([{id:'turn-media',thread_id:thread.id,harness:'claude',model:'claude-4.6-sonnet',reasoning_level:'standard',state:'COMPLETED',updated_at:now}]);
  if(path.includes('/api/threads/media-thread/items')||path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs'))return json([]);
  if(path.endsWith('/api/threads/media-thread/media/drafts'))return json(drafts);
  if(path.endsWith('/api/turns/turn-media/media/drafts/confirm')){drafts[0]={...drafts[0],status:'confirmed',job_id:'job-image'};visibleJobs.push(...jobs);return json(jobs)}
  if(path.endsWith('/api/threads/media-thread/media/jobs'))return json(visibleJobs);
  if(path.includes('/api/media/jobs/')&&path.endsWith('/refresh')){refreshAttempts+=1;if(refreshAttempts===1)return json({detail:'refresh_failed'},503);return json({status:'running'})}
  if(path.endsWith('/api/media/jobs/job-video/cancel')){cancelAttempts+=1;if(cancelAttempts===1)return json({detail:'cancel_failed'},503);jobs[1]={...jobs[1],status:'cancel_requested',phase:'cancelling',cancelable:false};visibleJobs[1]=jobs[1];return json(jobs[1])}
  if(path.endsWith('/api/artifacts/artifact-audio'))return route.fulfill({status:200,contentType:'audio/mpeg',body:Buffer.from('ID3')});
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});
  return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24767/');await page.waitForSelector('.new-thread');
 await page.locator('.thread-list button').filter({hasText:'媒体任务检查'}).click();await page.waitForSelector('.media-confirmation');assert.match(await page.locator('.media-confirmation').textContent(),/蓝色圆形海报/);await page.getByRole('button',{name:'生成图片'}).click();await page.locator('.media-confirmation').waitFor({state:'detached'});const progress=page.locator('.media-progress');await progress.waitFor();await page.waitForFunction(()=>document.querySelector('.media-progress')?.open===true);await progress.locator('summary').click();assert.equal(await progress.evaluate(node=>node.open),false);await progress.getByLabel('刷新媒体任务').click();await page.getByRole('alert').filter({hasText:'服务暂时不可用，正在恢复'}).waitFor();await progress.getByLabel('刷新媒体任务').click();await page.getByRole('alert').filter({hasText:'服务暂时不可用，正在恢复'}).waitFor({state:'detached'});await page.getByTitle('Workspace').click();await page.waitForSelector('.media-jobs');
 assert.equal(await page.locator('.workspace-media-job').count(),3);assert.equal(await page.locator('.workspace-media-main b').count(),3);assert.ok(await page.locator('.workspace-media-main b').first().evaluate(node=>node.getBoundingClientRect().width>40));const spinners=page.locator('.workspace-media-job .media-card-spinner');assert.equal(await spinners.count(),2);assert.notEqual((await spinners.first().evaluate(node=>getComputedStyle(node).animationName)),'none');assert.match(await page.locator('.workspace-media-job').last().textContent(),/耗时 5 秒/);
 await page.setViewportSize({width:390,height:844});await page.waitForTimeout(100);assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));assert.ok(await page.locator('.workspace-media-main b').first().evaluate(node=>node.getBoundingClientRect().right<=innerWidth));await page.setViewportSize({width:1280,height:820});
 await page.getByRole('button',{name:'预览音频成果'}).click();await page.waitForSelector('.media-preview audio');await page.getByLabel('关闭预览').click();
 await page.locator('.workspace-media-detail').nth(0).click();let modal=page.getByRole('dialog');await modal.waitFor();assert.match(await modal.textContent(),/生成中/);visibleJobs[0]={...visibleJobs[0],status:'succeeded',phase:'committed',progress:100,finished_at:new Date().toISOString()};await page.waitForFunction(()=>document.querySelector('.media-job-modal')?.innerText.includes('已完成'),null,{timeout:5000});await modal.getByLabel('关闭详情').click();
 await page.locator('.workspace-media-detail').nth(1).click();modal=page.getByRole('dialog');await modal.waitFor();
 assert.match(await modal.textContent(),/sed2/);assert.match(await modal.textContent(),/provider_queue/);assert.match(await modal.textContent(),/支持/);assert.equal((await modal.textContent()).includes('API'),false);
 await page.screenshot({path:'/data/wujing02/sandbox-harness-platform-parallel-ali/.data/media-job-inspector.png',fullPage:true});
 const cancel=modal.getByRole('button',{name:'取消任务'});await cancel.click();await page.getByRole('alert').filter({hasText:'服务暂时不可用，正在恢复'}).waitFor();assert.equal(await cancel.isEnabled(),true);await cancel.click();await modal.waitFor({state:'detached'});await page.waitForTimeout(50);assert.equal(await page.locator('.workspace-media-job').count(),3);assert.equal(cancelAttempts,2);
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,draft_confirmation:true,progress_auto_open:true,progress_user_collapse:true,refresh_failure_visible:true,jobs:3,animated_running_state:true,frozen_elapsed:true,audio_preview:true,details_modal_live:true,cancel_retry:true,page_errors:0}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
