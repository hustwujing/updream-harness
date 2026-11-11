const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const {execFileSync}=require('node:child_process');
const fs=require('node:fs');

const videoPath='/data2/wujing03/sandbox-harness-platform/.data/media-gallery-fixture.mp4';
fs.mkdirSync('/data2/wujing03/sandbox-harness-platform/.data',{recursive:true});
execFileSync('ffmpeg',['-y','-f','lavfi','-i','color=c=0x256f5b:s=320x180:d=1','-vf',
 'drawtext=text=VIDEO:x=(w-text_w)/2:y=(h-text_h)/2:fontsize=36:fontcolor=white','-pix_fmt','yuv420p',videoPath],{stdio:'ignore'});
const video=fs.readFileSync(videoPath);

function image(label,color){return Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="320" height="240"><rect width="100%" height="100%" fill="${color}"/><text x="50%" y="52%" text-anchor="middle" fill="white" font-family="sans-serif" font-size="34">${label}</text></svg>`)}

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:900}});const errors=[];page.on('pageerror',error=>errors.push(error.message));
 const now=new Date().toISOString(),thread={id:'gallery',title:'媒体缩略图画廊',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn-gallery',latest_turn_state:'COMPLETED',unread_completion:false};
 const artifacts=[
  ['image-red','海报一.png','image/png','#a43d35'],['image-blue','海报二.png','image/png','#3568a4'],
  ['image-gold','海报三.png','image/png','#a4772f'],['image-violet','海报四.png','image/png','#6e4b8f'],['image-svg','矢量图.svg','image/svg+xml','#3b7d72'],
  ['video-one','演示视频.mp4','video/mp4','#256f5b'],['report-one','生成说明.txt','text/plain',''],
 ].map(([id,name,mime],index)=>({id,thread_id:'gallery',turn_id:'turn-gallery',sequence:index+3,type:'artifact',status:'completed',summary:name,payload:{artifact_id:id,mime,size:mime.startsWith('video/')?video.length:4096},created_at:now,updated_at:now}));
 const items=[{id:'user',thread_id:'gallery',turn_id:'turn-gallery',sequence:1,type:'user_message',status:'completed',summary:'生成一组图片和视频',payload:{},created_at:now,updated_at:now},{id:'agent',thread_id:'gallery',turn_id:'turn-gallery',sequence:2,type:'agent_message',status:'completed',summary:'已完成媒体生成。',payload:{},created_at:now,updated_at:now},...artifacts];
 await page.route('**/api/**',async route=>{const path=new URL(route.request().url()).pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.includes('/api/artifacts/image-')){const id=path.split('/').at(-1),colors={'image-red':['RED','#a43d35'],'image-blue':['BLUE','#3568a4'],'image-gold':['GOLD','#a4772f'],'image-violet':['VIOLET','#6e4b8f'],'image-svg':['SVG','#3b7d72']};return route.fulfill({status:200,contentType:'image/svg+xml',body:image(...colors[id])})}
  if(path.endsWith('/api/artifacts/video-one'))return route.fulfill({status:200,contentType:'video/mp4',body:video});
  if(path.endsWith('/api/artifacts/report-one'))return route.fulfill({status:200,contentType:'text/plain',body:'media report'});
  if(path.endsWith('/api/session'))return json({ready:true});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/gallery/turns'))return json([{id:'turn-gallery',thread_id:'gallery',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now}]);
  if(path.includes('/api/threads/gallery/items'))return json(items);
  if(path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs')||path.includes('/media/drafts')||path.includes('/media/jobs'))return json([]);
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});
  return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:22766/');await page.waitForSelector('.new-thread');
 await page.locator('.thread-list button').filter({hasText:'媒体缩略图画廊'}).click();
 try{await page.waitForSelector('.artifact-gallery',{timeout:5000})}catch(error){throw new Error(`媒体画廊未渲染: ${await page.locator('body').innerText()}\n页面错误: ${errors.join(' | ')}\n${error.message}`)}
 const tiles=page.locator('.artifact-gallery .artifact-media');assert.equal(await tiles.count(),6);assert.equal(await page.locator('.artifact-row').count(),1);await page.getByAltText('矢量图.svg').waitFor();assert.ok(await page.getByAltText('矢量图.svg').evaluate(image=>image.naturalWidth>0));
 const desktopColumns=(await page.locator('.artifact-gallery').evaluate(node=>getComputedStyle(node).gridTemplateColumns.split(' ').length));assert.equal(desktopColumns,4);
 const desktopBoxes=await tiles.evaluateAll(nodes=>nodes.map(node=>{const box=node.getBoundingClientRect();return {x:box.x,y:box.y,width:box.width,height:box.height}}));
 assert.equal(new Set(desktopBoxes.slice(0,4).map(box=>box.y)).size,1);assert.ok(desktopBoxes[0].width<190&&desktopBoxes[0].width>130);
 const thumbnail=page.locator('.artifact-thumbnail video');await thumbnail.waitFor();assert.equal(await thumbnail.getAttribute('preload'),'metadata');assert.equal(await thumbnail.getAttribute('controls'),null);assert.equal(await thumbnail.getAttribute('autoplay'),null);
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/media-gallery-desktop.png',fullPage:true});
 await page.getByRole('button',{name:'预览 演示视频.mp4'}).click();const preview=page.locator('.media-preview video');await preview.waitFor();assert.equal(await preview.getAttribute('controls'),'');await page.getByLabel('关闭预览').click();
 await page.setViewportSize({width:390,height:844});await page.waitForTimeout(100);const mobileColumns=await page.locator('.artifact-gallery').evaluate(node=>getComputedStyle(node).gridTemplateColumns.split(' ').length);assert.equal(mobileColumns,2);
 const mobileBoxes=await tiles.evaluateAll(nodes=>nodes.map(node=>{const box=node.getBoundingClientRect();return {x:box.x,y:box.y,width:box.width}}));assert.equal(mobileBoxes[0].y,mobileBoxes[1].y);assert.ok(mobileBoxes[2].y>mobileBoxes[0].y);
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/media-gallery-mobile.png',fullPage:true});
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,media_tiles:6,svg_thumbnail:true,desktop_columns:4,mobile_columns:2,video_thumbnail:true,preview:true,download:true}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
