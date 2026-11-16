const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1280,height:850}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
  const now=new Date().toISOString(),thread={id:'markdown-thread',title:'海豚抠图与合成',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now};
  const rows=[{id:'user',type:'user_message',summary:'把 QQ 图片里的海豚抠出，叠加到合成照片上。'},
   {id:'agent',type:'agent_message',summary:'任务已继续完成：已从 QQ 图片中抠出**海豚**，并叠加到合成照片顶部中央，没有遮挡人物。\n\n[查看合成结果：女生照片-合成海豚.jpg](</workspace/project/outputs/女生照片-合成海豚.jpg>)\n\n另外单独保存了透明背景抠图：\n\n[QQ海豚-抠图.png](</workspace/outputs/QQ海豚-抠图.png>)\n\n- 保留原图标外观\n- 未进行生成式重绘\n\n```python\nimage.save("result.png")\n```\n\n<script>window.markdownUnsafe=true</script>'},
   {id:'agent-followup',type:'agent_message',summary:'补充：两个成果文件都已经校验。'},
   {id:'file1',type:'artifact',summary:'女生照片-合成海豚.jpg',payload:{artifact_id:'photo',mime:'image/jpeg',size:1200}},
   {id:'file2',type:'artifact',summary:'QQ海豚-抠图.png',payload:{artifact_id:'dolphin',mime:'image/png',size:1200}}].map((r,i)=>({thread_id:thread.id,turn_id:'turn',sequence:i+1,status:'completed',created_at:now,updated_at:now,payload:{},...r}));
  await page.route('**/api/**',async route=>{
   const path=new URL(route.request().url()).pathname;let data=[];
   if(path.endsWith('/session'))data={ready:true};
   else if(path.endsWith('/catalog/harnesses'))data=[{id:'codex',label:'Codex',default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT',reasoning:[{id:'standard',label:'标准'}]}]}];
   else if(path.endsWith('/threads'))data=[thread];
   else if(path.endsWith('/turns'))data=[{id:'turn',harness:'codex',model:'gpt-5.6',state:'COMPLETED',updated_at:now}];
   else if(path.endsWith('/items'))data=rows;
   else if(path.includes('/artifacts/'))return route.fulfill({status:200,contentType:'image/png',body:Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==','base64')});
   await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(data)});
  });
  await page.goto((process.env.TEST_BASE_URL||'http://127.0.0.1:22892/')+'?thread=markdown-thread');
  await page.waitForSelector('.markdown-copy strong');
  assert.equal(await page.locator('.markdown-copy strong').innerText(),'海豚');
  assert(await page.locator('.agent-avatar img[alt=Codex]').evaluate(img=>img.complete&&img.naturalWidth>0));
  assert.equal(await page.locator('.agent-message').count(),2);
  assert.equal(await page.locator('.agent-avatar').count(),1);
  assert.equal(await page.locator('.agent-avatar-spacer').count(),1);
  const copies=await page.locator('.agent-copy').evaluateAll(nodes=>nodes.map(node=>node.getBoundingClientRect().x));assert.equal(copies[0],copies[1]);
  assert.equal(await page.locator('.markdown-copy li').count(),2);
  assert.match(await page.locator('.markdown-copy pre code').innerText(),/image.save/);
  const link=page.locator('.markdown-copy a').filter({hasText:'查看合成结果'});
  assert.match(await link.getAttribute('href'),/api\/artifacts\/photo$/);
  assert(!await page.evaluate(()=>window.markdownUnsafe));
  await link.click();await page.waitForSelector('.media-preview img');
  assert.match(await page.locator('.media-preview a').getAttribute('href'),/api\/artifacts\/photo$/);
  await page.getByRole('button',{name:'关闭预览'}).click();
  await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform-parallel/.data/self-review2/markdown-desktop.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform-parallel/.data/self-review2/markdown-mobile.png',fullPage:true});
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({passed:true,markdown:true,workspace_links:true,image_preview:true,download:true,raw_html_disabled:true}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
