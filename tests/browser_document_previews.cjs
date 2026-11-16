const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const {execFileSync}=require('node:child_process');
const fs=require('node:fs');
const path=require('node:path');

const root='/data2/wujing03/sandbox-harness-platform-parallel',fixtures=path.join(root,'.data/document-preview-fixtures');
execFileSync(path.join(root,'.venv/bin/python'),[path.join(root,'tests/document_preview_fixtures.py')]);
const files={pdf:fs.readFileSync(path.join(fixtures,'sample.pdf')),pptx:fs.readFileSync(path.join(fixtures,'sample.pptx')),docx:fs.readFileSync(path.join(fixtures,'sample.docx')),xlsx:fs.readFileSync(path.join(fixtures,'sample.xlsx'))};

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:900}}),errors=[],uploads=[];page.on('pageerror',error=>errors.push(error.message));
 const now=new Date().toISOString(),thread={id:'documents',title:'文档预览',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn-documents',latest_turn_state:'COMPLETED',unread_completion:false};
 const specs=[['pdf','report.pdf','application/pdf'],['pptx','slides.pptx','application/vnd.openxmlformats-officedocument.presentationml.presentation'],['docx','notes.docx','application/vnd.openxmlformats-officedocument.wordprocessingml.document'],['xlsx','table.xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']];
 const items=[{id:'user',thread_id:thread.id,turn_id:'turn-documents',sequence:1,type:'user_message',status:'completed',summary:'查看文档',payload:{},created_at:now,updated_at:now},...specs.map(([id,name,mime],index)=>({id,thread_id:thread.id,turn_id:'turn-documents',sequence:index+2,type:'artifact',status:'completed',summary:name,payload:{artifact_id:id,mime,size:files[id].length},created_at:now,updated_at:now}))];
 await page.route('**/api/**',async route=>{const request=route.request(),url=new URL(request.url()),pathname=url.pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(pathname.endsWith('/api/session'))return json({ready:true,authentication_required:false,authenticated:false,user:null});
  if(pathname.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(pathname.endsWith('/api/threads'))return json([thread]);
  if(pathname.endsWith('/api/threads/documents/turns'))return json([{id:'turn-documents',thread_id:thread.id,harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now}]);
  if(pathname.includes('/api/threads/documents/items'))return json(items);
  if(pathname.includes('/interactions')||pathname.includes('/project-files')||pathname.includes('/background-jobs')||pathname.includes('/media/drafts')||pathname.includes('/media/jobs'))return json([]);
  if(pathname.endsWith('/api/previews/artifact/pptx'))return route.fulfill({status:200,contentType:'application/pdf',body:files.pdf});
  for(const [id,,mime] of specs)if(pathname.endsWith(`/api/artifacts/${id}`))return route.fulfill({status:200,contentType:mime,body:files[id]});
  if(pathname.endsWith('/api/uploads')&&request.method()==='POST'){uploads.push(url.searchParams.get('name'));return json({name:url.searchParams.get('name'),object_ref:'sha256:'+'1'.repeat(64),sha256:'1'.repeat(64),size:Number(request.headers()['content-length']||5),mime:url.searchParams.get('mime')})}
  if(pathname.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});
  return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24766/');await page.waitForSelector('.new-thread');
 await page.locator('.thread-list button').filter({hasText:'文档预览'}).click();await page.waitForSelector('.artifact-document-media');assert.equal(await page.locator('.artifact-document-media').count(),4);await page.waitForFunction(()=>document.querySelectorAll('.artifact-document-media canvas').length===2);await page.waitForFunction(()=>document.querySelector('.document-thumbnail-docx')?.innerText.includes('DOCX preview'));await page.waitForFunction(()=>document.querySelector('.document-thumbnail-sheet')?.innerText.includes('Images'));
 await page.getByRole('button',{name:/report.pdf/}).click();await page.waitForSelector('.document-preview-shell .document-pdf');await page.waitForFunction(()=>document.querySelectorAll('.document-preview-shell .react-pdf__Page').length===2);assert.ok(await page.locator('.document-preview-shell .document-pdf').evaluate(node=>getComputedStyle(node).overflowY==='auto'));await page.getByLabel('关闭预览').click();
 await page.getByRole('button',{name:/slides.pptx/}).click();await page.waitForSelector('.document-pdf.paged');await page.getByLabel('下一页').click();assert.match(await page.locator('.document-pdf nav').textContent(),/2 \/ 2/);await page.getByLabel('关闭预览').click();
 await page.getByRole('button',{name:/notes.docx/}).click();await page.waitForSelector('.docx-preview',{state:'visible'});await page.waitForFunction(()=>document.body.innerText.includes('Scrollable Word document content'));await page.getByLabel('关闭预览').click();
 await page.getByRole('button',{name:/table.xlsx/}).click();await page.waitForSelector('.spreadsheet-preview');assert.match(await page.locator('.spreadsheet-preview').textContent(),/Images/);await page.getByRole('button',{name:'Details'}).click();assert.match(await page.locator('.spreadsheet-preview').textContent(),/Video/);await page.getByLabel('关闭预览').click();
 await page.locator('textarea').evaluate(element=>{const transfer=new DataTransfer();transfer.items.add(new File(['paste'],'pasted.txt',{type:'text/plain'}));element.dispatchEvent(new ClipboardEvent('paste',{clipboardData:transfer,bubbles:true,cancelable:true}))});await page.waitForFunction(()=>document.body.innerText.includes('pasted.txt')&&document.body.innerText.includes('已上传'));assert.deepEqual(uploads,['pasted.txt']);
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,file_paste:true,document_thumbnails:4,pdf_scroll:true,ppt_paging:true,docx_scroll:true,excel_scroll:true,page_errors:0}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
