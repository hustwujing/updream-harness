const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const {execFileSync}=require('node:child_process');

function minimalGlb(){
 const positions=Buffer.alloc(36);[-1,-1,0,1,-1,0,0,1,0].forEach((value,index)=>positions.writeFloatLE(value,index*4));
 const json=Buffer.from(JSON.stringify({asset:{version:'2.0'},scene:0,scenes:[{nodes:[0]}],nodes:[{mesh:0}],meshes:[{primitives:[{attributes:{POSITION:0}}]}],accessors:[{bufferView:0,componentType:5126,count:3,type:'VEC3',min:[-1,-1,0],max:[1,1,0]}],bufferViews:[{buffer:0,byteOffset:0,byteLength:36}],buffers:[{byteLength:36}]}));
 const jsonPad=(4-json.length%4)%4,binPad=(4-positions.length%4)%4,total=12+8+json.length+jsonPad+8+positions.length+binPad,out=Buffer.alloc(total);let offset=0;
 out.writeUInt32LE(0x46546c67,offset);offset+=4;out.writeUInt32LE(2,offset);offset+=4;out.writeUInt32LE(total,offset);offset+=4;
 out.writeUInt32LE(json.length+jsonPad,offset);offset+=4;out.writeUInt32LE(0x4e4f534a,offset);offset+=4;json.copy(out,offset);offset+=json.length;out.fill(0x20,offset,offset+jsonPad);offset+=jsonPad;
 out.writeUInt32LE(positions.length+binPad,offset);offset+=4;out.writeUInt32LE(0x004e4942,offset);offset+=4;positions.copy(out,offset);return out;
}

(async()=>{let browser;try{
 const headed=process.env.MODEL3D_HEADED==='1';browser=await chromium.launch({headless:!headed,args:['--no-sandbox',...(headed?[]:['--use-gl=swiftshader','--enable-webgl','--ignore-gpu-blocklist','--enable-unsafe-swiftshader'])],ignoreDefaultArgs:headed?[]:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:900}}),errors=[],requests=[];page.on('pageerror',error=>errors.push(error.stack||error.message));page.on('request',request=>requests.push(request.url()));
 const now=new Date().toISOString(),thread={id:'model3d',title:'3D 模型预览',harness:'codex',model:'gpt-5.6',reasoning_level:'standard',updated_at:now,latest_turn_id:'turn-model3d',latest_turn_state:'COMPLETED',unread_completion:false};
 const item={id:'model-item',thread_id:thread.id,turn_id:'turn-model3d',sequence:2,type:'artifact',status:'completed',summary:'triangle.glb',payload:{artifact_id:'model-artifact',mime:'model/gltf-binary',size:minimalGlb().length},created_at:now,updated_at:now};
 await page.route('**/api/**',async route=>{const path=new URL(route.request().url()).pathname,json=value=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/artifacts/model-artifact')){await new Promise(resolve=>setTimeout(resolve,650));return route.fulfill({status:200,contentType:'model/gltf-binary',headers:{'Content-Length':String(minimalGlb().length)},body:minimalGlb()})}
  if(path.endsWith('/api/session'))return json({ready:true});
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([thread]);
  if(path.endsWith('/api/threads/model3d/turns'))return json([{id:'turn-model3d',thread_id:thread.id,harness:'codex',model:'gpt-5.6',reasoning_level:'standard',state:'COMPLETED',updated_at:now}]);
  if(path.includes('/api/threads/model3d/items'))return json([{id:'user',thread_id:thread.id,turn_id:'turn-model3d',sequence:1,type:'user_message',status:'completed',summary:'生成 3D 模型',payload:{},created_at:now,updated_at:now},item]);
  if(path.includes('/interactions')||path.includes('/project-files')||path.includes('/background-jobs')||path.includes('/media/drafts')||path.includes('/media/jobs'))return json([]);
  if(path.includes('/events'))return route.fulfill({status:200,contentType:'text/event-stream',body:''});return json({});
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:24767/');await page.waitForSelector('.new-thread');await page.getByText('3D 模型预览').click();await page.waitForTimeout(350);assert.ok(requests.some(value=>value.includes('Model3DPreview-')),'3D viewer chunk was not prefetched');
 const webgl=await page.evaluate(()=>{const canvas=document.createElement('canvas');return Boolean(canvas.getContext('webgl2')||canvas.getContext('webgl'))});
 await page.getByRole('button',{name:'预览 triangle.glb'}).click({force:true});
 if(!webgl){const fallback=page.locator('.model3d-error');await fallback.waitFor();assert.match(await fallback.textContent(),/无法启动 3D 渲染/);assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,prefetched:true,webgl:false,friendly_fallback:true}));return}
 const loading=page.locator('.model3d-loading');await loading.waitFor({state:'attached',timeout:12000}).catch(async error=>{throw new Error(`3D loading state did not appear: ${JSON.stringify({errors,text:(await page.locator('body').textContent()).slice(-500),previewLoading:await page.locator('.preview-loading').count(),modelRequests:requests.filter(value=>value.includes('Model3DPreview-'))})}`,{cause:error})});assert.match(await loading.textContent(),/正在下载 3D 模型.*正在建立连接/s);
 const canvas=page.locator('.model3d-preview canvas');await canvas.waitFor();await page.waitForSelector('.model3d-loading',{state:'detached',timeout:5000});await page.waitForTimeout(100);
 const shot='/data2/wujing03/sandbox-harness-platform-parallel-ali/.data/model3d-canvas.png';await canvas.screenshot({path:shot});
 const unique=Number(execFileSync('python3',['-c','from PIL import Image\nimport sys\nim=Image.open(sys.argv[1]).convert("RGB").resize((80,80))\nprint(len(set(im.getdata())))',shot],{encoding:'utf8'}).trim());
 assert.ok(unique>20,`3D canvas appears blank: ${unique} colors`);assert.equal(await page.getByText('拖动旋转 · 滚轮缩放 · 右键平移').count(),1);
 await page.setViewportSize({width:390,height:844});await page.waitForTimeout(100);const box=await canvas.boundingBox();assert.ok(box.x>=0&&box.x+box.width<=390&&box.height>=400);
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,prefetched:true,loading_state:true,webgl:true,canvas_colors:unique,mobile_width:box.width}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
