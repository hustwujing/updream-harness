const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:800}});const errors=[];page.on('pageerror',error=>errors.push(error.message));
 let authenticated=false,ingress='frpc',user={id:'user-1',username:'frpc.user',disabled:false};
 await page.route('**/api/**',async route=>{const path=new URL(route.request().url()).pathname,json=(value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true,ingress,authentication_required:ingress==='frpc'&&!authenticated,authenticated,user:authenticated?user:null});
  if(path.endsWith('/api/auth/login')){const body=route.request().postDataJSON();if(body.username!=='frpc.user'||body.password!=='initial account password')return json({detail:'invalid_credentials'},401);authenticated=true;return json({authenticated:true,user})}
  if(path.endsWith('/api/auth/register')){if(ingress==='frpc')return json({detail:'registration_not_available'},403);const body=route.request().postDataJSON();authenticated=true;user={id:'user-2',username:body.username,disabled:false};return json({authenticated:true,user})}
  if(path.endsWith('/api/auth/change-username')){user={...user,username:route.request().postDataJSON().username};return json({authenticated:true,user})}
  if(path.endsWith('/api/auth/change-password'))return json({changed:true});
  if(path.endsWith('/api/auth/logout')){authenticated=false;return json({authenticated:false})}
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([]);
  return json([]);
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:22766/');await page.getByRole('heading',{name:'登录 updream-harness'}).waitFor();
 assert.equal(await page.locator('.new-thread').count(),0);assert.equal(await page.getByRole('button',{name:'注册',exact:true}).count(),0);await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/auth-login.png',fullPage:true});
 await page.getByLabel('账号').fill('frpc.user');await page.getByLabel('密码').fill('wrong');await page.getByRole('button',{name:'登录'}).click();await page.getByText('账号或密码不正确。',{exact:true}).waitFor();
 await page.getByLabel('密码').fill('initial account password');await page.getByRole('button',{name:'登录'}).click();await page.waitForSelector('.new-thread');
 assert.equal(await page.locator('.topbar>div:nth-child(2)>span').evaluate(node=>getComputedStyle(node,'::after').content),'" · 公网访问"');
 await page.getByTitle('frpc.user').click();await page.getByText('账号设置',{exact:true}).waitFor();const forms=page.locator('.account-settings form');assert.equal(await page.getByText('修改用户名',{exact:true}).count(),0);
 await forms.nth(0).getByLabel('当前密码').fill('initial account password');await forms.nth(0).getByLabel('新密码').fill('replacement account password');await forms.nth(0).getByLabel('再次输入').fill('replacement account password');await forms.nth(0).getByRole('button',{name:'保存密码'}).click();await page.getByText('密码已更新，其他设备已退出登录',{exact:true}).waitFor();
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/auth-settings.png',fullPage:true});await page.getByRole('button',{name:'退出登录'}).click();await page.getByRole('heading',{name:'登录 updream-harness'}).waitFor();
 ingress='direct';authenticated=false;await page.reload();await page.waitForSelector('.new-thread');assert.equal(await page.locator('.topbar>div:nth-child(2)>span').evaluate(node=>getComputedStyle(node,'::after').content),'" · 网络受限"');await page.getByTitle('登录').click();await page.getByRole('button',{name:'注册',exact:true}).click();await page.getByRole('textbox',{name:'账号',exact:true}).fill('new.direct');await page.getByLabel('密码',{exact:true}).fill('new direct password');await page.getByLabel('再次输入').fill('new direct password');await page.getByRole('button',{name:'创建账号'}).click();await page.getByTitle('new.direct').waitFor();
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,frpc_login_required:true,frpc_no_registration:true,frpc_no_rename:true,invalid_login_visible:true,password_change:true,logout:true,direct_registration:true,network_mode_visible:true}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
