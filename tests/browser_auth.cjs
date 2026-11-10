const {chromium}=require('playwright');
const assert=require('node:assert/strict');

(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],ignoreDefaultArgs:['--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:1280,height:800}});const errors=[];page.on('pageerror',error=>errors.push(error.message));
 let authenticated=false,user={id:'user-1',username:'frpc.user',disabled:false};
 await page.route('**/api/**',async route=>{const path=new URL(route.request().url()).pathname,json=(value,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(value)});
  if(path.endsWith('/api/session'))return json({ready:true,ingress:'frpc',authentication_required:!authenticated,authenticated,user:authenticated?user:null});
  if(path.endsWith('/api/auth/login')){const body=route.request().postDataJSON();if(body.username!=='frpc.user'||body.password!=='initial account password')return json({detail:'invalid_credentials'},401);authenticated=true;return json({authenticated:true,user})}
  if(path.endsWith('/api/auth/change-username')){user={...user,username:route.request().postDataJSON().username};return json({authenticated:true,user})}
  if(path.endsWith('/api/auth/change-password'))return json({changed:true});
  if(path.endsWith('/api/auth/logout')){authenticated=false;return json({authenticated:false})}
  if(path.endsWith('/api/catalog/harnesses'))return json([{id:'codex',label:'Codex',available:true,default_model:'gpt-5.6',models:[{id:'gpt-5.6',label:'GPT-5.6',available:true,reasoning:[{id:'standard',label:'标准'}]}]}]);
  if(path.endsWith('/api/threads'))return json([]);
  return json([]);
 });
 await page.goto(process.env.TEST_BASE_URL||'http://127.0.0.1:22766/');await page.getByRole('heading',{name:'登录 updream-harness'}).waitFor();
 assert.equal(await page.locator('.new-thread').count(),0);await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/auth-login.png',fullPage:true});
 await page.getByLabel('账号').fill('frpc.user');await page.getByLabel('密码').fill('wrong');await page.getByRole('button',{name:'登录'}).click();await page.getByText('账号或密码不正确。',{exact:true}).waitFor();
 await page.getByLabel('密码').fill('initial account password');await page.getByRole('button',{name:'登录'}).click();await page.waitForSelector('.new-thread');
 await page.getByTitle('frpc.user').click();await page.getByText('账号设置',{exact:true}).waitFor();const forms=page.locator('.account-settings form');
 await forms.nth(0).getByLabel('新用户名').fill('renamed.user');await forms.nth(0).getByLabel('当前密码').fill('initial account password');await forms.nth(0).getByRole('button',{name:'保存用户名'}).click();await page.getByText('用户名已更新',{exact:true}).waitFor();assert.ok(await page.getByTitle('renamed.user').isVisible());
 await forms.nth(1).getByLabel('当前密码').fill('initial account password');await forms.nth(1).getByLabel('新密码').fill('replacement account password');await forms.nth(1).getByLabel('再次输入').fill('replacement account password');await forms.nth(1).getByRole('button',{name:'保存密码'}).click();await page.getByText('密码已更新，其他设备已退出登录',{exact:true}).waitFor();
 await page.screenshot({path:'/data2/wujing03/sandbox-harness-platform/.data/auth-settings.png',fullPage:true});await page.getByRole('button',{name:'退出登录'}).click();await page.getByRole('heading',{name:'登录 updream-harness'}).waitFor();
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,frpc_login_required:true,invalid_login_visible:true,username_change:true,password_change:true,logout:true}));
}finally{await browser?.close()}})().catch(error=>{console.error(error);process.exit(1)});
