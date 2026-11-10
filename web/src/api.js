export function apiUrl(path){return new URL(`api/${path}`,document.baseURI).toString();}

export async function api(path,options={}){
  const response=await fetch(apiUrl(path),{...options,headers:{...(options.body instanceof Blob?{}:{'Content-Type':'application/json'}),...(options.headers||{})}});
  if(!response.ok){const raw=await response.text();let detail=raw;try{detail=JSON.parse(raw).detail||raw}catch{}const labels={authentication_required:'请先登录。',invalid_credentials:'账号或密码不正确。',login_rate_limited:'尝试次数过多，请稍后再试。',username_taken:'该用户名已被使用。',input_delivery_failed:'追加消息未能确认送达，请查看消息状态后再继续。',interaction_delivery_failed:'回答暂未确认送达，请稍后重试。',interaction_response_in_progress:'回答正在处理中，请稍后重试。',interaction_response_conflict:'上一次回答尚未确认，请先重试原回答，不能更改为不同决定。',interaction_expired:'该确认已失效，请按当前任务状态继续。',turn_not_accepting_input:'本轮已进入收尾，请发送到下一轮。'};const error=new Error(labels[detail]|| (typeof detail==='string'?detail:'请求参数不符合要求'));error.code=detail;error.status=response.status;error.requestId=response.headers.get('x-request-id');error.traceId=response.headers.get('x-trace-id');if(error.requestId)error.message+=`（排查编号：${error.requestId}）`;throw error;}
  return response.status===204?null:response.json();
}

export const listThreads=(archived=false)=>api(`threads?archived=${archived}`);
export const markThreadRead=(id,turnId)=>api(`threads/${id}/read`,{method:'POST',body:JSON.stringify({turn_id:turnId})});
export const ensureSession=()=>api('session');
export const loginAccount=value=>api('auth/login',{method:'POST',body:JSON.stringify(value)});
export const logoutAccount=()=>api('auth/logout',{method:'POST'});
export const changeAccountUsername=value=>api('auth/change-username',{method:'POST',body:JSON.stringify(value)});
export const changeAccountPassword=value=>api('auth/change-password',{method:'POST',body:JSON.stringify(value)});
export const getHarnessCatalog=()=>api('catalog/harnesses');
export const createThread=value=>api('threads',{method:'POST',body:JSON.stringify(value)});
export const setThreadArchived=(id,archived)=>api(`threads/${id}/${archived?'archive':'unarchive'}`,{method:'POST'});
export const deleteThread=id=>api(`threads/${id}`,{method:'DELETE'});
export const listTurns=id=>api(`threads/${id}/turns`);
export async function listItems(id,after=0){const items=[];let cursor=after;while(true){const page=await api(`threads/${id}/items?after=${cursor}&limit=500`);items.push(...page);if(page.length<500)return items;const next=page.at(-1).sequence;if(next<=cursor)throw new Error('会话记录分页未能继续');cursor=next;}}
export const listProjectFiles=id=>api(`threads/${id}/project-files`);
export const listInteractions=id=>api(`threads/${id}/interactions`);
export const respondInteraction=(id,value)=>api(`interactions/${id}/respond`,{method:'POST',body:JSON.stringify(value)});
export const projectFileHistory=(id,path)=>api(`threads/${id}/project-files/history?path=${encodeURIComponent(path)}`);
export const renameProjectFile=(id,path)=>api(`project-files/${id}/rename`,{method:'POST',body:JSON.stringify({path})});
export const deleteProjectFile=id=>api(`project-files/${id}`,{method:'DELETE'});
export const uploadProjectFile=(threadId,file)=>api(`threads/${threadId}/project-files?path=${encodeURIComponent(`assets/${file.name}`)}&mime=${encodeURIComponent(file.type||'application/octet-stream')}`,{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});
export const listBackgroundJobs=turnId=>api(`turns/${turnId}/background-jobs`);
export const cancelBackgroundJob=(turnId,jobId)=>api(`turns/${turnId}/background-jobs/${jobId}/cancel`,{method:'POST'});
export const getTurn=id=>api(`turns/${id}`);
export const createTurn=(id,value)=>api(`threads/${id}/turns`,{method:'POST',body:JSON.stringify(value)});
export const appendTurnInput=(id,value)=>api(`turns/${id}/inputs`,{method:'POST',body:JSON.stringify(value)});
export const interruptTurn=id=>api(`turns/${id}/cancel`,{method:'POST'});
export async function upload(file){return api(`uploads?name=${encodeURIComponent(file.name)}&mime=${encodeURIComponent(file.type||'application/octet-stream')}`,{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});}
