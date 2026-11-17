export function apiUrl(path){return new URL(`api/${path}`,document.baseURI).toString();}

export async function api(path,options={}){
  const response=await fetch(apiUrl(path),{...options,headers:{...(options.body instanceof Blob?{}:{'Content-Type':'application/json'}),...(options.headers||{})}});
  if(!response.ok){const raw=await response.text();let detail=raw;try{detail=JSON.parse(raw).detail||raw}catch{}const validation=Array.isArray(detail)?detail.map(item=>item?.msg||'').find(Boolean):'';if(validation.includes('Duplicate attachment filename'))detail='duplicate_attachment_filename';const labels={authentication_required:'请先登录。',invalid_credentials:'账号或密码不正确。',login_rate_limited:'尝试次数过多，请稍后再试。',username_taken:'该用户名已被使用。',active_work_prevents_registration:'当前仍有任务执行，请等待结束后注册。',already_authenticated:'当前已经登录。',registration_not_available:'FRPC 入口不开放注册。',account_action_not_available:'FRPC 入口不允许修改用户名。',duplicate_attachment_filename:'不能同时发送两个同名文件，请先重命名其中一个文件。',input_delivery_failed:'追加消息未能确认送达，请查看消息状态后再继续。',interaction_delivery_failed:'回答暂未确认送达，请稍后重试。',interaction_delivery_unknown:'回答可能已经送达，系统不会自动重复发送；请等待任务状态更新或重新进入会话核对。',interaction_response_in_progress:'回答正在处理中，请稍后重试。',interaction_response_conflict:'上一次回答尚未确认，请先重试原回答，不能更改为不同决定。',interaction_expired:'该确认已失效，请按当前任务状态继续。',turn_not_accepting_input:'本轮已进入收尾，请发送到下一轮。',sandbox_takeover_active:'请先保存并释放沙箱，再修改项目文件或继续执行。',managed_project_file_read_only:'托管输入和成果不能直接覆盖，请通过版本操作更新。',managed_files_modified:'托管文件已发生变化，请重新选择释放时的处理方式。',managed_file_changed_during_release:'托管文件在保存过程中再次变化，请检查后重试。',active_turn_prevents_sandbox_takeover:'当前任务仍在执行，请使用“停止并接管”。',sandbox_takeover_waiting_for_turn_cleanup:'任务正在清理，请稍后再次接管。',sandbox_takeover_required:'请先接管沙箱。',sandbox_not_running:'沙箱当前未运行，请先恢复环境。',sandbox_checkpoint_retryable:'保存检查点暂时失败，沙箱仍保留；请重试释放。',sandbox_checkpoint_unavailable:'沙箱已不可用，无法创建新的检查点。确认后可使用上一检查点完成释放。',sandbox_release_in_progress:'沙箱正在释放，请继续当前释放流程。',sandbox_port_not_listening:'沙箱端口当前没有监听服务。',sandbox_port_not_previewable:'该端口属于系统保留端口，不能预览。'};const error=new Error(labels[detail]|| (typeof detail==='string'?detail:'请求参数不符合要求'));error.code=detail;error.status=response.status;error.requestId=response.headers.get('x-request-id');error.traceId=response.headers.get('x-trace-id');if(error.requestId)error.message+=`（排查编号：${error.requestId}）`;throw error;}
  return response.status===204?null:response.json();
}

export const listThreads=(archived=false)=>api(`threads?archived=${archived}`);
export const markThreadRead=(id,turnId)=>api(`threads/${id}/read`,{method:'POST',body:JSON.stringify({turn_id:turnId})});
export const ensureSession=()=>api('session');
export const loginAccount=value=>api('auth/login',{method:'POST',body:JSON.stringify(value)});
export const registerAccount=value=>api('auth/register',{method:'POST',body:JSON.stringify(value)});
export const logoutAccount=()=>api('auth/logout',{method:'POST'});
export const changeAccountUsername=value=>api('auth/change-username',{method:'POST',body:JSON.stringify(value)});
export const changeAccountPassword=value=>api('auth/change-password',{method:'POST',body:JSON.stringify(value)});
export const updateAccountProfile=value=>api('auth/profile',{method:'POST',body:JSON.stringify(value)});
export const uploadAccountAvatar=file=>api('auth/avatar',{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});
export const deleteAccountAvatar=()=>api('auth/avatar',{method:'DELETE'});
export const getHarnessCatalog=()=>api('catalog/harnesses');
export const getMediaCatalog=(kind='')=>api(`catalog/media${kind?`?kind=${encodeURIComponent(kind)}`:''}`);
export const listJobGroupDrafts=threadId=>api(`threads/${threadId}/job-group-drafts`);
export const editJobGroupDraft=(draftId,value)=>api(`job-group-drafts/${draftId}`,{method:'PATCH',body:JSON.stringify(value)});
export const confirmJobGroupDraft=draftId=>api(`job-group-drafts/${draftId}/confirm`,{method:'POST'});
export const rejectJobGroupDraft=draftId=>api(`job-group-drafts/${draftId}/reject`,{method:'POST'});
export const listMediaJobs=threadId=>api(`threads/${threadId}/media/jobs`);
export const listMediaDrafts=threadId=>api(`threads/${threadId}/media/drafts`);
export const getMediaJob=jobId=>api(`media/jobs/${jobId}`);
export const cancelMediaJob=jobId=>api(`media/jobs/${jobId}/cancel`,{method:'POST'});
export const refreshMediaJob=jobId=>api(`media/jobs/${jobId}/refresh`,{method:'POST'});
export const createMediaDraft=(threadId,value)=>api(`threads/${threadId}/media/drafts`,{method:'POST',body:JSON.stringify(value)});
export const updateMediaDraft=(draftId,value)=>api(`media/drafts/${draftId}`,{method:'PATCH',body:JSON.stringify(value)});
export const confirmMediaDraft=draftId=>api(`media/drafts/${draftId}/confirm`,{method:'POST'});
export const cancelMediaDraft=draftId=>api(`media/drafts/${draftId}/cancel`,{method:'POST'});
export const listSiteDrafts=threadId=>api(`threads/${threadId}/sites/drafts`);
export const confirmSiteDraft=draftId=>api(`sites/drafts/${draftId}/confirm`,{method:'POST'});
export const cancelSiteDraft=draftId=>api(`sites/drafts/${draftId}/cancel`,{method:'POST'});
export const listSiteJobs=threadId=>api(`threads/${threadId}/sites/jobs`);
export const retrySiteJob=jobId=>api(`sites/jobs/${jobId}/retry`,{method:'POST'});
export const listSiteProjects=threadId=>api(`threads/${threadId}/sites`);
export const activateSiteVersion=(projectId,versionId)=>api(`sites/${projectId}/versions/${versionId}/activate`,{method:'POST'});
export const unpublishSite=projectId=>api(`sites/${projectId}/unpublish`,{method:'POST'});
export const createThread=value=>api('threads',{method:'POST',body:JSON.stringify(value)});
export const setThreadArchived=(id,archived)=>api(`threads/${id}/${archived?'archive':'unarchive'}`,{method:'POST'});
export const deleteThread=id=>api(`threads/${id}`,{method:'DELETE'});
export const listTurns=id=>api(`threads/${id}/turns`);
export async function listItems(id,after=0){const items=[];let cursor=after;while(true){const page=await api(`threads/${id}/items?after=${cursor}&limit=500`);items.push(...page);if(page.length<500)return items;const next=page.at(-1).sequence;if(next<=cursor)throw new Error('会话记录分页未能继续');cursor=next;}}
export const listProjectFiles=id=>api(`threads/${id}/project-files`);
export const listInteractions=id=>api(`threads/${id}/interactions`);
export const getExecutionGraph=(id,cursor='',history=false)=>api(`threads/${id}/execution-graph?cursor=${encodeURIComponent(cursor)}&include_history=${history}`);
export const listJobActivities=(id,after=0)=>api(`jobs/${id}/activities?after=${after}&limit=500`);
export const respondInteraction=(id,value)=>api(`interactions/${id}/respond`,{method:'POST',body:JSON.stringify(value)});
export const projectFileHistory=(id,path)=>api(`threads/${id}/project-files/history?path=${encodeURIComponent(path)}`);
export const restoreProjectFile=id=>api(`project-files/${id}/restore`,{method:'POST'});
export const compareProjectFiles=(leftId,rightId)=>api(`project-file-versions/compare?left_id=${encodeURIComponent(leftId)}&right_id=${encodeURIComponent(rightId)}`);
export const listTurnProjectFiles=turnId=>api(`turns/${turnId}/project-files`);
export const renameProjectFile=(id,path)=>api(`project-files/${id}/rename`,{method:'POST',body:JSON.stringify({path})});
export const deleteProjectFile=id=>api(`project-files/${id}`,{method:'DELETE'});
export const uploadProjectFile=(threadId,file)=>api(`threads/${threadId}/project-files?path=${encodeURIComponent(`assets/${file.name}`)}&mime=${encodeURIComponent(file.type||'application/octet-stream')}`,{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});
export const listBackgroundJobs=turnId=>api(`turns/${turnId}/background-jobs`);
export const cancelBackgroundJob=(turnId,jobId)=>api(`turns/${turnId}/background-jobs/${jobId}/cancel`,{method:'POST'});
export const getTurn=id=>api(`turns/${id}`);
export const createTurn=(id,value)=>api(`threads/${id}/turns`,{method:'POST',body:JSON.stringify(value)});
export const appendTurnInput=(id,value)=>api(`turns/${id}/inputs`,{method:'POST',body:JSON.stringify(value)});
export const interruptTurn=id=>api(`turns/${id}/cancel`,{method:'POST'});
export const getThreadPause=id=>api(`threads/${id}/pause`);
export const pauseThread=id=>api(`threads/${id}/pause`,{method:'POST'});
export const resumeThread=id=>api(`threads/${id}/resume`,{method:'POST'});
export const stopAllThread=id=>api(`threads/${id}/stop-all`,{method:'POST'});
export const getSandbox=id=>api(`threads/${id}/sandbox`);
export const restoreSandbox=id=>api(`threads/${id}/sandbox/restore`,{method:'POST'});
export const listSandboxFiles=(id,path='project',depth=2)=>api(`threads/${id}/sandbox/files?path=${encodeURIComponent(path)}&depth=${depth}`);
export const readSandboxFile=async(id,path)=>{const response=await fetch(apiUrl(`threads/${id}/sandbox/file?path=${encodeURIComponent(path)}`));const text=await response.text();if(!response.ok)throw new Error(text||'文件读取失败');return {text,mime:response.headers.get('content-type')||'text/plain'}};
export const writeSandboxFile=(id,path,text,mime='text/plain')=>api(`threads/${id}/sandbox/file?path=${encodeURIComponent(path)}`,{method:'PUT',body:new Blob([text],{type:mime}),headers:{'Content-Type':mime}});
export const listSandboxProcesses=id=>api(`threads/${id}/sandbox/processes`);
export const stopSandboxProcess=(id,pid)=>api(`threads/${id}/sandbox/processes/${pid}/stop`,{method:'POST'});
export const listSandboxPorts=id=>api(`threads/${id}/sandbox/ports`);
export const listSandboxAudit=id=>api(`threads/${id}/sandbox/audit`);
export const getSandboxManagedChanges=id=>api(`threads/${id}/sandbox/managed-changes`);
export const takeoverSandbox=(id,interrupt=false)=>api(`threads/${id}/sandbox/takeover?interrupt=${interrupt}`,{method:'POST'});
export const releaseSandbox=(id,managedAction='',allowCheckpointLoss=false)=>api(`threads/${id}/sandbox/release`,{method:'POST',body:JSON.stringify({managed_action:managedAction,allow_checkpoint_loss:allowCheckpointLoss})});
export const createTerminalTicket=id=>api(`threads/${id}/sandbox/terminal-ticket`,{method:'POST'});
export const createPortGrant=(id,port)=>api(`threads/${id}/sandbox/ports/${port}/grant`,{method:'POST'});
export const revokePortGrant=id=>api(`sandbox-preview/${id}`,{method:'DELETE'});
export function terminalSocketUrl(id,ticket,since=0){const url=new URL(apiUrl(`threads/${id}/sandbox/terminal`));url.protocol=url.protocol==='https:'?'wss:':'ws:';url.searchParams.set('ticket',ticket);url.searchParams.set('since',String(since));return url.toString()}
export async function upload(file){return api(`uploads?name=${encodeURIComponent(file.name)}&mime=${encodeURIComponent(file.type||'application/octet-stream')}`,{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});}
