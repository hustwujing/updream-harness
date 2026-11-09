import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {Archive,ArrowDown,ChevronDown,ChevronRight,Clock3,File,Menu,MessageSquare,Paperclip,PanelRight,Pencil,Plus,Search,Send,Square,Terminal,Trash2,Upload,X,Wrench} from 'lucide-react';
import {apiUrl,appendTurnInput,cancelBackgroundJob,createThread,createTurn,deleteProjectFile,ensureSession,getHarnessCatalog,interruptTurn,listBackgroundJobs,listInteractions,listItems,listProjectFiles,listThreads,listTurns,projectFileHistory,renameProjectFile,respondInteraction,upload,uploadProjectFile} from './api';
import './styles.css';
import './mobile.css';
import './enhancements.css';

const terminalStates=new Set(['COMPLETED','WAITING_USER','FAILED','CANCELLED']);
const activityTypes=new Set(['reasoning','plan','command','tool','file_change','warning','error']);
const statusLabels={QUEUED:'已排队',RESTORING:'恢复环境',STARTING:'启动沙箱',RUNNING:'正在工作',AWAITING_USER:'等待你的操作',DRAINING_INPUTS:'接收追加消息',CHECKPOINTING:'保存环境',WAITING_USER:'等待回复',COMPLETED:'已完成',FAILED:'失败',CANCELLED:'已停止'};
const harnessLabels={claude:'Claude',codex:'Codex',deepseek:'DeepSeek'};

function idempotencyKey(){
 const bytes=new Uint8Array(16);
 if(globalThis.crypto?.getRandomValues)globalThis.crypto.getRandomValues(bytes);
 else for(let index=0;index<bytes.length;index++)bytes[index]=Math.floor(Math.random()*256);
 bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
 return [...bytes].map(value=>value.toString(16).padStart(2,'0')).join('');
}

function relativeTime(value){const seconds=Math.max(1,(Date.now()-new Date(value).getTime())/1000);if(seconds<60)return '刚刚';if(seconds<3600)return `${Math.floor(seconds/60)} 分钟前`;if(seconds<86400)return `${Math.floor(seconds/3600)} 小时前`;return new Date(value).toLocaleDateString();}
function groupThreads(rows){const today=[],previous=[];for(const row of rows)(Date.now()-new Date(row.updated_at).getTime()<86400000?today:previous).push(row);return {today,previous};}
function compactValue(value){if(value==null)return '';if(typeof value==='string')return value;if(typeof value==='number'||typeof value==='boolean')return String(value);if(typeof value==='object'){const selected=value.command||value.filePath||value.path||value.stdout||value.content||value.error||value.message;if(selected)return compactValue(selected);try{return JSON.stringify(value)}catch{return ''}}return ''}
function activityName(item){if(item.type==='plan')return item.summary||'执行阶段';if(item.type==='warning')return item.summary||item.payload?.code||'警告';if(item.type==='error')return item.summary||item.payload?.code||'错误';const name=item.summary||item.payload?.name;return !name||name==='tool'||name==='tool_output'?'工具调用':name}
function activityDetail(item){const detail=compactValue(item.payload?.detail)||compactValue(item.payload?.result)||compactValue(item.payload?.text);return detail||({in_progress:'执行中',completed:'已完成',failed:'失败',interrupted:'已中断'}[item.status]||item.status)}

function Sidebar({threads,current,onSelect,onNew,open,onClose}){
 const groups=groupThreads(threads);
 return <aside className={`sidebar ${open?'open':''}`}>
  <div className="sidebar-head"><button className="brand" onClick={onNew}><span>H</span><b>Harness</b></button><button className="icon-button mobile-only" onClick={onClose} aria-label="关闭会话列表"><X size={17}/></button></div>
  <button className="new-thread" onClick={onNew}><Plus size={16}/>新对话</button>
  <label className="thread-search"><Search size={14}/><input placeholder="搜索对话"/></label>
  <nav className="thread-list">
   {[['今天',groups.today],['之前',groups.previous]].map(([label,rows])=>rows.length?<section key={label}><h2>{label}</h2>{rows.map(thread=><button key={thread.id} className={thread.id===current?.id?'active':''} onClick={()=>onSelect(thread)}><MessageSquare size={14}/><span><b>{thread.title}</b><small>{harnessLabels[thread.harness]} · {relativeTime(thread.updated_at)}</small></span></button>)}</section>:null)}
  </nav>
  <button className="archive-link"><Archive size={15}/>已归档</button>
 </aside>;
}

function Activity({items,active}){
 if(!items.length)return null;
 const tools=items.filter(item=>['command','tool','file_change'].includes(item.type)).length,stages=items.length-tools;
 const failure=items.find(item=>item.type==='error'||item.status==='failed');
 const summary=failure?activityName(failure):active?'正在执行':tools&&stages?`${tools} 个工具 · ${stages} 个阶段`:tools?`${tools} 个工具`:`${stages} 个阶段`;
 return <details className="activity" open={active||Boolean(failure)}><summary><span className={active?'activity-pulse':''}></span><b>{summary}</b><ChevronDown size={14}/></summary><div>{items.map(item=><div className={`activity-row ${item.status}`} key={item.id}>{item.type==='command'?<Terminal size={14}/>:<Wrench size={14}/>}<span><b>{activityName(item)}</b><small title={activityDetail(item)}>{activityDetail(item)}</small></span></div>)}</div></details>;
}

function ArtifactRow({item,onOpen}){return <button className="artifact-row" onClick={()=>onOpen(item)}><File size={16}/><span><b>{item.summary}</b><small>{item.payload?.mime||'文件'} · {item.payload?.size?`${Math.ceil(item.payload.size/1024)} KB`:''}</small></span><ChevronRight size={14}/></button>}

function InteractionCard({interaction,onRespond}){
 const [answers,setAnswers]=useState({}),[busy,setBusy]=useState(false),[error,setError]=useState('');
 const request=interaction.request||{},pending=interaction.status==='pending';
 const submit=async action=>{setBusy(true);setError('');try{await onRespond(interaction,{idempotency_key:idempotencyKey(),action,answers})}catch(value){setError(value.message||'提交失败')}finally{setBusy(false)}};
 const questions=(request.questions||[]).length?request.questions:[{id:'answer',question:request.prompt||'请补充信息',options:request.options||[]}];
 return <section className={`interaction-card ${pending?'pending':'resolved'}`}>
  <header><span className={pending?'activity-pulse':''}/><b>{interaction.kind==='approval'?'需要确认':'需要你的回答'}</b><small>{pending?'等待操作':'已响应'}</small></header>
  <p>{request.prompt||'Harness 正在等待用户输入'}</p>
  {pending&&interaction.kind==='question'&&<div className="interaction-fields">{questions.map((question,index)=>{
   const id=String(question.id||question.name||index),options=question.options||[];
   return <label key={id}><span>{question.question||question.header||question.label||`问题 ${index+1}`}</span>{options.length?
    <select value={answers[id]||''} onChange={event=>setAnswers({...answers,[id]:event.target.value})}><option value="">请选择</option>{options.map(option=>{const value=typeof option==='string'?option:option.value||option.label;return <option value={value} key={value}>{typeof option==='string'?option:option.label||option.value}</option>})}</select>:
    <input value={answers[id]||''} onChange={event=>setAnswers({...answers,[id]:event.target.value})}/>}</label>;
  })}</div>}
  {pending&&<div className="interaction-actions">{interaction.kind==='approval'?<><button disabled={busy} onClick={()=>submit('reject')}>拒绝</button><button className="primary" disabled={busy} onClick={()=>submit('approve')}>允许</button></>:<button className="primary" disabled={busy||!Object.values(answers).some(Boolean)} onClick={()=>submit('answer')}>提交回答</button>}</div>}
  {error&&<div className="interaction-error" role="alert">{error}</div>}
 </section>;
}

function Transcript({items,turns,interactions,onInteraction,onArtifact,follow,setFollow}){
 const root=useRef(null),lastLength=useRef(0),turnById=useMemo(()=>new Map(turns.map(turn=>[turn.id,turn])),[turns]);
 useEffect(()=>{if(follow&&items.length!==lastLength.current)root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'});lastLength.current=items.length;},[items,follow]);
 const onScroll=()=>{const node=root.current;setFollow(node.scrollHeight-node.scrollTop-node.clientHeight<80);};
 const groups=[];for(const item of items){let group=groups.at(-1);if(!group||group.turnId!==item.turn_id){group={turnId:item.turn_id,items:[]};groups.push(group)}group.items.push(item)}
 return <div className="transcript" ref={root} onScroll={onScroll}><div className="transcript-inner">{!items.length?<div className="welcome"><div className="welcome-mark">H</div><h1>今天想做什么？</h1></div>:groups.map(group=>{const turn=turnById.get(group.turnId),activity=group.items.filter(item=>activityTypes.has(item.type)),active=turn&&!terminalStates.has(turn.state),turnInteractions=interactions.filter(item=>item.turn_id===group.turnId);return <section className="turn" key={group.turnId}>{group.items.filter(item=>item.type==='user_message').map(item=><div className="user-message" key={item.id}>{item.summary}</div>)}{group.items.filter(item=>item.type==='agent_message').map(item=><div className={`agent-message ${item.status==='in_progress'?'streaming':''}`} key={item.id}><div className="agent-avatar">{harnessLabels[turn?.harness]?.[0]||'H'}</div><div className="agent-copy">{item.summary}</div></div>)}<Activity items={activity} active={active}/>{turnInteractions.map(interaction=><InteractionCard key={interaction.id} interaction={interaction} onRespond={onInteraction}/>)}{group.items.filter(item=>item.type==='artifact').map(item=><ArtifactRow key={item.id} item={item} onOpen={onArtifact}/>)}</section>})}</div>{!follow&&<button className="latest" onClick={()=>{setFollow(true);root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'})}}><ArrowDown size={14}/>回到最新</button>}</div>;
}

function Composer({disabled,running,harnessLocked,settings,setSettings,catalog,onSend,onStop}){
 const [text,setText]=useState(''),[files,setFiles]=useState([]),[uploading,setUploading]=useState(false),[submitting,setSubmitting]=useState(false);const fileInput=useRef(null);
 const attach=async selected=>{setUploading(true);try{for(const file of selected)setFiles(current=>[...current,{file,remote:null}]);for(const file of selected){const remote=await upload(file);setFiles(current=>current.map(item=>item.file===file?{...item,remote}:item));}}finally{setUploading(false)}};
 const submit=async event=>{event.preventDefault();if(!text.trim()||uploading||submitting)return;setSubmitting(true);try{await onSend(text.trim(),files.map(item=>item.remote).filter(Boolean));setText('');setFiles([])}finally{setSubmitting(false)}};
 const selectedHarness=catalog.find(item=>item.id===settings.harness),catalogModels=selectedHarness?.models||[],fallbackModel=catalogModels[0],models=catalogModels.some(item=>item.id===settings.model)||!settings.model?[...catalogModels]:[...catalogModels,{id:settings.model,label:`${settings.model}（当前会话）`,reasoning:fallbackModel?.reasoning||[]}],selectedModel=models.find(item=>item.id===settings.model)||models[0],levels=selectedModel?.reasoning||[];
 const chooseHarness=id=>{const next=catalog.find(item=>item.id===id),model=next?.models.find(item=>item.id===next.default_model)||next?.models[0],level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({harness:id,model:model?.id||'',reasoning_level:level?.id||''})};
 const chooseModel=id=>{const model=models.find(item=>item.id===id),level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({...settings,model:id,reasoning_level:level?.id||''})};
 return <div className="composer-shell">{files.length>0&&<div className="attachments">{files.map((item,index)=><span key={index}><File size={13}/>{item.file.name}<small>{item.remote?'已上传':'上传中'}</small><button type="button" aria-label={`移除 ${item.file.name}`} onClick={()=>setFiles(files.filter((_,i)=>i!==index))}><X size={12}/></button></span>)}</div>}<form className="composer" onSubmit={submit}><div className="composer-config"><label><span>Harness</span><select aria-label="Harness" value={settings.harness} disabled={harnessLocked} onChange={event=>chooseHarness(event.target.value)}>{catalog.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select></label><label><span>模型</span><select value={settings.model} disabled={running} onChange={event=>chooseModel(event.target.value)} aria-label="模型">{models.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select></label><label><span>推理强度</span><select aria-label="推理强度" value={settings.reasoning_level} disabled={running} onChange={event=>setSettings({...settings,reasoning_level:event.target.value})}>{levels.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select></label></div><textarea autoFocus value={text} onChange={event=>setText(event.target.value)} onKeyDown={event=>{if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();event.currentTarget.form.requestSubmit()}}} placeholder="输入消息，描述你要完成的任务…" disabled={disabled||submitting}/><div className="composer-bar"><button type="button" className="attach-button" onClick={()=>fileInput.current.click()}><Paperclip size={16}/><span>{uploading?'上传中':'添加文件'}</span></button><input ref={fileInput} type="file" multiple hidden onChange={event=>{attach([...event.target.files]);event.target.value=''}}/><span className="composer-spacer"/>{running&&<button type="button" className="send stop" onClick={onStop} aria-label="停止"><Square size={14} fill="currentColor"/></button>}<button className="send" disabled={disabled||!text.trim()||uploading||submitting} aria-label={running?'追加消息':'发送'}><Send size={16}/></button></div></form></div>;
}

function ProjectFiles({title,files,onRename,onDelete,onHistory}){
 const [editing,setEditing]=useState(''),[name,setName]=useState(''),[histories,setHistories]=useState({});
 const history=async item=>{if(histories[item.id]){setHistories({...histories,[item.id]:null});return}setHistories({...histories,[item.id]:await onHistory(item)})};
 return <section><h2>{title}</h2>{files.length?files.map(item=><div className="project-file-wrap" key={item.id}><div className="project-file"><File size={15}/><a href={apiUrl(`project-files/${item.id}`)}><b>{item.path.split('/').at(-1)}</b><small>{item.path} · v{item.version} · {item.size>=1048576?`${(item.size/1048576).toFixed(1)} MB`:`${Math.max(1,Math.ceil(item.size/1024))} KB`}</small></a><button title="版本记录" onClick={()=>history(item)}><Clock3 size={13}/></button><button title="重命名" onClick={()=>{setEditing(item.id);setName(item.path.split('/').at(-1))}}><Pencil size={13}/></button><button title="删除" onClick={()=>onDelete(item)}><Trash2 size={13}/></button></div>{editing===item.id&&<form className="project-file-rename" onSubmit={async event=>{event.preventDefault();await onRename(item,item.path.split('/').slice(0,-1).concat(name).join('/'));setEditing('')}}><input value={name} onChange={event=>setName(event.target.value)}/><button>保存</button><button type="button" onClick={()=>setEditing('')}>取消</button></form>}{histories[item.id]&&<div className="project-file-history">{histories[item.id].map(version=><small key={version.id}>v{version.version} · {version.deleted?'已删除':new Date(version.created_at).toLocaleString()}</small>)}</div>}</div>):<p>暂无文件</p>}</section>;
}

function BackgroundJobs({jobs,onCancel}){return <section><h2>后台任务</h2>{jobs.length?jobs.map(job=><div className="background-job" key={job.id}><span className={job.status==='running'?'activity-pulse':''}/><div><b>{job.name}</b><small>{job.status} · PID {job.pid||'—'}</small>{job.log_tail&&<pre>{job.log_tail}</pre>}</div>{job.status==='running'&&<button onClick={()=>onCancel(job)}><Square size={11}/>停止</button>}</div>):<p>没有后台任务</p>}</section>}

function Inspector({open,onClose,thread,turn,projectFiles,jobs,onCancelJob,onRefresh}){
 const input=useRef(null),assets=projectFiles.filter(item=>item.path.startsWith('assets/')),artifacts=projectFiles.filter(item=>item.path.startsWith('artifacts/'));
 const rename=async(item,path)=>{await renameProjectFile(item.id,path);await onRefresh()};
 const remove=async item=>{await deleteProjectFile(item.id);await onRefresh()};
 const history=item=>projectFileHistory(thread.id,item.path);
 const add=async files=>{for(const file of files)await uploadProjectFile(thread.id,file);await onRefresh()};
 return <aside className={`inspector ${open?'open':''}`}><header><b>Workspace</b><button className="icon-button" onClick={onClose}><X size={17}/></button></header><section><h2>Sandbox</h2><dl><dt>Harness</dt><dd>{harnessLabels[turn?.harness||thread?.harness]||'—'}</dd><dt>Model</dt><dd>{turn?.model||thread?.model||'—'}</dd><dt>Status</dt><dd>{turn?statusLabels[turn.state]||turn.state:'Idle'}</dd></dl></section><BackgroundJobs jobs={jobs} onCancel={onCancelJob}/>{thread&&<button className="workspace-upload" onClick={()=>input.current.click()}><Upload size={14}/>上传到项目素材</button>}<input hidden multiple type="file" ref={input} onChange={event=>{add([...event.target.files]);event.target.value=''}}/><ProjectFiles title="项目素材" files={assets} onRename={rename} onDelete={remove} onHistory={history}/><ProjectFiles title="项目成果" files={artifacts} onRename={rename} onDelete={remove} onHistory={history}/></aside>;
}

function App(){
 const [threads,setThreads]=useState([]),[catalog,setCatalog]=useState([]),[thread,setThread]=useState(null),[turns,setTurns]=useState([]),[items,setItems]=useState([]),[interactions,setInteractions]=useState([]),[projectFiles,setProjectFiles]=useState([]),[backgroundJobs,setBackgroundJobs]=useState([]),[sidebar,setSidebar]=useState(false),[inspector,setInspector]=useState(false),[follow,setFollow]=useState(true),[settings,setSettings]=useState({harness:'claude',model:'',reasoning_level:''});const source=useRef(null),refreshTimer=useRef(null);
 const activeTurn=turns.find(turn=>!terminalStates.has(turn.state));
 const refresh=useCallback(async selected=>{if(!selected)return;const nextTurns=await listTurns(selected.id),active=nextTurns.find(turn=>!terminalStates.has(turn.state));const [nextItems,nextInteractions,nextFiles,nextJobs]=await Promise.all([listItems(selected.id),listInteractions(selected.id),listProjectFiles(selected.id),active?listBackgroundJobs(active.id).catch(()=>[]):[]]);setTurns(nextTurns);setItems(nextItems);setInteractions(nextInteractions);setProjectFiles(nextFiles);setBackgroundJobs(nextJobs);if(active)connect(active,selected);},[]);
 const connect=(turn,selected=thread)=>{source.current?.close();source.current=new EventSource(apiUrl(`turns/${turn.id}/events`));const update=()=>{clearTimeout(refreshTimer.current);refreshTimer.current=setTimeout(()=>refresh(selected),80)};for(const event of ['item.created','item.updated','item.delta','text_delta','tool_started','tool_output','tool_finished','step_started','step_progress','step_finished','artifact_created','state','error'])source.current.addEventListener(event,update);source.current.onerror=update;};
 useEffect(()=>{ensureSession().then(()=>Promise.all([listThreads(),getHarnessCatalog()])).then(([rows,options])=>{setThreads(rows);setCatalog(options);const selected=options.find(item=>item.id==='claude')||options[0],model=selected.models.find(item=>item.id===selected.default_model)||selected.models[0],level=model.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({harness:selected.id,model:model.id,reasoning_level:level.id})});return()=>source.current?.close()},[]);
 const select=async value=>{source.current?.close();setThread(value);setSettings({harness:value.harness,model:value.model,reasoning_level:value.reasoning_level});setSidebar(false);setFollow(true);await refresh(value)};
 const newThread=()=>{source.current?.close();setThread(null);setTurns([]);setItems([]);setInteractions([]);setProjectFiles([]);setBackgroundJobs([]);setSidebar(false);setFollow(true)};
 const send=async(message,attachments)=>{let selected=thread;if(!selected){selected=await createThread({...settings,title:message.slice(0,72)});setThreads(current=>[selected,...current]);setThread(selected)}if(activeTurn){try{await appendTurnInput(activeTurn.id,{message,attachments,idempotency_key:idempotencyKey()});await refresh(selected);return}catch(error){if(error.status!==409)throw error}}const turn=await createTurn(selected.id,{message,attachments,idempotency_key:idempotencyKey(),...settings,delivery_mode:activeTurn?'queue':'start'});setTurns(current=>[...current,turn]);connect(turn,selected);await refresh(selected)};
 const stop=async()=>{if(activeTurn)await interruptTurn(activeTurn.id)};
 const answerInteraction=async(interaction,value)=>{await respondInteraction(interaction.id,value);await refresh(thread)};
 const cancelJob=async job=>{if(activeTurn){await cancelBackgroundJob(activeTurn.id,job.id);await refresh(thread)}};
 const latest=turns.at(-1);
 return <div className={`app ${inspector?'with-inspector':''}`}><Sidebar threads={threads} current={thread} onSelect={select} onNew={newThread} open={sidebar} onClose={()=>setSidebar(false)}/><main><header className="topbar"><button className="icon-button mobile-only" onClick={()=>{setInspector(false);setSidebar(true)}}><Menu size={18}/></button><div><h1>{thread?.title||'新对话'}</h1><span>{latest?`${harnessLabels[latest.harness]} · ${latest.model} · ${statusLabels[latest.state]||latest.state}`:'在下方选择 Harness、模型与推理强度'}</span></div><div className="top-actions"><button className="icon-button" onClick={()=>{setSidebar(false);setInspector(!inspector)}} title="Workspace"><PanelRight size={18}/></button></div></header><Transcript items={items} turns={turns} interactions={interactions} onInteraction={answerInteraction} onArtifact={()=>{setSidebar(false);setInspector(true)}} follow={follow} setFollow={setFollow}/><Composer disabled={!catalog.length} running={Boolean(activeTurn)} harnessLocked={Boolean(thread)} settings={settings} setSettings={setSettings} catalog={catalog} onSend={send} onStop={stop}/></main><Inspector open={inspector} onClose={()=>setInspector(false)} thread={thread} turn={latest} projectFiles={projectFiles} jobs={backgroundJobs} onCancelJob={cancelJob} onRefresh={()=>refresh(thread)}/>{sidebar&&<button className="scrim mobile-only" onClick={()=>setSidebar(false)}/>}</div>;
}

createRoot(document.getElementById('root')).render(<App/>);
