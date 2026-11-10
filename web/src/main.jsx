import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {Archive,ArrowDown,ChevronDown,ChevronRight,CircleCheck,CircleX,Clock3,Download,File,LoaderCircle,Maximize2,Menu,MessageSquare,Paperclip,PanelRight,Pencil,Plus,Search,Send,Square,Trash2,Upload,X} from 'lucide-react';
import {apiUrl,appendTurnInput,cancelBackgroundJob,createThread,createTurn,deleteProjectFile,deleteThread,ensureSession,getHarnessCatalog,interruptTurn,listBackgroundJobs,listInteractions,listItems,listProjectFiles,listThreads,listTurns,markThreadRead,projectFileHistory,renameProjectFile,respondInteraction,upload,uploadProjectFile} from './api';
import AgentMarkdown from './AgentMarkdown';
import {modelHealthLabel,loadModelChoice,saveModelChoice} from './model-choice';
import brandMark from './assets/brand/updream-mark.svg?no-inline';
import codexIcon from './assets/harness/codex.png';
import claudeIcon from './assets/harness/claude.svg';
import deepseekIcon from './assets/harness/deepseek.ico';
import {activityTiming,formatDuration} from './activity-time';
import './styles.css';
import './mobile.css';
import './enhancements.css';

const terminalStates=new Set(['COMPLETED','WAITING_USER','FAILED','CANCELLED']);
const activityTypes=new Set(['reasoning','plan','command','tool','file_change','warning','error']);
const statusLabels={QUEUED:'已排队',RESTORING:'恢复环境',STARTING:'启动沙箱',RUNNING:'正在工作',AWAITING_USER:'等待你的操作',DRAINING_INPUTS:'接收追加消息',CHECKPOINTING:'保存环境',WAITING_USER:'等待回复',COMPLETED:'已完成',FAILED:'失败',CANCELLED:'已停止'};
const harnessLabels={claude:'Claude',codex:'Codex',deepseek:'DeepSeek'};
const harnessIcons={codex:codexIcon,claude:claudeIcon,deepseek:deepseekIcon};
function HarnessIcon({harness}){return harnessIcons[harness]?<img className="harness-icon" src={harnessIcons[harness]} alt={harnessLabels[harness]} title={harnessLabels[harness]}/>:<MessageSquare size={16}/>}

function idempotencyKey(){
 const bytes=new Uint8Array(16);
 if(globalThis.crypto?.getRandomValues)globalThis.crypto.getRandomValues(bytes);
 else for(let index=0;index<bytes.length;index++)bytes[index]=Math.floor(Math.random()*256);
 bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
 return [...bytes].map(value=>value.toString(16).padStart(2,'0')).join('');
}

function relativeTime(value){const seconds=Math.max(1,(Date.now()-new Date(value).getTime())/1000);if(seconds<60)return '刚刚';if(seconds<3600)return `${Math.floor(seconds/60)} 分钟前`;if(seconds<86400)return `${Math.floor(seconds/3600)} 小时前`;return new Date(value).toLocaleDateString();}
function urlThread(){return new URLSearchParams(location.search).get('thread')||''}
function writeThreadUrl(id,mode='push'){const url=new URL(location.href);if(id)url.searchParams.set('thread',id);else url.searchParams.delete('thread');history[mode==='replace'?'replaceState':'pushState']({},'',url.pathname+url.search+url.hash)}
function defaultSettings(options){const selected=options.find(item=>item.id==='codex'&&item.available!==false)||options.find(item=>item.available!==false)||options[0],model=selected.models.find(item=>item.id===selected.default_model&&item.available!==false)||selected.models.find(item=>item.available!==false)||selected.models[0],level=model.reasoning[Math.min(1,model.reasoning.length-1)];return {harness:selected.id,model:model.id,reasoning_level:level.id}}
function availableThreadSettings(options,current){return current.model?current:defaultSettings(options)}
function groupThreads(rows){const today=[],previous=[];for(const row of rows)(Date.now()-new Date(row.updated_at).getTime()<86400000?today:previous).push(row);return {today,previous};}
function compactValue(value){if(value==null)return '';if(typeof value==='string')return value;if(typeof value==='number'||typeof value==='boolean')return String(value);if(typeof value==='object'){const selected=value.command||value.filePath||value.path||value.stdout||value.content||value.error||value.message;if(selected)return compactValue(selected);try{return JSON.stringify(value)}catch{return ''}}return ''}
function parsedToolDetail(item){const raw=item.payload?.detail||{},parse=value=>{if(typeof value!=='string')return value;try{return JSON.parse(value)}catch{return value}},detail=parse(raw),argumentsValue=parse(detail?.arguments);return typeof argumentsValue==='object'&&argumentsValue?{...detail,...argumentsValue}:detail}
function displayItem(item,turn){
 const terminal=turn&&terminalStates.has(turn.state);
 if(item.status==='in_progress'&&terminal)return {...item,status:['COMPLETED','WAITING_USER'].includes(turn.state)?'completed':'failed'};
 const nativeFailed=item.payload?.is_error||['failed','interrupted','cancelled'].includes(item.payload?.turn?.status);
 if(nativeFailed||(item.payload?.input_completed&&turn?.state==='FAILED'))return {...item,status:'failed'};
 if(item.type==='error'){
  const detail=turn?.error_detail||'';
  if(/\b503\b|servers are currently overloaded/i.test(detail))return {...item,summary:'模型服务繁忙（503），本轮未能完成。'};
  if(/stream ended without a result/i.test(detail))return {...item,summary:'与沙箱的输出连接中断，未收到最终执行结果。'};
 }
 return item;
}

function activityName(item){if(item.payload?.model_request_id&&item.status==='interrupted')return `${item.payload.model} 请求已结束`;if(item.type==='plan')return ({'Codex response':'Codex 本轮响应','Claude response':'Claude 本轮响应','Input completed':'输入处理结果'}[item.summary]||item.summary||'执行阶段');if(item.type==='warning')return item.payload?.code||'警告';if(item.type==='error')return '执行错误';const name=String(item.summary||item.payload?.name||'').toLowerCase(),labels={bash:'执行命令',commandexecution:'执行命令',read:'读取文件',write:'写入文件',edit:'修改文件',filechange:'修改文件',read_image:'查看图片',imageview:'查看图片',job_output:'等待后台任务',todo_write:'更新任务计划'};return labels[name]||(!name||name==='tool'||name==='tool_output'?'工具调用':item.summary||item.payload?.name)}
function activityDetail(item){if(item.payload?.model_request_id){const p=item.payload;return [p.phase==='waiting'?'正在等待模型生成内容':p.first_output_ms!=null?`等待首次输出 ${formatDuration(p.first_output_ms)}`:'未收到有效输出',p.http_status?`HTTP ${p.http_status}`:'',p.error_code||''].filter(Boolean).join(' · ')}if(item.type==='error'||item.type==='warning')return item.summary||compactValue(item.payload?.detail)||item.status;const detail=parsedToolDetail(item),command=detail?.command||detail?.cmd,description=detail?.description,resource=detail?.file_path||detail?.filePath||detail?.path,query=detail?.query;const useful=description||resource||query||(typeof command==='string'?command.split('\n').find(Boolean):'');const result=item.status==='failed'&&compactValue(item.payload?.result);return result||useful||compactValue(item.payload?.text)||({in_progress:'执行中',completed:'已完成',failed:'失败',interrupted:'已中断'}[item.status]||item.status)}
function ActivityStatus({status,active}){if(status==='in_progress'&&active)return <LoaderCircle className="activity-status running" size={14} aria-label="执行中"/>;if(status==='completed')return <CircleCheck className="activity-status completed" size={14} aria-label="执行完成"/>;return <CircleX className="activity-status failed" size={14} aria-label={status==='interrupted'||status==='in_progress'?'执行中断':'执行错误'}/>}

function Sidebar({threads,readTurns,current,onSelect,onNew,onDelete,open,onClose}){
 const groups=groupThreads(threads);
 return <aside className={`sidebar ${open?'open':''}`}>
  <div className="sidebar-head"><button className="brand" onClick={onNew}><img className="brand-mark" src={brandMark} alt=""/><b>updream-harness</b></button><button className="icon-button mobile-only" onClick={onClose} aria-label="关闭会话列表"><X size={17}/></button></div>
  <button className="new-thread" onClick={onNew}><Plus size={16}/>新对话</button>
  <label className="thread-search"><Search size={14}/><input placeholder="搜索对话"/></label>
  <nav className="thread-list">
   {[['今天',groups.today],['之前',groups.previous]].map(([label,rows])=>rows.length?<section key={label}><h2>{label}</h2>{rows.map(thread=>{const running=thread.latest_turn_state&&!terminalStates.has(thread.latest_turn_state),done=thread.unread_completion===true&&readTurns.get(thread.id)!==thread.latest_turn_id;return <div className={`thread-entry ${thread.id===current?.id?'active':''}`} key={thread.id}><button className="thread-select" onClick={()=>onSelect(thread)}><span title={running?'正在执行':done?'已完成，尚未查看':undefined} className={`thread-status ${running?'running':done?'done':''}`}>{running?'':<HarnessIcon harness={thread.harness}/>}</span><span><b>{thread.title}</b><small>{harnessLabels[thread.harness]} · {running?statusLabels[thread.latest_turn_state]:relativeTime(thread.updated_at)}</small></span></button><button className="thread-delete" disabled={running} title={running?'运行中的会话不能删除':'删除会话'} onClick={()=>onDelete(thread)}><Trash2 size={13}/></button></div>})}</section>:null)}
  </nav>
  <button className="archive-link"><Archive size={15}/>已归档</button>
 </aside>;
}

function Activity({items,active,endedAt,failed}){
 const [now,setNow]=useState(Date.now());
 const timing=active&&items.some(item=>item.status==='in_progress');
 useEffect(()=>{if(!timing)return;setNow(Date.now());const timer=setInterval(()=>setNow(Date.now()),1000);return()=>clearInterval(timer)},[timing]);
 if(!items.length)return null;
 const failures=items.filter(item=>item.type==='error'||['failed','interrupted','cancelled'].includes(item.status));
 const failureIds=new Set(failures.map(item=>item.id)),running=items.filter(item=>item.status==='in_progress'&&!failureIds.has(item.id));
 const completed=items.filter(item=>!failureIds.has(item.id)&&item.status!=='in_progress');
 const summary=`正在执行 ${running.length} · 失败 ${failures.length} · 已完成 ${completed.length}`;
 const row=item=>{const elapsed=activityTiming(item,active,now,endedAt);return <div className={`activity-row ${item.status}`} key={item.id}><ActivityStatus status={item.status} active={active}/><span><b>{activityName(item)}</b><small title={activityDetail(item)}>{activityDetail(item)}</small></span><time className={`activity-duration ${elapsed.running?'running':''}`} aria-label={elapsed.label}>{elapsed.label}</time></div>};
 const runningMiddle=running.length>5?running.slice(2,-3):[],runningStart=runningMiddle.length?running.slice(0,2):running,runningEnd=runningMiddle.length?running.slice(-3):[];
 const olderFailures=failures.length>5?failures.slice(0,-5):[],recentFailures=olderFailures.length?failures.slice(-5):failures;
 const olderCompleted=completed.length>6?completed.slice(0,-6):[],recentCompleted=olderCompleted.length?completed.slice(-6):completed;
 return <details className="activity" open={active||Boolean(failed)}><summary><span className={running.length?'activity-pulse':''}></span><b>{summary}</b><ChevronDown size={14}/></summary><div>
  {running.length>0&&<section className="activity-group running"><h3>执行中</h3>{runningStart.map(row)}{runningMiddle.length>0&&<details className="activity-history"><summary>另有 {runningMiddle.length} 个正在执行</summary>{runningMiddle.map(row)}</details>}{runningEnd.map(row)}</section>}
  {failures.length>0&&<section className="activity-group failed"><h3>失败或中断</h3>{olderFailures.length>0&&<details className="activity-history"><summary>较早的 {olderFailures.length} 个失败步骤</summary>{olderFailures.map(row)}</details>}{recentFailures.map(row)}</section>}
  {completed.length>0&&<section className="activity-group completed"><h3>已完成</h3>{olderCompleted.length>0&&<details className="activity-history"><summary>较早的 {olderCompleted.length} 个已完成步骤</summary>{olderCompleted.map(row)}</details>}{recentCompleted.map(row)}</section>}
 </div></details>;
}

function ArtifactRow({item,onOpen}){const mime=item.payload?.mime||'application/octet-stream',url=apiUrl(`artifacts/${item.payload?.artifact_id}`),inline=`${url}?inline=true`,image=mime.startsWith('image/'),video=mime.startsWith('video/');if(image||video)return <section className="artifact-media"><button onClick={()=>onOpen({name:item.summary,mime,url:inline,download:url})}>{image?<img src={inline} alt={item.summary}/>:<video src={inline} preload="metadata" muted/>}<span><Maximize2 size={13}/>{item.summary}{item.payload?.partial?'（未完成任务的文件）':''}</span></button><a href={url} download><Download size={13}/>下载</a></section>;return <a className="artifact-row" href={url} download><File size={16}/><span><b>{item.summary}{item.payload?.partial?'（未完成任务的文件）':''}</b><small>{mime} · {item.payload?.size?`${Math.ceil(item.payload.size/1024)} KB`:''}</small></span><Download size={14}/></a>}

function MediaPreview({value,onClose}){const [text,setText]=useState('');useEffect(()=>{setText('');if(value?.mime?.startsWith('text/'))fetch(value.url).then(response=>response.text()).then(setText).catch(()=>setText('无法加载文本预览'))},[value]);if(!value)return null;return <div className="media-preview" role="dialog" aria-modal="true" onClick={onClose}><div onClick={event=>event.stopPropagation()}><header><b>{value.name}</b><a href={value.download} download><Download size={15}/>下载</a><button onClick={onClose} aria-label="关闭预览"><X size={18}/></button></header>{value.mime.startsWith('image/')?<img src={value.url} alt={value.name}/>:value.mime.startsWith('video/')?<video src={value.url} controls autoPlay/>:<pre>{text||'正在加载…'}</pre>}</div></div>}

function InteractionCard({interaction,onRespond}){
 const [answers,setAnswers]=useState(interaction.response?.answers||{}),[busy,setBusy]=useState(false),[error,setError]=useState('');
 const request=interaction.request||{},pending=['pending','responding'].includes(interaction.status);
 const submit=async action=>{if(interaction.status==='responding'&&interaction.response?.action){action=interaction.response.action}setBusy(true);setError('');try{await onRespond(interaction,{idempotency_key:idempotencyKey(),action,answers:interaction.status==='responding'?(interaction.response?.answers||answers):answers})}catch(value){setError(value.message||'提交失败')}finally{setBusy(false)}};
 const questions=(request.questions||[]).length?request.questions:[{id:'answer',question:request.prompt||'请补充信息',options:request.options||[]}];
 return <section className={`interaction-card ${pending?'pending':'resolved'}`}>
  <header><span className={pending?'activity-pulse':''}/><b>{interaction.kind==='approval'?'需要确认':'需要你的回答'}</b><small>{interaction.status==='responding'?'等待确认送达':interaction.status==='expired'?'已失效':pending?'等待操作':'已响应'}</small></header>
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

function Transcript({items,turns,interactions,projectFiles,onInteraction,onArtifact,follow,setFollow}){
 const root=useRef(null),lastLength=useRef(0),turnById=useMemo(()=>new Map(turns.map(turn=>[turn.id,turn])),[turns]);
 useEffect(()=>{if(follow&&items.length!==lastLength.current)root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'});lastLength.current=items.length;},[items,follow]);
 const onScroll=()=>{const node=root.current;setFollow(node.scrollHeight-node.scrollTop-node.clientHeight<80);};
 const groups=[];for(const item of items){let group=groups.at(-1);if(!group||group.turnId!==item.turn_id){group={turnId:item.turn_id,items:[]};groups.push(group)}group.items.push(item)}
 return <div className="transcript" ref={root} onScroll={onScroll}><div className="transcript-inner">{!items.length?<div className="welcome"><div className="welcome-mark"><img src={brandMark} alt=""/></div><h1>今天想做什么？</h1></div>:groups.map(group=>{const turn=turnById.get(group.turnId),activity=group.items.filter(item=>activityTypes.has(item.type)).map(item=>displayItem(item,turn)),active=turn&&!terminalStates.has(turn.state),turnInteractions=interactions.filter(item=>item.turn_id===group.turnId);return <section className="turn" key={group.turnId}>{group.items.filter(item=>item.type==='user_message').map(item=><div className="user-message" key={item.id}>{item.summary}{item.payload?.input_id&&item.status!=='completed'&&<small className={`input-status ${item.status}`}>{item.status==='failed'?'未能确认处理，请核对后重试':'消息已记录，等待处理'}</small>}</div>)}{group.items.filter(item=>item.type==='agent_message').map(item=><div className={`agent-message ${item.status==='in_progress'?'streaming':''}`} key={item.id}><div className="agent-avatar"><HarnessIcon harness={turn?.harness}/></div><div className="agent-copy markdown-copy"><AgentMarkdown text={item.summary} artifacts={group.items.filter(value=>value.type==='artifact')} files={projectFiles||[]} onPreview={onArtifact}/></div></div>)}<Activity items={activity} active={active} endedAt={turn?.updated_at} failed={turn?.state==='FAILED'}/>{turnInteractions.map(interaction=><InteractionCard key={interaction.id} interaction={interaction} onRespond={onInteraction}/>)}{group.items.filter(item=>item.type==='artifact').map(item=><ArtifactRow key={item.id} item={item} onOpen={onArtifact}/>)}</section>})}</div>{!follow&&<button className="latest" onClick={()=>{setFollow(true);root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'})}}><ArrowDown size={14}/>回到最新</button>}</div>;
}

function Composer({disabled,running,harnessLocked,settings,setSettings,catalog,onSend,onStop,stopping}){
 const [text,setText]=useState(''),[files,setFiles]=useState([]),[uploading,setUploading]=useState(false),[submitting,setSubmitting]=useState(false),[error,setError]=useState(''),[composing,setComposing]=useState(false);const fileInput=useRef(null),composition=useRef(false),compositionEndedAt=useRef(-Infinity),pointerStartedDuringComposition=useRef(false);
 const attach=async selected=>{setUploading(true);setError('');try{for(const file of selected)setFiles(current=>[...current,{file,remote:null}]);for(const file of selected){const remote=await upload(file);setFiles(current=>current.map(item=>item.file===file?{...item,remote}:item));}}catch(value){setError(value.message||'上传失败')}finally{setUploading(false)}};
 const submit=async event=>{event.preventDefault();const blocked=composition.current||pointerStartedDuringComposition.current||performance.now()-compositionEndedAt.current<80;pointerStartedDuringComposition.current=false;if(blocked||!text.trim()||uploading||submitting||files.some(item=>!item.remote))return;setSubmitting(true);setError('');try{await onSend(text.trim(),files.map(item=>item.remote).filter(Boolean));setText('');setFiles([])}catch(value){setError(value.message||'消息未发送成功')}finally{setSubmitting(false)}};
 const selectedHarness=catalog.find(item=>item.id===settings.harness),catalogModels=selectedHarness?.models||[],fallbackModel=catalogModels.find(item=>item.available!==false)||catalogModels[0],models=catalogModels.some(item=>item.id===settings.model)||!settings.model?[...catalogModels]:[...catalogModels,{id:settings.model,label:`${settings.model}（当前选择）`,reasoning:fallbackModel?.reasoning||[],available:false,error_code:'catalog_removed'}],selectedModel=models.find(item=>item.id===settings.model)||fallbackModel,levels=selectedModel?.reasoning||[];
 const chooseHarness=id=>{const next=catalog.find(item=>item.id===id),model=next?.models.find(item=>item.id===next.default_model&&item.available!==false)||next?.models.find(item=>item.available!==false),level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({harness:id,model:model?.id||'',reasoning_level:level?.id||''})};
 const chooseModel=id=>{const model=models.find(item=>item.id===id),level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({...settings,model:id,reasoning_level:level?.id||''})};
 return <div className="composer-shell">{error&&<div className="interaction-error" role="alert">{error}</div>}{files.length>0&&<div className="attachments">{files.map((item,index)=><span key={index}><File size={13}/>{item.file.name}<small>{item.remote?'已上传':'上传中'}</small><button type="button" aria-label={`移除 ${item.file.name}`} onClick={()=>setFiles(files.filter((_,i)=>i!==index))}><X size={12}/></button></span>)}</div>}<form className="composer" onSubmit={submit}><div className={`composer-config ${harnessLocked?'thread-config':''}`}>{!harnessLocked&&<label><span>Harness</span><select aria-label="Harness" value={settings.harness} onChange={event=>chooseHarness(event.target.value)}>{catalog.map(item=><option value={item.id} key={item.id} disabled={item.available===false}>{item.label}{item.available===false?'（暂无可用模型）':''}</option>)}</select></label>}<label><span>模型</span><select value={settings.model} disabled={running} onChange={event=>chooseModel(event.target.value)} aria-label="模型">{models.map(item=><option value={item.id} key={item.id} disabled={item.available===false}>{item.label}{modelHealthLabel(item)?`（${modelHealthLabel(item)}）`:''}</option>)}</select></label><label><span>推理强度</span><select aria-label="推理强度" value={settings.reasoning_level} disabled={running} onChange={event=>setSettings({...settings,reasoning_level:event.target.value})}>{levels.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select></label></div>{modelHealthLabel(selectedModel||{})&&<div className="model-health-note" role="status">{settings.model}：{modelHealthLabel(selectedModel||{})}。{selectedModel?.available===false?'已保留你的选择，请手动选择其他模型。':'你的模型选择已保留。'}</div>}<textarea autoFocus value={text} onChange={event=>setText(event.target.value)} onCompositionStart={()=>{composition.current=true;setComposing(true)}} onCompositionEnd={()=>{composition.current=false;compositionEndedAt.current=performance.now();setComposing(false)}} onKeyDown={event=>{if(event.key==='Enter'&&!event.shiftKey){if(event.nativeEvent.isComposing||composition.current||event.keyCode===229)return;if(performance.now()-compositionEndedAt.current<80){event.preventDefault();return}event.preventDefault();event.currentTarget.form.requestSubmit()}}} placeholder="输入消息，描述你要完成的任务…" disabled={disabled||submitting}/><div className="composer-bar"><button type="button" className="attach-button" onClick={()=>fileInput.current.click()}><Paperclip size={16}/><span>{uploading?'上传中':'添加文件'}</span></button><input ref={fileInput} type="file" multiple hidden onChange={event=>{attach([...event.target.files]);event.target.value=''}}/><span className="composer-spacer"/>{running&&<button type="button" className="send stop" disabled={stopping} onClick={async()=>{try{await onStop()}catch(err){setError(err.message||'停止失败，请重试')}}} aria-label={stopping?'正在停止':'停止'}><Square size={14} fill="currentColor"/></button>}<button className="send" onPointerDown={()=>{pointerStartedDuringComposition.current=composition.current}} disabled={disabled||composing||!text.trim()||uploading||submitting||files.some(item=>!item.remote)||(!running&&selectedModel?.available===false)} aria-label={running?'追加消息':'发送'}><Send size={16}/></button></div></form></div>;
}

function ProjectFiles({title,files,onRename,onDelete,onHistory,onOpen}){
 const [editing,setEditing]=useState(''),[name,setName]=useState(''),[histories,setHistories]=useState({});
 const history=async item=>{if(histories[item.id]){setHistories({...histories,[item.id]:null});return}setHistories({...histories,[item.id]:await onHistory(item)})};
 return <section><h2>{title}</h2>{files.length?files.map(item=>{const url=apiUrl(`project-files/${item.id}`),filename=item.path.split('/').at(-1),previewable=item.mime.startsWith('image/')||item.mime.startsWith('video/')||item.mime.startsWith('text/'),label=<><b title={filename}>{filename}</b><small title={item.path}>{item.path} · v{item.version} · {item.size>=1048576?`${(item.size/1048576).toFixed(1)} MB`:`${Math.max(1,Math.ceil(item.size/1024))} KB`}</small></>;return <div className="project-file-wrap" key={item.id}><div className="project-file"><File size={15}/>{previewable?<button className="project-file-name" onClick={()=>onOpen({name:filename,mime:item.mime,url:`${url}?inline=true`,download:url})}>{label}</button>:<a href={url} download>{label}</a>}<a className="project-file-action" title="下载" href={url} download><Download size={13}/></a><button title="版本记录" onClick={()=>history(item)}><Clock3 size={13}/></button><button title="重命名" onClick={()=>{setEditing(item.id);setName(filename)}}><Pencil size={13}/></button><button title="删除" onClick={()=>onDelete(item)}><Trash2 size={13}/></button></div>{editing===item.id&&<form className="project-file-rename" onSubmit={async event=>{event.preventDefault();await onRename(item,item.path.split('/').slice(0,-1).concat(name).join('/'));setEditing('')}}><input value={name} onChange={event=>setName(event.target.value)}/><button>保存</button><button type="button" onClick={()=>setEditing('')}>取消</button></form>}{histories[item.id]&&<div className="project-file-history">{histories[item.id].map(version=><small key={version.id}>v{version.version} · {version.deleted?'已删除':new Date(version.created_at).toLocaleString()}</small>)}</div>}</div>}):<p>暂无文件</p>}</section>;
}

function BackgroundJobs({jobs,onCancel}){return <section><h2>后台任务</h2>{jobs.length?jobs.map(job=><div className="background-job" key={job.id}><span className={job.status==='running'?'activity-pulse':''}/><div><b>{job.name}</b><small>{job.status} · PID {job.pid||'—'}</small>{job.log_tail&&<pre>{job.log_tail}</pre>}</div>{job.status==='running'&&<button onClick={()=>onCancel(job)}><Square size={11}/>停止</button>}</div>):<p>没有后台任务</p>}</section>}

function Inspector({open,onClose,thread,turn,projectFiles,jobs,onCancelJob,onRefresh,onPreview}){
 const input=useRef(null),assets=projectFiles.filter(item=>item.path.startsWith('assets/')),artifacts=projectFiles.filter(item=>item.path.startsWith('artifacts/'));
 const rename=async(item,path)=>{await renameProjectFile(item.id,path);await onRefresh()};
 const remove=async item=>{await deleteProjectFile(item.id);await onRefresh()};
 const history=item=>projectFileHistory(thread.id,item.path);
 const add=async files=>{for(const file of files)await uploadProjectFile(thread.id,file);await onRefresh()};
 return <aside className={`inspector ${open?'open':''}`}><header><b>Workspace</b><button className="icon-button" onClick={onClose}><X size={17}/></button></header><section><h2>Sandbox</h2><dl><dt>Harness</dt><dd>{harnessLabels[turn?.harness||thread?.harness]||'—'}</dd><dt>Model</dt><dd>{turn?.model||thread?.model||'—'}</dd><dt>Status</dt><dd>{turn?statusLabels[turn.state]||turn.state:'Idle'}</dd></dl></section><BackgroundJobs jobs={jobs} onCancel={onCancelJob}/>{thread&&<button className="workspace-upload" onClick={()=>input.current.click()}><Upload size={14}/>上传到项目素材</button>}<input hidden multiple type="file" ref={input} onChange={event=>{add([...event.target.files]);event.target.value=''}}/><ProjectFiles title="项目素材" files={assets} onRename={rename} onDelete={remove} onHistory={history} onOpen={onPreview}/><ProjectFiles title="项目成果" files={artifacts} onRename={rename} onDelete={remove} onHistory={history} onOpen={onPreview}/></aside>;
}

function App(){
 const [threads,setThreads]=useState([]),[catalog,setCatalog]=useState([]),[thread,setThread]=useState(null),[turns,setTurns]=useState([]),[items,setItems]=useState([]),[interactions,setInteractions]=useState([]),[projectFiles,setProjectFiles]=useState([]),[backgroundJobs,setBackgroundJobs]=useState([]),[sidebar,setSidebar]=useState(false),[inspector,setInspector]=useState(false),[preview,setPreview]=useState(null),[follow,setFollow]=useState(true),[settings,setSettings]=useState({harness:'claude',model:'',reasoning_level:''});const source=useRef(null),refreshTimer=useRef(null),selectedThread=useRef(null),viewEpoch=useRef(0),streamTurn=useRef(null),refreshing=useRef(false),refreshPending=useRef(false),readTurns=useRef(new Map()),reading=useRef(new Set());
 const activeTurn=turns.find(turn=>!terminalStates.has(turn.state));
 const closeStream=()=>{source.current?.close();source.current=null;streamTurn.current=null};
 const connect=(turn,selected)=>{
  if(selectedThread.current!==selected.id||streamTurn.current===turn.id)return;
  closeStream();streamTurn.current=turn.id;const stream=new EventSource(apiUrl(`turns/${turn.id}/events`));source.current=stream;
  const epoch=viewEpoch.current,update=()=>{if(epoch!==viewEpoch.current||selectedThread.current!==selected.id||refreshTimer.current)return;refreshTimer.current=setTimeout(()=>{refreshTimer.current=null;refresh(selected,epoch).catch(()=>{})},100)};
  for(const event of ['item.created','item.updated','item.delta','text_delta','tool_started','tool_output','tool_finished','step_started','step_progress','step_finished','artifact_created','waiting_user','warning','state','error'])stream.addEventListener(event,update);
  stream.onerror=update;
 };
 const refresh=useCallback(async(selected,epoch=viewEpoch.current)=>{
  if(!selected||selectedThread.current!==selected.id)return;
  if(refreshing.current===epoch){refreshPending.current=epoch;return}
  refreshing.current=epoch;
  try{
   const nextTurns=await listTurns(selected.id),active=nextTurns.find(turn=>!terminalStates.has(turn.state));
   const [nextItems,nextInteractions,nextFiles,nextJobs]=await Promise.all([listItems(selected.id),listInteractions(selected.id),listProjectFiles(selected.id),active?listBackgroundJobs(active.id).catch(()=>[]):[]]);
   if(epoch!==viewEpoch.current||selectedThread.current!==selected.id)return;
   setTurns(nextTurns);setItems(nextItems);setInteractions(nextInteractions);setProjectFiles(nextFiles);setBackgroundJobs(nextJobs);
   if(active)connect(active,selected);else closeStream();
  }finally{
   if(refreshing.current===epoch)refreshing.current=false;
   if(refreshPending.current===epoch){refreshPending.current=false;if(epoch===viewEpoch.current&&selectedThread.current===selected.id)setTimeout(()=>refresh(selected,epoch),0)}
  }
 },[]);
 useEffect(()=>{
  const latest=turns.at(-1),selectedId=thread?.id,summary=threads.find(row=>row.id===selectedId);
  const mark=async()=>{
   if(!latest||(summary?.latest_turn_id&&summary.latest_turn_id!==latest.id)||latest.thread_id!==selectedId||latest.state!=='COMPLETED'||document.visibilityState!=='visible'||!document.hasFocus()||selectedThread.current!==selectedId||readTurns.current.get(selectedId)===latest.id||reading.current.has(latest.id))return;
   reading.current.add(latest.id);
   try{const result=await markThreadRead(selectedId,latest.id);if(result.read_turn_id===latest.id){readTurns.current.set(selectedId,latest.id);setThreads(rows=>rows.map(row=>row.id===selectedId&&row.latest_turn_id===latest.id?{...row,last_read_turn_id:latest.id,unread_completion:false}:row))}}catch{}finally{reading.current.delete(latest.id)}
  };
  mark();window.addEventListener('focus',mark);document.addEventListener('visibilitychange',mark);
  return()=>{window.removeEventListener('focus',mark);document.removeEventListener('visibilitychange',mark)};
 },[thread?.id,turns,items,threads]);
 const applyCatalog=useCallback(options=>{setCatalog(options);if(!options.length)return;setSettings(current=>current.model?current:loadModelChoice('draft')||defaultSettings(options))},[]);
 useEffect(()=>{saveModelChoice(thread?.id||'draft',settings)},[settings,thread?.id]);
 useEffect(()=>{ensureSession().then(()=>Promise.all([listThreads(),getHarnessCatalog()])).then(([rows,options])=>{setThreads(rows);applyCatalog(options);const requested=rows.find(value=>value.id===urlThread());if(requested)select(requested,'replace');else if(urlThread())writeThreadUrl('','replace')});const catalogTimer=setInterval(()=>getHarnessCatalog().then(applyCatalog).catch(()=>{}),60000);return()=>{clearInterval(catalogTimer);source.current?.close()}},[applyCatalog]);
 useEffect(()=>{const timer=setInterval(()=>{listThreads().then(setThreads).catch(()=>{});if(selectedThread.current&&streamTurn.current)refresh({id:selectedThread.current}).catch(()=>{})},2000);return()=>clearInterval(timer)},[]);
 const select=async(value,historyMode='push')=>{viewEpoch.current+=1;selectedThread.current=value.id;clearTimeout(refreshTimer.current);refreshTimer.current=null;closeStream();setThread(value);setSettings(loadModelChoice(value.id,value.harness)||availableThreadSettings(catalog,{harness:value.harness,model:value.model,reasoning_level:value.reasoning_level}));setSidebar(false);setFollow(true);if(historyMode!=='none')writeThreadUrl(value.id,historyMode);await refresh(value,viewEpoch.current)};
 const newThread=(historyMode='push')=>{viewEpoch.current+=1;selectedThread.current=null;clearTimeout(refreshTimer.current);refreshTimer.current=null;closeStream();setThread(null);setTurns([]);setItems([]);setInteractions([]);setProjectFiles([]);setBackgroundJobs([]);setSidebar(false);setFollow(true);if(historyMode!=='none')writeThreadUrl('',historyMode);if(catalog.length)setSettings(loadModelChoice('draft')||defaultSettings(catalog))};
 useEffect(()=>{const navigate=()=>{const target=threads.find(value=>value.id===urlThread());if(target&&target.id!==selectedThread.current)select(target,'none');else if(!target&&selectedThread.current)newThread('none')};addEventListener('popstate',navigate);return()=>removeEventListener('popstate',navigate)},[threads,catalog]);
 const removeThread=async value=>{if(!confirm(`删除会话“${value.title}”？`))return;await deleteThread(value.id);setThreads(current=>current.filter(item=>item.id!==value.id));if(value.id===selectedThread.current)newThread('replace')};
 const send=async(message,attachments)=>{let selected=thread;if(!selected){selected=await createThread({...settings,title:message.slice(0,72)});viewEpoch.current+=1;selectedThread.current=selected.id;setThreads(current=>[selected,...current]);setThread(selected);writeThreadUrl(selected.id)}if(activeTurn){try{await appendTurnInput(activeTurn.id,{message,attachments,idempotency_key:idempotencyKey()});await refresh(selected);return}catch(error){await refresh(selected);if(error.code!=='turn_not_accepting_input')throw error}}const turn=await createTurn(selected.id,{message,attachments,idempotency_key:idempotencyKey(),...settings,delivery_mode:activeTurn?'queue':'start'});setTurns(current=>[...current,turn]);connect(turn,selected);await refresh(selected)};
 const stop=async()=>{if(activeTurn){await interruptTurn(activeTurn.id);await refresh(thread)}};
 const answerInteraction=async(interaction,value)=>{await respondInteraction(interaction.id,value);await refresh(thread)};
 const cancelJob=async job=>{if(activeTurn){await cancelBackgroundJob(activeTurn.id,job.id);await refresh(thread)}};
 const latest=turns.at(-1);
 return <div className={`app ${inspector?'with-inspector':''}`}><Sidebar threads={threads} readTurns={readTurns.current} current={thread} onSelect={select} onNew={newThread} onDelete={removeThread} open={sidebar} onClose={()=>setSidebar(false)}/><main><header className="topbar"><button className="icon-button mobile-only" onClick={()=>{setInspector(false);setSidebar(true)}}><Menu size={18}/></button><div><h1>{thread?.title||'新对话'}</h1><span>{latest?`${harnessLabels[latest.harness]} · ${latest.model} · ${latest.cancel_requested&&!terminalStates.has(latest.state)?'正在停止':statusLabels[latest.state]||latest.state}`:'在下方选择 Harness、模型与推理强度'}</span></div><div className="top-actions"><button className="icon-button" onClick={()=>{setSidebar(false);setInspector(!inspector)}} title="Workspace"><PanelRight size={18}/></button></div></header><Transcript items={items} turns={turns} interactions={interactions} projectFiles={projectFiles} onInteraction={answerInteraction} onArtifact={setPreview} follow={follow} setFollow={setFollow}/><Composer stopping={Boolean(activeTurn?.cancel_requested)} disabled={!catalog.length||Boolean(activeTurn?.cancel_requested)} running={Boolean(activeTurn)} harnessLocked={Boolean(thread)} settings={settings} setSettings={setSettings} catalog={catalog} onSend={send} onStop={stop}/></main><Inspector open={inspector} onClose={()=>setInspector(false)} thread={thread} turn={latest} projectFiles={projectFiles} jobs={backgroundJobs} onCancelJob={cancelJob} onRefresh={()=>refresh(thread)} onPreview={setPreview}/>{sidebar&&<button className="scrim mobile-only" onClick={()=>setSidebar(false)}/>}<MediaPreview value={preview} onClose={()=>setPreview(null)}/></div>;
}

createRoot(document.getElementById('root')).render(<App/>);
