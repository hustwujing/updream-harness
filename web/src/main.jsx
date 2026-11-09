import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {Archive,ArrowDown,ChevronDown,ChevronRight,File,Menu,MessageSquare,MoreHorizontal,Paperclip,PanelRight,Plus,Search,Send,Square,Terminal,Wrench,X} from 'lucide-react';
import {apiUrl,createThread,createTurn,getHarnessCatalog,interruptTurn,listItems,listThreads,listTurns,upload} from './api';
import './styles.css';
import './mobile.css';

const terminalStates=new Set(['COMPLETED','WAITING_USER','FAILED','CANCELLED']);
const activityTypes=new Set(['reasoning','plan','command','tool','file_change','approval','warning','error']);
const statusLabels={QUEUED:'已排队',RESTORING:'恢复环境',STARTING:'启动沙箱',RUNNING:'正在工作',CHECKPOINTING:'保存环境',WAITING_USER:'等待回复',COMPLETED:'已完成',FAILED:'失败',CANCELLED:'已停止'};
const harnessLabels={claude:'Claude',codex:'Codex',deepseek:'DeepSeek'};

function relativeTime(value){const seconds=Math.max(1,(Date.now()-new Date(value).getTime())/1000);if(seconds<60)return '刚刚';if(seconds<3600)return `${Math.floor(seconds/60)} 分钟前`;if(seconds<86400)return `${Math.floor(seconds/3600)} 小时前`;return new Date(value).toLocaleDateString();}
function groupThreads(rows){const today=[],previous=[];for(const row of rows)(Date.now()-new Date(row.updated_at).getTime()<86400000?today:previous).push(row);return {today,previous};}

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
 return <details className="activity" open={active}><summary><span className={active?'activity-pulse':''}></span><b>{active?'正在工作':`完成了 ${items.length} 个步骤`}</b><ChevronDown size={14}/></summary><div>{items.map(item=><div className={`activity-row ${item.status}`} key={item.id}>{item.type==='command'?<Terminal size={14}/>:<Wrench size={14}/>}<span><b>{item.summary||item.type}</b><small>{item.payload?.detail||item.payload?.name||item.status}</small></span></div>)}</div></details>;
}

function ArtifactRow({item,onOpen}){return <button className="artifact-row" onClick={()=>onOpen(item)}><File size={16}/><span><b>{item.summary}</b><small>{item.payload?.mime||'文件'} · {item.payload?.size?`${Math.ceil(item.payload.size/1024)} KB`:''}</small></span><ChevronRight size={14}/></button>}

function Transcript({items,turns,onArtifact,follow,setFollow}){
 const root=useRef(null),lastLength=useRef(0),turnById=useMemo(()=>new Map(turns.map(turn=>[turn.id,turn])),[turns]);
 useEffect(()=>{if(follow&&items.length!==lastLength.current)root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'});lastLength.current=items.length;},[items,follow]);
 const onScroll=()=>{const node=root.current;setFollow(node.scrollHeight-node.scrollTop-node.clientHeight<80);};
 const groups=[];for(const item of items){let group=groups.at(-1);if(!group||group.turnId!==item.turn_id){group={turnId:item.turn_id,items:[]};groups.push(group)}group.items.push(item)}
 return <div className="transcript" ref={root} onScroll={onScroll}><div className="transcript-inner">{!items.length?<div className="welcome"><div className="welcome-mark">H</div><h1>今天想做什么？</h1></div>:groups.map(group=>{const turn=turnById.get(group.turnId),activity=group.items.filter(item=>activityTypes.has(item.type)),active=turn&&!terminalStates.has(turn.state);return <section className="turn" key={group.turnId}>{group.items.filter(item=>item.type==='user_message').map(item=><div className="user-message" key={item.id}>{item.summary}</div>)}{group.items.filter(item=>item.type==='agent_message').map(item=><div className={`agent-message ${item.status==='in_progress'?'streaming':''}`} key={item.id}><div className="agent-avatar">{harnessLabels[turn?.harness]?.[0]||'H'}</div><div className="agent-copy">{item.summary}</div></div>)}<Activity items={activity} active={active}/>{group.items.filter(item=>item.type==='artifact').map(item=><ArtifactRow key={item.id} item={item} onOpen={onArtifact}/>)}</section>})}</div>{!follow&&<button className="latest" onClick={()=>{setFollow(true);root.current?.scrollTo({top:root.current.scrollHeight,behavior:'smooth'})}}><ArrowDown size={14}/>回到最新</button>}</div>;
}

function Composer({disabled,running,settings,setSettings,catalog,onSend,onStop}){
 const [text,setText]=useState(''),[files,setFiles]=useState([]),[uploading,setUploading]=useState(false);const fileInput=useRef(null);
 const attach=async selected=>{setUploading(true);try{for(const file of selected)setFiles(current=>[...current,{file,remote:null}]);for(const file of selected){const remote=await upload(file);setFiles(current=>current.map(item=>item.file===file?{...item,remote}:item));}}finally{setUploading(false)}};
 const submit=async event=>{event.preventDefault();if(!text.trim()||uploading)return;await onSend(text.trim(),files.map(item=>item.remote).filter(Boolean));setText('');setFiles([])};
 const selectedHarness=catalog.find(item=>item.id===settings.harness),models=selectedHarness?.models||[],selectedModel=models.find(item=>item.id===settings.model)||models[0],levels=selectedModel?.reasoning||[];
 const chooseHarness=id=>{const next=catalog.find(item=>item.id===id),model=next?.models.find(item=>item.id===next.default_model)||next?.models[0],level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({harness:id,model:model?.id||'',reasoning_level:level?.id||''})};
 const chooseModel=id=>{const model=models.find(item=>item.id===id),level=model?.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({...settings,model:id,reasoning_level:level?.id||''})};
 return <div className="composer-shell">{files.length>0&&<div className="attachments">{files.map((item,index)=><span key={index}><File size={13}/>{item.file.name}<button onClick={()=>setFiles(files.filter((_,i)=>i!==index))}><X size={12}/></button></span>)}</div>}<form className="composer" onSubmit={submit}><textarea value={text} onChange={event=>setText(event.target.value)} onKeyDown={event=>{if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();event.currentTarget.form.requestSubmit()}}} placeholder="向 Harness 发送消息" disabled={disabled}/><div className="composer-bar"><button type="button" className="icon-button" onClick={()=>fileInput.current.click()} title="添加附件"><Paperclip size={17}/></button><input ref={fileInput} type="file" multiple hidden onChange={event=>attach([...event.target.files])}/><select value={settings.harness} onChange={event=>chooseHarness(event.target.value)}>{catalog.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select><select value={settings.model} onChange={event=>chooseModel(event.target.value)} aria-label="模型">{models.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select><select value={settings.reasoning_level} onChange={event=>setSettings({...settings,reasoning_level:event.target.value})}>{levels.map(item=><option value={item.id} key={item.id}>{item.label}</option>)}</select><span className="composer-spacer"/>{running?<button type="button" className="send stop" onClick={onStop} aria-label="停止"><Square size={14} fill="currentColor"/></button>:<button className="send" disabled={disabled||!text.trim()||uploading} aria-label="发送"><Send size={16}/></button>}</div></form><small className="composer-note">Harness 可能会调用工具并修改工作区文件</small></div>;
}

function Inspector({open,onClose,items,thread,turn}){const artifacts=items.filter(item=>item.type==='artifact');return <aside className={`inspector ${open?'open':''}`}><header><b>Workspace</b><button className="icon-button" onClick={onClose}><X size={17}/></button></header><section><h2>Sandbox</h2><dl><dt>Harness</dt><dd>{harnessLabels[turn?.harness||thread?.harness]||'—'}</dd><dt>Model</dt><dd>{turn?.model||thread?.model||'—'}</dd><dt>Status</dt><dd>{turn?statusLabels[turn.state]||turn.state:'Idle'}</dd></dl></section><section><h2>Outputs</h2>{artifacts.length?artifacts.map(item=><a key={item.id} href={apiUrl(`artifacts/${item.payload.artifact_id}`)}><File size={15}/><span>{item.summary}</span></a>):<p>还没有输出文件</p>}</section></aside>}

function App(){
 const [threads,setThreads]=useState([]),[catalog,setCatalog]=useState([]),[thread,setThread]=useState(null),[turns,setTurns]=useState([]),[items,setItems]=useState([]),[sidebar,setSidebar]=useState(false),[inspector,setInspector]=useState(false),[follow,setFollow]=useState(true),[settings,setSettings]=useState({harness:'claude',model:'',reasoning_level:''});const source=useRef(null),refreshTimer=useRef(null);
 const activeTurn=[...turns].reverse().find(turn=>!terminalStates.has(turn.state));
 const refresh=useCallback(async selected=>{if(!selected)return;const [nextTurns,nextItems]=await Promise.all([listTurns(selected.id),listItems(selected.id)]);setTurns(nextTurns);setItems(nextItems);const active=[...nextTurns].reverse().find(turn=>!terminalStates.has(turn.state));if(active)connect(active,selected);},[]);
 const connect=(turn,selected=thread)=>{source.current?.close();source.current=new EventSource(apiUrl(`turns/${turn.id}/events`));const update=()=>{clearTimeout(refreshTimer.current);refreshTimer.current=setTimeout(()=>refresh(selected),80)};for(const event of ['item.created','item.updated','item.delta','text_delta','tool_started','tool_output','tool_finished','step_started','step_progress','step_finished','artifact_created','state','error'])source.current.addEventListener(event,update);source.current.onerror=update;};
 useEffect(()=>{Promise.all([listThreads(),getHarnessCatalog()]).then(([rows,options])=>{setThreads(rows);setCatalog(options);const selected=options.find(item=>item.id==='claude')||options[0],model=selected.models.find(item=>item.id===selected.default_model)||selected.models[0],level=model.reasoning[Math.min(1,model.reasoning.length-1)];setSettings({harness:selected.id,model:model.id,reasoning_level:level.id})});return()=>source.current?.close()},[]);
 const select=async value=>{source.current?.close();setThread(value);setSettings({harness:value.harness,model:value.model,reasoning_level:value.reasoning_level});setSidebar(false);setFollow(true);await refresh(value)};
 const newThread=()=>{source.current?.close();setThread(null);setTurns([]);setItems([]);setSidebar(false);setFollow(true)};
 const send=async(message,attachments)=>{let selected=thread;if(!selected){selected=await createThread({...settings,title:message.slice(0,72)});setThreads(current=>[selected,...current]);setThread(selected)}const turn=await createTurn(selected.id,{message,attachments,idempotency_key:crypto.randomUUID(),...settings,delivery_mode:activeTurn?'queue':'start'});setTurns(current=>[...current,turn]);connect(turn,selected);await refresh(selected)};
 const stop=async()=>{if(activeTurn)await interruptTurn(activeTurn.id)};
 const latest=turns.at(-1);
 return <div className={`app ${inspector?'with-inspector':''}`}><Sidebar threads={threads} current={thread} onSelect={select} onNew={newThread} open={sidebar} onClose={()=>setSidebar(false)}/><main><header className="topbar"><button className="icon-button mobile-only" onClick={()=>{setInspector(false);setSidebar(true)}}><Menu size={18}/></button><div><h1>{thread?.title||'New thread'}</h1><span>{latest?`${harnessLabels[latest.harness]} · ${latest.model} · ${statusLabels[latest.state]||latest.state}`:'选择 Harness 后发送消息'}</span></div><div className="top-actions"><button className="icon-button" onClick={()=>{setSidebar(false);setInspector(!inspector)}} title="Workspace"><PanelRight size={18}/></button><button className="icon-button" title="更多"><MoreHorizontal size={18}/></button></div></header><Transcript items={items} turns={turns} onArtifact={()=>{setSidebar(false);setInspector(true)}} follow={follow} setFollow={setFollow}/><Composer disabled={!catalog.length} running={Boolean(activeTurn)} settings={settings} setSettings={setSettings} catalog={catalog} onSend={send} onStop={stop}/></main><Inspector open={inspector} onClose={()=>setInspector(false)} items={items} thread={thread} turn={latest}/>{sidebar&&<button className="scrim mobile-only" onClick={()=>setSidebar(false)}/>}</div>;
}

createRoot(document.getElementById('root')).render(<App/>);
