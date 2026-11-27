import React from 'react';
import {CircleAlert,CircleCheck,Clock3,LoaderCircle,MapPin,MessagesSquare} from 'lucide-react';
import {apiUrl} from './api';
import {formatDuration} from './activity-time';
import {buildHarnessTimeline,currentHarnessStage} from './harness-activity';
import {modelRoundDuration,modelRoundPhaseLabel,modelRoundSummary,modelRoundTokenTotal} from './harness-model-rounds';
import {executionActivityCategory,executionActivityCategoryLabel,executionActivityDetail,executionActivityHasFullDetail} from './execution-activities';
import {useAnimatedNumber} from './animated-number';
import './harness-run-details.css';
import './harness-token-usage.css';

const activeStates=new Set(['CREATED','QUEUED','RUNNING','WAITING_EXTERNAL','CANCELLING','PAUSING']);
const tokenMetrics=[['ordinary','普通输入','ordinary_input_tokens'],['cache-read','缓存读取','cache_read_input_tokens'],
 ['cache-write','缓存写入','cache_write_input_tokens'],['output','输出','output_tokens']];
export function tokenUsageTotal(usage={}){return tokenMetrics.reduce((total,value)=>total+Math.max(0,Number(usage[value[2]]||0)),0)}
export function formatCompactTokens(value){const amount=Math.max(0,Number(value||0));if(amount>=1_000_000)return `${Number((amount/1_000_000).toFixed(amount>=10_000_000?0:1))}M`;if(amount>=1000)return `${Number((amount/1000).toFixed(amount>=100_000?0:1))}k`;return Math.round(amount).toLocaleString()}
function AnimatedToken({nodeId,kind,value}){const display=useAnimatedNumber(nodeId,value,kind==='characters'?350:600);return <strong data-token-kind={kind} data-token-value={Math.round(display)} title={`${Math.round(Number(value||0)).toLocaleString()} ${kind==='characters'?'字符':'tokens'}`}>{formatCompactTokens(display)}</strong>}
function ActivityDetail({activity}){const value=executionActivityDetail(activity,{limit:120});
 return executionActivityHasFullDetail(value)?<details className="execution-activity-detail">
  <summary>{value.preview}</summary><pre>{value.full}</pre></details>:<small title={value.full||value.preview}>{value.preview}</small>}
function totalElapsed(node,now){
 if(node.ended_at)return Number(node.duration_ms||Math.max(0,Date.parse(node.ended_at)-Date.parse(node.started_at))||0);
 return node.started_at?Math.max(0,now-Date.parse(node.started_at)):0;
}
function StageIcon({status,active}){
 if(status==='failed')return <CircleAlert size={14}/>;
 if(status==='completed')return <CircleCheck size={14}/>;
 return active?<LoaderCircle className="harness-stage-spinner" size={14}/>:<Clock3 size={14}/>;
}
function ModelRounds({node,now}){
 const rounds=Array.isArray(node.model_rounds)?node.model_rounds:[],summary=modelRoundSummary(rounds,now);
 if(!rounds.length)return null;
 return <><p className="harness-model-round-summary"><MessagesSquare size={12}/><span>{summary.label}</span>
  {node.model_rounds_truncated&&<small>部分数据</small>}</p>
  <details className="harness-model-rounds">
   <summary><span>模型轮次明细</span><strong>{summary.total} 轮</strong></summary>
   <div>{rounds.map(round=>{const tokens=modelRoundTokenTotal(round),phase=modelRoundPhaseLabel(round);
    return <article className={`model-round ${round.status||'in_progress'}`} key={round.request_id||round.index}>
     <header><b>第 {round.index} 轮</b><span>{phase}</span><time>{formatDuration(modelRoundDuration(round,now))}</time></header>
     <small>{round.model||node.model||'模型'}
      {round.first_output_ms!=null?` · 首输出 ${formatDuration(Number(round.first_output_ms))}`:''}
      {tokens>0?` · ${formatCompactTokens(tokens)} tokens`:' · 用量待返回'}
      {round.output_characters>0&&<> · <AnimatedToken nodeId={`${node.id}:${round.request_id}`} kind="characters" value={round.output_characters}/> 字符</>}</small>
    </article>})}</div>
  </details></>;
}

export default function HarnessRunDetails({node,activities=[],progress=null,now=Date.now(),activityError=''}){
 const timeline=buildHarnessTimeline(activities,node,now),current=currentHarnessStage(timeline,node);
 const active=activeStates.has(node.state),elapsed=totalElapsed(node,now);
 return <div className="harness-run-details">
  <section className={`harness-run-overview ${current.status}`}>
   <div className="harness-current-stage"><StageIcon status={current.status} active={active}/>
    <div><small>当前阶段</small><b>{current.title}</b></div><time>{formatDuration(elapsed)}</time></div>
   <p>{node.objective||node.title}</p>
   <div className="harness-run-meta"><span><MapPin size={11}/>{node.location_label}</span>
    {(node.harness||node.model)&&<span>{[node.harness,node.model].filter(Boolean).join(' · ')}</span>}</div>
  </section>
  <section className={`harness-token-usage ${active?'active':''}`}>
   <header><b>模型用量</b><span>{formatCompactTokens(tokenUsageTotal(node.token_usage))} tokens{node.token_usage_truncated?'（部分）':''}</span></header>
   <div>{tokenMetrics.map(([kind,label,field])=><div className="harness-token-metric" key={`${node.id}:${kind}`}>
    <small>{label}</small><AnimatedToken nodeId={node.id} kind={kind} value={node.token_usage?.[field]}/>
   </div>)}</div>
   <ModelRounds node={node} now={now}/>
  </section>
  {progress&&progress.total>0&&<section className="harness-child-progress">
   <header><b>子任务进度</b><span>{progress.terminal}/{progress.total}</span></header>
   <div><i style={{width:`${Math.min(100,Math.round(progress.terminal/progress.total*100))}%`}}/></div>
   <small>{progress.succeeded||0} 成功 · {progress.failed||0} 失败 · {Math.max(0,progress.total-progress.terminal)} 进行中</small>
  </section>}
  {node.error_summary&&<p className="execution-node-error" role="alert"><b>{node.error_summary}</b>{node.error_reason&&<span>{node.error_reason}</span>}{node.error_guidance&&<small>{node.error_guidance}</small>}</p>}
  {activityError&&<p className="interaction-error" role="alert">{activityError}</p>}
  <section className="harness-run-timeline"><h3>执行过程</h3>
   {timeline.length?timeline.map(stage=>stage.kind==='activity_group'?<details className="harness-timeline-group" key={stage.id}>
    <summary><StageIcon status={stage.status} active={active}/><div><b>{stage.title}</b><small>{stage.detail}</small></div><time>{formatDuration(stage.duration_ms)}</time></summary>
    <div className="harness-timeline-children">{stage.activities.map(activity=><div key={activity.id||activity.sequence}>
     <b>{executionActivityCategoryLabel(executionActivityCategory(activity))}</b><ActivityDetail activity={activity}/>
    </div>)}</div>
   </details>:<article className={`harness-timeline-stage ${stage.status}`} key={stage.id||stage.sequence}>
    <StageIcon status={stage.status} active={active}/><div><b>{stage.title}</b>{stage.detail&&<small>{stage.detail}</small>}</div><time>{formatDuration(stage.duration_ms)}</time>
   </article>):<p>正在准备执行</p>}
  </section>
  {node.artifacts?.length>0&&<section className="harness-run-outputs"><h3>输出</h3><div>{node.artifacts.map(file=><a key={file.artifact_id} href={apiUrl(`artifacts/${file.artifact_id}`)} target="_blank" rel="noreferrer">
   <b>{file.filename}</b><small>{file.artifact_role} · {file.mime}</small></a>)}</div></section>}
 </div>;
}
