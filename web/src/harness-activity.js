import {executionActivityCategory,executionActivityCategoryLabel,groupExecutionActivities} from './execution-activities';

const activeStates=new Set(['CREATED','QUEUED','RUNNING','WAITING_EXTERNAL','CANCELLING','PAUSING']);

function timestamp(value,fallback=0){const result=Date.parse(value||'');return Number.isFinite(result)?result:fallback}
function elapsed(row,now){
 const start=timestamp(row.created_at,now),end=row.status==='in_progress'?now:timestamp(row.updated_at,start);
 return Math.max(0,end-start);
}
function normalize(row){
 const kind=row.kind||row.type||'';
 if(kind==='command')return {...row,kind:'tool',summary:'bash'};
 if(kind==='file_change')return {...row,kind:'tool',summary:'fileChange'};
 return {...row,kind};
}

function friendlyRow(row,now){
 if(row.kind==='activity_group'){
  return {...row,title:row.summary,detail:`${row.operation_count} 项操作`,
   duration_ms:row.activities.reduce((total,activity)=>total+elapsed(activity,now),0)};
 }
 if(row.kind==='tool'){
  const category=executionActivityCategory(row);
  return {...row,title:executionActivityCategoryLabel(category),detail:'1 项操作',
   operation_count:1,duration_ms:elapsed(row,now),activities:[row]};
 }
 const titles={plan:'更新执行计划',waiting_user:'等待用户确认',artifact_created:'生成产物',
  warning:'执行警告',error:'执行失败'};
 return {...row,title:String(row.summary||titles[row.kind]||'执行阶段'),detail:'',
  duration_ms:elapsed(row,now)};
}

export function buildHarnessTimeline(rows,node,now=Date.now()){
 const visible=rows.map(normalize).filter(row=>!['text_delta','reasoning'].includes(row.kind));
 const timeline=groupExecutionActivities(visible).map(row=>friendlyRow(row,now));
 const active=activeStates.has(node?.state),hasActive=timeline.some(row=>row.status==='in_progress');
 if(active&&!hasActive){
  const latest=timeline.reduce((value,row)=>Math.max(value,timestamp(row.updated_at,0)),
   timestamp(node?.started_at,now));
  timeline.push({id:'model-wait',kind:'model',title:node?.state==='WAITING_EXTERNAL'?'等待外部结果':
   node?.state==='QUEUED'?'等待调度':'等待模型响应',detail:'',status:'in_progress',
   sequence:Number.MAX_SAFE_INTEGER,started_at:new Date(latest).toISOString(),duration_ms:Math.max(0,now-latest)});
 }else if(!timeline.length&&node?.state==='SUCCEEDED'){
  const start=timestamp(node.started_at,now),end=timestamp(node.ended_at,now);
  timeline.push({id:'model-complete',kind:'model',title:'模型处理',detail:'',status:'completed',
   sequence:0,started_at:node.started_at,ended_at:node.ended_at,duration_ms:Math.max(0,end-start)});
 }
 return timeline;
}

export function currentHarnessStage(timeline,node){
 const terminal={SUCCEEDED:'已完成',COMPLETED:'已完成',FAILED:'执行失败',CANCELLED:'已取消',
  BLOCKED:'等待父任务处理',UNKNOWN:'结果待对账',PAUSED:'已暂停',WAITING_USER:'等待用户'};
 if(terminal[node?.state])return {title:terminal[node.state],status:node.state==='FAILED'?'failed':'completed'};
 const active=[...timeline].reverse().find(row=>row.status==='in_progress');
 return active||{title:'准备执行',status:'in_progress'};
}
