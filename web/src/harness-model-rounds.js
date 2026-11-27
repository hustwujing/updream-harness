import {formatDuration,timestamp} from './activity-time';

const terminalStatuses=new Set(['completed','failed','interrupted']);
const tokenFields=['ordinary_input_tokens','cache_read_input_tokens','cache_write_input_tokens','output_tokens'];

const nonNegative=value=>Number.isFinite(Number(value))&&Number(value)>=0?Number(value):0;

export function modelRoundTokenTotal(round={}){
 return tokenFields.reduce((total,field)=>total+nonNegative(round[field]),0);
}

export function modelRoundDuration(round={},now=Date.now()){
 const stored=nonNegative(round.duration_ms);
 if(terminalStatuses.has(round.status))return stored;
 const started=timestamp(round.started_at);
 return Number.isFinite(started)?Math.max(stored,now-started):stored;
}

export function modelRoundPhaseLabel(round={}){
 if(round.status==='failed')return '失败';
 if(round.status==='interrupted')return '已中断';
 if(round.status==='completed'||round.phase==='completed')return '已完成';
 if(round.phase==='streaming')return '正在生成';
 return '等待响应';
}

export function modelRoundSummary(rounds=[],now=Date.now()){
 const values=Array.isArray(rounds)?rounds:[];
 const completed=values.filter(round=>round.status==='completed').length;
 const failed=values.filter(round=>round.status==='failed').length;
 const interrupted=values.filter(round=>round.status==='interrupted').length;
 const active=[...values].reverse().find(round=>!terminalStatuses.has(round.status));
 const activeDurationMs=active?modelRoundDuration(active,now):0;
 const parts=[];
 if(!values.length)return {total:0,completed:0,failed:0,interrupted:0,activeIndex:0,
  activePhase:'',activeDurationMs:0,label:'暂无模型轮次'};
 if(completed)parts.push(`${completed} 轮已完成`);
 if(failed)parts.push(`${failed} 轮失败`);
 if(interrupted)parts.push(`${interrupted} 轮已中断`);
 if(active)parts.push(`第 ${active.index} 轮${modelRoundPhaseLabel(active)}`,formatDuration(activeDurationMs));
 return {total:values.length,completed,failed,interrupted,activeIndex:active?.index||0,
  activePhase:active?.phase||'',activeDurationMs,label:parts.join(' · ')};
}
