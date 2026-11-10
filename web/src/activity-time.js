export function timestamp(value){
 if(!value)return NaN;
 const text=String(value);
 return Date.parse(/[zZ]$|[+-]\d\d:\d\d$/.test(text)?text:`${text}Z`);
}
export function formatDuration(milliseconds){
 if(!Number.isFinite(milliseconds))return '时间未记录';
 const seconds=Math.max(0,Math.floor(milliseconds/1000));
 if(seconds<1)return '不足 1 秒';
 if(seconds<60)return `${seconds} 秒`;
 const minutes=Math.floor(seconds/60),rest=seconds%60;
 if(minutes<60)return `${minutes} 分 ${rest} 秒`;
 return `${Math.floor(minutes/60)} 小时 ${minutes%60} 分 ${rest} 秒`;
}
export function activityTiming(item,active,now,turnEndedAt){
 if(item.payload?.model_request_id&&item.status==='interrupted')return {running:false,label:'耗时未确认'};
 const running=active&&item.status==='in_progress';
 const start=timestamp(item.created_at);
 const end=running?now:timestamp(item.status==='in_progress'?turnEndedAt:item.updated_at);
 return {running,label:`${running?'执行中':'耗时'} ${formatDuration(end-start)}`};
}
