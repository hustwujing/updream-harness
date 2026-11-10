const reasons={access_denied:'当前凭据无权限',model_not_supported:'当前通道不支持',model_route_missing:'未配置模型路由'};
export function modelHealthLabel(model){
 if(model.available===false)return reasons[model.blocking_reason||model.error_code]||'当前不可用';
 if(model.status==='busy')return '暂时繁忙，可尝试';
 if(model.status==='unknown')return '尚未确认，可尝试';
 return '';
}
export function loadModelChoice(key,harness){
 try{
  const value=JSON.parse(sessionStorage.getItem(`sandbox-harness:model-choice:${key}`)||'null');
  return value&&['codex','claude','deepseek'].includes(value.harness)&&(!harness||harness===value.harness)&&typeof value.model==='string'&&value.model&&typeof value.reasoning_level==='string'?value:null;
 }catch{return null}
}
export function saveModelChoice(key,value){
 if(!value.model)return;
 try{sessionStorage.setItem(`sandbox-harness:model-choice:${key}`,JSON.stringify(value))}catch{}
}
