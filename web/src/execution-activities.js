export function activityCursor(rows,current=0){
 return rows.reduce((value,row)=>Math.max(value,Number(row.cursor_sequence??row.sequence??0)),current);
}

export function mergeExecutionActivities(current,incoming){
 const values=new Map(current.map(row=>[row.id||`sequence:${row.sequence}`,row]));
 for(const row of incoming)values.set(row.id||`sequence:${row.sequence}`,row);
 return [...values.values()].sort((left,right)=>(left.sequence??0)-(right.sequence??0));
}

const categories={
 read:{label:'读取资料'},change:{label:'修改文件'},command:{label:'执行处理命令'},
 image:{label:'检查图片'},external:{label:'调用外部工具'},plan:{label:'更新执行计划'},
 tool:{label:'调用工具'},
};

export function executionActivityCategory(row){
 const name=String(row.summary||row.payload?.name||'').toLowerCase();
 if(name==='imageview')return 'image';
 if(name==='mcptoolcall'||name==='dynamictoolcall')return 'external';
 if(name==='read')return 'read';
 if(['write','edit','filechange'].includes(name))return 'change';
 if(name==='plan')return 'plan';
 if(name==='commandexecution'||name==='bash'){
  const actions=row.payload?.detail?.commandActions;
  if(Array.isArray(actions)&&actions.length){
   const types=new Set(actions.map(action=>String(action?.type||'').toLowerCase()));
   if([...types].every(type=>type==='read'))return 'read';
   if([...types].every(type=>['write','edit','delete','move'].includes(type)))return 'change';
  }
  return 'command';
 }
 return 'tool';
}

export function executionActivityCategoryLabel(category){return categories[category]?.label||'执行操作'}

export function groupExecutionActivities(rows){
 const groups=new Map(),visible=[];
 for(const row of rows){
  if(row.kind!=='tool'||row.status==='failed'){visible.push(row);continue}
  const category=executionActivityCategory(row),existing=groups.get(category);
  if(existing){
   existing.activities.push(row);
   existing.operation_count+=1;
   existing.raw_count+=Number(row.raw_count||1);
   existing.cursor_sequence=Math.max(existing.cursor_sequence,Number(row.cursor_sequence??row.sequence??0));
   existing.status=(existing.status==='in_progress'||row.status==='in_progress')?'in_progress':'completed';
   existing.updated_at=row.updated_at||existing.updated_at;
   continue;
  }
  const label=executionActivityCategoryLabel(category);
  const group={...row,id:`activity-group:${category}`,kind:'activity_group',summary:label,
   operation_count:1,raw_count:Number(row.raw_count||1),activities:[row]};
  groups.set(category,group);visible.push(group);
 }
 return visible.sort((left,right)=>(left.sequence??0)-(right.sequence??0));
}
