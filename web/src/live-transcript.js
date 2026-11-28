export function recordLiveTranscript(overlays,value){
 if(!value||typeof value.execution_id!=='string'||!Array.isArray(value.parts))return overlays;
 const target=value.item_id?`item:${value.item_id}`:`execution:${value.execution_id}`;
 const next=new Map(overlays),current=new Map(next.get(target)||[]);
 for(const part of value.parts){
  if(Number.isInteger(part?.sequence)&&part.sequence>0&&typeof part.text==='string')current.set(part.sequence,part.text);
 }
 next.set(target,current);return next;
}

export function mergeLiveTranscript(items,overlays){
 const retained=new Map(overlays),values=items.map(item=>{
  if(item.type!=='agent_message')return item;
  const execution=String(item.payload?._execution_id||''),target=`item:${item.id}`,
   fallback=`execution:${execution}`,parts=retained.get(target)||retained.get(fallback),
   overlayKey=retained.has(target)?target:fallback;
  if(!execution||!parts)return item;
  const committed=Number(item.payload?._runtime_sequence||0),remaining=new Map(
   [...parts].filter(([sequence])=>sequence>committed).sort((a,b)=>a[0]-b[0]));
  if(remaining.size)retained.set(overlayKey,remaining);else retained.delete(overlayKey);
  const applied=Math.max(committed,Number(item.payload?._live_sequence||0)),fresh=[...remaining]
   .filter(([sequence])=>sequence>applied);
  return fresh.length?{...item,summary:item.summary+fresh.map(([,text])=>text).join(''),
   payload:{...item.payload,_live_sequence:fresh.at(-1)[0]}}:item;
 });
 return {items:values,overlays:retained};
}
