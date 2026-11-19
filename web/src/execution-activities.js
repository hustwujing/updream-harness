export function activityCursor(rows,current=0){
 return rows.reduce((value,row)=>Math.max(value,Number(row.cursor_sequence??row.sequence??0)),current);
}

export function mergeExecutionActivities(current,incoming){
 const values=new Map(current.map(row=>[row.id||`sequence:${row.sequence}`,row]));
 for(const row of incoming)values.set(row.id||`sequence:${row.sequence}`,row);
 return [...values.values()].sort((left,right)=>(left.sequence??0)-(right.sequence??0));
}
