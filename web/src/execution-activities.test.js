import {describe,expect,it} from 'vitest';
import {activityCursor,groupExecutionActivities,mergeExecutionActivities} from './execution-activities';

describe('execution activity projection',()=>{
 it('updates one semantic activity in place as its raw cursor advances',()=>{
  const started={id:'activity-1',sequence:10,cursor_sequence:10,status:'in_progress',raw_count:1};
  const completed={...started,cursor_sequence:42,status:'completed',raw_count:33};
  expect(mergeExecutionActivities([started],[completed])).toEqual([completed]);
  expect(activityCursor([completed],10)).toBe(42);
 });
 it('keeps concurrent semantic activities ordered by first sequence',()=>{
  const first={id:'first',sequence:2,cursor_sequence:8};
  const second={id:'second',sequence:4,cursor_sequence:7};
  expect(mergeExecutionActivities([second],[first]).map(row=>row.id)).toEqual(['first','second']);
 expect(activityCursor([first,second])).toBe(8);
 });
 it('groups successful tools by structured activity category without parsing commands',()=>{
  const rows=[
   {id:'read-1',kind:'tool',sequence:2,cursor_sequence:3,status:'completed',raw_count:2,
    summary:'commandExecution',payload:{detail:{command:'cat a',commandActions:[{type:'read',path:'/internal/a'}]}}},
   {id:'read-2',kind:'tool',sequence:4,cursor_sequence:5,status:'completed',raw_count:2,
    summary:'commandExecution',payload:{detail:{command:'cat b',commandActions:[{type:'read',path:'/internal/b'}]}}},
   {id:'command-1',kind:'tool',sequence:6,cursor_sequence:8,status:'in_progress',raw_count:3,
    summary:'commandExecution',payload:{detail:{command:'python render.py',commandActions:[{type:'unknown'}]}}},
   {id:'image-1',kind:'tool',sequence:9,cursor_sequence:10,status:'completed',raw_count:2,
    summary:'imageView',payload:{detail:{path:'/workspace/project/work/contact.png'}}},
  ];
  const grouped=groupExecutionActivities(rows);
  expect(grouped.map(row=>[row.summary,row.operation_count,row.status])).toEqual([
   ['读取资料',2,'completed'],['执行处理命令',1,'in_progress'],['检查图片',1,'completed']]);
  expect(grouped[0].activities.map(row=>row.id)).toEqual(['read-1','read-2']);
  expect(grouped[0].raw_count).toBe(4);
 });
 it('keeps failures and user-visible control events outside tool groups',()=>{
  const failed={id:'failed',kind:'tool',sequence:1,status:'failed',summary:'commandExecution',payload:{}};
  const warning={id:'warning',kind:'warning',sequence:2,status:'completed',summary:'连接恢复',payload:{}};
  expect(groupExecutionActivities([failed,warning])).toEqual([failed,warning]);
 });
});
