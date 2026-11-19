import {describe,expect,it} from 'vitest';
import {activityCursor,mergeExecutionActivities} from './execution-activities';

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
});
