import {describe,expect,it} from 'vitest';

import {indexModelActivityRounds,modelActivityTitle,modelRoundDuration,modelRoundPhaseLabel,
 modelRoundSummary,modelRoundTokenTotal} from './harness-model-rounds';

const now=Date.parse('2026-11-27T12:00:12Z');

describe('model round visibility',()=>{
 it('summarizes completed and active rounds with live elapsed time',()=>{
  const rounds=[
   {index:1,status:'completed',phase:'completed',duration_ms:4000,ordinary_input_tokens:10,output_tokens:2},
   {index:2,status:'completed',phase:'completed',duration_ms:7000,cache_read_input_tokens:8,output_tokens:3},
   {index:3,status:'in_progress',phase:'streaming',started_at:'2026-11-27T12:00:00Z',duration_ms:1000},
  ];
  expect(modelRoundSummary(rounds,now)).toEqual({total:3,completed:2,failed:0,interrupted:0,
   activeIndex:3,activePhase:'streaming',activeDurationMs:12000,
   label:'2 轮已完成 · 第 3 轮正在生成 · 12 秒'});
 });

 it('keeps terminal duration fixed and reports failures',()=>{
  const failed={index:2,status:'failed',phase:'failed',started_at:'2026-11-27T11:00:00Z',
   ended_at:'2026-11-27T11:00:05Z',duration_ms:5000,error_code:'provider_timeout'};
  expect(modelRoundDuration(failed,now)).toBe(5000);
  expect(modelRoundSummary([{index:1,status:'completed',phase:'completed'},failed],now).label)
   .toBe('1 轮已完成 · 1 轮失败');
  expect(modelRoundPhaseLabel(failed)).toBe('失败');
  const interrupted={index:3,status:'interrupted',phase:'interrupted',duration_ms:8000};
  expect(modelRoundSummary([interrupted],now).label).toBe('1 轮已中断');
  expect(modelRoundPhaseLabel(interrupted)).toBe('已中断');
 });

 it('handles empty and missing usage without invalid values',()=>{
  expect(modelRoundSummary([],now).label).toBe('暂无模型轮次');
  expect(modelRoundTokenTotal({ordinary_input_tokens:10,cache_read_input_tokens:null,
   cache_write_input_tokens:undefined,output_tokens:'bad'})).toBe(10);
  expect(modelRoundPhaseLabel({status:'in_progress',phase:'waiting'})).toBe('等待响应');
 });

 it('numbers primary and each child harness independently for chat activity',()=>{
  const items=[
   {id:'p1',payload:{model_request_id:'p1',job_id:'',model:'gpt-5.6',phase:'completed'},status:'completed'},
   {id:'c1',payload:{model_request_id:'c1',job_id:'job-a',model:'gpt-5.6',phase:'completed'},status:'completed'},
   {id:'p2',payload:{model_request_id:'p2',job_id:'',model:'gpt-5.6',phase:'streaming'},status:'in_progress'},
   {id:'c2',payload:{model_request_id:'c2',job_id:'job-a',model:'gpt-5.6',phase:'waiting'},status:'in_progress'},
   {id:'d1',payload:{model_request_id:'d1',job_id:'job-b',model:'gpt-5.6',phase:'failed'},status:'failed'},
  ],rounds=indexModelActivityRounds(items);
  expect([...rounds.values()].map(value=>[value.scope,value.index])).toEqual([
   ['主任务',1],['子任务',1],['主任务',2],['子任务',2],['子任务',1]]);
  expect(modelActivityTitle(items[2],rounds.get('p2'))).toBe('主任务 · 第 2 轮 · gpt-5.6 正在输出');
  expect(modelActivityTitle(items[3],rounds.get('c2'))).toBe('子任务 · 第 2 轮 · 等待 gpt-5.6 响应');
 });
});
