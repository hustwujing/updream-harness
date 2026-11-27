import {describe,expect,it} from 'vitest';

import {modelRoundDuration,modelRoundPhaseLabel,modelRoundSummary,modelRoundTokenTotal} from './harness-model-rounds';

const now=Date.parse('2026-11-27T12:00:12Z');

describe('model round visibility',()=>{
 it('summarizes completed and active rounds with live elapsed time',()=>{
  const rounds=[
   {index:1,status:'completed',phase:'completed',duration_ms:4000,ordinary_input_tokens:10,output_tokens:2},
   {index:2,status:'completed',phase:'completed',duration_ms:7000,cache_read_input_tokens:8,output_tokens:3},
   {index:3,status:'in_progress',phase:'streaming',started_at:'2026-11-27T12:00:00Z',duration_ms:1000},
  ];
  expect(modelRoundSummary(rounds,now)).toEqual({total:3,completed:2,failed:0,
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
 });

 it('handles empty and missing usage without invalid values',()=>{
  expect(modelRoundSummary([],now).label).toBe('暂无模型轮次');
  expect(modelRoundTokenTotal({ordinary_input_tokens:10,cache_read_input_tokens:null,
   cache_write_input_tokens:undefined,output_tokens:'bad'})).toBe(10);
  expect(modelRoundPhaseLabel({status:'in_progress',phase:'waiting'})).toBe('等待响应');
 });
});
