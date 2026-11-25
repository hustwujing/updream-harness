import {describe,expect,it} from 'vitest';
import {buildHarnessTimeline,currentHarnessStage} from './harness-activity';

const running={id:'job:harness',state:'RUNNING',started_at:'2026-11-19T10:00:00Z'};

describe('friendly harness timeline',()=>{
 it('shows one live model wait when a running harness has no semantic activity',()=>{
  const rows=buildHarnessTimeline([],running,Date.parse('2026-11-19T10:00:12Z'));
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({kind:'model',title:'等待模型响应',status:'in_progress',duration_ms:12000});
  expect(currentHarnessStage(rows,running).title).toBe('等待模型响应');
 });

 it('groups tools from structured metadata and ignores text stream chunks',()=>{
  const rows=buildHarnessTimeline([
   {id:'text',kind:'text_delta',sequence:1,status:'in_progress',summary:'token',payload:{text:'分析'}},
   {id:'read-1',kind:'tool',sequence:2,status:'completed',summary:'commandExecution',raw_count:3,
    created_at:'2026-11-19T10:00:02Z',updated_at:'2026-11-19T10:00:04Z',
    payload:{detail:{commandActions:[{type:'read',path:'/workspace/project/source.pdf'}]}}},
   {id:'read-2',kind:'tool',sequence:5,status:'completed',summary:'Read',raw_count:2,
    created_at:'2026-11-19T10:00:05Z',updated_at:'2026-11-19T10:00:06Z',payload:{detail:{path:'brief.txt'}}},
   {id:'render',kind:'tool',sequence:7,status:'in_progress',summary:'commandExecution',raw_count:4,
    created_at:'2026-11-19T10:00:07Z',updated_at:'2026-11-19T10:00:09Z',
    payload:{detail:{commandActions:[{type:'unknown'}]}}},
  ],running,Date.parse('2026-11-19T10:00:10Z'));
  expect(rows.map(row=>[row.title,row.operation_count,row.status])).toEqual([
   ['读取资料',2,'completed'],['执行处理命令',1,'in_progress']]);
  expect(rows[0].detail).toBe('读取 brief.txt · 2 项操作');
  expect(rows[1].detail).toBe('1 项操作');
  expect(rows[0].duration_ms).toBe(3000);
  expect(currentHarnessStage(rows,running).title).toBe('执行处理命令');
 });

 it('keeps a failed activity independent and reports a terminal harness state',()=>{
  const node={...running,state:'FAILED',ended_at:'2026-11-19T10:00:10Z'};
  const rows=buildHarnessTimeline([{id:'failure',kind:'error',sequence:3,status:'failed',
   summary:'生成演示文稿失败',created_at:'2026-11-19T10:00:08Z',updated_at:'2026-11-19T10:00:08Z',payload:{}}],node,Date.parse('2026-11-19T10:00:12Z'));
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({title:'生成演示文稿失败',status:'failed'});
  expect(currentHarnessStage(rows,node).title).toBe('执行失败');
 });
});
