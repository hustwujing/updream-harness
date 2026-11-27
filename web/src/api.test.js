import {afterEach,describe,expect,it,vi} from 'vitest';

import {api,apiUrl,deleteThread,registerAccount,setThreadArchived,terminalSocketUrl} from './api';
import {activityTiming,formatDuration,timestamp} from './activity-time';


afterEach(()=>{vi.restoreAllMocks();delete globalThis.document;delete globalThis.fetch});

describe('API boundary',()=>{
 it('keeps every request under the deployed base path',()=>{
  globalThis.document={baseURI:'https://example.test/sandbox-harness-platform-parallel/'};
  expect(apiUrl('threads')).toBe('https://example.test/sandbox-harness-platform-parallel/api/threads');
  expect(terminalSocketUrl('thread','ticket',4)).toBe(
   'wss://example.test/sandbox-harness-platform-parallel/api/threads/thread/sandbox/terminal?ticket=ticket&since=4');
 });
 it('maps a stable public error and preserves the troubleshooting id',async()=>{
  globalThis.document={baseURI:'https://example.test/sandbox-harness-platform-parallel/'};
  globalThis.fetch=vi.fn(async()=>new Response(JSON.stringify({detail:'authentication_required'}),{
   status:401,headers:{'x-request-id':'request-1'}}));
 await expect(api('threads')).rejects.toMatchObject({status:401,code:'authentication_required',
   requestId:'request-1',message:'请先登录。（排查编号：request-1）'});
 });
 it('classifies expired and changed media confirmations clearly',async()=>{
  globalThis.document={baseURI:'https://example.test/app/'};
  const replies=['media_confirmation_expired','media_confirmation_state_changed'];
  globalThis.fetch=vi.fn(async()=>new Response(JSON.stringify({detail:replies.shift()}),{status:409}));
  await expect(api('media/drafts/expired/confirm',{method:'POST'})).rejects.toMatchObject({
   code:'media_confirmation_expired',
   message:'确认已过期，未提交任何生成任务。发送“继续生成刚才的内容，参数不变”，Agent 会创建新的确认单。'});
  await expect(api('media/drafts/cancelled/confirm',{method:'POST'})).rejects.toMatchObject({
   code:'media_confirmation_state_changed',
   message:'确认状态已变化，未重复提交生成任务。请刷新后查看当前状态。'});
 });
 it('never exposes an HTML proxy error body',async()=>{
  globalThis.document={baseURI:'https://example.test/sandbox-harness-platform-parallel/'};
  globalThis.fetch=vi.fn(async()=>new Response('<!doctype html><title>502 Bad Gateway</title>',{
   status:502,headers:{'content-type':'text/html','x-request-id':'request-502'}}));
  await expect(api('threads')).rejects.toMatchObject({status:502,code:'service_temporarily_unavailable',
   requestId:'request-502',message:'服务暂时不可用，正在恢复。（排查编号：request-502）'});
 });
 it('does not expose a plain-text upstream response',async()=>{
  globalThis.document={baseURI:'https://example.test/sandbox-harness-platform-parallel/'};
  globalThis.fetch=vi.fn(async()=>new Response('internal upstream address: secret',{
   status:503,headers:{'content-type':'text/plain'}}));
  await expect(api('threads')).rejects.toMatchObject({status:503,
   code:'service_temporarily_unavailable',message:'服务暂时不可用，正在恢复。'});
 });
 it('maps a browser network failure to a bounded public error',async()=>{
  globalThis.document={baseURI:'https://example.test/sandbox-harness-platform-parallel/'};
  globalThis.fetch=vi.fn(async()=>{throw new TypeError('Failed to fetch internal host')});
  await expect(api('threads')).rejects.toMatchObject({code:'service_connection_failed',
   message:'服务连接暂时中断，正在重试。'});
 });
 it('waits for durable lifecycle completion before reporting success',async()=>{
  globalThis.document={baseURI:'https://example.test/app/'};
  const replies=[
   new Response(JSON.stringify({operation_id:'register-op',state:'pending'}),{status:202}),
   new Response(JSON.stringify({operation_id:'register-op',state:'completed'}),{status:200}),
   new Response(JSON.stringify({authenticated:true,user:{id:'user'}}),{status:200}),
   new Response(JSON.stringify({operation_id:'archive-op',state:'pending'}),{status:202}),
   new Response(JSON.stringify({operation_id:'archive-op',state:'completed'}),{status:200}),
   new Response(JSON.stringify({id:'thread',archived_at:'2026-11-26T00:00:00Z'}),{status:200}),
   new Response(JSON.stringify({operation_id:'delete-op',state:'pending'}),{status:202}),
   new Response(JSON.stringify({operation_id:'delete-op',state:'completed'}),{status:200}),
  ];
  globalThis.fetch=vi.fn(async()=>replies.shift());
  await expect(registerAccount({username:'user',password:'password'})).resolves.toMatchObject({authenticated:true});
  await expect(setThreadArchived('thread',true)).resolves.toMatchObject({id:'thread',archived_at:'2026-11-26T00:00:00Z'});
  await expect(deleteThread('thread')).resolves.toBeNull();
  expect(globalThis.fetch.mock.calls.map(([url])=>String(url))).toEqual([
   'https://example.test/app/api/auth/register',
   'https://example.test/app/api/auth/registrations/register-op',
   'https://example.test/app/api/session',
   'https://example.test/app/api/threads/thread/archive',
   'https://example.test/app/api/lifecycle/archive-op',
   'https://example.test/app/api/threads/thread',
   'https://example.test/app/api/threads/thread',
   'https://example.test/app/api/lifecycle/delete-op',
  ]);
 });
});

describe('activity timing',()=>{
 it('formats fixed and running durations without negative time',()=>{
  expect(formatDuration(0)).toBe('不足 1 秒');expect(formatDuration(65_000)).toBe('1 分 5 秒');
  expect(timestamp('2026-11-16T00:00:00')).toBe(Date.parse('2026-11-16T00:00:00Z'));
  expect(activityTiming({status:'in_progress',created_at:'2026-11-16T00:00:00Z',payload:{}},true,
   Date.parse('2026-11-16T00:00:03Z'),'')).toEqual({running:true,label:'执行中 3 秒'});
 });
});
