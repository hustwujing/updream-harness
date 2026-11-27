import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {describe,expect,it} from 'vitest';

import HarnessRunDetails from './HarnessRunDetails';

describe('model round details panel',()=>{
 it('renders the live summary and collapsed per-round detail',()=>{
  const node={id:'job:child',state:'RUNNING',title:'codex · gpt-5.6',objective:'Build assets',
   location_label:'Cloud Sandbox',harness:'codex',model:'gpt-5.6',started_at:'2026-11-27T12:00:00Z',
   token_usage:{ordinary_input_tokens:100,cache_read_input_tokens:50,cache_write_input_tokens:0,
    output_tokens:25},model_rounds:[
    {index:1,request_id:'one',model:'gpt-5.6',phase:'completed',status:'completed',
     duration_ms:5000,first_output_ms:1000,ordinary_input_tokens:60,output_tokens:10},
    {index:2,request_id:'two',model:'gpt-5.6',phase:'completed',status:'completed',
     duration_ms:7000,first_output_ms:900,ordinary_input_tokens:40,cache_read_input_tokens:50,
     output_tokens:15},
    {index:3,request_id:'three',model:'gpt-5.6',phase:'streaming',status:'in_progress',
     started_at:'2026-11-27T12:00:10Z',duration_ms:1000,output_characters:128},
   ]};
  const html=renderToStaticMarkup(<HarnessRunDetails node={node} now={Date.parse('2026-11-27T12:00:22Z')}/>);
  expect(html).toContain('2 轮已完成 · 第 3 轮正在生成 · 12 秒');
  expect(html).toContain('<details class="harness-model-rounds"');
  expect(html).toContain('第 3 轮');
  expect(html).toContain('正在生成');
  expect(html).toContain('data-token-kind="characters"');
  expect(html).toContain('128 字符');
 });
});
