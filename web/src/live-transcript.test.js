import {describe,expect,it} from 'vitest';

import {mergeLiveTranscript,recordLiveTranscript} from './live-transcript';


describe('live transcript overlay',()=>{
 it('deduplicates Runtime sequences and reconciles a newer durable snapshot',()=>{
  const durable={id:'message',type:'agent_message',status:'in_progress',summary:'A',
   payload:{_execution_id:'execution',_runtime_sequence:1}};
  let overlays=recordLiveTranscript(new Map(),{execution_id:'execution',parts:[
   {sequence:2,text:'B'},{sequence:3,text:'C'}]});
  let merged=mergeLiveTranscript([durable],overlays);overlays=merged.overlays;
  expect(merged.items[0].summary).toBe('ABC');

  overlays=recordLiveTranscript(overlays,{execution_id:'execution',parts:[
   {sequence:3,text:'C'},{sequence:4,text:'D'}]});
  merged=mergeLiveTranscript(merged.items,overlays);overlays=merged.overlays;
  expect(merged.items[0].summary).toBe('ABCD');

  merged=mergeLiveTranscript([{...durable,summary:'ABC',
   payload:{...durable.payload,_runtime_sequence:3}}],overlays);
  expect(merged.items[0].summary).toBe('ABCD');
  expect([...merged.overlays.get('execution').keys()]).toEqual([4]);
 });
});
