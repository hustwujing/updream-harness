import {describe,expect,it} from 'vitest';

import {defaultCollapsedGroups,executionGraphView} from './execution-graph-layout';

const node=(id,type,turn_id,started_at='2026-11-18T00:00:00Z')=>({id,type,turn_id,started_at,state:'SUCCEEDED'});
const edge=(from,to,kind='contains')=>({from,to,kind});

describe('execution graph layout',()=>{
 it('lays out a factual branch and join from left to right',()=>{
  const graph={nodes:[node('input','user_turn','turn'),node('primary','primary_harness','turn'),node('group','job_group','turn'),node('a','job','turn'),node('b','job','turn'),node('review','job','turn')],edges:[edge('input','primary','submitted_by'),edge('primary','group','submitted_by'),edge('group','a'),edge('group','b'),edge('a','review','reviewed_by'),edge('b','review','reviewed_by')]};
  const view=executionGraphView(graph,{turnId:'turn'}),byId=new Map(view.nodes.map(value=>[value.id,value]));
  expect(view.edges).toHaveLength(6);
  expect(byId.get('input').x).toBeLessThan(byId.get('primary').x);
  expect(byId.get('group').x).toBeLessThan(byId.get('a').x);
  expect(byId.get('group').x).toBeLessThan(byId.get('b').x);
  expect(byId.get('a').x).toBeLessThan(byId.get('review').x);
  expect(byId.get('a').y).not.toBe(byId.get('b').y);
 });

 it('filters dangling edges and scopes the view to one turn component',()=>{
  const graph={nodes:[node('p1','primary_harness','turn-1','2026-11-18T00:00:00Z'),node('p2','primary_harness','turn-2','2026-11-18T00:01:00Z'),node('j2','job','turn-2')],edges:[edge('p2','j2'),edge('missing','j2')]};
  const view=executionGraphView(graph,{turnId:'turn-2'});
  expect(view.nodes.map(value=>value.id).sort()).toEqual(['j2','p2']);
  expect(view.edges).toEqual([expect.objectContaining({from:'p2',to:'j2'})]);
  expect(view.turns.map(value=>value.id)).toEqual(['turn-1','turn-2']);
 });

 it('keeps connected predecessor work visible from a continuation turn',()=>{
  const graph={nodes:[node('p1','primary_harness','turn-1'),node('group','job_group','turn-1'),node('input2','continuation','turn-2'),node('p2','primary_harness','turn-2')],edges:[edge('p1','group','submitted_by'),edge('group','input2','continued_by'),edge('input2','p2','submitted_by')]};
  const view=executionGraphView(graph,{turnId:'turn-2'});
  expect(view.nodes.map(value=>value.id).sort()).toEqual(['group','input2','p1','p2']);
  expect(view.edges).toHaveLength(3);
  const collapsed=executionGraphView(graph,{turnId:'turn-2',collapsedGroups:new Set(['group'])});
  expect(collapsed.nodes.map(value=>value.id)).toEqual(expect.arrayContaining(['input2','p2']));
 });

 it('collapses direct group members without changing the source graph',()=>{
  const graph={nodes:[node('primary','primary_harness','turn'),node('group','job_group','turn'),...Array.from({length:7},(_,index)=>node(`job-${index}`,'job','turn'))],edges:[edge('primary','group'),...Array.from({length:7},(_,index)=>edge('group',`job-${index}`))]};
  expect(defaultCollapsedGroups(graph,'turn')).toEqual(new Set(['group']));
  const view=executionGraphView(graph,{turnId:'turn',collapsedGroups:new Set(['group'])});
  expect(view.nodes.map(value=>value.id).sort()).toEqual(['group','primary']);
  expect(view.nodes.find(value=>value.id==='group').collapsedCount).toBe(7);
  expect(graph.nodes).toHaveLength(9);
 });
 it('keeps a cyclic server projection bounded instead of crashing',()=>{
  const graph={nodes:[node('primary','primary_harness','turn'),node('a','job','turn'),node('b','job','turn')],edges:[edge('primary','a'),edge('a','b'),edge('b','a')]};
  const view=executionGraphView(graph,{turnId:'turn'});
  expect(view.nodes).toHaveLength(3);expect(view.edges).toHaveLength(3);
  expect(view.width).toBeGreaterThanOrEqual(640);expect(view.height).toBeGreaterThanOrEqual(360);
 });
 it('wraps ranks into alternating viewport-width bands',()=>{
  const nodes=Array.from({length:7},(_,index)=>node(`n${index}`,index===0?'primary_harness':'job','turn'));
  const graph={nodes,edges:nodes.slice(1).map((value,index)=>edge(`n${index}`,value.id))};
  const view=executionGraphView(graph,{turnId:'turn',maxWidth:700}),byId=new Map(view.nodes.map(value=>[value.id,value]));
  expect(view.width).toBeLessThanOrEqual(700);
  expect(byId.get('n0').band).toBe(0);expect(byId.get('n1').band).toBe(0);
  expect(byId.get('n2').band).toBe(1);expect(byId.get('n2').x).toBeGreaterThan(byId.get('n3').x);
  expect(Math.max(...view.nodes.map(value=>value.x+value.width))).toBeLessThanOrEqual(view.width);
 });
});
