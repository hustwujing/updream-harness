import {describe,expect,it} from 'vitest';
import {readFileSync} from 'node:fs';

import {visibleNetworkAccess} from './network-access';


describe('visible network access',()=>{
 it('uses the active Turn authority while work is running',()=>{
  expect(visibleNetworkAccess({network_access:'restricted'},{authenticated:true})).toBe('restricted');
 });
 it('uses the next-Turn account authority while the Thread is idle',()=>{
  expect(visibleNetworkAccess(null,{authenticated:true})).toBe('public');
 expect(visibleNetworkAccess(null,{authenticated:false})).toBe('restricted');
 });
 it('drives the application badge from active work instead of the last completed Turn',()=>{
  expect(readFileSync(new URL('./main.jsx',import.meta.url),'utf8')).toContain(
   'visibleNetworkAccess(activeTurn,session)');
 });
});
