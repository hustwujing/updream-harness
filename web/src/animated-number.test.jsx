import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {describe,expect,it} from 'vitest';

import {UnicodeCharacterCount} from './animated-number';


describe('live character count',()=>{
 it('counts Unicode code points instead of UTF-16 code units',()=>{
  const html=renderToStaticMarkup(<UnicodeCharacterCount identity="message" text="你😀好"/>);
  expect(html).toContain('data-character-count="3"');
  expect(html).toContain('3 字符');
 });
});
