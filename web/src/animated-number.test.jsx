import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {describe,expect,it} from 'vitest';

import {UnicodeCharacterCount,unicodeCharacterCount} from './animated-number';


describe('live character count',()=>{
 it('counts Unicode code points instead of UTF-16 code units',()=>{
  const html=renderToStaticMarkup(<UnicodeCharacterCount identity="message" text="你😀好"/>);
  expect(html).toContain('data-character-count="3"');
  expect(html).toContain('<strong>3</strong> 字符');
  expect(unicodeCharacterCount('前一段')+unicodeCharacterCount('后一段😀')).toBe(7);
 });
});
