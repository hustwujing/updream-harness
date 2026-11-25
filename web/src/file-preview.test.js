import {describe,it,expect} from 'vitest';
import {previewKind,previewable} from './file-preview';

describe('layered image artifacts',()=>{
 it.each([
  ['layers.ora','image/openraster'],
  ['layers.psd','image/vnd.adobe.photoshop'],
  ['layers.tiff','image/tiff'],
 ])('keeps %s downloadable without an unsupported browser image preview',(name,mime)=>{
  expect(previewKind({name,mime})).toBe('');
  expect(previewable({name,mime})).toBe(false);
 });
 it('keeps the accompanying PNG preview available',()=>{
  expect(previewKind({name:'composite.png',mime:'image/png'})).toBe('image');
  expect(previewable({name:'composite.png',mime:'image/png'})).toBe(true);
 });
});
