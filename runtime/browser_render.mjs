#!/usr/bin/env node
import {spawnSync} from 'node:child_process';
import {mkdirSync, rmSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import path from 'node:path';
import process from 'node:process';

const require = createRequire(import.meta.url);
const {chromium} = require('playwright');

function argumentsFrom(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    if (!key?.startsWith('--') || argv[index + 1] == null) throw new Error(`Invalid argument: ${key || ''}`);
    result[key.slice(2)] = argv[index + 1];
  }
  return result;
}

function integer(value, fallback, minimum, maximum) {
  const parsed = value == null ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`Invalid integer: ${value}`);
  return parsed;
}

const options = argumentsFrom(process.argv.slice(2));
if (!options.html || !options.output) {
  console.error('usage: sandbox-browser-render --html FILE --output FILE [--width 1280 --height 720 --frames 1 --fps 30 --video FILE --diagnostics FILE]');
  process.exit(2);
}

const html = path.resolve(options.html);
const output = path.resolve(options.output);
const width = integer(options.width, 1280, 16, 8192);
const height = integer(options.height, 720, 16, 8192);
const frames = integer(options.frames, 1, 1, 18000);
const fps = integer(options.fps, 30, 1, 120);
const diagnosticsPath = path.resolve(options.diagnostics || output + '.json');
const frameDirectory = path.resolve(options['frame-dir'] || output + '.frames');
const browserHome = path.join('/tmp', `sandbox-browser-${process.pid}`);
const consoleMessages = [];
const pageErrors = [];
let browser;

try {
  mkdirSync(path.dirname(output), {recursive: true});
  mkdirSync(browserHome, {recursive: true});
  browser = await chromium.launch({
    executablePath: process.env.CHROME_PATH || '/usr/bin/chromium',
    headless: true,
    env: {...process.env, HOME: browserHome, XDG_CACHE_HOME: path.join(browserHome, 'cache'), XDG_CONFIG_HOME: path.join(browserHome, 'config')},
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu-sandbox', '--disable-crash-reporter', '--disable-crashpad', '--enable-webgl',
      '--ignore-gpu-blocklist', '--use-gl=angle', '--use-angle=swiftshader', '--hide-scrollbars'],
  });
  const page = await browser.newPage({viewport: {width, height}, deviceScaleFactor: 1});
  page.on('console', message => consoleMessages.push({type: message.type(), text: message.text()}));
  page.on('pageerror', error => pageErrors.push(error.message));
  await page.goto(pathToFileURL(html).href, {waitUntil: 'load'});
  await page.evaluate(async () => { if (document.fonts?.ready) await document.fonts.ready; });
  await page.waitForFunction(() => globalThis.__RENDER_READY__ !== false, null, {timeout: 30000});
  const graphics = await page.evaluate(() => {
    const canvas = document.querySelector('canvas');
    const gl = canvas && (canvas.getContext('webgl2') || canvas.getContext('webgl'));
    const extension = gl?.getExtension('WEBGL_debug_renderer_info');
    return {canvas: Boolean(canvas), webgl: Boolean(gl), renderer: extension ? gl.getParameter(extension.UNMASKED_RENDERER_WEBGL) : '',
      animation_supported: typeof globalThis.renderFrame === 'function'};
  });
  if (frames === 1) {
    await page.evaluate(() => globalThis.renderFrame?.(0));
    await page.screenshot({path: output, animations: 'disabled'});
  } else {
    if (!graphics.animation_supported) throw new Error('Multi-frame rendering requires window.renderFrame(timeSeconds)');
    rmSync(frameDirectory, {recursive: true, force: true});
    mkdirSync(frameDirectory, {recursive: true});
    for (let frame = 0; frame < frames; frame++) {
      await page.evaluate(time => globalThis.renderFrame?.(time), frame / fps);
      await page.screenshot({path: path.join(frameDirectory, `frame-${String(frame).padStart(6, '0')}.png`), animations: 'disabled'});
    }
    await page.screenshot({path: output, animations: 'disabled'});
    if (options.video) {
      mkdirSync(path.dirname(path.resolve(options.video)), {recursive: true});
      const ffmpeg = spawnSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', '-framerate', String(fps),
        '-i', path.join(frameDirectory, 'frame-%06d.png'), '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', path.resolve(options.video)],
        {encoding: 'utf8'});
      if (ffmpeg.status !== 0) throw new Error(`ffmpeg failed: ${ffmpeg.stderr}`);
    }
  }
  const diagnostics = {html, output, width, height, frames, fps, graphics, console: consoleMessages, page_errors: pageErrors};
  writeFileSync(diagnosticsPath, JSON.stringify(diagnostics, null, 2) + '\n');
  if (pageErrors.length) throw new Error(`Page errors: ${pageErrors.join('; ')}`);
  console.log(JSON.stringify(diagnostics));
} finally {
  await browser?.close();
  rmSync(browserHome, {recursive: true, force: true});
}
