#!/usr/bin/env node
import {mkdirSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import {existsSync} from 'node:fs';

const require = createRequire(import.meta.url);
const {chromium} = require('playwright');
const localBridge = path.join(path.dirname(fileURLToPath(import.meta.url)), 'proxy_bridge.cjs');
const {createProxyBridge} = require(existsSync('/usr/local/lib/sandbox-proxy-bridge.cjs')
  ? '/usr/local/lib/sandbox-proxy-bridge.cjs' : localBridge);

function usage() {
  console.error('usage: sandbox-browser-screenshot URL OUTPUT [--width 1440 --height 1000 --wait-ms 2000 --timeout-ms 30000]');
}

function integer(value, fallback, minimum, maximum) {
  const parsed = value == null ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`Invalid integer: ${value}`);
  return parsed;
}

function options(argv) {
  if (argv.length < 2) return null;
  const result = {url: argv[0], output: path.resolve(argv[1])};
  for (let index = 2; index < argv.length; index += 2) {
    if (!argv[index]?.startsWith('--') || argv[index + 1] == null) throw new Error(`Invalid argument: ${argv[index] || ''}`);
    result[argv[index].slice(2)] = argv[index + 1];
  }
  return result;
}

const value = options(process.argv.slice(2));
if (!value || ['--help', '-h'].includes(value.url)) {
  usage();
  process.exit(value ? 0 : 2);
}
if (!/^https?:\/\//i.test(value.url)) throw new Error('URL must use HTTP or HTTPS');

const width = integer(value.width, 1440, 16, 8192);
const height = integer(value.height, 1000, 16, 8192);
const waitMs = integer(value['wait-ms'], 2000, 0, 60000);
const timeoutMs = integer(value['timeout-ms'], 30000, 1000, 120000);
mkdirSync(path.dirname(value.output), {recursive: true});

let browser;
let proxyBridge;
try {
  proxyBridge = await createProxyBridge();
  browser = await chromium.launch({
    headless: true,
    proxy: proxyBridge.value,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu-sandbox', '--disable-crash-reporter', '--disable-crashpad'],
  });
  const page = await browser.newPage({viewport: {width, height}, deviceScaleFactor: 1});
  const response = await page.goto(value.url, {waitUntil: 'domcontentloaded', timeout: timeoutMs});
  if (!response) throw new Error('Navigation returned no HTTP response');
  if (response.status() >= 400) throw new Error(`Navigation returned HTTP ${response.status()}`);
  if (waitMs) await page.waitForTimeout(waitMs);
  await page.screenshot({path: value.output, fullPage: true, animations: 'disabled'});
  console.log(JSON.stringify({url: value.url, output: value.output, status: response.status(), width, height}));
} finally {
  await browser?.close();
  await proxyBridge?.close();
}
