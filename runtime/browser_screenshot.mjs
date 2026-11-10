#!/usr/bin/env node
import {mkdirSync} from 'node:fs';
import {createRequire} from 'node:module';
import net from 'node:net';
import path from 'node:path';
import process from 'node:process';

const require = createRequire(import.meta.url);
const {chromium} = require('playwright');

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

async function browserProxy() {
  const raw = process.env.HTTPS_PROXY || process.env.https_proxy || process.env.HTTP_PROXY || process.env.http_proxy;
  if (!raw) return {value: undefined, close: async () => {}};
  const parsed = new URL(raw);
  if (parsed.protocol !== 'http:' || !parsed.hostname) throw new Error('Invalid sandbox proxy URL');
  if (!parsed.username) return {value: {server: `${parsed.protocol}//${parsed.host}`}, close: async () => {}};
  const authorization = `Basic ${Buffer.from(`${decodeURIComponent(parsed.username)}:${decodeURIComponent(parsed.password)}`).toString('base64')}`;
  const server = net.createServer(client => {
    let buffered = Buffer.alloc(0);
    const fail = () => client.destroy();
    client.once('error', () => {});
    const receive = chunk => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length > 64 * 1024) return fail();
      const boundary = buffered.indexOf('\r\n\r\n');
      if (boundary < 0) return;
      client.off('data', receive);
      const lines = buffered.subarray(0, boundary).toString('latin1').split('\r\n');
      const headers = lines.slice(1).filter(line => !line.toLowerCase().startsWith('proxy-authorization:'));
      const head = Buffer.from([lines[0], ...headers, `Proxy-Authorization: ${authorization}`, '', ''].join('\r\n'), 'latin1');
      const upstream = net.connect({host: parsed.hostname, port: Number(parsed.port || 80)}, () => {
        upstream.write(head);
        upstream.write(buffered.subarray(boundary + 4));
        client.pipe(upstream); upstream.pipe(client);
      });
      upstream.once('error', fail);
    };
    client.on('data', receive);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  return {value: {server: `http://127.0.0.1:${address.port}`},
    close: () => new Promise(resolve => server.close(resolve))};
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
  proxyBridge = await browserProxy();
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
