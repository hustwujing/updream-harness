#!/usr/bin/env node
'use strict';

const {spawn} = require('node:child_process');
const {createProxyBridge} = require('/usr/local/lib/sandbox-proxy-bridge.cjs');

(async () => {
  const bridge = await createProxyBridge();
  const args = process.argv.slice(2);
  if (bridge.value && !args.some(value => value.startsWith('--proxy-server='))) {
    args.unshift(`--proxy-server=${bridge.value.server}`);
  }
  const child = spawn('/usr/bin/chromium', args, {stdio: 'inherit', env: process.env});
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
    process.on(signal, () => child.kill(signal));
  }
  const [code, signal] = await new Promise(resolve => child.once('exit', (...value) => resolve(value)));
  await bridge.close();
  if (signal) process.kill(process.pid, signal);
  process.exitCode = code == null ? 1 : code;
})().catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
