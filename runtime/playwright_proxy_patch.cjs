'use strict';

const Module = require('node:module');
const {createProxyBridge} = require('/usr/local/lib/sandbox-proxy-bridge.cjs');
const originalLoad = Module._load;
const patched = new WeakSet();

function patchBrowserType(browserType) {
  if (!browserType || patched.has(browserType)) return;
  patched.add(browserType);
  const launch = browserType.launch.bind(browserType);
  Object.defineProperty(browserType, 'launch', {configurable: true, value: async options => {
    if (options?.proxy || !(process.env.HTTPS_PROXY || process.env.HTTP_PROXY || process.env.https_proxy || process.env.http_proxy)) {
      return launch(options);
    }
    const bridge = await createProxyBridge();
    try {
      const browser = await launch({...options, proxy: bridge.value});
      browser.once('disconnected', () => { bridge.close().catch(() => {}); });
      return browser;
    } catch (error) {
      await bridge.close();
      throw error;
    }
  }});
}

Module._load = function(request, parent, isMain) {
  const exported = originalLoad.call(this, request, parent, isMain);
  if (request === 'playwright') patchBrowserType(exported?.chromium);
  return exported;
};
