'use strict';

const net = require('node:net');

async function createProxyBridge() {
  const raw = process.env.HTTPS_PROXY || process.env.https_proxy || process.env.HTTP_PROXY || process.env.http_proxy;
  if (!raw) return {value: undefined, close: async () => {}};
  const parsed = new URL(raw);
  if (parsed.protocol !== 'http:' || !parsed.hostname) throw new Error('Invalid sandbox proxy URL');
  if (!parsed.username) return {value: {server: `${parsed.protocol}//${parsed.host}`}, close: async () => {}};
  const authorization = `Basic ${Buffer.from(`${decodeURIComponent(parsed.username)}:${decodeURIComponent(parsed.password)}`).toString('base64')}`;
  const sockets = new Set();
  const server = net.createServer(client => {
    sockets.add(client);
    let buffered = Buffer.alloc(0);
    const fail = () => client.destroy();
    client.once('error', () => {});
    client.once('close', () => sockets.delete(client));
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
      sockets.add(upstream);
      upstream.once('error', fail);
      upstream.once('close', () => sockets.delete(upstream));
    };
    client.on('data', receive);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  let closed = false;
  return {value: {server: `http://127.0.0.1:${address.port}`}, close: async () => {
    if (closed) return;
    closed = true;
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }};
}

module.exports = {createProxyBridge};
