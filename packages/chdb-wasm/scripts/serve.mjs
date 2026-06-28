// Tiny static server that sets the COOP/COEP headers required for
// SharedArrayBuffer (and therefore wasm threads) in the browser. Serves the
// package directory so examples/shell.html can load dist/ + the wasm.
//
//   node scripts/serve.mjs [port]            # http  on 0.0.0.0:<port>
//   SSL_CERT=cert.pem SSL_KEY=key.pem \
//     node scripts/serve.mjs [port]          # https on 0.0.0.0:<port>
//
// Build dist/ first:  npm run build  (tsc + copy-artifacts).
//
// IMPORTANT: SharedArrayBuffer (wasm threads) is only available in a SECURE
// CONTEXT (https:// or http://localhost). Plain http:// to a remote IP is NOT a
// secure context, so chdb will not run there even with COOP/COEP. For external
// access, use HTTPS (set SSL_CERT/SSL_KEY, a reverse proxy, or a tunnel), or
// reach it as localhost via an SSH port-forward.

import { createServer as createHttp } from 'node:http';
import { createServer as createHttps } from 'node:https';
import { readFile } from 'node:fs/promises';
import { readFileSync, existsSync } from 'node:fs';
import { networkInterfaces } from 'node:os';
import { extname, join, normalize, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const port = Number(process.argv[2] || 8099);
const host = process.env.HOST || '0.0.0.0';

const MIME = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
  '.map': 'application/json',
};

async function handle(req, res) {
  // Cross-origin isolation: required for SharedArrayBuffer / threads.
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
  try {
    const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
    // Strip leading separators and `..` segments so the path stays under root.
    const rel = normalize(urlPath === '/' ? '/examples/shell.html' : urlPath).replace(/^([/\\]|\.\.[/\\])+/, '');
    const file = join(root, rel);
    res.setHeader('Content-Type', MIME[extname(file)] || 'application/octet-stream');
    // Serve a precompressed sibling (file.br / file.gz) when the client accepts it.
    // wasm compresses ~3-4x, so this cuts the 95MB transfer to ~25-30MB.
    const accept = String(req.headers['accept-encoding'] || '');
    let served = file;
    if (accept.includes('br') && existsSync(file + '.br')) { served = file + '.br'; res.setHeader('Content-Encoding', 'br'); }
    else if (accept.includes('gzip') && existsSync(file + '.gz')) { served = file + '.gz'; res.setHeader('Content-Encoding', 'gzip'); }
    res.setHeader('Vary', 'Accept-Encoding');
    const body = await readFile(served);
    res.on('close', () => console.log(`${req.method} ${rel} -> 200 ${body.length}B (${served === file ? 'raw' : extname(served)}) ${res.writableFinished ? 'ok' : 'ABORTED'}`));
    res.end(body);
  } catch {
    res.statusCode = 404;
    console.log(`${req.method} ${(req.url || '').split('?')[0]} -> 404`);
    res.end('not found');
  }
}

const useHttps = !!(process.env.SSL_CERT && process.env.SSL_KEY);
const server = useHttps
  ? createHttps({ cert: readFileSync(process.env.SSL_CERT), key: readFileSync(process.env.SSL_KEY) }, handle)
  : createHttp(handle);

server.listen(port, host, () => {
  const scheme = useHttps ? 'https' : 'http';
  const lan = Object.values(networkInterfaces())
    .flat()
    .filter((i) => i && i.family === 'IPv4' && !i.internal)
    .map((i) => i.address);
  console.log(`chdb-wasm server (COOP/COEP) listening on ${host}:${port}`);
  console.log(`  local:    ${scheme}://localhost:${port}/`);
  for (const ip of lan) console.log(`  network:  ${scheme}://${ip}:${port}/`);
  if (!useHttps) {
    console.log('  NOTE: remote http:// is not a secure context -> SharedArrayBuffer/threads');
    console.log('        will be unavailable. Use HTTPS (SSL_CERT/SSL_KEY) or a tunnel for external access.');
  }
});
