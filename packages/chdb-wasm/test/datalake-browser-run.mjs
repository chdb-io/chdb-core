// Headless-Chrome end-to-end DataLakeCatalog test (the browser counterpart of
// datalake.test.mjs): moto serves local S3 behind a CORS proxy, pyiceberg writes
// a real Iceberg table into it, a mock Iceberg REST catalog serves the table, and
// a cross-origin-isolated page (mt bundle) creates ENGINE=DataLakeCatalog and
// reads the data — every HTTP request is a synchronous XHR on a wasm pthread.
//
// Requires a built dist/ (npm run build) and the pyiceberg venv (ICEBERG_PY,
// default /tmp/iceberg-venv); skips cleanly when the venv is absent.
//   node test/datalake-browser-run.mjs

import { createServer } from 'node:http';
import { spawn, execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert';
import puppeteer from 'puppeteer';
import { startMockCatalog } from './datalake/mock-rest-catalog.mjs';

const pkgDir = process.env.CHDB_PKG_DIR || dirname(dirname(fileURLToPath(import.meta.url)));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = join(pkgDir, 'test/datalake/make_iceberg_table.py');

const PAGE_PORT = 8141;
const S3_PORT = 8142; // moto itself (no CORS)
const S3_CORS_PORT = 8143; // CORS proxy in front of moto — what the page talks to
const CATALOG_PORT = 8144;
const BUCKET = 'lakebucket';

if (!existsSync(PY)) {
  console.log(`SKIP datalake-browser-run: no pyiceberg venv at ${PY} (set ICEBERG_PY)`);
  process.exit(0);
}

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json' };

// Cross-origin-isolated static server for the package (COOP/COEP -> mt bundle).
function startPageServer(port) {
  const server = createServer(async (req, res) => {
    res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
    res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
    res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    try {
      const p = decodeURIComponent((req.url || '/').split('?')[0]);
      const rel = normalize(p).replace(/^([/\\]|\.\.[/\\])+/, '');
      res.setHeader('Content-Type', MIME[extname(join(pkgDir, rel))] || 'application/octet-stream');
      res.end(await readFile(join(pkgDir, rel)));
    } catch { res.statusCode = 404; res.end('not found'); }
  });
  return listenOrReject(server, port);
}

// Reject on listen errors (e.g. EADDRINUSE from a leaked process) so the callers'
// try/finally cleanup runs instead of the process dying on an unhandled 'error'.
function listenOrReject(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.removeListener('error', reject);
      resolve(server);
    });
  });
}

// moto speaks no CORS; the page (COEP + cross-origin XHR with Authorization/Range
// headers) needs full CORS including preflight, so front it with a tiny proxy.
function startCorsProxy(port, upstream) {
  const server = createServer(async (req, res) => {
    const setCors = () => {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, PUT, POST, DELETE, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', req.headers['access-control-request-headers'] || '*');
      res.setHeader('Access-Control-Expose-Headers', '*');
      res.setHeader('Access-Control-Allow-Private-Network', 'true');
      res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    };
    if (req.method === 'OPTIONS') {
      setCors();
      res.statusCode = 204;
      res.end();
      return;
    }
    try {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const headers = { ...req.headers };
      delete headers.host;
      delete headers.connection;
      const upstreamRes = await fetch(`${upstream}${req.url}`, {
        method: req.method,
        headers,
        body: chunks.length ? Buffer.concat(chunks) : undefined,
        redirect: 'manual',
      });
      setCors();
      for (const [k, v] of upstreamRes.headers) {
        if (k === 'transfer-encoding' || k === 'content-encoding' || k === 'content-length') continue;
        res.setHeader(k, v);
      }
      const body = Buffer.from(await upstreamRes.arrayBuffer());
      res.statusCode = upstreamRes.status;
      // A HEAD response has no body; the upstream Content-Length is the object
      // size and must pass through (readers size their range requests off it).
      if (req.method === 'HEAD')
        res.setHeader('Content-Length', upstreamRes.headers.get('content-length') ?? '0');
      else
        res.setHeader('Content-Length', body.length);
      res.end(body);
    } catch (e) {
      // Log server-side (the page can't read a CORS-blocked body) and keep the
      // CORS headers so the browser reports 502 instead of an opaque status 0.
      console.error(`cors-proxy: ${req.method} ${req.url} -> ${e}`);
      setCors();
      res.statusCode = 502;
      res.end(String(e));
    }
  });
  return listenOrReject(server, port);
}

// --- 1. moto + table fixture ---
const moto = spawn(PY, ['-m', 'moto.server', '-p', String(S3_PORT)], { stdio: ['ignore', 'ignore', 'pipe'] });
// Drain stderr: werkzeug logs every S3 request there, and an unread 64KB pipe
// eventually blocks moto entirely; it's also the only diagnostics on failure.
let motoStderr = '';
moto.stderr.on('data', (d) => { motoStderr += d; });
const deadline = Date.now() + 30000;
for (;;) {
  if (moto.exitCode !== null) {
    console.error('moto exited early:\n' + motoStderr);
    process.exit(1);
  }
  try { await fetch(`http://127.0.0.1:${S3_PORT}`); break; }
  catch {
    if (Date.now() > deadline) {
      console.error('moto did not come up:\n' + motoStderr);
      moto.kill('SIGKILL');
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

let pageServer, corsProxy, mockCatalog, browser;
try {
  assert.ok((await fetch(`http://127.0.0.1:${S3_PORT}/${BUCKET}`, { method: 'PUT' })).ok, 'create bucket');
  const descriptor = JSON.parse(
    execFileSync(PY, [FIXTURE, `http://127.0.0.1:${S3_PORT}`, BUCKET], { encoding: 'utf8' }).trim());

  // --- 2. servers ---
  mockCatalog = await startMockCatalog({ port: CATALOG_PORT, descriptor });
  corsProxy = await startCorsProxy(S3_CORS_PORT, `http://127.0.0.1:${S3_PORT}`);
  pageServer = await startPageServer(PAGE_PORT);

  // --- 3. drive the page ---
  browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-gpu'] });
  const page = await browser.newPage();
  page.on('pageerror', (e) => console.error(`pageerror: ${e.message}`));
  page.on('console', (m) => console.error(`[page:${m.type()}] ${m.text()}`));
  await page.goto(
    `http://localhost:${PAGE_PORT}/test/datalake-browser-fixture.html?catalog=${CATALOG_PORT}&s3=${S3_CORS_PORT}`,
    { waitUntil: 'load', timeout: 30000 });
  const r = await page
    .waitForFunction('window.__RESULT__ !== undefined', { timeout: 180000, polling: 300 })
    .then(() => page.evaluate('window.__RESULT__'));
  console.log(JSON.stringify(r));

  assert.ok(r.ok, `fixture not ok: ${r.fatal || '?'}`);
  assert.strictEqual(r.coi, true, 'page must be cross-origin isolated');
  assert.strictEqual(r.variant, 'mt', 'DataLakeCatalog needs the threaded bundle');
  assert.strictEqual(
    r.tables.split('\n').sort().join(','),
    'lakehouse.city_events,lakehouse.deleted_events,lakehouse.events',
    `SHOW TABLES: ${r.tables}`);
  assert.ok(r.schema.includes('"id","Nullable(Int64)"'), `schema: ${r.schema}`);
  assert.strictEqual(
    r.rows,
    ['1,"alpha",1.5', '2,"beta",2.5', '3,"gamma",3.5', '4,"delta",4.5', '5,"epsilon",5.5', '6,"zeta",6.5', '7,"eta",7.5'].join('\n'),
    `rows: ${r.rows}`);
  assert.strictEqual(r.agg, '7\t28\t4.5', `agg: ${r.agg}`);
  console.log('PASS datalake-browser-run (DataLakeCatalog via sync XHR in headless Chrome)');
} finally {
  if (browser) await browser.close();
  if (pageServer) pageServer.close();
  if (corsProxy) corsProxy.close();
  if (mockCatalog) mockCatalog.close();
  moto.kill('SIGKILL');
}
