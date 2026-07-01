// Headless-Chrome verification of live query progress + mid-query cancel for chdb-wasm.
// Both need the mt bundle on a cross-origin-isolated page (SharedArrayBuffer): the page
// polls a shared-memory struct the engine writes for progress, and sets a shared-memory flag
// to cancel. This serves test/progress-cancel-fixture.html with COOP/COEP and drives the
// cached Chrome via raw CDP (no puppeteer dependency). Run with Node >=22:
//   node test/progress-cancel-run.mjs
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { extname, join, normalize, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import assert from 'node:assert';

const pkgDir = process.env.CHDB_PKG_DIR || dirname(dirname(fileURLToPath(import.meta.url)));
const CHROME = process.env.CHROME_BIN ||
  '/home/ubuntu/.cache/puppeteer/chrome/linux-149.0.7827.22/chrome-linux64/chrome';
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json' };

function startServer(port) {
  const server = createServer(async (req, res) => {
    res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
    res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
    res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    try {
      const p = decodeURIComponent((req.url || '/').split('?')[0]);
      const rel = normalize(p === '/' ? '/test/progress-cancel-fixture.html' : p).replace(/^([/\\]|\.\.[/\\])+/, '');
      res.setHeader('Content-Type', MIME[extname(join(pkgDir, rel))] || 'application/octet-stream');
      res.end(await readFile(join(pkgDir, rel)));
    } catch { res.statusCode = 404; res.end('not found'); }
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve(server)));
}

// Minimal CDP client over the (Node-global) WebSocket: id-keyed request/response + event taps.
class CDP {
  constructor(ws) { this.ws = ws; this.id = 0; this.waiters = new Map(); this.listeners = []; ws.onmessage = (e) => this._recv(e.data); }
  static async connect(wsUrl) { const ws = new WebSocket(wsUrl); await new Promise((res, rej) => { ws.onopen = res; ws.onerror = (e) => rej(new Error('ws ' + (e.message || 'error'))); }); return new CDP(ws); }
  _recv(raw) {
    const msg = JSON.parse(raw);
    if (msg.id != null && this.waiters.has(msg.id)) { const w = this.waiters.get(msg.id); this.waiters.delete(msg.id); msg.error ? w.rej(new Error(msg.error.message)) : w.res(msg.result); }
    else if (msg.method) for (const l of this.listeners) l(msg);
  }
  send(method, params = {}, sessionId) { const id = ++this.id; return new Promise((res, rej) => { this.waiters.set(id, { res, rej }); this.ws.send(JSON.stringify({ id, method, params, sessionId })); }); }
  on(fn) { this.listeners.push(fn); }
}

async function fetchJSON(url, tries = 50) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return await r.json(); } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('CDP endpoint did not come up: ' + url);
}

async function main() {
  const port = 9824, cdpPort = 9223;
  // Fail fast (before opening the socket) if the Chrome binary is missing — otherwise spawn()
  // reports it as an opaque async ENOENT much later. Set CHROME_BIN to point at your browser.
  if (!existsSync(CHROME))
    throw new Error(`Chrome binary not found at ${CHROME}. Set CHROME_BIN to a Chrome/Chromium executable.`);
  const server = await startServer(port);
  const userDir = '/tmp/chdb-cdp-profile';
  let chrome, cdp, chromeStderr = '';
  try {
    chrome = spawn(CHROME, [
      '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      `--remote-debugging-port=${cdpPort}`, `--user-data-dir=${userDir}`,
      `http://127.0.0.1:${port}/test/progress-cancel-fixture.html`,
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    chrome.stderr.on('data', (d) => { chromeStderr += d.toString(); });

    const ver = await fetchJSON(`http://127.0.0.1:${cdpPort}/json/version`);
    cdp = await CDP.connect(ver.webSocketDebuggerUrl);
    // Find (or wait for) the page target, then attach in flatten mode.
    let target;
    for (let i = 0; i < 50 && !target; i++) {
      const { targetInfos } = await cdp.send('Target.getTargets');
      target = targetInfos.find((t) => t.type === 'page' && t.url.includes('progress-cancel-fixture'));
      if (!target) await new Promise((r) => setTimeout(r, 100));
    }
    assert.ok(target, 'page target not found');
    const { sessionId } = await cdp.send('Target.attachToTarget', { targetId: target.targetId, flatten: true });
    cdp.on((m) => {
      if (m.method === 'Runtime.consoleAPICalled') console.log('  [page]', m.params.args.map((a) => a.value ?? a.description ?? '').join(' '));
      if (m.method === 'Runtime.exceptionThrown') console.error('  [pageerror]', m.params.exceptionDetails?.exception?.description || m.params.exceptionDetails?.text);
    });
    await cdp.send('Runtime.enable', {}, sessionId);

    // Poll for window.__RESULT__ (the fixture sets it when done / on timeout).
    const deadline = Date.now() + 120000;
    let result;
    while (Date.now() < deadline) {
      const { result: r } = await cdp.send('Runtime.evaluate',
        { expression: 'window.__RESULT__ ? JSON.stringify(window.__RESULT__) : null', returnByValue: true }, sessionId);
      if (r && r.value) { result = JSON.parse(r.value); break; }
      await new Promise((res) => setTimeout(res, 300));
    }
    assert.ok(result, 'timed out waiting for window.__RESULT__');
    console.log('\n=== RESULT ===\n' + JSON.stringify(result, null, 2) + '\n');

    // Assertions.
    assert.ok(result.ok, 'fixture not ok: ' + (result.fatal || (result.timeout && 'timeout') || '?'));
    assert.strictEqual(result.coi, true, 'page must be cross-origin isolated');
    assert.strictEqual(result.variant, 'mt', 'expected mt bundle in COI page');
    // Live progress.
    assert.ok(result.progressEvents > 1, `expected multiple progress events, got ${result.progressEvents}`);
    assert.ok(result.progressMonotonic, 'progress readRows must be non-decreasing');
    assert.ok(result.progressHasTotal, 'progress must report totalRowsToRead > 0');
    assert.ok(result.progressLast && result.progressLast.readRows > 0, 'last progress readRows must be > 0');
    // Genuine streaming, not just a start+end pair: at least one tick strictly between 0 and total.
    assert.ok(result.progressHasIntermediate, 'progress must include an intermediate tick (0 < readRows < total)');
    // Cancel.
    assert.ok(result.cancelOk, 'cancel must reject with QUERY_WAS_CANCELLED, got: ' + result.cancelErr);
    assert.ok(result.cancelMs != null && result.cancelMs < 5000, `cancel should be prompt, got ${result.cancelMs}ms`);

    console.log('PASS: live progress (' + result.progressEvents + ' events, ' +
      result.progressElapsed.toFixed(1) + 's scan), cancel in ' + result.cancelMs + 'ms (' + result.cancelErr.split('.')[0] + ')');
  } catch (e) {
    if (chromeStderr) console.error('--- chrome stderr (tail) ---\n' + chromeStderr.split('\n').slice(-15).join('\n'));
    throw e;
  } finally {
    try { cdp?.ws.close(); } catch { /* ignore */ }
    chrome?.kill('SIGKILL');
    server.close();
  }
}

main().then(() => process.exit(0)).catch((e) => { console.error('FAIL:', e.message); process.exit(1); });
