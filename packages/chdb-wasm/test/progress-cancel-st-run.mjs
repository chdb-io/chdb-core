// Headless-Chrome verification of the SINGLE-THREADED (st) bundle's progress/cancel
// behaviour: serves the fixture WITHOUT COOP/COEP, so the page is NOT cross-origin isolated
// and selectBundle picks st. Asserts the documented graceful degradation — no live progress,
// peak 0, cancel() throws — while ordinary queries still run and report scannedRows.
// Drives the cached Chrome via raw CDP (no puppeteer). Run with Node >=22:
//   node test/progress-cancel-st-run.mjs
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import assert from 'node:assert';

const pkgDir = process.env.CHDB_PKG_DIR || dirname(dirname(fileURLToPath(import.meta.url)));
const CHROME = process.env.CHROME_BIN ||
  '/home/ubuntu/.cache/puppeteer/chrome/linux-149.0.7827.22/chrome-linux64/chrome';
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json' };
const FIXTURE = '/test/progress-cancel-st-fixture.html';

// NB: intentionally NO Cross-Origin-Opener/Embedder-Policy headers -> not isolated -> st.
function startServer(port) {
  const server = createServer(async (req, res) => {
    try {
      const p = decodeURIComponent((req.url || '/').split('?')[0]);
      const rel = normalize(p === '/' ? FIXTURE : p).replace(/^([/\\]|\.\.[/\\])+/, '');
      res.setHeader('Content-Type', MIME[extname(join(pkgDir, rel))] || 'application/octet-stream');
      res.end(await readFile(join(pkgDir, rel)));
    } catch { res.statusCode = 404; res.end('not found'); }
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve(server)));
}

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
  const port = 9825, cdpPort = 9224;
  const server = await startServer(port);
  const chrome = spawn(CHROME, [
    '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
    `--remote-debugging-port=${cdpPort}`, '--user-data-dir=/tmp/chdb-cdp-profile-st',
    `http://127.0.0.1:${port}${FIXTURE}`,
  ], { stdio: ['ignore', 'ignore', 'pipe'] });
  let chromeStderr = '';
  chrome.stderr.on('data', (d) => { chromeStderr += d.toString(); });

  let cdp;
  try {
    const ver = await fetchJSON(`http://127.0.0.1:${cdpPort}/json/version`);
    cdp = await CDP.connect(ver.webSocketDebuggerUrl);
    let target;
    for (let i = 0; i < 50 && !target; i++) {
      const { targetInfos } = await cdp.send('Target.getTargets');
      target = targetInfos.find((t) => t.type === 'page' && t.url.includes('progress-cancel-st-fixture'));
      if (!target) await new Promise((r) => setTimeout(r, 100));
    }
    assert.ok(target, 'page target not found');
    const { sessionId } = await cdp.send('Target.attachToTarget', { targetId: target.targetId, flatten: true });
    cdp.on((m) => {
      if (m.method === 'Runtime.consoleAPICalled') console.log('  [page]', m.params.args.map((a) => a.value ?? a.description ?? '').join(' '));
      if (m.method === 'Runtime.exceptionThrown') console.error('  [pageerror]', m.params.exceptionDetails?.exception?.description || m.params.exceptionDetails?.text);
    });
    await cdp.send('Runtime.enable', {}, sessionId);

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

    assert.ok(result.ok, 'fixture not ok: ' + (result.fatal || (result.timeout && 'timeout') || '?'));
    assert.strictEqual(result.coi, false, 'page must NOT be cross-origin isolated');
    assert.strictEqual(result.variant, 'st', 'expected st bundle in a non-isolated page');
    // No live progress on st, and peak 0 — even though the query ran for seconds.
    assert.strictEqual(result.progressEvents, 0, `st must emit no progress events, got ${result.progressEvents}`);
    assert.strictEqual(result.peak, 0, `st peakMemoryUsage must be 0, got ${result.peak}`);
    // The query still ran correctly: scanned all source rows + a plausible count.
    assert.strictEqual(result.scannedRows, 200000000, `st query must still scan all rows, got ${result.scannedRows}`);
    assert.ok(result.count > 0, `st query must return a count, got ${result.count}`);
    // cancel() is mt-only -> must throw on st, with a guiding message.
    assert.ok(result.cancelThrew, 'st cancel() must throw');
    assert.ok(/multi-threaded|mt/i.test(result.cancelErr || ''), 'cancel error should mention the mt requirement, got: ' + result.cancelErr);

    console.log(`PASS (st): no live progress (events=0, peak=0) over a ${result.elapsed.toFixed(1)}s query that scanned ` +
      `${result.scannedRows} rows (count=${result.count}); cancel() threw -> "${(result.cancelErr || '').split('(')[0].trim()}"`);
  } catch (e) {
    if (chromeStderr) console.error('--- chrome stderr (tail) ---\n' + chromeStderr.split('\n').slice(-15).join('\n'));
    throw e;
  } finally {
    try { cdp?.ws.close(); } catch { /* ignore */ }
    chrome.kill('SIGKILL');
    server.close();
  }
}

main().then(() => process.exit(0)).catch((e) => { console.error('FAIL:', e.message); process.exit(1); });
