// Headless-Chrome end-to-end test for the chdb-wasm package. Verifies BOTH browser
// contexts with a real Chromium (puppeteer):
//   * cross-origin isolated page (COOP/COEP) -> selectBundle picks the mt bundle
//   * non-isolated page (no COOP/COEP)        -> selectBundle picks the st bundle
// and runs core queries (SELECT / JOIN / CREATE+INSERT / heavy count) in each.
//
// Requires a built dist/ (npm run build) with both dist/chdb.wasm and dist/st/chdb.wasm.
//   node test/browser-run.mjs

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert';
import puppeteer from 'puppeteer';

const pkgDir = process.env.CHDB_PKG_DIR || dirname(dirname(fileURLToPath(import.meta.url)));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json' };

function startServer(port, isolate) {
  const server = createServer(async (req, res) => {
    // Cross-origin isolation (-> SharedArrayBuffer/threads) is toggled per server.
    if (isolate) {
      res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
      res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
      res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    }
    try {
      const p = decodeURIComponent((req.url || '/').split('?')[0]);
      const rel = normalize(p === '/' ? '/test/browser-fixture.html' : p).replace(/^(\.\.[/\\])+/, '');
      res.setHeader('Content-Type', MIME[extname(join(pkgDir, rel))] || 'application/octet-stream');
      res.end(await readFile(join(pkgDir, rel)));
    } catch { res.statusCode = 404; res.end('not found'); }
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve(server)));
}

async function runContext(label, isolate, port, expectVariant) {
  const server = await startServer(port, isolate);
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-gpu'] });
  try {
    const page = await browser.newPage();
    page.on('pageerror', (e) => console.error(`[${label}] pageerror: ${e.message}`));
    await page.goto(`http://localhost:${port}/test/browser-fixture.html`, { waitUntil: 'load', timeout: 30000 });
    const r = await page
      .waitForFunction('window.__RESULT__ !== undefined', { timeout: 120000, polling: 300 })
      .then(() => page.evaluate('window.__RESULT__'));
    console.log(`[${label}] ${JSON.stringify(r)}`);
    assert.ok(r.ok, `${label}: not ok (${r.fatal || (r.timeout && 'timeout') || '?'})`);
    assert.strictEqual(r.coi, isolate, `${label}: crossOriginIsolated should be ${isolate}`);
    assert.strictEqual(r.variant, expectVariant, `${label}: expected variant '${expectVariant}', got '${r.variant}'`);
    assert.strictEqual(r.select1, '1', `${label}: SELECT 1`);
    assert.strictEqual(r.join, '1000', `${label}: JOIN`);
    assert.strictEqual(r.insert, '100,4950', `${label}: INSERT+read`);
    assert.strictEqual(r.heavy, '50000000', `${label}: heavy count`);
    assert.strictEqual(r.file, '3,60', `${label}: file() local read`);
    // WORKERFS lazy mount: works on st (reads the mounted Blob); on mt it must refuse
    // cleanly (the Blob isn't visible to the query pthread) — not trap the module.
    if (expectVariant === 'st')
      assert.strictEqual(r.mount, '3,45', `${label}: mountFile (WORKERFS lazy) read (got ${r.mount})`);
    else
      assert.ok(/single-threaded/.test(r.mount), `${label}: mountFile should refuse on mt (got ${r.mount})`);
    assert.strictEqual(r.url_local, '4,100', `${label}: url() same-origin http read`);
    assert.strictEqual(r.url_public, '265', `${label}: url() public S3 read (got ${r.url_public})`);
    assert.strictEqual(r.s3_public, '265', `${label}: s3() public anonymous read (got ${r.s3_public})`);
    console.log(`[${label}] OK`);
  } finally {
    await browser.close();
    server.close();
  }
}

// CHDB_BROWSER_CONTEXTS=mt,st (default both). Lets local runs check one bundle
// while CI exercises both (mt on an isolated page, st on a non-isolated page).
const contexts = (process.env.CHDB_BROWSER_CONTEXTS ?? 'mt,st').split(',').map((s) => s.trim()).filter(Boolean);
if (contexts.includes('mt')) await runContext('isolated->mt', true, 8131, 'mt');
if (contexts.includes('st')) await runContext('non-isolated->st', false, 8132, 'st');
console.log(`\nbrowser tests passed (${contexts.join(', ')})`);
