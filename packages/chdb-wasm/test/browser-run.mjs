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
    // registerFile + file('<name>') lazy read works on BOTH bundles: the read is
    // proxied (MAIN_THREAD_EM_ASM) to the runtime thread that holds the Blob.
    assert.strictEqual(r.regfile, '3,45', `${label}: registerFile lazy read (got ${r.regfile})`);
    // Parquet read through the lazy JS reader (footer + row-group seeks), with auto
    // format inference from the registered name's .parquet extension.
    assert.strictEqual(r.parquet, '10,45', `${label}: registerFile Parquet read (got ${r.parquet})`);
    assert.strictEqual(r.regfile_u8, '3,45', `${label}: registerFile Uint8Array input (got ${r.regfile_u8})`);
    assert.strictEqual(r.regfile_infer, '3,45', `${label}: file() auto format inference (got ${r.regfile_infer})`);
    assert.strictEqual(r.parquet_explicit, '10,45', `${label}: registerFile Parquet explicit format (got ${r.parquet_explicit})`);
    // multi-row-group seek: aggregate over all groups + a point lookup with exact values
    assert.strictEqual(r.parquet_seek, '20000,199990000|12345', `${label}: multi-row-group Parquet seek (got ${r.parquet_seek})`);
    // two registered files joined, then r1 re-registered (overwrite must take effect)
    assert.strictEqual(r.regfile_multi, '4,100|1,100', `${label}: multiple/overwrite registered files (got ${r.regfile_multi})`);
    assert.strictEqual(r.regfile_missing, 'ERR', `${label}: unregistered file() must reject (got ${r.regfile_missing})`);
    // unregisterFile makes a registered file unqueryable; clearFiles drops all (idempotent unregister)
    assert.strictEqual(r.unregister, '2,15|gone|gone', `${label}: unregisterFile/clearFiles (got ${r.unregister})`);
    assert.strictEqual(r.url_local, '4,100', `${label}: url() same-origin http read`);
    assert.strictEqual(r.url_public, '265', `${label}: url() public S3 read (got ${r.url_public})`);
    assert.strictEqual(r.s3_public, '265', `${label}: s3() public anonymous read (got ${r.s3_public})`);
    assert.ok(typeof r.url_404 === 'string' && r.url_404.startsWith('ERR'), `${label}: url() 404 must reject (got ${r.url_404})`);
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
