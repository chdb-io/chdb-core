// Assemble the chdb-wasm-lite dist:
//   * the SDK (compiled JS + typings) is copied verbatim from the sibling
//     chdb-wasm package — same API, the bundle path is just a parameter there
//   * chdb.mjs + chdb.wasm come from the LITE split output (single-threaded,
//     64MB initial memory, split against profile-corpus-lite.sql, glue patched
//     with --lite so cold calls throw a clear "not in lite" error)
//   * NO chdb.deferred.wasm and NO dist/st — lite is one bundle, one file
//
//   node scripts/build-lite.mjs [liteSplitOutDir]
// default liteSplitOutDir: /tmp/chdb-split-lite
//
// Run `npm run build` in ../chdb-wasm first (this script wants its dist/).

import { copyFileSync, mkdirSync, readdirSync, existsSync, rmSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const sdkDist = join(pkgDir, '..', 'chdb-wasm', 'dist');
const liteOut = process.argv[2] || '/tmp/chdb-split-lite';
const distDir = join(pkgDir, 'dist');

if (!existsSync(join(sdkDist, 'index.js'))) {
  console.error(`SDK dist not found at ${sdkDist} — run 'npm run build' in packages/chdb-wasm first`);
  process.exit(1);
}
for (const f of ['chdb.mjs', 'chdb.wasm']) {
  if (!existsSync(join(liteOut, f))) {
    console.error(`${join(liteOut, f)} not found — run the lite split pipeline first (see tools/split/README.md)`);
    process.exit(1);
  }
}
// The lite glue must carry the --lite patch, not the lazy loader: shipping a
// lazy-loading glue without a deferred module would fail with ENOENT instead
// of a clear error.
if (!readFileSync(join(liteOut, 'chdb.mjs'), 'utf8').includes('chdb-wasm-lite: this SQL feature is not included')) {
  console.error(`${join(liteOut, 'chdb.mjs')} lacks the --lite glue patch — run split-wasm.mjs with --lite-glue`);
  process.exit(1);
}

rmSync(distDir, { recursive: true, force: true });
mkdirSync(distDir, { recursive: true });

// SDK files. Skipped: wasm artifacts (lite ships its own bundle), any subdir
// (st/ today), and source maps — their sources point at ../src/*.ts, which the
// lite package does not ship, so they would be dangling in the tarball.
for (const e of readdirSync(sdkDist, { withFileTypes: true })) {
  if (!e.isFile() || e.name.endsWith('.map')) continue;
  if (e.name === 'chdb.mjs' || e.name === 'chdb.wasm' || e.name === 'chdb.deferred.wasm') continue;
  copyFileSync(join(sdkDist, e.name), join(distDir, e.name));
}
copyFileSync(join(liteOut, 'chdb.mjs'), join(distDir, 'chdb.mjs'));
copyFileSync(join(liteOut, 'chdb.wasm'), join(distDir, 'chdb.wasm'));

const gz = gzipSync(readFileSync(join(distDir, 'chdb.wasm')), { level: 9 }).length;
console.log(`chdb-wasm-lite dist assembled: chdb.wasm ${(gz / 1048576).toFixed(2)} MiB gzipped`);
