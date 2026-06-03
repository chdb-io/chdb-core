// Copy the Emscripten artifacts (chdb.mjs, chdb.wasm) produced by the CMake
// build into the package's dist/ so the published package is self-contained.
//
//   node scripts/copy-artifacts.mjs [buildDir]
// defaults buildDir to ../../buildwasm/programs/wasm relative to the package.

import { copyFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const buildDir =
  process.argv[2] || join(pkgDir, '..', '..', 'buildwasm', 'programs', 'wasm');
const distDir = join(pkgDir, 'dist');

mkdirSync(distDir, { recursive: true });
for (const f of ['chdb.mjs', 'chdb.wasm']) {
  const src = join(buildDir, f);
  if (!existsSync(src)) {
    console.error(`missing artifact: ${src} (build the chdb_wasm CMake target first)`);
    process.exit(1);
  }
  copyFileSync(src, join(distDir, f));
  console.log(`copied ${f} -> dist/`);
}
