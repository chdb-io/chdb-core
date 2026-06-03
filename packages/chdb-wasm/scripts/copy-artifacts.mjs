// Copy the Emscripten artifacts (chdb.mjs, chdb.wasm) produced by the CMake
// build into the package's dist/ so the published package is self-contained.
//
//   node scripts/copy-artifacts.mjs [mtBuildDir] [stBuildDir]
// defaults:
//   mtBuildDir = ../../buildwasm/programs/wasm    (threaded, WASM_THREADS=ON)  -> dist/
//   stBuildDir = ../../buildwasm-st/programs/wasm (single-threaded, OFF)       -> dist/st/
// The single-threaded bundle is optional: if its build dir is absent it is skipped.

import { copyFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const mtBuildDir = process.argv[2] || join(pkgDir, '..', '..', 'buildwasm', 'programs', 'wasm');
const stBuildDir = process.argv[3] || join(pkgDir, '..', '..', 'buildwasm-st', 'programs', 'wasm');
const distDir = join(pkgDir, 'dist');

function copyBundle(buildDir, destDir, { required }) {
  const present = ['chdb.mjs', 'chdb.wasm'].every((f) => existsSync(join(buildDir, f)));
  if (!present) {
    if (required) {
      console.error(`missing artifacts in ${buildDir} (build the chdb_wasm CMake target first)`);
      process.exit(1);
    }
    console.warn(`skipping ${destDir}: no artifacts in ${buildDir}`);
    return;
  }
  mkdirSync(destDir, { recursive: true });
  for (const f of ['chdb.mjs', 'chdb.wasm']) {
    copyFileSync(join(buildDir, f), join(destDir, f));
    console.log(`copied ${f} -> ${destDir.replace(pkgDir + '/', '')}/`);
  }
}

// Threaded bundle is the default/primary; single-threaded is optional.
copyBundle(mtBuildDir, distDir, { required: true });
copyBundle(stBuildDir, join(distDir, 'st'), { required: false });
