// Copy the Emscripten artifacts (chdb.mjs, chdb.wasm) produced by the CMake
// build into the package's dist/ so the published package is self-contained.
//
//   node scripts/copy-artifacts.mjs [mtBuildDir] [stBuildDir]
// defaults:
//   mtBuildDir = ../../buildwasm/programs/wasm    (threaded, WASM_THREADS=ON)  -> dist/
//   stBuildDir = ../../buildwasm-st/programs/wasm (single-threaded, OFF)       -> dist/st/
// The single-threaded bundle is optional: if its build dir is absent it is skipped.

import { copyFileSync, mkdirSync, existsSync, rmSync } from 'node:fs';
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
  // A dir containing chdb.wasm.orig is a -DWASM_SPLIT_MODULE=ON CMake build
  // tree, where chdb.wasm is the profiling-INSTRUMENTED module — never ship
  // that. Run tools/split/split-wasm.mjs and pass its --out dir here instead
  // (it holds the split primary + chdb.deferred.wasm + patched glue).
  if (existsSync(join(buildDir, 'chdb.wasm.orig'))) {
    console.error(`${buildDir} is a WASM_SPLIT_MODULE build tree (chdb.wasm there is instrumented);`);
    console.error('run tools/split/split-wasm.mjs and point copy-artifacts at its --out dir.');
    process.exit(1);
  }
  const files = ['chdb.mjs', 'chdb.wasm'];
  if (existsSync(join(buildDir, 'chdb.deferred.wasm'))) {
    files.push('chdb.deferred.wasm');
  } else if (existsSync(join(destDir, 'chdb.deferred.wasm'))) {
    // Don't ship a stale deferred module left over from a previous split run —
    // it would no longer correspond to the chdb.wasm being copied.
    rmSync(join(destDir, 'chdb.deferred.wasm'));
    console.log(`removed stale chdb.deferred.wasm from ${destDir.replace(pkgDir + '/', '')}/`);
  }
  for (const f of files) {
    copyFileSync(join(buildDir, f), join(destDir, f));
    console.log(`copied ${f} -> ${destDir.replace(pkgDir + '/', '')}/`);
  }
}

// Threaded bundle is the default/primary; single-threaded is optional.
copyBundle(mtBuildDir, distDir, { required: true });
copyBundle(stBuildDir, join(distDir, 'st'), { required: false });
