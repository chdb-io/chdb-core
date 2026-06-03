// Runtime capability detection + bundle selection.
//
// NOTE on variants: duckdb-wasm ships mvp/eh/coi bundles because its engine can
// run single-threaded. ClickHouse cannot (it hard-requires threads), so chdb has
// ONE feature level: Memory64 + native wasm exceptions + threads. The only useful
// variant axis is size(-Oz) vs speed(-O3) of the same bundle (see WASM_MIN_SIZE).
// selectBundle therefore returns one bundle and reports whether the runtime can
// run it at all.

const isNode = typeof process !== 'undefined' && !!(process as any).versions?.node;

export interface PlatformFeatures {
  wasmBigInt: boolean;
  wasmMemory64: boolean;
  sharedArrayBuffer: boolean;
  crossOriginIsolated: boolean;
  /** chdb's wasm needs threads: shared memory + (in the browser) cross-origin isolation. */
  wasmThreads: boolean;
}

function validateMemory64(): boolean {
  try {
    // A module declaring an i64-indexed (memory64) memory. Valid only where Memory64 is supported.
    return WebAssembly.validate(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0, 5, 3, 1, 4, 1]));
  } catch {
    return false;
  }
}

export function getPlatformFeatures(): PlatformFeatures {
  const sab = typeof SharedArrayBuffer !== 'undefined';
  const coi = isNode || (globalThis as any).crossOriginIsolated === true;
  return {
    wasmBigInt: typeof BigInt64Array !== 'undefined',
    wasmMemory64: validateMemory64(),
    sharedArrayBuffer: sab,
    crossOriginIsolated: coi,
    wasmThreads: sab && coi,
  };
}

export interface BundleConfig {
  /** Whether the current runtime can run the chdb wasm bundle. */
  supported: boolean;
  /** Human-readable reasons the bundle is unsupported (empty when supported). */
  reasons: string[];
  /** URL/path of the Emscripten module to load. */
  moduleUrl: string;
  /** URL/path of the .wasm (browser progress fetch). */
  wasmUrl?: string;
  features: PlatformFeatures;
}

export interface SelectOptions {
  /** Base URL/dir holding chdb.mjs / chdb.wasm (e.g. a CDN or ./dist). */
  baseUrl: string;
}

/**
 * Validate the runtime and resolve the chdb bundle URLs. chdb ships a single
 * wasm bundle (Memory64 + native exceptions + threads); there is no feature or
 * size/speed variant to choose. This only reports whether the runtime can run it.
 */
export function selectBundle(opts: SelectOptions): BundleConfig {
  const features = getPlatformFeatures();
  const reasons: string[] = [];
  if (!features.wasmBigInt) reasons.push('BigInt64Array unavailable (need WASM_BIGINT)');
  if (!features.wasmMemory64) reasons.push('Memory64 unsupported (need Node >= 23 / Chrome >= 133 / recent Firefox)');
  if (!features.sharedArrayBuffer) reasons.push('SharedArrayBuffer unavailable (threads require it)');
  if (!features.crossOriginIsolated) reasons.push('not cross-origin isolated (set COOP/COEP headers for threads)');

  const base = opts.baseUrl.replace(/\/$/, '');
  return {
    supported: reasons.length === 0,
    reasons,
    moduleUrl: `${base}/chdb.mjs`,
    wasmUrl: `${base}/chdb.wasm`,
    features,
  };
}
