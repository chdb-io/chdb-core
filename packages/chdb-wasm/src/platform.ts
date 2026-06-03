// Runtime capability detection + bundle selection.
//
// chdb ships TWO bundles of the same engine, differing only in threading:
//   * mt  (chdb.mjs / chdb.wasm): pthreads (Web Workers + SharedArrayBuffer).
//         Faster, but the page MUST be cross-origin isolated (COOP/COEP).
//   * st  (st/chdb.mjs / st/chdb.wasm): single-threaded, no SharedArrayBuffer.
//         Runs on any page (no cross-origin isolation required), serial execution.
// Both require Memory64 + native wasm exceptions (hard requirements). selectBundle
// picks mt when the runtime is cross-origin isolated, otherwise falls back to st.

const isNode = typeof process !== 'undefined' && !!(process as any).versions?.node;

export interface PlatformFeatures {
  wasmBigInt: boolean;
  wasmMemory64: boolean;
  sharedArrayBuffer: boolean;
  crossOriginIsolated: boolean;
  /** Threads need shared memory + (in the browser) cross-origin isolation. */
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
  /** Whether the current runtime can run a chdb wasm bundle at all. */
  supported: boolean;
  /** Human-readable reasons no bundle can run (empty when supported). */
  reasons: string[];
  /** Which bundle was chosen: 'mt' (threaded) or 'st' (single-threaded). */
  variant: 'mt' | 'st';
  /** Convenience flag: true for the multi-threaded bundle. */
  threaded: boolean;
  /** URL/path of the Emscripten module to load. */
  moduleUrl: string;
  /** URL/path of the .wasm (browser progress fetch). */
  wasmUrl?: string;
  features: PlatformFeatures;
}

export interface SelectOptions {
  /** Base URL/dir holding chdb.mjs / chdb.wasm and the st/ subdir (e.g. a CDN or ./dist). */
  baseUrl: string;
  /**
   * Threading preference. 'auto' (default) picks mt when cross-origin isolated,
   * else st. 'mt'/'st' force a specific bundle (e.g. for testing).
   */
  threads?: 'auto' | 'mt' | 'st';
}

/**
 * Validate the runtime and resolve the chdb bundle URLs. Memory64 + WASM_BIGINT are
 * hard requirements for either bundle; the single-threaded (st) bundle additionally
 * lets chdb run on pages that are NOT cross-origin isolated.
 */
export function selectBundle(opts: SelectOptions): BundleConfig {
  const features = getPlatformFeatures();
  const base = opts.baseUrl.replace(/\/$/, '');

  // Hard requirements shared by both bundles.
  const reasons: string[] = [];
  if (!features.wasmBigInt) reasons.push('BigInt64Array unavailable (need WASM_BIGINT)');
  if (!features.wasmMemory64)
    reasons.push('Memory64 unsupported (need Node >= 23 / Chrome >= 133 / recent Firefox)');

  // Pick the bundle: prefer threads when available, else fall back to single-threaded.
  const pref = opts.threads ?? 'auto';
  const useThreads = pref === 'mt' || (pref === 'auto' && features.wasmThreads);
  const variant: 'mt' | 'st' = useThreads ? 'mt' : 'st';
  const prefix = variant === 'mt' ? base : `${base}/st`;

  return {
    supported: reasons.length === 0,
    reasons,
    variant,
    threaded: variant === 'mt',
    moduleUrl: `${prefix}/chdb.mjs`,
    wasmUrl: `${prefix}/chdb.wasm`,
    features,
  };
}
