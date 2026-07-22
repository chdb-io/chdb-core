// Runtime capability detection + bundle selection.
//
// chdb ships TWO bundles of the same engine, differing only in threading:
//   * mt  (chdb.mjs / chdb.wasm): pthreads (Web Workers + SharedArrayBuffer).
//         Faster, but the page MUST be cross-origin isolated (COOP/COEP).
//   * st  (st/chdb.mjs / st/chdb.wasm): single-threaded, no SharedArrayBuffer.
//         Runs on any page (no cross-origin isolation required), serial execution.
// Both require Memory64 + native wasm exceptions (hard requirements). selectBundle
// picks mt when the runtime is cross-origin isolated, otherwise falls back to st.
const isNode = typeof process !== 'undefined' && !!process.versions?.node;
function validateMemory64() {
    try {
        // A module declaring an i64-indexed (memory64) memory. Valid only where Memory64 is supported.
        return WebAssembly.validate(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0, 5, 3, 1, 4, 1]));
    }
    catch {
        return false;
    }
}
export function getPlatformFeatures() {
    const sab = typeof SharedArrayBuffer !== 'undefined';
    const coi = isNode || globalThis.crossOriginIsolated === true;
    return {
        wasmBigInt: typeof BigInt64Array !== 'undefined',
        wasmMemory64: validateMemory64(),
        sharedArrayBuffer: sab,
        crossOriginIsolated: coi,
        wasmThreads: sab && coi,
    };
}
/**
 * Validate the runtime and resolve the chdb bundle URLs. Memory64 + WASM_BIGINT are
 * hard requirements for either bundle; the single-threaded (st) bundle additionally
 * lets chdb run on pages that are NOT cross-origin isolated.
 */
export function selectBundle(opts) {
    const features = getPlatformFeatures();
    const base = opts.baseUrl.replace(/\/$/, '');
    // Hard requirements shared by both bundles.
    const reasons = [];
    if (!features.wasmBigInt)
        reasons.push('BigInt64Array unavailable (need WASM_BIGINT)');
    if (!features.wasmMemory64)
        reasons.push('Memory64 unsupported (need Node >= 23 / Chrome >= 133 / recent Firefox)');
    // Pick the bundle: only 'mt' when threads are actually available (SharedArrayBuffer +
    // cross-origin isolation). Forcing threads:'mt' where they can't run is unsupported —
    // the mt bundle would otherwise be selected and then fail at instantiation.
    const pref = opts.threads ?? 'auto';
    if (pref === 'mt' && !features.wasmThreads)
        reasons.push('mt bundle requires threads (SharedArrayBuffer + cross-origin isolation), unavailable here');
    const useThreads = features.wasmThreads && (pref === 'mt' || pref === 'auto');
    const variant = useThreads ? 'mt' : 'st';
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
//# sourceMappingURL=platform.js.map