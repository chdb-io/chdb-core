export interface PlatformFeatures {
    wasmBigInt: boolean;
    wasmMemory64: boolean;
    sharedArrayBuffer: boolean;
    crossOriginIsolated: boolean;
    /** Threads need shared memory + (in the browser) cross-origin isolation. */
    wasmThreads: boolean;
}
export declare function getPlatformFeatures(): PlatformFeatures;
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
     * Threading preference. 'auto' (default) picks mt when threads are available
     * (cross-origin isolated + SharedArrayBuffer), else st. 'st' forces single-threaded
     * (runs anywhere Memory64 is supported). 'mt' requires threads — it reports
     * `supported: false` if they're unavailable rather than selecting an unrunnable bundle.
     */
    threads?: 'auto' | 'mt' | 'st';
}
/**
 * Validate the runtime and resolve the chdb bundle URLs. Memory64 + WASM_BIGINT are
 * hard requirements for either bundle; the single-threaded (st) bundle additionally
 * lets chdb run on pages that are NOT cross-origin isolated.
 */
export declare function selectBundle(opts: SelectOptions): BundleConfig;
//# sourceMappingURL=platform.d.ts.map