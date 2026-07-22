// Worker-side dispatcher. The Emscripten chdb module runs HERE, in the worker,
// so the caller's (main) thread is never blocked by a query. Works in both Node
// (worker_threads) and the browser (dedicated Worker).
//
// Protocol: receive WorkerRequest, run it synchronously against ChdbBindings,
// post back a WorkerResponse. The wasm module's own pthread pool is spawned from
// this worker, not the main thread.
var __rewriteRelativeImportExtension = (this && this.__rewriteRelativeImportExtension) || function (path, preserveJsx) {
    if (typeof path === "string" && /^\.\.?\//.test(path)) {
        return path.replace(/\.(tsx)$|((?:\.d)?)((?:\.[^./]+?)?)\.([cm]?)ts$/i, function (m, tsx, d, ext, cm) {
            return tsx ? preserveJsx ? ".jsx" : ".js" : d && (!ext || !cm) ? m : (d + ext + "." + cm.toLowerCase() + "js");
        });
    }
    return path;
};
import { ChdbBindings } from "./bindings.js";
const isNode = typeof process !== 'undefined' && !!process.versions?.node;
let post;
let listen;
if (isNode) {
    const { parentPort } = await import('node:worker_threads');
    post = (msg, transfer) => parentPort.postMessage(msg, transfer);
    listen = (cb) => parentPort.on('message', cb);
}
else {
    const g = self;
    post = (msg, transfer) => g.postMessage(msg, transfer || []);
    listen = (cb) => {
        g.onmessage = (e) => cb(e.data);
    };
}
let bindings = null;
/** Fetch a .wasm with byte progress (browser). Returns the bytes or null. */
async function fetchWithProgress(url, id) {
    if (typeof fetch === 'undefined')
        return null;
    const resp = await fetch(url);
    // Fail fast on non-2xx: otherwise a 404/500 error page would be fed to Emscripten as
    // wasmBinary, producing a confusing instantiation failure instead of a clear error.
    if (!resp.ok)
        throw new Error(`failed to fetch wasm module ${url}: HTTP ${resp.status} ${resp.statusText}`);
    // OK but no streaming body (e.g. some runtimes): fall back to a single arrayBuffer read.
    if (!resp.body)
        return new Uint8Array(await resp.arrayBuffer());
    const total = Number(resp.headers.get('content-length') || 0);
    const reader = resp.body.getReader();
    const chunks = [];
    let loaded = 0;
    for (;;) {
        const { done, value } = await reader.read();
        if (done)
            break;
        chunks.push(value);
        loaded += value.length;
        post({ id, event: 'progress', loaded, total });
    }
    const out = new Uint8Array(loaded);
    let off = 0;
    for (const c of chunks) {
        out.set(c, off);
        off += c.length;
    }
    return out;
}
async function init(payload, id) {
    let url = payload.moduleUrl;
    if (isNode && !/^[a-z]+:/i.test(url)) {
        const { pathToFileURL } = await import('node:url');
        url = pathToFileURL(url).href;
    }
    const factory = (await import(__rewriteRelativeImportExtension(url))).default;
    const moduleOpts = {};
    // Browser: pre-fetch the .wasm with progress and feed it to Emscripten.
    if (!isNode && payload.wasmUrl) {
        const bytes = await fetchWithProgress(payload.wasmUrl, id);
        if (bytes)
            moduleOpts.wasmBinary = bytes;
    }
    const mod = await factory(moduleOpts);
    bindings = new ChdbBindings(mod);
}
function requireBindings() {
    if (!bindings)
        throw new Error('chdb worker not initialized');
    return bindings;
}
listen((req) => {
    void (async () => {
        try {
            let result;
            switch (req.type) {
                case 'init':
                    await init(req.payload, req.id);
                    // mt only: share the wasm Memory SAB + the cancel-flag and live-progress offsets.
                    // The page sets the cancel flag (read by the C++ cancel check on any thread) and
                    // polls the progress struct (written by the engine on any thread). A non-shared
                    // heap (st build) => no cancel / no live progress.
                    {
                        const b = requireBindings();
                        const sharedMem = b.heapBuffer;
                        if (typeof SharedArrayBuffer !== 'undefined' && sharedMem instanceof SharedArrayBuffer) {
                            result = { sharedMem, cancelAddr: b.cancelFlagAddr(), progressAddr: b.progressAddr() };
                        }
                    }
                    break;
                case 'query':
                    result = requireBindings().query(req.payload.sql, req.payload.format);
                    break;
                case 'connect':
                    result = { conn: requireBindings().connect(req.payload?.path) };
                    break;
                case 'closeConn':
                    requireBindings().closeConn(req.payload.conn);
                    break;
                case 'queryConn':
                    result = requireBindings().queryConn(req.payload.conn, req.payload.sql, req.payload.format);
                    break;
                case 'streamStart':
                    result = { stream: requireBindings().streamStart(req.payload.conn, req.payload.sql, req.payload.format) };
                    break;
                case 'streamFetch':
                    result = requireBindings().streamFetch(req.payload.conn, req.payload.stream);
                    break;
                case 'streamCancel':
                    requireBindings().streamCancel(req.payload.conn, req.payload.stream);
                    break;
                case 'putFile':
                    requireBindings().writeFile(req.payload.path, req.payload.data);
                    break;
                case 'registerFile':
                    requireBindings().registerFile(req.payload.name, req.payload.data);
                    break;
                case 'unregisterFile':
                    requireBindings().unregisterFile(req.payload.name);
                    break;
                case 'clearFiles':
                    requireBindings().clearFiles();
                    break;
                case 'close':
                    bindings = null;
                    break;
                default:
                    throw new Error('unknown request type: ' + req.type);
            }
            // Transfer the result buffer (zero-copy) when present (query or stream chunk).
            const buf = result?.data instanceof Uint8Array
                ? result.data.buffer
                : result?.result?.data instanceof Uint8Array
                    ? result.result.data.buffer
                    : undefined;
            post({ id: req.id, ok: true, result }, buf ? [buf] : undefined);
        }
        catch (e) {
            post({ id: req.id, ok: false, error: e && e.message ? e.message : String(e) });
        }
    })();
});
//# sourceMappingURL=worker.js.map