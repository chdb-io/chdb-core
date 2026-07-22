/** A connection handle is an opaque wasm pointer (BigInt under the Memory64 ABI). */
export type ConnHandle = number | bigint;
/** Plain, transferable query result (the client wraps it with a text() helper). */
export interface WireResult {
    data: Uint8Array;
    rowsRead: number;
    bytesRead: number;
    /** Source rows/bytes SCANNED from storage (≠ result size); for the "Processed N rows" footer. */
    scannedRows: number;
    scannedBytes: number;
    elapsedSeconds: number;
}
/**
 * Live query-EXECUTION progress (distinct from the wasm-DOWNLOAD 'progress' event).
 * Delivered by the main thread polling a shared-memory struct the engine writes — see
 * AsyncChdb's progress poll — not by a worker message.
 */
export interface QueryProgress {
    readRows: number;
    totalRowsToRead: number;
    readBytes: number;
    totalBytesToRead: number;
    elapsedNs: number;
}
export type RequestType = 'init' | 'query' | 'connect' | 'closeConn' | 'queryConn' | 'streamStart' | 'streamFetch' | 'streamCancel' | 'putFile' | 'registerFile' | 'unregisterFile' | 'clearFiles' | 'close';
/** Result of one streaming fetch: a chunk, or done=true at end-of-stream. */
export interface WireChunk {
    done: boolean;
    result?: WireResult;
}
export interface WorkerRequest {
    id: number;
    type: RequestType;
    payload?: any;
}
export type WorkerResponse = {
    id: number;
    ok: true;
    result?: any;
} | {
    id: number;
    ok: false;
    error: string;
} | {
    id: number;
    event: 'progress';
    loaded: number;
    total: number;
};
//# sourceMappingURL=protocol.d.ts.map