// Message protocol between the main-thread client (AsyncChdb) and the worker
// dispatcher. Kept plain-data so it survives structured clone across the worker
// boundary (no functions, no class instances).

/** A connection handle is an opaque wasm pointer (BigInt under the Memory64 ABI). */
export type ConnHandle = number | bigint;

/** Plain, transferable query result (the client wraps it with a text() helper). */
export interface WireResult {
  data: Uint8Array;
  rowsRead: number;
  bytesRead: number;
  elapsedSeconds: number;
}

export type RequestType =
  | 'init'
  | 'query'
  | 'connect'
  | 'closeConn'
  | 'queryConn'
  | 'streamStart'
  | 'streamFetch'
  | 'streamCancel'
  | 'putFile'
  | 'mountFile'
  | 'close';

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

export type WorkerResponse =
  | { id: number; ok: true; result?: any }
  | { id: number; ok: false; error: string }
  | { id: number; event: 'progress'; loaded: number; total: number };
