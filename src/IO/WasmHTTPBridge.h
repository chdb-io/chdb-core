#pragma once

#if defined(OS_WASM)

#include <string>

namespace DB
{

/// One synchronous HTTP request performed by the host JavaScript environment,
/// for the WASM build where raw sockets (and thus Poco/curl based HTTP) are
/// unavailable. In a browser this is a synchronous XMLHttpRequest running on
/// the calling pthread's Web Worker (where sync XHR with a binary responseType
/// is permitted); under Node.js it shells out to a short-lived `node -e` child
/// process that runs fetch() and returns the response over stdout. TLS,
/// redirects and content decoding are all handled by the host.
struct WasmHTTPResult
{
    /// HTTP status code, or -1 when the request could not be performed at all
    /// (network error, environment without a usable transport, ...).
    int status = -1;
    /// Raw response headers, newline-delimited "Name: value" lines.
    std::string headers;
    std::string body;
};

/// headers_blob is a newline-delimited "Name: value" blob (same wire format as
/// ReadBufferFromWebFetch); forbidden/hop-by-hop headers are skipped by the host.
/// When range_offset >= 0, a "Range: bytes=<offset>-<offset+length-1>" header is
/// added and, if the server ignores it (HTTP 200 instead of 206), the host slices
/// the [offset, offset+length) window out of the full body *before* copying it
/// into wasm memory, so callers never haul the whole file across per read.
WasmHTTPResult performWasmHTTPRequest(
    const std::string & method,
    const std::string & url,
    const std::string & headers_blob,
    const std::string & body,
    long long range_offset = -1,
    long long range_length = -1);

}

#endif
