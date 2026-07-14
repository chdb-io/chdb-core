#include <IO/WasmHTTPBridge.h>

#if defined(OS_WASM)

#include <cstdint>
#include <cstdlib>

#include <emscripten.h>

namespace DB
{

/// Performs one synchronous HTTP request in the calling pthread's JS context.
/// Browser: synchronous XMLHttpRequest (legal on worker threads, where every
/// wasm pthread lives). Node: no in-process synchronous fetch exists, so a
/// short-lived `node -e` child performs the fetch and streams the response back
/// over stdout as JSON (status / headers / base64 body).
///
/// Pointer arguments arrive as BigInt under -sMEMORY64 and are normalized with
/// Number(). Variable-size outputs (headers, body) are _malloc'ed by JS and
/// their {ptr,len} written into the 5x int64 `out` struct:
///   [0] status  [1] headers ptr  [2] headers len  [3] body ptr  [4] body len
/// The C++ caller owns (frees) the two allocations. Returns 0 on success, -1
/// when no transport is available or the request failed outright.
EM_JS(int, chdb_wasm_http_request_js, (
    const char * method_ptr, const char * url_ptr, const char * headers_ptr,
    const char * body_ptr, double body_len,
    double range_offset, double range_length,
    char * out_ptr), {
    try {
        var method = UTF8ToString(Number(method_ptr));
        var url = UTF8ToString(Number(url_ptr));
        var headersBlob = UTF8ToString(Number(headers_ptr));
        var bodyLen = Number(body_len);
        var reqBody = null;
        if (bodyLen > 0) {
            // Copy out of the (possibly shared) wasm heap: XHR/fetch reject SAB views.
            reqBody = new Uint8Array(bodyLen);
            reqBody.set(HEAPU8.subarray(Number(body_ptr), Number(body_ptr) + bodyLen));
        }
        var rangeOff = range_offset;
        var rangeLen = range_length;
        var status = -1;
        var respHeaders = "";
        var respBytes = null;
        if (typeof XMLHttpRequest !== 'undefined') {
            var xhr = new XMLHttpRequest();
            xhr.open(method, url, false);  // synchronous; wasm pthreads run on worker threads where this is permitted
            xhr.responseType = 'arraybuffer';
            var lines = headersBlob ? headersBlob.split('\n') : [];
            for (var i = 0; i < lines.length; i++) {
                var line = lines[i];
                if (!line) continue;
                var c = line.indexOf(': ');
                // Forbidden request headers (Host, Connection, ...) make setRequestHeader throw; skip them.
                if (c > 0) { try { xhr.setRequestHeader(line.substring(0, c), line.substring(c + 2)); } catch (e) {} }
            }
            if (rangeOff >= 0) xhr.setRequestHeader('Range', 'bytes=' + rangeOff + '-' + (rangeOff + rangeLen - 1));
            xhr.send(reqBody);
            status = xhr.status;
            if (status === 0) return -1;  // network / CORS failure
            // No regex literal here: EM_JS bodies survive string escapes but not regex escapes.
            respHeaders = (xhr.getAllResponseHeaders() || "").split('\r\n').join('\n');
            respBytes = xhr.response ? new Uint8Array(xhr.response) : new Uint8Array(0);
        } else if (typeof process !== 'undefined' && typeof require !== 'undefined') {
            var cp = require('node:child_process');
            // NB: no backslash escapes in this script — EM_JS embedding + the JS
            // minifier reprocess escape sequences and corrupt them; newlines are
            // spelled String.fromCharCode(10) ("NL") so the text survives verbatim.
            var script =
                'var NL=String.fromCharCode(10);' +
                'var m=process.argv[1],u=process.argv[2],hb=Buffer.from(process.argv[3],"base64").toString();' +
                'var cs=[];process.stdin.on("data",function(c){cs.push(c)});' +
                'process.stdin.on("end",function(){' +
                'var h={};hb.split(NL).forEach(function(l){var i=l.indexOf(": ");if(i>0)h[l.slice(0,i)]=l.slice(i+2)});' +
                'var b=Buffer.concat(cs);' +
                'fetch(u,{method:m,headers:h,body:(b.length?b:undefined),redirect:"follow"}).then(async function(r){' +
                'var ab=Buffer.from(await r.arrayBuffer());' +
                'var hs="";r.headers.forEach(function(v,k){hs+=k+": "+v+NL});' +
                'process.stdout.write(JSON.stringify({s:r.status,h:hs,b:ab.toString("base64")}));' +
                '}).catch(function(e){process.stdout.write(JSON.stringify({s:-1,h:"",b:"",m:String(e&&e.message||e)}))});});';
            var fullBlob = headersBlob || "";
            if (rangeOff >= 0) fullBlob += (fullBlob ? "\n" : "") + 'Range: bytes=' + rangeOff + '-' + (rangeOff + rangeLen - 1);
            var res = cp.spawnSync(
                process.execPath, ['-e', script, method, url, Buffer.from(fullBlob).toString('base64')],
                { input: reqBody ? Buffer.from(reqBody) : Buffer.alloc(0), maxBuffer: 1 << 30 });
            if (res.status !== 0 || !res.stdout || !res.stdout.length) {
                if (typeof console !== 'undefined')
                    console.error('chdb wasm http bridge: fetch child failed', method, url, res.status, res.error || "", res.stderr ? res.stderr.toString() : "");
                return -1;
            }
            var parsed = JSON.parse(res.stdout.toString());
            if (parsed.s < 0) {
                // parsed.m carries the child-side cause (DNS failure, connection refused,
                // V8 string cap on huge unranged bodies, ...) — without it every failure
                // would be an indistinguishable NETWORK_ERROR.
                if (typeof console !== 'undefined') console.error('chdb wasm http bridge: fetch failed', method, url, parsed.m || "");
                return -1;
            }
            status = parsed.s;
            respHeaders = parsed.h;
            respBytes = new Uint8Array(Buffer.from(parsed.b, 'base64'));
        } else {
            if (typeof console !== 'undefined')
                console.error('chdb wasm http bridge: no transport (no XMLHttpRequest and no Node require) for', method, url);
            return -1;
        }
        // A ranged request answered with 200 means the server ignored Range and
        // sent the whole body; slice the requested window out here so only those
        // bytes cross into wasm memory.
        if (rangeOff >= 0 && status === 200 && respBytes)
            respBytes = respBytes.subarray(rangeOff, rangeOff + rangeLen);
        var hdrBytes = new TextEncoder().encode(respHeaders);
        var hdrPtr = 0;
        var bodyPtr = 0;
        // With ALLOW_MEMORY_GROWTH malloc returns 0 on OOM instead of aborting;
        // writing at address 0 would smash static data and surface as silent EOF.
        if (hdrBytes.length) {
            hdrPtr = _malloc(BigInt(hdrBytes.length));
            if (!Number(hdrPtr)) throw new Error('bridge OOM allocating ' + hdrBytes.length + ' header bytes');
            HEAPU8.set(hdrBytes, Number(hdrPtr));
        }
        if (respBytes && respBytes.length) {
            bodyPtr = _malloc(BigInt(respBytes.length));
            if (!Number(bodyPtr)) { if (Number(hdrPtr)) _free(hdrPtr); throw new Error('bridge OOM allocating ' + respBytes.length + ' body bytes'); }
            HEAPU8.set(respBytes, Number(bodyPtr));
        }
        var dv = new DataView(HEAPU8.buffer);
        var out = Number(out_ptr);
        dv.setBigInt64(out, BigInt(status), true);
        dv.setBigInt64(out + 8, BigInt(hdrPtr), true);
        dv.setBigInt64(out + 16, BigInt(hdrBytes.length), true);
        dv.setBigInt64(out + 24, BigInt(bodyPtr), true);
        dv.setBigInt64(out + 32, BigInt(respBytes ? respBytes.length : 0), true);
        return 0;
    } catch (e) {
        // Surface the host-side cause (the C++ layer only sees "-1"): CORS
        // rejections, missing transports, heap-view surprises all land here.
        if (typeof console !== 'undefined') console.error('chdb wasm http bridge:', e);
        return -1;
    }
});

WasmHTTPResult performWasmHTTPRequest(
    const std::string & method,
    const std::string & url,
    const std::string & headers_blob,
    const std::string & body,
    long long range_offset,
    long long range_length)
{
    int64_t out[5] = {};
    int rc = chdb_wasm_http_request_js(
        method.c_str(), url.c_str(), headers_blob.c_str(),
        body.data(), static_cast<double>(body.size()),
        static_cast<double>(range_offset), static_cast<double>(range_length),
        reinterpret_cast<char *>(out));

    WasmHTTPResult result;
    if (rc != 0)
        return result;

    result.status = static_cast<int>(out[0]);
    if (out[1] != 0)
    {
        if (out[2] > 0)
            result.headers.assign(reinterpret_cast<const char *>(out[1]), static_cast<size_t>(out[2]));
        std::free(reinterpret_cast<void *>(out[1]));  /// NOLINT
    }
    if (out[3] != 0)
    {
        if (out[4] > 0)
            result.body.assign(reinterpret_cast<const char *>(out[3]), static_cast<size_t>(out[4]));
        std::free(reinterpret_cast<void *>(out[3]));  /// NOLINT
    }
    return result;
}

}

#endif
