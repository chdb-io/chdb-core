#pragma once

#if defined(OS_WASM)

#include <Poco/Net/HTTPBasicStreamBuf.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>

#include <cstring>
#include <istream>
#include <optional>
#include <sstream>
#include <string>

namespace DB
{

/// HTTP "session" for the WASM build: no socket is ever opened. sendRequest()
/// only records the request line/headers and hands back a buffer for the body;
/// receiveResponse() then performs the whole exchange as one synchronous HTTP
/// request through the host JavaScript environment (WasmHTTPBridge) and returns
/// an in-memory stream over the fully-buffered response body. Plugged in behind
/// makeHTTPSession(), so every ClickHouse HTTP client path (ReadWriteBufferFromHTTP,
/// data-lake REST catalogs, OAuth token fetches, ...) transparently rides the
/// browser's fetch stack — which also supplies TLS, redirects and proxying.
/// Intended for small control-plane exchanges; bulk data reads should keep using
/// the range-based ReadBufferFromWebFetch, which never buffers whole files.
class WasmHTTPSession : public Poco::Net::HTTPClientSession
{
public:
    explicit WasmHTTPSession(const Poco::URI & uri);

    std::ostream & sendRequest(Poco::Net::HTTPRequest & request, uint64_t * connect_time, uint64_t * first_byte_time) override;
    std::istream & receiveResponse(Poco::Net::HTTPResponse & response) override;
    void flushRequest() override;
    void reset() override;
    bool secure() const override;

private:
    /// In-memory "device" for the fully-buffered response body. Consumers
    /// (ReadBufferFromIStream) require the response istream to be backed by an
    /// HTTPBasicStreamBuf and read through readFromDevice() directly.
    class ResponseStreamBuf : public Poco::Net::HTTPBasicStreamBuf
    {
    public:
        explicit ResponseStreamBuf(std::string body_)
            : Poco::Net::HTTPBasicStreamBuf(Poco::Net::HTTP_DEFAULT_BUFFER_SIZE, std::ios::in), body(std::move(body_))
        {
        }

        int readFromDevice(char * buffer, std::streamsize length) override
        {
            size_t n = std::min(static_cast<size_t>(length), body.size() - offset);
            if (n == 0)
                return 0;  /// EOF
            memcpy(buffer, body.data() + offset, n);
            offset += n;
            return static_cast<int>(n);
        }

    private:
        std::string body;
        size_t offset = 0;
    };

    /// scheme://host[:port]; the per-request URI (path + query) is appended.
    std::string base_url;
    bool https = false;

    std::string pending_method;
    std::string pending_uri;
    std::string pending_headers;
    std::optional<std::ostringstream> request_body;
    std::optional<ResponseStreamBuf> response_buf;
    std::optional<std::istream> response_stream;
};

}

#endif
