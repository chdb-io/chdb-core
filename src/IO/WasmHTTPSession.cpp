#include <IO/WasmHTTPSession.h>

#if defined(OS_WASM)

#include <IO/WasmHTTPBridge.h>
#include <Common/Exception.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace
{

/// Headers the host HTTP stack manages itself; forwarding them is at best a
/// no-op (XHR rejects forbidden headers) and at worst inconsistent with the
/// re-framed body (Content-Length/Transfer-Encoding after buffering).
bool isHopByHopHeader(const std::string & name)
{
    return Poco::icompare(name, "Host") == 0
        || Poco::icompare(name, "Connection") == 0
        || Poco::icompare(name, "Content-Length") == 0
        || Poco::icompare(name, "Transfer-Encoding") == 0
        || Poco::icompare(name, "Keep-Alive") == 0
        || Poco::icompare(name, "Proxy-Connection") == 0
        || Poco::icompare(name, "Expect") == 0;
}

}

WasmHTTPSession::WasmHTTPSession(const Poco::URI & uri)
    : Poco::Net::HTTPClientSession(uri.getHost(), uri.getPort())
    , base_url(uri.getScheme() + "://" + uri.getAuthority())
    , https(Poco::icompare(uri.getScheme(), "https") == 0)
{
}

std::ostream & WasmHTTPSession::sendRequest(Poco::Net::HTTPRequest & request, uint64_t *, uint64_t *)
{
    pending_method = request.getMethod();
    pending_uri = request.getURI();
    pending_headers.clear();
    for (const auto & [name, value] : request)
    {
        if (isHopByHopHeader(name))
            continue;
        pending_headers += name;
        pending_headers += ": ";
        pending_headers += value;
        pending_headers += '\n';
    }
    response_stream.reset();
    response_buf.reset();
    request_body.emplace();
    return *request_body;
}

std::istream & WasmHTTPSession::receiveResponse(Poco::Net::HTTPResponse & response)
{
    /// The request URI is normally in origin form (path + query); proxy requests
    /// may carry an absolute URL — pass those through untouched.
    std::string url;
    if (pending_uri.starts_with("http://") || pending_uri.starts_with("https://"))
        url = pending_uri;
    else if (pending_uri.empty() || pending_uri[0] != '/')
        url = base_url + "/" + pending_uri;
    else
        url = base_url + pending_uri;

    auto result = performWasmHTTPRequest(pending_method, url, pending_headers, request_body ? request_body->str() : std::string{});
    request_body.reset();

    if (result.status < 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to perform HTTP request to '{}' through the WASM host environment", url);

    response.clear();
    response.setStatusAndReason(static_cast<Poco::Net::HTTPResponse::HTTPStatus>(result.status));
    size_t line_start = 0;
    while (line_start < result.headers.size())
    {
        size_t line_end = result.headers.find('\n', line_start);
        if (line_end == std::string::npos)
            line_end = result.headers.size();
        std::string_view line(result.headers.data() + line_start, line_end - line_start);
        line_start = line_end + 1;

        size_t colon = line.find(": ");
        if (colon == std::string_view::npos || colon == 0)
            continue;
        std::string name(line.substr(0, colon));
        std::string value(line.substr(colon + 2));
        /// The host already decoded the body and its length may differ from the
        /// wire values; the real length is set below.
        if (Poco::icompare(name, "Content-Length") == 0
            || Poco::icompare(name, "Content-Encoding") == 0
            || Poco::icompare(name, "Transfer-Encoding") == 0)
            continue;
        response.add(name, value);
    }
    response.setContentLength(static_cast<std::streamsize>(result.body.size()));

    response_stream.reset();
    response_buf.emplace(std::move(result.body));
    response_stream.emplace(&*response_buf);
    return *response_stream;
}

void WasmHTTPSession::flushRequest()
{
    /// The body is buffered until receiveResponse(); nothing to flush.
}

void WasmHTTPSession::reset()
{
    pending_method.clear();
    pending_uri.clear();
    pending_headers.clear();
    request_body.reset();
    response_stream.reset();
    response_buf.reset();
}

bool WasmHTTPSession::secure() const
{
    return https;
}

}

#endif
