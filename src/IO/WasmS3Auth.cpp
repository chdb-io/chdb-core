#include <IO/WasmS3Auth.h>

#if defined(OS_WASM)

#include "config.h"

#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <algorithm>
#include <cctype>
#include <ctime>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace WasmS3
{

String extractRegionFromHost(const String & host)
{
    std::vector<String> tok;
    size_t start = 0;
    for (size_t i = 0; i <= host.size(); ++i)
    {
        if (i == host.size() || host[i] == '.')
        {
            tok.push_back(host.substr(start, i - start));
            start = i + 1;
        }
    }
    for (size_t i = 0; i < tok.size(); ++i)
    {
        if (tok[i] == "s3")
        {
            if (i + 1 < tok.size() && tok[i + 1] != "amazonaws")
                return tok[i + 1];
            return "us-east-1";
        }
        if (tok[i].rfind("s3-", 0) == 0 && tok[i].size() > 3)
            return tok[i].substr(3);
    }
    return "us-east-1";
}

void rewriteS3Source(const String & source, String & out_url, String & out_region)
{
    Poco::URI uri(source);
    String scheme = uri.getScheme();
    std::transform(scheme.begin(), scheme.end(), scheme.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (scheme == "s3")
    {
        /// An s3:// URL carries no region: map to the region-agnostic virtual-hosted
        /// endpoint (matching src/IO/S3/URI.cpp) and default the SigV4 region to us-east-1.
        /// For a bucket outside us-east-1 *with credentials*, pass the full regional URL
        /// (https://bucket.s3.<region>.amazonaws.com/...) so signing uses the right region.
        out_region = "us-east-1";
        const String bucket = uri.getHost();
        String path = uri.getPath();
        if (path.empty())
            path = "/";
        out_url = "https://" + bucket + ".s3.amazonaws.com" + path;
        if (!uri.getRawQuery().empty())
            out_url += "?" + uri.getRawQuery();
    }
    else
    {
        out_url = source;
        out_region = extractRegionFromHost(uri.getHost());
    }
}

#if USE_SSL

namespace
{

String toLowerHex(const uint8_t * data, size_t n)
{
    static const char digits[] = "0123456789abcdef";
    String out;
    out.resize(n * 2);
    for (size_t i = 0; i < n; ++i)
    {
        out[2 * i] = digits[data[i] >> 4];
        out[2 * i + 1] = digits[data[i] & 0xf];
    }
    return out;
}

String sha256HexLower(const String & s)
{
    const String raw = encodeSHA256(s); /// 32 raw bytes
    return toLowerHex(reinterpret_cast<const uint8_t *>(raw.data()), raw.size());
}

/// AWS RFC3986 percent-encoding. Leaves unreserved bytes; '/' is preserved in paths.
String uriEncode(const String & s, bool encode_slash)
{
    static const char hexu[] = "0123456789ABCDEF";
    String out;
    out.reserve(s.size());
    for (unsigned char c : s)
    {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
            || c == '-' || c == '_' || c == '.' || c == '~')
            out += static_cast<char>(c);
        else if (c == '/' && !encode_slash)
            out += '/';
        else
        {
            out += '%';
            out += hexu[c >> 4];
            out += hexu[c & 0xf];
        }
    }
    return out;
}

}

HTTPHeaderEntries signV4Request(
    const String & method,
    const String & url,
    const String & region,
    const String & access_key_id,
    const String & secret_access_key,
    const String & session_token)
{
    Poco::URI uri(url);
    /// The signed host header must match what the transport sends: browsers and
    /// fetch() include a non-default port ("127.0.0.1:9000" for MinIO/moto) and
    /// omit the scheme's default port.
    String host = uri.getHost();
    const auto port = uri.getPort();  /// the scheme's well-known port when unspecified
    const bool is_https = Poco::icompare(uri.getScheme(), "https") == 0;
    if (port != 0 && port != (is_https ? 443 : 80))
        host += ":" + std::to_string(port);
    String path = uri.getPath();
    if (path.empty())
        path = "/";
    const String canonical_uri = uriEncode(path, /*encode_slash=*/false);

    std::vector<std::pair<String, String>> qparams;
    for (const auto & p : uri.getQueryParameters())
        qparams.emplace_back(uriEncode(p.first, true), uriEncode(p.second, true));
    std::sort(qparams.begin(), qparams.end());
    String canonical_query;
    for (size_t i = 0; i < qparams.size(); ++i)
    {
        if (i)
            canonical_query += '&';
        canonical_query += qparams[i].first;
        canonical_query += '=';
        canonical_query += qparams[i].second;
    }

    const time_t now = time(nullptr);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    char amz_date[32];
    char date_stamp[16];
    strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &tm_utc);
    strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &tm_utc);

    const String payload_hash = "UNSIGNED-PAYLOAD";
    String signed_headers = "host;x-amz-content-sha256;x-amz-date";
    String canonical_headers
        = "host:" + host + "\n" + "x-amz-content-sha256:" + payload_hash + "\n" + "x-amz-date:" + String(amz_date) + "\n";
    if (!session_token.empty())
    {
        signed_headers += ";x-amz-security-token";
        canonical_headers += "x-amz-security-token:" + session_token + "\n";
    }

    const String canonical_request
        = method + "\n" + canonical_uri + "\n" + canonical_query + "\n" + canonical_headers + "\n" + signed_headers + "\n" + payload_hash;

    const String scope = String(date_stamp) + "/" + region + "/s3/aws4_request";
    const String string_to_sign
        = "AWS4-HMAC-SHA256\n" + String(amz_date) + "\n" + scope + "\n" + sha256HexLower(canonical_request);

    const String key0 = "AWS4" + secret_access_key;
    std::vector<uint8_t> k_secret(key0.begin(), key0.end());
    const auto k_date = hmacSHA256(k_secret, String(date_stamp));
    const auto k_region = hmacSHA256(k_date, region);
    const auto k_service = hmacSHA256(k_region, "s3");
    const auto k_signing = hmacSHA256(k_service, "aws4_request");
    const auto sig = hmacSHA256(k_signing, string_to_sign);
    const String signature = toLowerHex(sig.data(), sig.size());

    const String authorization = "AWS4-HMAC-SHA256 Credential=" + access_key_id + "/" + scope
        + ", SignedHeaders=" + signed_headers + ", Signature=" + signature;

    HTTPHeaderEntries headers;
    headers.emplace_back("x-amz-date", String(amz_date));
    headers.emplace_back("x-amz-content-sha256", payload_hash);
    headers.emplace_back("Authorization", authorization);
    if (!session_token.empty())
        headers.emplace_back("x-amz-security-token", session_token);
    return headers;
}

#else

HTTPHeaderEntries signV4Request(const String &, const String &, const String &, const String &, const String &, const String &)
{
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED, "AWS SigV4 signing requires OpenSSL support, which is disabled in this build");
}

#endif

}

}

#endif
