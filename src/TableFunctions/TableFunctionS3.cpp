#include <TableFunctions/TableFunctionS3.h>

#if defined(OS_WASM)

#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageURL.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/OpenSSLHelpers.h>
#include <Poco/URI.h>

#include <algorithm>
#include <cctype>
#include <ctime>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
#if !USE_SSL
    extern const int SUPPORT_IS_DISABLED;
#endif
}

namespace
{

bool iequalsNoSign(const String & s)
{
    if (s.size() != 6)
        return false;
    String u = s;
    std::transform(u.begin(), u.end(), u.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return u == "NOSIGN";
}

bool isFormatName(const String & s)
{
    return s == "auto" || FormatFactory::instance().exists(s);
}

/// Extract the AWS region from an S3 host, e.g.
///   bucket.s3.eu-west-3.amazonaws.com -> eu-west-3
///   bucket.s3.amazonaws.com           -> us-east-1
///   s3.us-west-2.amazonaws.com         -> us-west-2
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

/// s3://bucket/key -> https://bucket.s3.<region>.amazonaws.com/key (virtual-hosted).
/// http(s):// URLs are passed through unchanged.
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

/// AWS Signature V4 headers for a GET with UNSIGNED-PAYLOAD.
HTTPHeaderEntries signV4Get(const String & url, const String & region,
    const String & access_key_id, const String & secret_access_key, const String & session_token)
{
    Poco::URI uri(url);
    const String host = uri.getHost();
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
        = "GET\n" + canonical_uri + "\n" + canonical_query + "\n" + canonical_headers + "\n" + signed_headers + "\n" + payload_hash;

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

#endif

}

void TableFunctionS3::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (args.empty() || args.size() > 7)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    const size_t count = args.size();
    std::unordered_map<std::string_view, size_t> idx;

    /// Argument disambiguation mirrors src/Storages/ObjectStorage/S3/Configuration.cpp
    /// (with_structure == true).
    if (count == 2)
    {
        const auto a1 = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
        if (iequalsNoSign(a1))
            no_sign = true;
        else
            idx = {{"format", 1}};
    }
    else if (count == 3)
    {
        const auto a1 = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
        if (iequalsNoSign(a1))
        {
            no_sign = true;
            idx = {{"format", 2}};
        }
        else if (isFormatName(a1))
            idx = {{"format", 1}, {"structure", 2}};
        else
            idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
    }
    else if (count == 4)
    {
        const auto a1 = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
        if (iequalsNoSign(a1))
        {
            no_sign = true;
            idx = {{"format", 2}, {"structure", 3}};
        }
        else if (isFormatName(a1))
            idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
        else
        {
            const auto a3 = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
            if (isFormatName(a3))
                idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
            else
                idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
        }
    }
    else if (count == 5)
    {
        const auto a1 = checkAndGetLiteralArgument<String>(args[1], "NOSIGN/access_key_id");
        if (iequalsNoSign(a1))
        {
            no_sign = true;
            idx = {{"format", 2}, {"structure", 3}, {"compression_method", 4}};
        }
        else
        {
            const auto a3 = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (isFormatName(a3))
                idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}};
            else
                idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
        }
    }
    else if (count == 6)
    {
        const auto a3 = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
        if (isFormatName(a3))
            idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}};
        else
            idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}};
    }
    else if (count == 7)
    {
        idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3},
               {"format", 4}, {"structure", 5}, {"compression_method", 6}};
    }

    auto getArg = [&](const char * key) -> String
    {
        auto it = idx.find(key);
        if (it == idx.end())
            return {};
        return checkAndGetLiteralArgument<String>(args[it->second], key);
    };

    access_key_id = getArg("access_key_id");
    secret_access_key = getArg("secret_access_key");
    session_token = getArg("session_token");

    const String fmt = getArg("format");
    if (!fmt.empty())
        format = fmt;
    const String st = getArg("structure");
    if (!st.empty())
        structure = st;
    const String comp = getArg("compression_method");
    if (!comp.empty())
        compression_method = comp;

    const String source = checkAndGetLiteralArgument<String>(args[0], "url");
    rewriteS3Source(source, filename, region);

    if (format == "auto")
    {
        if (auto fmt_from_url = tryGetFormatFromFirstArgument())
            format = *fmt_from_url;
    }
}

HTTPHeaderEntries TableFunctionS3::computeAuthHeaders() const
{
    if (no_sign || access_key_id.empty() || secret_access_key.empty())
        return {};
#if USE_SSL
    return signV4Get(filename, region, access_key_id, secret_access_key, session_token);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "s3() with credentials requires SSL support (SigV4 signing)");
#endif
}

std::optional<String> TableFunctionS3::tryGetFormatFromFirstArgument()
{
    return FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath());
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        const auto headers = computeAuthHeaders();
        const auto compression = chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method);
        if (format == "auto")
            return StorageURL::getTableStructureAndFormatFromData(filename, compression, headers, std::nullopt, context).first;
        return StorageURL::getTableStructureFromData(format, filename, compression, headers, std::nullopt, context);
    }

    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & compression_method_, bool /*is_insert_query*/) const
{
    return std::make_shared<StorageURL>(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        compression_method_,
        computeAuthHeaders(),
        "GET",
        nullptr,
        /*distributed_processing=*/false);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>({});
}

}

#endif
