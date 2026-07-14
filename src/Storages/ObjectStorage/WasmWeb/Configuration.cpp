#include <Storages/ObjectStorage/WasmWeb/Configuration.h>

#if defined(OS_WASM)

#include <Core/Settings.h>
#include <IO/WasmS3Auth.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Poco/URI.h>

#include <algorithm>
#include <cctype>

namespace DB
{
namespace Setting
{
    extern const SettingsBool engine_url_skip_empty_files;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_url;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString storage_region;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

/// headers('Name' = 'value', ...) — the form vended credentials are injected in
/// (see GCSCredentials::addCredentialsToEngineArgs).
HTTPHeaderEntries parseHeadersFunction(const ASTFunction & func)
{
    HTTPHeaderEntries entries;
    if (!func.arguments)
        return entries;
    for (const auto & child : func.arguments->children)
    {
        const auto * equals = child->as<ASTFunction>();
        if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "headers(...) arguments must be 'Name' = 'value' pairs");
        auto name = checkAndGetLiteralArgument<String>(equals->arguments->children[0], "header name");
        auto value = checkAndGetLiteralArgument<String>(equals->arguments->children[1], "header value");
        entries.emplace_back(std::move(name), std::move(value));
    }
    return entries;
}

}

void StorageWasmWebConfiguration::fromAST(ASTs & args, ContextPtr context, bool /* with_structure */)
{
    if (args.empty() || args.size() > 4)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "WasmWeb storage requires 1 to 4 arguments: "
            "url[, access_key_id, secret_access_key[, session_token]] or url, NOSIGN[, headers(...)]");

    /// headers(...) is a function, not a constant — pull it out before evaluation.
    ASTs literal_args;
    for (auto & arg : args)
    {
        if (const auto * func = arg->as<ASTFunction>(); func && func->name == "headers")
        {
            static_headers = parseHeadersFunction(*func);
            continue;
        }
        literal_args.push_back(evaluateConstantExpressionOrIdentifierAsLiteral(arg, context));
    }

    if (literal_args.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "WasmWeb storage requires a url argument");

    raw_uri = checkAndGetLiteralArgument<String>(literal_args[0], "url");

    if (literal_args.size() > 1)
    {
        const auto a1 = checkAndGetLiteralArgument<String>(literal_args[1], "access_key_id/NOSIGN");
        if (iequalsNoSign(a1))
        {
            no_sign = true;
            if (literal_args.size() > 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected arguments after NOSIGN");
        }
        else
        {
            if (literal_args.size() < 3)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "access_key_id requires a secret_access_key argument");
            access_key_id = a1;
            secret_access_key = checkAndGetLiteralArgument<String>(literal_args[2], "secret_access_key");
            if (literal_args.size() > 3)
                session_token = checkAndGetLiteralArgument<String>(literal_args[3], "session_token");
        }
    }

    /// s3://bucket/key -> https virtual-hosted URL; http(s):// passes through.
    String url;
    WasmS3::rewriteS3Source(raw_uri, url, region);

    Poco::URI uri(url);
    base_url = uri.getScheme() + "://" + uri.getAuthority();
    String key_prefix = uri.getPath();
    while (key_prefix.starts_with('/'))
        key_prefix = key_prefix.substr(1);

    if (raw_uri.starts_with("s3://") || raw_uri.starts_with("S3://"))
    {
        /// Virtual-hosted rewrite: the bucket lives in the hostname, keys don't carry it.
        bucket = Poco::URI(raw_uri).getHost();
        keys_carry_bucket = false;
    }
    else
    {
        /// http(s) endpoint. Virtual-hosted hosts (bucket.s3.region.amazonaws.com,
        /// bucket.s3-accesspoint..., MinIO vhost setups) embed the bucket as the
        /// first host label; everything else is path-style and the bucket is the
        /// first segment of every key.
        const String host = uri.getHost();
        const size_t first_dot = host.find('.');
        const bool virtual_hosted = first_dot != String::npos
            && (host.compare(first_dot, 4, ".s3.") == 0 || host.compare(first_dot, 4, ".s3-") == 0);
        if (virtual_hosted)
        {
            bucket = host.substr(0, first_dot);
            keys_carry_bucket = false;
        }
        else
        {
            bucket = key_prefix.substr(0, key_prefix.find('/'));
            keys_carry_bucket = true;
        }
    }

    path = key_prefix;
    paths = {path};
}

void StorageWasmWebConfiguration::fromNamedCollection(const NamedCollection &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Named collections are not supported for WasmWeb storage");
}

StorageObjectStorageQuerySettings StorageWasmWebConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
        .truncate_on_insert = false,
        .create_new_file_on_insert = false,
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_url],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::engine_url_skip_empty_files],
        .list_object_keys_size = 0,
        .throw_on_zero_files_match = false,
        .ignore_non_existent_file = false};
}

ObjectStoragePtr StorageWasmWebConfiguration::createObjectStorage(
    ContextPtr, bool /* readonly */, CredentialsConfigurationCallback /* refresh_credentials_callback */)
{
    /// An explicit SETTINGS storage_region overrides the hostname-derived SigV4
    /// region (S3-compatible endpoints often don't encode their region in the
    /// host). Only reachable through DataLakeConfiguration, which implements
    /// getDataLakeSettings(); the base fallback throws, hence the guard.
    try
    {
        const auto & lake_settings = getDataLakeSettings();
        if (lake_settings[DataLakeStorageSetting::storage_region].changed)
            region = lake_settings[DataLakeStorageSetting::storage_region].value;
    }
    catch (...) /// NOLINT(bugprone-empty-catch)
    {
    }

    WasmWebObjectStorageSettings storage_settings;
    storage_settings.base_url = base_url;
    /// listObjects strips the bucket from key prefixes; only meaningful when
    /// keys actually carry it (path-style). Virtual-hosted keys are bucket-free.
    storage_settings.bucket = keys_carry_bucket ? bucket : String{};
    storage_settings.region = region;
    if (!no_sign)
    {
        storage_settings.access_key_id = access_key_id;
        storage_settings.secret_access_key = secret_access_key;
        storage_settings.session_token = session_token;
    }
    storage_settings.static_headers = static_headers;
    return std::make_shared<WasmWebObjectStorage>(std::move(storage_settings));
}

}

#endif
