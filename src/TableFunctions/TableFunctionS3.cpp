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
#include <IO/WasmS3Auth.h>
#include <Poco/URI.h>

#include <algorithm>
#include <cctype>
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
    WasmS3::rewriteS3Source(source, filename, region);

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
    return WasmS3::signV4Request("GET", filename, region, access_key_id, secret_access_key, session_token);
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
