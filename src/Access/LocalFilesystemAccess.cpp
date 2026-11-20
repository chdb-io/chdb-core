#include <Access/LocalFilesystemAccess.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/case_conv.hpp>

#include <algorithm>
#include <array>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

namespace
{

std::mutex restricted_user_mutex;
std::optional<RestrictedLocalFilesystemUser> restricted_user;

}

void setRestrictedLocalFilesystemUser(std::optional<RestrictedLocalFilesystemUser> user)
{
    std::lock_guard lock(restricted_user_mutex);
    restricted_user = std::move(user);
}

std::optional<RestrictedLocalFilesystemUser> tryGetRestrictedLocalFilesystemUser(const UUID & user_id)
{
    std::lock_guard lock(restricted_user_mutex);
    if (restricted_user && restricted_user->id == user_id)
        return restricted_user;
    return std::nullopt;
}

bool isLocalFilesystemAccessRestricted(const ContextPtr & context)
{
    if (!context)
        return false;
    const auto user_id = context->getUserID();
    if (!user_id)
        return false;
    std::lock_guard lock(restricted_user_mutex);
    return restricted_user && restricted_user->id == *user_id;
}

void checkLocalFilesystemAccess(const ContextPtr & context, std::string_view operation)
{
    if (isLocalFilesystemAccessRestricted(context))
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "{} is not allowed: local filesystem access is restricted for this connection (restrict_local_filesystem_access)",
            operation);
}

void checkDictionaryLocalFilesystemAccess(const ContextPtr & context, std::string_view source, std::string_view layout)
{
    static constexpr std::array<std::string_view, 7> local_sources{
        "file", "executable", "executable_pool", "library", "odbc", "jdbc", "yamlregexptree"};
    static constexpr std::array<std::string_view, 2> local_layouts{"ssd_cache", "complex_key_ssd_cache"};

    const String source_lower = boost::algorithm::to_lower_copy(String(source));
    const String layout_lower = boost::algorithm::to_lower_copy(String(layout));
    if (std::ranges::find(local_sources, source_lower) != local_sources.end())
        checkLocalFilesystemAccess(context, fmt::format("A dictionary with source {}", source));
    if (std::ranges::find(local_layouts, layout_lower) != local_layouts.end())
        checkLocalFilesystemAccess(context, fmt::format("A dictionary with layout {}", layout));
}

void restrictClickHouseDictionarySourceUser(const ContextPtr & context, ASTFunctionWithKeyValueArguments & source)
{
    if (!isLocalFilesystemAccessRestricted(context) || boost::algorithm::to_lower_copy(source.name) != "clickhouse")
        return;

    const String user_name = context->getUserName();
    if (!source.elements)
        source.elements = make_intrusive<ASTExpressionList>();
    for (const auto & child : source.elements->children)
    {
        const auto * pair = child->as<ASTPair>();
        if (!pair || pair->first != "user")
            continue;
        const auto * literal = pair->second ? pair->second->as<ASTLiteral>() : nullptr;
        if (!literal || literal->value.getType() != Field::Types::String || literal->value.safeGet<String>() != user_name)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "A CLICKHOUSE dictionary source on a connection that restricts local filesystem access must run as user {}",
                user_name);
        return;
    }

    auto pair = make_intrusive<ASTPair>();
    pair->first = "user";
    pair->set(pair->second, make_intrusive<ASTLiteral>(user_name));
    source.elements->children.push_back(pair);
}

}
