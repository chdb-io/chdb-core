#include <Access/LocalFilesystemAccess.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/case_conv.hpp>

#include <algorithm>
#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

bool hasLocalFilesystemAccess(const ContextPtr & context)
{
    const auto source = toStringSource(AccessTypeObjects::Source::FILE);
    const auto access = context->getAccess();
    return access->isGranted(AccessType::READ, source) && access->isGranted(AccessType::WRITE, source);
}

void checkLocalFilesystemAccess(const ContextPtr & context, std::string_view operation)
{
    if (!hasLocalFilesystemAccess(context))
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "{} is not allowed: local filesystem access is restricted for this connection "
            "(it needs the READ and WRITE grants on the FILE source)",
            operation);
}

void checkDictionaryLocalFilesystemAccess(const ContextPtr & context, std::string_view source, std::string_view layout)
{
    static constexpr std::array<std::string_view, 6> local_sources{"file", "executable", "executable_pool", "library", "odbc", "jdbc"};
    static constexpr std::array<std::string_view, 2> local_layouts{"ssd_cache", "complex_key_ssd_cache"};

    const String source_lower = boost::algorithm::to_lower_copy(String(source));
    const String layout_lower = boost::algorithm::to_lower_copy(String(layout));
    if (std::ranges::find(local_sources, source_lower) != local_sources.end())
        checkLocalFilesystemAccess(context, fmt::format("A dictionary with source {}", source));
    if (std::ranges::find(local_layouts, layout_lower) != local_layouts.end())
        checkLocalFilesystemAccess(context, fmt::format("A dictionary with layout {}", layout));
}

}
