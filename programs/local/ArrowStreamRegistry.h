#pragma once

#include "chdb-internal.h"

#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <utility>
#include <vector>

#include <base/types.h>

struct ArrowArrayStream;

namespace CHDB
{

class ArrowStreamRegistry
{
public:
    struct ArrowStreamInfo
    {
        ArrowArrayStream * stream = nullptr;
        bool is_owner = false;
        /// Keeps owned wrapper streams (chdb_arrow_array_scan) alive while any
        /// active query holds ArrowStreamInfo, and releases them automatically
        /// when unregistered or when the owning connection closes.
        std::shared_ptr<void> owner_keepalive;
        /// Shared across all ArrowTableReader / schema-probe instances touching
        /// this registered stream so concurrent or self-join scans serialize
        /// ArrowArrayStream::get_schema / get_next calls.
        std::shared_ptr<std::mutex> stream_mutex;
    };

private:
    std::unordered_map<String, ArrowStreamInfo> registered_streams;
    mutable std::shared_mutex registry_mutex;

public:
    ~ArrowStreamRegistry()
    {
        clear();
    }

    static ArrowStreamRegistry & instance()
    {
        static ArrowStreamRegistry registry_instance;
        return registry_instance;
    }

    bool registerArrowStream(const String & name, ArrowArrayStream * arrow_stream, bool is_owner)
    {
        std::unique_lock<std::shared_mutex> lock(registry_mutex);
        if (registered_streams.contains(name))
            return false;

        ArrowStreamInfo info;
        info.stream = arrow_stream;
        info.is_owner = is_owner;
        info.stream_mutex = std::make_shared<std::mutex>();
        if (is_owner && arrow_stream)
        {
            info.owner_keepalive = std::shared_ptr<void>(
                arrow_stream,
                [](void * ptr) { chdb_destroy_arrow_stream(static_cast<ArrowArrayStream *>(ptr)); });
        }

        registered_streams.emplace(name, std::move(info));
        return true;
    }

    std::optional<ArrowStreamInfo> getArrowStream(const String & name) const
    {
        std::shared_lock<std::shared_mutex> lock(registry_mutex);
        auto it = registered_streams.find(name);
        if (it != registered_streams.end())
            return it->second;
        return {};
    }

    bool unregisterArrowStream(const String & name)
    {
        ArrowStreamInfo removed;
        {
            std::unique_lock<std::shared_mutex> lock(registry_mutex);
            auto it = registered_streams.find(name);
            if (it == registered_streams.end())
                return false;
            removed = std::move(it->second);
            registered_streams.erase(it);
        }
        return true;
    }

    std::vector<String> listRegisteredNames() const
    {
        std::shared_lock<std::shared_mutex> lock(registry_mutex);
        std::vector<String> names;
        names.reserve(registered_streams.size());

        for (const auto & [name, info] : registered_streams)
            names.push_back(name);

        return names;
    }

    size_t size() const
    {
        std::shared_lock<std::shared_mutex> lock(registry_mutex);
        return registered_streams.size();
    }

    void clear()
    {
        std::unordered_map<String, ArrowStreamInfo> dropped;
        {
            std::unique_lock<std::shared_mutex> lock(registry_mutex);
            dropped.swap(registered_streams);
        }
        dropped.clear();
    }
};

}
