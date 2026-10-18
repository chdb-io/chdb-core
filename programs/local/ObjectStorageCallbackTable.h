#pragma once

#include "chdb.h"

#include <cstring>
#include <string>

namespace CHDB::ObjectStorageApi
{

inline std::string checkVersion(uint32_t version)
{
    if (version == CHDB_OBJECT_STORAGE_ABI_V1)
        return {};
    return "callback storage ABI mismatch: caller=" + std::to_string(version)
        + ", supported=" + std::to_string(CHDB_OBJECT_STORAGE_ABI_V1);
}

template <typename T>
bool copyField(const void * source, size_t source_size, size_t offset, T & destination)
{
    destination = T{};
    if (source_size <= offset)
        return true;
    if (source_size - offset < sizeof(T))
        return false;
    std::memcpy(&destination, static_cast<const char *>(source) + offset, sizeof(T));
    return true;
}

inline bool copyTable(const void * source, chdb_object_storage_callbacks & table, std::string & error)
{
    table = chdb_object_storage_callbacks_init();
    error.clear();
    if (!source)
    {
        error = "the callback table is NULL";
        return false;
    }
    uint32_t struct_size = 0;
    std::memcpy(&struct_size, source, sizeof(struct_size));
    if (struct_size < CHDB_OBJECT_STORAGE_CALLBACKS_HEADER_SIZE)
    {
        error = "struct_size " + std::to_string(struct_size) + " is smaller than the callback header size "
            + std::to_string(CHDB_OBJECT_STORAGE_CALLBACKS_HEADER_SIZE);
        return false;
    }
    uint32_t version = 0;
    std::memcpy(&version, static_cast<const char *>(source) + offsetof(chdb_object_storage_callbacks, version), sizeof(version));
    error = checkVersion(version);
    if (!error.empty())
        return false;
    table.version = version;
    if (struct_size < CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE)
    {
        error = "struct_size " + std::to_string(struct_size) + " is smaller than the v1 callback table size "
            + std::to_string(CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE);
        return false;
    }

#define COPY_CALLBACK_FIELD(field) \
    if (!copyField(source, struct_size, offsetof(chdb_object_storage_callbacks, field), table.field)) \
    { error = "struct_size cuts through callback field " #field; return false; }

    COPY_CALLBACK_FIELD(ud);
    COPY_CALLBACK_FIELD(metadata);
    COPY_CALLBACK_FIELD(read);
    COPY_CALLBACK_FIELD(write_begin);
    COPY_CALLBACK_FIELD(write_append);
    COPY_CALLBACK_FIELD(write_commit);
    COPY_CALLBACK_FIELD(write_abort);
    COPY_CALLBACK_FIELD(remove);
    COPY_CALLBACK_FIELD(list);
    COPY_CALLBACK_FIELD(copy);
    COPY_CALLBACK_FIELD(open_keyspace);
    COPY_CALLBACK_FIELD(close_keyspace);
#undef COPY_CALLBACK_FIELD

    auto require = [&](bool present, const char * field)
    {
        if (!present)
        {
            if (!error.empty())
                error += ", ";
            error += field;
        }
    };
    require(table.metadata != nullptr, "metadata");
    require(table.read != nullptr, "read");
    require(table.write_begin != nullptr, "write_begin");
    require(table.write_append != nullptr, "write_append");
    require(table.write_commit != nullptr, "write_commit");
    require(table.write_abort != nullptr, "write_abort");
    require(table.remove != nullptr, "remove");
    require(table.list != nullptr, "list");
    if (!error.empty())
    {
        error = "required callbacks are NULL: " + error;
        return false;
    }
    if ((table.open_keyspace == nullptr) != (table.close_keyspace == nullptr))
    {
        error = "open_keyspace and close_keyspace must be set together";
        return false;
    }
    return true;
}

}
