/* Tests the frozen v1 header and the production table reader without starting the engine. */
#include "ObjectStorageCallbackTable.h"

#include <cassert>
#include <cstdio>
#include <cstring>

extern "C" int register_frozen_object_storage_v1(void);
extern "C" size_t frozen_object_storage_v1_size(void);

static chdb_object_storage_callbacks registered;
static size_t registrations = 0;
static size_t unregistrations = 0;

static chdb_state fail(char * output, size_t size, const std::string & message)
{
    if (output && size)
        std::snprintf(output, size, "%s", message.c_str());
    return CHDBError;
}

extern "C" chdb_state chdb_register_object_storage(const char * name,
    const void * cb, char * error, size_t error_size)
{
    std::string message;
    if (!name || !*name)
        return fail(error, error_size, "empty name");
    chdb_object_storage_callbacks table;
    if (!CHDB::ObjectStorageApi::copyTable(cb, table, message))
        return fail(error, error_size, message);
    registered = table;
    ++registrations;
    return CHDBSuccess;
}

extern "C" chdb_state chdb_unregister_object_storage(const char * name, char * error, size_t error_size)
{
    (void)name; (void)error; (void)error_size;
    ++unregistrations;
    return CHDBSuccess;
}

static void optional_extension() { }

int main()
{
    assert(frozen_object_storage_v1_size() == CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE);
    assert(register_frozen_object_storage_v1() == CHDBSuccess);
    assert(registrations == 1);
    assert(registered.version == CHDB_OBJECT_STORAGE_ABI_V1);
    assert(registered.ud && registered.copy == nullptr && registered.open_keyspace == nullptr);
    char error[256] = {};
    chdb_object_storage_call call{CHDB_OBJECT_STORAGE_CALL_V1_SIZE, error, sizeof(error)};
    uint64_t size = 0;
    int64_t mtime = 0;
    assert(registered.metadata(registered.ud, &call, "blob", &size, &mtime) == CHDB_OBJECT_STORAGE_OK);
    assert(size == 7 && mtime == 123);
    char data[8];
    size_t count = 0;
    assert(registered.read(registered.ud, &call, "blob", 0, data, sizeof(data), &count) == CHDB_OBJECT_STORAGE_OK);
    assert(count == 7 && std::memcmp(data, "content", count) == 0);

    auto cb = registered;
    assert(chdb_register_object_storage("wrapper", &cb, error, sizeof(error)) == CHDBSuccess);
    assert(registrations == 2);
    assert(chdb_unregister_object_storage("wrapper", error, sizeof(error)) == CHDBSuccess);
    assert(unregistrations == 1);
    cb.version = CHDB_OBJECT_STORAGE_ABI_V1 + 1;
    assert(chdb_register_object_storage("wrong", &cb, error, sizeof(error)) == CHDBError);
    assert(std::strstr(error, "ABI mismatch"));
    assert(registrations == 2);
    cb = registered;
    cb.version = 0;
    assert(chdb_register_object_storage("unset", &cb, error, sizeof(error)) == CHDBError);
    assert(registrations == 2);

    std::string message;
    chdb_object_storage_callbacks copied;
    cb.struct_size = CHDB_OBJECT_STORAGE_CALLBACKS_HEADER_SIZE - 1;
    assert(!CHDB::ObjectStorageApi::copyTable(&cb, copied, message));
    assert(message.find("callback header size") != std::string::npos);
    cb = registered;
    cb.struct_size = CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE - 1;
    assert(!CHDB::ObjectStorageApi::copyTable(&cb, copied, message));
    cb = registered;
    cb.struct_size = CHDB_OBJECT_STORAGE_CALLBACKS_HEADER_SIZE;
    cb.version = CHDB_OBJECT_STORAGE_ABI_V1 + 1;
    assert(!CHDB::ObjectStorageApi::copyTable(&cb, copied, message));
    assert(message.find("ABI mismatch") != std::string::npos);
    struct FutureHeader
    {
        uint32_t struct_size;
        uint32_t version;
    } future{sizeof(FutureHeader), CHDB_OBJECT_STORAGE_ABI_V1 + 1};
    assert(chdb_register_object_storage("future", &future, error, sizeof(error)) == CHDBError);
    assert(std::strstr(error, "ABI mismatch"));
    assert(registrations == 2);
    cb = registered;
    cb.read = nullptr;
    cb.list = nullptr;
    assert(!CHDB::ObjectStorageApi::copyTable(&cb, copied, message));
    assert(message.find("read, list") != std::string::npos);

    struct ExtendedTable
    {
        chdb_object_storage_callbacks base;
        void (*extra)();
    } extended{registered, optional_extension};
    extended.base.struct_size = sizeof(extended);
    assert(CHDB::ObjectStorageApi::copyTable(&extended.base, copied, message));
    assert(copied.read == registered.read);
    void (*extra)() = optional_extension;
    const size_t offset = offsetof(ExtendedTable, extra);
    assert(CHDB::ObjectStorageApi::copyField(&extended, CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE, offset, extra));
    assert(extra == nullptr);
    assert(!CHDB::ObjectStorageApi::copyField(&extended, offset + sizeof(extra) - 1, offset, extra));
    assert(extra == nullptr);
    assert(CHDB::ObjectStorageApi::copyField(&extended, sizeof(extended), offset, extra));
    assert(extra == optional_extension);
    std::puts("PASS: callback ABI v1");
}
