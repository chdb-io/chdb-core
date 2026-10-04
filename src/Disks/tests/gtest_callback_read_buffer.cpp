#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/ReadBufferFromCallback.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The host side of the callback table: blobs in a map, reached through C-signature functions.
using MapStore = std::map<String, std::string>;

int storeMetadata(void * ud, const char * key, int * found, uint64_t * size, int64_t * mtime)
{
    const auto & store = *static_cast<MapStore *>(ud);
    auto it = store.find(key);
    *found = it != store.end();
    if (*found)
    {
        *size = it->second.size();
        *mtime = 0;
    }
    return 0;
}

int storeRead(void * ud, const char * key, uint64_t offset, void * buf, size_t len, size_t * out)
{
    const auto & store = *static_cast<MapStore *>(ud);
    auto it = store.find(key);
    if (it == store.end())
        return 1;
    const auto & data = it->second;
    *out = offset >= data.size() ? 0 : std::min(len, data.size() - offset);
    memcpy(buf, data.data() + offset, *out);
    return 0;
}

CallbackObjectStorageOpsPtr makeOps(MapStore & store)
{
    auto ops = std::make_shared<CallbackObjectStorageOps>();
    ops->name = "gtest";
    ops->ud = &store;
    ops->metadata = storeMetadata;
    ops->read = storeRead;
    return ops;
}

std::string pattern(size_t size)
{
    std::string data(size, 0);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>('a' + i % 26);
    return data;
}

std::string readAll(ReadBuffer & in)
{
    std::string out;
    while (!in.eof())
    {
        out.append(in.position(), in.available());
        in.position() = in.buffer().end();
    }
    return out;
}

/// Consumes the current window and returns a copy of it.
std::string takeWindow(ReadBuffer & in)
{
    std::string out(in.position(), in.available());
    in.position() = in.buffer().end();
    return out;
}

}

/// After eof() the base class leaves working_buffer as an empty window at the old end; the next
/// refill must expose the bytes it read into internal_buffer, not that stale position.
TEST(ReadBufferFromCallback, ReadToEofSeekBackReread)
{
    for (size_t object_size : {size_t(64), size_t(32)})
    {
        MapStore store;
        store["blob"] = pattern(object_size);
        ReadBufferFromCallback in(makeOps(store), "blob", /*buf_size=*/64, /*use_external_buffer_=*/false, /*file_size_=*/std::nullopt);

        EXPECT_EQ(readAll(in), store["blob"]);
        EXPECT_TRUE(in.eof());
        EXPECT_EQ(in.seek(0, SEEK_SET), 0);
        EXPECT_EQ(readAll(in), store["blob"]);
    }
}

/// With an external buffer the memory is a window inside the caller's allocation, as the gather's
/// SwapHelper hands it over: every refill must land inside that window.
TEST(ReadBufferFromCallback, ExternalWindowRefill)
{
    MapStore store;
    store["blob"] = pattern(100);
    ReadBufferFromCallback in(makeOps(store), "blob", /*buf_size=*/0, /*use_external_buffer_=*/true, /*file_size_=*/std::nullopt);

    std::vector<char> memory(256, '#');
    char * window = memory.data() + 100;
    in.set(window, 40);

    in.seek(10, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_EQ(takeWindow(in), store["blob"].substr(10, 40));

    in.seek(90, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_EQ(takeWindow(in), store["blob"].substr(90, 10));
    EXPECT_TRUE(in.eof());

    in.seek(0, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_GE(in.position(), window);
    EXPECT_LE(in.buffer().end(), window + 40);
    EXPECT_EQ(takeWindow(in), store["blob"].substr(0, 40));
}
