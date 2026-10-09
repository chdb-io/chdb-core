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

/// The host side: blobs in a map. Only metadata and read are reached by the read buffer.
class MapHost final : public ICallbackObjectStorageHost
{
public:
    std::map<String, std::string> blobs;
    /// When nonzero, every read returns at most this many bytes.
    size_t max_read = 0;

    Status metadata(const char * key, CallbackObjectStat & stat, String &) override
    {
        auto it = blobs.find(key);
        if (it == blobs.end())
            return Status::NotFound;
        stat.size = it->second.size();
        return Status::Ok;
    }

    Status read(const char * key, uint64_t offset, char * buf, size_t len, size_t & bytes_read, String & error) override
    {
        auto it = blobs.find(key);
        if (it == blobs.end())
        {
            error = "no such blob";
            return Status::NotFound;
        }
        const auto & data = it->second;
        bytes_read = offset >= data.size() ? 0 : std::min(len, data.size() - offset);
        if (max_read)
            bytes_read = std::min(bytes_read, max_read);
        memcpy(buf, data.data() + offset, bytes_read);
        return Status::Ok;
    }

    Status writeBegin(const char *, void *&, String &) override { return Status::Error; }
    Status writeAppend(void *, const char *, size_t, String &) override { return Status::Error; }
    Status writeCommit(void *, String &) override { return Status::Error; }
    void writeAbort(void *) override { }
    Status remove(const char *, String &) override { return Status::Error; }
    Status list(const char *, const ListSink &, String &) override { return Status::Error; }
    bool hasCopy() const override { return false; }
    Status copy(const char *, const char *, String &) override { return Status::Error; }
    bool hasKeyspaceHooks() const override { return false; }
    Status openKeyspace(const char *, bool, String &) override { return Status::Error; }
    void closeKeyspace(const char *) override { }
};

std::pair<CallbackObjectStorageOpsPtr, MapHost *> makeOps()
{
    auto host = std::make_unique<MapHost>();
    auto * raw = host.get();
    return {std::make_shared<CallbackObjectStorageOps>("gtest", std::move(host)), raw};
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
        auto [ops, host] = makeOps();
        host->blobs["blob"] = pattern(object_size);
        ReadBufferFromCallback in(ops, "blob", /*buf_size=*/64, /*use_external_buffer_=*/false, /*file_size_=*/std::nullopt);

        EXPECT_EQ(readAll(in), host->blobs["blob"]);
        EXPECT_TRUE(in.eof());
        EXPECT_EQ(in.seek(0, SEEK_SET), 0);
        EXPECT_EQ(readAll(in), host->blobs["blob"]);
    }
}

/// With an external buffer the memory is a window inside the caller's allocation, as the gather's
/// SwapHelper hands it over: every refill must land inside that window.
TEST(ReadBufferFromCallback, ExternalWindowRefill)
{
    auto [ops, host] = makeOps();
    host->blobs["blob"] = pattern(100);
    const auto & blob = host->blobs["blob"];
    ReadBufferFromCallback in(ops, "blob", /*buf_size=*/0, /*use_external_buffer_=*/true, /*file_size_=*/std::nullopt);

    std::vector<char> memory(256, '#');
    char * window = memory.data() + 100;
    in.set(window, 40);

    in.seek(10, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_EQ(takeWindow(in), blob.substr(10, 40));

    in.seek(90, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_EQ(takeWindow(in), blob.substr(90, 10));
    EXPECT_TRUE(in.eof());

    in.seek(0, SEEK_SET);
    ASSERT_TRUE(in.next());
    EXPECT_GE(in.position(), window);
    EXPECT_LE(in.buffer().end(), window + 40);
    EXPECT_EQ(takeWindow(in), blob.substr(0, 40));
}

/// A host may return less than asked; only a 0-byte read ends the blob.
TEST(ReadBufferFromCallback, ShortReadsAreNotEof)
{
    auto [ops, host] = makeOps();
    host->blobs["blob"] = pattern(1000);
    host->max_read = 7;
    ReadBufferFromCallback in(ops, "blob", /*buf_size=*/64, /*use_external_buffer_=*/false, /*file_size_=*/std::nullopt);

    EXPECT_EQ(readAll(in), host->blobs["blob"]);
}

/// One read-write disk per keyspace: equal, nested and empty prefixes overlap; trailing '/' is ignored.
/// A string prefix counts as nested too ('ab' after 'a'), as DiskSelector::recordDisk decides upstream.
TEST(CallbackObjectStorageOps, OverlappingKeyspacesAreRefused)
{
    auto [ops, host] = makeOps();
    ops->claimKeyspace("a");
    EXPECT_THROW(ops->claimKeyspace("a"), Exception);
    EXPECT_THROW(ops->claimKeyspace("a/"), Exception);
    EXPECT_THROW(ops->claimKeyspace("a/b"), Exception);
    EXPECT_THROW(ops->claimKeyspace("ab"), Exception);
    EXPECT_THROW(ops->claimKeyspace(""), Exception);
    ops->claimKeyspace("b");

    ops->releaseKeyspace("a/");
    ops->claimKeyspace("a/b");
    EXPECT_THROW(ops->claimKeyspace("a"), Exception);
}

/// After fence() no call reaches the host: a read fails with the dedicated error instead.
TEST(ReadBufferFromCallback, FencedStorageRefusesReads)
{
    auto [ops, host] = makeOps();
    host->blobs["blob"] = pattern(10);
    ReadBufferFromCallback in(ops, "blob", /*buf_size=*/64, /*use_external_buffer_=*/false, /*file_size_=*/std::nullopt);

    ops->fence();
    try
    {
        in.next();
        FAIL() << "a read on a fenced storage succeeded";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("unregistered"), std::string::npos) << e.message();
    }
}
