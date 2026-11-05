#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageRegistry.h>
#include <Common/Exception.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

class TestHost final : public ICallbackObjectStorageHost
{
public:
    /// nullopt simulates a successful read that leaves count unset.
    std::optional<size_t> read_count = 0;
    Status read_status = Status::Ok;
    std::function<Status(const ListSink &)> listing;
    std::function<Status(const char *, bool, String &)> opening;
    std::function<void(const char *)> closing;

    Status metadata(const char *, CallbackObjectStat &, String &) override { return Status::NotFound; }
    Status read(const char *, uint64_t, char *, size_t, size_t & count, String &) override
    {
        if (read_count)
            count = *read_count;
        return read_status;
    }
    Status writeBegin(const char *, void *&, String &) override { return Status::Error; }
    Status writeAppend(void *, const char *, size_t, String &) override { return Status::Error; }
    Status writeCommit(void *, String &) override { return Status::Error; }
    void writeAbort(void *) override { }
    Status remove(const char *, String &) override { return Status::Ok; }
    Status list(const char *, const ListSink & sink, String &) override { return listing(sink); }
    bool hasCopy() const override { return false; }
    Status copy(const char *, const char *, String &) override { return Status::Error; }
    bool hasKeyspaceHooks() const override { return static_cast<bool>(opening); }
    Status openKeyspace(const char * prefix, bool read_only, String & error) override { return opening(prefix, read_only, error); }
    void closeKeyspace(const char * prefix) override { if (closing) closing(prefix); }
};

std::pair<CallbackObjectStorageOpsPtr, TestHost *> makeTestOps()
{
    auto host = std::make_unique<TestHost>();
    auto * raw = host.get();
    return {std::make_shared<CallbackObjectStorageOps>("test_store", std::move(host)), raw};
}

}

TEST(CallbackObjectStorageOps, ReadMustSetCount)
{
    auto [ops, host] = makeTestOps();
    char data[8];
    host->read_count.reset();
    try
    {
        ops->read("blob", 0, data, sizeof(data));
        FAIL() << "an unset byte count succeeded";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("did not set"), String::npos);
    }
    for (size_t count : {size_t(0), size_t(3), sizeof(data)})
    {
        host->read_count = count;
        EXPECT_EQ(ops->read("blob", 0, data, sizeof(data)), count);
    }
    host->read_count = sizeof(data) + 1;
    EXPECT_THROW(ops->read("blob", 0, data, sizeof(data)), Exception);
    host->read_count.reset();
    host->read_status = TestHost::Status::Error;
    EXPECT_THROW(ops->read("blob", 0, data, sizeof(data)), Exception);
}

TEST(CallbackObjectStorageOps, ListRejectsKeysOutsidePrefix)
{
    auto [ops, host] = makeTestOps();
    host->listing = [](const auto & sink)
    {
        sink("main/ok", 1, 1);
        sink("foreign/live", 1, 1);
        sink("main/later", 1, 1);
        return TestHost::Status::Ok;
    };
    std::vector<String> seen;
    EXPECT_THROW(ops->list("main/", [&](const char * key, uint64_t, int64_t)
    {
        seen.emplace_back(key);
        return false;
    }), Exception);
    EXPECT_EQ(seen, std::vector<String>{"main/ok"});
    seen.clear();
    ops->list("", [&](const char * key, uint64_t, int64_t)
    {
        seen.emplace_back(key);
        return false;
    });
    EXPECT_EQ(seen.size(), 3);
}

TEST(CallbackObjectStorageOps, ListStopsDeliveryWhenHostKeepsListing)
{
    auto [ops, host] = makeTestOps();
    size_t stop_replies = 0;
    host->listing = [&](const auto & sink)
    {
        for (size_t i = 0; i < 100; ++i)
            stop_replies += sink("main/blob", 1, 1);
        return TestHost::Status::Ok;
    };
    size_t delivered = 0;
    ops->list("main/", [&](const char *, uint64_t, int64_t) { return ++delivered == 3; });
    EXPECT_EQ(delivered, 3);
    EXPECT_EQ(stop_replies, 98);
}

TEST(CallbackObjectStorageOps, ParallelListHonorsStop)
{
    auto [ops, host] = makeTestOps();
    host->listing = [](const auto & sink)
    {
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 16; ++i)
            threads.emplace_back([&] { sink("main/blob", 1, 1); });
        for (auto & thread : threads)
            thread.join();
        return TestHost::Status::Ok;
    };
    size_t delivered = 0;
    ops->list("main/", [&](const char *, uint64_t, int64_t) { return ++delivered == 3; });
    EXPECT_EQ(delivered, 3);
}

TEST(CallbackObjectStorageOps, ListCapturesSinkErrors)
{
    auto [ops, host] = makeTestOps();
    host->listing = [](const auto & sink)
    {
        sink("main/blob", 1, 1);
        sink(nullptr, 1, 1);
        return TestHost::Status::Ok;
    };
    EXPECT_THROW(ops->list("main/", [](const char *, uint64_t, int64_t) -> bool
    {
        throw std::runtime_error("sink failed");
    }), std::runtime_error);
    EXPECT_THROW(ops->list("main/", [](const char *, uint64_t, int64_t) { return false; }), Exception);
}

TEST(CallbackObjectStorageOps, ReadersCanOverlapWriterButWritersDoNotOverlap)
{
    auto [ops, host] = makeTestOps();
    std::vector<bool> opened_read_only;
    host->opening = [&](const char *, bool read_only, String &)
    {
        opened_read_only.push_back(read_only);
        return TestHost::Status::Ok;
    };
    auto reader1 = ops->attachDisk("main", true);
    auto reader2 = ops->attachDisk("main/", true);
    auto writer = ops->attachDisk("main", false);
    EXPECT_THROW(ops->attachDisk("main/child", false), Exception);
    ops->detachDisk(reader1);
    ops->detachDisk(reader1);
    EXPECT_THROW(ops->attachDisk("main", false), Exception);
    auto reader3 = ops->attachDisk("main/child", true);
    ops->detachDisk(reader2);
    auto independent = ops->attachDisk("other", false);
    ops->detachDisk(independent);
    ops->detachDisk(writer);
    auto replacement = ops->attachDisk("main", false);
    ops->detachDisk(replacement);
    ops->detachDisk(reader3);
    EXPECT_FALSE(ops->hasAttachedDisks());
    EXPECT_EQ(opened_read_only, (std::vector<bool>{true, true, false, true, false, false}));
}

TEST(CallbackObjectStorageOps, FailedOpenReleasesClaim)
{
    auto [ops, host] = makeTestOps();
    size_t opens = 0;
    size_t closes = 0;
    host->opening = [&](const char *, bool, String &)
    {
        ++opens;
        EXPECT_TRUE(ops->hasAttachedDisks());
        return TestHost::Status::Error;
    };
    host->closing = [&](const char *) { ++closes; };
    EXPECT_THROW(ops->attachDisk("main/", false), Exception);
    EXPECT_FALSE(ops->hasAttachedDisks());
    EXPECT_EQ(closes, 0);
    host->opening = [&](const char *, bool, String &)
    {
        ++opens;
        return TestHost::Status::Ok;
    };
    auto writer = ops->attachDisk("main/", false);
    auto reader = ops->attachDisk("main", true);
    EXPECT_EQ(opens, 3);
    ops->detachDisk(reader);
    ops->detachDisk(writer);
    ops->detachDisk(writer);
    EXPECT_EQ(closes, 2);
}

TEST(CallbackObjectStorageOps, ClosingKeepsClaimUntilHostReturns)
{
    auto [ops, host] = makeTestOps();
    host->opening = [](const char *, bool, String &) { return TestHost::Status::Ok; };
    auto writer = ops->attachDisk("main", false);
    host->closing = [&](const char * prefix)
    {
        EXPECT_EQ(String(prefix), "main");
        EXPECT_THROW(ops->attachDisk("main", false), Exception);
    };
    ops->detachDisk(writer);
    EXPECT_FALSE(ops->hasAttachedDisks());
    host->closing = {};
    auto replacement = ops->attachDisk("main", false);
    ops->detachDisk(replacement);
}

TEST(CallbackObjectStorageOps, FencedStorageCannotOpenDisks)
{
    auto [ops, host] = makeTestOps();
    ops->fence();
    EXPECT_THROW(ops->attachDisk("main", true), Exception);
    EXPECT_THROW(ops->attachDisk("main", false), Exception);
    EXPECT_FALSE(ops->hasAttachedDisks());
}

TEST(CallbackObjectStorageOps, OpeningDiskBlocksReregistration)
{
    auto [ops, host] = makeTestOps();
    auto & registry = CallbackObjectStorageRegistry::instance();
    const String name = "opening_disk_test";
    registry.add(name, ops);
    std::promise<void> entered;
    std::promise<void> release;
    auto ready = entered.get_future();
    auto released = release.get_future().share();
    host->opening = [&](const char *, bool, String &)
    {
        entered.set_value();
        released.wait();
        return TestHost::Status::Ok;
    };
    auto attaching = std::async(std::launch::async, [&] { return ops->attachDisk("main", false); });
    ready.wait();
    EXPECT_TRUE(ops->hasAttachedDisks());
    auto removing = std::async(std::launch::async, [&] { registry.remove(name); });

    bool fenced = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    char data[1];
    while (std::chrono::steady_clock::now() < deadline)
    {
        try { ops->read("blob", 0, data, sizeof(data)); }
        catch (const Exception &) { fenced = true; break; }
        std::this_thread::yield();
    }
    EXPECT_TRUE(fenced);
    auto [replacement, replacement_host] = makeTestOps();
    EXPECT_THROW(registry.add(name, replacement), Exception);
    EXPECT_EQ(removing.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    release.set_value();
    EXPECT_THROW(attaching.get(), Exception);
    removing.get();
    EXPECT_FALSE(ops->hasAttachedDisks());
    registry.add(name, replacement);
    registry.remove(name);
}
