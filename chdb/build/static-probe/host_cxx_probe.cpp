// Host-runtime consumer probe, C++ half (chdb-io/chdb-rust#53).
//
// A C++ program built against the host's libstdc++ that links libchdb.a must keep its own
// exception handling: __cxa_throw, the personality routine, the std::exception typeinfo and
// operator new all have to stay the host's. When the static link bound them to the LLVM
// libc++abi bundled in libchdb.a instead, this program's own throw/catch corrupted its heap.
// Rust's panics take the same _Unwind_RaiseException + personality route.
//
// It talks to chDB through chdb.hpp, which ships in the static tarball, so the header is
// covered too: it must need nothing but the C API from the archive.
//
// Linked the way a consumer links: no -rdynamic, no --allow-multiple-definition.

#include <cstdint>
#include <cstdio>
#include <exception>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "chdb.hpp"

namespace
{

int destroyed = 0;

struct Guard
{
    ~Guard() { ++destroyed; }
};

struct ProbeError : std::runtime_error
{
    explicit ProbeError(int code_) : std::runtime_error("probe"), code(code_) {}
    int code;
};

[[gnu::noinline]] void throwAfter(int depth)
{
    Guard guard;
    std::vector<std::string> allocations(static_cast<size_t>(depth) + 1, std::string(64, 'x'));
    if (depth == 0)
        throw ProbeError(42);
    throwAfter(depth - 1);
}

bool fail(const char * when, const char * what)
{
    std::fprintf(stderr, "host_cxx_probe (%s): %s\n", when, what);
    return false;
}

/// The host's own exceptions, thrown by its code and by libstdc++, caught by its code.
bool hostExceptionsWork(const char * when)
{
    destroyed = 0;
    try
    {
        throwAfter(3);
        return fail(when, "nothing was thrown");
    }
    catch (const std::exception & e)
    {
        const auto * probe = dynamic_cast<const ProbeError *>(&e);
        if (!probe || probe->code != 42 || destroyed != 4)
            return fail(when, "a derived exception was not caught intact through four cleaned-up frames");
    }

    try
    {
        throw 7;
    }
    catch (int value)
    {
        if (value != 7)
            return fail(when, "a thrown int changed value");
    }

    try
    {
        std::vector<int> empty;
        (void)empty.at(1); /// thrown inside libstdc++
        return fail(when, "vector::at did not throw");
    }
    catch (const std::out_of_range &)
    {
    }

    try
    {
        (void)::operator new(SIZE_MAX); /// thrown inside libstdc++'s operator new
        return fail(when, "operator new(SIZE_MAX) did not throw");
    }
    catch (const std::bad_alloc &)
    {
    }

    std::exception_ptr saved;
    try
    {
        throw std::string("again");
    }
    catch (...)
    {
        saved = std::current_exception();
    }
    try
    {
        std::rethrow_exception(saved);
    }
    catch (const std::string & value)
    {
        if (value == "again")
            return true;
    }
    catch (...)
    {
    }
    return fail(when, "a rethrown std::string was not caught as std::string");
}

}

int main()
{
    chdb_set_signal_handlers_enabled(0);

    if (!hostExceptionsWork("before connecting"))
        return 1;

    try
    {
        CHDB::Connection conn;

        try
        {
            conn.query("SELECT * FROM host_cxx_probe_missing_table", "CSV").throw_if_error();
            std::fprintf(stderr, "host_cxx_probe: the failing query did not report an error\n");
            return 1;
        }
        catch (const CHDB::ChdbError &)
        {
            /// chDB threw and caught its own exception, then chdb.hpp threw this one in host code.
        }

        auto result = conn.query("SELECT 40 + 2", "CSV");
        result.throw_if_error();
        if (result.str().rfind("42", 0) != 0)
        {
            std::fprintf(stderr, "host_cxx_probe: expected 42, got '%s'\n", result.str().c_str());
            return 1;
        }

        if (!hostExceptionsWork("with a connection open"))
            return 1;
    }
    catch (const std::exception & e)
    {
        std::fprintf(stderr, "host_cxx_probe: unexpected exception: %s\n", e.what());
        return 1;
    }

    if (!hostExceptionsWork("after closing the connection"))
        return 1;

    std::printf("host_cxx_probe: host exceptions work before, during and after chDB queries\n");
    return 0;
}
