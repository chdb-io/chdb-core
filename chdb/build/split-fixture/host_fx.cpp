/* The host of the split_static_lib.py fixture (test_split_static_lib.sh). It links the split
 * fixture library without --allow-multiple-definition and defines names the library also
 * uses internally - helper(), fx_policy(), the optional hook fx_hook(), the inline counter -
 * with values of its own. Every check goes both ways: the library's calls reach the
 * library's definitions, and the host's calls reach the host's. */
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <thread>

#include "fx.h"
#include "fx_counter.hpp"

extern "C" int helper(void) { return 2002; }
extern "C" int fx_policy(void) { return 3; }

static int hook_calls = 0;
extern "C" void fx_hook(int) { ++hook_calls; }

static int failures = 0;

static void check(const char * what, long got, long expected)
{
    if (got != expected)
    {
        std::fprintf(stderr, "host_fx: %s: got %ld, expected %ld\n", what, got, expected);
        ++failures;
    }
}

static void check_string(const char * what, const char * got, const char * expected)
{
    if (!got || std::strcmp(got, expected) != 0)
    {
        std::fprintf(stderr, "host_fx: %s: got \"%s\", expected \"%s\"\n", what, got ? got : "(null)", expected);
        ++failures;
    }
}

[[gnu::noinline]] static void host_throw(int depth)
{
    if (depth == 0)
        throw std::runtime_error("host");
    host_throw(depth - 1);
}

int main()
{
    /* 5: a symbol shared between shards, a symbol local to one, and the host's same name. */
    check("the library's helper(), from another shard", fx_call_helper(), 1001);
    check("the host's helper()", helper(), 2002);
    check("a function local to its shard", fx_a_entry(), 7);

    /* 6A: the strong definition over the weak default, from either shard; the host's own. */
    check("fx_policy() from a third shard", fx_get_policy(), 2);
    check("fx_policy() from the weak default's shard", fx_get_policy_same_shard(), 2);
    check("the host's fx_policy()", fx_policy(), 3);

    /* 6B: one counter inside the library, another in the host. */
    check("library counter, first unit", fx_bump_1(), 1);
    check("library counter, second unit", fx_bump_2(), 2);
    check("host counter", fx_bump(), 1);
    check("library counter after the host's", fx_bump_1(), 3);
    check("host counter after the library's", fx_bump(), 2);

    /* 6C: the optional hook is not the host's fx_hook(); the C library's weak one bound. */
    check("the library's optional hook", fx_fire(5), 0);
    check("calls of the host's fx_hook() by the library", hook_calls, 0);
    fx_hook(1);
    check("the host's fx_hook() called by the host", hook_calls, 1);
    check("the weak reference into the C library", fx_has_libc_weak(), 1);

    /* 8: a constructor, strings with addends, a thread_local, exceptions. */
    check("state set by another shard's constructor", fx_state(), 42);
    check_string("a pointer into a string of another shard", fx_beta(), "beta-gamma");
    check_string("a string with an addend in code", fx_code_string(), "hello");
    check("thread_local in the main thread", fx_tls_bump(), 6);
    int first = 0, second = 0;
    std::thread([&] { first = fx_tls_bump(); second = fx_tls_bump(); }).join();
    check("thread_local in a new thread, first", first, 6);
    check("thread_local in a new thread, second", second, 7);
    check("thread_local in the main thread again", fx_tls_bump(), 7);
    check("an exception thrown and caught inside the library, frames unwound", fx_catch(), 704);
    bool caught = false;
    try
    {
        host_throw(3);
    }
    catch (const std::runtime_error & e)
    {
        caught = std::strcmp(e.what(), "host") == 0;
    }
    check("the host's own exception after the library's", caught, 1);
    check("the library's exception again", fx_catch(), 704);

    if (failures)
    {
        std::fprintf(stderr, "host_fx: %d checks failed\n", failures);
        return 1;
    }
    std::printf("host_fx: all checks passed\n");
    return 0;
}
