#include "ForkGuard.h"

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__) && !defined(__wasi__) && !defined(OS_WASM)
#    include <atomic>
#    include <mutex>
#    include <pthread.h>
#    include <unistd.h>
#endif

namespace CHDB
{

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__) && !defined(__wasi__) && !defined(OS_WASM)
namespace
{

std::atomic<pid_t> g_engine_init_pid{0};
std::atomic<bool> g_forked_after_init{false};
std::once_flag g_atfork_registered;

/// Strictly async-signal-safe: only lock-free atomic load/store, zero locks,
/// zero heap allocations, zero logging.
void onForkInChild() noexcept
{
    if (g_engine_init_pid.load(std::memory_order_acquire) != 0)
        g_forked_after_init.store(true, std::memory_order_release);
}

}

void markEngineInitialized() noexcept
{
    const pid_t current_pid = ::getpid();
    pid_t expected = 0;
    g_engine_init_pid.compare_exchange_strong(expected, current_pid, std::memory_order_acq_rel);

    std::call_once(g_atfork_registered, []() noexcept
    {
        (void)::pthread_atfork(nullptr, nullptr, &onForkInChild);
    });
}

bool isForkedAfterEngineInit() noexcept
{
    if (g_forked_after_init.load(std::memory_order_acquire))
        return true;

    const pid_t init_pid = g_engine_init_pid.load(std::memory_order_acquire);
    if (init_pid != 0 && ::getpid() != init_pid)
    {
        g_forked_after_init.store(true, std::memory_order_release);
        return true;
    }

    return false;
}
#else
void markEngineInitialized() noexcept
{
}

bool isForkedAfterEngineInit() noexcept
{
    return false;
}
#endif

const char * forkedAfterEngineInitError() noexcept
{
    return "chDB cannot be used in a forked child process because the embedded ClickHouse engine "
           "was already initialized in the parent process before fork(), which invalidates "
           "background threads and internal locks. Use multiprocessing.get_context(\"spawn\") "
           "or \"forkserver\", or initialize chDB only after fork().";
}

}
