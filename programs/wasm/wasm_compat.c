/* Compatibility stubs for POSIX thread-scheduling functions that Emscripten's
 * single-threaded libc declares but does not implement. chdb's WASM build is
 * single-threaded, so thread priorities/scheduling are meaningless here.
 *
 * These are weak so that a future Emscripten which provides real
 * implementations transparently overrides them. Poco's ThreadImpl
 * (base/poco/Foundation/src/Thread_POSIX.cpp) references all three. */

#include <sched.h>
#include <pthread.h>

__attribute__((weak)) int sched_get_priority_min(int policy)
{
    (void)policy;
    return 0;
}

__attribute__((weak)) int sched_get_priority_max(int policy)
{
    (void)policy;
    return 0;
}

__attribute__((weak)) int pthread_setschedparam(pthread_t thread, int policy, const struct sched_param * param)
{
    (void)thread;
    (void)policy;
    (void)param;
    return 0;
}
