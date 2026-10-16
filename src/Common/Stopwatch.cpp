#include <Common/Stopwatch.h>

#include <base/defines.h>

#include <cerrno>
#include <system_error>


/// Out of line and uninstrumented: `clock_gettime` needs a `timespec` address, and inlining that
/// buffer would put a `-fstack-protector-strong` canary on every caller.
NO_STACK_PROTECTOR NO_INLINE UInt64 clock_gettime_ns(clockid_t clock_type)
{
    struct timespec ts{};
#if defined(OS_WASM)
    /// chdb: Emscripten's clock_gettime supports only a subset of clock ids (essentially
    /// CLOCK_REALTIME / CLOCK_MONOTONIC) and returns EINVAL for the others, e.g.
    /// CLOCK_MONOTONIC_RAW and CLOCK_MONOTONIC_COARSE. Fall back to a supported
    /// clock instead of throwing so the timing-heavy code paths work on WASM.
    if (0 != clock_gettime(clock_type, &ts)
        && 0 != clock_gettime(CLOCK_MONOTONIC, &ts)
        && 0 != clock_gettime(CLOCK_REALTIME, &ts))
        throw std::system_error(std::error_code(errno, std::system_category()));
#else
    if (0 != clock_gettime(clock_type, &ts))
        throw std::system_error(std::error_code(errno, std::system_category()));
#endif
    return UInt64(ts.tv_sec * 1000000000LL + ts.tv_nsec);
}
