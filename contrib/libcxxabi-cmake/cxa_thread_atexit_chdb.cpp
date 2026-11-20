/// chdb: libc++abi's cxa_thread_atexit.cpp, compiled unchanged except for where the secret that
/// mangles the thread_local destructor pointers comes from on Linux.
///
/// It copies the secret from the 16 bytes AT_RANDOM points to and aborts if they are zero. That
/// holds in an executable, but chDB is loaded into processes it does not own, and the host may
/// have consumed and cleared those bytes: the Go 1.22 runtime zeroes them once it has seeded its
/// own generator (runtime/rand.go), so the first thread_local with a destructor aborted
/// chdb_connect in every cgo program built with it ("cannot initialize the secret for
/// thread_local destructors"). Here the secret falls back to getrandom(2) when the bytes are gone.

#if defined(__linux__)

#include <string.h>
#include <sys/auxv.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace
{
unsigned long getauxvalForDtorSecret(unsigned long type)
{
    const unsigned long value = getauxval(type);
    if (type != AT_RANDOM)
        return value;

    static const unsigned char cleared[16] = {};
    if (value != 0 && memcmp(reinterpret_cast<const void *>(value), cleared, sizeof(cleared)) != 0)
        return value;

    /// Called once, from the constructor of a function-local static. A failure returns 0, on which
    /// libc++abi aborts as it did before.
    static unsigned char random_bytes[16];
    if (syscall(SYS_getrandom, random_bytes, sizeof(random_bytes), 0) != static_cast<long>(sizeof(random_bytes)))
        return 0;
    return reinterpret_cast<unsigned long>(random_bytes);
}
}

/// <sys/auxv.h> is already included, so the declaration it holds keeps its name.
#define getauxval getauxvalForDtorSecret

#endif

#include "../llvm-project/libcxxabi/src/cxa_thread_atexit.cpp"
