/* Shard 6: an optional hook nothing in the library defines, and a weak reference into the C
 * library. The hook must not reach the host's fx_hook(); the C library's must still bind. */
#include <stddef.h>

#include "fx.h"

extern void fx_hook(int value) __attribute__((weak));
/* glibc's, weakly referenced by libstdc++ and libc++abi alike. */
extern int __cxa_thread_atexit_impl(void (*)(void *), void *, void *) __attribute__((weak));

int fx_fire(int value)
{
    if (fx_hook)
    {
        fx_hook(value);
        return 1;
    }
    return 0;
}

int fx_has_libc_weak(void) { return __cxa_thread_atexit_impl != NULL; }
