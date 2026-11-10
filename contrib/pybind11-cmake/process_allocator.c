/// chdb_spec: malloc and friends for the pybind11 helper libraries on glibc.
///
/// chdb/__init__.py loads _chdb.abi3.so, which needs the stubs library, with RTLD_DEEPBIND, and the
/// stubs library loads libpybind11nonlimitedapi_chdb_3.x.so with RTLD_DEEPBIND, so that operator new
/// and delete in both bind to _chdb's jemalloc. DEEPBIND searches a library's own dependencies before
/// the global scope for every symbol, so malloc and free bind to glibc's even when the process
/// allocator is another one (jemalloc linked into the interpreter or LD_PRELOAD'ed). These libraries
/// free memory from the process allocator, such as the strings pybind11 copies with strdup(), and
/// glibc's free aborts on it.
///
/// These hidden definitions take every function from the main program's handle, which searches the
/// global scope, the same scope that resolves glibc's own calls and Python's. In a dlmopen() namespace,
/// which has a libc of its own that dlopen(NULL) does not return, they keep what RTLD_NEXT finds.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <stddef.h>

#define HIDDEN __attribute__((visibility("hidden")))

HIDDEN void * malloc(size_t size);
HIDDEN void * calloc(size_t nmemb, size_t size);
HIDDEN void * realloc(void * ptr, size_t size);
HIDDEN void free(void * ptr);
HIDDEN void * aligned_alloc(size_t alignment, size_t size);
HIDDEN int posix_memalign(void ** memptr, size_t alignment, size_t size);
HIDDEN void * memalign(size_t alignment, size_t size);
HIDDEN void * valloc(size_t size);
HIDDEN size_t malloc_usable_size(void * ptr);

static int in_default_namespace(void)
{
    Dl_info info;
    void * self = NULL; /// glibc's dlopen handles are link_map pointers
    Lmid_t namespace_id = LM_ID_BASE;
    if (dladdr1((void *)&in_default_namespace, &info, &self, RTLD_DL_LINKMAP) && self)
        dlinfo(self, RTLD_DI_LMID, &namespace_id);
    return namespace_id == LM_ID_BASE;
}

static void * resolve(const char * name)
{
    void * function = NULL;
    if (in_default_namespace())
    {
        void * main_program = dlopen(NULL, RTLD_LAZY);
        if (main_program)
            function = dlsym(main_program, name);
    }
    if (!function)
        function = dlsym(RTLD_NEXT, name);
    if (!function)
        __builtin_trap();
    return function;
}

/// Racing threads resolve the same address, so a relaxed cache is enough.
#define PROCESS_FUNCTION(name) \
    static __typeof__(&name) process_##name(void) \
    { \
        static __typeof__(&name) cached; \
        __typeof__(&name) function = __atomic_load_n(&cached, __ATOMIC_RELAXED); \
        if (!function) \
        { \
            function = (__typeof__(&name))resolve(#name); \
            __atomic_store_n(&cached, function, __ATOMIC_RELAXED); \
        } \
        return function; \
    }

PROCESS_FUNCTION(malloc)
PROCESS_FUNCTION(calloc)
PROCESS_FUNCTION(realloc)
PROCESS_FUNCTION(free)
PROCESS_FUNCTION(aligned_alloc)
PROCESS_FUNCTION(posix_memalign)
PROCESS_FUNCTION(memalign)
PROCESS_FUNCTION(valloc)
PROCESS_FUNCTION(malloc_usable_size)

void * malloc(size_t size)
{
    return process_malloc()(size);
}

void * calloc(size_t nmemb, size_t size)
{
    return process_calloc()(nmemb, size);
}

void * realloc(void * ptr, size_t size)
{
    return process_realloc()(ptr, size);
}

void free(void * ptr)
{
    process_free()(ptr);
}

void * aligned_alloc(size_t alignment, size_t size)
{
    return process_aligned_alloc()(alignment, size);
}

int posix_memalign(void ** memptr, size_t alignment, size_t size)
{
    return process_posix_memalign()(memptr, alignment, size);
}

void * memalign(size_t alignment, size_t size)
{
    return process_memalign()(alignment, size);
}

void * valloc(size_t size)
{
    return process_valloc()(size);
}

size_t malloc_usable_size(void * ptr)
{
    return process_malloc_usable_size()(ptr);
}
