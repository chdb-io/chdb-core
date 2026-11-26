/// chdb_spec: the C++ allocation functions of the pybind11 stub and shim DLLs on Windows.
///
/// _chdb.pyd replaces operator new/delete (src/Common/AllocationInterceptors.cpp) with
/// functions that allocate through base/alignedAlloc.h, i.e. mingw-w64's __mingw_aligned_*,
/// and those blocks must be released with __mingw_aligned_free. Without this file the stub and
/// shim DLLs would get libc++'s default operators, which use malloc/free. Memory crosses the DLL
/// boundary in both directions - pybind11 hands std::string, std::vector and function records
/// between the module and the shim, and the stubs fill a vector the module destroys - so a block
/// allocated on one side and freed on the other would corrupt the heap. Every DLL statically
/// links its own copy of mingwex, but they all sit on the same UCRT heap with the same block
/// layout, so defining the same operators here makes the two sides interchangeable.
///
/// No sized delete: clang does not enable sized deallocation for Windows targets, see
/// src/Common/AllocationInterceptors.cpp.

#include <base/alignedAlloc.h>

#include <algorithm>
#include <cstddef>
#include <new>

namespace
{

size_t effectiveAlignment(std::align_val_t align)
{
    return std::max(static_cast<size_t>(align), sizeof(void *));
}

void * allocateOrNull(std::size_t size)
{
    return DB::alignedMalloc(size);
}

void * allocateAlignedOrNull(std::size_t size, std::align_val_t align)
{
    return DB::alignedAlloc(effectiveAlignment(align), size);
}

void * allocate(std::size_t size)
{
    if (void * ptr = allocateOrNull(size))
        return ptr;
    throw std::bad_alloc();
}

void * allocateAligned(std::size_t size, std::align_val_t align)
{
    if (void * ptr = allocateAlignedOrNull(size, align))
        return ptr;
    throw std::bad_alloc();
}

}

void * operator new(std::size_t size) { return allocate(size); }
void * operator new[](std::size_t size) { return allocate(size); }
void * operator new(std::size_t size, std::align_val_t align) { return allocateAligned(size, align); }
void * operator new[](std::size_t size, std::align_val_t align) { return allocateAligned(size, align); }

void * operator new(std::size_t size, const std::nothrow_t &) noexcept { return allocateOrNull(size); }
void * operator new[](std::size_t size, const std::nothrow_t &) noexcept { return allocateOrNull(size); }
void * operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    return allocateAlignedOrNull(size, align);
}
void * operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    return allocateAlignedOrNull(size, align);
}

void operator delete(void * ptr) noexcept { DB::alignedFree(ptr); }
void operator delete[](void * ptr) noexcept { DB::alignedFree(ptr); }
void operator delete(void * ptr, std::align_val_t) noexcept { DB::alignedFree(ptr); }
void operator delete[](void * ptr, std::align_val_t) noexcept { DB::alignedFree(ptr); }
void operator delete(void * ptr, const std::nothrow_t &) noexcept { DB::alignedFree(ptr); }
void operator delete[](void * ptr, const std::nothrow_t &) noexcept { DB::alignedFree(ptr); }
void operator delete(void * ptr, std::align_val_t, const std::nothrow_t &) noexcept { DB::alignedFree(ptr); }
void operator delete[](void * ptr, std::align_val_t, const std::nothrow_t &) noexcept { DB::alignedFree(ptr); }
