#pragma once

#include <memory>

typedef struct _object PyObject; /// NOLINT(modernize-use-using)

namespace CHDB
{

/// Whether zero-copy mounting of Python buffers into ClickHouse columns is
/// enabled (CHDB_ZERO_COPY=0 disables it).
bool zeroCopyEnabled();

/// Take a strong reference on `obj` (GIL must be held) and wrap it in a
/// shared_ptr guard. The deleter never touches the GIL: it enqueues the
/// pointer for a deferred decref, drained at the next query start / cache
/// clear while the GIL is held. This makes guard release safe from any
/// executor thread and structurally free of GIL deadlocks.
std::shared_ptr<void> makePyBorrowGuard(PyObject * obj);

/// Decref all queued objects. GIL must be held.
void drainPyBorrowGuardQueue();

/// 63 bytes past a mounted buffer's end must be readable (PaddedPODArray
/// read-overflow contract). Same-page check: O(1) and conservative; a false
/// negative just falls back to copying.
inline bool borrowTailReadable(const void * end)
{
    const auto addr = reinterpret_cast<unsigned long long>(end); /// NOLINT(google-runtime-int)
    return ((addr - 1) >> 12) == ((addr + 62) >> 12);
}

}
