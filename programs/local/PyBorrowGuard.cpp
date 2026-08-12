#include "PyBorrowGuard.h"
#include "PybindWrapper.h"

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <vector>

namespace CHDB
{

/// Leaky singletons: guard deleters can run during static destruction (e.g. a
/// connection left open at process exit destroys columns whose deleters
/// enqueue here), so these must outlive every static destructor.
static std::mutex & pendingDecrefsMutex()
{
    static auto * mutex = new std::mutex;
    return *mutex;
}

static std::vector<PyObject *> & pendingDecrefs()
{
    static auto * pending = new std::vector<PyObject *>;
    return *pending;
}

static std::atomic_flag drain_scheduled = ATOMIC_FLAG_INIT;

bool zeroCopyEnabled()
{
    static const bool enabled = []
    {
        const char * env = getenv("CHDB_ZERO_COPY"); // NOLINT(concurrency-mt-unsafe)
        return !(env && env[0] == '0');
    }();
    return enabled;
}

static int drainPendingCallTrampoline(void *)
{
    drain_scheduled.clear(std::memory_order_release);
    drainPyBorrowGuardQueue();
    return 0;
}

std::shared_ptr<void> makePyBorrowGuard(PyObject * obj)
{
    py::gil_assert();
    Py_INCREF(obj);
    return {static_cast<void *>(obj), [](void * ptr)
    {
        {
            std::lock_guard lock(pendingDecrefsMutex());
            pendingDecrefs().push_back(static_cast<PyObject *>(ptr));
        }
        /// The deleter may run on a server thread with no GIL and no upcoming
        /// chdb API call (e.g. a table with borrowed columns dropped in the
        /// background). Schedule a drain on the interpreter's pending-call
        /// queue so the release point is bounded and query-independent;
        /// the drains at query boundaries remain as belt-and-braces.
        /// During/after finalization the pending-call machinery is gone:
        /// leave the queued refs intentionally leaked instead of crashing.
        if (!Py_IsInitialized())
            return;

        if (!drain_scheduled.test_and_set(std::memory_order_acq_rel))
        {
            if (Py_AddPendingCall(drainPendingCallTrampoline, nullptr) != 0)
                drain_scheduled.clear(std::memory_order_release);
        }
    }};
}

void drainPyBorrowGuardQueue()
{
    if (!Py_IsInitialized())
        return;

    std::vector<PyObject *> drained;
    {
        std::lock_guard lock(pendingDecrefsMutex());
        if (pendingDecrefs().empty())
            return;
        drained.swap(pendingDecrefs());
    }
    for (auto * obj : drained)
        Py_DECREF(obj);
}

}
