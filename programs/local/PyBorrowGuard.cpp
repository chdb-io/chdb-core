#include "PyBorrowGuard.h"
#include "PybindWrapper.h"

#include <cstdlib>
#include <mutex>
#include <vector>

namespace CHDB
{

static std::mutex pending_decrefs_mutex;
static std::vector<PyObject *> pending_decrefs;

bool zeroCopyEnabled()
{
    static const bool enabled = []
    {
        const char * env = getenv("CHDB_ZERO_COPY"); // NOLINT(concurrency-mt-unsafe)
        return !(env && env[0] == '0');
    }();
    return enabled;
}

std::shared_ptr<void> makePyBorrowGuard(PyObject * obj)
{
    py::gil_assert();
    Py_INCREF(obj);
    return {static_cast<void *>(obj), [](void * ptr)
    {
        std::lock_guard lock(pending_decrefs_mutex);
        pending_decrefs.push_back(static_cast<PyObject *>(ptr));
    }};
}

void drainPyBorrowGuardQueue()
{
    std::vector<PyObject *> drained;
    {
        std::lock_guard lock(pending_decrefs_mutex);
        if (pending_decrefs.empty())
            return;
        drained.swap(pending_decrefs);
    }
    for (auto * obj : drained)
        Py_DECREF(obj);
}

}
