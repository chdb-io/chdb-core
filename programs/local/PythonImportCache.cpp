#include "PythonImportCache.h"
#include "PythonImporter.h"

#include <Common/Exception.h>
#include <stack>

#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace CHDB {

py::handle PythonImportCacheItem::operator()(bool load) {
#ifndef CHDB_FREE_THREADING
	// On stock (GIL-bearing) builds the GIL serializes this read against the
	// call_once initializer's write to `object`, so the unsynchronized
	// pre-check is a safe fast path that avoids rebuilding the hierarchy
	// stack and re-entering PythonImporter::Import on every steady-state
	// attribute access.
	if (IsLoaded())
		return object;
#else
	// Free-threaded: `loaded` is set (release) only after the call_once
	// initializer has published `object`, so an acquire read here makes the
	// plain read of `object` well-defined and gives steady-state accesses a
	// lock-free fast path that skips the hierarchy rebuild below.
	if (loaded.load(std::memory_order_acquire))
		return object;
#endif
	std::stack<PythonImportCacheItem *> hierarchy;

	PythonImportCacheItem * item = this;
	while (item)
	{
		hierarchy.emplace(item);
		item = item->parent;
	}

	return PythonImporter::Import(hierarchy, load);
}

bool PythonImportCacheItem::LoadSucceeded() const
{
	return load_succeeded;
}

bool PythonImportCacheItem::IsLoaded() const
{
	return object.ptr() != nullptr;
}

py::handle PythonImportCacheItem::AddCache(PythonImportCache & cache, py::object object)
{
	return cache.AddCache(std::move(object));
}

void PythonImportCacheItem::LoadModule(PythonImportCache & cache)
{
#if USE_JEMALLOC
	::Memory::MemoryCheckScope memory_check_scope;
#endif
	try
	{
		py::gil_assert();
		object = AddCache(cache, std::move(py::module::import(name.c_str())));
		load_succeeded = true;
	}
	catch (py::error_already_set &e)
	{
		if (IsRequired())
		{
#if USE_JEMALLOC
			::Memory::MemoryCheckScope memory_check_scope;
#endif
			throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
			    				"Required module {} failed to import, due to the following Python exception:\n {}", name, e.what());
		}
		object = nullptr;
		return;
	}
}

void PythonImportCacheItem::LoadAttribute(PythonImportCache & cache, py::handle source)
{
#if USE_JEMALLOC
	::Memory::MemoryCheckScope memory_check_scope;
#endif
	if (py::hasattr(source, name.c_str()))
		object = AddCache(cache, std::move(source.attr(name.c_str())));
	else
		object = nullptr;
}

py::handle PythonImportCacheItem::Load(PythonImportCache & cache, py::handle source, bool load)
{
#ifndef CHDB_FREE_THREADING
	// Stock (GIL-bearing) builds: the GIL already serializes all callers, so a
	// direct call is correct and avoids a std::call_once + GIL deadlock window.
	// LoadModule() invokes py::module::import(), and CPython's import machinery
	// may release the GIL internally (per-module import lock, file I/O,
	// arbitrary module top-level code). If one thread were stuck inside
	// std::call_once while another acquired the GIL and reached the same
	// call_once, the second thread would block in call_once *while holding the
	// GIL*, leaving no thread able to re-acquire the GIL and finish the
	// initializer. Calling LoadModule()/LoadAttribute() directly under the GIL
	// avoids that window entirely.
	if (IsLoaded())
		return object;
	if (!load)
		return object;
	if (is_module)
		LoadModule(cache);
	else
		LoadAttribute(cache, source);
	return object;
#else
	// Free-threaded: no GIL to serialize callers, so use std::call_once for the
	// one-shot initialization — but the wait must happen DETACHED. LoadModule()
	// runs the import machinery (Python code: allocations, safepoints), and an
	// allocation-triggered GC on any thread issues a stop-the-world that only
	// completes once every attached thread parks at a safepoint. A thread
	// blocked on the once_flag futex while attached never reaches a safepoint,
	// so the stop-the-world never completes, the winner parks forever at the
	// next safepoint inside the import, and the flag is never released — a
	// process-wide deadlock (issue #131: cold process, first parallel UDF
	// query). Detaching the waiters lets the stop-the-world drain, the winner's
	// import finish, and everyone proceed.
	//
	// The initializer's write to `object` happens-before every passive
	// call_once on the same flag; `loaded`, released at the end of the
	// initializer itself, additionally publishes it to the lock-free fast
	// paths with no window between the load completing and the flag being
	// visible (a throwing initializer leaves it false, so retry semantics
	// are unchanged).
	if (!load)
		return loaded.load(std::memory_order_acquire) ? object : py::handle(nullptr);

	if (!loaded.load(std::memory_order_acquire))
	{
		py::gil_scoped_release detach_while_waiting;
		std::call_once(load_flag, [&]() {
			py::gil_scoped_acquire attach_for_import;
			if (is_module)
				LoadModule(cache);
			else
				LoadAttribute(cache, source);
			loaded.store(true, std::memory_order_release);
		});
	}

	return object;
#endif
}

PythonImportCache::~PythonImportCache()
{
	try
	{
		py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
		::Memory::MemoryCheckScope memory_check_scope;
#endif
		owned_objects.clear();
	}
	catch (...)
	{
	}
}

py::handle PythonImportCache::AddCache(py::object item)
{
	auto object_ptr = item.ptr();
	std::lock_guard<std::mutex> lock(cache_mutex);
	owned_objects.push_back(std::move(item));
	return object_ptr;
}

} // namespace CHDB
