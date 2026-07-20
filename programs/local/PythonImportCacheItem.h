#pragma once

#include "PybindWrapper.h"

#include <base/types.h>
#include <atomic>
#include <mutex>

namespace CHDB {

template <typename T>
static bool ModuleIsLoaded()
{
	auto dict = pybind11::module_::import("sys").attr("modules");
	return dict.contains(py::str(T::Name));
}

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	PythonImportCacheItem(const String & name, PythonImportCacheItem * parent_)
	    : name(name), is_module(false), load_succeeded(false), parent(parent_), object(nullptr)
	{
	}

	PythonImportCacheItem(const String & name)
	    : name(name), is_module(true), load_succeeded(false), parent(nullptr), object(nullptr)
	{
	}

	virtual ~PythonImportCacheItem() = default;

public:
	bool LoadSucceeded() const;
	bool IsLoaded() const;
	py::handle operator()(bool load = true);
	py::handle Load(PythonImportCache & cache, py::handle source, bool load);

protected:
	virtual bool IsRequired() const
	{
		return true;
	}

private:
	py::handle AddCache(PythonImportCache & cache, py::object object);
	void LoadAttribute(PythonImportCache & cache, py::handle source);
	void LoadModule(PythonImportCache & cache);

private:
	String name;
	bool is_module;
	bool load_succeeded;
	PythonImportCacheItem * parent;
	py::handle object;
	std::once_flag load_flag;
	/// Free-threaded builds only: set (release) after the call_once initializer
	/// has published `object`, giving steady-state accesses a lock-free fast
	/// path that skips the hierarchy rebuild and the call_once entirely.
	std::atomic<bool> loaded{false};
};

} // namespace CHDB
