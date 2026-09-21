#pragma once

#include "DatetimeCacheItem.h"
#include "DecimalCacheItem.h"
#include "NumpyCacheItem.h"
#include "PandasCacheItem.h"
#include "PolarsCacheItem.h"
#include "PyArrowCacheItem.h"
#include "PythonImportCacheItem.h"
#include "UUIDCacheItem.h"
#include "IPAddressCacheItem.h"
#include "ZoneInfoCacheItem.h"

#include <memory>
#include <mutex>
#include <vector>

namespace CHDB {

struct PythonImportCache;
using PythonImportCachePtr = std::shared_ptr<PythonImportCache>;

struct PythonImportCache
{
public:
	explicit PythonImportCache()  = default;

	~PythonImportCache();

	PandasCacheItem pandas;
	PolarsCacheItem polars;
	PyarrowCacheItem pyarrow;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	NumpyCacheItem numpy;
	UUIDCacheItem uuid;
	IPAddressCacheItem ipaddress;
	ZoneInfoCacheItem zoneinfo;

	py::handle AddCache(py::object item);

private:
	std::mutex cache_mutex;
	std::vector<py::object> owned_objects;
};

} // namespace CHDB
