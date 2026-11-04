#pragma once

#include "PythonImportCacheItem.h"

namespace CHDB {

struct PolarsCacheItem : public PythonImportCacheItem
{
public:
	static constexpr const char *Name = "polars";

	PolarsCacheItem()
	    : PythonImportCacheItem("polars"), DataFrame("DataFrame", this), LazyFrame("LazyFrame", this),
	      Series("Series", this)
	{
	}

	~PolarsCacheItem() override = default;

	PythonImportCacheItem DataFrame;
	PythonImportCacheItem LazyFrame;
	PythonImportCacheItem Series;

protected:
	/// polars is an optional dependency: never import it just to look.
	bool IsRequired() const override final
	{
		return false;
	}
};

} // namespace CHDB
