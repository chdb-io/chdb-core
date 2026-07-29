#pragma once

#include "ArrowStreamWrapper.h"
#include "PybindWrapper.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace CHDB
{

class DataSourceWrapper
{
public:
    explicit DataSourceWrapper(py::object reader) : data_source(std::move(reader)) {}

    DataSourceWrapper(const DataSourceWrapper &) = delete;
    DataSourceWrapper & operator=(const DataSourceWrapper &) = delete;

    ~DataSourceWrapper()
    {
        py::gil_scoped_acquire acquire;
        cached_arrow_stream.reset();
        column_cache.clear();
        data_source.dec_ref();
        data_source.release();
    }

    py::object & getDataSource() { return data_source; }
    const py::object & getDataSource() const { return data_source; }

    void cacheColumnData(const std::string & col_name, py::array data)
    {
        column_cache[col_name] = std::move(data);
    }

    py::array * getCachedColumnData(const std::string & col_name)
    {
        auto it = column_cache.find(col_name);
        if (it != column_cache.end())
            return &it->second;
        return nullptr;
    }

    bool hasCachedColumnData(const std::string & col_name) const
    {
        return column_cache.contains(col_name);
    }

    /// Arrow stream parked by schema inference for reuse by the scan, so a
    /// query lifecycle calls __arrow_c_stream__ on the producer only once
    /// (one-shot producers cannot export a second stream).
    void cacheArrowStream(std::unique_ptr<ArrowArrayStreamWrapper> stream) { cached_arrow_stream = std::move(stream); }
    ArrowArrayStreamWrapper * peekCachedArrowStream() { return cached_arrow_stream.get(); }
    std::unique_ptr<ArrowArrayStreamWrapper> takeCachedArrowStream() { return std::move(cached_arrow_stream); }

private:
    py::object data_source;
    std::unordered_map<std::string, py::array> column_cache;
    std::unique_ptr<ArrowArrayStreamWrapper> cached_arrow_stream;
};

using DataSourceWrapperPtr = std::shared_ptr<DataSourceWrapper>;

} // namespace CHDB
