#include <Processors/Sinks/SinkToStorage.h>

#include <Columns/IColumn.h>

namespace DB
{

namespace
{

bool hasBorrowedStorageDeep(const IColumn & column)
{
    if (column.hasBorrowedStorage())
        return true;
    bool found = false;
    column.forEachSubcolumnRecursively([&](const IColumn & subcolumn)
    {
        found = found || subcolumn.hasBorrowedStorage();
    });
    return found;
}

/// Columns handed to a storage sink may be retained beyond the current query
/// (Memory/Join/Set/Buffer tables, materialized views). A column mounted
/// zero-copy over an externally-owned buffer (e.g. Python(df)) must own its
/// memory before that: otherwise the stored data would alias - and be
/// silently rewritten by - later writes to the external buffer, and would pin
/// it for the lifetime of the storage. No-op for regular columns.
void materializeBorrowedColumns(Chunk & chunk)
{
    if (!chunk.hasColumns())
        return;
    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        if (column && hasBorrowedStorageDeep(*column))
            column = IColumn::mutate(std::move(column));
    chunk.setColumns(std::move(columns), num_rows);
}

}

SinkToStorage::SinkToStorage(SharedHeader header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::onConsume(Chunk chunk)
{
    materializeBorrowedColumns(chunk);
    consume(chunk);
    cur_chunk = std::move(chunk);
}

SinkToStorage::GenerateResult SinkToStorage::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
