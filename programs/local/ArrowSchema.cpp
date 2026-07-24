#include "ArrowSchema.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Formats/FormatFactory.h>
#include <Common/typeid_cast.h>
#include <arrow/c/bridge.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

}

using namespace DB;

namespace CHDB
{

ArrowSchemaWrapper::~ArrowSchemaWrapper()
{
    if (arrow_schema.release != nullptr)
    {
        arrow_schema.release(&arrow_schema);
        chassert(!arrow_schema.release);
    }
}

ArrowSchemaWrapper::ArrowSchemaWrapper(ArrowSchemaWrapper && other) noexcept
    : arrow_schema(other.arrow_schema)
{
    other.arrow_schema.release = nullptr;
}

ArrowSchemaWrapper & ArrowSchemaWrapper::operator=(ArrowSchemaWrapper && other) noexcept
{
    if (this != &other)
    {
        if (arrow_schema.release)
        {
            arrow_schema.release(&arrow_schema);
        }
        arrow_schema = other.arrow_schema;
        other.arrow_schema.release = nullptr;
    }
    return *this;
}

/// ClickHouse forbids Nullable over composite types (Tuple/Array/Map). The
/// Arrow-to-CH schema conversion wraps any nullable Arrow field in Nullable,
/// and its composite-type exemption list covers list/map but not struct, so a
/// nullable Arrow struct arrives as the illegal Nullable(Tuple(...)) — also
/// nested, e.g. list<struct> becomes Array(Nullable(Tuple(...))). Strip the
/// illegal Nullable layers recursively; scalar fields keep theirs.
static DataTypePtr dropIllegalNullables(const DataTypePtr & type)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto nested = dropIllegalNullables(nullable->getNestedType());
        /// Nullable(Tuple) is only creatable behind the experimental
        /// allow_experimental_nullable_tuple_type setting (its
        /// canBeInsideNullable() is already true), so never emit it here.
        if (isTuple(nested) || !nested->canBeInsideNullable())
            return nested;
        return makeNullable(nested);
    }
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(dropIllegalNullables(array->getNestedType()));
    if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        return std::make_shared<DataTypeMap>(
            dropIllegalNullables(map->getKeyType()), dropIllegalNullables(map->getValueType()));
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements;
        elements.reserve(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            elements.push_back(dropIllegalNullables(element));
        return tuple->hasExplicitNames() ? std::make_shared<DataTypeTuple>(elements, tuple->getElementNames())
                                         : std::make_shared<DataTypeTuple>(elements);
    }
    return type;
}

void ArrowSchemaWrapper::convertArrowSchema(
    ArrowSchemaWrapper & schema,
    NamesAndTypesList & names_and_types,
    ContextPtr & context,
    bool drop_illegal_nullables)
{
    if (!schema.arrow_schema.release)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ArrowSchema is already released");
    }

    /// Import ArrowSchema to arrow::Schema
    auto arrow_schema_result = arrow::ImportSchema(&schema.arrow_schema);
    if (!arrow_schema_result.ok())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Failed to import Arrow schema: {}", arrow_schema_result.status().message());
    }

    const auto & arrow_schema = arrow_schema_result.ValueOrDie();

    const auto format_settings = getFormatSettings(context);

    /// Convert Arrow schema to ClickHouse header
    auto block = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *arrow_schema,
        nullptr,
        "Arrow",
        format_settings,
        format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        false,
        format_settings.parquet.allow_geoparquet_parser);

    for (const auto & column : block)
    {
        names_and_types.emplace_back(
            column.name, drop_illegal_nullables ? dropIllegalNullables(column.type) : column.type);
    }
}

} // namespace CHDB
