#include "NativeChunkInputStream.h"
#include <memory>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <Common/typeid_cast.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/ChunkInfo.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNSUPPORTED_PARAMETER;
}

NativeChunkInputStream::NativeChunkInputStream(ReadBuffer & istr_, const Block & header_) : istr(istr_), header(header_)
{
}

void NativeChunkInputStream::readData(
    const IDataType & type, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    auto serialization = type.getDefaultSerialization();

    serialization->deserializeBinaryBulkStatePrefix(settings, state);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeChunkInputStream. Rows read:{}. Rows expected:{}.", column->size(), rows);
}

Chunk NativeChunkInputStream::readImpl()
{
    Chunk res;
    if (istr.eof())
    {
        return res;
    }

    /// chunk info
    UInt8 has_chunk_info;
    readVarUInt(has_chunk_info, istr);
    if (has_chunk_info == 1)
    {
        UInt8 chunk_info_type;
        readVarUInt(chunk_info_type, istr);
        // todo:: current we only support AggregatedChunkInfo
        if (chunk_info_type == static_cast<UInt8>(ChunkInfoType::AGGREGATED))
        {
            auto chunk_info = std::make_shared<AggregatedChunkInfo>();
            readAggregatedChunkInfo(istr, chunk_info);
            res.setChunkInfo(chunk_info);
        }
        else
            throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Unsupported chunk info type {}", static_cast<UInt16>(chunk_info_type));
    }
    /// Dimensions
    size_t col_num = 0;
    size_t row_num = 0;

    readVarUInt(col_num, istr);
    readVarUInt(row_num, istr);
    chassert(header.columns() == col_num);

    Columns columns;
    for (size_t i = 0; i < col_num; ++i)
    {
        DataTypePtr data_type = header.getDataTypes().at(i);

        /// Data
        ColumnPtr read_column = data_type->createColumn();

        double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
        if (row_num) /// If no row_num, nothing to read.
            readData(*data_type, read_column, istr, row_num, avg_value_size_hint);

        columns.emplace_back(std::move(read_column));
    }
    res.setColumns(columns, row_num);
    return res;
}
}

